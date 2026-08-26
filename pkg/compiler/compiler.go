// Package compiler ports the TS workflows compiler (compiler.ts): it turns a
// flat workflow graph (nodes + edges) into a compiled Workflow made of
// trigger-bound pipelines and run-scoped resource services.
package compiler

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/google/uuid"
)

// NodeType mirrors the TS WorkflowNode.type discriminator.
type NodeType string

const (
	NodeExecutable NodeType = "executable"
	NodeResource   NodeType = "resource"
	NodeContainer  NodeType = "container"
	NodePause      NodeType = "pause"
)

// Node is a flattened workflow node (the compiler's view of the graph).
type Node struct {
	ID       string
	Type     NodeType
	Kind     string
	Config   map[string]any
	ParentID string
	Position struct {
		X float64
		Y float64
	}
}

// EdgeRole mirrors the TS WorkflowEdgeData.role.
type EdgeRole string

const (
	EdgeFlow        EdgeRole = "flow"
	EdgeDependency  EdgeRole = "dependency"
	EdgePlaceholder EdgeRole = "placeholder"
)

// Edge is a directed connection between two nodes.
type Edge struct {
	ID           string
	Source       string
	Target       string
	SourceHandle string
	Role         EdgeRole
}

// manualEvent is the trigger event emitted by the runtime to start a workflow.
const manualEvent = "__manual__"

// ---------------------------------------------------------------------------
// Edge utilities
// ---------------------------------------------------------------------------

func flowEdges(edges []Edge) []Edge {
	out := make([]Edge, 0, len(edges))
	for _, e := range edges {
		if e.Role == EdgeFlow {
			out = append(out, e)
		}
	}
	return out
}

func dependencyEdgesTo(targetID string, edges []Edge) []Edge {
	out := make([]Edge, 0, len(edges))
	for _, e := range edges {
		if e.Role == EdgeDependency && e.Target == targetID {
			out = append(out, e)
		}
	}
	return out
}

// makeResolveHandle returns a resolver that maps a source handle id on the
// given node to the target node id of the matching flow edge. Edges that point
// at an output node are ignored (output nodes terminate flow).
func makeResolveHandle(sourceID string, flow []Edge, byID map[string]Node) func(string) string {
	return func(handle string) string {
		for _, e := range flow {
			if e.Source != sourceID {
				continue
			}
			if (e.SourceHandle == "") != (handle == "") {
				continue
			}
			if handle != "" && e.SourceHandle != handle {
				continue
			}
			target, ok := byID[e.Target]
			if !ok {
				return e.Target
			}
			if target.Type == NodeExecutable && target.Kind == "output" {
				return ""
			}
			return e.Target
		}
		return ""
	}
}

// nextDefaultTarget returns the target of the first outgoing flow edge from
// sourceID (used for linear/default routing).
func nextDefaultTarget(sourceID string, flow []Edge) string {
	for _, e := range flow {
		if e.Source == sourceID {
			return e.Target
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// Graph helpers
// ---------------------------------------------------------------------------

// buildResourcesFor collects the resource artifact keys a node depends on,
// keyed by resource kind: {kind: "resource:<sourceNodeId>"}. The runtime
// resolves these keys into initialized handles.
func buildResourcesFor(nodeID string, edges []Edge, byID map[string]Node) map[string]any {
	resources := map[string]any{}
	for _, e := range dependencyEdgesTo(nodeID, edges) {
		source, ok := byID[e.Source]
		if !ok {
			continue
		}
		if source.Kind == "" {
			continue
		}
		resources[source.Kind] = "resource:" + e.Source
	}
	return resources
}

// buildResourceServices compiles resource nodes into run-scoped services.
func buildResourceServices(nodes []Node) ([]pipeline.Service, error) {
	var services []pipeline.Service
	for _, node := range nodes {
		if node.Type != NodeResource {
			continue
		}
		def, ok := nodekit.Get(node.Kind)
		if !ok || def.Type != "resource" {
			return nil, fmt.Errorf(
				"Unknown resource kind %q (node %s). Ensure the resource type is registered in the node registry.",
				node.Kind, node.ID)
		}
		config := node.Config
		if config == nil {
			config = map[string]any{}
		}
		services = append(services, pipeline.Service{
			ID:    "resource:" + node.ID,
			Scope: "run",
			Kind:  def.Kind,
			Label: def.Label,
			Init: func(ctx context.Context) (any, error) {
				if def.ResourceInit == nil {
					return nil, nil
				}
				return def.ResourceInit(ctx, nodekit.NodeRunContext{
					NodeID: node.ID,
					Config: config,
				})
			},
			Cleanup: func(ctx context.Context, handle any) error {
				if def.ResourceEnd == nil {
					return nil
				}
				return def.ResourceEnd(ctx, nodekit.NodeRunContext{
					NodeID: node.ID,
					Config: config,
				}, handle)
			},
		})
	}
	return services, nil
}

// ---------------------------------------------------------------------------
// Outer BFS
// ---------------------------------------------------------------------------

func bfsStages(startID string, byID map[string]Node, allEdges []Edge) (reached []string, orderByID map[string]int) {
	flow := flowEdges(allEdges)
	orderByID = map[string]int{}
	reached = []string{}
	queue := []string{startID}
	seen := map[string]bool{}
	ord := 0

	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if seen[id] {
			continue
		}
		node, ok := byID[id]
		if !ok || node.ParentID != "" || node.Type == NodeResource {
			continue
		}
		seen[id] = true
		ord += 10
		orderByID[id] = ord
		reached = append(reached, id)

		if node.Type == NodeExecutable {
			def, hasDef := nodekit.Get(node.Kind)
			if hasDef && def.BodyHandle != "" {
				for _, e := range flow {
					if e.Source == id && e.SourceHandle != def.BodyHandle {
						queue = append(queue, e.Target)
					}
				}
			} else if hasDef && def.Router != nil {
				for _, e := range flow {
					if e.Source == id {
						queue = append(queue, e.Target)
					}
				}
			} else {
				if t := nextDefaultTarget(id, flow); t != "" {
					queue = append(queue, t)
				}
			}
		} else {
			if t := nextDefaultTarget(id, flow); t != "" {
				queue = append(queue, t)
			}
		}
	}

	return reached, orderByID
}

// ---------------------------------------------------------------------------
// Scoped BFS for a bounded node's body subgraph
// ---------------------------------------------------------------------------

func bfsBodyNodes(boundedNodeID, bodyEntryID string, byID map[string]Node, allEdges []Edge) (reached []string, orderByID map[string]int, bodyEdges []Edge) {
	flow := flowEdges(allEdges)
	orderByID = map[string]int{}
	reached = []string{}
	reachedSet := map[string]bool{}
	queue := []string{bodyEntryID}
	seen := map[string]bool{boundedNodeID: true}
	ord := 0

	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if seen[id] {
			continue
		}
		node, ok := byID[id]
		if !ok || node.ParentID != "" || node.Type == NodeResource {
			continue
		}
		seen[id] = true
		ord += 10
		orderByID[id] = ord
		reached = append(reached, id)
		reachedSet[id] = true

		if node.Type == NodeExecutable {
			def, hasDef := nodekit.Get(node.Kind)
			if hasDef && (def.BodyHandle != "" || def.Router != nil) {
				for _, e := range flow {
					if e.Source == id && e.Target != "" && e.Target != boundedNodeID {
						queue = append(queue, e.Target)
					}
				}
			} else {
				if t := nextDefaultTarget(id, flow); t != "" && t != boundedNodeID {
					queue = append(queue, t)
				}
			}
		} else {
			if t := nextDefaultTarget(id, flow); t != "" && t != boundedNodeID {
				queue = append(queue, t)
			}
		}
	}

	for _, e := range allEdges {
		if reachedSet[e.Source] && reachedSet[e.Target] {
			bodyEdges = append(bodyEdges, e)
		}
	}

	return reached, orderByID, bodyEdges
}

// ---------------------------------------------------------------------------
// Stage container compilation
// ---------------------------------------------------------------------------

func compileStageNode(stageNode Node, order int, childrenOf map[string][]Node, byID map[string]Node, allEdges []Edge) (pipeline.Stage, error) {
	flow := flowEdges(allEdges)
	stageID := stageNode.ID
	stageLabel := "Stage"
	if v, ok := stageNode.Config["label"].(string); ok && v != "" {
		stageLabel = v
	}
	children := childrenOf[stageID]

	var stepChildren []Node
	var routingChild *Node
	for i := range children {
		child := children[i]
		if child.Type != NodeExecutable {
			continue
		}
		def, hasDef := nodekit.Get(child.Kind)
		if hasDef && def.Router != nil {
			if routingChild != nil {
				return pipeline.Stage{}, fmt.Errorf(
					"Stage %q (%s) contains more than one routing node. Use separate top-level stages for sequential routing logic.",
					stageLabel, stageID)
			}
			rc := child
			routingChild = &rc
			if def.Run != nil {
				stepChildren = append(stepChildren, child)
			}
		} else {
			stepChildren = append(stepChildren, child)
		}
	}

	steps := map[string]pipeline.Step{}
	for _, child := range stepChildren {
		def, _ := nodekit.Get(child.Kind)
		if def.Run != nil {
			resources := buildResourcesFor(child.ID, allEdges, byID)
			steps[child.ID] = nodekit.BuildStep(child.ID, def, child.Config, staticResources(resources))
		}
	}

	if routingChild == nil {
		return pipeline.Stage{
			ID:    stageID,
			Order: order,
			Label: stageLabel,
			Steps: steps,
			Router: func(ctx context.Context, state map[string]any, st store.Store) (pipeline.RoutingInstruction, error) {
				target := nextDefaultTarget(stageID, flow)
				if target == "" {
					return nil, nil
				}
				return pipeline.Jump(target), nil
			},
		}, nil
	}

	def, _ := nodekit.Get(routingChild.Kind)
	resolveHandle := makeResolveHandle(routingChild.ID, flow, byID)
	router := nodekit.BuildRouter(routingChild.ID, def, routingChild.Config, resolveHandle)

	return pipeline.Stage{
		ID:     stageID,
		Order:  order,
		Label:  stageLabel,
		Steps:  steps,
		Router: router,
	}, nil
}

// staticResources wraps a fixed resource key map as a resources provider.
func staticResources(resources map[string]any) func() map[string]any {
	return func() map[string]any { return resources }
}

// ---------------------------------------------------------------------------
// Top-level stage compilation
// ---------------------------------------------------------------------------

func compileStages(
	reached []string,
	orderByID map[string]int,
	byID map[string]Node,
	childrenOf map[string][]Node,
	allEdges []Edge,
	registry pipeline.PipelineRegistry,
	requirements *[]pipeline.Requirement,
	forkBranches map[string]string,
	joinTargets map[string]string,
) ([]pipeline.Stage, error) {
	flow := flowEdges(allEdges)
	stages := make([]pipeline.Stage, 0, len(reached))

	for _, id := range reached {
		node := byID[id]
		order := orderByID[id]

		if node.Type == NodeContainer {
			stage, err := compileStageNode(node, order, childrenOf, byID, allEdges)
			if err != nil {
				return nil, err
			}
			stages = append(stages, stage)
			continue
		}

		if node.Type == NodePause {
			return nil, fmt.Errorf(
				"Pause node %q is not supported in the current backend. Pause nodes require frontend-specific registration.",
				id)
		}

		if node.Type != NodeExecutable {
			continue
		}

		// Skip nodes that belong to a fork branch — they're compiled as
		// sub-pipelines inside their parent fork stage.
		if forkID, ok := forkBranches[id]; ok {
			_ = forkID // available for error context if needed
			continue
		}

		def, hasDef := nodekit.Get(node.Kind)

		if !hasDef {
			return nil, fmt.Errorf(
				"Unknown node kind %q (node %s). Ensure the node type is registered in the node registry.",
				node.Kind, id)
		}

		// Validate node config against its schema at compile time.
		if err := def.ValidateConfig(node.Config); err != nil {
			return nil, fmt.Errorf("node %q (kind: %q): %w", id, node.Kind, err)
		}

		// Aggregate declared env/secret requirements for runtime pre-flight.
		for _, req := range def.Requirements {
			*requirements = appendRequirement(*requirements, req)
		}

		// ---- Pipeline reference -----------------------------------------
		if node.Kind == "pipeline-ref" {
			pipelineID, _ := node.Config["pipelineId"].(string)
			pipelineID = strings.TrimSpace(pipelineID)
			if pipelineID == "" {
				// @note #review-20260825-003 issue status=open priority=P2 tags=#review,#style : Error string ends with period
				//
				// Go convention: error strings should not end with punctuation.
				// Change to: fmt.Errorf("pipeline-ref node %s has no pipelineId configured", id)
				return nil, fmt.Errorf("pipeline-ref node %s has no pipelineId configured.", id)
			}
			referencedDef, ok := registry.Resolve(pipelineID)
			if !ok || referencedDef == nil {
				// @note #review-20260825-004 issue status=open priority=P2 tags=#review,#style : Error string ends with period
				//
				// Go convention: error strings should not end with punctuation.
				return nil, fmt.Errorf(
					"Pipeline %q not found in registry (node %s). Ensure the pipeline is compiled and registered before running this workflow.",
					pipelineID, id)
			}
			stageCfg := map[string]any{}
			if initialState, ok := node.Config["initialState"].(map[string]any); ok {
				stageCfg["initialState"] = initialState
			}
			if resultKey, ok := node.Config["resultKey"].(string); ok {
				stageCfg["resultKey"] = resultKey
			}
			stages = append(stages, pipeline.Stage{
				ID:        id,
				Order:     order,
				Label:     referencedDef.Label,
				Config:    stageCfg,
				Pipelines: []pipeline.PipelineDefinition{*referencedDef},
				PipelinesRouter: func(ctx context.Context, state map[string]any, results []pipeline.PipelineRunResult, st store.Store) (pipeline.RoutingInstruction, error) {
					for _, r := range results {
						if r.PipelineID == pipelineID && r.Status == "succeeded" {
							target := nextDefaultTarget(id, flow)
							if target == "" {
								return nil, nil
							}
							return pipeline.Jump(target), nil
						}
					}
					return nil, nil
				},
			})
			continue
		}

		// ---- Fork node ---------------------------------------------------
		if node.Kind == "fork" {
			joinID := joinTargets[id]
			branchStages := map[string][]pipeline.Stage{}
			for _, e := range flow {
				if e.Source == id && e.SourceHandle != "" {
					branchReached, branchOrderByID, branchEdges := bfsBodyNodes(id, e.Target, byID, allEdges)
					if len(branchReached) == 0 {
						return nil, fmt.Errorf(
							"Fork node %q branch %q is empty. Connect at least one node after the branch handle.",
							id, e.SourceHandle)
					}
					bs, err := compileStages(branchReached, branchOrderByID, byID, childrenOf, branchEdges, registry, requirements, forkBranches, joinTargets)
					if err != nil {
						return nil, err
					}
					branchStages[e.SourceHandle] = bs
				}
			}
			stages = append(stages, nodekit.BuildForkStage(id, def, order, branchStages, joinID))
			continue
		}

		resources := buildResourcesFor(id, allEdges, byID)
		resolveHandle := makeResolveHandle(id, flow, byID)

		// ---- Bounded node (try-catch, etc.) -----------------------------
		if def.BodyHandle != "" {
			var bodyEdge *Edge
			for i := range flow {
				e := flow[i]
				if e.Source == id && e.SourceHandle == def.BodyHandle {
					bodyEdge = &e
					break
				}
			}
			if bodyEdge == nil {
				return nil, fmt.Errorf(
					"Bounded node %q (kind: %q) has no outgoing %q edge. Connect the %q handle to the first node of the body.",
					id, node.Kind, def.BodyHandle, def.BodyHandle)
			}

			bodyReached, bodyOrderByID, bodyEdges := bfsBodyNodes(id, bodyEdge.Target, byID, allEdges)
			if len(bodyReached) == 0 {
				return nil, fmt.Errorf(
					"Bounded node %q (kind: %q): body is empty. Connect at least one node after the %q handle.",
					id, node.Kind, def.BodyHandle)
			}

			bodyStages, err := compileStages(bodyReached, bodyOrderByID, byID, childrenOf, bodyEdges, registry, requirements, forkBranches, joinTargets)
			if err != nil {
				return nil, err
			}

			// Distribute uses parallel sub-pipelines; other bounded nodes use sequential.
			if def.Kind == "distribute" {
				stages = append(stages, nodekit.BuildDistributeStage(
					id, def, node.Config, staticResources(resources), resolveHandle, order, bodyStages))
			} else {
				stages = append(stages, nodekit.BuildBoundedStage(
					id, def, node.Config, staticResources(resources), resolveHandle, order, bodyStages))
			}
			continue
		}

		// ---- Standard executable node -----------------------------------
		stages = append(stages, nodekit.BuildStage(id, def, node.Config, staticResources(resources), resolveHandle, order))
	}

	return stages, nil
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

// Compile compiles a workflow graph into a runnable Workflow. Each top-level
// trigger node produces its own pipeline keyed by the trigger node id.
func Compile(nodes []Node, edges []Edge, registry pipeline.PipelineRegistry) (*pipeline.Workflow, error) {
	var triggers []Node
	for _, n := range nodes {
		if n.Type == NodeExecutable && n.Kind == "trigger" && n.ParentID == "" {
			triggers = append(triggers, n)
		}
	}
	if len(triggers) == 0 {
		return nil, fmt.Errorf("Add at least one top-level Trigger node to start the workflow.")
	}

	byID := map[string]Node{}
	for _, n := range nodes {
		byID[n.ID] = n
	}

	childrenOf := map[string][]Node{}
	for _, n := range nodes {
		if n.ParentID == "" {
			continue
		}
		childrenOf[n.ParentID] = append(childrenOf[n.ParentID], n)
	}
	for id := range childrenOf {
		list := childrenOf[id]
		sort.SliceStable(list, func(i, j int) bool {
			if list[i].Position.Y != list[j].Position.Y {
				return list[i].Position.Y < list[j].Position.Y
			}
			return list[i].Position.X < list[j].Position.X
		})
		childrenOf[id] = list
	}

	workflowTriggers := map[string]pipeline.WorkflowTrigger{}
	pipelines := map[string]pipeline.PipelineDefinition{}
	var requirements []pipeline.Requirement

	for _, triggerNode := range triggers {
		def, hasDef := nodekit.Get(triggerNode.Kind)
		if !hasDef {
			return nil, fmt.Errorf(
				"Trigger node %s of kind %q has no buildTrigger implementation. Ensure the node type is registered.",
				triggerNode.ID, triggerNode.Kind)
		}
		trigger, err := nodekit.BuildTrigger(triggerNode.ID, def, triggerNode.Config)
		if err != nil {
			return nil, err
		}
		workflowTriggers[triggerNode.ID] = *trigger

		reached, orderByID := bfsStages(triggerNode.ID, byID, edges)

		// Detect fork nodes and validate convergence at join.
		forkBranches, joinTargets, err := detectForks(reached, byID, edges)
		if err != nil {
			return nil, err
		}

		stages, err := compileStages(reached, orderByID, byID, childrenOf, edges, registry, &requirements, forkBranches, joinTargets)
		if err != nil {
			return nil, err
		}
		pipelines[triggerNode.ID] = pipeline.PipelineDefinition{
			ID:     triggerNode.ID,
			Label:  "Pipeline for " + triggerNode.ID,
			Stages: stages,
		}
	}

	services, err := buildResourceServices(nodes)
	if err != nil {
		return nil, err
	}

	return &pipeline.Workflow{
		ID:           uuid.NewString(),
		Label:        "Flowforge workflow",
		Triggers:     workflowTriggers,
		Pipelines:    pipelines,
		Services:     services,
		Requirements: requirements,
	}, nil
}

// appendRequirement appends req unless an identical kind+key requirement is
// already present.
func appendRequirement(reqs []pipeline.Requirement, req pipeline.Requirement) []pipeline.Requirement {
	for _, existing := range reqs {
		if existing.Kind == req.Kind && existing.Key == req.Key {
			return reqs
		}
	}
	return append(reqs, req)
}

// detectForks scans the reached node set for fork nodes, traces each branch to
// its terminal node, and validates that all branches converge at the same join
// node (a node with kind "join"). Returns:
//   - forkBranches: maps branchNodeID → forkNodeID (used by compileStages to skip)
//   - joinTargets:  maps forkNodeID → joinNodeID   (used by BuildForkStage)
func detectForks(reached []string, byID map[string]Node, allEdges []Edge) (forkBranches map[string]string, joinTargets map[string]string, err error) {
	forkBranches = map[string]string{}
	joinTargets = map[string]string{}
	flow := flowEdges(allEdges)

	for _, id := range reached {
		node := byID[id]
		if node.Kind != "fork" {
			continue
		}

		// Collect branch edge targets.
		type branchEnd struct {
			handle     string
			terminalID string
		}
		var branches []branchEnd
		for _, e := range flow {
			if e.Source == id && e.SourceHandle != "" {
				terminal := traceToTerminal(e.Target, byID, flow)
				branches = append(branches, branchEnd{handle: e.SourceHandle, terminalID: terminal})
			}
		}

		if len(branches) == 0 {
			err = fmt.Errorf("Fork node %q has no outgoing branch edges. Connect branch handles to downstream nodes.", id)
			return
		}

		// Validate all branches terminate at the same join node.
		var joinID string
		for _, b := range branches {
			terminalNode, ok := byID[b.terminalID]
			if !ok {
				err = fmt.Errorf("Fork node %q branch %q leads to unknown node %q.", id, b.handle, b.terminalID)
				return
			}
			if terminalNode.Kind != "join" {
				err = fmt.Errorf("Fork node %q branch %q must terminate at a Join node, but terminates at %q (kind: %q).",
					id, b.handle, b.terminalID, terminalNode.Kind)
				return
			}
			if joinID == "" {
				joinID = b.terminalID
			} else if joinID != b.terminalID {
				err = fmt.Errorf("Fork node %q branches do not converge: branch %q ends at %q, but another branch ends at %q. All branches must terminate at the same Join node.",
					id, b.handle, b.terminalID, joinID)
				return
			}
		}

		joinTargets[id] = joinID

		// Mark all non-terminal branch nodes as fork branches (to skip from flat list).
		for _, e := range flow {
			if e.Source == id && e.SourceHandle != "" {
				markBranchNodes(e.Target, id, byID, flow, forkBranches, joinID)
			}
		}
	}

	return
}

// traceToTerminal follows the single outgoing edge from a node until it
// reaches a node with no outgoing flow edges (a terminal node).
func traceToTerminal(startID string, byID map[string]Node, flow []Edge) string {
	current := startID
	seen := map[string]bool{}
	for {
		if seen[current] {
			return current
		}
		seen[current] = true
		node, ok := byID[current]
		if !ok {
			return current
		}
		// If this is a join node, it's the terminal.
		if node.Kind == "join" {
			return current
		}
		// Find the single outgoing flow edge.
		var next string
		for _, e := range flow {
			if e.Source == current && e.Target != "" {
				if next != "" {
					// Multiple outgoing edges — this node is a branching point,
					// not a simple pass-through. Return current as terminal.
					return current
				}
				next = e.Target
			}
		}
		if next == "" {
			return current
		}
		current = next
	}
}

// markBranchNodes recursively marks all nodes reachable from startID as
// belonging to the given fork, stopping at the join node.
func markBranchNodes(startID, forkID string, byID map[string]Node, flow []Edge, forkBranches map[string]string, joinID string) {
	queue := []string{startID}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if id == joinID {
			continue // don't mark the join node
		}
		if _, already := forkBranches[id]; already {
			continue
		}
		if _, ok := byID[id]; !ok {
			continue
		}
		forkBranches[id] = forkID
		for _, e := range flow {
			if e.Source == id && e.Target != "" {
				queue = append(queue, e.Target)
			}
		}
	}
}
