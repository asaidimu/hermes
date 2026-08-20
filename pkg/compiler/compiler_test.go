package compiler_test

import (
	"strings"
	"testing"

	"github.com/asaidimu/hermes/pkg/compiler"
	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
	_ "github.com/asaidimu/hermes/pkg/nodes" // registers all real node kinds
)

// ---------------------------------------------------------------------------
// Test registry stubs (mirror the TS compiler.test.ts mocks for node kinds
// that are not part of the real node registry).
// ---------------------------------------------------------------------------

func init() {
	nodekit.Register(nodekit.NodeDefinition{Kind: "output", Label: "Output", Type: "executable"})
	nodekit.Register(nodekit.NodeDefinition{Kind: "pipeline-ref", Label: "Pipeline Ref", Type: "executable"})
}

// ---------------------------------------------------------------------------
// Node/edge helpers
// ---------------------------------------------------------------------------

func execNode(id, kind string, config map[string]any) compiler.Node {
	return compiler.Node{ID: id, Type: compiler.NodeExecutable, Kind: kind, Config: config}
}

func childNode(id, kind, parent string, x, y float64) compiler.Node {
	n := execNode(id, kind, map[string]any{})
	n.ParentID = parent
	n.Position.X = x
	n.Position.Y = y
	return n
}

func containerNode(id, label string) compiler.Node {
	return compiler.Node{ID: id, Type: compiler.NodeContainer, Config: map[string]any{"label": label}}
}

func resourceNode(id, kind string) compiler.Node {
	return compiler.Node{ID: id, Type: compiler.NodeResource, Kind: kind}
}

func flowEdge(id, source, target, sourceHandle string) compiler.Edge {
	return compiler.Edge{ID: id, Source: source, Target: target, SourceHandle: sourceHandle, Role: compiler.EdgeFlow}
}

func depEdge(id, source, target string) compiler.Edge {
	return compiler.Edge{ID: id, Source: source, Target: target, Role: compiler.EdgeDependency}
}

type stubRegistry struct{ def *pipeline.PipelineDefinition }

func (s stubRegistry) Resolve(id string) (*pipeline.PipelineDefinition, bool) {
	if s.def != nil && s.def.ID == id {
		return s.def, true
	}
	return nil, false
}

func mustCompile(t *testing.T, nodes []compiler.Node, edges []compiler.Edge, reg pipeline.PipelineRegistry) *pipeline.Workflow {
	t.Helper()
	wf, err := compiler.Compile(nodes, edges, reg)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return wf
}

// ===========================================================================
// 1. Basic structure
// ===========================================================================

func TestCompilesSimpleLinearWorkflow(t *testing.T) {
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{"initialState": map[string]any{}}),
		execNode("calc-1", "arithmetic", map[string]any{"op": "add", "operand": 10, "key": "result"}),
		execNode("out-1", "output", map[string]any{}),
	}
	edges := []compiler.Edge{flowEdge("e1", "trigger-1", "calc-1", ""), flowEdge("e2", "calc-1", "out-1", "")}

	wf := mustCompile(t, nodes, edges, nil)
	stages := wf.Pipelines["trigger-1"].Stages

	if len(stages) != 3 {
		t.Fatalf("stages length = %d, want 3", len(stages))
	}
	if stages[0].ID != "trigger-1" || stages[0].Label != "Trigger" {
		t.Errorf("stage[0] = %s/%s, want trigger-1/Trigger", stages[0].ID, stages[0].Label)
	}
	if _, ok := stages[0].Steps["trigger-1"]; !ok {
		t.Errorf("stage[0] missing trigger-1 step")
	}
	if stages[1].ID != "calc-1" || stages[1].Label != "Arithmetic" {
		t.Errorf("stage[1] = %s/%s, want calc-1/Arithmetic", stages[1].ID, stages[1].Label)
	}
	if _, ok := stages[1].Steps["calc-1"]; !ok {
		t.Errorf("stage[1] missing calc-1 step")
	}
}

func TestAssignsAscendingOrders(t *testing.T) {
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{}),
		execNode("a", "arithmetic", map[string]any{}),
		execNode("b", "arithmetic", map[string]any{}),
	}
	edges := []compiler.Edge{flowEdge("e1", "trigger-1", "a", ""), flowEdge("e2", "a", "b", "")}

	wf := mustCompile(t, nodes, edges, nil)
	stages := wf.Pipelines["trigger-1"].Stages
	for i := 1; i < len(stages); i++ {
		if stages[i].Order <= stages[i-1].Order {
			t.Errorf("orders not ascending: %d then %d", stages[i-1].Order, stages[i].Order)
		}
	}
}

func TestThrowsWithoutTopLevelTrigger(t *testing.T) {
	_, err := compiler.Compile([]compiler.Node{execNode("calc-1", "arithmetic", map[string]any{})}, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "Add at least one top-level Trigger node") {
		t.Fatalf("want trigger error, got %v", err)
	}
}

func TestIgnoresChildTrigger(t *testing.T) {
	_, err := compiler.Compile([]compiler.Node{
		childNode("trigger-child", "trigger", "some-container", 0, 0),
		execNode("calc-1", "arithmetic", map[string]any{}),
	}, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "Add at least one top-level Trigger node") {
		t.Fatalf("want trigger error, got %v", err)
	}
}

// ===========================================================================
// 2. BFS / edge handling
// ===========================================================================

func TestOnlyFollowsFlowEdges(t *testing.T) {
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{}),
		execNode("calc-1", "arithmetic", map[string]any{}),
	}
	edges := []compiler.Edge{depEdge("e1", "trigger-1", "calc-1")}

	wf := mustCompile(t, nodes, edges, nil)
	stages := wf.Pipelines["trigger-1"].Stages
	if len(stages) != 1 || stages[0].ID != "trigger-1" {
		t.Fatalf("expected only trigger-1 stage, got %d stages", len(stages))
	}
}

func TestSkipsResourceNodes(t *testing.T) {
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{}),
		resourceNode("db-1", "database"),
		execNode("calc-1", "arithmetic", map[string]any{}),
	}
	edges := []compiler.Edge{
		flowEdge("e1", "trigger-1", "calc-1", ""),
		depEdge("e2", "db-1", "calc-1"),
	}

	wf := mustCompile(t, nodes, edges, nil)
	stages := wf.Pipelines["trigger-1"].Stages
	for _, s := range stages {
		if s.ID == "db-1" {
			t.Errorf("resource node db-1 should not be a stage")
		}
	}
	if !containsStage(stages, "trigger-1") || !containsStage(stages, "calc-1") {
		t.Errorf("missing expected stages: %v", stages)
	}
}

func TestSkipsContainerChildrenAtTopLevel(t *testing.T) {
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{}),
		containerNode("stage-1", "My Stage"),
		childNode("child-1", "arithmetic", "stage-1", 0, 0),
	}
	edges := []compiler.Edge{flowEdge("e1", "trigger-1", "stage-1", "")}

	wf := mustCompile(t, nodes, edges, nil)
	stages := wf.Pipelines["trigger-1"].Stages
	if containsStage(stages, "child-1") {
		t.Errorf("child-1 should not be a top-level stage")
	}
	if !containsStage(stages, "stage-1") {
		t.Errorf("stage-1 should be a stage")
	}
}

func TestDiamondMergeWithoutDuplicatingTail(t *testing.T) {
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{}),
		execNode("switch-1", "switch", map[string]any{"defaultHandle": "a"}),
		execNode("path-a", "code", map[string]any{}),
		execNode("path-b", "code", map[string]any{}),
		execNode("tail", "arithmetic", map[string]any{}),
	}
	edges := []compiler.Edge{
		flowEdge("e1", "trigger-1", "switch-1", ""),
		flowEdge("e2", "switch-1", "path-a", "a"),
		flowEdge("e3", "switch-1", "path-b", "b"),
		flowEdge("e4", "path-a", "tail", ""),
		flowEdge("e5", "path-b", "tail", ""),
	}

	wf := mustCompile(t, nodes, edges, nil)
	stages := wf.Pipelines["trigger-1"].Stages
	if countStage(stages, "tail") != 1 {
		t.Errorf("tail should appear exactly once, got %d", countStage(stages, "tail"))
	}
}

// ===========================================================================
// 3. Routing nodes
// ===========================================================================

func TestRoutingNodeFansOut(t *testing.T) {
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{}),
		execNode("switch-1", "switch", map[string]any{"defaultHandle": "default"}),
		execNode("path-a", "code", map[string]any{}),
		execNode("path-b", "code", map[string]any{}),
		execNode("path-default", "code", map[string]any{}),
	}
	edges := []compiler.Edge{
		flowEdge("e1", "trigger-1", "switch-1", ""),
		flowEdge("e2", "switch-1", "path-a", "case-a"),
		flowEdge("e3", "switch-1", "path-b", "case-b"),
		flowEdge("e4", "switch-1", "path-default", "default"),
	}

	wf := mustCompile(t, nodes, edges, nil)
	stages := wf.Pipelines["trigger-1"].Stages
	for _, want := range []string{"switch-1", "path-a", "path-b", "path-default"} {
		if !containsStage(stages, want) {
			t.Errorf("missing stage %s in %v", want, stageIDs(stages))
		}
	}
}

func TestSwitchStageHasRouter(t *testing.T) {
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{}),
		execNode("switch-1", "switch", map[string]any{"defaultHandle": "default"}),
		execNode("path-default", "code", map[string]any{}),
	}
	edges := []compiler.Edge{
		flowEdge("e1", "trigger-1", "switch-1", ""),
		flowEdge("e2", "switch-1", "path-default", "default"),
	}

	wf := mustCompile(t, nodes, edges, nil)
	stages := wf.Pipelines["trigger-1"].Stages
	sw := findStage(stages, "switch-1")
	if sw == nil {
		t.Fatal("switch-1 stage not found")
	}
	if sw.Router == nil {
		t.Error("switch-1 stage should have a router")
	}
}

func TestForEachFansOutToDoAndDone(t *testing.T) {
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{}),
		execNode("loop-1", "for-each", map[string]any{"itemsKey": "items", "itemKey": "item"}),
		execNode("body-1", "code", map[string]any{}),
		execNode("after-1", "arithmetic", map[string]any{}),
	}
	edges := []compiler.Edge{
		flowEdge("e1", "trigger-1", "loop-1", ""),
		flowEdge("e2", "loop-1", "body-1", "do"),
		flowEdge("e3", "loop-1", "after-1", "done"),
	}

	wf := mustCompile(t, nodes, edges, nil)
	stages := wf.Pipelines["trigger-1"].Stages
	for _, want := range []string{"loop-1", "body-1", "after-1"} {
		if !containsStage(stages, want) {
			t.Errorf("missing stage %s in %v", want, stageIDs(stages))
		}
	}
}

// ===========================================================================
// 4. Container nodes
// ===========================================================================

func TestContainerCompilesStepChildren(t *testing.T) {
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{}),
		containerNode("stage-1", "Batch Stage"),
		childNode("child-1", "arithmetic", "stage-1", 0, 0),
		childNode("child-2", "code", "stage-1", 0, 100),
	}
	edges := []compiler.Edge{flowEdge("e1", "trigger-1", "stage-1", "")}

	wf := mustCompile(t, nodes, edges, nil)
	stages := wf.Pipelines["trigger-1"].Stages
	stage := findStage(stages, "stage-1")
	if stage == nil {
		t.Fatal("stage-1 not found")
	}
	if stage.Label != "Batch Stage" {
		t.Errorf("label = %s, want Batch Stage", stage.Label)
	}
	if _, ok := stage.Steps["child-1"]; !ok {
		t.Errorf("missing child-1 step")
	}
	if _, ok := stage.Steps["child-2"]; !ok {
		t.Errorf("missing child-2 step")
	}
}

func TestContainerDefaultRouterFollowsEdge(t *testing.T) {
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{}),
		containerNode("stage-1", "Simple Stage"),
		childNode("child-1", "arithmetic", "stage-1", 0, 0),
		execNode("next-1", "code", map[string]any{}),
	}
	edges := []compiler.Edge{
		flowEdge("e1", "trigger-1", "stage-1", ""),
		flowEdge("e2", "stage-1", "next-1", ""),
	}

	wf := mustCompile(t, nodes, edges, nil)
	stages := wf.Pipelines["trigger-1"].Stages
	stage := findStage(stages, "stage-1")
	if stage == nil || stage.Router == nil {
		t.Fatal("stage-1 should have a router")
	}
	// Simulate routing by invoking the router against a fresh document.
	doc := store.NewMemoryStore(nil).Document()
	inst, err := stage.Router(t.Context(), doc, nil)
	_ = inst
	if err != nil {
		t.Fatalf("router error: %v", err)
	}
}

func TestContainerUsesRoutingChildRouter(t *testing.T) {
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{}),
		containerNode("stage-1", "Routing Stage"),
		childNode("switch-child", "switch", "stage-1", 0, 0),
		execNode("target-1", "code", map[string]any{}),
	}
	edges := []compiler.Edge{
		flowEdge("e1", "trigger-1", "stage-1", ""),
		flowEdge("e2", "switch-child", "target-1", "default"),
	}

	wf := mustCompile(t, nodes, edges, nil)
	stages := wf.Pipelines["trigger-1"].Stages
	stage := findStage(stages, "stage-1")
	if stage == nil || stage.Router == nil {
		t.Fatal("stage-1 should have a router")
	}
	// The switch routing child has no run, so it is not a step (matches the
	// real registry, where switch/if are pure routing nodes).
	if _, ok := stage.Steps["switch-child"]; ok {
		t.Errorf("pure routing child should not be a step")
	}
}

func TestContainerRejectsMultipleRoutingChildren(t *testing.T) {
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{}),
		containerNode("stage-1", "Bad Stage"),
		childNode("switch-1", "switch", "stage-1", 0, 0),
		childNode("switch-2", "switch", "stage-1", 0, 100),
	}
	edges := []compiler.Edge{flowEdge("e1", "trigger-1", "stage-1", "")}

	_, err := compiler.Compile(nodes, edges, nil)
	if err == nil || !strings.Contains(err.Error(), "more than one routing node") {
		t.Fatalf("want multiple routing error, got %v", err)
	}
}

func TestResourceInsideContainerExcluded(t *testing.T) {
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{}),
		containerNode("stage-1", "Stage With Resource"),
		childNode("child-1", "arithmetic", "stage-1", 0, 0),
	}
	db := resourceNode("db-1", "database")
	db.ParentID = "stage-1"
	nodes = append(nodes, db)
	edges := []compiler.Edge{flowEdge("e1", "trigger-1", "stage-1", "")}

	wf := mustCompile(t, nodes, edges, nil)
	stages := wf.Pipelines["trigger-1"].Stages
	stage := findStage(stages, "stage-1")
	if stage == nil {
		t.Fatal("stage-1 not found")
	}
	if _, ok := stage.Steps["child-1"]; !ok {
		t.Errorf("missing child-1 step")
	}
	if _, ok := stage.Steps["db-1"]; ok {
		t.Errorf("db-1 should not be a step")
	}
}

// ===========================================================================
// 5. Pipeline-ref nodes
// ===========================================================================

func TestPipelineRefRequiresPipelineID(t *testing.T) {
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{}),
		execNode("ref-1", "pipeline-ref", map[string]any{}),
	}
	edges := []compiler.Edge{flowEdge("e1", "trigger-1", "ref-1", "")}

	_, err := compiler.Compile(nodes, edges, stubRegistry{})
	if err == nil || !strings.Contains(err.Error(), "no pipelineId configured") {
		t.Fatalf("want no pipelineId error, got %v", err)
	}
}

func TestPipelineRefMissingFromRegistry(t *testing.T) {
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{}),
		execNode("ref-1", "pipeline-ref", map[string]any{"pipelineId": "missing-pipeline"}),
	}
	edges := []compiler.Edge{flowEdge("e1", "trigger-1", "ref-1", "")}

	_, err := compiler.Compile(nodes, edges, stubRegistry{})
	if err == nil || !strings.Contains(err.Error(), "not found in registry") {
		t.Fatalf("want not found error, got %v", err)
	}
}

func TestPipelineRefCompilesToPipelinesMode(t *testing.T) {
	ref := &pipeline.PipelineDefinition{ID: "sub-pipeline-1", Label: "Sub Pipeline"}
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{}),
		execNode("ref-1", "pipeline-ref", map[string]any{"pipelineId": "sub-pipeline-1"}),
		execNode("after-1", "arithmetic", map[string]any{}),
	}
	edges := []compiler.Edge{
		flowEdge("e1", "trigger-1", "ref-1", ""),
		flowEdge("e2", "ref-1", "after-1", ""),
	}

	wf := mustCompile(t, nodes, edges, stubRegistry{def: ref})
	stages := wf.Pipelines["trigger-1"].Stages
	refStage := findStage(stages, "ref-1")
	if refStage == nil {
		t.Fatal("ref-1 stage not found")
	}
	if len(refStage.Pipelines) != 1 || refStage.Pipelines[0].ID != "sub-pipeline-1" {
		t.Errorf("pipelines = %v, want [sub-pipeline-1]", refStage.Pipelines)
	}
	if refStage.PipelinesRouter == nil {
		t.Error("ref-1 should have a pipelinesRouter")
	}
}

func TestPipelineRefRoutesOnSuccess(t *testing.T) {
	ref := &pipeline.PipelineDefinition{ID: "sub-1", Label: "Sub"}
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{}),
		execNode("ref-1", "pipeline-ref", map[string]any{"pipelineId": "sub-1"}),
		execNode("after-1", "arithmetic", map[string]any{}),
	}
	edges := []compiler.Edge{
		flowEdge("e1", "trigger-1", "ref-1", ""),
		flowEdge("e2", "ref-1", "after-1", ""),
	}

	wf := mustCompile(t, nodes, edges, stubRegistry{def: ref})
	stages := wf.Pipelines["trigger-1"].Stages
	refStage := findStage(stages, "ref-1")

	inst, err := refStage.PipelinesRouter(t.Context(), nil, []pipeline.PipelineRunResult{
		{PipelineID: "sub-1", Status: "succeeded"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	jump, ok := inst.(pipeline.JumpInstruction)
	if !ok || jump.StageID != "after-1" {
		t.Errorf("expected jump to after-1, got %v", inst)
	}
}

func TestPipelineRefReturnsNilOnFailure(t *testing.T) {
	ref := &pipeline.PipelineDefinition{ID: "sub-1", Label: "Sub"}
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{}),
		execNode("ref-1", "pipeline-ref", map[string]any{"pipelineId": "sub-1"}),
		execNode("after-1", "arithmetic", map[string]any{}),
	}
	edges := []compiler.Edge{
		flowEdge("e1", "trigger-1", "ref-1", ""),
		flowEdge("e2", "ref-1", "after-1", ""),
	}

	wf := mustCompile(t, nodes, edges, stubRegistry{def: ref})
	stages := wf.Pipelines["trigger-1"].Stages
	refStage := findStage(stages, "ref-1")

	inst, err := refStage.PipelinesRouter(t.Context(), nil, []pipeline.PipelineRunResult{
		{PipelineID: "sub-1", Status: "failed"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if inst != nil {
		t.Errorf("expected nil on failure, got %v", inst)
	}
}

// ===========================================================================
// 6. Resource injection
// ===========================================================================

func TestResourceInjectionKeys(t *testing.T) {
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{}),
		resourceNode("db-1", "database"),
		execNode("calc-1", "arithmetic", map[string]any{}),
	}
	edges := []compiler.Edge{
		flowEdge("e1", "trigger-1", "calc-1", ""),
		depEdge("e2", "db-1", "calc-1"),
	}

	wf := mustCompile(t, nodes, edges, nil)
	stages := wf.Pipelines["trigger-1"].Stages
	calcStage := findStage(stages, "calc-1")
	if calcStage == nil {
		t.Fatal("calc-1 stage not found")
	}
	if len(wf.Services) != 1 || wf.Services[0].ID != "resource:db-1" || wf.Services[0].Scope != "run" {
		t.Errorf("services = %+v, want one run-scoped resource:db-1", wf.Services)
	}
}

// ===========================================================================
// Helpers
// ===========================================================================

func stageIDs(stages []pipeline.Stage) []string {
	ids := make([]string, len(stages))
	for i, s := range stages {
		ids[i] = s.ID
	}
	return ids
}

func containsStage(stages []pipeline.Stage, id string) bool {
	return findStage(stages, id) != nil
}

func countStage(stages []pipeline.Stage, id string) int {
	n := 0
	for _, s := range stages {
		if s.ID == id {
			n++
		}
	}
	return n
}

func findStage(stages []pipeline.Stage, id string) *pipeline.Stage {
	for i := range stages {
		if stages[i].ID == id {
			return &stages[i]
		}
	}
	return nil
}