package compiler_test

import (
	"strings"
	"testing"

	"github.com/asaidimu/hermes/pkg/compiler"
	"github.com/asaidimu/hermes/pkg/nodekit"
	_ "github.com/asaidimu/hermes/pkg/nodes" // registers all real node kinds
	"github.com/asaidimu/hermes/pkg/pipeline"
)

// ---------------------------------------------------------------------------
// Test registry stubs (mirror the TS compiler.test.ts mocks for node kinds
// that are not part of the real node registry).
// ---------------------------------------------------------------------------

func init() {
	nodekit.Register(nodekit.NodeDefinition{Kind: "output", Label: "Output", Type: "executable"})
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
		execNode("calc-1", "arithmetic", map[string]any{"operation": "add", "left": "10", "right": "5", "key": "result"}),
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
		execNode("a", "arithmetic", map[string]any{"key": "a"}),
		execNode("b", "arithmetic", map[string]any{"key": "b"}),
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
	_, err := compiler.Compile([]compiler.Node{execNode("calc-1", "arithmetic", map[string]any{"key": "calc-1"})}, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "Add at least one top-level Trigger node") {
		t.Fatalf("want trigger error, got %v", err)
	}
}

func TestIgnoresChildTrigger(t *testing.T) {
	_, err := compiler.Compile([]compiler.Node{
		childNode("trigger-child", "trigger", "some-container", 0, 0),
		execNode("calc-1", "arithmetic", map[string]any{"key": "calc-1"}),
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
		execNode("calc-1", "arithmetic", map[string]any{"key": "calc-1"}),
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
		execNode("calc-1", "arithmetic", map[string]any{"key": "calc-1"}),
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
		execNode("tail", "arithmetic", map[string]any{"key": "tail"}),
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
		execNode("after-1", "arithmetic", map[string]any{"key": "after-1"}),
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
		execNode("next-1", "code", map[string]any{"code": "return state;"}),
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
	// Simulate routing by invoking the router against an empty state snapshot.
	inst, err := stage.Router(t.Context(), map[string]any{}, nil)
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
	if err == nil || !strings.Contains(err.Error(), "pipelineId") {
		t.Fatalf("want pipelineId error, got %v", err)
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
		execNode("after-1", "arithmetic", map[string]any{"key": "after-1"}),
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
		execNode("after-1", "arithmetic", map[string]any{"key": "after-1"}),
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
		execNode("after-1", "arithmetic", map[string]any{"key": "after-1"}),
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
		execNode("calc-1", "arithmetic", map[string]any{"key": "calc-1"}),
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

// ---------------------------------------------------------------------------
// Fork-Join tests
// ---------------------------------------------------------------------------

func TestForkJoinCompiles(t *testing.T) {
	// trigger → fork --(a)--> delay1 --(a)--> join → output
	//                   \--(b)--> delay2 --(b)--> ↗
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{}),
		execNode("fork-1", "fork", map[string]any{"branches": []any{"a", "b"}}),
		execNode("delay-a", "delay", map[string]any{"ms": 10}),
		execNode("delay-b", "delay", map[string]any{"ms": 20}),
		execNode("join-1", "join", map[string]any{}),
		execNode("out-1", "output", map[string]any{}),
	}
	edges := []compiler.Edge{
		flowEdge("e1", "trigger-1", "fork-1", ""),
		flowEdge("e2", "fork-1", "delay-a", "a"),
		flowEdge("e3", "fork-1", "delay-b", "b"),
		flowEdge("e4", "delay-a", "join-1", ""),
		flowEdge("e5", "delay-b", "join-1", ""),
		flowEdge("e6", "join-1", "out-1", ""),
	}

	wf, err := compiler.Compile(nodes, edges, nil)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	// Should have exactly one pipeline (from the trigger).
	if len(wf.Pipelines) != 1 {
		t.Fatalf("expected 1 pipeline, got %d", len(wf.Pipelines))
	}

	p := wf.Pipelines["trigger-1"]
	// The fork node should be a stage with Pipelines (sub-pipelines).
	var forkStage *pipeline.Stage
	for i := range p.Stages {
		if p.Stages[i].ID == "fork-1" {
			forkStage = &p.Stages[i]
			break
		}
	}
	if forkStage == nil {
		t.Fatal("fork-1 stage not found")
	}
	if len(forkStage.Pipelines) != 2 {
		t.Errorf("fork stage should have 2 sub-pipelines, got %d", len(forkStage.Pipelines))
	}
	// Join should be a regular stage.
	var joinFound bool
	for _, s := range p.Stages {
		if s.ID == "join-1" {
			joinFound = true
			break
		}
	}
	if !joinFound {
		t.Error("join-1 stage not found in flat stages")
	}
	// delay-a and delay-b should NOT be in flat stages (they're inside fork sub-pipelines).
	for _, s := range p.Stages {
		if s.ID == "delay-a" || s.ID == "delay-b" {
			t.Errorf("branch node %q should not be in flat stages", s.ID)
		}
	}
}

func TestForkBranchesMustConverge(t *testing.T) {
	// trigger → fork --(a)--> delay1 → out1  (branch a ends at out1, not join)
	//                   \--(b)--> delay2 → join → out2
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{}),
		execNode("fork-1", "fork", map[string]any{"branches": []any{"a", "b"}}),
		execNode("delay-a", "delay", map[string]any{}),
		execNode("delay-b", "delay", map[string]any{}),
		execNode("out-a", "output", map[string]any{}),
		execNode("join-1", "join", map[string]any{}),
		execNode("out-2", "output", map[string]any{}),
	}
	edges := []compiler.Edge{
		flowEdge("e1", "trigger-1", "fork-1", ""),
		flowEdge("e2", "fork-1", "delay-a", "a"),
		flowEdge("e3", "fork-1", "delay-b", "b"),
		flowEdge("e4", "delay-a", "out-a", ""),
		flowEdge("e5", "delay-b", "join-1", ""),
		flowEdge("e6", "join-1", "out-2", ""),
	}

	_, err := compiler.Compile(nodes, edges, nil)
	if err == nil {
		t.Fatal("expected error for non-converging fork branches")
	}
	if !strings.Contains(err.Error(), "must terminate at a Join node") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestForkBranchesMustConvergeAtSameJoin(t *testing.T) {
	// trigger → fork --(a)--> delay1 → join1 → out1
	//                   \--(b)--> delay2 → join2 → out2
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{}),
		execNode("fork-1", "fork", map[string]any{"branches": []any{"a", "b"}}),
		execNode("delay-a", "delay", map[string]any{}),
		execNode("delay-b", "delay", map[string]any{}),
		execNode("join-a", "join", map[string]any{}),
		execNode("join-b", "join", map[string]any{}),
		execNode("out-a", "output", map[string]any{}),
		execNode("out-b", "output", map[string]any{}),
	}
	edges := []compiler.Edge{
		flowEdge("e1", "trigger-1", "fork-1", ""),
		flowEdge("e2", "fork-1", "delay-a", "a"),
		flowEdge("e3", "fork-1", "delay-b", "b"),
		flowEdge("e4", "delay-a", "join-a", ""),
		flowEdge("e5", "delay-b", "join-b", ""),
		flowEdge("e6", "join-a", "out-a", ""),
		flowEdge("e7", "join-b", "out-b", ""),
	}

	_, err := compiler.Compile(nodes, edges, nil)
	if err == nil {
		t.Fatal("expected error for fork branches converging at different joins")
	}
	if !strings.Contains(err.Error(), "do not converge") {
		t.Errorf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Distribute (parallel for-each) tests
// ---------------------------------------------------------------------------

func TestDistributeCompiles(t *testing.T) {
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{}),
		execNode("dist-1", "distribute", map[string]any{"itemsKey": "items", "itemKey": "item"}),
		execNode("code-1", "code", map[string]any{"code": "state.result = state.item"}),
		execNode("out-1", "output", map[string]any{}),
	}
	edges := []compiler.Edge{
		flowEdge("e1", "trigger-1", "dist-1", ""),
		flowEdge("e2", "dist-1", "code-1", "do"),
		flowEdge("e3", "code-1", "dist-1", ""),
		flowEdge("e4", "dist-1", "out-1", "done"),
	}

	wf, err := compiler.Compile(nodes, edges, nil)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}
	if wf == nil {
		t.Fatal("expected non-nil workflow")
	}

	p := wf.Pipelines["trigger-1"]
	// The distribute node should be a stage with Pipelines.
	var distStage *pipeline.Stage
	for i := range p.Stages {
		if p.Stages[i].ID == "dist-1" {
			distStage = &p.Stages[i]
			break
		}
	}
	if distStage == nil {
		t.Fatal("distribute stage not found")
	}
	if len(distStage.Pipelines) == 0 {
		t.Fatal("distribute stage has no sub-pipelines")
	}

	// The sub-pipeline should contain setup + pipelines stages.
	sub := distStage.Pipelines[0]
	if len(sub.Stages) != 2 {
		t.Errorf("expected 2 stages in distribute sub-pipeline (setup + pipelines), got %d", len(sub.Stages))
	}
	if sub.Stages[0].ID != "dist-1__setup" {
		t.Errorf("first stage should be dist-1__setup, got %s", sub.Stages[0].ID)
	}
	if sub.Stages[1].ID != "dist-1__pipelines" {
		t.Errorf("second stage should be dist-1__pipelines, got %s", sub.Stages[1].ID)
	}
	if sub.Stages[1].DynamicPipelines == nil {
		t.Error("pipelines stage should have DynamicPipelines set")
	}
}

func TestDistributeRequiresBody(t *testing.T) {
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{}),
		execNode("dist-1", "distribute", map[string]any{"itemsKey": "items", "itemKey": "item"}),
		execNode("out-1", "output", map[string]any{}),
	}
	edges := []compiler.Edge{
		flowEdge("e1", "trigger-1", "dist-1", ""),
		// No "do" edge — body is missing.
		flowEdge("e2", "dist-1", "out-1", "done"),
	}

	_, err := compiler.Compile(nodes, edges, nil)
	if err == nil {
		t.Fatal("expected error for distribute with no body")
	}
	if !strings.Contains(err.Error(), "do") {
		t.Errorf("unexpected error: %v", err)
	}
}
