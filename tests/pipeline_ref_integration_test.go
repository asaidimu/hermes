package tests

import (
	"context"
	"testing"
	"time"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/compiler"
	_ "github.com/asaidimu/hermes/pkg/nodes"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/runtime"
	"github.com/stretchr/testify/require"
)

// @see #review-20260826-005 in pkg/nodes/pipeline-ref/pipeleref.go
//
// Adds the tests/-level scenario that note asks for: a real sub-pipeline is
// compiled and registered, and a parent workflow that references it via
// pipeline-ref is run end to end, asserting the three runtime behaviors the
// compile-time tests (compiler_test.go: TestPipelineRef*) don't reach:
// fresh-state isolation from the parent, initialState interpolation, and
// resultKey merging back into parent state.

// mapPipelineRegistry is a trivial pipeline.PipelineRegistry backed by a map,
// used to hand a pre-compiled sub-pipeline to the parent compiler so its
// pipeline-ref node can resolve it by id.
type mapPipelineRegistry map[string]*pipeline.PipelineDefinition

func (m mapPipelineRegistry) Resolve(id string) (*pipeline.PipelineDefinition, bool) {
	d, ok := m[id]
	return d, ok
}

// compileSinglePipeline compiles a graph with exactly one top-level trigger
// and returns its PipelineDefinition (keyed by the trigger node's id, per
// compiler.Compile).
func compileSinglePipeline(t *testing.T, triggerID string, nodes []compiler.Node, edges []compiler.Edge, registry pipeline.PipelineRegistry) *pipeline.PipelineDefinition {
	t.Helper()
	wf, err := compiler.Compile(nodes, edges, registry)
	require.NoError(t, err)
	def, ok := wf.Pipelines[triggerID]
	require.True(t, ok, "pipeline for trigger %q not found", triggerID)
	return &def
}

func TestPipelineRefRuntimeSemantics(t *testing.T) {
	const subTriggerID = "sub-trigger-1"

	// --- Sub-pipeline: reads its OWN "value" from state (seeded via
	// pipeline-ref's initialState) and doubles it into "doubled". If the
	// child were not isolated from the parent, "value" would instead
	// resolve to the parent's much larger number.
	subNodes := []compiler.Node{
		{ID: subTriggerID, Type: compiler.NodeExecutable, Kind: "trigger", Config: map[string]any{"initialState": map[string]any{}}},
		{ID: "sub-double", Type: compiler.NodeExecutable, Kind: "arithmetic", Config: map[string]any{
			"operation": "multiply", "left": "value", "right": "2", "key": "doubled",
		}},
	}
	subEdges := []compiler.Edge{
		{ID: "se1", Source: subTriggerID, Target: "sub-double", Role: compiler.EdgeFlow},
	}
	subDef := compileSinglePipeline(t, subTriggerID, subNodes, subEdges, nil)
	registry := mapPipelineRegistry{subTriggerID: subDef}

	// --- Parent pipeline: seeds a value pipeline-ref must NOT see
	// ("parentOnly") and a "value" the sub-pipeline's own initialState
	// must override, then calls pipeline-ref with an unrelated initialState
	// and merges the child's full final state under "subResult".
	const parentTriggerID = "parent-trigger-1"
	parentNodes := []compiler.Node{
		{ID: parentTriggerID, Type: compiler.NodeExecutable, Kind: "trigger", Config: map[string]any{
			"initialState": map[string]any{"parentOnly": "should-not-leak-into-child", "value": float64(999)},
		}},
		{ID: "ref-1", Type: compiler.NodeExecutable, Kind: "pipeline-ref", Config: map[string]any{
			"pipelineId":   subTriggerID,
			"initialState": map[string]any{"value": float64(21)},
			"resultKey":    "subResult",
		}},
	}
	parentEdges := []compiler.Edge{
		{ID: "pe1", Source: parentTriggerID, Target: "ref-1", Role: compiler.EdgeFlow},
	}

	rt := runtime.NewWorkflowRuntime(runtime.Options{
		ActionLog: actionlog.NewMemoryActionLog(),
	})
	defer rt.Shutdown(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := rt.Run(ctx, parentNodes, parentEdges, runtime.RunOptions{Registry: registry})
	require.NoError(t, err)
	require.True(t, res.OK, "run failed: %v", res.Error)
	require.Equal(t, "succeeded", res.Status)

	// resultKey merge: the child's full final state lands under "subResult".
	subResult, ok := res.FinalState["subResult"].(map[string]any)
	require.True(t, ok, "expected subResult in parent final state, got %v", res.FinalState)

	// initialState interpolation: the child computed doubled = 21 * 2 = 42
	// from the initialState pipeline-ref gave it, not from the parent's
	// "value" (999).
	require.Equal(t, float64(42), subResult["doubled"], "child did not use pipeline-ref's initialState")
	require.Equal(t, float64(21), subResult["value"], "child's seeded value should be 21, not the parent's 999")

	// fresh-state isolation: the parent's unrelated key must not have
	// leaked into the child's state/result.
	_, leaked := subResult["parentOnly"]
	require.False(t, leaked, "parent-only state key leaked into isolated child pipeline")

	// And the reverse: the parent's own "value" must be untouched by the
	// child's run (still 999, not overwritten by the child's 21).
	require.Equal(t, float64(999), res.FinalState["value"], "parent state mutated by isolated child pipeline")
}
