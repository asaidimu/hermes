package replay

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/effect"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/stretchr/testify/require"
)

// buildForkishDef builds a root pipeline whose single pipelines-mode stage
// forks TWO child pipelines that clone the SAME step id ("shared-step") —
// exactly what the fork/distribute compilers produce. Each child's step is
// effectful and records a distinct state key so a collapsed index (pre-fix)
// silently drops one child's contribution.
func buildForkishDef() pipeline.PipelineDefinition {
	child := func(id string, key string, val any) pipeline.PipelineDefinition {
		return pipeline.PipelineDefinition{
			ID:    id,
			Label: id,
			Stages: []pipeline.Stage{
				{
					ID:    "shared-stage",
					Order: 1,
					Steps: map[string]pipeline.Step{
						"shared-step": {
							ID:     "shared-step",
							Effect: int(effect.SideEffecting),
							Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
								return store.SetValue(key, val), nil
							},
						},
					},
				},
			},
		}
	}
	return pipeline.PipelineDefinition{
		ID:    "root",
		Label: "Root",
		Stages: []pipeline.Stage{
			{
				ID:        "fork-stage",
				Order:     1,
				Pipelines: []pipeline.PipelineDefinition{child("child-a", "fromA", int64(1)), child("child-b", "fromB", int64(2))},
				PipelinesRouter: func(ctx context.Context, state map[string]any, results []pipeline.PipelineRunResult, st store.Store) (pipeline.RoutingInstruction, error) {
					return pipeline.Advance(), nil
				},
			},
			{
				ID:    "after",
				Order: 2,
				Steps: map[string]pipeline.Step{
					"after-step": {
						ID:     "after-step",
						Effect: int(effect.SideEffecting),
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("after", true), nil
						},
					},
				},
			},
		},
	}
}

// TestRebuildSubpipelineInstancesNotCollapsed covers the resolved
// #review-20260910-015 end-to-end against the real engine: a run with two
// concurrent subpipeline instances sharing the same step ids must rebuild
// with BOTH instances' recorded deltas (the pre-fix flat index kept only the
// first completion entry per step id, dropping the other child's delta), and
// the rebuilt state must match the live final state.
func TestRebuildSubpipelineInstancesNotCollapsed(t *testing.T) {
	ctx := context.Background()
	log := actionlog.NewMemoryActionLog()

	def := buildForkishDef()
	runID := "run-subpipelines"

	// Run the pipeline for real through the engine so the log is exactly
	// what production writes.
	factory := pipeline.NewFactory(def, nil, pipeline.FactoryOptions{
		ActionLog: log,
	})
	st := store.NewMemoryStore(nil)
	rc := factory.Prepare(runID, st, events.NewMemoryScopedBus())
	res, err := rc.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, "succeeded", res.Status)

	live, err := st.ExportJSON()
	require.NoError(t, err)
	require.Equal(t, int64(1), live["fromA"], "live state: child A contribution")
	require.Equal(t, int64(2), live["fromB"], "live state: child B contribution")
	require.Equal(t, true, live["after"], "live state: post-fork stage")

	// The rebuilt state travels through the recorded JSON deltas, so numeric
	// values are float64 there; normalize both sides for comparison.
	normalize := func(m map[string]any) map[string]any {
		b, err := json.Marshal(m)
		require.NoError(t, err)
		var out map[string]any
		require.NoError(t, json.Unmarshal(b, &out))
		return out
	}

	// Rebuild from the log alone.
	rp := NewReplayer(log, func(id string) (*pipeline.PipelineDefinition, bool) {
		if id == "root" {
			d := def
			return &d, true
		}
		return nil, false
	})
	rebuilt, addr, err := rp.Rebuild(ctx, runID, "root")
	require.NoError(t, err)
	require.Empty(t, addr.Stage, "completed run must rebuild to an empty resume address")

	rebuiltState, err := rebuilt.ExportJSON()
	require.NoError(t, err)
	rebuiltNorm := normalize(rebuiltState)
	require.Equal(t, float64(1), rebuiltNorm["fromA"],
		"child A's delta must survive replay (pre-fix: collapsed by the shared step id)")
	require.Equal(t, float64(2), rebuiltNorm["fromB"],
		"child B's delta must survive replay (pre-fix: collapsed by the shared step id)")
	require.Equal(t, true, rebuiltNorm["after"], "post-fork stage delta")
	require.Equal(t, normalize(live), rebuiltNorm, "rebuilt state must equal the live final state")
}

// TestRebuildDynamicSubpipelines covers dynamic children (distribute-style):
// the pipelines-mode stage regenerates its child definitions from the
// reconstructed state via the same DynamicPipelines closure the runtime
// uses, and each item instance's recorded delta is replayed per instance.
func TestRebuildDynamicSubpipelines(t *testing.T) {
	ctx := context.Background()
	log := actionlog.NewMemoryActionLog()

	items := []any{"x", "y"}
	dynStage := pipeline.Stage{
		ID:    "dyn-stage",
		Order: 1,
		DynamicPipelines: func(state map[string]any) []pipeline.PipelineDefinition {
			raw, _ := state["items"].([]any)
			defs := make([]pipeline.PipelineDefinition, 0, len(raw))
			for i := range raw {
				idx := i
				defs = append(defs, pipeline.PipelineDefinition{
					ID: "dyn__" + jsonNumber(idx),
					Stages: []pipeline.Stage{
						{
							ID:    "item-stage",
							Order: 1,
							Steps: map[string]pipeline.Step{
								"item-step": {
									ID:     "item-step",
									Effect: int(effect.SideEffecting),
									Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
										return store.SetValue("got_"+jsonNumber(idx), raw[idx]), nil
									},
								},
							},
						},
					},
				})
			}
			return defs
		},
	}
	// Seed items through a recorded stage the way real distribute workflows
	// do (the setup stage's step records the items in state), so the
	// DynamicPipelines closure can regenerate children from the REBUILT state.
	seedStage := pipeline.Stage{
		ID:    "seed-stage",
		Order: 0,
		Steps: map[string]pipeline.Step{
			"seed-step": {
				ID:     "seed-step",
				Effect: int(effect.SideEffecting),
				Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
					return store.SetValue("items", items), nil
				},
			},
		},
	}
	def := pipeline.PipelineDefinition{
		ID:     "root-dyn",
		Stages: []pipeline.Stage{seedStage, dynStage},
	}
	runID := "run-dyn"

	factory := pipeline.NewFactory(def, nil, pipeline.FactoryOptions{ActionLog: log})
	st := store.NewMemoryStore(nil)
	rc := factory.Prepare(runID, st, events.NewMemoryScopedBus())
	res, err := rc.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, "succeeded", res.Status)

	live, _ := st.ExportJSON()
	require.Equal(t, "x", live["got_0"])
	require.Equal(t, "y", live["got_1"])
	_ = live

	rp := NewReplayer(log, func(id string) (*pipeline.PipelineDefinition, bool) {
		if id == "root-dyn" {
			d := def
			return &d, true
		}
		return nil, false
	})
	rebuilt, _, err := rp.Rebuild(ctx, runID, "root-dyn")
	require.NoError(t, err)
	rebuiltState, _ := rebuilt.ExportJSON()
	require.Equal(t, "x", rebuiltState["got_0"], "instance 0 delta (pre-fix: collapsed)")
	require.Equal(t, "y", rebuiltState["got_1"], "instance 1 delta (pre-fix: collapsed)")
	require.Len(t, rebuiltState, 3, "items seed + exactly the two instance deltas, no cross-instance bleed")
}

// TestRebuildChildPauseBubblesNestedAddress pins the nested resume address:
// when the log ends with a child pipeline paused inside a pipelines-mode
// stage, Rebuild must return the parent stage with a Pipeline component
// pointing at the child's resume stage — mirroring the runtime's nested
// checkpoint shape.
func TestRebuildChildPauseBubblesNestedAddress(t *testing.T) {
	ctx := context.Background()
	log := actionlog.NewMemoryActionLog()

	childDef := pipeline.PipelineDefinition{
		ID: "child-pause",
		Stages: []pipeline.Stage{
			{
				ID:    "child-stage-1",
				Order: 1,
				Steps: map[string]pipeline.Step{
					"child-step": {
						ID:     "child-step",
						Effect: int(effect.SideEffecting),
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("beforePause", true), nil
						},
					},
				},
				Router: func(ctx context.Context, state map[string]any, st store.Store) (pipeline.RoutingInstruction, error) {
					return pipeline.Pause("child-resume", 0), nil
				},
			},
			{ID: "child-resume", Order: 2},
		},
	}
	def := pipeline.PipelineDefinition{
		ID: "root-pause",
		Stages: []pipeline.Stage{
			{
				ID:        "wrapper-stage",
				Order:     1,
				Pipelines: []pipeline.PipelineDefinition{childDef},
			},
		},
	}
	runID := "run-child-pause"

	factory := pipeline.NewFactory(def, nil, pipeline.FactoryOptions{ActionLog: log})
	st := store.NewMemoryStore(nil)
	rc := factory.Prepare(runID, st, events.NewMemoryScopedBus())
	res, err := rc.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, "paused", res.Status, "the child's pause must bubble to the root")

	rp := NewReplayer(log, func(id string) (*pipeline.PipelineDefinition, bool) {
		if id == "root-pause" {
			d := def
			return &d, true
		}
		return nil, false
	})
	_, addr, err := rp.Rebuild(ctx, runID, "root-pause")
	require.NoError(t, err)
	require.Equal(t, "wrapper-stage", addr.Stage, "resume address points at the pipelines-mode stage")
	require.NotNil(t, addr.Pipeline, "resume address must be nested in the subpipeline")
	require.Equal(t, 0, addr.Pipeline.Index, "child index 0")
	require.Equal(t, "child-resume", addr.Pipeline.Stage, "child resume stage from the child's own checkpoint")
}

func jsonNumber(i int) string {
	b, _ := json.Marshal(i)
	return string(b)
}
