package replay

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/stretchr/testify/require"
)

// TestRebuild_PureStepsOnly verifies that a pipeline with only pure steps
// reconstructs state entirely from re-execution, with no action log entries.
func TestRebuild_PureStepsOnly(t *testing.T) {
	ctx := context.Background()
	log := actionlog.NewMemoryActionLog()

	def := &pipeline.PipelineDefinition{
		ID:    "pure-pipeline",
		Label: "Pure Pipeline",
		Stages: []pipeline.Stage{
			{
				ID:    "stage-1",
				Order: 1,
				Steps: map[string]pipeline.Step{
					"step-1": {
						ID:     "step-1",
						Effect: 1, // Pure
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("x", int64(10)), nil
						},
					},
				},
			},
			{
				ID:    "stage-2",
				Order: 2,
				Steps: map[string]pipeline.Step{
					"step-2": {
						ID:     "step-2",
						Effect: 1, // Pure
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							x := state["x"].(int64)
							return store.SetValue("y", x*2), nil
						},
					},
				},
			},
		},
	}

	// Simulate: run the pipeline, get entries in the log (none for pure steps)
	runID := "run-pure"
	rp := NewReplayer(log, func(id string) (*pipeline.PipelineDefinition, bool) {
		if id == runID {
			return def, true
		}
		return nil, false
	})

	// Rebuild with no entries — pure steps re-execute
	st, addr, err := rp.Rebuild(ctx, runID)
	require.NoError(t, err)
	require.Empty(t, addr.Stage, "should return empty address (run completed)")

	val, err := st.ExportJSON()
	require.NoError(t, err)
	require.Equal(t, int64(10), val["x"])
	require.Equal(t, int64(20), val["y"])
}

// TestRebuild_EffectfulStepsCompleted verifies that effectful steps with
// recorded state deltas are injected (not re-executed).
func TestRebuild_EffectfulStepsCompleted(t *testing.T) {
	ctx := context.Background()
	log := actionlog.NewMemoryActionLog()

	runID := "run-effectful"

	// Record state deltas for two effectful steps
	delta1, _ := json.Marshal(map[string]any{"api_result": "ok"})
	log.Append(ctx, actionlog.Entry{
		RunID:   runID,
		Kind:    actionlog.KindEffectCompleted,
		StageID: "stage-1",
		StepID:  "http-step",
		Output:  delta1,
	})
	delta2, _ := json.Marshal(map[string]any{"db_result": int64(42)})
	log.Append(ctx, actionlog.Entry{
		RunID:   runID,
		Kind:    actionlog.KindEffectCompleted,
		StageID: "stage-2",
		StepID:  "db-step",
		Output:  delta2,
	})

	def := &pipeline.PipelineDefinition{
		ID:    "effectful-pipeline",
		Stages: []pipeline.Stage{
			{
				ID:    "stage-1",
				Order: 1,
				Steps: map[string]pipeline.Step{
					"http-step": {
						ID:     "http-step",
						Effect: 2, // SideEffecting
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							// This should NOT be called during replay
							t.Fatal("effectful step should not be re-executed during replay")
							return nil, nil
						},
					},
				},
			},
			{
				ID:    "stage-2",
				Order: 2,
				Steps: map[string]pipeline.Step{
					"db-step": {
						ID:     "db-step",
						Effect: 2, // SideEffecting
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							t.Fatal("effectful step should not be re-executed during replay")
							return nil, nil
						},
					},
				},
			},
		},
	}

	rp := NewReplayer(log, func(id string) (*pipeline.PipelineDefinition, bool) {
		if id == runID {
			return def, true
		}
		return nil, false
	})

	st, addr, err := rp.Rebuild(ctx, runID)
	require.NoError(t, err)
	require.Empty(t, addr.Stage, "run should have completed")

	val, err := st.ExportJSON()
	require.NoError(t, err)
	require.Equal(t, "ok", val["api_result"])
	// JSON unmarshals numbers as float64
	require.InDelta(t, float64(42), val["db_result"], 0.001)
}

// TestRebuild_PartiallyCompletedEffectful verifies that when an effectful
// step has no recorded entry, Rebuild returns the correct resume address.
func TestRebuild_PartiallyCompletedEffectful(t *testing.T) {
	ctx := context.Background()
	log := actionlog.NewMemoryActionLog()

	runID := "run-partial"

	// Only the first effectful step completed
	delta1, _ := json.Marshal(map[string]any{"api_result": "ok"})
	log.Append(ctx, actionlog.Entry{
		RunID:   runID,
		Kind:    actionlog.KindEffectCompleted,
		StageID: "stage-1",
		StepID:  "http-step-1",
		Output:  delta1,
	})
	// http-step-2 has NO entry — this is where resume should happen

	def := &pipeline.PipelineDefinition{
		ID:    "partial-pipeline",
		Stages: []pipeline.Stage{
			{
				ID:    "stage-1",
				Order: 1,
				Steps: map[string]pipeline.Step{
					"http-step-1": {
						ID:     "http-step-1",
						Effect: 2,
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							t.Fatal("should not be re-executed")
							return nil, nil
						},
					},
				},
			},
			{
				ID:    "stage-2",
				Order: 2,
				Steps: map[string]pipeline.Step{
					"http-step-2": {
						ID:     "http-step-2",
						Effect: 2,
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("result", "done"), nil
						},
					},
				},
			},
		},
	}

	rp := NewReplayer(log, func(id string) (*pipeline.PipelineDefinition, bool) {
		if id == runID {
			return def, true
		}
		return nil, false
	})

	st, addr, err := rp.Rebuild(ctx, runID)
	require.NoError(t, err)
	require.Equal(t, "stage-2", addr.Stage)
	require.Equal(t, "http-step-2", addr.Step)

	val, err := st.ExportJSON()
	require.NoError(t, err)
	require.Equal(t, "ok", val["api_result"])
	// http-step-2 was NOT executed (it's the resume point)
	_, hasResult := val["result"]
	require.False(t, hasResult)
}

// TestRebuild_PureBeforeEffectful verifies that pure steps before an
// unrecorded effectful step are re-executed.
func TestRebuild_PureBeforeEffectful(t *testing.T) {
	ctx := context.Background()
	log := actionlog.NewMemoryActionLog()

	runID := "run-mixed"

	def := &pipeline.PipelineDefinition{
		ID:    "mixed-pipeline",
		Stages: []pipeline.Stage{
			{
				ID:    "stage-1",
				Order: 1,
				Steps: map[string]pipeline.Step{
					"pure-step": {
						ID:     "pure-step",
						Effect: 1,
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("computed", int64(99)), nil
						},
					},
				},
			},
			{
				ID:    "stage-2",
				Order: 2,
				Steps: map[string]pipeline.Step{
					"effectful-step": {
						ID:     "effectful-step",
						Effect: 2,
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("side_effect", true), nil
						},
					},
				},
			},
		},
	}

	// No entries in the log — effectful step hasn't run yet
	rp := NewReplayer(log, func(id string) (*pipeline.PipelineDefinition, bool) {
		if id == runID {
			return def, true
		}
		return nil, false
	})

	st, addr, err := rp.Rebuild(ctx, runID)
	require.NoError(t, err)
	require.Equal(t, "stage-2", addr.Stage)
	require.Equal(t, "effectful-step", addr.Step)

	// Pure step was re-executed
	val, err := st.ExportJSON()
	require.NoError(t, err)
	require.Equal(t, int64(99), val["computed"])
}

// TestRebuild_RoutingDecisionRecorded verifies that recorded routing
// decisions are followed (not re-evaluated).
func TestRebuild_RoutingDecisionRecorded(t *testing.T) {
	ctx := context.Background()
	log := actionlog.NewMemoryActionLog()

	runID := "run-routing"

	// Record a routing decision that jumped from "start" to "finish"
	log.Append(ctx, actionlog.Entry{
		RunID:   runID,
		Kind:    actionlog.KindRouted,
		StageID: "start",
		Handle:  "finish",
	})

	def := &pipeline.PipelineDefinition{
		ID:    "routing-pipeline",
		Stages: []pipeline.Stage{
			{
				ID:    "start",
				Order: 1,
				Steps: map[string]pipeline.Step{
					"init": {
						ID:     "init",
						Effect: 1,
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("started", true), nil
						},
					},
				},
				Router: func(ctx context.Context, state map[string]any, _ store.Store) (pipeline.RoutingInstruction, error) {
					// This should NOT be called during replay — the
					// recorded routing decision is used instead.
					t.Fatal("router should not be re-evaluated during replay")
					return nil, nil
				},
			},
			{
				ID:    "middle",
				Order: 2,
				Steps: map[string]pipeline.Step{
					"skipped": {
						ID:     "skipped",
						Effect: 1,
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("should_not_exist", true), nil
						},
					},
				},
			},
			{
				ID:    "finish",
				Order: 3,
				Steps: map[string]pipeline.Step{
					"done": {
						ID:     "done",
						Effect: 1,
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("finished", true), nil
						},
					},
				},
			},
		},
	}

	rp := NewReplayer(log, func(id string) (*pipeline.PipelineDefinition, bool) {
		if id == runID {
			return def, true
		}
		return nil, false
	})

	// Routing entry says "finish" — Replayer jumps to "finish" stage,
	// skipping "middle". "finish" has a pure step that re-executes,
	// no routing entry, so the run completes.
	st, addr, err := rp.Rebuild(ctx, runID)
	require.NoError(t, err)
	require.Empty(t, addr.Stage, "run should have completed (jumped to finish)")

	val, err := st.ExportJSON()
	require.NoError(t, err)
	require.Equal(t, true, val["started"])
	_, skipped := val["should_not_exist"]
	require.False(t, skipped, "skipped stage should not have been executed")
	_, finished := val["finished"]
	require.True(t, finished, "finish stage should have been executed")
}

// TestRebuild_DefinitionNotFound verifies error when definition is missing.
func TestRebuild_DefinitionNotFound(t *testing.T) {
	log := actionlog.NewMemoryActionLog()
	rp := NewReplayer(log, func(id string) (*pipeline.PipelineDefinition, bool) {
		return nil, false
	})

	_, _, err := rp.Rebuild(context.Background(), "nonexistent")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no pipeline definition")
}

// TestRebuild_EffectfulStepFailure verifies that a failed effectful step
// (KindEffectFailed) still produces a resume address.
func TestRebuild_EffectfulStepFailure(t *testing.T) {
	ctx := context.Background()
	log := actionlog.NewMemoryActionLog()

	runID := "run-failed"
	log.Append(ctx, actionlog.Entry{
		RunID:   runID,
		Kind:    actionlog.KindEffectFailed,
		StageID: "stage-1",
		StepID:  "failing-step",
		Error:   "connection refused",
	})

	def := &pipeline.PipelineDefinition{
		ID:    "failed-pipeline",
		Stages: []pipeline.Stage{
			{
				ID:    "stage-1",
				Order: 1,
				Steps: map[string]pipeline.Step{
					"failing-step": {
						ID:     "failing-step",
						Effect: 2,
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return nil, nil
						},
					},
				},
			},
			{
				ID:    "stage-2",
				Order: 2,
				Steps: map[string]pipeline.Step{
					"next-step": {
						ID:     "next-step",
						Effect: 2,
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("next", true), nil
						},
					},
				},
			},
		},
	}

	rp := NewReplayer(log, func(id string) (*pipeline.PipelineDefinition, bool) {
		if id == runID {
			return def, true
		}
		return nil, false
	})

	// The failing step IS recorded (KindEffectFailed), so Rebuild
	// treats it as "completed" and moves to the next stage.
	st, addr, err := rp.Rebuild(ctx, runID)
	require.NoError(t, err)
	require.Equal(t, "stage-2", addr.Stage)
	require.Equal(t, "next-step", addr.Step)

	val, err := st.ExportJSON()
	require.NoError(t, err)
	_ = val // stage-2 not executed yet
}

// TestStateAt verifies that StateAt reconstructs state at a specific point.
func TestStateAt(t *testing.T) {
	ctx := context.Background()
	log := actionlog.NewMemoryActionLog()

	runID := "run-stateat"

	def := &pipeline.PipelineDefinition{
		ID:    "stateat-pipeline",
		Stages: []pipeline.Stage{
			{
				ID:    "stage-1",
				Order: 1,
				Steps: map[string]pipeline.Step{
					"step-a": {
						ID:     "step-a",
						Effect: 1,
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("a", int64(1)), nil
						},
					},
					"step-b": {
						ID:     "step-b",
						Effect: 1,
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("b", int64(2)), nil
						},
					},
				},
			},
			{
				ID:    "stage-2",
				Order: 2,
				Steps: map[string]pipeline.Step{
					"step-c": {
						ID:     "step-c",
						Effect: 1,
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("c", int64(3)), nil
						},
					},
				},
			},
		},
	}

	rp := NewReplayer(log, func(id string) (*pipeline.PipelineDefinition, bool) {
		if id == runID {
			return def, true
		}
		return nil, false
	})

	// State at "after step-a" — only a should exist
	state, err := rp.StateAt(ctx, runID, StepAddress{Stage: "stage-1", Step: "step-b"})
	require.NoError(t, err)
	require.Equal(t, int64(1), state["a"])
	_, hasB := state["b"]
	require.False(t, hasB)

	// State at "after stage-1" — a and b should exist
	state, err = rp.StateAt(ctx, runID, StepAddress{Stage: "stage-2"})
	require.NoError(t, err)
	require.Equal(t, int64(1), state["a"])
	require.Equal(t, int64(2), state["b"])
	_, hasC := state["c"]
	require.False(t, hasC, "c should not exist at end of stage-1")
}
