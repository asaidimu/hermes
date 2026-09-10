package tests

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/stretchr/testify/require"
)

// TestActionLogRecordsEffectfulSteps verifies that effectful steps produce
// KindEffectCompleted entries in the action log, while pure steps do not.
func TestActionLogRecordsEffectfulSteps(t *testing.T) {
	ctx := context.Background()
	log := actionlog.NewMemoryActionLog()

	def := pipeline.PipelineDefinition{
		ID:    "actionlog-test",
		Label: "Action Log Test",
		Stages: []pipeline.Stage{
			{
				ID:    "stage-1",
				Order: 1,
				Label: "Stage 1",
				Steps: map[string]pipeline.Step{
					"pure-step": {
						ID:     "pure-step",
						Label:  "Pure Step",
						Effect: 1, // EffectPure
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("pure_result", "computed"), nil
						},
					},
					"effectful-step": {
						ID:     "effectful-step",
						Label:  "Effectful Step",
						Effect: 2, // EffectSideEffecting
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("effect_result", "http_response"), nil
						},
					},
				},
			},
			{
				ID:    "stage-2",
				Order: 2,
				Label: "Stage 2",
				Steps: map[string]pipeline.Step{
					"another-effectful": {
						ID:     "another-effectful",
						Label:  "Another Effectful",
						Effect: 2, // EffectSideEffecting
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("db_result", "inserted"), nil
						},
					},
				},
			},
		},
	}

	factory := pipeline.NewFactory(def, nil, pipeline.FactoryOptions{
		ActionLog: log,
	})
	runCtx := factory.Prepare("run-actionlog", nil)

	res, err := runCtx.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, "succeeded", res.Status)

	// Verify the action log has entries for effectful steps only
	entries, err := log.All(ctx, "run-actionlog", 0)
	require.NoError(t, err)

	// Should have: 2 KindEffectCompleted (effectful-step, another-effectful)
	// + 2 KindRouted (one per stage router decision)
	effectEntries := 0
	for _, e := range entries {
		switch e.Kind {
		case actionlog.KindEffectCompleted:
			effectEntries++
			require.NotEmpty(t, e.StepID, "effect entry must have StepID")
			require.NotEmpty(t, e.StageID, "effect entry must have StageID")
		}
	}

	require.Equal(t, 2, effectEntries, "expected 2 effect_completed entries")
}

// TestActionLogRecordsRoutingDecisions verifies that routing decisions are
// recorded in the action log regardless of router purity.
func TestActionLogRecordsRoutingDecisions(t *testing.T) {
	ctx := context.Background()
	log := actionlog.NewMemoryActionLog()

	def := pipeline.PipelineDefinition{
		ID:    "routing-test",
		Label: "Routing Test",
		Stages: []pipeline.Stage{
			{
				ID:    "start",
				Order: 1,
				Steps: map[string]pipeline.Step{
					"init": {
						ID:     "init",
						Effect: 1, // Pure
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("route", "jump"), nil
						},
					},
				},
				Router: func(ctx context.Context, state map[string]any, _ store.Store) (pipeline.RoutingInstruction, error) {
					return pipeline.Jump("finish"), nil
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

	factory := pipeline.NewFactory(def, nil, pipeline.FactoryOptions{
		ActionLog: log,
	})
	runCtx := factory.Prepare("run-routing", nil)

	res, err := runCtx.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, "succeeded", res.Status)

	entries, err := log.All(ctx, "run-routing", 0)
	require.NoError(t, err)

	// Find the routing entry for the "start" stage
	var startRoute *actionlog.Entry
	for i := range entries {
		if entries[i].Kind == actionlog.KindRouted && entries[i].StageID == "start" {
			startRoute = &entries[i]
			break
		}
	}

	require.NotNil(t, startRoute, "expected a routing entry for stage 'start'")
	require.Equal(t, "finish", startRoute.Handle, "routing entry handle should be the jump target")
}

// TestActionLogRecordsEffectfulStepFailure verifies that failed effectful
// steps produce KindEffectFailed entries.
func TestActionLogRecordsEffectfulStepFailure(t *testing.T) {
	ctx := context.Background()
	log := actionlog.NewMemoryActionLog()

	def := pipeline.PipelineDefinition{
		ID:    "fail-test",
		Label: "Failure Test",
		Stages: []pipeline.Stage{
			{
				ID:    "stage-1",
				Order: 1,
				Steps: map[string]pipeline.Step{
					"failing-step": {
						ID:      "failing-step",
						Label:   "Failing Step",
						Effect:  2, // EffectSideEffecting
						Retries: 0, // No retries
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return nil, core.NewSystemError(core.ErrCodeExecutionFailed, "simulated failure")
						},
					},
				},
			},
		},
	}

	factory := pipeline.NewFactory(def, nil, pipeline.FactoryOptions{
		ActionLog: log,
	})
	runCtx := factory.Prepare("run-fail", nil)

	_, err := runCtx.Run(ctx)
	require.Error(t, err)

	entries, err := log.All(ctx, "run-fail", 0)
	require.NoError(t, err)

	// Should have a KindEffectFailed entry
	var failEntry *actionlog.Entry
	for i := range entries {
		if entries[i].Kind == actionlog.KindEffectFailed {
			failEntry = &entries[i]
			break
		}
	}

	require.NotNil(t, failEntry, "expected an effect_failed entry")
	require.Equal(t, "failing-step", failEntry.StepID)
	require.NotEmpty(t, failEntry.Error, "failure entry should have error message")
}

// TestActionLogNoEntriesForPureSteps verifies that pure steps produce no
// action log entries.
func TestActionLogNoEntriesForPureSteps(t *testing.T) {
	ctx := context.Background()
	log := actionlog.NewMemoryActionLog()

	def := pipeline.PipelineDefinition{
		ID:    "pure-only-test",
		Label: "Pure Only Test",
		Stages: []pipeline.Stage{
			{
				ID:    "stage-1",
				Order: 1,
				Steps: map[string]pipeline.Step{
					"pure-1": {
						ID:     "pure-1",
						Effect: 1, // Pure
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("x", 42), nil
						},
					},
					"pure-2": {
						ID:     "pure-2",
						Effect: 1, // Pure
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("y", "hello"), nil
						},
					},
				},
			},
		},
	}

	factory := pipeline.NewFactory(def, nil, pipeline.FactoryOptions{
		ActionLog: log,
	})
	runCtx := factory.Prepare("run-pure", nil)

	res, err := runCtx.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, "succeeded", res.Status)

	entries, err := log.All(ctx, "run-pure", 0)
	require.NoError(t, err)

	// Pure steps should produce NO effect entries.
	// Only routing entries are expected (and even those may be absent
	// if the default router is used — the default advance router
	// produces no routing entry since routingHandle returns "" for AdvanceInstruction).
	for _, e := range entries {
		require.NotEqual(t, actionlog.KindEffectCompleted, e.Kind,
			"pure step should not produce effect_completed entry")
		require.NotEqual(t, actionlog.KindEffectFailed, e.Kind,
			"pure step should not produce effect_failed entry")
	}
}

// TestActionLogEntriesAreMonotonicallySequenced verifies that entries are
// assigned monotonically increasing sequence numbers.
func TestActionLogEntriesAreMonotonicallySequenced(t *testing.T) {
	ctx := context.Background()
	log := actionlog.NewMemoryActionLog()

	def := pipeline.PipelineDefinition{
		ID:    "seq-test",
		Label: "Sequence Test",
		Stages: []pipeline.Stage{
			{
				ID:    "stage-1",
				Order: 1,
				Steps: map[string]pipeline.Step{
					"effect-1": {
						ID:     "effect-1",
						Effect: 2,
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("a", 1), nil
						},
					},
				},
			},
			{
				ID:    "stage-2",
				Order: 2,
				Steps: map[string]pipeline.Step{
					"effect-2": {
						ID:     "effect-2",
						Effect: 2,
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("b", 2), nil
						},
					},
				},
			},
		},
	}

	factory := pipeline.NewFactory(def, nil, pipeline.FactoryOptions{
		ActionLog: log,
	})
	runCtx := factory.Prepare("run-seq", nil)

	_, err := runCtx.Run(ctx)
	require.NoError(t, err)

	entries, err := log.All(ctx, "run-seq", 0)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(entries), 2, "expected at least 2 entries")

	// Verify monotonically increasing Seq
	for i := 1; i < len(entries); i++ {
		require.Greater(t, entries[i].Seq, entries[i-1].Seq,
			"entry Seq should be monotonically increasing")
	}
}

// TestActionLogWithNopLog verifies that pipeline execution works fine with
// a nil/NopLog action log (no entries recorded, no errors).
func TestActionLogWithNopLog(t *testing.T) {
	ctx := context.Background()

	def := pipeline.PipelineDefinition{
		ID:    "noplog-test",
		Label: "NopLog Test",
		Stages: []pipeline.Stage{
			{
				ID:    "stage-1",
				Order: 1,
				Steps: map[string]pipeline.Step{
					"step-1": {
						ID:     "step-1",
						Effect: 2,
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("x", 1), nil
						},
					},
				},
			},
		},
	}

	// No ActionLog configured — should use NopLog
	factory := pipeline.NewFactory(def, nil)
	runCtx := factory.Prepare("run-noplog", nil)

	res, err := runCtx.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, "succeeded", res.Status)
}

// TestActionLogEntriesHaveTimestamps verifies that all entries have timestamps set.
func TestActionLogEntriesHaveTimestamps(t *testing.T) {
	ctx := context.Background()
	log := actionlog.NewMemoryActionLog()

	def := pipeline.PipelineDefinition{
		ID:    "timestamp-test",
		Label: "Timestamp Test",
		Stages: []pipeline.Stage{
			{
				ID:    "stage-1",
				Order: 1,
				Steps: map[string]pipeline.Step{
					"effectful": {
						ID:     "effectful",
						Effect: 2,
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("done", true), nil
						},
					},
				},
				Router: func(ctx context.Context, state map[string]any, _ store.Store) (pipeline.RoutingInstruction, error) {
					return pipeline.Jump("end"), nil
				},
			},
			{
				ID:    "end",
				Order: 2,
				Steps: map[string]pipeline.Step{},
			},
		},
	}

	factory := pipeline.NewFactory(def, nil, pipeline.FactoryOptions{
		ActionLog: log,
	})
	runCtx := factory.Prepare("run-ts", nil)

	_, err := runCtx.Run(ctx)
	require.NoError(t, err)

	entries, err := log.All(ctx, "run-ts", 0)
	require.NoError(t, err)

	for _, e := range entries {
		require.False(t, e.Timestamp.IsZero(), "entry %s should have a timestamp", e.Kind)
	}

	// Verify output is valid JSON for effect entries
	for _, e := range entries {
		if e.Kind == actionlog.KindEffectCompleted && e.Output != nil {
			var raw json.RawMessage
			require.NoError(t, json.Unmarshal(e.Output, &raw), "Output should be valid JSON")
		}
	}
}
