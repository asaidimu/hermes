package tests

// S.3 acceptance (plan Phase 5, architecture audit §A flagship fix):
// restart durability from the action log. Runtime A runs a workflow that
// pauses mid-graph; runtime B — a FRESH runtime on the SAME log — must
// recover the run: meta derived from the log, the pause re-armed at
// Register, the state rebuilt lazily by the Replayer on Resume, and the
// run driven to the same terminal state as an uninterrupted execution.

import (
	"context"
	"testing"
	"time"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/runtime"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/stretchr/testify/require"
)

// restartWorkflow: set x=1 (effectful — the delta is what the log records
// and the Replayer folds) → pause for event "go" → set y=2 (effectful).
// Pipelines is keyed by the trigger id with a distinct pipeline ID, so the
// restart path exercises the hand-built trigger binding (bindTriggerID)
// and the #022 resume lookup fallback the same way production layouts do.
func restartWorkflow(id string) *pipeline.Workflow {
	def := pipeline.PipelineDefinition{
		ID:    id + "-pipeline",
		Label: id,
		Stages: []pipeline.Stage{
			{
				ID: "stage:a", Label: "Set X",
				Steps: map[string]pipeline.Step{
					"step:a": {ID: "step:a", Effect: int(pipeline.EffectSideEffecting), Action: func(ctx context.Context, pcxt *pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
						return store.SetValue("x", float64(1)), nil
					}},
				},
			},
			{
				ID: "stage:wait", Label: "Wait",
				Router: func(ctx context.Context, state map[string]any, st store.Store) (pipeline.RoutingInstruction, error) {
					return pipeline.PauseForEvent("go", 0), nil
				},
			},
			{
				ID: "stage:b", Label: "Set Y",
				Steps: map[string]pipeline.Step{
					"step:b": {ID: "step:b", Effect: int(pipeline.EffectSideEffecting), Action: func(ctx context.Context, pcxt *pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
						return store.SetValue("y", float64(2)), nil
					}},
				},
			},
		},
	}
	return &pipeline.Workflow{
		ID:    id,
		Label: id,
		Pipelines: map[string]pipeline.PipelineDefinition{
			"trigger:manual:Run": def,
		},
		Triggers: map[string]pipeline.WorkflowTrigger{
			"trigger:manual:Run": {ID: "trigger:manual:Run", Event: runtime.ManualEvent},
		},
	}
}

// terminalWorkflow: set x=1 → set y=2, no pause — runs straight through.
func terminalWorkflow(id string) *pipeline.Workflow {
	def := pipeline.PipelineDefinition{
		ID:    id + "-pipeline",
		Label: id,
		Stages: []pipeline.Stage{
			{
				ID: "stage:a", Label: "Set X",
				Steps: map[string]pipeline.Step{
					"step:a": {ID: "step:a", Effect: int(pipeline.EffectSideEffecting), Action: func(ctx context.Context, pcxt *pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
						return store.SetValue("x", float64(1)), nil
					}},
				},
			},
			{
				ID: "stage:b", Label: "Set Y",
				Steps: map[string]pipeline.Step{
					"step:b": {ID: "step:b", Effect: int(pipeline.EffectSideEffecting), Action: func(ctx context.Context, pcxt *pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
						return store.SetValue("y", float64(2)), nil
					}},
				},
			},
		},
	}
	return &pipeline.Workflow{
		ID:    id,
		Label: id,
		Pipelines: map[string]pipeline.PipelineDefinition{
			"trigger:manual:Run": def,
		},
		Triggers: map[string]pipeline.WorkflowTrigger{
			"trigger:manual:Run": {ID: "trigger:manual:Run", Event: runtime.ManualEvent},
		},
	}
}

func awaitMetaStatusRestart(t *testing.T, rt *runtime.WorkflowRuntime, runID, want string) {
	t.Helper()
	require.Eventually(t, func() bool {
		meta, err := rt.GetRunMeta(context.Background(), runID)
		if err != nil {
			return false
		}
		return string(meta.Status) == want
	}, 5*time.Second, 10*time.Millisecond, "run %s never reached status %s", runID, want)
}

func awaitRestartResult(t *testing.T, done chan runtime.RunResult) runtime.RunResult {
	t.Helper()
	select {
	case res := <-done:
		return res
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for run completion")
		return runtime.RunResult{}
	}
}

// TestRestartDurabilityPausedRunResumesToTerminalState is the plan's
// acceptance test: pause mid-graph in runtime A, "restart" into runtime B
// on the same log, resume in B, and require the same terminal state an
// uninterrupted run reaches.
func TestRestartDurabilityPausedRunResumesToTerminalState(t *testing.T) {
	ctx := context.Background()
	log := actionlog.NewMemoryActionLog()
	wf := restartWorkflow("wf-restart-pause")

	// --- runtime A: run until paused, then "die" ---
	preparedA := make(chan string, 4)
	rtA := runtime.NewWorkflowRuntime(runtime.Options{
		ActionLog:   log,
		EventSource: runtime.NewManualEventSource(),
	})
	require.NoError(t, rtA.Register(wf, runtime.RegisterOptions{
		Mode:      runtime.Mode{Type: "transient"},
		OnPrepare: func(h *runtime.RunHandle) error { preparedA <- h.RunID; return nil },
	}))
	rtA.Bus().Emit(ctx, runtime.ManualEvent, events.PipelineEvent{Payload: map[string]any{}})
	runID := <-preparedA
	awaitMetaStatusRestart(t, rtA, runID, "paused")
	rtA.Shutdown(ctx)

	// --- runtime B: fresh runtime, SAME log ---
	// Recover must have classified the run from the log alone: meta paused
	// (the checkpoint rides in the KindCheckpoint entry's Output), staged
	// for re-arm.
	doneB := make(chan runtime.RunResult, 4)
	rtB := runtime.NewWorkflowRuntime(runtime.Options{
		ActionLog:   log,
		EventSource: runtime.NewManualEventSource(),
	})
	defer rtB.Shutdown(ctx)

	meta, err := rtB.GetRunMeta(ctx, runID)
	require.NoError(t, err, "recovered run must be queryable immediately after construction")
	require.Equal(t, "paused", string(meta.Status), "Recover must classify the run from the log's checkpoint entry")
	require.Equal(t, "wf-restart-pause-pipeline", meta.PipelineID)

	// Registering the owning workflow re-arms the pause (bindPendingPauses):
	// paused registry + watch registration + (no cron here). The resume
	// event is delivered through the re-armed watch.
	require.NoError(t, rtB.Register(wf, runtime.RegisterOptions{
		Mode:       runtime.Mode{Type: "transient"},
		OnComplete: func(r runtime.RunResult) { doneB <- r },
	}))

	rtB.Bus().Emit(ctx, "go", events.PipelineEvent{Payload: map[string]any{}})
	res := awaitRestartResult(t, doneB)
	require.True(t, res.OK, "resumed run failed: %v", res.Error)
	require.Equal(t, "succeeded", res.Status)

	// Terminal state equals the uninterrupted run's domain keys: x came
	// from the REBUILT store (the Replayer folded attempt 0's logged
	// delta), y from the resumed segment.
	require.Equal(t, float64(1), res.FinalState["x"], "pre-pause state must be rebuilt from the log")
	require.Equal(t, float64(2), res.FinalState["y"])

	// Rerun-index continuation: the resumed segment is attempt 1, one past
	// the recovered attempt 0 (derived from the log — S.2).
	idx, err := log.LatestRerunIndex(ctx, runID)
	require.NoError(t, err)
	require.Equal(t, 1, idx, "the resumed segment must continue at the next rerun index")

	// The runtime's meta cache now shows the terminal fact.
	meta, err = rtB.GetRunMeta(ctx, runID)
	require.NoError(t, err)
	require.Equal(t, "complete", string(meta.Status))
}

// TestRestartDurabilityTerminalRunsNeverResurrect pins the janitor
// interplay: a run that reached a terminal fact before the restart is
// recorded (meta derived from the log) and NOTHING re-arms it — no paused
// entry, no execution, no new attempt, no matter what events arrive.
func TestRestartDurabilityTerminalRunsNeverResurrect(t *testing.T) {
	ctx := context.Background()
	log := actionlog.NewMemoryActionLog()
	wf := terminalWorkflow("wf-restart-done")

	// Run to completion in runtime A.
	doneA := make(chan runtime.RunResult, 4)
	rtA := runtime.NewWorkflowRuntime(runtime.Options{
		ActionLog:   log,
		EventSource: runtime.NewManualEventSource(),
	})
	require.NoError(t, rtA.Register(wf, runtime.RegisterOptions{
		Mode:       runtime.Mode{Type: "transient"},
		OnComplete: func(r runtime.RunResult) { doneA <- r },
	}))
	rtA.Bus().Emit(ctx, runtime.ManualEvent, events.PipelineEvent{Payload: map[string]any{}})
	resA := awaitRestartResult(t, doneA)
	require.True(t, resA.OK, "run failed: %v", resA.Error)
	rtA.Shutdown(ctx)

	entriesBefore, err := log.Count(ctx, resA.RunID, 0)
	require.NoError(t, err)

	// Restart on the same log: the terminal run is classified and left
	// alone.
	rtB := runtime.NewWorkflowRuntime(runtime.Options{
		ActionLog:   log,
		EventSource: runtime.NewManualEventSource(),
	})
	defer rtB.Shutdown(ctx)

	meta, err := rtB.GetRunMeta(ctx, resA.RunID)
	require.NoError(t, err, "terminal run must be queryable after restart")
	require.Equal(t, "complete", string(meta.Status))

	require.NoError(t, rtB.Register(wf, runtime.RegisterOptions{Mode: runtime.Mode{Type: "transient"}}))

	// Events that would resume a pause (or trigger the workflow again) must
	// not resurrect the terminal run: no new attempts, no new entries.
	rtB.Bus().Emit(ctx, "go", events.PipelineEvent{Payload: map[string]any{}})
	rtB.Bus().Emit(ctx, runtime.ManualEvent, events.PipelineEvent{Payload: map[string]any{}})
	time.Sleep(200 * time.Millisecond)

	entriesAfter, err := log.Count(ctx, resA.RunID, 0)
	require.NoError(t, err)
	require.Equal(t, entriesBefore, entriesAfter, "terminal run must not gain entries after restart")

	idx, err := log.LatestRerunIndex(ctx, resA.RunID)
	require.NoError(t, err)
	require.Equal(t, 0, idx, "terminal run must never gain a new attempt")

	meta, err = rtB.GetRunMeta(ctx, resA.RunID)
	require.NoError(t, err)
	require.Equal(t, "complete", string(meta.Status), "terminal run meta must stay terminal")
}

// TestRestartDurabilityInFlightRunRecoveredFromLog covers the third
// Recover classification: a run whose last entry is neither terminal nor a
// checkpoint was IN-FLIGHT when the process died. Its meta is recovered as
// recording, its log stays intact, it is not auto-continued (no checkpoint
// → no resume address), and a later attempt would continue the numbering.
func TestRestartDurabilityInFlightRunRecoveredFromLog(t *testing.T) {
	ctx := context.Background()
	log := actionlog.NewMemoryActionLog()

	// Simulate a crashed in-flight run with raw log facts.
	deadRunID := "deadrun0000000000000000000000000"
	_, err := log.Append(ctx, actionlog.Entry{
		RunID: deadRunID, RerunIndex: 0, PipelineID: "wf-restart-pause-pipeline",
		Kind: actionlog.KindPipelineStarted, Type: "pipeline:start",
	})
	require.NoError(t, err)
	_, err = log.Append(ctx, actionlog.Entry{
		RunID: deadRunID, RerunIndex: 0, PipelineID: "wf-restart-pause-pipeline",
		Kind: actionlog.KindStepCompleted, Type: "step:success", StageID: "stage:a", StepID: "step:a",
		Output: []byte(`{"x":1}`),
	})
	require.NoError(t, err)

	rt := runtime.NewWorkflowRuntime(runtime.Options{
		ActionLog:   log,
		EventSource: runtime.NewManualEventSource(),
	})
	defer rt.Shutdown(ctx)

	// Meta recovered as recording — the run never reached a terminal fact.
	meta, err := rt.GetRunMeta(ctx, deadRunID)
	require.NoError(t, err, "in-flight run must be recovered from the log")
	require.Equal(t, "recording", string(meta.Status))
	require.Equal(t, int64(2), meta.EventCount)

	// Not resumable: no checkpoint, so no paused-registry entry — Resume
	// reports the absence instead of inventing a continuation.
	res := rt.Resume(deadRunID, nil)
	require.False(t, res.OK)
	require.Equal(t, "failed", res.Status)

	// The log facts stay intact and queryable.
	entries, err := log.All(ctx, deadRunID, 0)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	// (Attempt numbering continuation from the log is pinned end-to-end by
	// the paused-run acceptance test above: the resumed segment lands at
	// rerun index 1 — one past the recovered attempt 0.)
}
