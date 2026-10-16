package tests

// H1.2 acceptance (v2.1 hardening plan): definition-drift detection.
//
// The workflow fingerprint (pipeline.WorkflowFingerprint) is stamped on
// KindPipelineStarted and KindCheckpoint entries; Resume and the restart
// re-arm compare it against the workflow NOW registered. Default policy
// "warn" proceeds but tags the meta; "require-same" refuses (typed
// ErrCodeDefinitionDrift) — and a refused pause is never lost: the
// same-process pause stays resumable once the matching workflow registers
// again, the restart pause stays staged in pendingPauses for a later bind.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/runtime"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/stretchr/testify/require"
)

// driftWorkflow: set x=1 (effectful) → pause for "go" → set y=2 (effectful).
// mutate is applied to the pipeline definition BEFORE wrapping, so callers
// can produce a fingerprint-different (but structurally valid) version of
// the same workflow id.
func driftWorkflow(id string, mutate func(*pipeline.PipelineDefinition)) *pipeline.Workflow {
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
	if mutate != nil {
		mutate(&def)
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

// editRetries changes a hashed-but-behavior-identical field: the fingerprint
// must move while the run semantics stay the same.
func editRetries(d *pipeline.PipelineDefinition) {
	s := d.Stages[2].Steps["step:b"]
	s.Retries = 2
	d.Stages[2].Steps["step:b"] = s
}

func startPausedRun(t *testing.T, rt *runtime.WorkflowRuntime, ms *runtime.ManualEventSource, done chan runtime.RunResult) string {
	t.Helper()
	prepared := make(chan string, 1)
	require.NoError(t, rt.Register(driftWorkflow("drift-wf", nil), runtime.RegisterOptions{
		Mode:       runtime.Mode{Type: "transient"},
		OnPrepare:  func(h *runtime.RunHandle) error { prepared <- h.RunID; return nil },
		OnComplete: func(r runtime.RunResult) { done <- r },
	}))
	rt.Bus().Emit(context.Background(), runtime.ManualEvent, events.PipelineEvent{Payload: map[string]any{}})
	runID := <-prepared
	awaitMetaStatusRestart(t, rt, runID, "paused")
	return runID
}

// TestDefinitionDriftWarnPolicyProceedsAndTags: default policy — the resume
// proceeds onto the edited definition, but the meta records the drift.
func TestDefinitionDriftWarnPolicyProceedsAndTags(t *testing.T) {
	ms := runtime.NewManualEventSource()
	done := make(chan runtime.RunResult, 4)
	rt := runtime.NewWorkflowRuntime(runtime.Options{
		ActionLog:   actionlog.NewMemoryActionLog(),
		EventSource: ms,
	})
	defer rt.Shutdown(context.Background())

	runID := startPausedRun(t, rt, ms, done)

	meta, err := rt.GetRunMeta(context.Background(), runID)
	require.NoError(t, err)
	require.NotEmpty(t, meta.DefHash, "spawn must stamp the workflow fingerprint into the run meta")
	require.False(t, meta.DefinitionDrift)

	// Re-register the SAME workflow id with an edited definition.
	rt.Deregister("drift-wf")
	require.NoError(t, rt.Register(driftWorkflow("drift-wf", editRetries), runtime.RegisterOptions{
		Mode:       runtime.Mode{Type: "transient"},
		OnComplete: func(r runtime.RunResult) { done <- r },
	}))

	ms.Emit("go", map[string]any{"resumed": true})

	select {
	case res := <-done:
		require.True(t, res.OK)
		require.Equal(t, "succeeded", res.Status)
	case <-time.After(5 * time.Second):
		t.Fatal("warn policy must proceed — run did not complete")
	}

	meta, err = rt.GetRunMeta(context.Background(), runID)
	require.NoError(t, err)
	require.True(t, meta.DefinitionDrift, "warn policy must tag DefinitionDrift on the meta")

	st := rt.Store(runID)
	require.NotNil(t, st)
	var y any
	require.NoError(t, st.Read(func(state map[string]any) error { y = state["y"]; return nil }))
	require.Equal(t, float64(2), y, "resumed segment must have executed on the registered definition")
}

// TestDefinitionDriftRequireSameRefusesButNeverLoses: "require-same" refuses
// the resume with the typed code, the run stays paused — and once the
// ORIGINAL workflow registers again, the same pause still resumes to
// completion. Refusal is a guard, not a deletion.
func TestDefinitionDriftRequireSameRefusesButNeverLoses(t *testing.T) {
	ms := runtime.NewManualEventSource()
	done := make(chan runtime.RunResult, 4)
	rt := runtime.NewWorkflowRuntime(runtime.Options{
		ActionLog:        actionlog.NewMemoryActionLog(),
		EventSource:      ms,
		DefinitionPolicy: "require-same",
	})
	defer rt.Shutdown(context.Background())

	runID := startPausedRun(t, rt, ms, done)

	rt.Deregister("drift-wf")
	require.NoError(t, rt.Register(driftWorkflow("drift-wf", editRetries), runtime.RegisterOptions{
		Mode:       runtime.Mode{Type: "transient"},
		OnComplete: func(r runtime.RunResult) { done <- r },
	}))

	// Async dispatch resume refuses; nothing may complete.
	ms.Emit("go", map[string]any{"resumed": true})
	time.Sleep(200 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("require-same must refuse the drifted resume")
	default:
	}

	// Direct resume surfaces the typed error.
	res := rt.Resume(runID, nil)
	require.False(t, res.OK)
	require.Equal(t, "failed", res.Status)
	var se *core.SystemError
	require.True(t, errors.As(res.Error, &se), "error must be a *core.SystemError, got %T", res.Error)
	require.Equal(t, core.ErrCodeDefinitionDrift, se.Code)

	meta, err := rt.GetRunMeta(context.Background(), runID)
	require.NoError(t, err)
	require.True(t, meta.DefinitionDrift)
	require.Equal(t, "paused", string(meta.Status), "refused run stays paused")

	// Restore the ORIGINAL definition: the same pause resumes to completion.
	rt.Deregister("drift-wf")
	require.NoError(t, rt.Register(driftWorkflow("drift-wf", nil), runtime.RegisterOptions{
		Mode:       runtime.Mode{Type: "transient"},
		OnComplete: func(r runtime.RunResult) { done <- r },
	}))
	finish := rt.Resume(runID, map[string]any{"resumed": true})
	require.True(t, finish.OK, "resume after restoring the original workflow must succeed: %v", finish.Error)
	require.Equal(t, "succeeded", finish.Status)

	st := rt.Store(runID)
	require.NotNil(t, st)
	var y any
	require.NoError(t, st.Read(func(state map[string]any) error { y = state["y"]; return nil }))
	require.Equal(t, float64(2), y)
}

// TestRestartRearmDriftRequireSameStaysPending: the restart path. Runtime A
// records the pause; runtime B ("require-same") registers an EDITED workflow
// — the re-arm is refused but the pause stays staged; registering the
// original workflow re-claims it and the run completes.
func TestRestartRearmDriftRequireSameStaysPending(t *testing.T) {
	log := actionlog.NewMemoryActionLog()
	ms := runtime.NewManualEventSource()
	done := make(chan runtime.RunResult, 4)
	rtA := runtime.NewWorkflowRuntime(runtime.Options{ActionLog: log, EventSource: ms})
	runID := startPausedRun(t, rtA, ms, done)
	rtA.Shutdown(context.Background())

	rtB := runtime.NewWorkflowRuntime(runtime.Options{
		ActionLog:        log,
		DefinitionPolicy: "require-same",
	})
	defer rtB.Shutdown(context.Background())

	// B's constructor Recover staged the pause; the edited workflow defines
	// the paused pipeline, but its fingerprint differs → refusal, stay pending.
	require.NoError(t, rtB.Register(driftWorkflow("drift-wf", editRetries), runtime.RegisterOptions{
		Mode: runtime.Mode{Type: "transient"},
	}))

	meta, err := rtB.GetRunMeta(context.Background(), runID)
	require.NoError(t, err)
	require.True(t, meta.DefinitionDrift, "restart bind must tag the drift")
	require.Equal(t, "paused", string(meta.Status))
	require.NotEmpty(t, meta.DefHash, "recovered meta must carry the recorded fingerprint")

	// Emitting the waited event must NOT resume (nothing is armed).
	msB := runtime.NewManualEventSource()
	_ = msB
	rtB.Deregister("drift-wf")
	require.NoError(t, rtB.Register(driftWorkflow("drift-wf", nil), runtime.RegisterOptions{
		Mode:       runtime.Mode{Type: "transient"},
		OnComplete: func(r runtime.RunResult) { done <- r },
	}))

	// The pending pause is re-claimed by the matching registration and armed.
	finish := rtB.Resume(runID, nil)
	require.True(t, finish.OK, "pause must be re-armed after the matching workflow registers: %v", finish.Error)
	require.Equal(t, "succeeded", finish.Status)

	st := rtB.Store(runID)
	require.NotNil(t, st)
	var y any
	require.NoError(t, st.Read(func(state map[string]any) error { y = state["y"]; return nil }))
	require.Equal(t, float64(2), y, "replayed+resumed run must reach the same terminal state")
}
