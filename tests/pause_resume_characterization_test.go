package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/runtime"
	"github.com/asaidimu/hermes/pkg/scheduler"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/stretchr/testify/require"
)

// Characterization tests for the runtime's pause/resume surface (refactor
// plan P0.2). They pin CURRENT observable behavior so the Phase 3 fixes
// (C3-fix terminal cron cancel, C5-fix resume ordering) and the Phase 2
// rebuild can each be judged against a recorded baseline. Where a test
// documents a bug as today's behavior (the H9 cron leak), the fixing commit
// flips the assertion in the same change.

// recordingScheduler is a counting scheduler fake: it records every
// Schedule/Cancel call so tests can assert arming/cancellation without
// real timers. scheduler.Scheduler is kept as an interface by the refactor
// plan (R.3) precisely because this fake is a genuine test seam.
type recordingScheduler struct {
	mu        sync.Mutex
	schedules map[string]string // id → cron expression (latest wins)
	cancels   []string
}

func newRecordingScheduler() *recordingScheduler {
	return &recordingScheduler{schedules: map[string]string{}}
}

func (f *recordingScheduler) Schedule(id string, cron string, cb func(context.Context)) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.schedules[id] = cron
	return nil
}

func (f *recordingScheduler) Cancel(id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancels = append(f.cancels, id)
	return nil
}

func (f *recordingScheduler) Shutdown(context.Context) error { return nil }

func (f *recordingScheduler) scheduledIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.schedules))
	for id := range f.schedules {
		out = append(out, id)
	}
	return out
}

func (f *recordingScheduler) wasCancelled(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.cancels {
		if c == id {
			return true
		}
	}
	return false
}

var _ scheduler.Scheduler = (*recordingScheduler)(nil)

// parityWorkflow builds the deterministic graph used for pause/resume
// parity: set x=1 → [optional event-pause stage] → set y=2.
func parityWorkflow(id string, withPause bool) *pipeline.Workflow {
	stages := []pipeline.Stage{
		{
			ID: "stage:a", Label: "Set X",
			Steps: map[string]pipeline.Step{
				"step:a": {ID: "step:a", Action: func(ctx context.Context, pcxt *pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
					return store.SetValue("x", float64(1)), nil
				}},
			},
		},
	}
	if withPause {
		stages = append(stages, pipeline.Stage{
			ID: "stage:wait", Label: "Wait",
			Router: func(ctx context.Context, state map[string]any, st store.Store) (pipeline.RoutingInstruction, error) {
				return pipeline.PauseForEvent("go", 0), nil
			},
		})
	}
	stages = append(stages, pipeline.Stage{
		ID: "stage:b", Label: "Set Y",
		Steps: map[string]pipeline.Step{
			"step:b": {ID: "step:b", Action: func(ctx context.Context, pcxt *pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
				return store.SetValue("y", float64(2)), nil
			}},
		},
	})
	def := pipeline.PipelineDefinition{ID: id + "-pipeline", Label: id, Stages: stages}
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

// startRuntime registers wf on a fresh runtime (MemoryActionLog +
// ManualEventSource) and emits the manual trigger. Returns the runtime, the
// run-id channel, and the completion channel.
func startRuntime(t *testing.T, wf *pipeline.Workflow, extra ...func(*runtime.Options)) (*runtime.WorkflowRuntime, chan string, chan runtime.RunResult) {
	t.Helper()
	ms := runtime.NewManualEventSource()
	done := make(chan runtime.RunResult, 4)
	prepared := make(chan string, 4)
	opts := runtime.Options{
		ActionLog:   actionlog.NewMemoryActionLog(),
		EventSource: ms,
	}
	for _, fn := range extra {
		fn(&opts)
	}
	rt := runtime.NewWorkflowRuntime(opts)
	require.NoError(t, rt.Register(wf, runtime.RegisterOptions{
		Mode:       runtime.Mode{Type: "transient"},
		OnPrepare:  func(h *runtime.RunHandle) error { prepared <- h.RunID; return nil },
		OnComplete: func(r runtime.RunResult) { done <- r },
	}))
	rt.Bus().Emit(context.Background(), runtime.ManualEvent, events.PipelineEvent{Payload: map[string]any{}})
	return rt, prepared, done
}

func awaitResult(t *testing.T, done chan runtime.RunResult) runtime.RunResult {
	t.Helper()
	select {
	case res := <-done:
		return res
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for run completion")
		return runtime.RunResult{}
	}
}

// awaitMetaStatus polls the run's recorded status until it reaches want.
func awaitMetaStatus(t *testing.T, rt *runtime.WorkflowRuntime, runID, want string) {
	t.Helper()
	require.Eventually(t, func() bool {
		meta, err := rt.GetRunMeta(context.Background(), runID)
		if err != nil {
			return false
		}
		return string(meta.Status) == want
	}, 5*time.Second, 10*time.Millisecond, "run %s never reached status %s", runID, want)
}

// TestPauseResumeParityCharacterization pins the parity contract: a
// deterministic graph's pause→resume end state (domain keys) equals the
// same graph's uninterrupted end state. It also pins the checkpoint residue
// the pausing run carries in state — the uninterrupted run has none.
func TestPauseResumeParityCharacterization(t *testing.T) {
	ctx := context.Background()

	// Uninterrupted baseline.
	rtA, _, doneA := startRuntime(t, parityWorkflow("wf-parity-straight", false))
	resA := awaitResult(t, doneA)
	require.True(t, resA.OK, "straight run failed: %v", resA.Error)
	rtA.Shutdown(ctx)

	// Pausing run.
	rtB, preparedB, doneB := startRuntime(t, parityWorkflow("wf-parity-pause", true))
	runID := <-preparedB
	awaitMetaStatus(t, rtB, runID, "paused")

	rtB.Resume(runID, nil)
	resB := awaitResult(t, doneB)
	require.True(t, resB.OK, "resumed run failed: %v", resB.Error)
	require.Equal(t, "succeeded", resB.Status)

	// Domain keys identical across both executions.
	require.Equal(t, resA.FinalState["x"], resB.FinalState["x"], "x identical after pause/resume")
	require.Equal(t, resA.FinalState["y"], resB.FinalState["y"], "y identical after pause/resume")

	// Pinned residue: the pausing run carries a checkpoint under
	// __pipeline_data__; the uninterrupted run does not.
	_, hadCkptB := resB.FinalState["__pipeline_data__"]
	require.True(t, hadCkptB, "paused run keeps checkpoint state after completion (today's behavior)")
	_, hadCkptA := resA.FinalState["__pipeline_data__"]
	require.False(t, hadCkptA, "uninterrupted run carries no checkpoint state")

	rtB.Shutdown(ctx)
}

// cronWorkflow arms a cron auto-resume at stage:wait when withCron is set;
// otherwise the same stage waits on a plain event.
func cronWorkflow(id string, withCron bool) *pipeline.Workflow {
	router := func(ctx context.Context, state map[string]any, st store.Store) (pipeline.RoutingInstruction, error) {
		if withCron {
			return pipeline.PauseForCron("cron-tick", "@every 1h"), nil
		}
		return pipeline.PauseForEvent("cron-tick", 0), nil
	}
	def := pipeline.PipelineDefinition{
		ID: id + "-pipeline",
		Stages: []pipeline.Stage{
			{ID: "stage:wait", Label: "Wait", Router: router},
			{ID: "stage:done", Label: "Done", Steps: map[string]pipeline.Step{
				"step:done": {ID: "step:done", Action: func(ctx context.Context, pcxt *pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
					return store.SetValue("done", true), nil
				}},
			}},
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

func newRuntimeWithFakeScheduler(t *testing.T, wf *pipeline.Workflow) (*runtime.WorkflowRuntime, *recordingScheduler, *runtime.ManualEventSource, chan string, chan runtime.RunResult) {
	t.Helper()
	fake := newRecordingScheduler()
	ms := runtime.NewManualEventSource()
	done := make(chan runtime.RunResult, 4)
	prepared := make(chan string, 4)
	rt := runtime.NewWorkflowRuntime(runtime.Options{
		ActionLog:   actionlog.NewMemoryActionLog(),
		EventSource: ms,
		Scheduler:   fake,
	})
	require.NoError(t, rt.Register(wf, runtime.RegisterOptions{
		Mode:       runtime.Mode{Type: "transient"},
		OnPrepare:  func(h *runtime.RunHandle) error { prepared <- h.RunID; return nil },
		OnComplete: func(r runtime.RunResult) { done <- r },
	}))
	rt.Bus().Emit(context.Background(), runtime.ManualEvent, events.PipelineEvent{Payload: map[string]any{}})
	return rt, fake, ms, prepared, done
}

// awaitArmed polls until the scheduler holds an auto-resume schedule for
// runID (the arming behavior under characterization).
func awaitArmed(t *testing.T, fake *recordingScheduler, runID string) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, id := range fake.scheduledIDs() {
			if id == runID {
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond, "auto-resume for %s never armed", runID)
}

// TestCronAutoResumeArmingCharacterization pins the arming rule: the
// auto-resume timer is scheduled (keyed by runID) only when the pause
// checkpoint carries a cron expression; a plain event pause arms nothing.
func TestCronAutoResumeArmingCharacterization(t *testing.T) {
	// With cron → armed.
	rt, fake, ms, prepared, done := newRuntimeWithFakeScheduler(t, cronWorkflow("wf-cron-armed", true))
	runID := <-prepared
	awaitArmed(t, fake, runID)

	ms.Emit("cron-tick", nil)
	res := awaitResult(t, done)
	require.Equal(t, "succeeded", res.Status)
	rt.Shutdown(context.Background())

	// Without cron → never armed.
	rt2, fake2, _, prepared2, done2 := newRuntimeWithFakeScheduler(t, cronWorkflow("wf-cron-absent", false))
	runID2 := <-prepared2
	time.Sleep(200 * time.Millisecond) // give any (wrong) arming a chance to show up
	for _, id := range fake2.scheduledIDs() {
		require.NotEqual(t, runID2, id, "plain event pause must not arm an auto-resume timer")
	}
	require.Len(t, fake2.scheduledIDs(), 0, "no schedules at all for the cron-less workflow")

	// Complete via the awaited event to end the run cleanly.
	rt2.Resume(runID2, nil)
	res2 := awaitResult(t, done2)
	require.Equal(t, "succeeded", res2.Status)
	rt2.Shutdown(context.Background())
}

// TestRepauseCancelsStaleCronCharacterization pins the re-pause branch: when
// a run with an armed cron re-pauses on a checkpoint WITHOUT a cron, the
// stale schedule is cancelled so it cannot resume the new wait prematurely
// (runtime.go re-pause bookkeeping, the `else { rt.scheduler.Cancel(runID) }`
// arm).
func TestRepauseCancelsStaleCronCharacterization(t *testing.T) {
	fake := newRecordingScheduler()
	ms := runtime.NewManualEventSource()
	done := make(chan runtime.RunResult, 4)
	prepared := make(chan string, 4)

	def := pipeline.PipelineDefinition{
		ID: "wf-repause-cron-pipeline",
		Stages: []pipeline.Stage{
			{ID: "stage:p1", Label: "Pause 1", Router: func(ctx context.Context, state map[string]any, st store.Store) (pipeline.RoutingInstruction, error) {
				return pipeline.PauseForCron("evt:1", "@every 1h"), nil
			}},
			{ID: "stage:mid", Label: "Mid", Steps: map[string]pipeline.Step{
				"step:mid": {ID: "step:mid", Action: func(ctx context.Context, pcxt *pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
					return store.SetValue("mid", true), nil
				}},
			}},
			{ID: "stage:p2", Label: "Pause 2", Router: func(ctx context.Context, state map[string]any, st store.Store) (pipeline.RoutingInstruction, error) {
				return pipeline.PauseForEvent("evt:2", 0), nil
			}},
			{ID: "stage:end", Label: "End", Steps: map[string]pipeline.Step{
				"step:end": {ID: "step:end", Action: func(ctx context.Context, pcxt *pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
					return store.SetValue("end", true), nil
				}},
			}},
		},
	}
	wf := &pipeline.Workflow{
		ID:    "wf-repause-cron",
		Label: "Repause Cron",
		Pipelines: map[string]pipeline.PipelineDefinition{
			"trigger:manual:Run": def,
		},
		Triggers: map[string]pipeline.WorkflowTrigger{
			"trigger:manual:Run": {ID: "trigger:manual:Run", Event: runtime.ManualEvent},
		},
	}

	rt := runtime.NewWorkflowRuntime(runtime.Options{
		ActionLog:   actionlog.NewMemoryActionLog(),
		EventSource: ms,
		Scheduler:   fake,
	})
	defer rt.Shutdown(context.Background())
	require.NoError(t, rt.Register(wf, runtime.RegisterOptions{
		Mode:       runtime.Mode{Type: "transient"},
		OnPrepare:  func(h *runtime.RunHandle) error { prepared <- h.RunID; return nil },
		OnComplete: func(r runtime.RunResult) { done <- r },
	}))
	rt.Bus().Emit(context.Background(), runtime.ManualEvent, events.PipelineEvent{Payload: map[string]any{}})
	runID := <-prepared

	awaitArmed(t, fake, runID)

	// Resume via the awaited event; the run re-pauses on stage:p2 (no cron)
	// — the stale cron schedule must be cancelled during that re-pause.
	ms.Emit("evt:1", nil)
	require.Eventually(t, func() bool { return fake.wasCancelled(runID) },
		5*time.Second, 10*time.Millisecond, "stale cron schedule was not cancelled on re-pause")

	// The run still completes through the second event.
	ms.Emit("evt:2", nil)
	res := awaitResult(t, done)
	require.True(t, res.OK, "run failed after re-pause: %v", res.Error)
}

// TestTerminalOutcomeCancelsCronAutoResume pins the C3-fix behavior
// (formerly a characterization of the H9 leak): a run that pauses with a
// cron checkpoint and then completes through its event must cancel the
// auto-resume timer. Pre-fix, the timer leaked — a permanent
// self-re-arming timer grabbing rt.mu on every fire.
func TestTerminalOutcomeCancelsCronAutoResume(t *testing.T) {
	rt, fake, ms, prepared, done := newRuntimeWithFakeScheduler(t, cronWorkflow("wf-cron-leak", true))
	runID := <-prepared
	awaitArmed(t, fake, runID)

	ms.Emit("cron-tick", nil)
	res := awaitResult(t, done)
	require.Equal(t, "succeeded", res.Status)

	// H9 (fixed): terminal outcome releases the scheduler's timer.
	require.True(t, fake.wasCancelled(runID),
		"terminal outcome must cancel the cron auto-resume (H9)")
	rt.Shutdown(context.Background())
}
