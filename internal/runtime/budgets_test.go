package runtime

// H5 acceptance (v2.1 hardening plan): budgets and gates.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/asaidimu/hermes/pkg/watch"
	"github.com/stretchr/testify/require"
)

// gatedWorkflow: a run whose first step signals `entered` (the segment is
// now executing — and holds its global-gate slot) and then blocks on
// `gate` until released. The signal closes the acquire-before-observe
// race: a bare "completed == 0" check passes before the spawning
// goroutine has even acquired the slot.
func gatedWorkflow(id string, gate chan struct{}, entered chan struct{}) *pipeline.Workflow {
	def := pipeline.PipelineDefinition{
		ID: id + "-pipeline",
		Stages: []pipeline.Stage{
			{ID: "s1", Steps: map[string]pipeline.Step{
				"a": {ID: "a", Effect: int(pipeline.EffectSideEffecting), Action: func(ctx context.Context, pcxt *pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
					entered <- struct{}{}
					select {
					case <-gate:
					case <-ctx.Done():
					}
					return store.SetValue("done", float64(1)), nil
				}},
			}},
		},
	}
	return &pipeline.Workflow{
		ID:        id,
		Pipelines: map[string]pipeline.PipelineDefinition{"trigger:manual:Run": def},
		Triggers:  map[string]pipeline.WorkflowTrigger{"trigger:manual:Run": {ID: "trigger:manual:Run", Event: ManualEvent}},
	}
}

func TestMaxActiveRunsGateRejectsOverflow(t *testing.T) {
	gate := make(chan struct{})
	entered := make(chan struct{}, 4)
	ms := NewManualEventSource()
	rt := NewWorkflowRuntime(Options{
		ActionLog:     actionlog.NewMemoryActionLog(),
		EventSource:   ms,
		MaxActiveRuns: 1,
	})
	defer rt.Shutdown(context.Background())

	// The gate rejection and the real completion both fire the workflow's
	// OnComplete (same record), so run1 collects BOTH results: the
	// rejection's failure first (Invoke fires it synchronously), then the
	// completion after close(gate).
	done1 := make(chan RunResult, 2)
	require.NoError(t, rt.Register(gatedWorkflow("gate-wf", gate, entered), RegisterOptions{
		Mode:       Mode{Type: "transient", Concurrency: 10},
		OnComplete: func(r RunResult) { done1 <- r },
	}))

	// First run takes the only slot and blocks on the gate. The ENTERED
	// signal proves the segment is executing (and holds the slot) before
	// the test probes the gate — a bare "completed == 0" check passes
	// before the spawning goroutine has even acquired the slot (TOCTOU).
	rt.Bus().Emit(context.Background(), ManualEvent, events.PipelineEvent{Payload: map[string]any{}})
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first run never started executing")
	}

	// Second run overflows the global gate → typed conflict, no run created.
	res := rt.Invoke("gate-wf", "trigger:manual:Run", events.PipelineEvent{Payload: map[string]any{}})
	require.False(t, res.OK)
	var se *core.SystemError
	require.ErrorAs(t, res.Error, &se)
	require.Equal(t, core.ErrCodeConflict, se.Code)

	// Release: the first segment completes and frees the slot. done1 then
	// holds TWO results in order: the gate rejection's failure (fired
	// synchronously by Invoke — same workflow record) and run1's success.
	close(gate)
	require.Eventually(t, func() bool {
		return len(done1) == 2
	}, 5*time.Second, 5*time.Millisecond, "run1 must complete after the gate releases")
	rRejection := <-done1
	require.False(t, rRejection.OK, "first result is the rejection's failure")
	rCompletion := <-done1
	require.True(t, rCompletion.OK, "the completion result must be a success: %v", rCompletion.Error)
	require.Equal(t, "succeeded", rCompletion.Status)

	// Retire the first workflow BEFORE the second phase: it is still
	// registered on the same manual trigger, so the next Emit would spawn
	// runs for BOTH workflows and race them for the single slot.
	rt.Deregister("gate-wf")

	// A new run fits again.
	gate2 := make(chan struct{})
	done2 := make(chan RunResult, 1)
	require.NoError(t, rt.Register(gatedWorkflow("gate-wf2", gate2, entered), RegisterOptions{
		Mode:       Mode{Type: "transient", Concurrency: 10},
		OnComplete: func(r RunResult) { done2 <- r },
	}))
	rt.Bus().Emit(context.Background(), ManualEvent, events.PipelineEvent{Payload: map[string]any{}})
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("post-release run never started executing")
	}
	close(gate2)
	select {
	case r := <-done2:
		require.True(t, r.OK, "post-release run must complete: %v", r.Error)
	case <-time.After(5 * time.Second):
		t.Fatal("post-release run never completed")
	}
}

func TestMaxStateBytesRejectsOversizedSpawn(t *testing.T) {
	ms := NewManualEventSource()
	rt := NewWorkflowRuntime(Options{
		ActionLog:     actionlog.NewMemoryActionLog(),
		EventSource:   ms,
		MaxStateBytes: 64 * 1024,
	})
	defer rt.Shutdown(context.Background())

	def := pipeline.PipelineDefinition{
		ID: "big-pipeline",
		Stages: []pipeline.Stage{
			{ID: "s1", Steps: map[string]pipeline.Step{
				"a": {ID: "a", Effect: int(pipeline.EffectSideEffecting), Action: func(ctx context.Context, pcxt *pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
					return store.SetValue("touched", true), nil
				}},
			}},
		},
	}
	require.NoError(t, rt.Register(&pipeline.Workflow{
		ID:        "big-wf",
		Pipelines: map[string]pipeline.PipelineDefinition{"trigger:manual:Run": def},
		Triggers:  map[string]pipeline.WorkflowTrigger{"trigger:manual:Run": {ID: "trigger:manual:Run", Event: ManualEvent}},
	}, RegisterOptions{Mode: Mode{Type: "transient"}}))

	huge := strings.Repeat("x", 128*1024)
	res := rt.Invoke("big-wf", "trigger:manual:Run", events.PipelineEvent{Payload: map[string]any{"blob": huge}})
	require.False(t, res.OK)
	var se *core.SystemError
	require.ErrorAs(t, res.Error, &se)
	require.Equal(t, core.ErrCodeValidation, se.Code)

	// No run bookkeeping survives the rejection.
	_, err := rt.GetRunMeta(context.Background(), res.RunID)
	require.Error(t, err, "rejected spawn must leave no run meta")
	require.Nil(t, rt.Store(res.RunID), "rejected spawn must leave no store")

	// A within-budget spawn works.
	res2 := rt.Invoke("big-wf", "trigger:manual:Run", events.PipelineEvent{Payload: map[string]any{"blob": "small"}})
	require.True(t, res2.OK, "within-budget spawn must run: %v", res2.Error)
}

func TestEventTTLExpiresBufferedEvents(t *testing.T) {
	bus := events.NewMemoryScopedBus()
	svc := NewWatchService(bus, func(runID string, patch map[string]any) {}, 5*time.Millisecond)
	defer func() { _ = svc }()

	require.NoError(t, svc.Register("run-ttl", watchDescriptorFor("evt:x")))

	// Buffer one event (the run is not parked → queued).
	svc.mu.Lock()
	reg := svc.registrations["run-ttl"]["evt:x"]
	reg.queue = append(reg.queue, watchEventFor("evt:x"))
	reg.enqueuedAt = append(reg.enqueuedAt, time.Now().Add(-time.Hour)) // ancient
	q0 := len(reg.queue)
	svc.mu.Unlock()
	require.Equal(t, 1, q0)

	// Drain: the ancient event must be dropped, the run parks instead.
	ev := svc.OnRunPaused("run-ttl")
	require.Nil(t, ev, "expired buffered event must be dropped")
	svc.mu.Lock()
	require.Equal(t, 0, len(reg.queue))
	svc.mu.Unlock()

	// A fresh event survives the same drain.
	svc.mu.Lock()
	reg.queue = append(reg.queue, watchEventFor("evt:x"))
	reg.enqueuedAt = append(reg.enqueuedAt, time.Now())
	svc.mu.Unlock()
	ev = svc.OnRunPaused("run-ttl")
	require.NotNil(t, ev, "fresh buffered event must drain")
}

func TestEventTTLOffByDefault(t *testing.T) {
	bus := events.NewMemoryScopedBus()
	svc := NewWatchService(bus, func(runID string, patch map[string]any) {}, 0)
	require.NoError(t, svc.Register("run-ttl2", watchDescriptorFor("evt:y")))
	svc.mu.Lock()
	reg := svc.registrations["run-ttl2"]["evt:y"]
	reg.queue = append(reg.queue, watchEventFor("evt:y"))
	reg.enqueuedAt = append(reg.enqueuedAt, time.Now().Add(-time.Hour))
	svc.mu.Unlock()
	require.NotNil(t, svc.OnRunPaused("run-ttl2"), "no TTL → ancient events still drain")
}

func watchDescriptorFor(eventType string) watch.WatchDescriptor {
	return watch.WatchDescriptor{EventTypes: []string{eventType}}
}

func watchEventFor(eventType string) watch.WatchEvent {
	return watch.WatchEvent{EventType: eventType}
}
