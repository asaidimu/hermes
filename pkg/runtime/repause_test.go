package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/effect"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/asaidimu/hermes/pkg/watch"
	"github.com/stretchr/testify/require"
)

// awaitPaused waits until the runtime tracks exactly want paused runs.
func awaitPaused(t *testing.T, rt *WorkflowRuntime, want int) {
	t.Helper()
	require.Eventually(t, func() bool {
		rt.mu.Lock()
		defer rt.mu.Unlock()
		return len(rt.paused) == want
	}, 5*time.Second, 10*time.Millisecond, "expected %d paused runs", want)
}

// multiPauseWorkflow builds a workflow with two consecutive pauses:
// set a=1 → pause(evt:1) → set b=2 → pause(via pauseFn) → set c=3.
// Pipelines is keyed by the trigger id with a distinct pipeline ID, so the
// test also exercises the resume definition lookup fallback
// (#review-20260910-022) the same way hand-built workflows hit it.
func multiPauseWorkflow(t *testing.T, secondPause func(ctx context.Context, state map[string]any, st store.Store) (pipeline.RoutingInstruction, error)) *pipeline.Workflow {
	t.Helper()
	def := pipeline.PipelineDefinition{
		ID:    "multi-pause-pipeline",
		Label: "Multi Pause Pipeline",
		Stages: []pipeline.Stage{
			{
				ID: "stage:a", Label: "Set A",
				Steps: map[string]pipeline.Step{
					"step:a": {ID: "step:a", Effect: int(effect.SideEffecting), Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
						return store.SetValue("a", float64(1)), nil
					}},
				},
			},
			{
				ID: "stage:pause1", Label: "Pause 1",
				Router: func(ctx context.Context, state map[string]any, st store.Store) (pipeline.RoutingInstruction, error) {
					return pipeline.PauseForEvent("evt:1", 0), nil
				},
			},
			{
				ID: "stage:b", Label: "Set B",
				Steps: map[string]pipeline.Step{
					"step:b": {ID: "step:b", Effect: int(effect.SideEffecting), Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
						return store.SetValue("b", float64(2)), nil
					}},
				},
			},
			{
				ID: "stage:pause2", Label: "Pause 2",
				Router: secondPause,
			},
			{
				ID: "stage:c", Label: "Set C",
				Steps: map[string]pipeline.Step{
					"step:c": {ID: "step:c", Effect: int(effect.SideEffecting), Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
						return store.SetValue("c", float64(3)), nil
					}},
				},
			},
		},
	}
	return &pipeline.Workflow{
		ID:    "wf-multi-pause",
		Label: "Multi Pause",
		Pipelines: map[string]pipeline.PipelineDefinition{
			"trigger:manual:Run": def,
		},
		Triggers: map[string]pipeline.WorkflowTrigger{
			"trigger:manual:Run": {ID: "trigger:manual:Run", Event: ManualEvent},
		},
	}
}

// TestResumeRepauseSingleEvent covers #review-20260910-001 (re-pause tracking:
// watch re-registration so the second pause's awaited event can arrive) and
// #review-20260910-002 (the final outcome must reflect the post-resume store,
// including mutations from both resumed segments).
func TestResumeRepauseSingleEvent(t *testing.T) {
	wf := multiPauseWorkflow(t, func(ctx context.Context, state map[string]any, st store.Store) (pipeline.RoutingInstruction, error) {
		return pipeline.PauseForEvent("evt:2", 0), nil
	})

	ms := NewManualEventSource()
	done := make(chan RunResult, 1)
	rt := NewWorkflowRuntime(Options{
		ActionLog:   actionlog.NewMemoryActionLog(),
		EventSource: ms,
	})
	defer rt.Shutdown(context.Background())

	prepared := make(chan string, 1)
	err := rt.Register(wf, RegisterOptions{
		Mode:       Mode{Type: "transient"},
		OnPrepare:  func(h *RunHandle) error { prepared <- h.RunID; return nil },
		OnComplete: func(r RunResult) { done <- r },
	})
	require.NoError(t, err)

	rt.Bus().Emit(context.Background(), ManualEvent, events.PipelineEvent{Payload: map[string]any{}})
	runID := <-prepared

	// First pause.
	awaitPaused(t, rt, 1)
	ms.Emit("evt:1", map[string]any{"first": true})

	// Second pause — re-tracked with its own watch registration.
	awaitPaused(t, rt, 1)
	ms.Emit("evt:2", map[string]any{"second": true})

	select {
	case res := <-done:
		require.True(t, res.OK)
		require.Equal(t, "succeeded", res.Status)
		// #review-20260910-002: FinalState must reflect the completed run —
		// mutations from both resumed segments included.
		require.Equal(t, float64(1), res.FinalState["a"])
		require.Equal(t, float64(2), res.FinalState["b"])
		require.Equal(t, float64(3), res.FinalState["c"])
		require.Equal(t, true, res.FinalState["first"])
		require.Equal(t, true, res.FinalState["second"])
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for multi-pause run completion")
	}

	// The canonical store must carry the final state too (#review-20260910-002).
	rt.mu.Lock()
	canonical := rt.stores[runID]
	rt.mu.Unlock()
	require.NotNil(t, canonical)
	var b any
	_ = canonical.Read(func(state map[string]any) error { b = state["b"]; return nil })
	require.Equal(t, float64(2), b)
}

// TestResumeRepauseMultiEvent covers the multi-event half of
// #review-20260910-001: a re-paused PauseForEvents wait must be tracked with
// waitForEvents/waitMode and re-registered with the WatchService, or the run
// is stuck forever after its second pause.
func TestResumeRepauseMultiEvent(t *testing.T) {
	wf := multiPauseWorkflow(t, func(ctx context.Context, state map[string]any, st store.Store) (pipeline.RoutingInstruction, error) {
		return pipeline.PauseForEvents([]string{"evt:2a", "evt:2b"}, "any", 0), nil
	})

	ms := NewManualEventSource()
	done := make(chan RunResult, 1)
	rt := NewWorkflowRuntime(Options{
		ActionLog:   actionlog.NewMemoryActionLog(),
		EventSource: ms,
	})
	defer rt.Shutdown(context.Background())

	err := rt.Register(wf, RegisterOptions{
		Mode:       Mode{Type: "transient"},
		OnComplete: func(r RunResult) { done <- r },
	})
	require.NoError(t, err)

	rt.Bus().Emit(context.Background(), ManualEvent, events.PipelineEvent{Payload: map[string]any{}})

	awaitPaused(t, rt, 1)
	ms.Emit("evt:1", nil)

	awaitPaused(t, rt, 1)
	ms.Emit("evt:2a", map[string]any{"resumed": true})

	select {
	case res := <-done:
		require.True(t, res.OK)
		require.Equal(t, "succeeded", res.Status)
		require.Equal(t, float64(1), res.FinalState["a"])
		require.Equal(t, float64(2), res.FinalState["b"])
		require.Equal(t, float64(3), res.FinalState["c"])
		require.Equal(t, true, res.FinalState["resumed"])
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for multi-event re-pause completion")
	}
}

// TestWatchServiceMultiEventAll covers the WatchService half of
// #review-20260910-007's companion fix: a parked mode=all multi-event
// registration must not resume until every watched event type has delivered.
func TestWatchServiceMultiEventAll(t *testing.T) {
	resumed := make(chan map[string]any, 1)
	ws := NewWatchService(events.NewMemoryScopedBus(), func(runID string, patch map[string]any) {
		resumed <- patch
	})

	err := ws.Register("run-1", watch.WatchDescriptor{
		EventTypes: []string{"evt:a", "evt:b"},
		Mode:       "all",
		Timeout:    0,
	})
	require.NoError(t, err)
	require.Nil(t, ws.OnRunPaused("run-1"), "freshly parked run has nothing buffered")

	// First of two watched types: must NOT resume.
	ws.onEvent("evt:a", map[string]any{"n": 1})
	select {
	case p := <-resumed:
		t.Fatalf("mode=all resumed after only the first event: %v", p)
	default:
	}

	// Second watched type: now the completed set resumes the run.
	ws.onEvent("evt:b", map[string]any{"n": 2})
	select {
	case p := <-resumed:
		require.Equal(t, 2, p["n"], "the latest event's payload is the resume patch")
	case <-time.After(time.Second):
		t.Fatal("mode=all did not resume after all watched types delivered")
	}

	// A new parked cycle resets delivered-type tracking: evt:a alone must not
	// resume again from stale tracking.
	require.NoError(t, ws.Register("run-2", watch.WatchDescriptor{
		EventTypes: []string{"evt:a", "evt:b"},
		Mode:       "all",
		Timeout:    0,
	}))
	require.Nil(t, ws.OnRunPaused("run-2"))
	ws.onEvent("evt:a", map[string]any{"n": 3})
	select {
	case p := <-resumed:
		t.Fatalf("stale delivered tracking resumed the run: %v", p)
	default:
	}
}
