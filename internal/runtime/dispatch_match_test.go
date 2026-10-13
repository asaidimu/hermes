package runtime

import (
        "context"
        "testing"
        "time"

        "github.com/asaidimu/hermes/pkg/actionlog"
        "github.com/asaidimu/hermes/pkg/events"
        "github.com/asaidimu/hermes/pkg/pipeline"
        "github.com/asaidimu/hermes/pkg/store"
        "github.com/stretchr/testify/require"
)

// P4.6 dispatch-matching contract, pinned BEFORE the pausedByEvent index
// replaces the O(paused) scan: event matching is EXACT string equality on
// the paused wait's event type(s) — no wildcard expansion in either
// direction — and multi-event waits accumulate ReceivedEvents until the
// mode's condition is met.
//
// waitEventWorkflow builds a run that pauses on the given event type(s)
// (mode any|all), letting the test observe which runtime dispatches
// resume it.
func waitEventWorkflow(t *testing.T, waitMode string, eventTypes ...string) *pipeline.Workflow {
        t.Helper()
        pauseStage := pipeline.Stage{
                ID: "stage:wait", Label: "Wait",
                Router: func(ctx context.Context, state map[string]any, st store.Store) (pipeline.RoutingInstruction, error) {
                        return pipeline.PauseForEvents(eventTypes, waitMode, 0), nil
                },
        }
        final := pipeline.Stage{
                ID: "stage:final", Label: "Final",
                Steps: map[string]pipeline.Step{
                        "final": {ID: "final", Effect: int(pipeline.EffectPure), Action: func(ctx context.Context, pcxt *pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
                                return store.SetValue("done", true), nil
                        }},
                },
        }
        def := pipeline.PipelineDefinition{
                ID:    "wait-pipeline",
                Label: "Wait",
                Stages: []pipeline.Stage{pauseStage, final},
        }
        return &pipeline.Workflow{
                ID:    "wf-wait",
                Label: "Wait",
                Pipelines: map[string]pipeline.PipelineDefinition{
                        "trigger:manual:Run": def,
                },
                Triggers: map[string]pipeline.WorkflowTrigger{
                        "trigger:manual:Run": {ID: "trigger:manual:Run", Event: ManualEvent},
                },
        }
}

// newWaitRun registers the workflow, starts a run, and waits for pause.
func newWaitRun(t *testing.T, wf *pipeline.Workflow) (*WorkflowRuntime, *ManualEventSource, string, chan RunResult) {
        t.Helper()
        ms := NewManualEventSource()
        rt := NewWorkflowRuntime(Options{
                ActionLog:   actionlog.NewMemoryActionLog(),
                EventSource: ms,
        })
        t.Cleanup(func() { _ = rt.Shutdown(context.Background()) })
        done := make(chan RunResult, 1)
        require.NoError(t, rt.Register(wf, RegisterOptions{
                Mode:       Mode{Type: "transient"},
                OnComplete: func(r RunResult) { done <- r },
        }))
        rt.Bus().Emit(context.Background(), ManualEvent, events.PipelineEvent{Payload: map[string]any{}})
        awaitPaused(t, rt, 1)
        return rt, ms, <-pausedRunID(t, rt), done
}

func pausedRunID(t *testing.T, rt *WorkflowRuntime) chan string {
        t.Helper()
        ch := make(chan string, 1)
        rt.mu.Lock()
        for id := range rt.paused {
                ch <- id
                break
        }
        rt.mu.Unlock()
        return ch
}

// TestDispatchExactMatchNoWildcard: "evt:*" is a LITERAL event type, not
// a pattern — it resumes only runs waiting for exactly "evt:*", and a
// run waiting for "evt:1" is not resumed by "evt:*" or "evt:1x".
func TestDispatchExactMatchNoWildcard(t *testing.T) {
        wf := waitEventWorkflow(t, "any", "evt:1")
        rt, ms, _, done := newWaitRun(t, wf)

        // Wildcard-ish and prefix-ish events must NOT resume the run.
        ms.Emit("evt:*", nil)
        ms.Emit("evt:1x", nil)
        ms.Emit("evt:10", nil)
        rt.mu.Lock()
        stillPaused := len(rt.paused) == 1
        rt.mu.Unlock()
        require.True(t, stillPaused, "non-exact events must not resume an exact wait")

        // The exact event resumes and completes.
        ms.Emit("evt:1", nil)
        select {
        case res := <-done:
                require.Equal(t, "succeeded", res.Status)
        case <-time.After(5 * time.Second):
                t.Fatal("exact event did not resume the run")
        }
}

// TestDispatchMultiEventAnyMode: mode=any resumes on the FIRST matching
// event of the wait list.
func TestDispatchMultiEventAnyMode(t *testing.T) {
        wf := waitEventWorkflow(t, "any", "evt:a", "evt:b")
        rt, ms, _, done := newWaitRun(t, wf)

        ms.Emit("evt:b", nil)
        select {
        case res := <-done:
                require.Equal(t, "succeeded", res.Status)
        case <-time.After(5 * time.Second):
                t.Fatal("any-mode wait did not resume on first matching event")
        }
        _ = rt
}

// TestDispatchMultiEventAllMode: mode=all stays paused until EVERY
// watched type has delivered; a duplicate delivery does not resume
// early; the set completing resumes and completes the run. (The
// ReceivedEvents bookkeeping itself is not pinned here — dispatch's
// checkpoint accumulation and the WatchService's delivered-type
// tracking run in parallel by design; the observable contract is the
// pause/resume boundary.)
func TestDispatchMultiEventAllMode(t *testing.T) {
        wf := waitEventWorkflow(t, "all", "evt:a", "evt:b")
        rt, ms, _, done := newWaitRun(t, wf)

        ms.Emit("evt:a", nil)
        time.Sleep(50 * time.Millisecond)
        rt.mu.Lock()
        stillPaused := len(rt.paused) == 1
        rt.mu.Unlock()
        require.True(t, stillPaused, "all-mode wait must stay paused until every type delivers")

        // Duplicate delivery must not resume early.
        ms.Emit("evt:a", nil)
        time.Sleep(50 * time.Millisecond)
        rt.mu.Lock()
        stillPaused = len(rt.paused) == 1
        rt.mu.Unlock()
        require.True(t, stillPaused, "duplicate delivery must not resume an all-mode wait")

        // Second distinct type completes the set.
        ms.Emit("evt:b", nil)
        select {
        case res := <-done:
                require.Equal(t, "succeeded", res.Status)
        case <-time.After(5 * time.Second):
                t.Fatal("all-mode wait did not resume when the set completed")
        }
}
