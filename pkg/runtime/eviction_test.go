package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/asaidimu/hermes/pkg/effect"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/asaidimu/hermes/pkg/timeline"
	"github.com/stretchr/testify/require"
)

// evictionWorkflow is a minimal one-stage workflow recording state["done"].
func evictionWorkflow() *pipeline.Workflow {
	def := pipeline.PipelineDefinition{
		ID:    "eviction-pipeline",
		Label: "Eviction Pipeline",
		Stages: []pipeline.Stage{
			{
				ID: "stage:done", Label: "Done",
				Steps: map[string]pipeline.Step{
					"step:done": {ID: "step:done", Effect: int(effect.SideEffecting), Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
						return store.SetValue("done", true), nil
					}},
				},
			},
		},
	}
	return &pipeline.Workflow{
		ID:    "wf-eviction",
		Label: "Eviction",
		Pipelines: map[string]pipeline.PipelineDefinition{
			"trigger:manual:Run": def,
		},
		Triggers: map[string]pipeline.WorkflowTrigger{
			"trigger:manual:Run": {ID: "trigger:manual:Run", Event: ManualEvent},
		},
	}
}

// TestTerminalRunBookkeepingEvictedByJanitor covers the resolved
// #review-20260910-004: per-run bookkeeping (stores, rerunIdx) was never
// evicted, leaking one full state map per run forever. A terminal run's
// store must survive an immediate post-completion read (the documented
// rt.Store(runID) pattern and the #review-20260910-002 canonical-store
// contract) but be gone once the grace horizon passes, while the outcome
// stays readable. Audit history (runMetas/outcomes) is only purged when an
// explicit RunHistoryTTL is configured.
func TestTerminalRunBookkeepingEvictedByJanitor(t *testing.T) {
	ms := NewManualEventSource()
	rt := NewWorkflowRuntime(Options{
		EventSource: ms,
		// RunHistoryTTL unset: stores evicted after the default grace;
		// history kept forever.
	})
	defer rt.Shutdown(context.Background())

	done := make(chan RunResult, 1)
	require.NoError(t, rt.Register(evictionWorkflow(), RegisterOptions{
		Mode:       Mode{Type: "transient"},
		OnComplete: func(r RunResult) { done <- r },
	}))
	rt.Bus().Emit(context.Background(), ManualEvent, events.PipelineEvent{Payload: map[string]any{}})

	var res RunResult
	select {
	case res = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for run")
	}
	require.True(t, res.OK, "run failed: %v", res.Error)

	// Immediately after completion the store is still reachable (grace
	// window) — the documented post-completion read pattern.
	st := rt.Store(res.RunID)
	require.NotNil(t, st, "store must survive the grace window for post-completion reads")

	// Backdate the run's EndTime past the default store grace, then run the
	// janitor sweep directly.
	rt.mu.Lock()
	meta := rt.runMetas[res.RunID]
	require.NotNil(t, meta)
	old := time.Now().Add(-defaultStoreGrace - time.Minute).UnixMilli()
	meta.EndTime = &old
	rt.mu.Unlock()
	rt.purgeExpiredRuns()

	rt.mu.Lock()
	_, storeLeft := rt.stores[res.RunID]
	_, rerunLeft := rt.rerunIdx[res.RunID]
	metaLeft := rt.runMetas[res.RunID]
	rt.mu.Unlock()
	require.False(t, storeLeft, "store must be evicted after the grace horizon")
	require.False(t, rerunLeft, "rerun index must be evicted after the grace horizon")
	require.NotNil(t, metaLeft, "audit history (runMetas) must be kept when no RunHistoryTTL is set")

	// The outcome remains readable after eviction: RunResult carries the
	// final state, so eviction does not blind the host to what happened.
	outcome, ok := rt.GetRunOutcome(res.RunID)
	require.True(t, ok, "outcome must remain readable after store eviction")
	require.True(t, outcome.FinalState["done"] == true, "outcome must carry the final state")
}

// TestHistoryEvictedWithExplicitTTL covers tier 2 of the resolved
// #review-20260910-004: with Options.RunHistoryTTL set, audit history
// (runMetas/outcomes) is purged too, once the TTL passes.
func TestHistoryEvictedWithExplicitTTL(t *testing.T) {
	ms := NewManualEventSource()
	rt := NewWorkflowRuntime(Options{
		EventSource:   ms,
		RunHistoryTTL: time.Hour,
	})
	defer rt.Shutdown(context.Background())

	done := make(chan RunResult, 1)
	require.NoError(t, rt.Register(evictionWorkflow(), RegisterOptions{
		Mode:       Mode{Type: "transient"},
		OnComplete: func(r RunResult) { done <- r },
	}))
	rt.Bus().Emit(context.Background(), ManualEvent, events.PipelineEvent{Payload: map[string]any{}})

	var res RunResult
	select {
	case res = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for run")
	}
	require.True(t, res.OK, "run failed: %v", res.Error)

	// Backdate beyond the TTL and sweep.
	rt.mu.Lock()
	meta := rt.runMetas[res.RunID]
	require.NotNil(t, meta)
	old := time.Now().Add(-time.Hour - time.Minute).UnixMilli()
	meta.EndTime = &old
	rt.mu.Unlock()
	rt.purgeExpiredRuns()

	rt.mu.Lock()
	_, storeLeft := rt.stores[res.RunID]
	_, metaLeft := rt.runMetas[res.RunID]
	_, outcomeLeft := rt.outcomes[res.RunID]
	rt.mu.Unlock()
	require.False(t, storeLeft, "store must be evicted after the TTL")
	require.False(t, metaLeft, "runMetas must be evicted when RunHistoryTTL is set")
	require.False(t, outcomeLeft, "outcomes must be evicted when RunHistoryTTL is set")

	// A paused (or otherwise non-terminal) run is never touched by the
	// janitor even when backdated.
	rt.mu.Lock()
	rt.runMetas["paused-run"] = &timeline.RunTimelineMeta{
		RunID:     "paused-run",
		Status:    timeline.StatusPaused,
		StartTime: time.Now().Add(-2 * time.Hour).UnixMilli(),
	}
	rt.stores["paused-run"] = store.NewMemoryStore(nil)
	rt.mu.Unlock()
	rt.purgeExpiredRuns()
	rt.mu.Lock()
	_, pausedStoreLeft := rt.stores["paused-run"]
	rt.mu.Unlock()
	require.True(t, pausedStoreLeft, "paused runs must never be evicted")
}
