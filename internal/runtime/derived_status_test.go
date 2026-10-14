package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/timeline"
	"github.com/stretchr/testify/require"
)

// S.2 (plan Phase 5): the action log is the authoritative record of run
// status; runMetas is a write-through cache reconciled from the log on
// cache miss. These tests pin the reconcile-on-read contract.

// TestRunMetaReconciledFromLogAfterCacheLoss completes a run, wipes the RAM
// meta (as RunHistoryTTL eviction would), and requires GetRunMeta to
// rebuild status/end time/pipeline id from the log instead of reporting
// NotFound. Before S.2 the meta read was RAM-authoritative and the run
// went blind once evicted.
func TestRunMetaReconciledFromLogAfterCacheLoss(t *testing.T) {
	ms := NewManualEventSource()
	log := actionlog.NewMemoryActionLog()
	rt := NewWorkflowRuntime(Options{
		EventSource: ms,
		ActionLog:   log,
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

	// The RAM cache has the run: status complete.
	meta, err := rt.GetRunMeta(context.Background(), res.RunID)
	require.NoError(t, err)
	require.Equal(t, timeline.StatusComplete, meta.Status)
	require.NotNil(t, meta.EndTime)

	// Simulate tier-2 eviction (RunHistoryTTL purge) of the audit history.
	rt.mu.Lock()
	delete(rt.runMetas, res.RunID)
	delete(rt.outcomes, res.RunID)
	rt.mu.Unlock()

	// Cache miss must reconcile from the log — the run stays queryable.
	meta, err = rt.GetRunMeta(context.Background(), res.RunID)
	require.NoError(t, err, "cache miss must reconcile from the log, not report NotFound")
	require.Equal(t, res.RunID, meta.RunID)
	require.Equal(t, timeline.StatusComplete, meta.Status, "terminal status must come from the log's last entry")
	require.NotNil(t, meta.EndTime)
	require.Equal(t, "eviction-pipeline", meta.PipelineID)
	require.Greater(t, meta.EventCount, int64(0), "event count must be derived from the log entries")

	// A run with no log at all still reports NotFound after reconcile.
	_, err = rt.GetRunMeta(context.Background(), "run-that-never-was")
	require.Error(t, err, "unknown run must remain NotFound")
}

// TestRerunIndexSurvivesJanitorEviction pins the S.2 deletion of
// rt.rerunIdx: the rerun index is derived from the log's
// LatestRerunIndex, so janitor eviction of the RAM map (removed) can no
// longer diverge from the log. After eviction, a forced recovery still
// gets a fresh attempt index (1, one past attempt 0).
func TestRerunIndexSurvivesJanitorEviction(t *testing.T) {
	ms := NewManualEventSource()
	log := actionlog.NewMemoryActionLog()
	rt := NewWorkflowRuntime(Options{
		EventSource: ms,
		ActionLog:   log,
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

	// Backdate past the store grace and sweep.
	rt.mu.Lock()
	meta := rt.runMetas[res.RunID]
	require.NotNil(t, meta)
	old := time.Now().Add(-defaultStoreGrace - time.Minute).UnixMilli()
	meta.EndTime = &old
	rt.mu.Unlock()
	rt.purgeExpiredRuns()

	rt.mu.Lock()
	_, storeLeft := rt.stores[res.RunID]
	rt.mu.Unlock()
	require.False(t, storeLeft, "store must be evicted after the grace horizon")
	// (No rerun-map assertion: rt.rerunIdx was DELETED in S.2 — referencing
	// it is a compile error, which is the strongest possible pin.)

	// The rerun index SURVIVES eviction because the log holds it: the next
	// attempt continues at 1 (one past attempt 0). Before S.2 the RAM map
	// was evicted here and a divergent 0 would have been served.
	require.Equal(t, 1, rt.nextRerunIndex(context.Background(), res.RunID),
		"rerun index must derive from the log's LatestRerunIndex, surviving eviction")
}
