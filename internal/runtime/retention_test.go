package runtime

// H3 acceptance (v2.1 hardening plan): retention wiring on the runtime.
// The backend prune/usage semantics live in pkg/actionlog's retention
// tests; this file pins the runtime-level contracts: StoreGrace overrides
// the tier-1 horizon, StorageUsage reports via the capability with typed
// errors, and the janitor prune path is wired to MaxLogAge.

import (
	"context"
	"testing"
	"time"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/timeline"
	"github.com/stretchr/testify/require"
)

// seedTerminatedStore registers a terminal run whose store "ended" ago.
func seedTerminatedStore(rt *WorkflowRuntime, runID string, ended time.Time) {
	rt.mu.Lock()
	rt.stores[runID] = nil // presence is what the janitor deletes
	rt.runMetas[runID] = &timeline.RunTimelineMeta{
		RunID:   runID,
		Status:  timeline.StatusComplete,
		EndTime: new(int64),
	}
	*rt.runMetas[runID].EndTime = ended.UnixMilli()
	rt.mu.Unlock()
}

func storePresent(rt *WorkflowRuntime, runID string) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	_, ok := rt.stores[runID]
	return ok
}

// TestStoreGraceOverridesTier1Horizon: a 2-minute-old terminal store is
// KEPT under the 10-minute default and EVICTED with StoreGrace=1m.
func TestStoreGraceOverridesTier1Horizon(t *testing.T) {
	ended := time.Now().Add(-2 * time.Minute)

	defaultRT := NewWorkflowRuntime(Options{ActionLog: actionlog.NewMemoryActionLog()})
	seedTerminatedStore(defaultRT, "grace-run", ended)
	defaultRT.purgeExpiredRuns()
	require.True(t, storePresent(defaultRT, "grace-run"),
		"2min-old store must survive the 10-minute default grace")
	defaultRT.Shutdown(context.Background())

	shortRT := NewWorkflowRuntime(Options{
		ActionLog:  actionlog.NewMemoryActionLog(),
		StoreGrace: time.Minute,
	})
	seedTerminatedStore(shortRT, "grace-run", ended)
	shortRT.purgeExpiredRuns()
	require.False(t, storePresent(shortRT, "grace-run"),
		"StoreGrace=1m must evict a 2-minute-old terminal store")
	shortRT.Shutdown(context.Background())
}

// TestStorageUsageCapabilityWiring: the runtime reports usage through the
// capability and typed errors without it — never a silent zero.
func TestStorageUsageCapabilityWiring(t *testing.T) {
	withLog := NewWorkflowRuntime(Options{ActionLog: actionlog.NewMemoryActionLog()})
	defer withLog.Shutdown(context.Background())
	usage, err := withLog.StorageUsage(context.Background())
	require.NoError(t, err)
	require.Zero(t, usage.Entries)
	require.Zero(t, usage.Runs)

	withoutLog := NewWorkflowRuntime(Options{})
	defer withoutLog.Shutdown(context.Background())
	_, err = withoutLog.StorageUsage(context.Background())
	require.Error(t, err, "no log configured → typed error")

	noCapability := &stubLogNoUsage{}
	ncRT := NewWorkflowRuntime(Options{ActionLog: noCapability})
	defer ncRT.Shutdown(context.Background())
	_, err = ncRT.StorageUsage(context.Background())
	require.Error(t, err, "backend without UsageReporter → typed error")
}

// TestPruneLogWiring: the janitor's prune path calls the capability when
// MaxLogAge is set, and skips it when unset or the capability is missing.
func TestPruneLogWiring(t *testing.T) {
	stub := newStubLogNoUsage()
	rt := NewWorkflowRuntime(Options{ActionLog: stub})
	defer rt.Shutdown(context.Background())

	rt.maxLogAge = 0
	rt.pruneLogIfConfigured()
	require.Zero(t, stub.pruned, "no MaxLogAge → no prune call")

	rt.maxLogAge = time.Hour
	rt.pruneLogIfConfigured()
	require.Equal(t, 1, stub.pruned, "MaxLogAge + capability → prune called with now-maxLogAge")
	require.WithinDuration(t, time.Now().Add(-time.Hour), stub.lastCutoff, 2*time.Second)
}

// stubLogNoUsage satisfies Store (+ RunEnumerator + LogPruner) but
// deliberately lacks UsageReporter: embedding the actionlog.Store
// INTERFACE contributes exactly the interface's method set — embedding
// the concrete MemoryActionLog would inherit its StorageUsage too.
type stubLogNoUsage struct {
	actionlog.Store
	pruned     int
	lastCutoff time.Time
}

func newStubLogNoUsage() *stubLogNoUsage {
	return &stubLogNoUsage{Store: actionlog.NewMemoryActionLog()}
}

func (s *stubLogNoUsage) PruneRuns(_ context.Context, olderThan time.Time) (int, error) {
	s.pruned++
	s.lastCutoff = olderThan
	return s.pruned, nil
}
