package actionlog

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// H3 acceptance: the retention contract.
//
// PruneRuns deletes WHOLE runs whose latest attempt ended TERMINALLY before
// the cutoff — paused and in-flight runs are never touched, fresh terminal
// runs are kept, and pruned runs' fold snapshots go with them.

func TestPruneRunsAnansi(t *testing.T) {
	store, cleanup := newLogFactory(t)
	defer cleanup()
	ctx := context.Background()

	// run-old: terminal (completed) 2h ago → pruned.
	old := time.Now().Add(-2 * time.Hour)
	for i, kind := range []EntryKind{KindPipelineStarted, KindPipelineCompleted} {
		_, err := store.Append(ctx, Entry{RunID: "run-old", RerunIndex: 0, Kind: kind, Timestamp: old.Add(time.Duration(i) * time.Second)})
		require.NoError(t, err)
	}
	require.NoError(t, store.SaveFoldSnapshot(ctx, FoldSnapshot{RunID: "run-old", RerunIndex: 0, Seq: 2, State: []byte(`{}`)}))

	// run-paused: last entry is a checkpoint, 2h ago → NEVER pruned.
	for i, kind := range []EntryKind{KindPipelineStarted, KindCheckpoint} {
		_, err := store.Append(ctx, Entry{RunID: "run-paused", RerunIndex: 0, Kind: kind, Timestamp: old.Add(time.Duration(i) * time.Second)})
		require.NoError(t, err)
	}
	require.NoError(t, store.SaveFoldSnapshot(ctx, FoldSnapshot{RunID: "run-paused", RerunIndex: 0, Seq: 2, State: []byte(`{}`)}))

	// run-fresh: terminal 1 minute ago → kept.
	fresh := time.Now().Add(-time.Minute)
	for i, kind := range []EntryKind{KindPipelineStarted, KindPipelineFailed} {
		_, err := store.Append(ctx, Entry{RunID: "run-fresh", RerunIndex: 0, Kind: kind, Timestamp: fresh.Add(time.Duration(i) * time.Second)})
		require.NoError(t, err)
	}

	// run-inflight: last entry is a step, 2h ago → never pruned.
	for i, kind := range []EntryKind{KindPipelineStarted, KindStepStarted} {
		_, err := store.Append(ctx, Entry{RunID: "run-inflight", RerunIndex: 0, Kind: kind, Timestamp: old.Add(time.Duration(i) * time.Second)})
		require.NoError(t, err)
	}

	n, err := store.PruneRuns(ctx, time.Now().Add(-time.Hour))
	require.NoError(t, err)
	require.Equal(t, 1, n, "exactly the old terminal run is pruned")

	// Pruned: entries AND snapshot gone.
	entries, err := store.All(ctx, "run-old", 0)
	require.NoError(t, err)
	require.Empty(t, entries)
	snap, err := store.LoadFoldSnapshot(ctx, "run-old", 0)
	require.NoError(t, err)
	require.Nil(t, snap, "pruned run's snapshot goes with it")

	// Paused run and its snapshot survive.
	entries, err = store.All(ctx, "run-paused", 0)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	snap, err = store.LoadFoldSnapshot(ctx, "run-paused", 0)
	require.NoError(t, err)
	require.NotNil(t, snap)

	// Fresh terminal and in-flight runs survive.
	for _, id := range []string{"run-fresh", "run-inflight"} {
		entries, err = store.All(ctx, id, 0)
		require.NoError(t, err)
		require.NotEmpty(t, entries, "%s must survive", id)
	}
}

func TestStorageUsageBackends(t *testing.T) {
	ctx := context.Background()

	// Memory backend.
	mem := NewMemoryActionLog()
	_, err := mem.Append(ctx, Entry{RunID: "r1", RerunIndex: 0, Kind: KindPipelineStarted})
	require.NoError(t, err)
	_, err = mem.Append(ctx, Entry{RunID: "r1", RerunIndex: 0, Kind: KindStepCompleted})
	require.NoError(t, err)
	_, err = mem.Append(ctx, Entry{RunID: "r2", RerunIndex: 0, Kind: KindPipelineStarted})
	require.NoError(t, err)
	usage, err := mem.StorageUsage(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(2), usage.Runs)
	require.Equal(t, int64(3), usage.Entries)
	require.Positive(t, usage.Bytes)

	// Anansi backend.
	anansi, cleanup := newLogFactory(t)
	defer cleanup()
	_, err = anansi.Append(ctx, Entry{RunID: "r1", RerunIndex: 0, Kind: KindPipelineStarted})
	require.NoError(t, err)
	usage, err = anansi.StorageUsage(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), usage.Runs)
	require.Equal(t, int64(1), usage.Entries)
	require.Positive(t, usage.Bytes)
}
