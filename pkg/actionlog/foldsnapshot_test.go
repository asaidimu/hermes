package actionlog

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// H2.1 acceptance: the FoldSnapshotStore capability on both backends.
// Contract: upsert-by-(runID, rerunIndex) — latest pause wins; absent load
// is (nil, nil), never an error; snapshots are invisible to All/Count/Runs.

func requireSnapshotContract(t *testing.T, store interface {
	SaveFoldSnapshot(ctx context.Context, snap FoldSnapshot) error
	LoadFoldSnapshot(ctx context.Context, runID string, rerunIndex int) (*FoldSnapshot, error)
	All(ctx context.Context, runID string, rerunIndex int) ([]Entry, error)
	Count(ctx context.Context, runID string, rerunIndex int) (uint64, error)
}) {
	t.Helper()
	ctx := context.Background()

	// Absent load: (nil, nil).
	snap, err := store.LoadFoldSnapshot(ctx, "run-1", 0)
	require.NoError(t, err)
	require.Nil(t, snap, "absent snapshot must be nil,nil, not an error")

	// Save → load round-trip.
	require.NoError(t, store.SaveFoldSnapshot(ctx, FoldSnapshot{
		RunID: "run-1", RerunIndex: 0, Seq: 42, DefHash: "abc123",
		State: []byte(`{"x":1}`),
	}))
	snap, err = store.LoadFoldSnapshot(ctx, "run-1", 0)
	require.NoError(t, err)
	require.NotNil(t, snap)
	require.Equal(t, uint64(42), snap.Seq)
	require.Equal(t, "abc123", snap.DefHash)
	require.JSONEq(t, `{"x":1}`, string(snap.State))

	// Attempts are isolated.
	snap, err = store.LoadFoldSnapshot(ctx, "run-1", 1)
	require.NoError(t, err)
	require.Nil(t, snap)

	// Latest pause overwrites (one snapshot per attempt).
	require.NoError(t, store.SaveFoldSnapshot(ctx, FoldSnapshot{
		RunID: "run-1", RerunIndex: 0, Seq: 87, DefHash: "abc123",
		State: []byte(`{"x":2,"y":3}`),
	}))
	snap, err = store.LoadFoldSnapshot(ctx, "run-1", 0)
	require.NoError(t, err)
	require.NotNil(t, snap)
	require.Equal(t, uint64(87), snap.Seq)
	require.JSONEq(t, `{"x":2,"y":3}`, string(snap.State))

	// Snapshots are invisible to the entry surface.
	require.NoError(t, store.SaveFoldSnapshot(ctx, FoldSnapshot{
		RunID: "run-other", RerunIndex: 0, Seq: 5, State: []byte(`{}`),
	}))
	entries, err := store.All(ctx, "run-other", 0)
	require.NoError(t, err)
	require.Empty(t, entries, "snapshots must never surface through All")
	n, err := store.Count(ctx, "run-other", 0)
	require.NoError(t, err)
	require.Zero(t, n, "snapshots must never surface through Count")

	// Empty RunID rejected loudly.
	require.Error(t, store.SaveFoldSnapshot(ctx, FoldSnapshot{RerunIndex: 0}))
}

func TestFoldSnapshotMemory(t *testing.T) {
	requireSnapshotContract(t, NewMemoryActionLog())
}

func TestFoldSnapshotAnansi(t *testing.T) {
	store, cleanup := newLogFactory(t)
	defer cleanup()
	requireSnapshotContract(t, store)
}

// TestFoldSnapshotAnansiPersistsAcrossReopen: the snapshot survives losing
// the RAM state — reopen the same sqlite file (shared-cache memory DSN
// keyed by test name; use a second store over the SAME db) and load.
func TestFoldSnapshotAnansiPersistsAcrossReopen(t *testing.T) {
	store, cleanup := newLogFactory(t)
	ctx := context.Background()
	require.NoError(t, store.SaveFoldSnapshot(ctx, FoldSnapshot{
		RunID: "run-9", RerunIndex: 3, Seq: 12, DefHash: "h", State: []byte(`{"a":true}`),
	}))
	_ = cleanup

	// Same DSN → same underlying sqlite (cache=shared). A fresh store
	// instance must find the persisted snapshot.
	store2, cleanup2 := newLogFactory(t)
	defer cleanup2()
	snap, err := store2.LoadFoldSnapshot(ctx, "run-9", 3)
	require.NoError(t, err)
	require.NotNil(t, snap)
	require.Equal(t, uint64(12), snap.Seq)
}
