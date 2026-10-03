package actionlog

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Characterization tests pinning the action log's sequence-numbering
// behavior (refactor plan P0.2c). The contract the Replayer relies on:
//
//   - Seq numbering is scoped per (runID, rerunIndex) — two runs, and two
//     attempts of one run, number independently from 1.
//   - Appends continue from the highest recorded Seq (MemoryActionLog keeps
//     an explicit per-(run, index) counter; AnansiStore derives max+1 per
//     document — anansi.go's Append loop), NOT from a global counter.
//   - Entries without a RunID are rejected by AnansiStore (identity comes
//     from the entry, not the store).

// TestMemoryLogSeqPerRunAndAttempt pins that sequence numbers restart at 1
// for every (runID, rerunIndex) pair — the property allEntries' re-baseline
// stitching depends on.
func TestMemoryLogSeqPerRunAndAttempt(t *testing.T) {
	ctx := context.Background()
	log := NewMemoryActionLog()

	for i := 0; i < 3; i++ {
		seq, err := log.Append(ctx, Entry{RunID: "run-a", RerunIndex: 0, Kind: KindStepStarted, StepID: "s"})
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), seq)
	}
	// Interleaved second run numbers independently.
	seq, err := log.Append(ctx, Entry{RunID: "run-b", RerunIndex: 0, Kind: KindStepStarted})
	require.NoError(t, err)
	require.Equal(t, uint64(1), seq, "second run's seq starts at 1")

	// New attempt of run-a restarts at 1 too.
	seq, err = log.Append(ctx, Entry{RunID: "run-a", RerunIndex: 1, Kind: KindResumed})
	require.NoError(t, err)
	require.Equal(t, uint64(1), seq, "new attempt's seq restarts at 1")

	// Latest attempt 0 seq continues (not reset by the interleaves).
	seq, err = log.Append(ctx, Entry{RunID: "run-a", RerunIndex: 0, Kind: KindStepCompleted, StepID: "s"})
	require.NoError(t, err)
	require.Equal(t, uint64(4), seq, "attempt 0's counter unaffected by other (run, index) pairs")
}

// TestMemoryLogLatestRerunIndex tracks the highest index that has entries.
func TestMemoryLogLatestRerunIndex(t *testing.T) {
	ctx := context.Background()
	log := NewMemoryActionLog()

	idx, err := log.LatestRerunIndex(ctx, "ghost")
	require.NoError(t, err)
	require.Equal(t, -1, idx)

	_, err = log.Append(ctx, Entry{RunID: "run-a", RerunIndex: 0})
	require.NoError(t, err)
	_, err = log.Append(ctx, Entry{RunID: "run-a", RerunIndex: 2})
	require.NoError(t, err)
	idx, err = log.LatestRerunIndex(ctx, "run-a")
	require.NoError(t, err)
	require.Equal(t, 2, idx, "highest index with entries wins (gaps allowed)")
}

// TestAnansiStoreIdentityAndSeqContract pins the AnansiStore behaviors the
// refactor must preserve through the P4.1 storage-layout change:
//
//  1. Entries without RunID are rejected (identity comes from the entry).
//  2. Negative RerunIndex is rejected.
//  3. Seq numbering is per (runID, rerunIndex) document and continues from
//     the document's highest recorded Seq.
//  4. One store instance serves multiple runs without cross-talk.
func TestAnansiStoreIdentityAndSeqContract(t *testing.T) {
	ctx := context.Background()
	logStore, cleanup := newLogFactory(t)
	defer cleanup()

	store := logStore

	// 1. Empty RunID rejected.
	_, err := store.Append(ctx, Entry{Kind: KindPipelineStarted})
	require.Error(t, err, "entry without RunID must be rejected")

	// 2. Negative rerun index rejected.
	_, err = store.Append(ctx, Entry{RunID: "run-x", RerunIndex: -1})
	require.Error(t, err, "negative RerunIndex must be rejected")

	// 3+4. Two runs through one store: independent per-document seq.
	for i := 0; i < 2; i++ {
		seq, err := store.Append(ctx, Entry{RunID: "run-1", RerunIndex: 0, Kind: KindStepStarted})
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), seq)
	}
	seq, err := store.Append(ctx, Entry{RunID: "run-2", RerunIndex: 0, Kind: KindStepStarted})
	require.NoError(t, err)
	require.Equal(t, uint64(1), seq, "second run independent of the first")

	// Attempt 1 of run-1 restarts numbering.
	seq, err = store.Append(ctx, Entry{RunID: "run-1", RerunIndex: 1, Kind: KindResumed})
	require.NoError(t, err)
	require.Equal(t, uint64(1), seq)

	entries, err := store.All(ctx, "run-1", 0)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Equal(t, uint64(1), entries[0].Seq)
	require.Equal(t, uint64(2), entries[1].Seq)

	// LatestRerunIndex honors per-run documents.
	idx, err := store.LatestRerunIndex(ctx, "run-1")
	require.NoError(t, err)
	require.Equal(t, 1, idx)
	idx, err = store.LatestRerunIndex(ctx, "run-2")
	require.NoError(t, err)
	require.Equal(t, 0, idx)
	idx, err = store.LatestRerunIndex(ctx, "never-appended")
	require.NoError(t, err)
	require.Equal(t, -1, idx)
}
