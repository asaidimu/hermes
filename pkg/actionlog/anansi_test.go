package actionlog

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/asaidimu/go-anansi/v8/core/persistence/base"
	pevents "github.com/asaidimu/go-anansi/v8/core/persistence/events"
	"github.com/asaidimu/go-anansi/v8/core/persistence/persistence"
	"github.com/asaidimu/go-anansi/v8/core/query/native"
	sqliteExecutor "github.com/asaidimu/go-anansi/v8/sqlite/executor"
	sqliteQuery "github.com/asaidimu/go-anansi/v8/sqlite/query"
	rootutils "github.com/asaidimu/go-anansi/v8/utils"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// newLogFactory spins up an AnansiStoreFactory over a unique in-memory
// sqlite database.
func newLogFactory(t *testing.T) (*AnansiStoreFactory, func()) {
	t.Helper()

	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())
	db, err := sql.Open("sqlite3", dsn)
	require.NoError(t, err)

	logger := zap.NewNop()
	exec, err := sqliteExecutor.NewSQLiteExecutor(db, logger)
	require.NoError(t, err)
	queryFactory := sqliteQuery.NewSQLiteFactory(nil)
	interactor, err := native.NewNativeInteractor(exec, queryFactory, logger)
	require.NoError(t, err)

	bus, err := rootutils.NewInMemoryGoEventsBus(t.Name())
	require.NoError(t, err)
	p, err := persistence.NewPersistence(interactor, pevents.NewGoEventsBusAdapter[base.PersistenceEvent](bus), logger, nil)
	require.NoError(t, err)

	factory, err := NewAnansiStoreFactory(context.Background(), p, "actionlog")
	require.NoError(t, err)

	return factory, func() { _ = db.Close() }
}

func TestAnansiStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	factory, cleanup := newLogFactory(t)
	defer cleanup()

	// Append through one multi-run store instance. Entries carry their
	// own identity — the store stamps nothing.
	writer := factory.NewStore()
	seq1, err := writer.Append(ctx, Entry{RunID: "run-1", RerunIndex: 0, Kind: KindPipelineStarted, Type: "pipeline:start"})
	require.NoError(t, err)
	require.Equal(t, uint64(1), seq1)
	_, err = writer.Append(ctx, Entry{
		RunID:      "run-1",
		RerunIndex: 0,
		Kind:       KindStepCompleted,
		Type:       "step:success",
		StepID:     "s1",
		Output:     json.RawMessage(`{"total":42}`),
	})
	require.NoError(t, err)

	// Read back through the same shared instance — proves immediate
	// persistence and that reads address documents by (runID, rerunIndex).
	reader := factory.NewStore()
	entries, err := reader.All(ctx, "run-1", 0)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Equal(t, uint64(1), entries[0].Seq)
	require.Equal(t, KindPipelineStarted, entries[0].Kind)
	require.Equal(t, "run-1", entries[0].RunID)
	require.Equal(t, 0, entries[0].RerunIndex)
	require.JSONEq(t, `{"total":42}`, string(entries[1].Output))

	// Rerun attempts are isolated documents.
	retryRun := "run-1"
	_, err = writer.Append(ctx, Entry{RunID: retryRun, RerunIndex: 1, Kind: KindPipelineStarted, Type: "pipeline:start"})
	require.NoError(t, err)

	first, err := reader.All(ctx, "run-1", 0)
	require.NoError(t, err)
	require.Len(t, first, 2, "rerun must not touch the first attempt's log")
	second, err := reader.All(ctx, "run-1", 1)
	require.NoError(t, err)
	require.Len(t, second, 1)

	idx, err := reader.LatestRerunIndex(ctx, "run-1")
	require.NoError(t, err)
	require.Equal(t, 1, idx)

	idx, err = reader.LatestRerunIndex(ctx, "unknown")
	require.NoError(t, err)
	require.Equal(t, -1, idx)
}

// TestLogDocIDDeterministic pins the detachment contract: document identity
// is a pure function of the run identity — stable across processes — and
// satisfies anansi's UUIDv7-shaped document ID constraint.
func TestLogDocIDDeterministic(t *testing.T) {
	a := logDocID("run-1", 0)
	b := logDocID("run-1", 0)
	require.Equal(t, a, b, "same (runID, rerunIndex) must always map to the same document")
	require.NotEqual(t, a, logDocID("run-1", 1))
	require.NotEqual(t, a, logDocID("run-2", 0))
	require.Len(t, a, 32, "anansi requires 32-hex document IDs")
	_, err := uuid.Parse(a)
	require.NoError(t, err)
}

// TestAnansiStoreServesMultipleRuns is the regression test for
// #review-20260910-014: ONE store instance — the shape the runtime's
// Options.ActionLog takes — must keep concurrent runs' logs isolated and
// must preserve each entry's own RunID instead of force-rewriting it to a
// store-level scope.
func TestAnansiStoreServesMultipleRuns(t *testing.T) {
	ctx := context.Background()
	factory, cleanup := newLogFactory(t)
	defer cleanup()

	// A single shared store, exactly how WorkflowRuntime consumes it.
	shared := factory.NewStore()

	// Interleave appends across two runs and two rerun attempts.
	_, err := shared.Append(ctx, Entry{RunID: "run-a", RerunIndex: 0, Kind: KindPipelineStarted, Type: "pipeline:start"})
	require.NoError(t, err)
	_, err = shared.Append(ctx, Entry{RunID: "run-b", RerunIndex: 0, Kind: KindPipelineStarted, Type: "pipeline:start"})
	require.NoError(t, err)
	_, err = shared.Append(ctx, Entry{RunID: "run-a", RerunIndex: 0, Kind: KindStepCompleted, StepID: "a1"})
	require.NoError(t, err)
	_, err = shared.Append(ctx, Entry{RunID: "run-b", RerunIndex: 0, Kind: KindStepCompleted, StepID: "b1"})
	require.NoError(t, err)
	_, err = shared.Append(ctx, Entry{RunID: "run-a", RerunIndex: 1, Kind: KindResumed, Type: "pipeline:resumed"})
	require.NoError(t, err)

	// Each run's log is isolated and carries its OWN identity — the old
	// scoped store rewrote every entry to its constructor scope.
	for _, tc := range []struct {
		runID      string
		rerun      int
		wantCount  int
		wantStepID string
	}{
		{runID: "run-a", rerun: 0, wantCount: 2, wantStepID: "a1"},
		{runID: "run-b", rerun: 0, wantCount: 2, wantStepID: "b1"},
		{runID: "run-a", rerun: 1, wantCount: 1},
	} {
		entries, err := shared.All(ctx, tc.runID, tc.rerun)
		require.NoError(t, err, tc.runID)
		require.Len(t, entries, tc.wantCount, tc.runID)
		for _, e := range entries {
			require.Equal(t, tc.runID, e.RunID, "entry identity must be preserved, not rewritten to a store scope")
			require.Equal(t, tc.rerun, e.RerunIndex)
		}
		// Seq is contiguous per (runID, rerunIndex) document.
		for i, e := range entries {
			require.Equal(t, uint64(i+1), e.Seq)
		}
	}

	count, err := shared.Count(ctx, "run-b", 0)
	require.NoError(t, err)
	require.Equal(t, uint64(2), count)

	// Cross-run reads never leak: querying run-b's attempt-1 document
	// (never written) returns nothing even though run-b attempt-0 exists.
	entries, err := shared.All(ctx, "run-b", 1)
	require.NoError(t, err)
	require.Empty(t, entries)

	// Identity validation: an entry without a RunID is rejected instead of
	// being silently merged somewhere.
	_, err = shared.Append(ctx, Entry{Kind: KindPipelineStarted})
	require.Error(t, err)
	_, err = shared.Append(ctx, Entry{RunID: "run-a", RerunIndex: -1, Kind: KindPipelineStarted})
	require.Error(t, err)

	// Seq numbering is stable: it derives from the max recorded seq of the
	// document, not the entry count, so it cannot regress or collide.
	last, err := shared.Append(ctx, Entry{RunID: "run-a", RerunIndex: 0, Kind: KindStepCompleted, StepID: "a2"})
	require.NoError(t, err)
	require.Equal(t, uint64(3), last)

	// Fresh store instances over the same collection see the same durable
	// documents — document identity lives in the data, not the handle.
	fresh := factory.NewStore()
	entries, err = fresh.All(ctx, "run-a", 0)
	require.NoError(t, err)
	require.Len(t, entries, 3)
}
