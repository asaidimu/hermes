package store_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/asaidimu/go-anansi/v8/core/persistence/base"
	anansicollection "github.com/asaidimu/go-anansi/v8/core/persistence/collection"
	pevents "github.com/asaidimu/go-anansi/v8/core/persistence/events"
	"github.com/asaidimu/go-anansi/v8/core/persistence/persistence"
	"github.com/asaidimu/go-anansi/v8/core/query"
	"github.com/asaidimu/go-anansi/v8/core/query/native"
	sqliteExecutor "github.com/asaidimu/go-anansi/v8/sqlite/executor"
	sqliteQuery "github.com/asaidimu/go-anansi/v8/sqlite/query"
	rootutils "github.com/asaidimu/go-anansi/v8/utils"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
)

// newRunsFactory provisions a run-state model collection over a unique
// in-memory sqlite database. The collection schema is derived from
// store.PipelineState by NewAnansiStateCollection itself.
func newRunsFactory(t *testing.T) (*anansicollection.ModelCollection[*store.PipelineState], base.Persistence, func()) {
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

	models, err := store.NewAnansiStateCollection(context.Background(), p, "runs")
	require.NoError(t, err)

	return models, p, func() { _ = db.Close() }
}

func TestMemoryStoreMintsUUIDv7ID(t *testing.T) {
	s := store.NewMemoryStore(nil)
	id := s.ID()
	require.Len(t, id, 32)
	require.Equal(t, byte('7'), id[12], "expected UUIDv7 version nibble")
	// Identity is stable per store and unique across stores.
	require.Equal(t, id, s.ID())
	require.NotEqual(t, id, store.NewMemoryStore(nil).ID())
}

func TestPersistentStoreMintDoesNotTouchDB(t *testing.T) {
	// A nil model collection is fine for minting: identity creation must not
	// require any database round-trip.
	s := store.NewPersistentStore(nil, nil)
	require.NotEmpty(t, s.ID())
	require.Len(t, s.ID(), 32)
}

func TestRunStateRoundTrip(t *testing.T) {
	ctx := context.Background()
	models, _, cleanup := newRunsFactory(t)
	defer cleanup()

	// Mint a run and seed state.
	created := store.NewPersistentStore(models, nil)
	runID := created.ID()

	ckpt := pipeline.PipelineCheckpoint{
		RunID:           runID,
		PipelineID:      "p1",
		PausedAtStageID: "stage-1",
		WaitForEvent:    "resume:event",
	}
	require.NoError(t, created.Update(ctx, store.SetValue("total", 11.0)))
	require.NoError(t, created.Update(ctx, func(state map[string]any) error {
		return pipeline.WriteCheckpoint(state, ckpt)
	}))
	require.NoError(t, created.Update(ctx, store.SetValue(store.RunMetaKey, map[string]any{
		"workflowId": "wf-1",
		"triggerId":  "trig-1",
		"pipelineId": "p1",
	})))

	// Sync pushes in-memory state to the database for queryability.
	// The Action Log is the durable source of truth; Sync is optional.
	require.NoError(t, created.Sync(ctx))

	loaded, err := store.NewPersistentStoreForID(ctx, models, runID)
	require.NoError(t, err)

	var total float64
	var gotCkpt *pipeline.PipelineCheckpoint
	var meta map[string]any
	require.NoError(t, loaded.Read(func(state map[string]any) error {
		total = toFloat(state["total"])
		meta, _ = state[store.RunMetaKey].(map[string]any)
		gotCkpt, _ = pipeline.ReadCheckpoint(state, "p1")
		return nil
	}))
	require.InDelta(t, 11.0, total, 0.0001)
	require.NotNil(t, gotCkpt, "checkpoint must survive persistence")
	require.Equal(t, "p1", gotCkpt.PipelineID)
	require.Equal(t, "resume:event", gotCkpt.WaitForEvent)
	require.Equal(t, runID, gotCkpt.RunID)
	require.Equal(t, "wf-1", meta["workflowId"], "run linkage must survive persistence")

	// The persisted row follows the PipelineState shape: state under the
	// "state" column, linkage under "metadata".
	rows, err := models.Read(ctx, ptrQuery(query.NewQueryBuilder().Where("_id_").Eq(runID).Build()))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	row := rows[0]
	require.Equal(t, runID, row.GetID())
	require.InDelta(t, 11.0, toFloat(row.Data["total"]), 0.0001)
	require.Equal(t, "wf-1", row.RunInfo.WorkflowID)
	rowJSON, _ := json.Marshal(row.Metadata)
	require.NotContains(t, string(rowJSON), pipeline.PipelineDataKey,
		"checkpoints must live in the state column, not system metadata")

	// Further writes update in place — Sync again to persist.
	require.NoError(t, loaded.Update(ctx, store.SetValue("total", 20.0)))
	require.NoError(t, loaded.Sync(ctx))
	reloaded, err := store.NewPersistentStoreForID(ctx, models, runID)
	require.NoError(t, err)
	require.NoError(t, reloaded.Read(func(state map[string]any) error {
		require.InDelta(t, 20.0, toFloat(state["total"]), 0.0001)
		return nil
	}))

	all, err := models.Read(ctx, &query.Query{})
	require.NoError(t, err)
	require.Len(t, all, 1, "writes must update in place, not duplicate documents")
}

func TestLoadUnknownRunFails(t *testing.T) {
	ctx := context.Background()
	models, _, cleanup := newRunsFactory(t)
	defer cleanup()

	_, err := store.NewPersistentStoreForID(ctx, models, "00000000000000000000000000000000")
	require.Error(t, err)
}

func ptrQuery(q query.Query) *query.Query { return &q }

// toFloat coerces the numeric types containers may hand back after a
// persistence round-trip.
func toFloat(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case json.Number:
		f, _ := n.Float64()
		return f
	default:
		return 0
	}
}

// TestSyncDoesNotHoldDataLockAcrossIO pins the P4.7 (H7) contract: the
// DB write runs OUTSIDE the data lock, so state reads and mutations
// proceed while a flush is stuck on the backend. The proof blocks the
// backend's write with an exclusive sqlite lock from a second
// connection (temp-file DB — real file locking, so busy_timeout
// applies), then requires an Update+Read to complete while Sync is
// still blocked.
func TestSyncDoesNotHoldDataLockAcrossIO(t *testing.T) {
	ctx := context.Background()

	// File-backed sqlite: shared-cache in-memory DBs return SQLITE_LOCKED
	// immediately without honoring busy_timeout — file locking doesn't.
	dir := t.TempDir()
	dsn := "file:" + dir + "/runs.db?_busy_timeout=10000"
	db, err := sql.Open("sqlite3", dsn)
	require.NoError(t, err)
	defer db.Close()

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
	models, err := store.NewAnansiStateCollection(ctx, p, "runs")
	require.NoError(t, err)

	s := store.NewPersistentStore(models, nil)
	require.NoError(t, s.Update(ctx, store.SetValue("k", "v")))

	// First Sync creates the document (warms the exists flag) so the
	// blocked write below goes down the Update path.
	require.NoError(t, s.Sync(ctx))

	// Second connection to the SAME file, holding the exclusive sqlite
	// write lock — the store's write blocks until we release.
	blocker, err := sql.Open("sqlite3", dsn)
	require.NoError(t, err)
	defer blocker.Close()
	_, err = blocker.Exec("BEGIN EXCLUSIVE")
	require.NoError(t, err)

	syncDone := make(chan error, 1)
	go func() { syncDone <- s.Sync(ctx) }()

	// Give the blocked Sync a moment to reach the DB write.
	time.Sleep(200 * time.Millisecond)

	// While the flush is stuck on the backend, the DATA lock must be
	// free: a mutation + read completes promptly.
	procDone := make(chan error, 1)
	go func() {
		if err := s.Update(ctx, store.SetValue("k2", "v2")); err != nil {
			procDone <- err
			return
		}
		var got any
		readErr := s.Read(func(state map[string]any) error {
			got = state["k2"]
			return nil
		})
		if readErr != nil {
			procDone <- readErr
			return
		}
		if got != "v2" {
			procDone <- fmt.Errorf("read during blocked flush saw %v", got)
			return
		}
		procDone <- nil
	}()
	select {
	case err := <-procDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("state access blocked behind a stuck flush — Sync is holding the data lock across I/O")
	}

	// Release the backend; the flush completes.
	_, err = blocker.Exec("COMMIT")
	require.NoError(t, err)
	select {
	case err := <-syncDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("sync never completed after the backend unblocked")
	}
}
