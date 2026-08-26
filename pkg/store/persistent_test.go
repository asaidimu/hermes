package store_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/asaidimu/go-anansi/v8/core/persistence/base"
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

// newRunsFactory spins up an AnansiStoreFactory over a unique in-memory sqlite
// database. The runs collection schema is derived from store.PipelineState by
// the factory itself.
func newRunsFactory(t *testing.T) (*store.AnansiStoreFactory, base.Persistence, func()) {
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

	factory, err := store.NewAnansiStoreFactory(context.Background(), p, "runs")
	require.NoError(t, err)

	return factory, p, func() { _ = db.Close() }
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
	factory, _, cleanup := newRunsFactory(t)
	defer cleanup()

	// Mint a run and seed state. The first write-through inserts the doc.
	created, err := factory.Create(ctx)
	require.NoError(t, err)
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

	loaded, err := factory.Load(ctx, runID)
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
	rows, err := factory.Models().Read(ctx, ptrQuery(query.NewQueryBuilder().Where("_id_").Eq(runID).Build()))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	row := rows[0]
	require.Equal(t, runID, row.GetID())
	require.InDelta(t, 11.0, toFloat(row.Data["total"]), 0.0001)
	require.Equal(t, "wf-1", row.RunInfo.WorkflowID)
	rowJSON, _ := json.Marshal(row.Metadata)
	require.NotContains(t, string(rowJSON), pipeline.PipelineDataKey,
		"checkpoints must live in the state column, not system metadata")

	// Further writes update in place — still exactly one document.
	require.NoError(t, loaded.Update(ctx, store.SetValue("total", 20.0)))
	reloaded, err := factory.Load(ctx, runID)
	require.NoError(t, err)
	require.NoError(t, reloaded.Read(func(state map[string]any) error {
		require.InDelta(t, 20.0, toFloat(state["total"]), 0.0001)
		return nil
	}))

	all, err := factory.Models().Read(ctx, &query.Query{})
	require.NoError(t, err)
	require.Len(t, all, 1, "writes must update in place, not duplicate documents")
}

func TestLoadUnknownRunFails(t *testing.T) {
	ctx := context.Background()
	factory, _, cleanup := newRunsFactory(t)
	defer cleanup()

	_, err := factory.Load(ctx, "00000000000000000000000000000000")
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
