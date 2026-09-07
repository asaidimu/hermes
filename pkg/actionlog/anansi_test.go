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

	// Append through one store instance.
	writer := factory.NewStore("run-1", 0)
	seq1, err := writer.Append(ctx, Entry{Kind: KindPipelineStarted, Type: "pipeline:start"})
	require.NoError(t, err)
	require.Equal(t, uint64(1), seq1)
	_, err = writer.Append(ctx, Entry{
		Kind:   KindStepCompleted,
		Type:   "step:success",
		StepID: "s1",
		Output: json.RawMessage(`{"total":42}`),
	})
	require.NoError(t, err)

	// Read back through a fresh instance — proves immediate persistence.
	reader := factory.NewStore("run-1", 0)
	entries, err := reader.All(ctx, "run-1", 0)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Equal(t, uint64(1), entries[0].Seq)
	require.Equal(t, KindPipelineStarted, entries[0].Kind)
	require.Equal(t, "run-1", entries[0].RunID)
	require.Equal(t, 0, entries[0].RerunIndex)
	require.JSONEq(t, `{"total":42}`, string(entries[1].Output))

	// Rerun attempts are isolated documents.
	retry := factory.NewStore("run-1", 1)
	_, err = retry.Append(ctx, Entry{Kind: KindPipelineStarted, Type: "pipeline:start"})
	require.NoError(t, err)

	first, err := reader.All(ctx, "run-1", 0)
	require.NoError(t, err)
	require.Len(t, first, 2, "rerun must not touch the first attempt's log")
	second, err := retry.All(ctx, "run-1", 1)
	require.NoError(t, err)
	require.Len(t, second, 1)

	idx, err := factory.NewStore("run-1", 0).LatestRerunIndex(ctx, "run-1")
	require.NoError(t, err)
	require.Equal(t, 1, idx)

	idx, err = factory.NewStore("x", 0).LatestRerunIndex(ctx, "unknown")
	require.NoError(t, err)
	require.Equal(t, -1, idx)
}
