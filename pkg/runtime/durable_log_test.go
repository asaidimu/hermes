package runtime

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/asaidimu/go-anansi/v8/core/persistence/base"
	pevents "github.com/asaidimu/go-anansi/v8/core/persistence/events"
	"github.com/asaidimu/go-anansi/v8/core/persistence/persistence"
	"github.com/asaidimu/go-anansi/v8/core/query/native"
	sqliteExecutor "github.com/asaidimu/go-anansi/v8/sqlite/executor"
	sqliteQuery "github.com/asaidimu/go-anansi/v8/sqlite/query"
	rootutils "github.com/asaidimu/go-anansi/v8/utils"
	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/effect"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/replay"
	"github.com/asaidimu/hermes/pkg/store"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// newDurableActionLog spins up a multi-run AnansiStore over a unique
// in-memory sqlite database — the same harness pkg/actionlog tests use, but
// returned as the Store a host would wire into Options.ActionLog.
func newDurableActionLog(t *testing.T) (actionlog.Store, func()) {
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

	factory, err := actionlog.NewAnansiStoreFactory(context.Background(), p, "actionlog")
	require.NoError(t, err)

	// One scope-free store for the whole runtime — the integration shape
	// #review-20260910-014 called for.
	return factory.NewStore(), func() { _ = db.Close() }
}

// durableEchoWorkflow is a minimal one-stage workflow whose single
// side-effecting step records state["done"]=true.
func durableEchoWorkflow() *pipeline.Workflow {
	def := pipeline.PipelineDefinition{
		ID:    "durable-log-pipeline",
		Label: "Durable Log Pipeline",
		Stages: []pipeline.Stage{
			{
				ID: "stage:echo", Label: "Echo",
				Steps: map[string]pipeline.Step{
					"step:echo": {ID: "step:echo", Effect: int(effect.SideEffecting), Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
						return store.SetValue("done", true), nil
					}},
				},
			},
		},
	}
	return &pipeline.Workflow{
		ID:    "wf-durable-log",
		Label: "Durable Log",
		Pipelines: map[string]pipeline.PipelineDefinition{
			"trigger:manual:Run": def,
		},
		Triggers: map[string]pipeline.WorkflowTrigger{
			"trigger:manual:Run": {ID: "trigger:manual:Run", Event: ManualEvent},
		},
	}
}

// TestDurableActionLogServesMultiRunRuntime covers #review-20260910-014
// end-to-end: ONE durable AnansiStore wired as the runtime's single
// Options.ActionLog must keep every run's log isolated under its own
// identity (never rewritten to a store-level scope), and the Replayer must
// be able to rebuild a run's state from that shared durable log.
func TestDurableActionLogServesMultiRunRuntime(t *testing.T) {
	ctx := context.Background()
	durableLog, cleanup := newDurableActionLog(t)
	defer cleanup()

	ms := NewManualEventSource()
	rt := NewWorkflowRuntime(Options{
		ActionLog:   durableLog,
		EventSource: ms,
	})
	defer rt.Shutdown(context.Background())

	var results []RunResult
	done := make(chan RunResult, 4)
	err := rt.Register(durableEchoWorkflow(), RegisterOptions{
		Mode:       Mode{Type: "transient"},
		OnComplete: func(r RunResult) { done <- r },
	})
	require.NoError(t, err)

	// Two runs through the SAME runtime sharing ONE durable store.
	const runs = 2
	for i := 0; i < runs; i++ {
		rt.Bus().Emit(ctx, ManualEvent, events.PipelineEvent{Payload: map[string]any{}})
	}
	for i := 0; i < runs; i++ {
		select {
		case res := <-done:
			require.True(t, res.OK, "run %d failed: %v", i, res.Error)
			results = append(results, res)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for runs")
		}
	}
	require.Len(t, results, runs)
	require.NotEqual(t, results[0].RunID, results[1].RunID, "distinct runs must mint distinct identities")

	// Per-run isolation with identity preserved: every entry of run A
	// carries run A's own RunID at rerunIndex 0 with contiguous Seq —
	// the scoped pre-fix store would have stamped every entry with the
	// single scope it was constructed with.
	runA, runB := results[0].RunID, results[1].RunID
	lens := map[string]int{}
	for _, tc := range []struct {
		name string
		run  string
	}{
		{name: "run-a", run: runA},
		{name: "run-b", run: runB},
	} {
		entries, err := durableLog.All(ctx, tc.run, 0)
		require.NoError(t, err, tc.name)
		require.NotEmpty(t, entries, tc.name)
		lens[tc.run] = len(entries)
		for i, e := range entries {
			require.Equal(t, tc.run, e.RunID, "%s: entry identity must be the run's own, not a store scope", tc.name)
			require.Equal(t, 0, e.RerunIndex, "%s: first attempt", tc.name)
			require.Equal(t, uint64(i+1), e.Seq, "%s: Seq must be contiguous per run document", tc.name)
		}
	}

	countA, err := durableLog.Count(ctx, runA, 0)
	require.NoError(t, err)
	require.Equal(t, uint64(lens[runA]), countA)

	// The durable log is the recovery source of truth: the Replayer, fed
	// the same shared multi-run store, rebuilds run A's state.
	rp := replay.NewReplayer(durableLog, func(string) (*pipeline.PipelineDefinition, bool) {
		wf := durableEchoWorkflow()
		def := wf.Pipelines["trigger:manual:Run"]
		return &def, true
	})
	st, _, err := rp.Rebuild(ctx, runA, "durable-log-pipeline")
	require.NoError(t, err)
	require.NoError(t, st.Read(func(state map[string]any) error {
		require.Equal(t, true, state["done"], "rebuilt state must reflect the recorded effect delta")
		return nil
	}))
}
