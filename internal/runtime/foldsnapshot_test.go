package runtime

// H2.2 acceptance (v2.1 hardening plan): fold snapshots.
//
// Parity contract: for a currently-paused run, the snapshot fast path must
// rebuild state byte-equal to the full walk on every key the walk produces
// (the fast path legitimately carries ADDITIONAL runtime-seeded keys —
// __trigger_event__, run linkage, resume payload — that the walk cannot
// reproduce from the log and that factory.Resume's copyStoreState overlay
// restores identically on both paths).
//
// Gate contract: stale seq, drifted fingerprint, or corrupt state all
// degrade to the walk — the result is then IDENTICAL to the walk's, never
// the degraded checkpoint seed.

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// snapshotWorkflow: set x=1 (effectful) → set p=7 (PURE — the walk
// re-executes this action; the snapshot caches its result) → wait "go".
func snapshotWorkflow(id string) *pipeline.Workflow {
	def := pipeline.PipelineDefinition{
		ID:    id + "-pipeline",
		Label: id,
		Stages: []pipeline.Stage{
			{
				ID: "stage:a",
				Steps: map[string]pipeline.Step{
					"step:a": {ID: "step:a", Effect: int(pipeline.EffectSideEffecting), Action: func(ctx context.Context, pcxt *pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
						return store.SetValue("x", float64(1)), nil
					}},
				},
			},
			{
				ID: "stage:pure",
				Steps: map[string]pipeline.Step{
					"step:p": {ID: "step:p", Effect: int(pipeline.EffectPure), Action: func(ctx context.Context, pcxt *pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
						return store.SetValue("p", float64(7)), nil
					}},
				},
			},
			{
				ID: "stage:wait",
				Router: func(ctx context.Context, state map[string]any, st store.Store) (pipeline.RoutingInstruction, error) {
					return pipeline.PauseForEvent("go", 0), nil
				},
			},
		},
	}
	return &pipeline.Workflow{
		ID:    id,
		Label: id,
		Pipelines: map[string]pipeline.PipelineDefinition{
			"trigger:manual:Run": def,
		},
		Triggers: map[string]pipeline.WorkflowTrigger{
			"trigger:manual:Run": {ID: "trigger:manual:Run", Event: ManualEvent},
		},
	}
}

// rebuildBoth captures (fast, walk) rebuild results for a paused run: the
// fast path from the live runtime, then the walk with snapshots disabled.
func rebuildBoth(t *testing.T, rt *WorkflowRuntime, runID string) (fastState, walkState map[string]any, fastAddr, walkAddr pipeline.EntryAddress) {
	t.Helper()
	ctx := context.Background()
	record := rt.workflows["drift-wf"]
	require.NotNil(t, record)
	pdef, ok := record.workflow.Pipelines["trigger:manual:Run"]
	require.True(t, ok)

	rebuild := rt.rebuildCapability(ctx, runID, record, pdef)
	require.NotNil(t, rebuild)
	stF, addrF, errF := rebuild(ctx, runID, pdef.ID)
	require.NoError(t, errF)
	require.NotNil(t, stF)

	rt.disableFoldSnapshots = true
	defer func() { rt.disableFoldSnapshots = false }()
	rebuild2 := rt.rebuildCapability(ctx, runID, record, pdef)
	require.NotNil(t, rebuild2)
	stW, addrW, errW := rebuild2(ctx, runID, pdef.ID)
	require.NoError(t, errW)
	require.NotNil(t, stW)

	fastJSON, err := stF.ExportJSON()
	require.NoError(t, err)
	walkJSON, err := stW.ExportJSON()
	require.NoError(t, err)
	return fastJSON, walkJSON, addrF, addrW
}

// requireParity: the walk's keys must be present and equal in the fast
// state; extra fast keys are the runtime-seeded ones (documented above).
func requireParity(t *testing.T, fast, walk map[string]any) {
	t.Helper()
	for k, want := range walk {
		got, ok := fast[k]
		require.True(t, ok, "fast rebuild missing walk key %q", k)
		gotJSON, err1 := json.Marshal(got)
		wantJSON, err2 := json.Marshal(want)
		require.NoError(t, err1)
		require.NoError(t, err2)
		require.JSONEq(t, string(wantJSON), string(gotJSON), "walk key %q differs on the fast path", k)
	}
	require.Contains(t, fast, "__trigger_event__", "fast rebuild carries the runtime-seeded keys the overlay restores")
	require.NotContains(t, walk, "__trigger_event__")
}

func TestFoldSnapshotParitySimplePause(t *testing.T) {
	ms := NewManualEventSource()
	rt := NewWorkflowRuntime(Options{
		ActionLog:   actionlog.NewMemoryActionLog(),
		EventSource: ms,
	})
	defer rt.Shutdown(context.Background())

	prepared := make(chan string, 1)
	require.NoError(t, rt.Register(snapshotWorkflow("drift-wf"), RegisterOptions{
		Mode:      Mode{Type: "transient"},
		OnPrepare: func(h *RunHandle) error { prepared <- h.RunID; return nil },
	}))
	rt.Bus().Emit(context.Background(), ManualEvent, events.PipelineEvent{Payload: map[string]any{}})
	runID := <-prepared
	awaitPaused(t, rt, 1)
	awaitFoldSnapshot(t, rt, runID)

	fast, walk, fastAddr, walkAddr := rebuildBoth(t, rt, runID)
	requireParity(t, fast, walk)
	require.Equal(t, walkAddr.Stage, fastAddr.Stage)
	require.Equal(t, walkAddr.Step, fastAddr.Step)
	require.Equal(t, "stage:wait", fastAddr.Stage, "both paths must address the pause stage")
}

// awaitFoldSnapshot waits for the pause-path writeFoldSnapshot to land for
// the run's NEWEST checkpoint. awaitPaused only observes the paused-map
// insert, which happens a few lines before the snapshot write, and a chained
// re-pause can briefly leave the PREVIOUS pause's snapshot as the latest
// non-nil one — so tests that then LOAD the snapshot must wait for the
// fresh one (Seq == last log entry) or they flake.
func awaitFoldSnapshot(t *testing.T, rt *WorkflowRuntime, runID string) *actionlog.FoldSnapshot {
	t.Helper()
	var snap *actionlog.FoldSnapshot
	require.Eventually(t, func() bool {
		latest, err := rt.actionLog.LatestRerunIndex(context.Background(), runID)
		if err != nil || latest < 0 {
			return false
		}
		entries, err := rt.actionLog.All(context.Background(), runID, latest)
		if err != nil || len(entries) == 0 {
			return false
		}
		s, err := rt.actionLog.(actionlog.FoldSnapshotStore).LoadFoldSnapshot(context.Background(), runID, latest)
		if err != nil || s == nil || s.Seq != entries[len(entries)-1].Seq {
			return false
		}
		snap = s
		return true
	}, 5*time.Second, 10*time.Millisecond, "expected fresh fold snapshot for run %s", runID)
	return snap
}

// TestFoldSnapshotParityChainedRepause: pause → resume → re-pause. The
// second pause's snapshot must supersede the first (latest wins), and the
// fast path must still be fresh (last entry IS the newest checkpoint).
func TestFoldSnapshotParityChainedRepause(t *testing.T) {
	ms := NewManualEventSource()
	rt := NewWorkflowRuntime(Options{
		ActionLog:   actionlog.NewMemoryActionLog(),
		EventSource: ms,
	})
	defer rt.Shutdown(context.Background())

	// Two consecutive waits: set x → wait evt:1 → set y → wait evt:2.
	wf := multiPauseWorkflow(t, func(ctx context.Context, state map[string]any, st store.Store) (pipeline.RoutingInstruction, error) {
		return pipeline.PauseForEvent("evt:2", 0), nil
	})
	// The helper names the workflow wf-multi-pause; re-key for rebuildBoth.
	wf.ID = "drift-wf"
	for tid, pd := range wf.Pipelines {
		pd.ID = "drift-wf-pipeline"
		wf.Pipelines[tid] = pd
	}
	prepared := make(chan string, 1)
	require.NoError(t, rt.Register(wf, RegisterOptions{
		Mode:      Mode{Type: "transient"},
		OnPrepare: func(h *RunHandle) error { prepared <- h.RunID; return nil },
	}))
	rt.Bus().Emit(context.Background(), ManualEvent, events.PipelineEvent{Payload: map[string]any{}})
	runID := <-prepared
	awaitPaused(t, rt, 1)
	ms.Emit("evt:1", map[string]any{"first": true})
	awaitPaused(t, rt, 1)
	awaitFoldSnapshot(t, rt, runID)

	fast, walk, fastAddr, walkAddr := rebuildBoth(t, rt, runID)
	requireParity(t, fast, walk)
	require.Equal(t, walkAddr.Stage, fastAddr.Stage)
	require.Equal(t, walkAddr.Step, fastAddr.Step)

	// And the snapshot under the latest attempt is the SECOND pause's.
	latest, err := rt.actionLog.LatestRerunIndex(context.Background(), runID)
	require.NoError(t, err)
	snap, err := rt.actionLog.(actionlog.FoldSnapshotStore).LoadFoldSnapshot(context.Background(), runID, latest)
	require.NoError(t, err)
	require.NotNil(t, snap)
	entries, err := rt.actionLog.All(context.Background(), runID, latest)
	require.NoError(t, err)
	require.Equal(t, entries[len(entries)-1].Seq, snap.Seq, "snapshot must anchor the newest checkpoint")
}

// TestFoldSnapshotGateDegradesToWalk: every gate failure produces the
// WALK's result — never the degraded checkpoint seed.
func TestFoldSnapshotGateDegradesToWalk(t *testing.T) {
	cases := []struct {
		name   string
		damage func(snap *actionlog.FoldSnapshot)
	}{
		{"stale seq", func(s *actionlog.FoldSnapshot) { s.Seq = 1 }},
		{"drifted fingerprint", func(s *actionlog.FoldSnapshot) { s.DefHash = "00000000000000000000000000000000" }},
		{"corrupt state", func(s *actionlog.FoldSnapshot) { s.State = []byte("{not json") }},
		{"empty state", func(s *actionlog.FoldSnapshot) { s.State = nil }},
		{"hashless snapshot", func(s *actionlog.FoldSnapshot) { s.DefHash = "" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ms := NewManualEventSource()
			rt := NewWorkflowRuntime(Options{
				ActionLog:   actionlog.NewMemoryActionLog(),
				EventSource: ms,
			})
			defer rt.Shutdown(context.Background())

			prepared := make(chan string, 1)
			require.NoError(t, rt.Register(snapshotWorkflow("drift-wf"), RegisterOptions{
				Mode:      Mode{Type: "transient"},
				OnPrepare: func(h *RunHandle) error { prepared <- h.RunID; return nil },
			}))
			rt.Bus().Emit(context.Background(), ManualEvent, events.PipelineEvent{Payload: map[string]any{}})
			runID := <-prepared
			awaitPaused(t, rt, 1)
			awaitFoldSnapshot(t, rt, runID)

			// Damage the persisted snapshot (re-save with the mutation).
			log := rt.actionLog
			latest, err := log.LatestRerunIndex(context.Background(), runID)
			require.NoError(t, err)
			store_ := log.(actionlog.FoldSnapshotStore)
			snap, err := store_.LoadFoldSnapshot(context.Background(), runID, latest)
			require.NoError(t, err)
			require.NotNil(t, snap)
			tc.damage(snap)
			require.NoError(t, store_.SaveFoldSnapshot(context.Background(), *snap))

			record := rt.workflows["drift-wf"]
			pdef := record.workflow.Pipelines["trigger:manual:Run"]

			// Damaged fast path must equal the walk, exactly.
			rebuild := rt.rebuildCapability(context.Background(), runID, record, pdef)
			stF, addrF, errF := rebuild(context.Background(), runID, pdef.ID)
			require.NoError(t, errF)

			rt.disableFoldSnapshots = true
			rebuild2 := rt.rebuildCapability(context.Background(), runID, record, pdef)
			stW, addrW, errW := rebuild2(context.Background(), runID, pdef.ID)
			require.NoError(t, errW)
			rt.disableFoldSnapshots = false

			fastJSON, _ := stF.ExportJSON()
			walkJSON, _ := stW.ExportJSON()
			fj, _ := json.Marshal(fastJSON)
			wj, _ := json.Marshal(walkJSON)
			require.JSONEq(t, string(wj), string(fj), "damaged gate must produce walk-identical state")
			require.Equal(t, addrW.Stage, addrF.Stage)
			require.Equal(t, addrW.Step, addrF.Step)
		})
	}
}

// TestFoldSnapshotParityAcrossRestart: the actual durability scenario —
// pause in runtime A, rebuild in fresh runtime B on the same log, snapshots
// ON in both. B's first rebuild must take the snapshot path and land on the
// same state the walk produces (asserted via the resumed outcome parity the
// S.3 suite already pins; here we assert the snapshot is FOUND and fresh).
func TestFoldSnapshotParityAcrossRestart(t *testing.T) {
	ms := NewManualEventSource()
	var log actionlog.Store = actionlog.NewMemoryActionLog()
	rt := NewWorkflowRuntime(Options{ActionLog: log, EventSource: ms})
	prepared := make(chan string, 1)
	require.NoError(t, rt.Register(snapshotWorkflow("drift-wf"), RegisterOptions{
		Mode:      Mode{Type: "transient"},
		OnPrepare: func(h *RunHandle) error { prepared <- h.RunID; return nil },
	}))
	rt.Bus().Emit(context.Background(), ManualEvent, events.PipelineEvent{Payload: map[string]any{}})
	runID := <-prepared
	awaitPaused(t, rt, 1)
	awaitFoldSnapshot(t, rt, runID)
	rt.Shutdown(context.Background())

	rtB := NewWorkflowRuntime(Options{ActionLog: log, Logger: zap.NewNop().Sugar()})
	defer rtB.Shutdown(context.Background())

	// The snapshot survived the "restart" and is fresh for B's gate.
	latest, err := log.LatestRerunIndex(context.Background(), runID)
	require.NoError(t, err)
	snap, err := log.(actionlog.FoldSnapshotStore).LoadFoldSnapshot(context.Background(), runID, latest)
	require.NoError(t, err)
	require.NotNil(t, snap, "snapshot must persist across the restart")

	// Rebuild in B takes the fast path: state includes the seeded trigger
	// event (impossible from the log alone) — the snapshot hit's signature.
	require.NoError(t, rtB.Register(snapshotWorkflow("drift-wf"), RegisterOptions{Mode: Mode{Type: "transient"}}))
	record := rtB.workflows["drift-wf"]
	require.NotNil(t, record)
	pdef := record.workflow.Pipelines["trigger:manual:Run"]
	rebuild := rtB.rebuildCapability(context.Background(), runID, record, pdef)
	st, _, err := rebuild(context.Background(), runID, pdef.ID)
	require.NoError(t, err)
	exported, err := st.ExportJSON()
	require.NoError(t, err)
	require.Contains(t, exported, "__trigger_event__")
	require.Equal(t, float64(1), exported["x"])
	require.Equal(t, float64(7), exported["p"])
}

// BenchmarkPausedRunRebuild measures the resume-time rebuild of a paused
// run with a 1000-entry history: full walk vs fold-snapshot fast path.
// The shape mirrors pkg/replay's Rebuild bench (5 stages × effectful steps
// with recorded deltas), plus the trailing KindCheckpoint a real pause
// leaves. Resume calls this synchronously — it bounds resume throughput.
func BenchmarkPausedRunRebuild(b *testing.B) {
	for _, entryCount := range []int{100, 1000} {
		b.Run(fmt.Sprintf("entries-%d", entryCount), func(b *testing.B) {
			ctx := context.Background()
			var log actionlog.Store = actionlog.NewMemoryActionLog()

			// 5-stage definition, one effectful step per stage (same shape
			// as the replay bench), then a pause on stage-3.
			stages := make([]pipeline.Stage, 5)
			for i := 0; i < 5; i++ {
				stageID := fmt.Sprintf("stage-%d", i+1)
				stepID := fmt.Sprintf("step-%d", i+1)
				key := fmt.Sprintf("k%d", i+1)
				val := float64(i + 1)
				stages[i] = pipeline.Stage{
					ID:    stageID,
					Steps: map[string]pipeline.Step{stepID: {ID: stepID, Effect: int(pipeline.EffectSideEffecting)}},
				}
				_ = key
				_ = val
			}
			def := pipeline.PipelineDefinition{ID: "bench-pipeline", Stages: stages}

			wf := &pipeline.Workflow{
				ID:        "bench-wf",
				Pipelines: map[string]pipeline.PipelineDefinition{"trigger:manual:Run": def},
				Triggers:  map[string]pipeline.WorkflowTrigger{"trigger:manual:Run": {ID: "trigger:manual:Run", Event: ManualEvent}},
			}
			record := &workflowRecord{workflow: wf}

			// Populate: pipeline:start, then 1000 step completions with
			// deltas, then the pause checkpoint.
			runID := "bench-run"
			_, err := log.Append(ctx, actionlog.Entry{RunID: runID, RerunIndex: 0, Kind: actionlog.KindPipelineStarted, PipelineID: def.ID})
			require.NoError(b, err)
			for i := 0; i < entryCount; i++ {
				stageIdx := i % 5
				delta, _ := json.Marshal(map[string]any{fmt.Sprintf("k%d", stageIdx+1): float64(stageIdx + 1)})
				_, err := log.Append(ctx, actionlog.Entry{
					RunID: runID, RerunIndex: 0, Kind: actionlog.KindStepCompleted,
					PipelineID: def.ID, StageID: fmt.Sprintf("stage-%d", stageIdx+1),
					StepID: fmt.Sprintf("step-%d", stageIdx+1), Output: delta,
				})
				require.NoError(b, err)
			}
			ckpt := pipeline.PipelineCheckpoint{
				PipelineID: def.ID,
				ResumeAt:   pipeline.EntryAddress{Stage: "stage-3"},
			}
			ckptJSON, _ := json.Marshal(ckpt)
			lastSeq, err := log.Append(ctx, actionlog.Entry{
				RunID: runID, RerunIndex: 0, Kind: actionlog.KindCheckpoint,
				PipelineID: def.ID, StageID: "stage-3", Output: ckptJSON,
			})
			require.NoError(b, err)

			// The snapshot: full state at the pause.
			state := map[string]any{"__trigger_event__": map[string]any{"type": "manual"}}
			for i := 0; i < 5; i++ {
				state[fmt.Sprintf("k%d", i+1)] = float64(i + 1)
			}
			stateBytes, _ := json.Marshal(state)
			snapStore := log.(actionlog.FoldSnapshotStore)
			require.NoError(b, snapStore.SaveFoldSnapshot(ctx, actionlog.FoldSnapshot{
				RunID: runID, RerunIndex: 0, Seq: lastSeq,
				DefHash: pipeline.WorkflowFingerprint(wf),
				State:   stateBytes,
			}))

			newRT := func(disabled bool) *WorkflowRuntime {
				rt := NewWorkflowRuntime(Options{ActionLog: log, Logger: zap.NewNop().Sugar()})
				rt.disableFoldSnapshots = disabled
				return rt
			}

			pdef := wf.Pipelines["trigger:manual:Run"]
			b.Run("walk", func(b *testing.B) {
				rt := newRT(true)
				defer rt.Shutdown(ctx)
				rebuild := rt.rebuildCapability(ctx, runID, record, pdef)
				require.NotNil(b, rebuild)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					st, _, err := rebuild(ctx, runID, def.ID)
					require.NoError(b, err)
					_ = st
				}
			})
			b.Run("snapshot", func(b *testing.B) {
				rt := newRT(false)
				defer rt.Shutdown(ctx)
				rebuild := rt.rebuildCapability(ctx, runID, record, pdef)
				require.NotNil(b, rebuild)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					st, _, err := rebuild(ctx, runID, def.ID)
					require.NoError(b, err)
					_ = st
				}
			})
		})
	}
}
