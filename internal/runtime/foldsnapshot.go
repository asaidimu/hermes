// Fold snapshots (v2.1 plan H2.2): log compaction for paused-run rebuilds.
//
// The final gate of the 20261003 refactor documented seq-fold rebuild at
// ~1.9µs per recorded delta, linear in history length — and rebuild runs
// once per Resume. For a CURRENTLY-PAUSED run (the restart-durability hot
// path: process died while runs were paused, host re-registers, resumes),
// the entire pre-pause history is folded only to reproduce the state as it
// stood at the pause. This file caches exactly that state:
//
//   - WRITE (pause time): after the paused-registry write and BEFORE
//     buffered delivery (C5 ordering — a chained re-pause's own bookkeeping
//     then overwrites with the newer snapshot), the run's store is exported
//     and saved beside the log entries under the pause's sequence number.
//
//   - READ (rebuild time): the fast path is attempted ONLY when every gate
//     holds — snapshot present for the latest attempt, the attempt's last
//     entry is the KindCheckpoint AT the snapshot's Seq, and the definition
//     fingerprint matches (H1 composes here). Any miss, corruption, or
//     doubt degrades to the FULL WALK — never to the checkpoint seed (that
//     degraded path stays as loud as R.4 made it). The gate is therefore
//     fail-closed toward the exact semantics the golden vectors pin.
//
// Snapshots are a derived cache: deleting every one of them changes no
// semantics, only latency. Options.DisableFoldSnapshots turns the whole
// mechanism off (also the A/B switch for the parity tests and bench).
package runtime

import (
	"context"
	"encoding/json"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
)

// writeFoldSnapshot exports the run's state at its latest pause and saves
// it beside the log. Called from BOTH pause-bookkeeping sites (spawnRun's
// first pause, Resume's re-pause) after the paused-registry write and
// before buffered delivery. Best-effort: every failure is logged Warn and
// the walk covers the miss; a snapshot save must never fail a pause.
//
// The identity check (still paused on THIS checkpoint) narrows the race
// against a chained re-pause (C5): if a newer pause already superseded
// this one, the export is skipped — the newer pause's own bookkeeping
// writes the newer snapshot. A residual race (state read after the check,
// newer pause before the save) is harmless by construction: the snapshot
// would record an older Seq than the attempt's last entry, and the
// read-side freshness gate rejects exactly that.
func (rt *WorkflowRuntime) writeFoldSnapshot(runID string, st store.Store, wfHash string) {
	if rt.actionLog == nil || rt.disableFoldSnapshots || st == nil {
		return
	}
	snapStore, ok := rt.actionLog.(actionlog.FoldSnapshotStore)
	if !ok {
		return
	}

	ctx := context.Background()
	latest, err := rt.actionLog.LatestRerunIndex(ctx, runID)
	if err != nil || latest < 0 {
		return
	}
	entries, err := rt.actionLog.All(ctx, runID, latest)
	if err != nil || len(entries) == 0 {
		return
	}
	last := entries[len(entries)-1]
	if last.Kind != actionlog.KindCheckpoint || len(last.Output) == 0 {
		return
	}
	var ckpt pipeline.PipelineCheckpoint
	if json.Unmarshal(last.Output, &ckpt) != nil || ckpt.ResumeAt.Stage == "" {
		return
	}

	// Identity gate: the runtime must still hold THIS run paused on THIS
	// checkpoint (same resume address). Caller ordering (before buffered
	// delivery) makes this true in the normal path; the check makes the
	// chained case skip instead of mislabel.
	rt.mu.Lock()
	pr := rt.paused[runID]
	samePause := pr != nil && pr.checkpoint != nil &&
		pr.checkpoint.ResumeAt.Stage == ckpt.ResumeAt.Stage &&
		pr.checkpoint.ResumeAt.Step == ckpt.ResumeAt.Step
	rt.mu.Unlock()
	if !samePause {
		return
	}

	state, err := st.ExportJSON()
	if err != nil {
		rt.logger.Warn("fold snapshot: state export failed; pause rebuild will walk the log", "runId", runID, "error", err)
		return
	}
	stateBytes, err := json.Marshal(state)
	if err != nil {
		rt.logger.Warn("fold snapshot: state marshal failed; pause rebuild will walk the log", "runId", runID, "error", err)
		return
	}

	snap := actionlog.FoldSnapshot{
		RunID:      runID,
		RerunIndex: latest,
		Seq:        last.Seq,
		DefHash:    wfHash,
		State:      stateBytes,
	}
	if err := snapStore.SaveFoldSnapshot(ctx, snap); err != nil {
		rt.logger.Warn("fold snapshot: save failed; pause rebuild will walk the log", "runId", runID, "error", err)
	}
}

// foldSnapshotRebuild wraps the replayer's Rebuild with the snapshot fast
// path. The returned func evaluates the FULL freshness gate at call time
// (the log may have grown between rebuildCapability and the factory's
// Rebuild call) and falls back to the walk on any doubt. Returns nil when
// the capability/policy preconditions fail — the caller then wires the
// plain walk, byte-identical to pre-H2 behavior.
func (rt *WorkflowRuntime) foldSnapshotRebuild(
	snapStore actionlog.FoldSnapshotStore,
	runID string,
	latest int,
	wfHash string,
	walk pipeline.RebuildFunc,
) pipeline.RebuildFunc {
	if rt.disableFoldSnapshots || snapStore == nil || walk == nil {
		return nil
	}
	return func(ctx context.Context, runIDArg, rootPipelineID string) (store.Store, pipeline.EntryAddress, error) {
		seeded, addr, ok := rt.tryFoldSnapshot(ctx, snapStore, runIDArg, latest, wfHash)
		if ok {
			rt.logger.Debug("rebuild: fold snapshot hit",
				"runId", runIDArg, "rerunIndex", latest)
			return seeded, addr, nil
		}
		return walk(ctx, runIDArg, rootPipelineID)
	}
}

// tryFoldSnapshot evaluates the freshness gate and, only when every
// condition holds, returns (seeded store, checkpoint resume address).
// The zero ok value sends the caller to the full walk.
func (rt *WorkflowRuntime) tryFoldSnapshot(
	ctx context.Context,
	snapStore actionlog.FoldSnapshotStore,
	runID string,
	latest int,
	wfHash string,
) (store.Store, pipeline.EntryAddress, bool) {
	if latest < 0 {
		return nil, pipeline.EntryAddress{}, false
	}
	snap, err := snapStore.LoadFoldSnapshot(ctx, runID, latest)
	if err != nil || snap == nil {
		return nil, pipeline.EntryAddress{}, false
	}

	// Gate 1: the attempt's last entry IS the checkpoint the snapshot
	// anchors. If anything appended after the pause (a resume, a crash
	// mid-segment), the snapshot is stale — walk.
	entries, err := rt.actionLog.All(ctx, runID, latest)
	if err != nil || len(entries) == 0 {
		return nil, pipeline.EntryAddress{}, false
	}
	last := entries[len(entries)-1]
	if last.Kind != actionlog.KindCheckpoint || last.Seq != snap.Seq {
		return nil, pipeline.EntryAddress{}, false
	}

	// Gate 2: same definition fingerprint (H1). A drifted workflow must
	// never be seeded from a snapshot recorded under the old one.
	if wfHash == "" || snap.DefHash != wfHash {
		return nil, pipeline.EntryAddress{}, false
	}

	// Gate 3: the checkpoint must carry a usable resume address.
	var ckpt pipeline.PipelineCheckpoint
	if len(last.Output) == 0 || json.Unmarshal(last.Output, &ckpt) != nil || ckpt.ResumeAt.Stage == "" {
		return nil, pipeline.EntryAddress{}, false
	}

	// Gate 4: parseable state.
	var state map[string]any
	if len(snap.State) == 0 || json.Unmarshal(snap.State, &state) != nil {
		return nil, pipeline.EntryAddress{}, false
	}

	seeded := store.NewMemoryStore(nil)
	if err := seeded.Update(ctx, func(m map[string]any) error {
		for k, v := range state {
			m[k] = v
		}
		return nil
	}); err != nil {
		return nil, pipeline.EntryAddress{}, false
	}
	return seeded, ckpt.ResumeAt, true
}
