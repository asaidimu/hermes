// Log-derived indexes and stitched entry loading. Split out of replayer.go
// by R.1 (pure move); P4.8 memoizes these.
package replay

import (
	"context"
	"encoding/json"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/pipeline"
)

// allEntries reads the run's COMPLETE recorded history: every attempt
// (rerunIndex 0..latest), stitched in attempt order with monotonically
// re-baselined Seq values. Returns nil when no log exists — the caller
// then resumes from the start.
//
// @note #review-20260910-025 issue P1 resolved status=resolved priority=P1 tags=#review,#replay,#resume : Replayer rebuilt from only the latest attempt's entries, so every re-paused run resumed at the wrong address
// @author hermes-review
// @see #review-20260910-001
//
// Resume advances the run's rerunIndex on every resume (deliberately —
// the resolved #review-20260910-006 note in runtime.go gives each resumed
// segment its own audit trail), so a multi-pause run's history is SPREAD
// across several attempts: attempt 0 records the first segment (its
// effects and pause #1's checkpoint), attempt 1 the first resumed segment
// (its effects and pause #2's checkpoint), and so on. The old
// latestEntries read ONLY the highest index, so on the second resume the
// Replayer was handed a log that began at the first resumed segment: no
// record of the opening stage's completed effect, no first-pause
// checkpoint. walkDef then concluded that forward execution starts at the
// first unrecorded effectful step — the opening stage's — and the resumed
// run re-executed it, marched into the FIRST pause's router and re-paused
// on an event that had already been consumed. The run could never make
// progress: any workflow pausing twice deadlocked on its second resume
// (TestResumeRepauseSingleEvent / TestResumeRepauseMultiEvent in
// pkg/runtime/repause_test.go; masked until #review-20260910-007 and
// -022 unmasked the pause path itself).
//
// Stitching is chronological by construction: nextRerunIndex assigns
// attempt indexes strictly increasing, and every entry of attempt N is
// appended after every entry of attempt N-1 (the runtime bumps the index
// before the resumed segment appends anything), so concatenating attempts
// in index order reproduces execution order.
//
// Seq re-baselining: both shipped stores derive Seq per (runID,
// rerunIndex) — MemoryActionLog counts per (run, index) pair and
// AnansiStore per document (its Append computes max+1 within the
// (runID, rerunIndex) document) — so raw Seq restarts at 1 in every
// attempt. The walk's helpers (checkpointSeq / resumedAfterCheckpoint)
// compare Seq ACROSS attempts once stitched, so the concatenated entries
// are renumbered 1..N in stitch order. Range copies make this safe: the
// renumbered slice is local and never written back to the store.
func (rp *Replayer) allEntries(ctx context.Context, runID string) ([]actionlog.Entry, error) {
	latest, err := rp.log.LatestRerunIndex(ctx, runID)
	if err != nil {
		return nil, err
	}
	if latest < 0 {
		return nil, nil
	}

	var stitched []actionlog.Entry
	var nextSeq uint64 = 1
	for idx := 0; idx <= latest; idx++ {
		part, err := rp.log.All(ctx, runID, idx)
		if err != nil {
			return nil, err
		}
		for _, e := range part {
			e.Seq = nextSeq
			nextSeq++
			stitched = append(stitched, e)
		}
	}
	return stitched, nil
}

// indexByStepID builds a map from StepID to the first completed entry.
// For effectful steps, this is the KindEffectCompleted or KindEffectFailed
// entry that records the step's outcome. Callers must pre-filter entries to
// a single pipeline's own entries (#review-20260910-015).
func indexByStepID(entries []actionlog.Entry) map[string]actionlog.Entry {
	m := make(map[string]actionlog.Entry)
	for _, e := range entries {
		if e.StepID != "" && (e.Kind == actionlog.KindEffectCompleted || e.Kind == actionlog.KindEffectFailed) {
			if _, exists := m[e.StepID]; !exists {
				m[e.StepID] = e
			}
		}
	}
	return m
}

// indexByStage builds a map from StageID to the routing handle of the
// most recent KindRouted entry for that stage.
func indexByStage(entries []actionlog.Entry) map[string]string {
	m := make(map[string]string)
	for _, e := range entries {
		if e.Kind == actionlog.KindRouted && e.StageID != "" {
			m[e.StageID] = e.Handle
		}
	}
	return m
}

// checkpointSeq returns the Seq of the LATEST pause checkpoint recorded for
// the stage (0 when the stage never paused).
func checkpointSeq(entries []actionlog.Entry, stageID string) uint64 {
	var last uint64
	for _, e := range entries {
		if e.Kind == actionlog.KindCheckpoint && e.StageID == stageID && e.Seq > last {
			last = e.Seq
		}
	}
	return last
}

// resumedAfterCheckpoint reports whether the log shows a resume of the
// stage's latest pause: a KindResumed entry recorded after that checkpoint.
// A pause whose latest checkpoint has no following KindResumed means the run
// is STILL WAITING at that pause — replay must stop there and hand back the
// checkpoint's resume address instead of walking on as if the route had been
// taken.
func resumedAfterCheckpoint(entries []actionlog.Entry, stageID string) bool {
	ckptSeq := checkpointSeq(entries, stageID)
	if ckptSeq == 0 {
		return false
	}
	for _, e := range entries {
		if e.Kind == actionlog.KindResumed && e.Seq > ckptSeq {
			return true
		}
	}
	return false
}

// indexCheckpoints builds a map from StageID to the resume stage recorded in
// that stage's pause checkpoint (KindCheckpoint / "pipeline:pause"). The
// checkpoint's ResumeAt is where execution continues after the wait — replay
// resolves pause wait-marker routes through it (see walkDef).
func indexCheckpoints(entries []actionlog.Entry) map[string]string {
	m := make(map[string]string)
	for _, e := range entries {
		if e.Kind != actionlog.KindCheckpoint || e.StageID == "" || len(e.Output) == 0 {
			continue
		}
		var ckpt pipeline.PipelineCheckpoint
		if err := json.Unmarshal(e.Output, &ckpt); err != nil {
			continue
		}
		if ckpt.ResumeAt.Stage != "" {
			m[e.StageID] = ckpt.ResumeAt.Stage
		}
	}
	return m
}
