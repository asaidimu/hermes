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

// logIndex is the memoized view of one pipeline's own entries (plan
// P4.8, H8): every lookup walkDef/foldDef needs, built in ONE pass over
// the entry slice. Pre-P4.8, the per-stage helpers (checkpointSeq,
// resumedAfterCheckpoint, stageForked) re-scanned the whole slice for
// every stage — O(stages x entries) per replay.
type logIndex struct {
	byStep      map[string][]actionlog.Entry // stepID -> recorded outcomes, seq order (S.1)
	routes      map[string]string          // stageID -> latest routed handle
	checkpoints map[string]string          // stageID -> checkpoint ResumeAt stage
	forked      map[string]bool            // stageID -> subpipeline forked here
	ckptSeq     map[string]uint64          // stageID -> latest checkpoint seq
	maxResumed  uint64                     // highest KindResumed seq in the slice
}

// buildLogIndex makes one pass over the entries, computing every index
// the walk needs. Kind values are disjoint, so the switch is total over
// the interesting kinds.
func buildLogIndex(entries []actionlog.Entry) *logIndex {
	idx := &logIndex{
		byStep:      make(map[string][]actionlog.Entry),
		routes:      make(map[string]string),
		checkpoints: make(map[string]string),
		forked:      make(map[string]bool),
		ckptSeq:     make(map[string]uint64),
	}
	for _, e := range entries {
		switch e.Kind {
		case actionlog.KindStepCompleted, actionlog.KindStepFailed:
			// S.1: KindStepCompleted/KindStepFailed are the ONLY
			// completion records (the KindEffect* duplicates were
			// deleted with this commit). Every recorded outcome for a
			// step is kept IN SEQ ORDER — retries and loop iterations
			// append, and the canonical fold replays the sequence.
			if e.StepID != "" {
				idx.byStep[e.StepID] = append(idx.byStep[e.StepID], e)
			}
		case actionlog.KindRouted:
			if e.StageID != "" {
				idx.routes[e.StageID] = e.Handle
			}
		case actionlog.KindCheckpoint:
			if e.StageID == "" {
				continue
			}
			if e.Seq > idx.ckptSeq[e.StageID] {
				idx.ckptSeq[e.StageID] = e.Seq
			}
			if len(e.Output) > 0 {
				var ckpt pipeline.PipelineCheckpoint
				if err := json.Unmarshal(e.Output, &ckpt); err == nil && ckpt.ResumeAt.Stage != "" {
					idx.checkpoints[e.StageID] = ckpt.ResumeAt.Stage
				}
			}
		case actionlog.KindSubpipelineForked:
			if e.StageID != "" {
				idx.forked[e.StageID] = true
			}
		case actionlog.KindResumed:
			if e.Seq > idx.maxResumed {
				idx.maxResumed = e.Seq
			}
		}
	}
	return idx
}

// resumedAfter reports whether a KindResumed entry follows the stage's
// latest checkpoint: the run is NOT still waiting at that pause.
// Memoized as maxResumed > ckptSeq — "some resume exists above the
// threshold" is exactly "the highest resume is above it".
func (idx *logIndex) resumedAfter(stageID string) bool {
	ckpt, ok := idx.ckptSeq[stageID]
	if !ok || ckpt == 0 {
		return false
	}
	return idx.maxResumed > ckpt
}
