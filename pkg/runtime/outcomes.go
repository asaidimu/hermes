// Run outcomes, metadata projections, rerun indexes, and the terminal
// history janitor. Split out of runtime.go by R.1 (pure move).
package runtime

import (
	"context"
	"github.com/asaidimu/hermes/pkg/store"
	"time"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/timeline"
)

// GetRunOutcome returns the recorded outcome for a completed run.
func (rt *WorkflowRuntime) GetRunOutcome(runID string) (RunResult, bool) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	res, ok := rt.outcomes[runID]
	return res, ok
}

// ListRuns returns metadata for all runs this runtime has started.
// Event counts are refreshed from the action log.
func (rt *WorkflowRuntime) ListRuns(ctx context.Context) ([]timeline.RunTimelineMeta, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	list := make([]timeline.RunTimelineMeta, 0, len(rt.runMetas))
	for runID, meta := range rt.runMetas {
		cp := *meta
		if rt.actionLog == nil {
			list = append(list, cp)
			continue
		}
		if idx, err := rt.actionLog.LatestRerunIndex(ctx, runID); err == nil && idx >= 0 {
			if n, err := rt.actionLog.Count(ctx, runID, idx); err == nil {
				cp.EventCount = int64(n)
			}
		}
		list = append(list, cp)
	}
	return list, nil
}

// GetRunMeta returns metadata for a single run.
func (rt *WorkflowRuntime) GetRunMeta(ctx context.Context, runID string) (*timeline.RunTimelineMeta, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	meta, ok := rt.runMetas[runID]
	if !ok {
		return nil, core.NewSystemError(core.ErrCodeNotFound, "run not found: "+runID)
	}
	cp := *meta
	if rt.actionLog != nil {
		if idx, err := rt.actionLog.LatestRerunIndex(ctx, runID); err == nil && idx >= 0 {
			if n, err := rt.actionLog.Count(ctx, runID, idx); err == nil {
				cp.EventCount = int64(n)
			}
		}
	}
	return &cp, nil
}

// GetEvents returns the run's action log entries for its latest attempt,
// projected onto the frontend timeline wire shape.
func (rt *WorkflowRuntime) GetEvents(ctx context.Context, runID string, fromSeq, toSeq int64) ([]timeline.TimelineEvent, error) {
	if rt.actionLog == nil {
		return []timeline.TimelineEvent{}, nil
	}
	idx, err := rt.actionLog.LatestRerunIndex(ctx, runID)
	if err != nil {
		return nil, err
	}
	if idx < 0 {
		return []timeline.TimelineEvent{}, nil
	}
	entries, err := rt.actionLog.All(ctx, runID, idx)
	if err != nil {
		return nil, err
	}

	rt.mu.Lock()
	pipelineID, pipelineLabel := rt.pipelineLabelLocked(runID)
	rt.mu.Unlock()

	out := make([]timeline.TimelineEvent, 0, len(entries))
	for _, e := range entries {
		seq := int64(e.Seq)
		if fromSeq > 0 && seq < fromSeq {
			continue
		}
		if toSeq > 0 && seq > toSeq {
			continue
		}
		out = append(out, timeline.EntryToTimelineEvent(e, pipelineID, pipelineLabel))
	}
	return out, nil
}

// GetActionLog returns the raw action log entries for a run's latest attempt.
// Unlike GetEvents (which projects to timeline shapes), this returns the full
// entry structs including state deltas — suitable for the TS Replayer.
func (rt *WorkflowRuntime) GetActionLog(ctx context.Context, runID string) ([]actionlog.Entry, error) {
	if rt.actionLog == nil {
		return []actionlog.Entry{}, nil
	}
	idx, err := rt.actionLog.LatestRerunIndex(ctx, runID)
	if err != nil {
		return nil, err
	}
	if idx < 0 {
		return []actionlog.Entry{}, nil
	}
	return rt.actionLog.All(ctx, runID, idx)
}

// pipelineLabelLocked resolves the pipeline identity for event projection.
// Caller must hold rt.mu.
func (rt *WorkflowRuntime) pipelineLabelLocked(runID string) (string, string) {
	if meta, ok := rt.runMetas[runID]; ok {
		return meta.PipelineID, ""
	}
	if st, ok := rt.stores[runID]; ok {
		var pipelineID string
		_ = st.Read(func(state map[string]any) error {
			if m, ok := state[store.RunMetaKey].(map[string]any); ok {
				pipelineID, _ = m["pipelineId"].(string)
			}
			return nil
		})
		return pipelineID, ""
	}
	return "", ""
}

// nextRerunIndex returns the rerun index for a new attempt of a run:
// one past the latest recorded attempt, or 0 for a fresh run.
func (rt *WorkflowRuntime) nextRerunIndex(ctx context.Context, runID string) int {
	if rt.actionLog == nil {
		return 0
	}
	idx, err := rt.actionLog.LatestRerunIndex(ctx, runID)
	if err != nil || idx < 0 {
		return 0
	}
	return idx + 1
}

// rerunIndexOf returns the current rerun index tracked for a run.
func (rt *WorkflowRuntime) rerunIndexOf(runID string) int {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.rerunIdx[runID]
}

// ---------------------------------------------------------------------------
// Workflow registry
// ---------------------------------------------------------------------------

// setRunStatus records the terminal/paused status of a run in its metadata.
func (rt *WorkflowRuntime) setRunStatus(runID, status string) {
	var s timeline.RunTimelineStatus
	switch status {
	case "succeeded":
		s = timeline.StatusComplete
	case "failed", "aborted":
		s = timeline.StatusFailed
	case "paused":
		s = timeline.StatusPaused
	default:
		return
	}
	now := time.Now().UnixMilli()
	rt.mu.Lock()
	defer rt.mu.Unlock()
	meta, ok := rt.runMetas[runID]
	if !ok {
		return
	}
	meta.Status = s
	if s == timeline.StatusComplete || s == timeline.StatusFailed {
		meta.EndTime = &now
	}
}

func (rt *WorkflowRuntime) clearActive(runID string) {
	rt.mu.Lock()
	delete(rt.active, runID)
	rt.mu.Unlock()
}

// terminalStatus reports whether a run status is final (no resume can ever
// need the run's in-memory bookkeeping again). "paused" is deliberately NOT
// terminal: a paused run's store is its resume payload.
func terminalStatus(status string) bool {
	return status == "succeeded" || status == "failed" || status == "aborted"
}

// defaultStoreGrace is the eviction horizon for terminal runs' state stores
// and rerun indexes when Options.RunHistoryTTL is not set.
const defaultStoreGrace = 10 * time.Minute

// @note #review-20260910-004 issue P2 resolved status=resolved priority=P2 tags=#review,#memory-leak : Per-run bookkeeping maps were never evicted
// @author hermes-review
//
// Resolved with a janitor-based eviction policy. rt.stores, rt.outcomes,
// rt.runMetas and rt.rerunIdx previously only ever gained entries (only
// rt.active/rt.paused were pruned), so a long-lived host accumulated one
// full state map per run forever — a genuine leak, not just metadata
// growth. Two tiers now:
//
//  1. STATE STORES + rerun indexes (the leak proper) are always evicted once
//     a run is terminal and its grace horizon has passed — RunHistoryTTL when
//     set, else a 10-minute default (defaultStoreGrace). The grace preserves
//     the documented post-completion read pattern (rt.Store(runID)
//     immediately after a run finishes, and the #review-20260910-002
//     canonical-store contract asserted by tests) while still bounding
//     memory; the final state stays reachable via GetRunOutcome and the
//     action log. An immediate purge at the terminal write was tried first
//     and rejected: it nils rt.Store(runID) the moment a run completes,
//     breaking hosts that read the run's document afterwards.
//  2. AUDIT HISTORY (runMetas for ListRuns, outcomes for GetRunOutcome) is
//     evicted only when the host opts in with Options.RunHistoryTTL; the
//     default keeps it forever, matching the pre-fix audit behavior.
//
// Paused and in-flight runs are never touched. Shutdown stops the janitor.
func (rt *WorkflowRuntime) startHistoryJanitor() {
	// Tick fast enough for a 10-minute default grace to feel prompt, slow
	// enough to be invisible; a configured TTL always gets a proportional
	// interval.
	interval := rt.runHistoryTTL / 10
	if interval < time.Minute {
		interval = time.Minute
	}
	rt.janitorStop = make(chan struct{})
	rt.janitorDone = make(chan struct{})
	stop, done := rt.janitorStop, rt.janitorDone
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				rt.purgeExpiredRuns()
			}
		}
	}()
}

// purgeExpiredRuns drops in-memory bookkeeping for terminal runs whose
// EndTime is older than the applicable horizon: stores + rerun indexes after
// the store grace (RunHistoryTTL or the 10-minute default), and audit
// history (runMetas/outcomes) after RunHistoryTTL when the host set one.
func (rt *WorkflowRuntime) purgeExpiredRuns() {
	now := time.Now()
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for runID, meta := range rt.runMetas {
		if meta.Status != timeline.StatusComplete && meta.Status != timeline.StatusFailed {
			continue
		}
		if meta.EndTime == nil {
			continue
		}
		ended := time.UnixMilli(*meta.EndTime)

		// Tier 1: state store + rerun index, after the store grace.
		storeHorizon := defaultStoreGrace
		if rt.runHistoryTTL > 0 {
			storeHorizon = rt.runHistoryTTL
		}
		if now.Sub(ended) >= storeHorizon {
			delete(rt.stores, runID)
			delete(rt.rerunIdx, runID)
		}

		// Tier 2: audit history, only with an explicit TTL.
		if rt.runHistoryTTL > 0 && now.Sub(ended) >= rt.runHistoryTTL {
			delete(rt.runMetas, runID)
			delete(rt.outcomes, runID)
		}
	}
}

func (rt *WorkflowRuntime) callOnComplete(record *workflowRecord, result RunResult) {
	if record.opts.OnComplete != nil {
		record.opts.OnComplete(result)
	}
	if record.opts.OnCleanup != nil {
		record.opts.OnCleanup()
	}
}

func (rt *WorkflowRuntime) finishRun(record *workflowRecord, result RunResult) RunResult {
	rt.mu.Lock()
	rt.outcomes[result.RunID] = result
	rt.mu.Unlock()
	return result
}

// @note #secrets-credentials-are-a-host-s-a51c544f observation P2 #review,#production-readiness,#security,#secrets : Secrets/credentials are a host-supplied interface, not a built-in vault
// @author hermes-review
//
// SecretProvider (runtime.go) is just Get/Has — hermes has no built-in
// encrypted credential store, no per-credential access scoping to
// workflows/nodes, and no credential rotation/versioning. Every adopter
// must build (or already own) a secrets backend and implement this
// interface themselves before nodes like http/database can use
// credentials safely. Compare n8n, which ships an encrypted-at-rest
// credentials vault, per-credential sharing/permissions, and OAuth
// flows out of the box. Not a bug — the interface design is reasonable
// for an embeddable library — but it means 'secrets management' is a
// build-it-yourself line item for anyone evaluating hermes as a
// drop-in replacement for a product like n8n.
