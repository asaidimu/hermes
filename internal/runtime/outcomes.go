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
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	res, ok := rt.outcomes[runID]
	return res, ok
}

// ListRuns returns metadata for all runs this runtime has started.
// Event counts are refreshed from the action log.
func (rt *WorkflowRuntime) ListRuns(ctx context.Context) ([]timeline.RunTimelineMeta, error) {
	// P4.6: snapshot the metas under the lock, then run the per-run
	// action-log queries OUTSIDE it — the DB round trips previously ran
	// under the EXCLUSIVE runtime lock, stalling dispatch/Resume/Register
	// process-wide for as long as the log backend took to answer.
	rt.mu.Lock()
	list := make([]timeline.RunTimelineMeta, 0, len(rt.runMetas))
	for _, meta := range rt.runMetas {
		list = append(list, *meta)
	}
	rt.mu.Unlock()

	if rt.actionLog == nil {
		return list, nil
	}
	for i := range list {
		runID := list[i].RunID
		if idx, err := rt.actionLog.LatestRerunIndex(ctx, runID); err == nil && idx >= 0 {
			if n, err := rt.actionLog.Count(ctx, runID, idx); err == nil {
				list[i].EventCount = int64(n)
			}
		}
	}
	return list, nil
}

// GetRunMeta returns metadata for a single run. On a runMetas cache miss
// (tier-2 eviction, or a run recorded only in a shared log) the meta is
// reconciled from the action log — the log is authoritative, the RAM map
// is a write-through cache (S.2).
func (rt *WorkflowRuntime) GetRunMeta(ctx context.Context, runID string) (*timeline.RunTimelineMeta, error) {
	// P4.6: same split as ListRuns — copy under the lock, query outside.
	rt.mu.Lock()
	meta, ok := rt.runMetas[runID]
	if ok {
		cp := *meta
		rt.mu.Unlock()

		if rt.actionLog != nil {
			if idx, err := rt.actionLog.LatestRerunIndex(ctx, runID); err == nil && idx >= 0 {
				if n, err := rt.actionLog.Count(ctx, runID, idx); err == nil {
					cp.EventCount = int64(n)
				}
			}
		}
		return &cp, nil
	}
	rt.mu.Unlock()

	if reconciled, ok := rt.reconcileRunMeta(ctx, runID); ok {
		return reconciled, nil
	}
	return nil, core.NewSystemError(core.ErrCodeNotFound, "run not found: "+runID)
}

// metaFromEntries synthesizes a run meta from the latest attempt's log
// entries (S.2/S.3 shared shape). The first entry supplies pipeline id and
// start time; the LAST entry supplies status:
// KindPipelineCompleted/KindPipelineFailed are terminal with an EndTime,
// anything else is a run still recording (in-flight or recovered) — the
// caller refines non-terminal kinds (Recover maps KindCheckpoint to
// paused, which the generic reconcile deliberately does not: a checkpoint
// observed mid-run is a pause SOME pipeline is nested at, but a live run
// keeps recording past it).
func metaFromEntries(runID string, entries []actionlog.Entry) *timeline.RunTimelineMeta {
	first, last := entries[0], entries[len(entries)-1]
	meta := &timeline.RunTimelineMeta{
		RunID:      runID,
		PipelineID: first.PipelineID,
		StartTime:  first.Timestamp.UnixMilli(),
		EventCount: int64(len(entries)),
	}
	// H1.2: the workflow fingerprint rides on KindPipelineStarted and
	// KindCheckpoint entries; take the first stamped one (recovered runs may
	// predate stamping on their first entry).
	for i := range entries {
		if entries[i].DefHash != "" {
			meta.DefHash = entries[i].DefHash
			break
		}
	}
	// Root-pipeline entries may carry an empty PipelineID (pre-#015
	// entries); fall back to the last entry's, then leave the id empty
	// rather than inventing one.
	if meta.PipelineID == "" {
		meta.PipelineID = last.PipelineID
	}
	switch last.Kind {
	case actionlog.KindPipelineCompleted:
		end := last.Timestamp.UnixMilli()
		meta.Status, meta.EndTime = timeline.StatusComplete, &end
	case actionlog.KindPipelineFailed:
		end := last.Timestamp.UnixMilli()
		meta.Status, meta.EndTime = timeline.StatusFailed, &end
	default:
		meta.Status = timeline.StatusRecording
	}
	return meta
}

// putRunMetaIfAbsent inserts a synthesized meta when the run is unknown,
// returning the authoritative entry — a concurrent dispatch may have
// inserted the live meta while the log was being read; the live record
// (with fresh status writes) wins.
func (rt *WorkflowRuntime) putRunMetaIfAbsent(meta *timeline.RunTimelineMeta) *timeline.RunTimelineMeta {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if existing, ok := rt.runMetas[meta.RunID]; ok {
		return existing
	}
	rt.runMetas[meta.RunID] = meta
	return meta
}

// reconcileRunMeta rebuilds a run's meta from the action log after a
// runMetas cache miss (S.2: one run-status fact is recorded once — the
// log; runMetas/outcomes are derived caches). Reports false for runs with
// no log facts at all (or no log) — those are genuinely unknown.
func (rt *WorkflowRuntime) reconcileRunMeta(ctx context.Context, runID string) (*timeline.RunTimelineMeta, bool) {
	if rt.actionLog == nil {
		return nil, false
	}
	idx, err := rt.actionLog.LatestRerunIndex(ctx, runID)
	if err != nil || idx < 0 {
		return nil, false
	}
	entries, err := rt.actionLog.All(ctx, runID, idx)
	if err != nil || len(entries) == 0 {
		return nil, false
	}
	meta := rt.putRunMetaIfAbsent(metaFromEntries(runID, entries))
	return meta, true
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

// ---------------------------------------------------------------------------
// Lifecycle facts: where each is recorded (S.2 dual-write audit)
// ---------------------------------------------------------------------------

// Every run-status fact is recorded ONCE authoritatively — the action
// log — with RAM structures as derived caches. Per-fact disposition
// (audited across the 15 lifecycle write sites; S.2):
//
//   pipeline:start      emit "pipeline:start" (context.go startPipeline)
//                       + KindPipelineStarted   → log authoritative
//   stage:start/done    emit + KindStageStarted/KindStageCompleted → log
//   stage:failure       emit + KindStageFailed → log
//   step:start/retry    emit + KindStepStarted/KindStepRetry → log
//   step:success        emit "step:success" + KindStepCompleted → log
//                       (S.1 deleted the duplicate KindEffectCompleted)
//   step:failure        emit + KindStepFailed → log
//   pipeline:success    emit + KindPipelineCompleted → log; ALSO the only
//                       fact that seeds outcomes (RunResult) and
//                       setRunStatus — both are caches of this+the log
//   pipeline:failure    emit + KindPipelineFailed (failStage and
//                       abortedResult) → log
//   routed              LOG ONLY (routing_interp.go, KindRouted) — no
//                       subscriber; the Replayer and timeline read it
//   checkpoint          LOG ONLY (KindCheckpoint) — the durable resume
//                       address; recovery's single resume source
//   pipeline:resumed    LOG ONLY (KindResumed, pauseresume.go) — emitted
//                       implicitly via downstream step events
//   subpipeline fork/join  LOG ONLY (KindSubpipelineForked/Joined)
//   run status          setRunStatus (below): write-through RAM cache of
//                       the terminal/paused fact the log already holds;
//                       reconciled from the log on cache miss
//                       (reconcileRunMeta) and at startup (S.3)
//   outcome             finishRun → rt.outcomes: RAM-only RunResult
//                       convenience (carries FinalState); the durable
//                       terminal fact is the log's KindPipeline*
//   timeline            pure projection of the log (verified in S.1)
//
// Rule of thumb: emit = tell subscribers now; log = make it true
// forever. Facts needed to recover a run are log-only; facts needed to
// notify are emit-only; facts needed for both are dual-written with the
// log authoritative.

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

// defaultStoreGrace is the eviction horizon for terminal runs' state
// stores when Options.RunHistoryTTL is not set.
const defaultStoreGrace = 10 * time.Minute

// @note #review-20260910-004 issue P2 resolved status=resolved priority=P2 tags=#review,#memory-leak : Per-run bookkeeping maps were never evicted
// @author hermes-review
//
// Resolved with a janitor-based eviction policy. rt.stores, rt.outcomes
// and rt.runMetas previously only ever gained entries (only
// rt.active/rt.paused were pruned), so a long-lived host accumulated one
// full state map per run forever — a genuine leak, not just metadata
// growth. Two tiers now (rt.rerunIdx was deleted in S.2 — the rerun
// index derives from the log's LatestRerunIndex and cannot diverge):
//
//  1. STATE STORES (the leak proper) are always evicted once a run is
//     terminal and its grace horizon has passed — RunHistoryTTL when
//     set, else a 10-minute default (defaultStoreGrace). The grace preserves
//     the documented post-completion read pattern (rt.Store(runID)
//     immediately after a run finishes, and the #review-20260910-002
//     canonical-store contract asserted by tests) while still bounding
//     memory; the final state stays reachable via GetRunOutcome and the
//     action log. An immediate purge at the terminal write was tried first
//     and rejected: it nils rt.Store(runID) the moment a run completes,
//     breaking hosts that read the run's document afterwards.
//  2. AUDIT HISTORY (runMetas for ListRuns/GetRunMeta, outcomes for
//     GetRunOutcome) is evicted only when the host opts in with
//     Options.RunHistoryTTL; the default keeps it forever, matching the
//     pre-fix audit behavior. After a tier-2 eviction GetRunMeta
//     reconciles the meta back from the log (reconcileRunMeta, S.2).
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
				// H3: whole-run log pruning for backends with the
				// LogPruner capability, when the host opted in.
				rt.pruneLogIfConfigured()
			}
		}
	}()
}

// purgeExpiredRuns drops in-memory bookkeeping for terminal runs whose
// EndTime is older than the applicable horizon: state stores after the
// store grace (RunHistoryTTL or the 10-minute default), and audit
// history (runMetas/outcomes) after RunHistoryTTL when the host set one.
// The rerun index needs no eviction — it derives from the log (S.2).
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
		// H3: an explicit Options.StoreGrace wins for THIS tier only;
		// audit history (tier 2) still follows RunHistoryTTL.
		if rt.storeGrace > 0 {
			storeHorizon = rt.storeGrace
		}
		if now.Sub(ended) >= storeHorizon {
			delete(rt.stores, runID)
		}

		// Tier 2: audit history, only with an explicit TTL.
		if rt.runHistoryTTL > 0 && now.Sub(ended) >= rt.runHistoryTTL {
			delete(rt.runMetas, runID)
			delete(rt.outcomes, runID)
		}
	}
}

// pruneLogIfConfigured (v2.1 H3): when Options.MaxLogAge is set and the
// action log implements LogPruner, delete terminal runs older than the
// horizon. Called from the janitor tick — never on a hot path.
func (rt *WorkflowRuntime) pruneLogIfConfigured() {
	if rt.maxLogAge <= 0 || rt.actionLog == nil {
		return
	}
	pruner, ok := rt.actionLog.(actionlog.LogPruner)
	if !ok {
		return
	}
	n, err := pruner.PruneRuns(context.Background(), time.Now().Add(-rt.maxLogAge))
	if err != nil {
		rt.logger.Warn("retention: action-log prune failed", "error", err)
		return
	}
	if n > 0 {
		rt.logger.Info("retention: pruned terminal runs from the action log", "runs", n, "maxLogAge", rt.maxLogAge)
	}
}

// StorageUsage (v2.1 H3) reports the action log's durable footprint via
// the UsageReporter capability. Typed error when no log is configured or
// the backend lacks the capability — never a silent zero.
func (rt *WorkflowRuntime) StorageUsage(ctx context.Context) (actionlog.LogUsage, error) {
	if rt.actionLog == nil {
		return actionlog.LogUsage{}, core.NewSystemError(core.ErrCodeNotFound, "no action log configured")
	}
	reporter, ok := rt.actionLog.(actionlog.UsageReporter)
	if !ok {
		return actionlog.LogUsage{}, core.NewSystemError(core.ErrCodeInvalidCommand,
			"action log backend does not implement UsageReporter")
	}
	return reporter.StorageUsage(ctx)
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
