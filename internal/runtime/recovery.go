// Recovery (R.4, architecture audit §A): this file is the ONLY code in the
// tree that decides how a run recovers. Before R.4 the decision lived in two
// places (the runtime's Resume type-asserted the log implementation; the
// factory carried its own rebuild-vs-checkpoint branch) and the checkpoint
// fallback degraded guarantees SILENTLY.
//
// The recovery contract:
//
//   - Capability is decided by DATA, not type assertion: a run is
//     event-sourced-rebuildable iff the action log holds entries for it
//     (rebuildCapability probes LatestRerunIndex).
//   - No log entries + a checkpoint in the run store → checkpoint path,
//     LOGGED (never silent).
//   - A failed rebuild degrades loudly: logged fallback to the checkpoint
//     address.
//   - KindCheckpoint entries stay IN the log and fold during replay — the
//     checkpoint is a replay optimization inside this one component, not a
//     parallel recovery system.
//
// Startup rebuild and re-arm (plan S.3) live here too — this is the single
// home they must live in: Recover classifies every enumerated run from the
// log alone and re-arms what a restart must not lose.
package runtime

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/replay"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/asaidimu/hermes/pkg/timeline"
	"github.com/asaidimu/hermes/pkg/watch"
)

// rebuildCapability reports — from DATA — whether the action log can support
// an event-sourced rebuild of runID, and returns the Rebuild func when it
// can. No log, no entries: no rebuild capability.
func (rt *WorkflowRuntime) rebuildCapability(ctx context.Context, runID string, record *workflowRecord, pdef pipeline.PipelineDefinition) pipeline.RebuildFunc {
	if rt.actionLog == nil {
		return nil
	}
	latest, err := rt.actionLog.LatestRerunIndex(ctx, runID)
	if err != nil || latest < 0 {
		return nil
	}
	// @note #review-20260910-003 issue P2 resolved status=resolved priority=P2 tags=#review,#replay : DefinitionResolver answered every id with the root pipeline definition
	// @author hermes-review
	//
	// Resolved: the resolver answers with the definition the id actually
	// names, looked up across the whole compiled workflow
	// (Workflow.FindPipeline), falling back to the run's root definition.
	// Combined with per-pipeline entry partitioning (#review-20260910-015),
	// event-sourced recovery is trustworthy for subpipeline-bearing
	// pipelines. Dynamic distribute children are runtime-generated and are
	// never resolved here — the Replayer regenerates them from the parent
	// stage's DynamicPipelines closure.
	definitionResolver := func(resolvedID string) (*pipeline.PipelineDefinition, bool) {
		if d, ok := record.workflow.FindPipeline(resolvedID); ok {
			return d, true
		}
		if resolvedID == pdef.ID {
			d := pdef
			return &d, true
		}
		return nil, false
	}
	return replay.NewReplayer(rt.actionLog, definitionResolver).Rebuild
}

// resumeRunContext produces the run context a resume executes on. When
// rebuild is non-nil, event-sourced recovery is attempted through the
// factory; any failure logs and falls back to the checkpoint address
// (guarantees degrade loudly, never silently). Returns the context and
// whether the rebuilt store is canonical.
func (rt *WorkflowRuntime) resumeRunContext(
	ctx context.Context,
	runID string,
	pdef pipeline.PipelineDefinition,
	st store.Store,
	bus *events.MemoryScopedBus,
	ckpt *pipeline.PipelineCheckpoint,
	rebuild pipeline.RebuildFunc,
	rerunIndex int,
) (*pipeline.RunContextImpl, bool) {
	factory := pipeline.NewFactory(pdef, pipeline.FactoryOptions{
		Logger:       rt.logger,
		ActionLog:    rt.actionLog,
		Rebuild:      rebuild,
		RerunIndex:   rerunIndex,
		RunEnv:       rt.env,
		SecretLookup: rt.secretLookup(),
	})

	if rebuild != nil {
		runCtx, resumeErr := factory.Resume(ctx, runID, st, bus)
		if resumeErr != nil {
			// Rebuild failed — fall back to the legacy checkpoint path,
			// loudly.
			rt.logger.Warn("resume: event-sourced rebuild failed, falling back to checkpoint",
				"runId", runID, "error", resumeErr)
			return factory.PrepareWithEntry(runID, st, bus, ckpt.ResumeAt), false
		}
		return runCtx, true
	}

	// No rebuild data (no action log, or no entries for this run): the
	// checkpoint in the run store is the recovery source. Logged — the
	// pre-R.4 code degraded silently here.
	rt.logger.Warn("resume: no action-log entries for run — recovering from the checkpoint in run state",
		"runId", runID, "pipelineId", pdef.ID)
	return factory.PrepareWithEntry(runID, st, bus, ckpt.ResumeAt), false
}

// ---------------------------------------------------------------------------
// Startup rebuild (S.3, architecture audit §A flagship fix)
// ---------------------------------------------------------------------------

// pendingPause is a paused run discovered by Recover whose owning workflow
// was not registered yet. The log records facts about RUNS, but resuming
// needs the pipeline's definition, which only exists once the host
// re-registers the workflow after the restart — bindPendingPauses completes
// the paused-registry entry at Register time.
type pendingPause struct {
	runID      string
	pipelineID string
	ckpt       *pipeline.PipelineCheckpoint
}

// Recover rebuilds the runtime's re-arm state from the action log (S.3).
// NewWorkflowRuntime calls it when the configured action log implements
// actionlog.RunEnumerator — without that capability there is nothing to
// enumerate and no restart durability (docs/RECOVERY.md carries the
// per-backend contract).
//
// Each enumerated run is classified by the LAST entry of its latest
// attempt:
//
//   - KindPipelineCompleted / KindPipelineFailed → terminal. The run meta
//     is recorded (eager — one All() read); NOTHING is re-armed: terminal
//     runs never resurrect.
//   - KindCheckpoint → paused. The serialized checkpoint rides in the
//     entry's Output (routing_interp.go writes it there), so the pause can
//     be repopulated from the log alone — waits, wait mode, timeout,
//     received events, cron. The actual re-arm (paused registry, watch
//     registration, cron timer) happens at Register time
//     (bindPendingPauses): a runtime cannot resume a pipeline it has no
//     definition for.
//   - Anything else → in-flight when the process died (no terminal fact,
//     no pause). Meta recorded as recording; the run is NOT auto-continued
//     — without a checkpoint there is no resume address. Its log remains
//     the audit trail, and LatestRerunIndex continues the attempt
//     numbering if the host forces a new attempt.
//
// The state rebuild stays LAZY: Resume already rebuilds through the
// Replayer (rebuildCapability above), so Recover does no replay work. The
// eager part is exactly the re-arming data: run metas now, and at bind
// time the paused-registry entries, watch registrations, and cron timers.
// Cost: one key scan plus one All() per enumerated run, synchronously at
// construction.
//
// Recover is idempotent — runs whose meta is already known are skipped —
// so a host may call it again. Recovering the SAME log into two live
// runtimes double-arms every pause; a log has one owner, and enforcing
// that is the host's responsibility.
func (rt *WorkflowRuntime) Recover(ctx context.Context) {
	if rt.actionLog == nil {
		return
	}
	enum, ok := rt.actionLog.(actionlog.RunEnumerator)
	if !ok {
		return
	}
	refs, err := enum.Runs(ctx)
	if err != nil {
		rt.logger.Warn("recover: action-log enumeration failed; no restart recovery", "error", err)
		return
	}

	latest := make(map[string]int, len(refs))
	for _, ref := range refs {
		// NB: not `ref.RerunIndex > latest[ref.RunID]` — that never
		// assigns for index 0 (a missing key reads 0) and the map stayed
		// empty, silently disabling the whole rebuild.
		if cur, seen := latest[ref.RunID]; !seen || ref.RerunIndex > cur {
			latest[ref.RunID] = ref.RerunIndex
		}
	}
	runIDs := make([]string, 0, len(latest))
	for runID := range latest {
		runIDs = append(runIDs, runID)
	}
	sort.Strings(runIDs)

	var pending []*pendingPause
	terminal, inFlight := 0, 0
	for _, runID := range runIDs {
		// Idempotency: a run this runtime already knows about (live,
		// paused, or previously recovered) is not reclassified.
		rt.mu.Lock()
		known := len(rt.runMetas) > 0 && rt.runMetas[runID] != nil
		rt.mu.Unlock()
		if known {
			continue
		}

		entries, err := rt.actionLog.All(ctx, runID, latest[runID])
		if err != nil || len(entries) == 0 {
			rt.logger.Warn("recover: run entries unreadable; run left unclassified", "runId", runID, "error", err)
			continue
		}
		last := entries[len(entries)-1]
		meta := metaFromEntries(runID, entries)

		switch {
		case last.Kind == actionlog.KindPipelineCompleted || last.Kind == actionlog.KindPipelineFailed:
			terminal++
		case last.Kind == actionlog.KindCheckpoint:
			meta.Status = timeline.StatusPaused
			var ckpt pipeline.PipelineCheckpoint
			if len(last.Output) > 0 && json.Unmarshal(last.Output, &ckpt) == nil && ckpt.PipelineID != "" {
				pending = append(pending, &pendingPause{runID: runID, pipelineID: ckpt.PipelineID, ckpt: &ckpt})
			} else {
				rt.logger.Warn("recover: checkpoint entry has no parseable checkpoint; paused run cannot be re-armed",
					"runId", runID, "pipelineId", last.PipelineID)
			}
		default:
			inFlight++
			rt.logger.Warn("recover: run was in-flight when the process died; not auto-continued (no checkpoint to resume from)",
				"runId", runID, "pipelineId", last.PipelineID, "lastKind", string(last.Kind))
		}
		rt.putRunMetaIfAbsent(meta)
	}

	rt.mu.Lock()
	if rt.pendingPauses == nil {
		rt.pendingPauses = make(map[string]*pendingPause)
	}
	for _, p := range pending {
		if _, exists := rt.pendingPauses[p.runID]; !exists {
			rt.pendingPauses[p.runID] = p
		}
	}
	rt.mu.Unlock()

	if len(runIDs) > 0 {
		rt.logger.Info("recover: action-log scan complete",
			"runs", len(runIDs), "terminal", terminal, "pendingPause", len(pending), "inFlight", inFlight)
	}
}

// bindPendingPauses completes the paused-registry entries Recover staged,
// for the pipelines this newly registered workflow defines. The runtime
// cannot resume a pipeline it has no definition for, so a pause recovered
// from the log waits here — invisible but never lost — until its workflow
// registers. Binding is first-match: a log entry does not name the owning
// workflow (the action log records run facts, not registration facts), so
// the first registered workflow whose FindPipeline resolves the paused
// pipeline id claims it. Hosts that reuse one pipeline id across workflows
// must register them in a deterministic order.
func (rt *WorkflowRuntime) bindPendingPauses(record *workflowRecord) {
	if record == nil || record.workflow == nil {
		return
	}
	rt.mu.Lock()
	var bound []*pendingPause
	for runID, p := range rt.pendingPauses {
		if workflowDefinesPipeline(record.workflow, p.pipelineID) {
			bound = append(bound, p)
			delete(rt.pendingPauses, runID)
		}
	}
	rt.mu.Unlock()

	for _, p := range bound {
		rt.rearmPausedRun(record, p.runID, p.pipelineID, p.ckpt)
	}
	if len(bound) > 0 {
		rt.logger.Info("recover: re-armed paused runs for registered workflow",
			"workflowId", record.workflow.ID, "count", len(bound))
	}
}

// workflowDefinesPipeline reports whether wf defines the pipeline id a
// recovered pause is paused on. Three layouts must match: compiled
// workflows key Pipelines by the trigger id and the definition's ID equals
// it (key hit); hand-built workflows key Pipelines by trigger id with a
// distinct root definition ID (root-ID scan); pauses inside subpipelines
// name the child definition (FindPipeline's nested search).
func workflowDefinesPipeline(wf *pipeline.Workflow, pipelineID string) bool {
	if _, ok := wf.Pipelines[pipelineID]; ok {
		return true
	}
	for _, def := range wf.Pipelines {
		if def.ID == pipelineID {
			return true
		}
	}
	_, ok := wf.FindPipeline(pipelineID)
	return ok
}

// rearmPausedRun repopulates the paused registry for a run recovered from
// the log, mirroring spawnRun's pause bookkeeping exactly (S.3): watch
// registration (pre-pause buffering + bus subscription for the wait event
// types), the paused entry + dispatch index, buffered-event delivery, and
// the cron auto-resume — identity-gated, so a buffered event that resumed
// and re-paused the run on a NEWER wait is not overwritten by a stale arm.
// The rebuild itself stays lazy: Resume rebuilds through the Replayer.
func (rt *WorkflowRuntime) rearmPausedRun(record *workflowRecord, runID, pipelineID string, ckpt *pipeline.PipelineCheckpoint) {
	var eventTypes []string
	if len(ckpt.WaitForEvents) > 0 {
		eventTypes = ckpt.WaitForEvents
	} else if ckpt.WaitForEvent != "" {
		eventTypes = []string{ckpt.WaitForEvent}
	}
	if len(eventTypes) == 0 {
		rt.logger.Warn("recover: recovered pause waits for nothing; leaving it un-armed",
			"runId", runID, "pipelineId", pipelineID)
		return
	}

	// Placeholder store: Resume's event-sourced path replaces it with the
	// Replayer's rebuilt store; the checkpoint is seeded so a degraded
	// (checkpoint-fallback) resume still finds its address in-state.
	st := store.NewMemoryStore(nil)
	if err := st.Update(context.Background(), func(state map[string]any) error {
		return pipeline.WriteCheckpoint(state, *ckpt)
	}); err != nil {
		rt.logger.Warn("recover: failed to seed the placeholder checkpoint", "runId", runID, "error", err)
	}

	if err := rt.watchService.Register(runID, watch.WatchDescriptor{
		EventTypes: eventTypes,
		Mode:       ckpt.WaitMode,
		Timeout:    ckpt.Timeout,
	}); err != nil {
		rt.logger.Error("recover: failed to register watch for recovered pause", "runId", runID, "error", err)
	}

	rt.mu.Lock()
	if _, exists := rt.paused[runID]; exists {
		// Already armed (double Recover, or a live run with this id —
		// impossible for minted UUIDs, but cheap to guard).
		rt.mu.Unlock()
		return
	}
	newPaused := &pausedRun{
		runID:         runID,
		workflowID:    record.workflow.ID,
		triggerID:     rt.bindTriggerID(record, pipelineID),
		pipelineID:    pipelineID,
		waitForEvent:  ckpt.WaitForEvent,
		waitForEvents: ckpt.WaitForEvents,
		waitMode:      ckpt.WaitMode,
		store:         st,
		checkpoint:    ckpt,
	}
	rt.paused[runID] = newPaused
	rt.pauseIndexPutLocked(newPaused)
	if meta, ok := rt.runMetas[runID]; ok {
		meta.Status = timeline.StatusPaused
	}
	rt.mu.Unlock()

	// Deliver anything buffered for this wait's event types, then arm
	// the cron auto-resume only if the run is STILL paused on THIS
	// checkpoint (the delivery may have resumed → re-paused on a newer
	// wait, or completed the run — C5/H5 ordering, same as spawnRun).
	if bufferedEvent := rt.watchService.OnRunPaused(runID); bufferedEvent != nil {
		rt.Resume(runID, bufferedEvent.Patch)
	}
	rt.mu.Lock()
	current := rt.paused[runID]
	samePause := current != nil && current.checkpoint == ckpt
	rt.mu.Unlock()
	if samePause {
		rt.armCronAutoResume(runID, ckpt)
	}
}

// bindTriggerID resolves the trigger id a recovered pause reports. The log
// does not record it; compiled layouts key Pipelines by the trigger id with
// the definition's ID equal to it (so the checkpoint's pipeline id IS the
// trigger id), while hand-built layouts key Pipelines by trigger id with a
// distinct definition ID — find the trigger whose pipeline is the paused
// one. Falls back to the pipeline id (Resume's #022 fallback then applies).
func (rt *WorkflowRuntime) bindTriggerID(record *workflowRecord, pipelineID string) string {
	wf := record.workflow
	if _, ok := wf.Pipelines[pipelineID]; ok {
		return pipelineID
	}
	for tid, def := range wf.Pipelines {
		if def.ID == pipelineID {
			return tid
		}
	}
	return pipelineID
}

// armCronAutoResume schedules the cron auto-resume for a pause. The timer
// is identity-gated: it resumes only if the run is still paused on THIS
// checkpoint — a superseded timer is a no-op (C5/H5) — and stamps
// ResumeReason "cron" before resuming. Shared by spawnRun, Resume's
// re-pause arm, and the S.3 re-arm.
func (rt *WorkflowRuntime) armCronAutoResume(runID string, ckpt *pipeline.PipelineCheckpoint) {
	if ckpt == nil || ckpt.Cron == "" {
		return
	}
	if err := rt.scheduler.Schedule(runID, ckpt.Cron, func(ctx context.Context) {
		rt.mu.Lock()
		paused, stillPaused := rt.paused[runID]
		if stillPaused && paused.checkpoint != ckpt {
			stillPaused = false
		}
		if stillPaused && paused.checkpoint != nil {
			paused.checkpoint.ResumeReason = "cron"
		}
		rt.mu.Unlock()
		if stillPaused {
			rt.Resume(runID, nil)
		}
	}); err != nil {
		rt.logger.Error("failed to schedule cron auto-resume", "runId", runID, "error", err)
	}
}
