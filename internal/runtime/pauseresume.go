// Resume: the event-sourced (or checkpoint-fallback) continuation of a
// paused run, including full re-pause bookkeeping. Split out of runtime.go
// by R.1 (pure move).
package runtime

import (
	"context"
	"github.com/asaidimu/hermes/pkg/watch"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
)

// Resume resumes a paused pipeline by runID with the event payload that arrived.
// The pipeline resumes from its checkpoint entry address.
func (rt *WorkflowRuntime) Resume(runID string, payload map[string]any) RunResult {
	rt.mu.Lock()
	paused, ok := rt.paused[runID]
	if !ok {
		rt.mu.Unlock()
		return RunResult{
			OK:     false,
			Status: "failed",
			Error:  core.NewSystemError(core.ErrCodeNotFound, "no paused run found for "+runID),
		}
	}
	delete(rt.paused, runID)
	rt.pauseIndexDeleteLocked(paused)
	record := rt.workflows[paused.workflowID]
	st := paused.store
	ckpt := paused.checkpoint
	rt.mu.Unlock()

	if record == nil {
		return RunResult{
			OK:     false,
			Status: "failed",
			Error:  core.NewSystemError(core.ErrCodeNotFound, "workflow "+paused.workflowID+" no longer registered"),
		}
	}

	// H1.2: the workflow may have been re-registered (Deregister+Register)
	// with edits while this run sat paused. Under "require-same" the resume
	// is refused BEFORE any state mutation or log write — replaying the
	// pause against a drifted definition would reconstruct wrong state.
	// The refusal is NON-destructive: the paused entry (and its dispatch
	// index registration) is restored, so registering the matching workflow
	// makes the same pause resumable again — a guard, not a deletion.
	if err := rt.checkDefinitionDrift(runID, paused.defHash, record, "resume"); err != nil {
		rt.mu.Lock()
		if _, stillGone := rt.paused[runID]; !stillGone {
			rt.paused[runID] = paused
			rt.pauseIndexPutLocked(paused)
		}
		rt.mu.Unlock()
		return RunResult{
			RunID:      runID,
			WorkflowID: paused.workflowID,
			TriggerID:  paused.triggerID,
			PipelineID: paused.pipelineID,
			OK:         false,
			Status:     "failed",
			Error:      err,
		}
	}

	// Fold the resume event payload directly into state so each pause's
	// data is accessible without overwriting previous payloads. M7 (plan
	// P4.9): ONE Update writes every key — the loop issued one lock-
	// acquiring store Update per payload key. Store update errors are
	// still logged, not silently discarded (see the resolved
	// #review-20260822-048): SetValue mutators cannot fail on either
	// shipped Store implementation, so the per-key "continue on failure"
	// semantics collapse into the whole-fold case without behavior
	// change.
	if payload != nil {
		if err := st.Update(context.Background(), func(state map[string]any) error {
			for key, val := range payload {
				state[key] = val
			}
			return nil
		}); err != nil {
			rt.logger.Error("resume: failed to fold event payload into state", "runId", runID, "error", err)
		}
	}

	// Write the resume reason into state so routers can distinguish event vs timeout.
	if ckpt.ResumeReason != "" {
		_ = st.Update(context.Background(), store.SetValue("__resume_reason__", ckpt.ResumeReason))
	}

	pdef := record.workflow.Pipelines[paused.pipelineID]
	if len(pdef.Stages) == 0 {
		// @note #review-20260910-022 issue P1 resolved status=resolved priority=P1 tags=#review,#resume,#bug : Resume resolves the pipeline definition with the wrong map key, silently succeeding without executing anything
		// @author hermes-review
		// @see #review-20260910-001
		//
		// Resolved: workflow.Pipelines is keyed by trigger id for compiled
		// workflows (where the pipeline definition's own ID equals the trigger
		// node id, so the old lookup worked by coincidence), but hand-built
		// workflows may key it only by the pipeline's ID. With the recorded
		// pipelineID missing from the map, pdef was the ZERO definition: the
		// resume scoped its bus with an empty label, the Replayer rebuilt
		// nothing ("run already completed"), and the fallback prepared a run
		// context with zero stages — which emitted pipeline:start followed
		// immediately by pipeline:success, reporting "succeeded" while
		// executing NOTHING after the pause. Resume now falls back to the
		// trigger-id key (the key the dispatch/executePipeline path itself
		// uses) and every use below reads the resolved pdef.
		if alt, ok := record.workflow.Pipelines[paused.triggerID]; ok && len(alt.Stages) > 0 {
			pdef = alt
		}
	}
	// @note #scoped-bus-opportunity-003 issue P2 resolved status=resolved priority=P2 tags=#event-bus,#isolation : Empty EventPath scoping provides no real isolation
	//
	// Resolved: scope the per-run bus with a real ["pipeline", pipelineID, runID]
	// path node instead of an empty EventPath{}. This does not add topic-level
	// routing isolation (MemoryScopedBus still dispatches through its own
	// synchronous handlers map, bubbling to parent — see the note on
	// scoped-bus-opportunity-001 for why a wholesale ScopedBus swap isn't
	// safe here), but it does give every event a real, inspectable Path
	// identifying which run produced it, instead of an empty placeholder —
	// which is what PipelineEvent.Path is actually consumed for (the
	// frontend timeline).
	bus := rt.bus.Scope(events.EventPath{}.Append("pipeline", runID, pdef.Label))

	// R.4: recovery.go is the single decision point — capability by data
	// (log entries for this run), rebuild attempt, logged checkpoint
	// fallback. Resume itself no longer decides anything.
	rebuild := rt.rebuildCapability(context.Background(), runID, record, pdef)

	// Advance the rerun index for this attempt — see the resolved
	// #review-20260910-006 note on executePipeline's rerunIndex seed above
	// for why this must be recomputed here rather than reused. S.2: the
	// index derives from the log (nextRerunIndex) — no RAM copy.
	rerunIndex := rt.nextRerunIndex(context.Background(), runID)

	runCtx, rebuiltStoreUsed := rt.resumeRunContext(context.Background(), runID, pdef, st, bus, ckpt, rebuild, rerunIndex, pipeline.WorkflowFingerprint(record.workflow))

	// @note #review-20260910-002 issue P1 resolved status=resolved priority=P1 tags=#review,#replay,#state : Replayer-path resume executes on a rebuilt store but reports and re-pauses with the stale original
	// @author hermes-review
	// @see #review-20260910-015
	//
	// Resolved: after a successful Rebuilder-path resume, the rebuilt store
	// returned by factory.Resume (bound into runCtx) is adopted as the
	// canonical store: the local st, rt.stores[runID], and — via the re-pause
	// tracking below — any subsequent pausedRun.store all reference it. So
	// RunResult.FinalState now reflects the completed segment (not the state
	// as of the pause), and on a multi-pause run the next resume's
	// copyStoreState only overlays values at least as fresh as the replay
	// itself, instead of reverting every key to the previous pause's values.
	// The legacy checkpoint path is unaffected: it reuses st directly.
	if rebuiltStoreUsed {
		if rebuilt := runCtx.Store(); rebuilt != nil {
			st = rebuilt
			rt.mu.Lock()
			rt.stores[runID] = st
			rt.mu.Unlock()
		}
	}

	// Record the resume in the action log so the Replayer knows this run
	// is no longer paused.
	if rt.actionLog != nil {
		rt.actionLog.Append(context.Background(), actionlog.Entry{
			RunID:      runID,
			RerunIndex: rerunIndex,
			PipelineID: paused.pipelineID,
			Kind:       actionlog.KindResumed,
			Type:       "pipeline:resumed",
			StageID:    ckpt.ResumeAt.Stage,
		})
	}

	// The rebuilt store was adopted as the canonical store above (see the
	// resolved #review-20260910-002 note): st, rt.stores[runID], and the
	// re-pause pausedRun.store all reference it.
	resolver, cleanup := rt.initResources(record, bus, runID, paused.pipelineID)
	if len(resolver) > 0 {
		runCtx.SetResourceResolver(func(key string) (any, bool) {
			v, ok := resolver[key]
			return v, ok
		})
	}

	rt.mu.Lock()
	rt.active[runID] = runCtx
	rt.mu.Unlock()

	res, runErr := runCtx.Run(context.Background())

	cleanup()
	rt.clearActive(runID)

	// M1 (plan P4.9): reuse the segment's own final-state snapshot (see
	// the dispatch.go twin of this change).
	finalState := res.FinalState
	if finalState == nil {
		finalState, _ = st.ExportJSON()
	}

	result := RunResult{
		RunID:      runID,
		WorkflowID: paused.workflowID,
		TriggerID:  paused.triggerID,
		PipelineID: paused.pipelineID,
		FinalState: finalState,
	}
	if runErr != nil {
		result.OK = false
		result.Status = "failed"
		if res.Status == "aborted" {
			result.Status = "aborted"
		}
		result.Error = runErr
	} else {
		result.OK = true
		result.Status = res.Status
		if result.Status == "" {
			result.Status = "succeeded"
		}
	}

	// Record the segment outcome and status before re-pause bookkeeping below:
	// if the run paused again and a buffered event triggers another resume
	// immediately, that resume's final outcome must land after this write.
	rt.mu.Lock()
	rt.outcomes[runID] = result
	rt.mu.Unlock()
	rt.setRunStatus(runID, result.Status)

	// @note #review-20260910-001 issue P1 resolved status=resolved priority=P1 tags=#review,#resume,#bug : Re-pause after resume drops multi-event waits and skips watch/cron re-registration
	// @author hermes-review
	//
	// Resolved: the re-pause tracking now mirrors spawnRun's pause handling
	// in full. (1) The guard accepts multi-event waits (WaitForEvents +
	// WaitMode), not just single WaitForEvent, so a re-paused PauseForEvents
	// run is tracked instead of being recorded as paused with nothing waiting
	// for it. (2) waitForEvents/waitMode are propagated onto the pausedRun so
	// dispatch can match (and accumulate ReceivedEvents for) a re-paused
	// multi-event wait. (3) The WatchService registration for the new wait's
	// event types is re-done (restoring bus subscriptions and pre-pause event
	// buffering), OnRunPaused drains anything buffered while the segment ran,
	// and the cron-based auto-resume is re-armed from the new checkpoint —
	// with a stale schedule from the previous pause canceled when the new one
	// carries no cron. The buffered-event resume runs in its own goroutine:
	// a synchronous call would recurse inside this Resume frame, and this
	// frame's paused outcome must not overwrite the final one (the outcome
	// write above happens before the goroutine is spawned, so ordering holds).
	if result.Status == "paused" && res.Checkpoint != nil &&
		(res.WaitForEvent != "" || len(res.WaitForEvents) > 0) {
		var eventTypes []string
		if len(res.WaitForEvents) > 0 {
			eventTypes = res.WaitForEvents
		} else {
			eventTypes = []string{res.WaitForEvent}
		}

		// Re-register pre-pause event buffering for the new wait.
		if err := rt.watchService.Register(runID, watch.WatchDescriptor{
			EventTypes: eventTypes,
			Mode:       res.WaitMode,
			Timeout:    res.Checkpoint.Timeout,
		}); err != nil {
			rt.logger.Error("resume: failed to register watch for re-paused run", "runId", runID, "error", err)
		}

		rt.mu.Lock()
		repaused := &pausedRun{
			runID:         runID,
			workflowID:    paused.workflowID,
			triggerID:     paused.triggerID,
			pipelineID:    paused.pipelineID,
			defHash:       pipeline.WorkflowFingerprint(record.workflow),
			waitForEvent:  res.WaitForEvent,
			waitForEvents: res.WaitForEvents,
			waitMode:      res.WaitMode,
			store:         st,
			checkpoint:    res.Checkpoint,
		}
		rt.paused[runID] = repaused
		rt.pauseIndexPutLocked(repaused)
		rt.mu.Unlock()

		// H2.2: persist the fold snapshot at the re-pause (before buffered
		// delivery — same C5 ordering as the spawn path).
		rt.writeFoldSnapshot(runID, st, repaused.defHash)

		// Deliver any event that arrived while the resumed segment was
		// executing, then re-arm (or drop) the cron auto-resume.
		if bufferedEvent := rt.watchService.OnRunPaused(runID); bufferedEvent != nil {
			// C5-fix (20261003): resume dispatch is uniformly
			// synchronous — the nested resume completes before this
			// frame proceeds, so the cron arm below reads the FINAL
			// pause state. The old `go rt.Resume` raced this arm
			// against the nested re-pause (L3) and left chained
			// re-pause ordering nondeterministic. Sync is the
			// direction the pinned contract at NewWorkflowRuntime's
			// watch callback already requires (Emit must not return
			// before a chained re-pause settles — the f1e5630
			// revert was async-ward, this is the opposite); the
			// posting-serializer variant was rejected because it
			// breaks that pinned contract or deadlocks on it.
			// Ordering is safe: the paused outcome was written
			// above, so a synchronous nested chain can only append
			// the final outcome after it.
			rt.Resume(runID, bufferedEvent.Patch)
		}

		// C5-fix (20261003): arm or drop the cron auto-resume only if
		// the run is STILL paused on THIS checkpoint after buffered
		// delivery settled. If the delivery re-paused the run on a
		// newer wait, that path owns the schedule; a stale arm here
		// could fire during the newer wait and resume it as if
		// cron-authorized (H5 ordering).
		rt.mu.Lock()
		current := rt.paused[runID]
		samePause := current != nil && current.checkpoint == res.Checkpoint
		rt.mu.Unlock()
		if samePause {
			if res.Checkpoint.Cron != "" {
				rt.armCronAutoResume(runID, res.Checkpoint)
			} else {
				// The previous pause may have armed a cron schedule; the new wait
				// must not be resumed prematurely by that stale schedule.
				rt.scheduler.Cancel(runID)
			}
		}
	}

	// Don't call OnComplete if the run paused again — the next resume event
	// will trigger completion.
	if result.Status != "paused" {
		// C3-fix (20261003): a terminal outcome must cancel the cron
		// auto-resume the pause armed (H9). Pre-fix, every run that paused
		// with Checkpoint.Cron and then completed leaked a permanent
		// self-re-arming timer grabbing rt.mu on every fire. Timers belong
		// to the scheduler; orchestration releases what it asked it to hold.
		rt.scheduler.Cancel(runID)
		// Clean up watch service when run completes
		rt.watchService.OnRunEnded(runID)
		rt.callOnComplete(record, result)
	}
	return result
}
