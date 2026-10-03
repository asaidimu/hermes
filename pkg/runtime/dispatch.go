// Event dispatch and run entry paths: bus-event fan-out to triggers and
// paused runs, spawn/execute/invoke/abort, and the compile-and-run
// convenience. Split out of runtime.go by R.1 (pure move).
package runtime

import (
	"context"
	"fmt"
	"time"

	"github.com/asaidimu/hermes/pkg/compiler"
	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/asaidimu/hermes/pkg/timeline"
	"github.com/asaidimu/hermes/pkg/watch"
)

// dispatch fans a bus event out to every matching trigger and spawns runs in
// the background (mirrors WorkflowRuntime.dispatch + spawnRun). It also resumes
// any paused runs that are waiting for this event type.
func (rt *WorkflowRuntime) dispatch(eventType string, evt events.PipelineEvent) {
	// @note #review-20260822-039 issue P1 resolved status=resolved priority=P1 tags=#review,#concurrency : TOCTOU race in dispatch between lock acquisitions
	//
	// Resolved: Snapshot toResume candidates, route entries, and workflow records under a single lock acquisition.
	type matchedRoute struct {
		trigger   pipeline.WorkflowTrigger
		triggerID string
		record    *workflowRecord
	}

	rt.mu.Lock()
	var toResume []*pausedRun
	for _, pr := range rt.paused {
		// Single event match (backward compat).
		if pr.waitForEvent == eventType {
			toResume = append(toResume, pr)
			continue
		}
		// Multi-event match: check if this event is in the wait list.
		if len(pr.waitForEvents) > 0 {
			for _, we := range pr.waitForEvents {
				if we == eventType {
					// Record the received event.
					if pr.checkpoint.ReceivedEvents == nil {
						pr.checkpoint.ReceivedEvents = []string{}
					}
					// Avoid duplicates.
					found := false
					for _, re := range pr.checkpoint.ReceivedEvents {
						if re == eventType {
							found = true
							break
						}
					}
					if !found {
						pr.checkpoint.ReceivedEvents = append(pr.checkpoint.ReceivedEvents, eventType)
					}
					// Check if condition is met.
					if rt.multiEventConditionMet(pr) {
						toResume = append(toResume, pr)
					}
					break
				}
			}
		}
	}

	bucket := rt.index[eventType]
	routes := make([]matchedRoute, 0, len(bucket))
	for _, entry := range bucket {
		if rec, ok := rt.workflows[entry.workflowID]; ok && rec != nil {
			routes = append(routes, matchedRoute{
				trigger:   entry.trigger,
				triggerID: entry.triggerID,
				record:    rec,
			})
		}
	}
	rt.mu.Unlock()

	for _, pr := range toResume {
		go func(p *pausedRun) {
			rt.Resume(p.runID, evt.Payload)
		}(pr)
	}

	if len(routes) == 0 && len(toResume) == 0 {
		return
	}
	evt.Type = eventType
	if evt.Timestamp == 0 {
		evt.Timestamp = time.Now().UnixMilli()
	}

	for _, r := range routes {
		matched := true
		if r.trigger.Predicate != nil {
			matched = r.trigger.Predicate(evt)
		}
		if !matched {
			continue
		}

		ok, release := r.record.gate.tryAcquire()
		if !ok {
			rt.logger.Warn(`[WorkflowRuntime] run rejected by execution gate for workflow "` + r.record.workflow.ID + `"`)
			continue
		}
		go func(rec *workflowRecord, tid string, ev events.PipelineEvent) {
			defer release()
			rt.spawnRun(rec, tid, ev)
		}(r.record, r.triggerID, evt)
	}
}

// multiEventConditionMet checks if a multi-event pause condition is satisfied.
func (rt *WorkflowRuntime) multiEventConditionMet(pr *pausedRun) bool {
	received := pr.checkpoint.ReceivedEvents
	if received == nil {
		received = []string{}
	}
	if pr.waitMode == "all" {
		// All events must be received.
		return len(received) >= len(pr.waitForEvents)
	}
	// Default: "any" — at least one event received.
	return len(received) > 0
}

// spawnRun executes a pipeline for a trigger and fires onComplete. It runs
// synchronously within its goroutine; callers dispatch it in the background.
func (rt *WorkflowRuntime) spawnRun(record *workflowRecord, triggerID string, evt events.PipelineEvent) {
	result := rt.executePipeline(record, triggerID, evt)
	runID := result.RunID
	if result.Status == "paused" {
		// Track the paused run so it can be resumed when the awaited event(s) arrive.
		if result.Checkpoint != nil && (result.WaitForEvent != "" || len(result.WaitForEvents) > 0) {
			timeoutMs := result.Checkpoint.Timeout

			// Determine which event types to subscribe to.
			var eventTypes []string
			if len(result.WaitForEvents) > 0 {
				eventTypes = result.WaitForEvents
			} else {
				eventTypes = []string{result.WaitForEvent}
			}

			// Register with WatchService (pre-pause buffering)
			if err := rt.watchService.Register(runID, watch.WatchDescriptor{
				EventTypes: eventTypes,
				Mode:       result.WaitMode,
				Timeout:    timeoutMs,
			}); err != nil {
				rt.logger.Error("spawnRun: failed to register watch for paused run", "runId", runID, "error", err)
			}

			rt.mu.Lock()
			rt.paused[runID] = &pausedRun{
				runID:         runID,
				workflowID:    record.workflow.ID,
				triggerID:     triggerID,
				pipelineID:    result.PipelineID,
				waitForEvent:  result.WaitForEvent,
				waitForEvents: result.WaitForEvents,
				waitMode:      result.WaitMode,
				store:         rt.stores[runID],
				checkpoint:    result.Checkpoint,
			}
			if meta, ok := rt.runMetas[runID]; ok {
				meta.Status = timeline.StatusPaused
			}
			rt.mu.Unlock()

			// Notify WatchService that run is now paused
			if bufferedEvent := rt.watchService.OnRunPaused(runID); bufferedEvent != nil {
				// Buffered event found - resume immediately
				rt.Resume(runID, bufferedEvent.Patch)
			}

			// If a cron expression is set, schedule the resume using the scheduler.
			if result.Checkpoint.Cron != "" {
				rt.scheduler.Schedule(runID, result.Checkpoint.Cron, func(ctx context.Context) {
					// Only resume if the run is still paused.
					rt.mu.Lock()
					paused, stillPaused := rt.paused[runID]
					if stillPaused && paused.checkpoint != nil {
						paused.checkpoint.ResumeReason = "cron"
					}
					rt.mu.Unlock()
					if stillPaused {
						rt.Resume(runID, nil)
					}
				})
			}
		}
		return
	}
	rt.callOnComplete(record, result)
}

// Invoke runs a trigger's pipeline directly and awaits the result (mirrors
// WorkflowRuntime.invoke). It bypasses the event bus and execution gate.
func (rt *WorkflowRuntime) Invoke(workflowID, triggerID string, evt events.PipelineEvent) RunResult {
	rt.mu.Lock()
	record := rt.workflows[workflowID]
	rt.mu.Unlock()
	if record == nil {
		return RunResult{
			OK:     false,
			Status: "failed",
			Error:  core.NewSystemError(core.ErrCodeNotFound, `[WorkflowRuntime] invoke() failed: workflow "`+workflowID+`" is not registered.`),
		}
	}
	if _, ok := record.workflow.Pipelines[triggerID]; !ok {
		return RunResult{
			OK:         false,
			WorkflowID: workflowID,
			Status:     "failed",
			Error: core.NewSystemError(core.ErrCodeNotFound,
				`[WorkflowRuntime] invoke() failed: no pipeline definition for workflow "`+workflowID+`" trigger "`+triggerID+`".`),
		}
	}
	result := rt.executePipeline(record, triggerID, evt)
	if result.Status != "paused" {
		rt.callOnComplete(record, result)
	}
	return result
}

// AbortRun aborts a running pipeline (mirrors WorkflowRuntime.abortRun).
func (rt *WorkflowRuntime) AbortRun(runID string) {
	rt.mu.Lock()
	ctx := rt.active[runID]
	rt.mu.Unlock()
	if ctx != nil {
		ctx.Abort(core.NewSystemError(core.ErrCodeAbort, "pipeline execution aborted"))
	}
}

// Run compiles a workflow graph, registers it, emits the manual trigger event,
// and awaits the run outcome. Used by the HTTP server adapter (Phase 7).
func (rt *WorkflowRuntime) Run(ctx context.Context, nodes []compiler.Node, edges []compiler.Edge, opts ...RunOptions) (RunResult, error) {
	var opt RunOptions
	if len(opts) > 0 {
		opt = opts[0]
	}

	wf, err := compiler.Compile(nodes, edges, opt.Registry)
	if err != nil {
		return RunResult{}, err
	}

	done := make(chan RunResult, 1)
	if err := rt.Register(wf, RegisterOptions{
		Mode:      Mode{Type: "transient"},
		OnPrepare: opt.OnPrepare,
		OnComplete: func(res RunResult) {
			done <- res
			if opt.OnComplete != nil {
				opt.OnComplete(res)
			}
		},
	}); err != nil {
		return RunResult{}, err
	}
	defer rt.Deregister(wf.ID)

	rt.bus.Emit(ctx, ManualEvent, events.PipelineEvent{Payload: map[string]any{}})

	timeout := opt.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	// @note #review-20260822-038 issue P1 resolved status=resolved priority=P1 tags=#review,#bug : Timer leak in select
	//
	// Resolved: Use time.NewTimer with explicit defer timer.Stop().
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case res := <-done:
		return res, nil
	case <-ctx.Done():
		return RunResult{}, ctx.Err()
	case <-timer.C:
		return RunResult{}, core.NewSystemError(core.ErrCodeTimeout, "workflow run timed out")
	}
}

// ---------------------------------------------------------------------------
// Pipeline execution
// ---------------------------------------------------------------------------

func (rt *WorkflowRuntime) executePipeline(record *workflowRecord, triggerID string, evt events.PipelineEvent) RunResult {
	wf := record.workflow
	def := wf.Pipelines[triggerID]
	if def.ID == "" {
		return rt.finishRun(record, RunResult{
			WorkflowID: wf.ID,
			TriggerID:  triggerID,
			Status:     "failed",
			Error:      core.NewSystemError(core.ErrCodeNotFound, "no pipeline definition for trigger "+triggerID),
		})
	}

	// Creating the state document creates the run: its minted _id_ is the run id.
	st, err := rt.newStore()
	if err != nil || st == nil {
		return rt.finishRun(record, RunResult{
			WorkflowID: wf.ID,
			TriggerID:  triggerID,
			Status:     "failed",
			Error:      core.NewSystemError(core.ErrCodeExecutionFailed, "failed to create store for run: "+fmt.Sprintf("%v", err)),
		})
	}
	runID := st.ID()

	// @note #review-20260910-006 observation P3 resolved status=resolved priority=P3 tags=#review,#audit : rerunIndex is never incremented, so resume attempts share one audit trail
	// @author hermes-review
	//
	// Resolved: this seeds attempt 0 for a brand-new run, which was always
	// correct on its own. The gap was that Resume re-read rt.rerunIndexOf
	// instead of calling nextRerunIndex again, so every forced recovery
	// reused attempt 0's audit trail. Resume now calls nextRerunIndex and
	// persists the advanced value into rt.rerunIdx on each resume, so
	// LatestRerunIndex advances past 0 and each retry gets its own trail,
	// honoring the FactoryOptions.RerunIndex contract end to end.
	rerunIndex := rt.nextRerunIndex(context.Background(), runID)

	// Per-run bookkeeping registration. Eviction policy: see the resolved
	// #review-20260910-004 note attached to purgeExpiredRuns — terminal
	// runs' stores/rerunIdx are reclaimed by the janitor after the grace
	// horizon; runMetas/outcomes history is bounded by Options.RunHistoryTTL.
	rt.mu.Lock()
	rt.stores[runID] = st
	rt.rerunIdx[runID] = rerunIndex
	rt.runMetas[runID] = &timeline.RunTimelineMeta{
		RunID:      runID,
		PipelineID: def.ID,
		StartTime:  time.Now().UnixMilli(),
		Status:     timeline.StatusRecording,
	}
	rt.mu.Unlock()

	// The run's bus scopes under the root bus so all pipeline events bubble to
	// the runtime bus (external subscribers and timeline recorder see them).
	// @note #scoped-bus-opportunity-003 issue status=resolved priority=P2 tags=#event-bus,#isolation : Empty EventPath scoping provides no real isolation
	//
	// Resolved: see the matching note in Resume() above — scope with a real
	// ["pipeline", runID, label] path node instead of an empty EventPath{},
	// so PipelineEvent.Path (consumed by the frontend timeline) identifies
	// the run that produced each event.
	bus := rt.bus.Scope(events.EventPath{}.Append("pipeline", runID, def.Label))

	factory := pipeline.NewFactory(def, pipeline.FactoryOptions{
		Logger:       rt.logger,
		ActionLog:    rt.actionLog,
		RerunIndex:   rerunIndex,
		RunEnv:       rt.env,
		SecretLookup: rt.secretLookup(),
	})
	runCtx := factory.Prepare(runID, st, bus)

	// Seed run linkage so crash recovery can resolve the workflow directly,
	// plus the trigger event metadata. Payload keys become top-level state
	// fields so configs can address them with the standardized dotted path
	// (e.g. `status`, `userId`).
	// @note #review-20260822-047 issue status=resolved priority=P2 tags=#review,#error-handling : Store update errors discarded when seeding trigger metadata
	//
	// Resolved: log each discarded error instead of ignoring it, matching
	// the same best-effort-but-now-visible choice used for the other
	// discarded-store-update notes in this file (review-20260822-048) and
	// pkg/pipeline/context.go (review-20260822-033, review-20260825-001).
	if err := st.Update(context.Background(), store.SetValue(store.RunMetaKey, map[string]any{
		"workflowId": wf.ID,
		"triggerId":  triggerID,
		"pipelineId": def.ID,
	})); err != nil {
		rt.logger.Error("trigger: failed to seed run metadata", "runId", runID, "workflowId", wf.ID, "error", err)
	}
	if err := st.Update(context.Background(), store.SetValue("__trigger_event__", map[string]any{
		"type":      evt.Type,
		"payload":   evt.Payload,
		"timestamp": evt.Timestamp,
	})); err != nil {
		rt.logger.Error("trigger: failed to seed trigger event metadata", "runId", runID, "workflowId", wf.ID, "error", err)
	}
	if evt.Payload != nil {
		for key, val := range evt.Payload {
			if err := st.Update(context.Background(), store.SetValue(key, val)); err != nil {
				rt.logger.Error("trigger: failed to seed trigger payload key into state", "runId", runID, "key", key, "error", err)
			}
		}
	}

	resolver, cleanup := rt.initResources(record, bus, runID, def.ID)
	if len(resolver) > 0 {
		runCtx.SetResourceResolver(func(key string) (any, bool) {
			v, ok := resolver[key]
			return v, ok
		})
	}

	rt.mu.Lock()
	rt.active[runID] = runCtx
	rt.mu.Unlock()

	handle := &RunHandle{
		RunID:      runID,
		WorkflowID: wf.ID,
		TriggerID:  triggerID,
		PipelineID: def.ID,
		Store:      st,
		Event:      evt,
		Context:    runCtx,
		Events:     bus,
	}

	if record.opts.OnPrepare != nil {
		if err := record.opts.OnPrepare(handle); err != nil {
			cleanup()
			rt.clearActive(runID)
			return rt.finishRun(record, RunResult{
				RunID:      runID,
				WorkflowID: wf.ID,
				TriggerID:  triggerID,
				PipelineID: def.ID,
				Status:     "failed",
				Error:      core.NewSystemError(core.ErrCodeExecutionFailed, "onPrepare hook failed: "+err.Error()),
			})
		}
	}

	res, runErr := runCtx.Run(context.Background())

	cleanup()
	rt.clearActive(runID)

	finalState, _ := st.ExportJSON()

	result := RunResult{
		RunID:      runID,
		WorkflowID: wf.ID,
		TriggerID:  triggerID,
		PipelineID: def.ID,
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

	// Propagate pause fields from pipeline result.
	if res.WaitForEvent != "" {
		result.WaitForEvent = res.WaitForEvent
	}
	if len(res.WaitForEvents) > 0 {
		result.WaitForEvents = res.WaitForEvents
	}
	if res.WaitMode != "" {
		result.WaitMode = res.WaitMode
	}
	if res.Checkpoint != nil {
		result.Checkpoint = res.Checkpoint
	}

	rt.mu.Lock()
	rt.outcomes[runID] = result
	rt.mu.Unlock()
	rt.setRunStatus(runID, result.Status)

	return result
}

// secretLookup adapts the configured SecretProvider into the context-free
// lookup function steps receive. Secrets are resolved lazily at execution
// time; no request context is available (or needed) for host-backed stores.
func (rt *WorkflowRuntime) secretLookup() func(key string) (any, bool) {
	return func(key string) (any, bool) {
		if rt.secrets == nil {
			return nil, false
		}
		return rt.secrets.Get(context.Background(), key)
	}
}

// newStore mints a brand-new run store. The store's ID() becomes the run
// identifier; a run IS its state document. Falls back to a bare MemoryStore
// when no factory is configured or the factory errors.
func (rt *WorkflowRuntime) newStore() (store.Store, error) {
	if rt.storeFactory != nil {
		st, err := rt.storeFactory()
		if err != nil || st == nil {
			if rt.logger != nil {
				rt.logger.Error("store factory failed, falling back to memory store", map[string]any{"error": fmt.Sprintf("%v", err)})
			}
			return store.NewMemoryStore(nil), nil
		}
		return st, nil
	}
	return store.NewMemoryStore(nil), nil
}
