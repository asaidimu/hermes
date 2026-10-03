// Workflow registry lifecycle: Register/Deregister, bus subscription
// refcounting, requirement validation, and workflow-scoped resource
// initialization. Split out of runtime.go by R.1 (pure move).
package runtime

import (
	"context"
	"fmt"
	"strings"

	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"go.uber.org/zap"
)

// Register subscribes a compiled workflow's triggers to the event bus. Each
// matching event spawns a run (subject to the execution mode gate). When an
// EventSource is configured, it is notified so it can wire external event
// listeners (HTTP webhooks, cron, message queues, etc.).
//
// C2 (20261003 refactor plan): the critical section is BOUNDED. Under
// rt.mu — uniqueness check, index insertion, bus subscription refcounting.
// Outside it — secrets validation (vault/network I/O), cron scheduling, and
// the host's EventSource.OnRegister callback, whose emit closure emits on
// the runtime bus and can dispatch back into rt.mu. Host callbacks never
// run inside a runtime critical section. Any failure rolls back under a
// fresh critical section, re-validating existence (a concurrent Deregister
// may have removed the record while the lock was released) — rollback is
// idempotent.
func (rt *WorkflowRuntime) Register(wf *pipeline.Workflow, opts RegisterOptions) error {
	if wf == nil || wf.ID == "" {
		return core.NewSystemError(core.ErrCodeValidation, "workflow must have an id")
	}

	rt.mu.Lock()
	if _, ok := rt.workflows[wf.ID]; ok {
		rt.mu.Unlock()
		return core.NewSystemError(core.ErrCodeConflict,
			`[WorkflowRuntime] Workflow "`+wf.ID+`" is already registered. Call deregister("`+wf.ID+`") before registering again.`)
	}

	record := &workflowRecord{
		workflow:    wf,
		opts:        opts,
		mode:        opts.Mode,
		gate:        newExecutionGate(opts.Mode),
		wfResources: make(map[string]any),
	}
	rt.workflows[wf.ID] = record

	for triggerID, trigger := range wf.Triggers {
		rt.index[trigger.Event] = append(rt.index[trigger.Event], &routeEntry{
			workflowID: wf.ID,
			triggerID:  triggerID,
			trigger:    trigger,
		})
		rt.acquireBusSubscriptionLocked(trigger.Event)
	}
	rt.mu.Unlock()

	// --- outside the critical section: host-visible work ---

	// Validate requirements (secrets I/O) with the record tentatively
	// inserted; on failure roll back under a fresh lock.
	if err := rt.validateRequirements(wf); err != nil {
		rt.rollbackRegistration(wf)
		return err
	}

	for triggerID, trigger := range wf.Triggers {
		if trigger.Cron == "" {
			continue
		}
		scheduleID := wf.ID + ":" + triggerID
		// @note #review-20260822-040 issue P1 resolved status=resolved priority=P1 tags=#review,#error-handling : Discarded Schedule error
		//
		// Resolved: propagate the error. Register already returns
		// error and is called at workflow-registration time (not from
		// a hot path), so failing registration outright when a cron
		// trigger can't be scheduled (e.g. invalid cron expression) is
		// the correct behavior — silently registering a workflow whose
		// trigger will never fire is strictly worse than failing loud
		// at registration.
		if err := rt.scheduler.Schedule(scheduleID, trigger.Cron, func(ctx context.Context) {
			rt.bus.Emit(context.Background(), trigger.Event, events.PipelineEvent{
				Payload: map[string]any{},
			})
		}); err != nil {
			rt.rollbackRegistration(wf)
			return core.NewSystemError(core.ErrCodeValidation,
				"failed to schedule cron trigger "+triggerID+" for workflow "+wf.ID).WithCause(err)
		}
	}

	// Notify the EventSource so it can wire external event listeners. The
	// returned unsubscribe func is RETAINED on the record and invoked on
	// Deregister (the pre-C2 code discarded it — audit §E row
	// "event-source unsubscribe").
	if rt.eventSource != nil {
		triggers := make(map[string]RegisteredTrigger, len(wf.Triggers))
		for id, t := range wf.Triggers {
			triggers[id] = RegisteredTrigger{
				ID:        t.ID,
				Event:     t.Event,
				Predicate: t.Predicate,
			}
		}
		emit := func(eventType string, payload map[string]any) {
			rt.bus.Emit(context.Background(), eventType, events.PipelineEvent{
				Payload: payload,
			})
		}
		unsub, err := rt.eventSource.OnRegister(context.Background(), RegisterParams{
			WorkflowID: wf.ID,
			Triggers:   triggers,
			Emit:       emit,
		})
		if err != nil {
			rt.rollbackRegistration(wf)
			return core.NewSystemError(core.ErrCodeExecutionFailed, "EventSource.OnRegister failed: "+err.Error())
		}
		rt.mu.Lock()
		// Re-validate: a concurrent Deregister may have removed the record
		// while OnRegister ran outside the lock.
		if rec, ok := rt.workflows[wf.ID]; ok {
			rec.unsub = unsub
		} else if unsub != nil {
			// Lost the race — tear the listener down immediately.
			unsub()
		}
		rt.mu.Unlock()
	}

	return nil
}

// rollbackRegistration undoes a failed Register attempt under a fresh
// critical section. Idempotent: if a concurrent Deregister already removed
// the record, nothing to do (the index entries and subscriptions went with
// it).
func (rt *WorkflowRuntime) rollbackRegistration(wf *pipeline.Workflow) {
	rt.mu.Lock()
	if _, ok := rt.workflows[wf.ID]; !ok {
		rt.mu.Unlock()
		return
	}
	delete(rt.workflows, wf.ID)
	for triggerID, trigger := range wf.Triggers {
		bucket := rt.index[trigger.Event]
		filtered := bucket[:0]
		for _, entry := range bucket {
			if entry.workflowID != wf.ID || entry.triggerID != triggerID {
				filtered = append(filtered, entry)
			}
		}
		if len(filtered) == 0 {
			delete(rt.index, trigger.Event)
		} else {
			rt.index[trigger.Event] = filtered
		}
		rt.releaseBusSubscriptionLocked(trigger.Event)
	}
	rt.mu.Unlock()

	// Cancel any cron trigger schedules the attempt managed to make.
	for triggerID, trigger := range wf.Triggers {
		if trigger.Cron != "" {
			rt.scheduler.Cancel(wf.ID + ":" + triggerID)
		}
	}
}

// Deregister removes a workflow and its bus subscriptions, tears down its
// EventSource listener (the retained OnRegister unsubscribe) and cleans up
// its workflow-scoped resource handles. Host-visible cleanup (unsubscribe,
// service Cleanup callbacks) runs OUTSIDE rt.mu — locks never span user
// code (plan C2).
func (rt *WorkflowRuntime) Deregister(workflowID string) {
	rt.mu.Lock()
	record, ok := rt.workflows[workflowID]
	if !ok {
		rt.mu.Unlock()
		return
	}
	delete(rt.workflows, workflowID)

	resources := rt.stealWorkflowResourcesLocked(record)
	unsub := record.unsub

	for triggerID, trigger := range record.workflow.Triggers {
		bucket := rt.index[trigger.Event]
		filtered := bucket[:0]
		for _, entry := range bucket {
			if entry.workflowID != workflowID || entry.triggerID != triggerID {
				filtered = append(filtered, entry)
			}
		}
		if len(filtered) == 0 {
			delete(rt.index, trigger.Event)
		} else {
			rt.index[trigger.Event] = filtered
		}
		rt.releaseBusSubscriptionLocked(trigger.Event)

		// Cancel cron schedule if one was registered.
		if trigger.Cron != "" {
			scheduleID := workflowID + ":" + triggerID
			rt.scheduler.Cancel(scheduleID)
		}
	}
	rt.mu.Unlock()

	if unsub != nil {
		unsub()
	}
	teardownWorkflowResources(rt.logger, record.workflow, resources)
}

// stealWorkflowResourcesLocked drains record.wfResources under rt.mu and
// returns the handles for out-of-lock cleanup. Caller must hold rt.mu.
func (rt *WorkflowRuntime) stealWorkflowResourcesLocked(record *workflowRecord) map[string]any {
	record.mu.Lock()
	defer record.mu.Unlock()
	handles := record.wfResources
	record.wfResources = make(map[string]any)
	return handles
}

// teardownWorkflowResources invokes Cleanup for workflow-scoped services
// that hold handles. Best-effort: failures are logged, never fatal.
func teardownWorkflowResources(logger *zap.SugaredLogger, wf *pipeline.Workflow, handles map[string]any) {
	if len(handles) == 0 {
		return
	}
	cleanups := map[string]func(context.Context, any) error{}
	for _, svc := range wf.Services {
		if svc.Scope == "workflow" && svc.Cleanup != nil {
			cleanups[svc.ID] = svc.Cleanup
		}
	}
	for svcID, handle := range handles {
		cleanup, ok := cleanups[svcID]
		if !ok {
			continue
		}
		if err := cleanup(context.Background(), handle); err != nil {
			if logger != nil {
				logger.Error("[WorkflowRuntime] workflow service cleanup failed for "+svcID+":", err)
			}
		}
	}
}

// HasWorkflow reports whether a workflow is registered.
func (rt *WorkflowRuntime) HasWorkflow(workflowID string) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	_, ok := rt.workflows[workflowID]
	return ok
}

// ListWorkflows returns the ids of all registered workflows.
func (rt *WorkflowRuntime) ListWorkflows() []string {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	out := make([]string, 0, len(rt.workflows))
	for id := range rt.workflows {
		out = append(out, id)
	}
	return out
}

func (rt *WorkflowRuntime) acquireBusSubscriptionLocked(eventType string) {
	ref, ok := rt.subs[eventType]
	if ok {
		ref.refs++
		return
	}
	unsub := rt.bus.Subscribe(eventType, func(ctx context.Context, evt events.PipelineEvent) error {
		rt.dispatch(eventType, evt)
		return nil
	})
	rt.subs[eventType] = &busRef{refs: 1, unsub: unsub}
}

func (rt *WorkflowRuntime) releaseBusSubscriptionLocked(eventType string) {
	ref, ok := rt.subs[eventType]
	if !ok {
		return
	}
	ref.refs--
	if ref.refs <= 0 {
		if ref.unsub != nil {
			ref.unsub()
		}
		delete(rt.subs, eventType)
	}
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

// Shutdown gracefully shuts down the runtime and its event source.
func (rt *WorkflowRuntime) Shutdown(ctx context.Context) error {
	// Stop the terminal-history janitor (Options.RunHistoryTTL) before
	// tearing down the event source, so no purge races the shutdown.
	rt.janitorStopOnce.Do(func() {
		if rt.janitorStop != nil {
			close(rt.janitorStop)
			<-rt.janitorDone
		}
	})
	if rt.eventSource != nil {
		if err := rt.eventSource.OnShutdown(ctx); err != nil {
			return err
		}
	}
	// Clean up all active watch registrations, and tear down every
	// registered workflow's EventSource listener + workflow-scoped
	// resources (C2: Shutdown drains what Register accumulated).
	type teardown struct {
		rec     *workflowRecord
		handles map[string]any
	}
	rt.mu.Lock()
	for runID := range rt.paused {
		rt.watchService.OnRunEnded(runID)
	}
	pendings := make([]teardown, 0, len(rt.workflows))
	for id, rec := range rt.workflows {
		rec.mu.Lock()
		handles := rec.wfResources
		rec.wfResources = make(map[string]any)
		rec.mu.Unlock()
		pendings = append(pendings, teardown{rec: rec, handles: handles})
		delete(rt.workflows, id)
	}
	rt.mu.Unlock()

	// Host-visible unsubscribes and service cleanups run outside the lock.
	for _, p := range pendings {
		if p.rec.unsub != nil {
			p.rec.unsub()
		}
		teardownWorkflowResources(rt.logger, p.rec.workflow, p.handles)
	}

	if rt.scheduler != nil {
		return rt.scheduler.Shutdown(ctx)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Run(nodes, edges) — compile-and-run convenience (server-facing)
// ---------------------------------------------------------------------------

// validateRequirements checks a workflow's declared env/secret requirements
// against what this runtime can provide. Required env keys must exist in the
// configured Env layers; required secrets must be resolvable via the
// configured SecretProvider. Returns an error naming every unsatisfied key.
func (rt *WorkflowRuntime) validateRequirements(wf *pipeline.Workflow) error {
	if len(wf.Requirements) == 0 {
		return nil
	}
	var missing []string
	for _, req := range wf.Requirements {
		if !req.Required {
			continue
		}
		switch req.Kind {
		case pipeline.ReqEnv:
			if _, ok := rt.env[req.Key]; !ok {
				missing = append(missing, fmt.Sprintf("env:%s", req.Key))
			}
		case pipeline.ReqSecret:
			if rt.secretHas == nil {
				missing = append(missing, fmt.Sprintf("secret:%s (no secret provider configured)", req.Key))
				continue
			}
			if !rt.secretHas(context.Background(), req.Key) {
				missing = append(missing, fmt.Sprintf("secret:%s", req.Key))
			}
		}
	}
	if len(missing) > 0 {
		return core.NewSystemError(core.ErrCodeValidation,
			"workflow requirements not satisfied — missing: "+strings.Join(missing, ", "))
	}
	return nil
}

// ValidateWorkflowRequirements checks whether this runtime can satisfy the
// workflow's declared env/secret requirements without registering it. Hosts
// call this when saving workflow definitions to fail early with actionable
// errors.
func (rt *WorkflowRuntime) ValidateWorkflowRequirements(wf *pipeline.Workflow) error {
	if wf == nil {
		return core.NewSystemError(core.ErrCodeValidation, "workflow must not be nil")
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.validateRequirements(wf)
}

// initResources initializes workflow-scoped services (cached) and run/transient
// services (per run), emitting resource lifecycle events on the run's bus, and
// returning a resolver map keyed by service id ("resource:<nodeId>") plus a
// cleanup func for run-scoped handles.
func (rt *WorkflowRuntime) initResources(record *workflowRecord, bus *events.MemoryScopedBus, runID, pipelineID string) (map[string]any, func()) {
	resolver := map[string]any{}
	var cleanups []func()

	// Register the WatchService as a built-in resource for nodes to access
	resolver["resource:watch-service"] = rt.watchService

	emit := func(eventType string, payload map[string]any) {
		bus.Emit(context.Background(), eventType, events.PipelineEvent{
			RunID:      runID,
			PipelineID: pipelineID,
			Path: events.EventPath{
				{Kind: "pipeline", ID: pipelineID, Label: pipelineID},
			},
			Payload: payload,
		})
	}
	base := func(svc pipeline.Service) map[string]any {
		return map[string]any{
			"resourceId":    svc.ID,
			"resourceKind":  svc.Kind,
			"resourceLabel": svc.Label,
		}
	}

	record.mu.Lock()
	defer record.mu.Unlock()

	for _, svc := range record.workflow.Services {
		if svc.Scope == "workflow" {
			handle, ok := record.wfResources[svc.ID]
			if !ok {
				if svc.Init != nil {
					emit("resource:init", base(svc))
					var err error
					handle, err = svc.Init(context.Background())
					if err != nil {
						rt.logger.Error("[WorkflowRuntime] workflow service init failed for "+svc.ID+":", err)
						emit("resource:init:failure", addErr(base(svc), err))
						continue
					}
				}
				record.wfResources[svc.ID] = handle
			}
			emit("resource:ready", base(svc))
			resolver[svc.ID] = handle
			continue
		}

		emit("resource:init", base(svc))
		handle, err := svc.Init(context.Background())
		if err != nil {
			rt.logger.Error("[WorkflowRuntime] run service init failed for "+svc.ID+":", err)
			emit("resource:init:failure", addErr(base(svc), err))
			continue
		}
		emit("resource:ready", base(svc))
		resolver[svc.ID] = handle
		if svc.Cleanup != nil {
			s := svc
			cleanups = append(cleanups, func() {
				if err := s.Cleanup(context.Background(), resolver[s.ID]); err != nil {
					rt.logger.Error("[WorkflowRuntime] run service cleanup failed for "+s.ID+":", err)
					emit("resource:cleanup:failure", addErr(base(s), err))
					return
				}
				emit("resource:cleanup", base(s))
			})
		}
	}

	cleanup := func() {
		for _, c := range cleanups {
			c()
		}
	}
	return resolver, cleanup
}

// addErr copies a base resource payload and attaches an errorMessage key.
func addErr(payload map[string]any, err error) map[string]any {
	cp := make(map[string]any, len(payload)+1)
	for k, v := range payload {
		cp[k] = v
	}
	cp["errorMessage"] = err.Error()
	return cp
}
