// Package runtime ports the TS WorkflowRuntime (utils/src/runtime/runtime/
// runtime.ts) and WorkflowsEngine orchestration (utils/src/workflows/engine.ts):
// a bus-driven orchestrator that dispatches trigger events to registered
// workflows, spawns per-run stores, initializes resource services, records
// timelines, and tracks run outcomes.
package runtime

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/asaidimu/hermes/pkg/compiler"
	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/scheduler"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/asaidimu/hermes/pkg/timeline"
	"github.com/asaidimu/hermes/pkg/watch"
)

// SecretProvider supplies credentials to runs declaring secret requirements.
// Implementations back onto the host's credential store (settings, vault,
// keychain). Get values are consumed in-process by steps; implementations and
// callers must never persist or log them.
type SecretProvider interface {
	// Get resolves a secret by key. Returns (nil, false) when unknown.
	Get(ctx context.Context, key string) (any, bool)
	// Has reports whether a key is resolvable without reading its value.
	// Used for pre-flight validation at workflow registration time.
	Has(ctx context.Context, key string) bool
}

// Event names used by the runtime (mirror the TS constants).
const (
	ManualEvent = "__manual__"
	AbortEvent  = "run:abort"
)

// Options configures a WorkflowRuntime.
type Options struct {
	// Bus is the root event bus. Subscribers (including callers) see all
	// pipeline/run events emitted during execution. When nil, the runtime
	// creates an isolated in-memory bus.
	Bus events.ScopedEventBus
	// StoreFactory mints a brand-new store per run. The returned store's ID()
	// becomes the run identifier: a run IS its state document, and creating
	// that document creates the run's identity (a system-minted UUIDv7). When
	// nil, a bare store.NewMemoryStore is used for every run.
	StoreFactory func() (store.Store, error)
	// StoreLoader recovers an existing run's store by run identifier after a
	// restart or crash. When nil, runs paused in memory cannot be recovered
	// from persistence.
	StoreLoader func(runID string) (store.Store, error)
	// Secrets resolves credentials requested by nodes via their declared
	// Requirements. Values surface to steps only through NodeRunContext.Secret
	// lookups — they never persist to state, checkpoints, or events. When nil,
	// workflows whose nodes declare required secrets fail registration.
	Secrets SecretProvider
	// Timeline, when set, records every run into the store (TimelineRecorder).
	Timeline timeline.TimelineStore
	Logger   core.Logger
	// Env holds global environment layers available to runs.
	Env map[string]any
	// Services are runtime-global services. Kept for API parity with the TS
	// constructor; services are made available through the run resource
	// resolver keyed by service ID.
	Services []pipeline.Service
	// EventSource is the inversion-of-control interface for wiring external
	// events to workflow triggers. When nil, a ManualEventSource is used.
	EventSource EventSource
	// Scheduler handles time-based scheduling (delays, cron). When nil, an
	// InMemoryScheduler is used.
	Scheduler scheduler.Scheduler
}

// Mode mirrors the TS WorkflowExecutionMode. It controls per-workflow run
// concurrency. "transient" (default) allows up to Concurrency concurrent runs
// and drops overflow; "serialized", "exclusive" and "loop" only allow one
// active run per workflow and drop/reject while one is running.
type Mode struct {
	Type        string
	Concurrency int
	Capacity    int
	OnActive    string
}

func (m Mode) concurrency() int {
	if m.Concurrency > 0 {
		return m.Concurrency
	}
	return 10
}

// RegisterOptions configures a workflow registration.
type RegisterOptions struct {
	Mode       Mode
	OnPrepare  func(*RunHandle) error
	OnComplete func(RunResult)
	OnCleanup  func()
}

// RunHandle exposes a live run to onPrepare hooks (mirrors the TS RunContext
// surface used by tests: id, on/off, abort, write).
type RunHandle struct {
	RunID      string
	WorkflowID string
	TriggerID  string
	PipelineID string
	Store      store.Store
	Event      events.PipelineEvent
	Context    *pipeline.RunContextImpl
	Events     events.ScopedEventBus
}

func (h *RunHandle) ID() string { return h.RunID }

// On subscribes to run-scoped events (e.g. "step:failure").
func (h *RunHandle) On(eventType string, handler events.EventHandler) func() {
	return h.Events.Subscribe(eventType, handler)
}

func (h *RunHandle) Write(mutator store.Mutator) { h.Context.Write(mutator) }

func (h *RunHandle) Abort(err error) { h.Context.Abort(err) }

// RunResult is the terminal outcome of a run.
type RunResult struct {
	OK            bool
	RunID         string
	WorkflowID    string
	TriggerID     string
	PipelineID    string
	Status        string // "succeeded" | "failed" | "aborted" | "paused"
	Error         error
	FinalState    map[string]any
	WaitForEvent  string                       `json:"waitForEvent,omitempty"`  // single event (backward compat)
	WaitForEvents []string                     `json:"waitForEvents,omitempty"` // multiple events
	WaitMode      string                       `json:"waitMode,omitempty"`      // "any" or "all"
	Checkpoint    *pipeline.PipelineCheckpoint `json:"checkpoint,omitempty"`
}

// RunOptions configures a Run(nodes, edges) invocation.
type RunOptions struct {
	OnPrepare  func(*RunHandle) error
	OnComplete func(RunResult)
	Timeout    time.Duration
	Registry   pipeline.PipelineRegistry
}

// WorkflowRuntime orchestrates workflow runs. It is safe for concurrent use.
type WorkflowRuntime struct {
	bus          events.ScopedEventBus
	logger       core.Logger
	env          map[string]any
	secrets      SecretProvider
	storeFactory func() (store.Store, error)
	storeLoader  func(runID string) (store.Store, error)
	timeline     timeline.TimelineStore
	eventSource  EventSource
	scheduler    scheduler.Scheduler
	watchService *WatchService

	mu        sync.Mutex
	workflows map[string]*workflowRecord
	index     map[string][]*routeEntry
	subs      map[string]*busRef
	outcomes  map[string]RunResult
	active    map[string]*pipeline.RunContextImpl
	stores    map[string]store.Store
	paused    map[string]*pausedRun // runID → paused run waiting for an event
}

// pausedRun tracks a pipeline that is paused waiting for specific event(s).
type pausedRun struct {
	runID         string
	workflowID    string
	triggerID     string
	pipelineID    string
	waitForEvent  string   // single event (backward compat)
	waitForEvents []string // multiple events
	waitMode      string   // "any" or "all"
	store         store.Store
	checkpoint    *pipeline.PipelineCheckpoint
}

type busRef struct {
	refs  int
	unsub func()
}

type routeEntry struct {
	workflowID string
	triggerID  string
	trigger    pipeline.WorkflowTrigger
}

type workflowRecord struct {
	workflow *pipeline.Workflow
	opts     RegisterOptions
	mode     Mode
	gate     *executionGate

	mu          sync.Mutex
	wfResources map[string]any
}

type executionGate struct {
	sem    chan struct{}
	mu     sync.Mutex
	active int
}

func newExecutionGate(mode Mode) *executionGate {
	if mode.Type == "" || mode.Type == "transient" {
		return &executionGate{sem: make(chan struct{}, mode.concurrency())}
	}
	return &executionGate{}
}

// tryAcquire reserves a concurrency slot. It returns a release function on
// success, or (false, nil) when the workflow's concurrency limit rejects the
// run (serialized/exclusive/loop: one active run at a time).
func (g *executionGate) tryAcquire() (bool, func()) {
	if g.sem != nil {
		select {
		case g.sem <- struct{}{}:
			return true, func() { <-g.sem }
		default:
			return false, nil
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.active > 0 {
		return false, nil
	}
	g.active++
	return true, func() {
		g.mu.Lock()
		g.active--
		g.mu.Unlock()
	}
}

// NewWorkflowRuntime creates a runtime. A bus is created when Options.Bus is
// nil; an abort subscription is wired on the bus so AbortEvent dispatches to
// AbortRun. When Options.EventSource is nil, a ManualEventSource is used.
func NewWorkflowRuntime(opts Options) *WorkflowRuntime {
	rt := &WorkflowRuntime{
		logger:       opts.Logger,
		env:          opts.Env,
		secrets:      opts.Secrets,
		storeFactory: opts.StoreFactory,
		storeLoader:  opts.StoreLoader,
		timeline:     opts.Timeline,
		workflows:    make(map[string]*workflowRecord),
		index:        make(map[string][]*routeEntry),
		subs:         make(map[string]*busRef),
		outcomes:     make(map[string]RunResult),
		active:       make(map[string]*pipeline.RunContextImpl),
		stores:       make(map[string]store.Store),
		paused:       make(map[string]*pausedRun),
	}
	if opts.EventSource != nil {
		rt.eventSource = opts.EventSource
	} else {
		rt.eventSource = NewManualEventSource()
	}
	if opts.Scheduler != nil {
		rt.scheduler = opts.Scheduler
	} else {
		rt.scheduler = scheduler.New()
	}
	if rt.logger == nil {
		rt.logger = core.NopLogger{}
	}
	rt.bus = opts.Bus
	if rt.bus == nil {
		// @note #scoped-bus-opportunity-005 issue status=open priority=P2 tags=#event-bus,#durability : Durable event backend designed but never wired
		//
		// NewMemoryScopedBus accepts an optional go-events SimpleEventBus for
		// durable Pebble LSM event sourcing (designed in the spec). However,
		// the runtime never passes one — the underlying field is always nil.
		// All events are purely in-memory and lost on restart.
		//
		// Fix with go-events ScopedBus: create a root EventBus with Pebble-backed
		// Config, then use bus.Scope() for per-run and per-workflow isolation.
		// This gives durable event logs, checkpoint recovery, and compaction
		// out of the box.
		rt.bus = events.NewMemoryScopedBus()
	}
	rt.watchService = NewWatchService(rt.bus, func(runID string, patch map[string]any) {
		rt.Resume(runID, patch)
	})
	rt.bus.Subscribe(AbortEvent, func(ctx context.Context, evt events.PipelineEvent) error {
		if runID, ok := evt.Payload["run"].(string); ok && runID != "" {
			rt.AbortRun(runID)
		}
		return nil
	})
	return rt
}

// Bus returns the runtime's root event bus.
func (rt *WorkflowRuntime) Bus() events.ScopedEventBus { return rt.bus }

// Store returns the store created for a run (nil if the run never prepared).
func (rt *WorkflowRuntime) Store(runID string) store.Store {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.stores[runID]
}

// GetRunOutcome returns the recorded outcome for a completed run.
func (rt *WorkflowRuntime) GetRunOutcome(runID string) (RunResult, bool) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	res, ok := rt.outcomes[runID]
	return res, ok
}

// ListRuns returns timeline metadata for all runs when a timeline store is
// configured. Returns nil when the runtime has no timeline store.
func (rt *WorkflowRuntime) ListRuns(ctx context.Context) ([]timeline.RunTimelineMeta, error) {
	if rt.timeline == nil {
		return nil, nil
	}
	return rt.timeline.ListRuns(ctx)
}

// GetRunMeta returns timeline metadata for a single run.
func (rt *WorkflowRuntime) GetRunMeta(ctx context.Context, runID string) (*timeline.RunTimelineMeta, error) {
	if rt.timeline == nil {
		return nil, core.NewSystemError(core.ErrCodeNotFound, "no timeline store configured")
	}
	return rt.timeline.GetRunMeta(ctx, runID)
}

// GetEvents returns recorded timeline events for a run.
func (rt *WorkflowRuntime) GetEvents(ctx context.Context, runID string, fromSeq, toSeq int64) ([]timeline.TimelineEvent, error) {
	if rt.timeline == nil {
		return nil, core.NewSystemError(core.ErrCodeNotFound, "no timeline store configured")
	}
	return rt.timeline.GetEvents(ctx, runID, fromSeq, toSeq)
}

// ---------------------------------------------------------------------------
// Workflow registry
// ---------------------------------------------------------------------------

// Register subscribes a compiled workflow's triggers to the event bus. Each
// matching event spawns a run (subject to the execution mode gate). When an
// EventSource is configured, it is notified so it can wire external event
// listeners (HTTP webhooks, cron, message queues, etc.).
func (rt *WorkflowRuntime) Register(wf *pipeline.Workflow, opts RegisterOptions) error {
	if wf == nil || wf.ID == "" {
		return core.NewSystemError(core.ErrCodeValidation, "workflow must have an id")
	}

	rt.mu.Lock()
	defer rt.mu.Unlock()

	if _, ok := rt.workflows[wf.ID]; ok {
		return core.NewSystemError(core.ErrCodeConflict,
			`[WorkflowRuntime] Workflow "`+wf.ID+`" is already registered. Call deregister("`+wf.ID+`") before registering again.`)
	}

	if err := rt.validateRequirements(wf); err != nil {
		return err
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

		// Schedule cron-based recurring triggers.
		if trigger.Cron != "" {
			scheduleID := wf.ID + ":" + triggerID
			// @note #review-20260822-040 issue status=open priority=P1 tags=#review,#error-handling : Discarded Schedule error
			//
			// The error from rt.scheduler.Schedule is silently discarded. If scheduling fails
			// (e.g., invalid cron expression), the trigger will never fire and the failure
			// will be invisible to the caller.
			rt.scheduler.Schedule(scheduleID, trigger.Cron, func(ctx context.Context) {
				rt.bus.Emit(context.Background(), trigger.Event, events.PipelineEvent{
					Payload: map[string]any{},
				})
			})
		}
	}

	// Notify the EventSource so it can wire external event listeners.
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
		if _, err := rt.eventSource.OnRegister(context.Background(), RegisterParams{
			WorkflowID: wf.ID,
			Triggers:   triggers,
			Emit:       emit,
		}); err != nil {
			// Rollback: remove from workflows and index
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
			return core.NewSystemError(core.ErrCodeExecutionFailed, "EventSource.OnRegister failed: "+err.Error())
		}
	}

	return nil
}

// Deregister removes a workflow and its bus subscriptions.
func (rt *WorkflowRuntime) Deregister(workflowID string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	record, ok := rt.workflows[workflowID]
	if !ok {
		return
	}

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

	delete(rt.workflows, workflowID)
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

// dispatch fans a bus event out to every matching trigger and spawns runs in
// the background (mirrors WorkflowRuntime.dispatch + spawnRun). It also resumes
// any paused runs that are waiting for this event type.
func (rt *WorkflowRuntime) dispatch(eventType string, evt events.PipelineEvent) {
	// @note #review-20260822-039 issue status=open priority=P1 tags=#review,#concurrency : TOCTOU race in dispatch between lock acquisitions
	//
	// dispatch calls rt.mu.Unlock() at line 531 then rt.mu.Lock() again at line 540.
	// Between these two critical sections, rt.paused and rt.index could be mutated by
	// other goroutines (e.g., Deregister). The snapshot of toResume and entries is taken
	// under separate locks, creating a TOCTOU window.
	//
	// Fix by taking both snapshots under a single lock acquisition, or by using a
	// snapshot that is immutable after release.
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
	rt.mu.Unlock()

	for _, pr := range toResume {
		go func(p *pausedRun) {
			rt.Resume(p.runID, evt.Payload)
		}(pr)
	}

	// Also dispatch to matching triggers (start new runs).
	rt.mu.Lock()
	bucket := rt.index[eventType]
	entries := make([]*routeEntry, 0, len(bucket))
	entries = append(entries, bucket...)
	rt.mu.Unlock()

	if len(entries) == 0 && len(toResume) == 0 {
		return
	}
	evt.Type = eventType
	if evt.Timestamp == 0 {
		evt.Timestamp = time.Now().UnixMilli()
	}

	for _, entry := range entries {
		matched := true
		if entry.trigger.Predicate != nil {
			matched = entry.trigger.Predicate(evt)
		}
		if !matched {
			continue
		}

		rt.mu.Lock()
		record := rt.workflows[entry.workflowID]
		rt.mu.Unlock()
		if record == nil {
			continue
		}

		ok, release := record.gate.tryAcquire()
		if !ok {
			rt.logger.Warn(`[WorkflowRuntime] run rejected by execution gate for workflow "` + entry.workflowID + `"`)
			continue
		}
		go func(rec *workflowRecord, tid string, ev events.PipelineEvent) {
			defer release()
			rt.spawnRun(rec, tid, ev)
		}(record, entry.triggerID, evt)
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
			rt.watchService.Register(runID, watch.WatchDescriptor{
				EventTypes: eventTypes,
				Mode:       result.WaitMode,
				Timeout:    timeoutMs,
			})

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

	// Fold the resume event payload directly into state so each pause's
	// data is accessible without overwriting previous payloads.
	if payload != nil {
		for key, val := range payload {
			// @note #review-20260822-048 issue status=open priority=P2 tags=#review,#error-handling : Store update errors discarded when folding resume payload
			//
			// _ = st.Update(...) discards the error when folding the resume event payload
			// into state. If the store rejects the update, the resumed pipeline will run
			// with incomplete state.
			_ = st.Update(context.Background(), store.SetValue(key, val))
		}
	}

	// Write the resume reason into state so routers can distinguish event vs timeout.
	if ckpt.ResumeReason != "" {
		_ = st.Update(context.Background(), store.SetValue("__resume_reason__", ckpt.ResumeReason))
	}

	bus := rt.bus.Scope(events.EventPath{})
	// @note #scoped-bus-opportunity-003 issue status=open priority=P2 tags=#event-bus,#isolation : Empty EventPath scoping provides no real isolation
	//
	// Per-run buses are scoped with an empty EventPath{}. The path is only used
	// for metadata decoration (populated into PipelineEvent.Path for the frontend
	// timeline) — it does NOT filter or route events. All runs share the same
	// flat namespace on the root bus.
	//
	// Fix with go-events ScopedBus: use bus.Scope("run:"+runID) instead of
	// bus.Scope(EventPath{}). This gives actual topic isolation — each run's
	// events are prefixed with its runID, preventing cross-run interference.
	factory := pipeline.NewFactory(record.workflow.Pipelines[paused.pipelineID], record.workflow.Pipelines[paused.pipelineID].Schema, pipeline.FactoryOptions{
		Logger:       rt.logger,
		RunEnv:       rt.env,
		SecretLookup: rt.secretLookup(),
	})
	runCtx := factory.PrepareWithEntry(runID, st, bus, ckpt.ResumeAt)

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

	finalState, _ := st.ExportJSON()

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

	// If the resumed run paused again (waiting for another event), track it.
	if result.Status == "paused" && res.WaitForEvent != "" && res.Checkpoint != nil {
		rt.mu.Lock()
		rt.paused[runID] = &pausedRun{
			runID:        runID,
			workflowID:   paused.workflowID,
			triggerID:    paused.triggerID,
			pipelineID:   paused.pipelineID,
			waitForEvent: res.WaitForEvent,
			store:        st,
			checkpoint:   res.Checkpoint,
		}
		rt.mu.Unlock()
	}

	rt.mu.Lock()
	rt.outcomes[runID] = result
	rt.mu.Unlock()

	// Don't call OnComplete if the run paused again — the next resume event
	// will trigger completion.
	if result.Status != "paused" {
		// Clean up watch service when run completes
		rt.watchService.OnRunEnded(runID)
		rt.callOnComplete(record, result)
	}
	return result
}

// resumeFromPersistence attempts to load a paused run from the persistent store.
// It reads the run's state document by id, resolves its workflow from the
// seeded __run_meta__ linkage, and reconstructs the pausedRun from the
// checkpoint stored in the document body.
func (rt *WorkflowRuntime) resumeFromPersistence(runID string, payload map[string]any) RunResult {
	st, err := rt.newStoreForID(runID)
	if err != nil || st == nil {
		return RunResult{
			OK:     false,
			Status: "failed",
			Error:  core.NewSystemError(core.ErrCodeNotFound, "no store available for run "+runID),
		}
	}

	// Read run linkage and checkpoint from state.
	var meta struct{ workflowID, triggerID string }
	var ckpt *pipeline.PipelineCheckpoint
	_ = st.Read(func(state map[string]any) error {
		if m, ok := state[store.RunMetaKey].(map[string]any); ok {
			meta.workflowID, _ = m["workflowId"].(string)
			meta.triggerID, _ = m["triggerId"].(string)
		}
		if meta.workflowID != "" {
			rt.mu.Lock()
			if rec, ok := rt.workflows[meta.workflowID]; ok {
				for pipeID := range rec.workflow.Pipelines {
					if c, rErr := pipeline.ReadCheckpoint(state, pipeID); c != nil && rErr == nil {
						ckpt = c
						break
					}
				}
			}
			rt.mu.Unlock()
			return nil
		}
		// No linkage (legacy run): scan all workflows' pipelines.
		rt.mu.Lock()
		for _, rec := range rt.workflows {
			for pipeID := range rec.workflow.Pipelines {
				if c, rErr := pipeline.ReadCheckpoint(state, pipeID); c != nil && rErr == nil {
					ckpt = c
					rt.mu.Unlock()
					return nil
				}
			}
		}
		rt.mu.Unlock()
		return nil
	})

	if ckpt == nil {
		return RunResult{
			OK:     false,
			Status: "failed",
			Error:  core.NewSystemError(core.ErrCodeNotFound, "no checkpoint found for run "+runID),
		}
	}

	// Resolve the workflow: direct from seeded linkage, falling back to a
	// scan for the workflow containing this pipeline.
	workflowID := meta.workflowID
	if workflowID == "" {
		rt.mu.Lock()
		for wfID, rec := range rt.workflows {
			if _, ok := rec.workflow.Pipelines[ckpt.PipelineID]; ok {
				workflowID = wfID
				break
			}
		}
		rt.mu.Unlock()
	}

	if workflowID == "" {
		return RunResult{
			OK:     false,
			Status: "failed",
			Error:  core.NewSystemError(core.ErrCodeNotFound, "workflow not found for pipeline "+ckpt.PipelineID),
		}
	}

	record := rt.workflows[workflowID]
	if record == nil {
		return RunResult{
			OK:     false,
			Status: "failed",
			Error:  core.NewSystemError(core.ErrCodeNotFound, "workflow "+workflowID+" no longer registered"),
		}
	}

	// Determine wait-for-event from checkpoint.
	waitForEvent := ckpt.WaitForEvent
	waitForEvents := ckpt.WaitForEvents
	waitMode := ckpt.WaitMode

	rt.mu.Lock()
	rt.paused[runID] = &pausedRun{
		runID:         runID,
		workflowID:    workflowID,
		triggerID:     meta.triggerID, // empty when the doc predates run linkage
		pipelineID:    ckpt.PipelineID,
		waitForEvent:  waitForEvent,
		waitForEvents: waitForEvents,
		waitMode:      waitMode,
		store:         st,
		checkpoint:    ckpt,
	}
	rt.mu.Unlock()

	// Now delegate to the normal Resume path.
	return rt.Resume(runID, payload)
}

// Shutdown gracefully shuts down the runtime and its event source.
func (rt *WorkflowRuntime) Shutdown(ctx context.Context) error {
	if rt.eventSource != nil {
		if err := rt.eventSource.OnShutdown(ctx); err != nil {
			return err
		}
	}
	// Clean up all active watch registrations
	rt.mu.Lock()
	for runID := range rt.paused {
		rt.watchService.OnRunEnded(runID)
	}
	rt.mu.Unlock()
	if rt.scheduler != nil {
		return rt.scheduler.Shutdown(ctx)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Run(nodes, edges) — compile-and-run convenience (server-facing)
// ---------------------------------------------------------------------------

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
	// @note #review-20260822-038 issue status=open priority=P1 tags=#review,#bug : Timer leak in select
	//
	// time.After(timeout) creates a timer that is not stopped if the select completes via
	// done or ctx.Done(). This leaks a timer until it fires. Use time.NewTimer with
	// explicit Stop().
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

	rt.mu.Lock()
	rt.stores[runID] = st
	rt.mu.Unlock()

	// The run's bus scopes under the root bus so all pipeline events bubble to
	// the runtime bus (external subscribers and timeline recorder see them).
	bus := rt.bus.Scope(events.EventPath{})

	factory := pipeline.NewFactory(def, def.Schema, pipeline.FactoryOptions{
		Logger:       rt.logger,
		RunEnv:       rt.env,
		SecretLookup: rt.secretLookup(),
	})
	runCtx := factory.Prepare(runID, st, bus)

	// Seed run linkage so crash recovery can resolve the workflow directly,
	// plus the trigger event metadata. Payload keys become top-level state
	// fields so configs can address them with the standardized dotted path
	// (e.g. `status`, `userId`).
	// @note #review-20260822-047 issue status=open priority=P2 tags=#review,#error-handling : Store update errors discarded when seeding trigger metadata
	//
	// _ = st.Update(...) discards the error when seeding trigger event metadata. If the
	// store rejects the update (e.g., validation error, key collision), state will be
	// missing critical trigger data.
	_ = st.Update(context.Background(), store.SetValue(store.RunMetaKey, map[string]any{
		"workflowId": wf.ID,
		"triggerId":  triggerID,
		"pipelineId": def.ID,
	}))
	_ = st.Update(context.Background(), store.SetValue("__trigger_event__", map[string]any{
		"type":      evt.Type,
		"payload":   evt.Payload,
		"timestamp": evt.Timestamp,
	}))
	if evt.Payload != nil {
		for key, val := range evt.Payload {
			_ = st.Update(context.Background(), store.SetValue(key, val))
		}
	}

	resolver, cleanup := rt.initResources(record, bus, runID, def.ID)
	if len(resolver) > 0 {
		runCtx.SetResourceResolver(func(key string) (any, bool) {
			v, ok := resolver[key]
			return v, ok
		})
	}

	var unsub func()
	if rt.timeline != nil {
		rec := timeline.NewTimelineRecorder(runID, def.ID, rt.timeline)
		unsub = rec.Attach(bus, st)
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
			if unsub != nil {
				unsub()
			}
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
	if unsub != nil {
		unsub()
	}
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
			if rt.secrets == nil {
				missing = append(missing, fmt.Sprintf("secret:%s (no secret provider configured)", req.Key))
				continue
			}
			if !rt.secrets.Has(context.Background(), req.Key) {
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

// newStoreForID recovers an existing run's store from persistence via the
// configured loader.
func (rt *WorkflowRuntime) newStoreForID(runID string) (store.Store, error) {
	if rt.storeLoader == nil {
		return nil, core.NewSystemError(core.ErrCodeNotFound, "no store loader configured for run "+runID)
	}
	st, err := rt.storeLoader(runID)
	if err != nil {
		return nil, err
	}
	if st == nil {
		return nil, core.NewSystemError(core.ErrCodeNotFound, "store loader returned nothing for run "+runID)
	}
	return st, nil
}

func (rt *WorkflowRuntime) clearActive(runID string) {
	rt.mu.Lock()
	delete(rt.active, runID)
	rt.mu.Unlock()
}

// initResources initializes workflow-scoped services (cached) and run/transient
// services (per run), emitting resource lifecycle events on the run's bus, and
// returning a resolver map keyed by service id ("resource:<nodeId>") plus a
// cleanup func for run-scoped handles.
func (rt *WorkflowRuntime) initResources(record *workflowRecord, bus events.ScopedEventBus, runID, pipelineID string) (map[string]any, func()) {
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
