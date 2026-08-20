// Package runtime ports the TS WorkflowRuntime (utils/src/runtime/runtime/
// runtime.ts) and WorkflowsEngine orchestration (utils/src/workflows/engine.ts):
// a bus-driven orchestrator that dispatches trigger events to registered
// workflows, spawns per-run stores, initializes resource services, records
// timelines, and tracks run outcomes.
package runtime

import (
	"context"
	"sync"
	"time"

	"github.com/asaidimu/hermes/pkg/compiler"
	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/asaidimu/hermes/pkg/timeline"
	"github.com/google/uuid"
)

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
	// StoreFactory creates a fresh store per run. When nil, a bare
	// store.NewMemoryStore is used for every run.
	StoreFactory func(runID string) store.Store
	// Timeline, when set, records every run into the store (TimelineRecorder).
	Timeline timeline.TimelineStore
	Logger core.Logger
	// Env holds global environment layers available to runs.
	Env map[string]any
	// Services are runtime-global services. Kept for API parity with the TS
	// constructor; services are made available through the run resource
	// resolver keyed by service ID.
	Services []pipeline.Service
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

func (h *RunHandle) Write(mutator store.DocumentMutator) { h.Context.Write(mutator) }

func (h *RunHandle) Abort(err error) { h.Context.Abort(err) }

// RunResult is the terminal outcome of a run.
type RunResult struct {
	OK         bool
	RunID      string
	WorkflowID string
	TriggerID  string
	PipelineID string
	Status     string // "succeeded" | "failed" | "aborted" | "paused"
	Error      error
	FinalState map[string]any
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
	storeFactory func(runID string) store.Store
	timeline     timeline.TimelineStore

	mu        sync.Mutex
	workflows map[string]*workflowRecord
	index     map[string][]*routeEntry
	subs      map[string]*busRef
	outcomes  map[string]RunResult
	active    map[string]*pipeline.RunContextImpl
	stores    map[string]store.Store
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
// AbortRun.
func NewWorkflowRuntime(opts Options) *WorkflowRuntime {
	rt := &WorkflowRuntime{
		logger:       opts.Logger,
		env:          opts.Env,
		storeFactory: opts.StoreFactory,
		timeline:     opts.Timeline,
		workflows:    make(map[string]*workflowRecord),
		index:        make(map[string][]*routeEntry),
		subs:         make(map[string]*busRef),
		outcomes:     make(map[string]RunResult),
		active:       make(map[string]*pipeline.RunContextImpl),
		stores:       make(map[string]store.Store),
	}
	if rt.logger == nil {
		rt.logger = core.NopLogger{}
	}
	rt.bus = opts.Bus
	if rt.bus == nil {
		rt.bus = events.NewMemoryScopedBus()
	}
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
// matching event spawns a run (subject to the execution mode gate).
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
// the background (mirrors WorkflowRuntime.dispatch + spawnRun).
func (rt *WorkflowRuntime) dispatch(eventType string, evt events.PipelineEvent) {
	rt.mu.Lock()
	bucket := rt.index[eventType]
	entries := make([]*routeEntry, 0, len(bucket))
	entries = append(entries, bucket...)
	rt.mu.Unlock()

	if len(entries) == 0 {
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

// spawnRun executes a pipeline for a trigger and fires onComplete. It runs
// synchronously within its goroutine; callers dispatch it in the background.
func (rt *WorkflowRuntime) spawnRun(record *workflowRecord, triggerID string, evt events.PipelineEvent) {
	runID := rt.generateRunID(record.workflow.ID, triggerID)
	result := rt.executePipeline(record, triggerID, runID, evt)
	if result.Status == "paused" {
		return // pause/resume is a Phase 6 concern; the run stays recorded
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
	runID := rt.generateRunID(workflowID, triggerID)
	result := rt.executePipeline(record, triggerID, runID, evt)
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
		Mode:       Mode{Type: "transient"},
		OnPrepare:  opt.OnPrepare,
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
	select {
	case res := <-done:
		return res, nil
	case <-ctx.Done():
		return RunResult{}, ctx.Err()
	case <-time.After(timeout):
		return RunResult{}, core.NewSystemError(core.ErrCodeTimeout, "workflow run timed out")
	}
}

// ---------------------------------------------------------------------------
// Pipeline execution
// ---------------------------------------------------------------------------

func (rt *WorkflowRuntime) executePipeline(record *workflowRecord, triggerID, runID string, evt events.PipelineEvent) RunResult {
	wf := record.workflow
	def := wf.Pipelines[triggerID]
	if def.ID == "" {
		return rt.finishRun(record, RunResult{
			RunID:      runID,
			WorkflowID: wf.ID,
			TriggerID:  triggerID,
			Status:     "failed",
			Error:      core.NewSystemError(core.ErrCodeNotFound, "no pipeline definition for trigger "+triggerID),
		})
	}

	st := rt.newStore(runID)
	rt.mu.Lock()
	rt.stores[runID] = st
	rt.mu.Unlock()

	// The run's bus scopes under the root bus so all pipeline events bubble to
	// the runtime bus (external subscribers and timeline recorder see them).
	bus := rt.bus.Scope(events.EventPath{})

	factory := pipeline.NewFactory(def, def.Schema, pipeline.FactoryOptions{Logger: rt.logger})
	runCtx := factory.Prepare(runID, st, bus)

	// Seed the trigger event into state (mirrors TS executePipeline). The event
	// payload is also aliased at `state.payload` so configs can address it with
	// the standardized dotted path (e.g. `payload.status`).
	_ = st.Update(context.Background(), store.SetValue("__trigger_event__", map[string]any{
		"type":      evt.Type,
		"payload":   evt.Payload,
		"timestamp": evt.Timestamp,
	}))
	_ = st.Update(context.Background(), store.SetValue("payload", evt.Payload))

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

	rt.mu.Lock()
	rt.outcomes[runID] = result
	rt.mu.Unlock()

	return result
}

func (rt *WorkflowRuntime) newStore(runID string) store.Store {
	if rt.storeFactory != nil {
		return rt.storeFactory(runID)
	}
	return store.NewMemoryStore(nil)
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

func (rt *WorkflowRuntime) generateRunID(workflowID, triggerID string) string {
	return workflowID + ":" + triggerID + ":" + uuid.NewString()
}