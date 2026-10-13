// @note #no-production-grade-persistent-s-a8ee856c observation P1 #review,#production-readiness,#persistence,#ha : No production-grade persistent store or HA story ships by default
// @author hermes-review
//
// store.Store defaults to store.NewMemoryStore for every run
// (newStore/storeFactory). The action log defaults to an in-memory
// implementation too (actionlog.NewMemoryActionLog is used throughout the
// tests and examples); no persistent event bus ships. Net effect: a single
// process crash loses every paused run and in-memory action log entry
// unless the embedding host has independently wired a persistent Store +
// ActionLog — none of which hermes ships an implementation of (no
// Postgres/SQLite-backed Store, no clustering/leader-election for running
// more than one runtime instance against the same workflows). This is the
// single biggest gap versus a tool like n8n, which persists executions and
// credentials in Postgres/SQLite out of the box and supports queue-mode
// HA. Until hermes ships at least one real persistent Store
// implementation, it should be described as an embeddable in-memory
// engine with pluggable persistence hooks, not as production-ready
// workflow infrastructure on its own.
//
// 20261003 refactor note (plan D.1): Options.StoreLoader and
// newStoreForID — the "recover a run's store by id" hook that participated
// in no recovery path (zero callers; the only "call site" was its own
// wiring) — were deleted under the delete policy. Restart recovery returns
// in Phase 5 (S.3) as ONE startup-rebuild procedure off the action log.

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

        "github.com/asaidimu/hermes/pkg/actionlog"
        "github.com/asaidimu/hermes/pkg/events"
        "github.com/asaidimu/hermes/pkg/pipeline"
        "github.com/asaidimu/hermes/pkg/scheduler"
        "github.com/asaidimu/hermes/pkg/store"
        "github.com/asaidimu/hermes/pkg/timeline"

        "go.uber.org/zap"
)

// 20261003 refactor (plan R.3): the former SecretProvider interface had zero
// implementations in the tree — hosts back the two operations with plain
// funcs on Options instead:
//
//   - SecretGet resolves a secret by key (nil, false) when unknown. Values
//     are consumed in-process by steps and must never be persisted or logged.
//   - SecretHas reports whether a key is resolvable WITHOUT reading its
//     value; used for pre-flight validation at workflow registration time.

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
        Bus *events.MemoryScopedBus
        // StoreFactory mints a brand-new store per run. The returned store's ID()
        // becomes the run identifier: a run IS its state document, and creating
        // that document creates the run's identity (a system-minted UUIDv7). When
        // nil, a bare store.NewMemoryStore is used for every run.
        StoreFactory func() (store.Store, error)
        // SecretGet resolves credentials requested by nodes via their declared
        // Requirements. Values surface to steps only through NodeRunContext.Secret
        // lookups — they never persist to state, checkpoints, or events. When nil,
        // workflows whose nodes declare required secrets fail registration.
        SecretGet func(ctx context.Context, key string) (any, bool)
        // SecretHas reports whether a secret key is resolvable without reading
        // its value (Register-time pre-flight). When nil, no secret requirements
        // can be satisfied.
        SecretHas func(ctx context.Context, key string) bool
        Logger    *zap.SugaredLogger
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
        // ActionLog is the unified, append-only observability log. Every
        // significant event during execution is recorded here, keyed by
        // (runID, rerunIndex). It is the source of truth for crash recovery
        // (via the Replayer) and for run history (ListRuns/GetEvents derive
        // from it). When nil, logging is disabled.
        ActionLog actionlog.Store
        // RunHistoryTTL bounds how long the runtime keeps per-run bookkeeping
        // in memory (#review-20260910-004).
        //
        // Regardless of this setting, a run's STATE STORE and rerun index are
        // evicted as soon as the run reaches a terminal status (succeeded,
        // failed, aborted) — paused runs keep theirs until they resume and
        // finish, since resume needs them. What the TTL additionally bounds is
        // the audit metadata that outlives a run on purpose: runMetas (ListRuns
        // history) and outcomes (GetRunOutcome results, which carry the full
        // FinalState map). When RunHistoryTTL > 0, a janitor purges that
        // metadata for terminal runs older than the TTL (checked every
        // TTL/10, at least once per minute); when 0 (the default), terminal
        // history is kept forever — matching the pre-fix behavior for hosts
        // that rely on unbounded ListRuns/GetRunOutcome.
        //
        // The durable record is unaffected: the action log remains the source
        // of truth for recovery and history.
        RunHistoryTTL time.Duration
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
        Events     *events.MemoryScopedBus
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
        bus          *events.MemoryScopedBus
        logger       *zap.SugaredLogger
        env          map[string]any
        secretGet    func(ctx context.Context, key string) (any, bool)
        secretHas    func(ctx context.Context, key string) bool
        storeFactory func() (store.Store, error)
        eventSource  EventSource
        scheduler    scheduler.Scheduler
        actionLog    actionlog.Store
        watchService *WatchService

        mu        sync.RWMutex
        workflows map[string]*workflowRecord
        index     map[string][]*routeEntry
        subs      map[string]*busRef
        outcomes  map[string]RunResult
        active    map[string]*pipeline.RunContextImpl
        stores    map[string]store.Store
        paused        map[string]*pausedRun // runID → paused run waiting for an event
        pausedByEvent map[string]map[string]*pausedRun // eventType → runID → paused run (P4.6 dispatch index)
        runMetas      map[string]*timeline.RunTimelineMeta
        rerunIdx  map[string]int // runID → current rerunIndex (0 = first attempt)

        // runHistoryTTL drives the terminal-history janitor (see
        // Options.RunHistoryTTL). janitorStop cancels its ticker loop;
        // janitorStopOnce makes Shutdown idempotent against double-close.
        runHistoryTTL   time.Duration
        janitorStop     chan struct{}
        janitorDone     chan struct{}
        janitorStopOnce sync.Once
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

        mu sync.Mutex
        // wfResources caches workflow-scoped service handles across runs (the
        // init-once guard in initResources). The 20261003 audit (§E) flagged
        // this map as leaked because nothing ever iterated it for cleanup —
        // the D.2 demolition KEPT it for that reason: it is a live cache, not
        // write-only state. C2 added the teardown: Deregister and Shutdown
        // iterate it via workflowResourceCleanup.
        wfResources map[string]any
        // unsub is the EventSource.OnRegister cleanup func, retained from
        // Register (pre-C2 it was discarded — audit §E "event-source
        // unsubscribe") and invoked on Deregister.
        unsub func()
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
                logger:        opts.Logger,
                env:           opts.Env,
                secretGet:     opts.SecretGet,
                secretHas:     opts.SecretHas,
                storeFactory:  opts.StoreFactory,
                actionLog:     opts.ActionLog,
                workflows:     make(map[string]*workflowRecord),
                index:         make(map[string][]*routeEntry),
                subs:          make(map[string]*busRef),
                outcomes:      make(map[string]RunResult),
                active:        make(map[string]*pipeline.RunContextImpl),
                stores:        make(map[string]store.Store),
                paused:         make(map[string]*pausedRun),
                pausedByEvent:  make(map[string]map[string]*pausedRun),
                runMetas:      make(map[string]*timeline.RunTimelineMeta),
                rerunIdx:      make(map[string]int),
                runHistoryTTL: opts.RunHistoryTTL,
        }
        rt.startHistoryJanitor()
        // rt.actionLog nil = logging disabled (R.3 deleted NopLog).
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
                rt.logger = zap.NewNop().Sugar()
        }
        rt.bus = opts.Bus
        if rt.bus == nil {
                // 20261003 refactor (plan D.1): the "durable" bus backend
                // (events.WithDurableBackend) was deleted — its store was an
                // in-memory map, its reader had zero callers, and its presence
                // implied durability the design did not deliver. Options.Bus stays
                // the injection point for any future genuinely-durable bus.
                rt.bus = events.NewMemoryScopedBus()
        }
        // @note #review-20260910-005 observation P2 resolved status=resolved priority=P2 tags=#review,#concurrency,#api : Watch resume runs the whole pipeline synchronously in the emitter's goroutine
        // @author hermes-review
        //
        // Reopened and re-resolved: the earlier fix here wrapped this callback
        // in a goroutine to stop a fire-and-forget Emit from blocking for the
        // whole resumed pipeline's duration. That broke chained re-pause
        // semantics: TestResumeRepauseSingleEvent/MultiEvent in
        // repause_test.go rely on ms.Emit("evt:1", ...) returning only after
        // the run has resumed AND re-paused on stage:pause2's watch — so that
        // the immediately-following ms.Emit("evt:2", ...) lands on a watch
        // registration that already exists. Made async, Emit returns before
        // Resume even starts; awaitPaused's `len(rt.paused) == 1` check then
        // passes vacuously against the STALE first pause, the test emits
        // evt:2 into a watch that doesn't exist yet, and the run hangs forever
        // waiting for an event that was already dropped.
        //
        // Kept synchronous, matching the original code and the line ~1241
        // buffered-redelivery path's contract (which also depends on
        // rt.paused being updated before it fires further work): chained
        // pause -> resume -> re-pause within one external event must complete
        // before the triggering Emit call returns, so a caller who then emits
        // the NEXT expected event finds the watch already registered. This
        // does mean a workflow-internal bus.Emit (or an HTTP webhook handler
        // built on one) blocks for the resumed segment's duration — that's
        // the tradeoff the test suite pins down as correct today. A safer fix
        // for the original blocking concern would need the watch service (or
        // its caller) to expose a way to wait for "resumed and re-paused (or
        // completed)" explicitly, rather than a bare goroutine hand-off.
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
func (rt *WorkflowRuntime) Bus() *events.MemoryScopedBus { return rt.bus }

// Store returns the store created for a run (nil if the run never prepared).
func (rt *WorkflowRuntime) Store(runID string) store.Store {
        rt.mu.Lock()
        defer rt.mu.Unlock()
        return rt.stores[runID]
}
