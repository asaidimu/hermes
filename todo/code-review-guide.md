# Hermes Code Review Guide

A layer-by-layer review protocol for the hermes durable execution engine.
Each section defines what to check, what can go wrong, and this codebase's
specific known issues.

---

## How to Use This Guide

Read layers bottom-up (1 → 6). Each layer assumes the layers below it are
correct. If you find a bug in layer N, check whether layers N+1..6 compound
it.

For each finding, tag it with:
- **[C]** Correctness — produces wrong results or loses data
- **[R]** Resource — leaks memory, goroutines, timers, or file descriptors
- **[P]** Performance — unnecessary latency, allocations, or lock contention
- **[S]** Safety — data races, deadlocks, undefined behavior
- **[I]** Integration — breaks hestia compatibility or interface contracts

---

## Layer 1: Event Log (Pebble + go-events)

**Scope:** `go-events/v2` Pebble backend, `pkg/events/events.go`

### What it does
Append-only, ordered, at-least-once event delivery with subscriber
checkpoints. This is the system of record — if this layer is wrong,
everything above it is wrong.

### Review checklist

```
[ ] Ordering: UUIDv7 high bits = ms timestamp. Verify no code path
    generates events with duplicate or non-monotonic IDs.

[ ] Delivery: After crash, does Pebble redeliver from the last
    checkpoint? Check that subscriber offsets persist to disk.

[ ] Checkpoint gap: What happens if a subscriber crashes between
    processing event N and checkpointing N? Redelivery of N — is
    that safe for all handlers? (At-least-once requires idempotent
    handlers.)

[ ] MemoryScopedBus handler leak: Subscribe() at events.go:178
    compares &h == &handler — loop variable address, always the same.
    Unsubscribe silently fails. Every subscription leaks permanently.
    This is a known P0. FIX BEFORE MERGE.

[ ] Emit without lock: MemoryScopedBus.Emit() reads b.parent and
    b.underlying without lock. Are these fields truly immutable after
    construction? Document the immutability contract.

[ ] Wildcard dispatch: Subscribe("*", handler) receives ALL events.
    Does the TimelineRecorder's wildcard subscription cause O(n)
    handler calls per event? What's n under load?

[ ] Bus scope isolation: Scope() creates child buses. Does a child's
    Emit() only bubble to its parent chain, or does it leak to
    siblings?
```

### Known issues
| ID | File:Line | Tag | Description |
|----|-----------|-----|-------------|
| 015 | events.go:178 | [C][R] | Unsubscribe compares loop var address, never matches |
| 016 | events.go:108 | [S] | Emit reads parent/underlying without lock |

---

## Layer 2: State Persistence (Store + Checkpoint)

**Scope:** `pkg/store/`, `pkg/pipeline/checkpoint.go`

### What it does
Manages per-run document state. `MemoryStore` is in-process.
`PersistentStore` writes through to anansi. Checkpoints serialize
resume position into document metadata.

### Review checklist

```
[ ] Document() pointer escape: store.go:64 returns *document.Document
    after releasing RLock. Callers mutate freely — data race with
    concurrent Read/Update. Known P0. All 21 call sites that call
    doc.Set() directly are affected.

[ ] WriteCheckpoint → Flush gap: context.go:584-586 writes checkpoint
    then Flush(). If crash occurs between, checkpoint is in memory
    but not on disk. Is the window acceptable? (μs with local
    anansi, ms with remote.)

[ ] PersistentStore I/O under lock: persistent.go:112 holds
    mu.Lock() during persist() which does anansi network I/O.
    Slow network → all callers block. Consider write-through queue
    pattern: mutate in-memory under lock, persist outside lock.

[ ] Checkpoint backward compat: PipelineCheckpoint.Snapshot was
    added recently. Old checkpoints without this field deserialize
    to nil snapshot. Does resumeFromPersistence handle nil snapshot
    correctly?

[ ] Transact vs Update: Both have identical lock semantics. No
    rollback, retry, or isolation. Either add real transactional
    semantics or remove Transact from the interface.

[ ] ExportJSON round-trip: store.go:133 marshals to JSON then
    immediately unmarshals back. Wasteful. Add ToMap() method to
    document type.

[ ] Clone deadlock: store.go:156 holds RLock then calls ExportJSON
    which also acquires RLock. Safe today (readers don't block
    readers), but fragile if any path changes to Lock.
    Use exportJSONLocked() (already done in PersistentStore).
```

### Known issues
| ID | File:Line | Tag | Description |
|----|-----------|-----|-------------|
| 005 | store.go:64 | [S] | Document() returns mutable pointer after RUnlock |
| 008 | store.go:156 | [S] | Clone RLock → ExportJSON RLock deadlock risk |
| 019 | store.go:29 | [I] | Transact is identical to Update |
| 020 | store.go:126 | [P] | ExportJSON Marshal/Unmarshal round-trip |

---

## Layer 3: Pipeline State Machine

**Scope:** `pkg/pipeline/context.go`, `factory.go`, `types.go`

### What it does
Executes stages → steps → routers as a state machine. Handles
sub-pipeline fork/join, pause/resume, abort, and cron scheduling.

### Review checklist

```
[ ] Goroutine leak: context.go:124 spawns goroutine per Run() to
    bridge abortChan → cancel(). On normal path, blocks until
    runCtx.Done(). Brief window where goroutine outlives Run().
    Use context.AfterFunc (Go 1.21+).

[ ] Sub-pipeline pause bubbling: context.go:290-303 constructs
    nested checkpoint. Does the Snapshot capture the FULL document
    state including nested pipeline state?

[ ] Router error handling: What happens if a router returns an error?
    Check that the pipeline transitions to "failed" and the
    checkpoint is still written.

[ ] Step mutation safety: Steps call pcxt.Write() which calls
    r.store.Update(context.Background(), ...). The Background
    context means mutations survive abort. Is this intentional?

[ ] Abort vs Pause: Abort closes abortChan, which cancels runCtx.
    But if a step is blocked on I/O (e.g., HTTP call), does the
    context cancellation actually interrupt it? Depends on step
    implementation.

[ ] Nil bus/logger guard: context.go:106-112 now creates defaults
    in Run() if nil. But NewRunContext no longer creates defaults.
    Ensure all callers of NewRunContext provide non-nil bus/logger.

[ ] Factory Prepare fallback: factory.go:51-58 still falls back to
    NewMemoryScopedBus() if no bus provided. Is this the right
    place for the default, or should it error?
```

### Known issues
| ID | File:Line | Tag | Description |
|----|-----------|-----|-------------|
| 055 | context.go:124 | [R] | Goroutine leak on normal Run path |
| 048 | context.go:84 | [C] | Write() uses Background ctx, survives abort |

---

## Layer 4: Recovery & Resume

**Scope:** `pkg/runtime/runtime.go` (Resume, resumeFromPersistence)

### What it does
Two-tier crash recovery:
1. In-memory `rt.paused` map (fast path)
2. Anansi persistent store via `resumeFromPersistence` (crash recovery)

### Review checklist

```
[ ] Checkpoint loading: resumeFromPersistence iterates ALL workflows'
    ALL pipelines looking for a checkpoint. O(w*p) per resume. Is
    this acceptable? Consider indexing by runID.

[ ] Nil snapshot handling: If old checkpoint has no Snapshot field,
    ckpt.Snapshot is nil. Does Resume() handle nil snapshot when
    folding payload into state?

[ ] triggerID unknown: resumeFromPersistence sets triggerID to "".
    Does anything break? Check callOnComplete, watchService,
    outcomes.

[ ] Workflow deregistered: If workflow is deregistered between crash
    and resume, "workflow not found" is returned. Is this the right
    behavior, or should we attempt reconstruction from timeline?

[ ] Store state consistency: PersistentStore loads full document from
    anansi. But checkpoint was written before Flush(). If crash
    occurred between WriteCheckpoint and Flush, the anansi document
    has old state but checkpoint has new position. On resume, the
    pipeline resumes from new position but state is old. Data loss.

[ ] Resume goroutine unbounded: runtime.go:547 spawns goroutine per
    resume event with no cap. Burst of resume events → unbounded
    goroutines. Add worker pool.

[ ] Active map cleanup: rt.active[runID] is set before Run() and
    cleared after. But if Run() panics, clearActive is never called.
    Is the panic recovered somewhere?
```

### Known issues
| ID | File:Line | Tag | Description |
|----|-----------|-----|-------------|
| — | runtime.go:547 | [R] | Unbounded resume goroutines |
| — | runtime.go:821 | [R] | outcomes map never cleaned |
| — | runtime.go:1017 | [R] | stores map never cleaned |

---

## Layer 5: Concurrency & Resource Safety

**Scope:** Cross-cutting — locks, goroutines, timers, maps

### Review checklist

```
[ ] PersistentStore lock during I/O: persistent.go:112 holds
    mu.Lock() during persist() → anansi I/O. All concurrent
    Update/Transact/Flush calls block. Measure under load.

[ ] WorkflowRuntime mutex discipline: runtime.go acquires rt.mu
    multiple times per method (e.g., Resume: lock→unlock at 732,
    lock→unlock at 772, lock→unlock at 820). Between unlocks,
    maps can mutate. Is each critical section independent?

[ ] WatchService lock gap: watchservice.go:203 unlocks to call
    Resume(), re-locks at 207. Registrations map can mutate.
    Collect entries first, unlock, process, re-lock.

[ ] Timer leaks: server.go:229 uses time.After (leaks 10s per
    request). delay/delay.go:66 same pattern. Replace with
    time.NewTimer + defer Stop().

[ ] context.Background() audit: List of all uses:
    - context.go:84 (Write) — should propagate run context
    - runtime.go:777,1094 (Run) — intentional, pipelines run to completion
    - runtime.go:360 (cron emit) — acceptable
    - inmemory.go:62 (scheduler callback) — context never propagated
    - runtime.go:1199,1214,1225 (service init/cleanup) — should use
      shutdown context

[ ] Memory leak path: event subscription → handler closure captures
    store/store.Update → store written to rt.stores → never deleted.
    Chain: subscription leak + store leak = unbounded memory.

[ ] Node type registry: nodekit.go:104 global map grows unbounded.
    Acceptable for static registration. Block dynamic registration.
```

---

## Layer 6: Integration Surface (Hestia Adapters)

**Scope:** `pkg/core/zap_adapter.go`, `pkg/scheduler/hestia.go`,
`pkg/runtime/hestia_eventsource.go`, `pkg/store/anansi.go`

### What it does
Bridges hermes interfaces to hestia implementations. These adapters
must preserve exact semantics or the integration silently breaks.

### Review checklist

```
[ ] ZapLogger field conversion: zap_adapter.go:44 assumes
    keysAndValues are alternating string/any pairs. What if caller
    passes odd number of args? What if key is not a string?

[ ] HestiaSchedulerFunc cancel semantics: hestia.go:38 — Cancel()
    calls s.remove(id) and returns nil. But remove returns bool
    (found or not). Should Cancel return error when not found?
    Check what hermes callers expect.

[ ] HestiaSchedulerFunc shutdown: hestia.go:43 — if stop is nil,
    Shutdown returns nil. Is this safe, or should it error to
    surface misconfiguration?

[ ] HestiaEventSourceFunc register: hestia_eventsource.go:30 — if
    register is nil, OnRegister returns (nil, nil). Caller gets
    nil cleanup function. Does runtime handle nil cleanup?

[ ] AnansiStoreFactory error swallowing: anansi.go:52 — AsStoreFactory
    returns NewMemoryStore on error. Caller has no idea persistence
    failed. This silently degrades from durable to ephemeral.
    At minimum, log the error.

[ ] AnansiStoreFactory collection lifecycle: anansi.go:33 — opens
    collection once. If anansi restarts/reconnects, is the
    collection handle still valid?

[ ] Interface compliance: Check all `var _ Interface = (*Type)(nil)`
    assertions compile. Adding Flush to Store broke any external
    implementations.
```

---

## Quick Reference: Reviewer Decision Tree

```
Is the finding a data loss risk?
├─ Yes → Tag [C], must fix before merge
└─ No
    Is it a resource leak?
    ├─ Yes → Tag [R], fix if runtime > 1 hour
    └─ No
        Is it a performance issue?
        ├─ Yes → Tag [P], measure before optimizing
        └─ No
            Is it a safety issue (race/deadlock)?
            ├─ Yes → Tag [S], fix before merge
            └─ No
                Is it an integration issue?
                ├─ Yes → Tag [I], fix before hestia merge
                └─ No → Document as tech debt
```

---

## Pre-Commit Checklist

Before committing any changes touching these layers:

```
[ ] go build ./...
[ ] go vet ./...
[ ] go test ./... (all pass)
[ ] No new goroutine spawns without bounded pools
[ ] No new maps without cleanup paths
[ ] No new time.After (use time.NewTimer + defer Stop)
[ ] No new context.Background() without justification comment
[ ] No new mutex held during I/O
[ ] All new interface methods have implementations in all implementors
[ ] Known issues section updated if new findings
```
