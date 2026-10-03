# Task: Architecture audit — complexities that add no benefit

- [x] Describe Hermes as an embeddable, durable, observable workflow runtime
- [x] Audit the architecture against that baseline for complexity with no benefit
  - **Context:** Third phase of the port audit (see `20261003-over-abstraction-audit.md` for type-level findings, `20261003-hotpath-ts-go-audit.md` for hot-path findings). Sources: full execution-architecture map (pkg/pipeline, pkg/store, pkg/nodekit, pkg/compiler) and full durability/observability map (actionlog, replay, events, timeline, runtime state).
  - **Status:** Done.

---

## Part 1 — What an embeddable, durable, observable workflow runtime is

Three properties, each with a minimal essential architecture:

### Embeddable
A library the host process imports. No required external services; persistence,
events, and scheduling are pluggable. The host defines workflows (from a graph or
directly), registers them, triggers them, and observes them — through one runtime
object.

### Durable
A run that is paused or in flight when the process dies must be recoverable. This
requires exactly **one** durable source of truth (an append-only log of what
happened), **one** recovery procedure (rebuild state from the log, resume at the
recorded address), and **one** place that maps run IDs to their recovery
information (a durable index that is repopulated at startup).

### Observable
Live subscribers receive lifecycle events; historical consumers read the same
recorded stream. Timeline/UI views are **projections** of the durable log — never
independently maintained state.

### The six essential components

```
1. Compiler      graph → executable workflow definition
2. Engine        stage/step state machine with routing (in-memory, synchronous)
3. State store   per-run state map, atomic commit at stage boundaries (in-memory)
4. Durable log   append-only, per-run; sole source of truth for recovery + history
5. Runtime       workflow registry, trigger dispatch, pause/resume bookkeeping
6. Event bus     fan-out to live subscribers and internal watchers
```

Everything else must be derived from these six or justify its existence.

---

## Part 2 — Audit: complexity with no benefit

### A. Six recovery paths where one is needed

The durable contract needs exactly one recovery procedure. The code has six:

| Path | Status | Verdict |
|---|---|---|
| 1. Checkpoint resume (`factory.go:169-183`) | Wired — fallback when action log is NopLog | **Redundant with path 2** — two mechanisms for the same job, selected by a *type assertion on the log implementation* (`runtime.go:1052`), not by data availability |
| 2. Event-sourced Replayer (`runtime.go:1051-1083`) | Wired — primary when log exists | The correct design; but falls back to path 1 on any error, silently degrading guarantees |
| 3. StoreLoader cold recovery (`runtime.go:1675-1687`) | **Dead** — zero callers repo-wide | Complexity with no benefit: `Options.StoreLoader`, `AnansiStoreFactory.Loader`, `NewPersistentStoreForID` all unreachable |
| 4. Durable Pebble event log (`events/durable.go`) | **Write-only** — `Underlying()` never called, `Subscribe` never touches it | Accumulates data that participates in no recovery path |
| 5. Watch buffering (`watchservice.go`) | Wired — closes register/pause race | Legitimate; not a recovery path |
| 6. `registry.FastPathResume`/`MarkPaused` (`registry.go:117-179`) | **Disconnected** — never called by `WorkflowRuntime` | A fourth resume mechanism with its own timers, documented in README as "two-tier resumption" that doesn't exist |

**No path survives process restart**: every path funnels through
`rt.paused[runID]`, a RAM-only map never repopulated from disk. So the system
pays for six recovery designs and delivers zero restart durability — the one
thing "durable" means.

### B. Checkpoint stored five times; state delta stored up to five times

- **Checkpoint ×5**: state map `__pipeline_data__` → DB row via milestone sync →
  action-log `KindCheckpoint.Output` → `rt.paused[].checkpoint` →
  `RunResult.Checkpoint` (`checkpoint.go:84`, `context.go:859`, `runtime.go:887,1584`).
- **Effectful step delta ×5**: `KindStepCompleted.Output` + byte-identical
  `KindEffectCompleted.Output` (`stage.go:335,351`) + committed store + milestone
  DB row + `outcomes[].FinalState` + `pipeline:success` event payload.
- **Run linkage ×5**: `state[__run_meta__]` → `metadata` column → `runMetas` →
  `Entry.PipelineID` → `pausedRun.*` → `RunResult` fields.
- **Run status ×3**: `runMetas[].Status`, `outcomes[].Status`, terminal log entries.
- **Rerun index ×2**: `rt.rerunIdx` vs derivable `actionLog.LatestRerunIndex` —
  two sources that diverge after janitor eviction.

Each copy is a consistency obligation with no consumer that couldn't read the
original.

### C. Two parallel observability channels, then a third independent one

Every lifecycle site does `bus.Emit` **and** `actionlog.Append` (15 append sites,
all paired with emissions). The timeline is a projection of the log (correct), but
`runMetas` — status/start/end — is maintained **independently in RAM**, never
reconciled against the log (`runtime.go:1448-1453, 1296-1319`), and `outcomes`
is a fourth store of terminal state. So:

```
one fact (run status) → log entries + runMetas + outcomes + RunResult = 4 records
one event (step:success) → bus delivery + 1–2 log entries + derived timeline row
```

Also: `KindEffectCompleted` duplicates `KindEffectCompleted`'s delta with
`KindStepCompleted` (`stage.go:343-353`) — a second log write of identical bytes
per effectful step, consumed by neither the Replayer (which keys by step id) nor
the timeline.

### D. Two replay engines that disagree

Go `pkg/replay` and TypeScript `src/replay.ts:67-135` implement the same
delta-folding replay. They diverge: Go keeps the *first* effect entry per step id
and the *last* route per stage; TS folds *every* output in seq order. For a
looping pipeline with effectful steps they produce different state. One engine,
one semantics — maintaining two is unbounded consistency cost.

### E. Dead and promised-but-absent subsystems

| Item | Evidence |
|---|---|
| `Options.StoreLoader` / `newStoreForID` | zero callers (`runtime.go:1675-1687`) |
| Durable event log reader | `Underlying()` uncalled (`events.go:126`) |
| `registry` two-tier resume | only a test touches `MarkPaused` |
| `TimelineStore.Append`, snapshots | `SPEC.md:309-322`; Go has only projection helpers; `SnapshotSeqs`/`SnapshotInterval`/`TimelineEvent.Snapshot` never populated |
| `pkg/server` HTTP layer | `SPEC.md:345`; `Run()` doc says "Phase 7" (`runtime.go:1352`); no `ListenAndServe` outside tests |
| `store.Flush` | no-op both impls, no caller in tree |
| `PipelineFactory.schema`, `PipelineDefinition.Schema` | written, never read; compiler never sets it (`factory.go:60`, `compiler.go:720`) |
| `Stage.Order` | assigned by compiler, asserted in tests, never read by `Run` (slice position is the real order) |
| `state["results"]` | read in 3 places (`nodekit.go:255`, `build.go:83,123`), never written anywhere in `pkg/` |
| `placeholder` edge role | accepted on wire, never consumed |
| `wfResources` | written (`runtime.go:574,1837`), never iterated for cleanup — leaked DB pools/connections |
| Event-source unsubscribe | returned `func()` discarded at `runtime.go:624`; never called on Deregister |

### F. Same concept, many representations (consistency obligations)

| Concept | Representations |
|---|---|
| Routing decision | `RoutingInstruction` → decision string → `serializeInstruction` → `interpretationOf` → `routingHandle` → event payload fields (`context.go:705-994`) |
| Pause | `PauseInstruction` ↔ `PipelineCheckpoint` ↔ nested JSON in state ↔ `KindCheckpoint` entry ↔ `RunResult` fields ↔ `pausedRun` ↔ event payload |
| Run ID | `store.ID()`, `RunContextImpl.runID`, `PipelineContext.RunID()`, `NodeRunContext.RunID`, context value, `Entry.RunID`, `PipelineEvent.RunID` — 7 carriers |
| Effect flag | `nodekit.Effect` enum ↔ `pkg/effect` int constants (hand-synced, import-cycle workaround) ↔ `Step.Effect int` |
| Handles | Go `Handles func` + `HandlesJS` source string per node, parity enforced only by a test |
| PipelineRegistry | `pipeline.PipelineRegistry` (resolve defs) vs `registry.PipelineRegistry` (track runs) — same name, unrelated |
| "Stage" | container→stage, executable node→stage, nested body/branch stages |
| Node definition | `TypedDefinition[C]` + raw `NodeDefinition` authoring styles |

### G. Execution chains with unnecessary hops

- **Run one step**: `Run` → `ExecuteStageSteps` (14-arg free function) → goroutine
  → `st.Read`+deep-copy → `executeStepAttempt` → BuildStep closure → resource
  resolution → **schema recompile + interpolate + coerce** → `bindFromMap` type
  erasure → typed `Run` → Mutator → collect → single `st.Update`. Nine layers;
  three of them (schema compile, interpolate, bind) repeat **per attempt**.
- **Branch**: node Router → BuildRouter closure → `resolveHandle` (compile-time
  edge closure) → `Jump` → `evaluateInstruction` → `FindStageIndex` → index assign.
  Default/advance routing is implemented **four times** (`compiler.go:413-419`,
  `compiler.go:563-574`, `build.go:194`, `router.go:11`).
- **Config**: `map[string]any` → interpolated map → coerced map → `*C` struct —
  three intermediate forms per execution for what is usually a static value.
- **Compiler**: traverses the graph three times (outer BFS, body BFS, fork-branch
  BFS) with near-identical logic, plus a fourth fork-membership computation.

### H. Structural costs that exist only to mimic JS semantics

- **Three simultaneous state copies** live at every step/route boundary (step
  snapshot, router snapshot, result snapshot) — mirrors JS reference semantics
  where a "copy" was a structuredClone only when expected.
- **`Promise.all` transliteration**: goroutine + WaitGroup + recover-closure per
  step even for single-step stages — fan-out where the work is synchronous.
- **EventEmitter transliteration**: per-emit handler-slice allocation + recursive
  scope bubbling; no topic isolation (`events.go:233-262` wontfix) — a scoped bus
  that doesn't isolate scope.
- **Single `rt.mu` over 10 maps** — direct transliteration of "all runtime state
  in one JS object"; forces exclusive lock across DB queries in `ListRuns`.

---

## Verdict

The six essential components are present and recognizable. The excess is
concentrated in four places:

1. **Durability**: six recovery paths, none restart-safe — the paid-for
   complexity doesn't buy the promised property. Collapse to: durable log +
   repopulated run index + one rebuild procedure; delete paths 3, 4, 6; fold
   checkpoint into the log (or keep it as the sole fast path, but not both).
2. **Observability**: dual-channel recording at every site plus two independent
   RAM stores of run status. Make `runMetas`/`outcomes` derived from the log;
   drop `KindEffectCompleted`; emit or log, not always both for the same fact.
3. **Representation churn**: every concept (routing, pause, run id, effect,
   handles) exists in 3–7 forms with hand-maintained correspondence. Pick one
   canonical form per concept; derive the rest at the boundary.
4. **Per-attempt recompilation**: schema/interpolation/binding/VM construction on
   the hot path where the inputs are static. Move to compile time (graph
   compilation already exists — it's the natural place).

---

### §E Dispositions (20261003 refactor — recorded item-by-item by D.5)

| §E Item | Disposition |
|---|---|
| `Options.StoreLoader` / `newStoreForID` | → D.1: deleted outright (zero callers; restart recovery returns as ONE startup-rebuild procedure in S.3) |
| Durable event log reader (`Underlying()`) | → D.1: deleted with the whole of `pkg/events/durable.go` (durable shape, no durability) |
| `registry` two-tier resume | → D.1: `MarkPaused`/`FastPathResume`/timer machinery deleted; README claim removed same commit |
| `TimelineStore.Append`, snapshots | → D.2: SPEC.md §9 carries an explicit NOT IMPLEMENTED marker; no code existed to delete |
| `pkg/server` HTTP layer | → D.2: SPEC.md directory line marked NOT IMPLEMENTED; `Run()` doc reference corrected in D.2/P6.1 |
| `store.Flush` | → D.2: deleted from interface + both impls; `Sync` remains |
| `PipelineFactory.schema`, `PipelineDefinition.Schema` | → D.2: field, factory field, and `NewFactory` parameter deleted; call sites updated |
| `Stage.Order` | → D.2: field deleted; `WorkflowView` emits slice index under the existing `"order"` key (view shape stable) |
| `state["results"]` | → D.4: three dead reads + `NodeRunContext.Results` deleted; the LIVE pipelines-router channel (`resultsByID` → `InterpolationContext.Results`, `${results.*}` interpolation, `PipelinesRouterFunc` results argument) is kept — the audit's read-never-written observation held for the step path only |
| `placeholder` edge role | → D.2: kept as accepted-and-ignored wire compat, documented in `pkg/compiler/wire.go` |
| `wfResources` | → D.2 DISPOSITION DEVIATION (recorded in-code): the map is a LIVE init-once cache read by `initResources`, not write-only state — deleting it would re-init shared services per run (Trap #5: demolition ≠ behavior change). The audit's real finding (never iterated for cleanup) is remedied by C2, which adds the teardown that iterates it in Deregister/Shutdown |
| Event-source unsubscribe discarded | → C2 (Phase 3): the unsubscribe func is retained in `workflowRecord` and invoked on Deregister |

OA dead-weight rows are dispositioned in the over-abstraction audit file (R.3) and CHANGELOG `Removed` (D.1–D.4); the machine gate for both is `tests/deletion_audit_test.go` (zero `// Deprecated:` declarations; deleted symbols stay dead) plus zero-declaration grep proofs in each demolition commit.
