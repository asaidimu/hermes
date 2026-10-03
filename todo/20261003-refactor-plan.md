# Task: Refactor plan — phased remediation of the 20261003 audit findings

- [ ] Execute the refactor in phases 0–5 below, on one branch, committing sequentially.
  - **Context:** Three audits, all dated 20261003 and all verified against HEAD `3957d54`, found: (1) TS→Go hot-path hazards — deadlocks, leaks, O(n²) I/O (`todo/20261003-hotpath-ts-go-audit.md`); (2) system-level complexity with no benefit — six recovery paths, none of which survive a process restart, state duplication ×5, dual observability channels (`todo/20261003-architecture-audit.md`); (3) TS-port over-abstraction — 11 single-implementation interfaces, wrapper layers, dead code (`todo/20261003-over-abstraction-audit.md`). Every task below cites its audit ID (C*/H*/M*/L* = hot-path audit; §A–§H = architecture audit; OA-* = over-abstraction audit). Spot-verification at HEAD confirmed C1, C2, C3, H9, the dead `newStoreForID` path, read-only `state["results"]`, and uncalled `Underlying()` are all still live.
  - **Details:** Binding constraints (decided 20261003, see table below): correctness-first ordering; restart durability IS in scope; public Go API preserved (deprecate before removal — removals deferred to v2); TypeScript side is Go-first with the `.ts` catalog frozen as wire-format; verification bar is `go test -race ./...` green per commit plus `make bench` before/after for every perf change; delivery is ONE branch with sequential commits, shipped together.
  - **Files:** Repo-wide; per-task file lists below.

## Binding constraints

| Constraint | Decision |
|---|---|
| Scope | All three audits in one plan |
| Ordering | Correctness → performance → architecture → abstraction → docs |
| Restart durability | In scope: one durable log, one repopulated run index, one rebuild procedure (architecture §A verdict) |
| API compat | v1.x preserved: exported symbols stay (deprecated), internal call paths go concrete; v2 removal list maintained |
| TypeScript | Go-first: Go replay semantics are canonical; `src/*.ts` + node `.ts` catalog frozen as wire-format; divergence documented, golden vectors published |
| Verification | Per commit: `go vet ./...` + `go test -race ./...`. Perf commits: `make bench` before/after; `benchstat` table required for the structural batch (H1, H8, M1–M3). Phase gates: `make bench-race` |
| Delivery | One branch `refactor/20261003-audits`; sequential commits; each commit green; ship together |

## Non-negotiable traps (read before starting any phase)

1. **Chained re-pause semantics.** Commit `f1e5630` already reverted an async watch-resume change because it broke chained re-pause. Any H5/L3 fix must preserve re-pause ordering: `pkg/runtime/repause_test.go` and `pkg/runtime/repause_bench_test.go` are the contract. Do not make resume uniformly async without proving chained re-pause still settles in order.
2. **`KindEffectCompleted` removal is coupled to replay semantics.** The Replayer keys effectful deltas on `KindEffectCompleted | KindEffectFailed` (`pkg/replay/replayer.go:669-675`). Stopping the duplicate write without updating the fold loses replay data. Sequence: C3 fixes storage only (P2.1); the write-stop + replayer fold update happen together in P3.4/P3.5 with golden vectors in place.
3. **Action log storage migration.** Anansi runs persist as ONE document holding the whole entries array (`pkg/actionlog/anansi.go:187-214`). A new append-only layout must dual-read old documents, or old runs become unrecoverable.
4. **API compat.** Every "delete" below that touches an exported symbol becomes "deprecate + preserve behavior". Maintain the v2 removal list in CHANGELOG as you go.
5. **Wire format stability.** Canvas JSON (`compiler.DecodeWireGraph` / `compiler.CompileWire`) and the npm package types are a public contract. The `placeholder` edge role stays accepted-and-ignored; `.ts` node catalog and `src/*.ts` are not modified behaviorally in this refactor.

---

## Phase 0 — Safety net (before any refactor)

- [ ] P0.1 Capture baseline: benchmarks + race status
  - **Context:** Phase 2 perf claims need before/after evidence, and the race detector must be green before locking is touched. The repo already has `make bench` / `make bench-race` targets (`#improvement-bench-ci-001`) and suites: `pkg/replay/rebuild_bench_test.go`, `pkg/runtime/repause_bench_test.go`, `pkg/scheduler/cron_cache_bench_test.go`.
  - **Details:** Run `go test -race ./...` and record any failures as pre-existing (file them, do not fix inside the refactor silently). Run `make bench` and save output as the baseline. Existing benchmarks cover rebuild latency, chained re-pause, and cron parsing — the stage-execution path (H1/H2) has no benchmark yet, which P0.2 adds.
  - **Files:** artifact `todo/20261003-bench-baseline.txt`; read-only `Makefile`.

- [ ] P0.2 Characterization tests pinning current observable behavior
  - **Context:** The refactor changes locking, storage, and (later) replay semantics. Tests must pin observable behavior first so semantic drift is caught at the offending commit, not in production. This follows the pattern of `pkg/pipeline/stage_snapshot_test.go`, which already pins `#review-20260910-009`.
  - **Details:** Add table-driven tests: (a) **replay fold** — given a scripted entry stream (step start/complete, effect complete/fail, retry, rerun attempt re-baselining per `#review-20260910-025`, checkpoint mid-stream), assert rebuilt state and resume address; (b) **checkpoint/resume parity** — a deterministic graph's pause→resume end state equals its uninterrupted end state; (c) **action log seq stability** — seq numbering derives from max recorded Seq, not entry count (behavior documented at `pkg/actionlog/anansi.go:192-200`); (d) **cron auto-resume arming** — arms only when `Checkpoint.Cron` set; re-pause cancels the stale schedule (documents H9's missing cancel as today's behavior, then Phase 1 flips the terminal branch and the test extends).
  - **Files:** new `pkg/replay/characterization_test.go`, `tests/pause_resume_characterization_test.go`, `pkg/actionlog/characterization_test.go`.

- [ ] P0.3 Golden replay vectors (shared artifact)
  - **Context:** Phase 3 unifies replay semantics (architecture §D: Go keeps first effect entry per step id, TS folds every output in seq order) and stops writing `KindEffectCompleted`. Golden vectors make the semantics change explicit, reviewable, and give npm consumers a reference implementation target.
  - **Details:** JSON files of shape `{entries: [...], expectedState: {...}, expectedResumeAddress: {...}}` covering: first-vs-last effect per step, route folding across loop iterations, multi-attempt rerun re-baselining, checkpoint folded mid-stream. A Go test loads and runs every vector. `src/replay.ts` gets a frozen comment block pointing at the vectors and documenting the known divergence (no behavior change — TS is frozen wire-format per constraints).
  - **Files:** new `pkg/replay/testdata/golden/*.json`, `pkg/replay/golden_test.go`; comment-only touch `src/replay.ts`.

---

## Phase 1 — Correctness: deadlocks, leaks, re-entrancy (hot-path audit §Suggested order 1)

- [ ] P1.1 C1 — Replay must not execute step code under the store read lock
  - **Context:** `pkg/replay/replayer.go:780-783` runs `step.Action` inside `st.Read(...)`. Any step calling `store.Update` self-deadlocks (RWMutex is non-reentrant), and arbitrary user code (goja eval, HTTP) runs while all writers are blocked. The identical hazard was fixed for normal execution in `#review-20260910-009` (`pkg/pipeline/stage.go:166-196`); the replay path never got the fix.
  - **Details:** Apply the same shape as `executeStepAttempt`: snapshot under `Read` (or reuse `store.DeepCopyMap` on the pinned map), release, run `step.Action` on the snapshot, then apply the returned mutator through `st.Update`. Preserve the replay-specific context (`pipeline.NewReplayContext()`) and error semantics (`actionErr` vs `readErr` distinction must survive). Extend the P0.2 replay characterization test with a step whose Action calls `st.Update` to pin the deadlock fix.
  - **Files:** `pkg/replay/replayer.go` (executeStep path ~line 730-800); test in `pkg/replay/replayer_test.go`.
  - **Verify:** `go test -race ./pkg/replay/...`; new deadlock-regression test green.

- [ ] P1.2 C2 — Bound `Register`'s critical section; fix the two resource leaks on the same path
  - **Context:** `pkg/runtime/runtime.go:557-646` holds `rt.mu` (global, non-reentrant) across (a) `secrets.Has` (vault/network I/O, via `validateRequirements` → :565), (b) `rt.scheduler.Schedule`, and (c) host `eventSource.OnRegister` (:624), whose `emit` closure calls `rt.bus.Emit → dispatch → rt.mu.Lock()` (:751) — a latent self-deadlock the EventSource docs invite. Two adjacent leaks: the `OnRegister` unsubscribe `func()` is discarded (:624, never called on Deregister), and `wfResources` is written (:574, :1837) but never iterated for cleanup — leaked DB pools/connections (architecture §E).
  - **Details:** Restructure: under `rt.mu` — validate uniqueness, insert `workflowRecord`, update `rt.index`, snapshot the trigger set; release `rt.mu`; then run `validateRequirements` (secrets I/O), `scheduler.Schedule`, and `eventSource.OnRegister`. On failure, roll back under a fresh `rt.mu` critical section (rollback body already exists at :629-646, keep it, but move it into a helper so it locks rather than assumes the caller's lock). Call and retain the EventSource unsubscribe func in `workflowRecord`; invoke it in Deregister. Add `wfResources` teardown (close/iterate) in Deregister and Shutdown. Care: after releasing `rt.mu` before `OnRegister`, a concurrent Deregister of the same workflow ID becomes possible — re-validate existence before rollback and make record removal idempotent.
  - **Files:** `pkg/runtime/runtime.go` (:553-660, Deregister ~:660-690, Shutdown ~:1322-1360); new `pkg/runtime/register_lifecycle_test.go` covering: host `emit()` inside `OnRegister` (deadlock regression), Deregister unsubscribe, wfResources cleanup.
  - **Verify:** `go test -race ./pkg/runtime/...` including a test where `OnRegister` synchronously calls `emit`.

- [ ] P1.3 H9 + L2 — Cancel cron auto-resume on terminal outcomes; deterministic watch-timer owner
  - **Context:** `pkg/runtime/runtime.go` schedules a self-re-arming auto-resume timer on pause (:901-914) and cancels it ONLY on re-pause (:1281) or workflow Deregister (:681, workflow-trigger schedules only). The terminal branch (:1287-1291) cleans `watchService.OnRunEnded` but never `scheduler.Cancel(runID)` — every run that pauses with `Checkpoint.Cron` and then completes leaks a permanent timer that grabs `rt.mu` forever. L2: `watchservice.go` `startTimeoutLocked` assigns the shared timeout timer to an arbitrary registration from map iteration.
  - **Details:** Add `rt.scheduler.Cancel(runID)` next to `watchService.OnRunEnded(runID)` in ALL terminal branches (completion at :1287-1291, and any failed/aborted paths that clean the watch service — grep `OnRunEnded` for the full set; also check the aborted branch). Extend P0.2's cron test: pause with cron → force-complete → assert no further timer fires (expose a counting scheduler in the test, `scheduler.Scheduler` is already an interface — acceptable to use here since a fake is genuinely needed). For L2, assign the timer to the earliest-deadline registration (deterministic) or store per-registration timers; keep behavior identical otherwise.
  - **Files:** `pkg/runtime/runtime.go` (terminal branches), `pkg/runtime/watchservice.go`; tests in `pkg/runtime/runtime_test.go` / `eviction_test.go` area.
  - **Verify:** leak test green under `-race`; `make bench-race` unaffected.

- [ ] P1.4 M5 — Fix lock-order fragility in `copyStoreState` and `ReadCheckpoint`
  - **Context:** `pkg/pipeline/factory.go:201-210` nests `src.Read` → `dst.Update` (two stores' locks held; safe today only by accident of ordering). `factory.go:172-176` runs a full checkpoint JSON round trip inside `Read`, stretching the RLock.
  - **Details:** `copyStoreState`: snapshot src under `Read` into a local map, return from `Read`, then apply a single mutator on dst. `ReadCheckpoint`: copy the raw checkpoint bytes out under `Read`, parse outside. No behavior change intended; pin with a unit test that runs copy between two stores concurrently with reverse-direction copies to prove no inversion.
  - **Files:** `pkg/pipeline/factory.go` (:169-215).
  - **Verify:** `go test -race ./pkg/pipeline/...`.

- [ ] P1.5 H5 + L3 — Serialize watch-service resume without breaking chained re-pause
  - **Context:** `pkg/runtime/watchservice.go` `onEvent` releases `s.mu` and calls `resumeCallback` → `rt.Resume` → full pipeline → `bus.Emit` → `onEvent` again: unbounded same-goroutine recursion with an unlock/relock window on `s.registrations`. Resume is inconsistently sync/async: inline at `runtime.go:895-897` (runs BEFORE cron scheduling at :901-914, so a stale cron can overwrite the newer schedule), async at :801-804 and :1260 (L3). **Trap:** commit `f1e5630` reverted an async watch-resume callback because it broke chained re-pause semantics — this task must beat that history, not repeat it.
  - **Details:** Two moves, kept minimal: (1) make the resume dispatch uniform — one code path, always through the same mechanism (a dedicated serializer goroutine or ordered task queue for `resumeCallback`, with the watch service callback posting a task and returning; registration mutation happens only on the serializer). (2) Order fix: cron scheduling for a newly paused run must read the FINAL pause state (move the inline `rt.Resume` :895-897 out of the pre-cron position, or schedule cron after the resume attempt settles) so a stale schedule can never overwrite a newer wait. Before writing code, write the failing test from `repause_test.go` scenarios at depth ≥ 3 chained re-pauses; run it against the new implementation AND against pre-refactor code to confirm it captures the ordering contract. If the full serializer proves riskier than the window it closes, land only the cron-ordering half and mark the serializer half `[X]` with the reason — the unlock window is real but the re-pause regression is worse.
  - **Files:** `pkg/runtime/watchservice.go`, `pkg/runtime/runtime.go` (:795-915 spawn/dispatch resume paths); tests `pkg/runtime/repause_test.go` (extend), new `pkg/runtime/watch_resume_order_test.go`.
  - **Verify:** `go test -race ./pkg/runtime/...` with repause suite at depth 10; `make bench-race` includes `BenchmarkChainedRePause` — no regression.

---

## Phase 2 — Hot-path performance (hot-path audit §Suggested order 2–5)

Every commit in this phase carries `make bench` before/after output; the structural batch (P2.7, P2.9, and M-items in P2.8) additionally requires a `benchstat` table against the P0.1 baseline.

- [ ] P2.1 C3 — Action log append becomes O(1) tail writes with per-run lock sharding
  - **Context:** `pkg/actionlog/anansi.go:180-214` `Append` loads ALL prior entries, re-marshals the full array, and rewrites the whole document — two DB round trips per append, O(entries²) bytes per run, behind one package mutex shared by ALL runs. Callers: `pkg/pipeline/stage.go:126-141, 326-337, 343-353` (hot path of every step). **Trap:** this commit changes STORAGE ONLY; dropping the duplicate `KindEffectCompleted` write is P3.4 (coupled to replay semantics).
  - **Details:** New storage layout: per-entry rows or chunked tail-append (choose during implementation; per-entry rows under `(runID, rerunIndex, seq)` keying is simplest and makes `All` a range scan). Provide `RunEnumerator` (new optional interface on `actionlog.Store`: `Runs(ctx) ([]RunRef, error)` + `LatestRerunIndex(ctx, runID)`) — needed later by P3.3 startup rebuild; implement for `AnansiStore` and `MemoryActionLog`, `NopLog` returns none. Dual-read: `All` must still decode legacy single-document runs. Lock sharding: replace the store-wide `mu` with per-`(runID, rerunIndex)` mutexes. Batch hint: stage.go appends 1-3 entries per stage; add no batching API yet — measure first. Benchmark: extend `pkg/actionlog` with an append benchmark at 10/100/1000-entry runs (Anansi-backed benchmarks stay behind the Makefile skip).
  - **Files:** `pkg/actionlog/anansi.go`, `pkg/actionlog/actionlog.go` (optional interface), `pkg/actionlog/actionlog_test.go` (legacy-format read test); new `pkg/actionlog/append_bench_test.go`.
  - **Verify:** characterization tests from P0.2 green on BOTH legacy and new formats; append benchmark shows flat (not quadratic) growth.

- [ ] P2.2 H2 + H10-amplification — Guard delta computation; mutators declare touched keys
  - **Context:** `pkg/pipeline/stage.go:311-319` computes the state delta UNCONDITIONALLY, then checks `actLog == nil` at :320 — the work (2× `DeepCopyMap` + 2× full marshal + up to 2×len(state) per-key marshals in `computeStateDelta`/`deepEqual` at :423-469) is discarded whenever there is no action log. `deepEqual` marshals both sides to JSON per key (the JS `JSON.stringify` equality reflex). H10's for-each cost is mostly this delta storm amplified.
  - **Details:** Move the delta block inside `if actLog != nil`. Replace `deepEqual` with `reflect.DeepEqual` (L1 subsumed). Then introduce the real fix: an optional interface `TouchedKeys(ctx) []string` (or `MutatorKeys()`) checked via type assertion — `store.PatchMutator` already knows its keys; return them and compute the delta only over touched keys. Mutators without the interface fall back to whole-state diff (compat preserved — `store.Mutator` is an existing type; adding an optional interface breaks nobody). This one change removes H2's cost AND H10's amplification.
  - **Files:** `pkg/pipeline/stage.go` (:300-470), `pkg/store/store.go` (PatchMutator / optional interface), `pkg/nodes/for-each/foreach.go` (adopt TouchedKeys).
  - **Verify:** new stage benchmark with 1 MB state + NopLog shows delta cost ≈ 0; action-log-enabled run produces byte-identical log entries vs pre-change (characterization test).

- [ ] P2.3 H6 — Split the global runtime lock; index paused runs; move I/O out of critical sections
  - **Context:** One `rt.mu sync.Mutex` guards 9+ maps. Dispatch scans all `rt.paused` under the lock per event (`runtime.go:751-799`); `ListRuns`/`GetRunMeta` (:428-441, :445-458) hold the exclusive lock across per-run action-log DB queries, stalling dispatch/Resume/Register process-wide.
  - **Details:** Convert `rt.mu` to `sync.RWMutex` and split by concern where contention warrants (workflows/index = read-mostly → RLock in dispatch; paused/outcomes/runMetas keep write path under Lock). Build `pausedByEvent map[string][]*pausedRun` index maintained on pause/resume/cancel; dispatch consults the index (O(matching) instead of O(paused × waitEvents)) — the `PauseInstruction` `waitForEvent` match rule must be applied identically to today (pin with a test on wildcard/exact matching before switching). `ListRuns`/`GetRunMeta`: snapshot run IDs under lock, do log queries outside. Keep `rt.mu` as the single lock for the simple maps — do not over-shard in one commit.
  - **Files:** `pkg/runtime/runtime.go` (struct defs ~:180-330, dispatch :751-799, ListRuns/GetRunMeta :428-458); tests `pkg/runtime/runtime_test.go` (dispatch matching characterization).
  - **Verify:** race detector under `make bench-race`; dispatch benchmark if present stays flat or improves; existing concurrency tests green.

- [ ] P2.4 H7 — Persistent store `Sync` flushes off the data lock
  - **Context:** `pkg/store/persistent.go` `Sync` holds the exclusive write lock across the DB write; called at every stage boundary from `pkg/pipeline/context.go:532, 597, 662, 845`. Readers of the run's state queue behind DB/network latency exactly when they are busiest.
  - **Details:** Serial flush per run: snapshot the state under lock, mark `dirty=false`, release, write outside the lock (last-write-wins). A `sync.Once`-per-boundary or a single-flusher-goroutine per PersistentStore both work; choose the simplest that keeps `Sync` synchronous-in-contract (callers may rely on durability at return — check context.go call sites: pause/checkpoint paths DO want durability-before-return; stage-boundary syncs can tolerate eventual flush). If the distinction matters, add `SyncBlocking` vs async `Flush` semantics rather than changing pause durability guarantees.
  - **Files:** `pkg/store/persistent.go`, `pkg/pipeline/context.go` (call sites only if semantics split).
  - **Verify:** `pkg/store/persistent_test.go` extended: concurrent reads during flush don't block; pause-persist test (`pkg/pipeline/pause_persist_test.go`) still proves durability at pause.

- [ ] P2.5 H4 — Compile node config once at Step/Router build time
  - **Context:** `pkg/nodekit/build.go:17-18` runs `prepareNodeConfig` → `CompileConfigSchema`, `pkg/nodekit/config.go:244` `Interpolate`, and reflection binding (`typed.go`) on EVERY step attempt and router eval — JSON parse + schema compile + regex substitution + struct bind per attempt, for what is usually a static config.
  - **Details:** Precompute at `BuildStep`/`BuildRouter` time: compiled schema + static-resolved config. Per-run work shrinks to env/secret interpolation ONLY when the config actually references dynamic sources (detect at compile time by scanning for `${...}`/env markers; static configs skip interpolation entirely). Keep `prepareNodeConfig` behavior identical for dynamic configs (characterization: same coerced `*C` output for a config matrix before/after).
  - **Files:** `pkg/nodekit/build.go`, `pkg/nodekit/config.go`, `pkg/nodekit/typed.go`; tests `pkg/nodekit/typed_test.go`, `pkg/compiler/compiler_test.go` (end-to-end equality).
  - **Verify:** stage benchmark (from P2.2) shows config cost removed from the per-attempt path.

- [ ] P2.6 H3 + M4 — Pool goja VMs; fast-path simple predicates; parse switch cases at Define time
  - **Context:** `pkg/expr/expr.go:151-164` constructs a fresh `goja.Runtime` + interrupt goroutine per evaluation and re-runs ~30 validation regexes (:56-118); invoked per condition from `pkg/nodes/if/if.go:76-117`, `while.go:70,76`, switch router. `pkg/nodes/switch/switch.go` additionally `json.Unmarshal`s `cfg.Cases` on every routing call.
  - **Details:** Pool `goja.Runtime` (per-`expr.Eval` call site or sync.Pool; interrupt goroutine stays — one per pooled VM lifecycle, not per eval). Cache validation results: validation is a pure function of the expression string — memoize on first compile. Direct interpretation for simple binary forms (`a <op> b` over state paths) that never enter the VM. Switch: parse cases once into a typed slice at config coercion (`Define`/`prepareNodeConfig` output), so routing evaluates pre-parsed values. Pin behavior with `pkg/expr/expr_test.go` + `pkg/nodes/switch/switch_test.go` — eval results must be byte-identical (JS semantics quirks included).
  - **Files:** `pkg/expr/expr.go`, `pkg/nodes/switch/switch.go`, `pkg/nodes/if/if.go`, `pkg/nodes/while/while.go` (call-site only).
  - **Verify:** expression eval benchmark (add: pool vs fresh) + `go test -race ./pkg/nodes/... ./pkg/expr/...`.

- [ ] P2.7 H1 + M2 — One snapshot per stage; inline single-step stages
  - **Context:** `pkg/pipeline/stage.go:192-196` deep-copies the FULL state under the store RLock for EVERY attempt of EVERY step (32 MB HTTP bodies memcpy'd per attempt — `pkg/nodes/http/http.go:22,257`). Steps already commit atomically at stage end (:381-393), so per-attempt full copies over-serialize. M2: goroutine + WaitGroup + recover closure per step even for single-step stages (:95-113, :362).
  - **Details:** Snapshot once per stage execution (before the step fan-out), hand each attempt the same immutable snapshot; mutators still apply atomically at commit — semantics unchanged because attempts never see each other's mutations today either (they read the same pre-stage state; verify this claim against `stage.go` retry loop and pin it in the P0.2 characterization suite first). Inline the synchronous path when `len(stage.Steps) <= 1` (keep the goroutine path for >1). Hoist context/closure creation out of the retry loop (:164-165). **This task needs the P0.1 baseline + `benchstat` table** (20-stage/100-step graph, 1 MB state, per hot-path audit §5).
  - **Files:** `pkg/pipeline/stage.go` (:95-208, :380-393); tests `pkg/pipeline/stage_snapshot_test.go` (extend: concurrent steps still isolated).
  - **Verify:** benchstat: allocations/op and ns/op on the stage benchmark; race detector; snapshot-isolation test green.

- [ ] P2.8 M1 + M3 + M6 + M7 — Copy-once export, cheaper emits, bounded outcomes, single Update
  - **Context:** M1: ≥3 full-state exports per successful run (`context.go:578,603`, `runtime.go:1550`) and the full state ships inside the success event payload. M3: every `Emit` allocates handler slices + re-walks the scope chain (`events.go:146-158,184-187`). M6: `rt.outcomes[runID]` retains full FinalState until TTL (default forever). M7: seeding/resume does one `st.Update` per payload key (`runtime.go:1499-1505`, `:989-1005`).
  - **Details:** M1: export once on success, reuse for FinalState + event payload — check consumers of `pipeline:success` payload first (timeline/UI); if the payload can be a summary + reference, shrink it, otherwise keep payload but stop the duplicate copies. M3: publish handler slices via `atomic.Pointer` on mutation; `Emit` reads the pointer and flattens scope-chain handlers into one slice at registration time per scope, not per emit. M6: store a summary + load FinalState on demand from the log/store when `RunResult` needs it; or bound with the existing janitor TTL — choose on-demand (TTL=0 default stays non-leaking). M7: single `st.Update` writing all keys per payload.
  - **Files:** `pkg/pipeline/context.go` (:570-610, FinalState copy sites), `pkg/runtime/runtime.go` (:1550, :1499-1505, :989-1005, outcomes struct ~:422), `pkg/events/events.go`.
  - **Verify:** benchstat on the structural parts; `pkg/events/events_test.go` parity; success-path benchmark.

- [ ] P2.9 H8 — Replayer: index once, iterate in order
  - **Context:** `pkg/replay/replayer.go` does full-log reads plus linear scans (`checkpointSeq`, `resumedAfterCheckpoint`, `indexByStepID`, `indexByStage`) — O(stages × entries) per replay — and `walkDef` ranges over `stage.Steps` (Go map), making step order nondeterministic across runs (reproducibility hazard; JS objects guaranteed insertion order).
  - **Details:** Build `map[stepID]Entry` and stage indexes ONCE on load (they already exist as functions — memoize). Replace `walkDef` map iteration with the compiled definition's ordered step slice (compiler already produces deterministic structures — verify `PipelineDefinition` stage/step ordering is slice-based; if any remaining map iteration defines observable order, sort deterministically and pin with the golden vectors from P0.3). **benchstat against `pkg/replay/rebuild_bench_test.go` required.**
  - **Files:** `pkg/replay/replayer.go`.
  - **Verify:** golden vectors + characterization green; benchstat on rebuild benchmark (10/100/1000 entries × 1 and 10 attempts).

---

## Phase 3 — Architecture: durability & observability (architecture audit §Verdict 1–2, §A–§F)

- [ ] P3.1 Delete dead recovery paths 3, 4, 6 (§A rows 3, 4, 6)
  - **Context:** Three of the six recovery paths participate in nothing: `Options.StoreLoader`/`newStoreForID` (zero callers, `runtime.go:1673-1687`); the durable-shaped Pebble event bus (`pkg/events/durable.go` — `Underlying()` never called, `Subscribe` never reads it, and its "durable" store is actually an in-memory map); `registry.FastPathResume`/`MarkPaused` (never called by `WorkflowRuntime`, README falsely advertises "two-tier resumption").
  - **Details:** API-compat handling: (a) `Options.StoreLoader` — keep the field, mark `// Deprecated:` (ignored, logged once if set), delete `newStoreForID` and the `rt.storeLoader` wiring (internal, free); `pkg/store/anansi.go:96` `Loader` adapter gets the same deprecation. (b) `events/durable.go` — deprecate `NewDurableEventBus`/`WithDurableBackend`/`DurableBusConfig` (keep constructors returning the plain bus), delete internal `memoryStore` if unreferenced. (c) `pkg/registry/registry.go` — deprecate `MarkPaused`/`FastPathResume`/`ActiveRun` timer machinery; keep compiling. Update README to stop claiming "two-tier resumption" (full doc pass is P5.1 but the false claim goes here so the code and docs never disagree within the branch).
  - **Files:** `pkg/runtime/runtime.go`, `pkg/store/anansi.go`, `pkg/events/durable.go`, `pkg/events/events.go`, `pkg/registry/registry.go`, `README.md` (claim fix only).
  - **Verify:** `go vet ./...` (deprecation comments well-formed); full suite green; grep proves no internal callers remain.

- [ ] P3.2 One recovery path: the Replayer (§A rows 1–2)
  - **Context:** `runtime.go` (~:1051-1083) selects recovery by type-asserting the action log implementation (`_, isNop := rt.actionLog.(actionlog.NopLog)`): Replayer when durable, checkpoint-fallback when Nop — and the fallback silently degrades guarantees on ANY replayer error. Checkpoint resume (`pkg/pipeline/factory.go:169-183`) is a second mechanism for the same job.
  - **Details:** Decide capability by DATA, not type assertion: attempt event-sourced rebuild whenever the log returns any entries for the run; NopLog (returns none) naturally falls through to the checkpoint path — but only for runs that have a checkpoint, and the fallback is now LOGGED (never silent) and counted. Remove the type assertion entirely; `actionlog.Store` gains the `RunEnumerator` from P2.1 as the capability probe. `KindCheckpoint` entries stay IN the log and fold during replay (checkpoint becomes a replay optimization, not a parallel recovery system). Document the decision in `pkg/runtime/runtime.go` package docs.
  - **Files:** `pkg/runtime/runtime.go` (Resume/rebuild dispatch ~:1051-1083), `pkg/pipeline/factory.go` (checkpoint path comment/behavior), `pkg/actionlog/actionlog.go`.
  - **Verify:** characterization tests: run with MemoryActionLog → rebuild from log; run with NopLog + checkpoint → legacy fallback; mixed failure (log throws mid-rebuild) → logged fallback + metric/log line.

- [ ] P3.3 Restart durability: durable run index + startup rebuild (§A — the flagship fix)
  - **Context:** Every recovery path funnels through `rt.paused[runID]` — a RAM-only map never repopulated from disk. The system pays for six recovery designs and delivers zero restart durability. The audit's verdict: durable log + repopulated run index + one rebuild procedure.
  - **Details:** Startup recovery in `NewWorkflowRuntime` (or explicit `rt.Recover(ctx)` called by it when the action log is durable): (1) enumerate runs via `RunEnumerator` (P2.1) — `(runID, rerunIndex)` refs; (2) for each, read the run's terminal state from the log: last entry `KindPipelineCompleted`/`KindPipelineFailed` → record meta as terminal (rebuild `runMetas` lazily, not eagerly — see P3.4); last entry otherwise (started/staged/checkpointed) → non-terminal; (3) for non-terminal runs, rebuild state via the Replayer (P3.2 path) and repopulate the paused registry (`rt.paused` equivalent) from the folded `KindCheckpoint` entry; (4) re-arm: cron auto-resume timers from `Checkpoint.Cron`, watch-service registrations from checkpoint wait conditions, event-source subscriptions as needed. Costs bounded: enumeration is a key scan; full rebuild deferred per-run to first Resume (lazy) EXCEPT re-arming data, which must be eager. Acceptance test: build runtime with MemoryActionLog + persistent-backed store factory, run a workflow that pauses mid-graph, create a SECOND runtime instance on the same log, resume the run there, and drive it to the same terminal state as an uninterrupted run. Also cover: in-flight (never-paused) run recovered from log; rerun-index continuation (`LatestRerunIndex` feeds `nextRerunIndex`); janitor eviction interplay (terminal runs must NOT resurrect).
  - **Files:** `pkg/runtime/runtime.go` (new recovery.go section or `pkg/runtime/recovery.go`), `pkg/actionlog` (RunEnumerator from P2.1), new `tests/restart_durability_test.go`.
  - **Verify:** the acceptance test above under `-race`; docs updated (P5.1).

- [ ] P3.4 Observability: log is the source of truth; drop duplicate writes (§B, §C)
  - **Context:** One run-status fact is recorded 4× (log entries + `runMetas` + `outcomes` + `RunResult`); every lifecycle site does `bus.Emit` AND `actionlog.Append` (15 sites); `KindEffectCompleted` re-writes `KindStepCompleted`'s delta byte-identically (`stage.go:343-353`) and is consumed by neither the Replayer's step fold (it keys effect entries separately, `replayer.go:669-675`) nor the timeline; `rt.rerunIdx` duplicates `actionLog.LatestRerunIndex` and diverges after janitor eviction.
  - **Details:** (1) Stop writing `KindEffectCompleted` on effectful step success; `KindStepCompleted` (with its `Effect` flag) becomes the only completion record — **update the Replayer fold** (effectful steps fall back to `KindStepCompleted` entries) and the P0.3 golden vectors in the SAME commit; keep the `KindEffectCompleted` constant exported (compat: log readers may see old entries). (2) Derive `runMetas` at startup from terminal log entries (P3.3 does the recovery half; this task makes the runtime write path single-source: `setRunStatus` stays as the RAM cache write-through but is no longer authoritative — reconcile-on-read from log on miss). (3) Delete `rt.rerunIdx`; derive from `RunEnumerator.LatestRerunIndex`. (4) Timeline stays a pure projection (already correct) — verify it never reads `KindEffectCompleted` before removing the write.
  - **Files:** `pkg/pipeline/stage.go` (:343-353), `pkg/replay/replayer.go` (fold update), `pkg/timeline/timeline.go` (verify-only), `pkg/runtime/runtime.go` (rerunIdx, setRunStatus).
  - **Verify:** golden vectors updated + green; timeline tests green; log-size benchmark shows per-run entry count drop.

- [ ] P3.5 Canonical replay semantics (Go-first) (§D)
  - **Context:** Go `pkg/replay` and TS `src/replay.ts:67-135` disagree: Go keeps the FIRST effect entry per step id and the LAST route per stage; TS folds EVERY output in seq order. For looping pipelines with effectful steps they produce different state.
  - **Details:** Choose canonical semantics = **seq-order folding** (matches TS, is the natural event-sourcing semantic, and makes first-vs-last moot once P3.4 stops double-writing effect deltas). Update the Go Replayer fold; encode both old and new behavior in golden vectors with an explicit `semantics: "seq-fold"` field so the change is reviewable. TS side: frozen — update `src/replay.ts` comment block to state the npm package now matches Go semantics via the shared vectors (behavior already matches; only the note changes). Document the chosen semantics in `pkg/replay/replayer.go` package docs.
  - **Files:** `pkg/replay/replayer.go` (fold), `pkg/replay/testdata/golden/*`, `src/replay.ts` (comment only).
  - **Verify:** all golden vectors green; characterization tests updated to the canonical fold; subpipeline replay tests (`pkg/replay/replay_subpipeline_test.go`) green.

- [ ] P3.6 Canonical representations (§F)
  - **Context:** Every core concept exists in 3–7 hand-synced forms: Pause (7 forms), routing decision (6 forms), run ID (7 carriers), effect flag (3), node definition styles (2), plus two unrelated `PipelineRegistry` types with the same name.
  - **Details:** Pick canonical + derive at the boundary, API-compat rules applied: (1) **Effect flag** — move the constants into `pkg/pipeline` as the canonical enum; `pkg/effect` becomes two alias constants (deprecated) — kills the import-cycle workaround package (OA-low finding) once the cycle that motivated it is confirmed gone. `Step.Effect int` accepts both (same values). (2) **Pause** — `PauseInstruction` canonical; `PipelineCheckpoint` remains the serialized boundary form (it is the wire/log format) but is PRODUCED only by one serializer function and consumed by one parser (locate the scattered construction sites and centralize; grep `__pipeline_data__` / checkpoint construction). (3) **Routing** — keep exported `RoutingInstruction` interface (public API), but internal evaluation produces one tagged struct consumed by `evaluateInstruction` (the type switch already exists, `context.go:705-994`); `serializeInstruction`/`interpretationOf` collapse into the tagged struct's methods. (4) **Run ID** — `pipeline.RunIDFromContext(ctx)` is the accessor; new code stops adding carriers (no mechanical refactor of all 7 — diminishing returns; note them as deprecated carriers instead). (5) **Registry naming** — rename the runtime-internal `registry.PipelineRegistry` usage sites to make the distinction explicit (rename is internal; if the type is exported, keep the name, fix the docs).
  - **Files:** `pkg/pipeline/types.go`, `pkg/pipeline/context.go` (:705-994), `pkg/pipeline/checkpoint.go`, `pkg/effect/effect.go`, `pkg/nodekit/effect.go`, `pkg/registry/registry.go`.
  - **Verify:** full suite; wire round-trip tests (`pkg/compiler/wire_test.go`) prove boundary forms unchanged.

- [ ] P3.7 Execution-chain slimming (§G) — routing fourfold default + compiler triple BFS
  - **Context:** Default/advance routing is implemented four times (`compiler.go:413-419`, `compiler.go:563-574`, `pkg/nodekit/build.go:194`, `pkg/pipeline/router.go:11`); the compiler traverses the graph three times (outer BFS, body BFS, fork-branch BFS) with near-identical logic plus a fourth fork-membership computation.
  - **Details:** Extract one `defaultTargetFor(node)` helper used by all four routing sites (behavior-preserving; each site's nuances — terminal leaves, pause — become parameters, pin with `pkg/compiler/wire_test.go` graphs). Compiler BFS: factor ONE `walkGraph(seeds, enterNested func)` used three times; fork-membership folds into the same walk as a collector. This is a pure refactor — no wire or execution behavior change — keep it late in the phase because compiler regressions are subtle (samples_test.go is the safety net).
  - **Files:** `pkg/compiler/compiler.go`, `pkg/nodekit/build.go`, `pkg/pipeline/router.go`.
  - **Verify:** `go test ./pkg/compiler/... ./tests/...` including `samples_test.go` and `fork_while_workflow_test.go`.

- [ ] P3.8 Resolve `state["results"]` (§E row)
  - **Context:** `state["results"]` is read in three places (`pkg/nodekit/build.go:83,123`, `pkg/nodekit/nodekit.go:255`) to feed `NodeRunContext.Results`, and written NOWHERE in `pkg/` — node authors' `Results` is always nil unless a host seeds it manually.
  - **Details:** Decision: **deprecate, do not remove.** `NodeRunContext.Results` is part of the node-authoring API (Go + the frozen `.ts` catalog's handle signatures may reference `results`). Mark the field `// Deprecated: always empty unless the host seeds state["results"] manually`, keep the reads (harmless), and document in the node-authoring docs. If a future for-each/join node wants to publish results, that is a feature — not this refactor.
  - **Files:** `pkg/nodekit/nodekit.go` (NodeRunContext), `pkg/nodekit/build.go` (comments), README node-authoring section.
  - **Verify:** suite green; no behavior change.

---

## Phase 4 — Abstraction cleanup (over-abstraction audit; API-preserving)

- [ ] P4.1 Interface boundaries go concrete internally
  - **Context:** 11 single-implementation interfaces (OA high-impact table). The exported interface TYPES stay (public API, Options fields are typed with them); the internal plumbing stops paying interface tax and Nop types become nil checks.
  - **Details:** Pattern per interface: internal struct fields and unexported function params take the concrete type; only the exported `Options`/constructor boundary keeps the interface. Specifically: `store.Store` → internal `*MemoryStore` (PersistentStore embeds it already); `events.ScopedEventBus` → `*MemoryScopedBus`; `pipeline.PipelineContext`/`RunContext` → concrete structs; `actionlog.Store` → keep interface ONLY at `Options.ActionLog` (three impls incl. tests legitimately need it — this one is a real interface, note the audit's row accordingly); `core.Logger` → `*zap.Logger` + `zap.NewNop()` internally, `NopLogger`/`ZapLogger` adapters deprecated; `scheduler.Scheduler` stays an interface at Options (fake-ability is real) but runtime internals stop re-wrapping; `watch.WatchService` → concrete; `runtime.EventSource` → interface at Options only; `runtime.SecretProvider` (zero impls) → replace with `func(context.Context, string) (any, bool)` type alias, deprecated interface name kept as alias; `pipeline.PipelineRegistry` → function type at boundary. For each deprecation, add `// Deprecated:` docs pointing at the replacement. EXCEPTION logged: `actionlog.Store` and `scheduler.Scheduler`/`runtime.EventSource` have genuine multi-impl/test-fake value — record a `[=]` skipped note in this file when the plan is executed, so the audit's table is answered item-by-item.
  - **Files:** `pkg/store/store.go`, `pkg/events/events.go`, `pkg/pipeline/types.go`, `pkg/actionlog/actionlog.go`, `pkg/core/logger.go`, `pkg/core/zap_adapter.go`, `pkg/scheduler/scheduler.go`, `pkg/watch/watch.go`, `pkg/runtime/eventsource.go`, `pkg/runtime/runtime.go`, `pkg/pipeline/workflow.go`.
  - **Verify:** full suite + `go vet`; public API diff reviewed (only additions of concrete-typed constructors, no signature changes).

- [ ] P4.2 Wrapper and factory deletion (OA medium-impact)
  - **Context:** Factories wrapping one call (`store.NewFreshStore`, `actionlog.AnansiStoreFactory`, `store.AnansiStoreFactory`, `events.NewDurableEventBus`/`WithDurableBackend` — the latter already deprecated in P3.1), adapters delegating to function fields (`HestiaEventSourceFunc`, `HestiaSchedulerFunc`, `core.ZapLogger`), option functions that set one field (`WithRunEnv` etc.), no-op null objects (`NopLog`, `NopLogger`), trivial wrappers (`RunHandle`, `store.SetValue`, `store.DeepCopyMap`).
  - **Details:** Exported one-liners: deprecate + keep (compat). Internal plumbing: delete outright once P4.1 makes internals concrete — `RunHandle` collapses into `*RunContextImpl`, internal Nop usage becomes nil checks (callers of `rt.actionLog` check `nil` OR keep a package-level `nopLog` var — nil-check at the two guard sites is cleaner). `store.DeepCopyMap` stays exported (it has real callers in `pkg/pipeline/stage.go` — it is API by use) but stops being a wrapper: export `deepCopyMap`'s body directly. `pipeline.NewFactoryFromModel` (dead code, discards its computation): delete if unexported-only callers, else deprecate.
  - **Files:** as listed in the OA audit's medium-impact section.
  - **Verify:** suite green; grep for each deleted symbol returns only deprecation shims.

- [ ] P4.3 Dead weight (OA low-impact + architecture §E remainder)
  - **Context:** Remaining §E rows not covered by P3.1/P3.8: `store.Flush` (no-op, no callers), `PipelineFactory.schema`/`PipelineDefinition.Schema` (written never read, `factory.go:60`, `compiler.go:720`), `Stage.Order` (assigned, asserted in tests, never read by Run), `placeholder` edge role (accepted, never consumed), trivial compiler helpers (`flowEdges`, `dependencyEdgesTo`, `nextDefaultTarget`, `appendRequirement`, `staticResources`, `buildResourcesFor`), `timeline.TimelineEventSource` (4 constants, 1 used), SPEC-promised-but-absent (`TimelineStore.Append`/snapshots never populated — `SPEC.md:309-322`; `pkg/server` "Phase 7" — `SPEC.md:345`, `runtime.go:1352`).
  - **Details:** `store.Flush`: keep no-op method, `// Deprecated:`. `.Schema` fields: stop writing, deprecate. `Stage.Order`: stop assigning; update the tests that assert it; deprecate. `placeholder`: document accepted-and-ignored (wire compat). Trivial helpers: inline ONLY where the call site gets clearer — judgment per helper, no mechanical inlining. Timeline constants: leave (trivial, public). SPEC.md: mark snapshot/server sections as "not implemented in Go runtime" (P5.1 does the doc pass; this task lists them so §E is fully answered).
  - **Files:** `pkg/store/store.go`, `pkg/pipeline/factory.go`, `pkg/pipeline/types.go`, `pkg/compiler/compiler.go`, `pkg/compiler/wire.go`, `SPEC.md` (notes), node tests asserting `Stage.Order`.
  - **Verify:** suite green; §E table gets a disposition column in the architecture audit file (edit `todo/20261003-architecture-audit.md` adding `→ P3.x/P4.x disposition` notes).

- [ ] P4.4 Fix `typed.go` generic field shadowing
  - **Context:** OA audit: `TypedRunContext[C]`'s generic wrapper causes field shadowing during construction; the generic parameter erases immediately after.
  - **Details:** Flatten construction so `Define[C]` builds `NodeDefinition` directly without the intermediate shadowing wrapper; `TypedDefinition[C]`/`Define[C]`/`TypeOf[C]` are public authoring API — keep signatures, fix internals. `pkg/nodekit/typed_test.go` + `handles_parity_test.go` pin behavior.
  - **Files:** `pkg/nodekit/typed.go`.
  - **Verify:** nodekit tests green.

---

## Phase 5 — Docs, deprecations, release

- [ ] P5.1 Documentation truth pass
  - **Context:** README currently documents mechanisms that do not exist ("two-tier resumption" — fixed in P3.1; restart recovery — new in P3.3); SPEC promises snapshots and a server phase that were never built (`SPEC.md:309-322`, `SPEC.md:345`).
  - **Details:** README: rewrite the pause/resume section around the single recovery path + startup rebuild; add an "Operational limits" note (NopLog = no restart durability; MemoryActionLog = process-lifetime durability; AnansiStore = restart durability). SPEC: mark unimplemented sections explicitly. Add a RECOVERY.md (or README section) documenting: the durable contract, the log kinds that matter for recovery, the RunEnumerator contract for backends.
  - **Files:** `README.md`, `SPEC.md`, optional `docs/RECOVERY.md`.
  - **Verify:** doc claims match code behavior — walk each claim to its file:line once.

- [ ] P5.2 Deprecation ledger + CHANGELOG
  - **Context:** Constraint: API preserved, removals deferred to v2. Every deprecated symbol from P3.1/P3.4/P3.6/P4.x must be listed in one place.
  - **Details:** Add `CHANGELOG.md` entries per phase (user-visible: restart durability, action log storage layout + legacy read support, deprecations list with replacements). Maintain a "v2 removal candidates" section. Note the replay semantics decision (seq-fold) and its golden vectors as the compatibility reference for downstream log consumers.
  - **Files:** `CHANGELOG.md`.
  - **Verify:** every `// Deprecated:` comment in the diff appears in the ledger (scripted grep check).

- [ ] P5.3 Final gate + version decision
  - **Context:** One branch ships together; the gate must prove the whole plan, phase by phase.
  - **Details:** Run: `go vet ./...`, `go test -race ./...`, `make bench`, `make bench-race`, `benchstat` summary vs P0.1 baseline (attach table to the PR/branch description). Confirm the restart-durability acceptance test runs in CI (`tests/restart_durability_test.go` is in the default suite). Version: recommend `v1.3.0` (minor: new durability capability + deprecations, no removals). Bun/TS package untouched except comments — no npm release needed.
  - **Files:** branch `refactor/20261003-audits` final state.
  - **Verify:** all green; branch ready to merge as one unit.

---

## Commit sequence (branch `refactor/20261003-audits`)

| # | Commit | Tasks | Gate |
|---|---|---|---|
| 1 | `test: characterization suite, golden replay vectors, bench baselines` | P0.1–P0.3 | race + bench baseline |
| 2 | `fix(replay): run step actions outside the store read lock` | P1.1 (C1) | race |
| 3 | `fix(runtime): bound Register critical section; fix unsubscribe + wfResources leaks` | P1.2 (C2) | race |
| 4 | `fix(runtime): cancel cron auto-resume on terminal outcomes; deterministic watch timer` | P1.3 (H9, L2) | race |
| 5 | `fix(pipeline): release store locks before cross-store copy and checkpoint parse` | P1.4 (M5) | race |
| 6 | `fix(runtime): serialize watch-service resume; cron reads final pause state` | P1.5 (H5, L3) | race + repause bench |
| 7 | `perf(actionlog): append-only entry storage, per-run lock sharding, legacy dual-read` | P2.1 (C3) | bench |
| 8 | `perf(pipeline): guard delta computation; mutator-declared touched keys` | P2.2 (H2, H10 amp) | bench |
| 9 | `perf(runtime): split runtime lock, index paused runs, unblock ListRuns` | P2.3 (H6) | race + bench |
| 10 | `perf(store): flush persistent state off the data lock` | P2.4 (H7) | race + bench |
| 11 | `perf(nodekit): compile config once; pool goja VMs; pre-parse switch cases` | P2.5–P2.6 (H4, H3, M4) | bench |
| 12 | `perf(pipeline): stage-level snapshot, inline single-step stages` | P2.7 (H1, M2) | benchstat |
| 13 | `perf: copy-once export, atomic emit slices, bounded outcomes, single update` | P2.8 (M1, M3, M6, M7) | benchstat |
| 14 | `perf(replay): index once, deterministic ordered iteration` | P2.9 (H8) | benchstat |
| 15 | `refactor(runtime): single recovery path; deprecate dead recovery paths` | P3.1–P3.2 | race |
| 16 | `feat(runtime): startup rebuild — restart durability from the action log` | P3.3 | race + acceptance test |
| 17 | `refactor: log as source of truth; drop duplicate effect entries; canonical seq-fold replay` | P3.4–P3.5 | golden vectors |
| 18 | `refactor: canonical representations; slim routing/compiler chains` | P3.6–P3.8 | suite + wire tests |
| 19 | `refactor: concrete internals behind preserved API; deprecation shims; dead weight` | P4.1–P4.4 | vet + suite |
| 20 | `docs: recovery semantics, deprecation ledger, changelog` | P5.1–P5.3 | full gate |

Commits 15–17 must land in that order (dead-path deletion → single path → restart durability depends on RunEnumerator from 7 and the recovery dispatch from 15).

## Risk register

| Risk | Likelihood | Mitigation |
|---|---|---|
| Async-resume change re-breaks chained re-pause (history: `f1e5630`) | High | P1.5 ordering tests first; land cron-ordering half alone if needed; `[X]` marker allowed with reasoning |
| `KindEffectCompleted` write-stop loses replay data | High if sequenced wrong | Coupled commit P3.4 with golden vectors updated in the SAME commit; replayer fold updated first in test, then write removed |
| Action log layout breaks old persisted runs | Medium | Dual-read legacy format + characterization tests run on both formats (P2.1) |
| Replay semantics change (first-effect → seq-fold) surprises downstream log consumers | Medium | Golden vectors published; decision documented in CHANGELOG; npm package unchanged (frozen) |
| Register restructure introduces Deregister race (lock released before OnRegister) | Medium | Re-validate + idempotent rollback (P1.2); race detector + lifecycle test |
| API-compat violations sneak into refactors | Medium | vet deprecation comments; P5.2 scripted grep: every deprecated symbol has a shim; public API diff review |
| Stage snapshot change breaks step isolation assumptions | Medium | Pin current semantics in P0.2 BEFORE P2.7; `stage_snapshot_test.go` extension |
| Compiler BFS unification regresses nested-graph compilation | Medium | P3.7 kept late; `samples_test.go` + fork/while workflow tests are the net |
| Benchmarks regress instead of improving on small-state workloads | Low | benchstat per structural commit; revert-by-commit granularity preserved (one branch, sequential commits) |

## Definition of done

1. Every audit finding has a disposition: fixed, deprecated, documented, or `[=]`/`[X]` with reasoning recorded in the respective audit file.
2. `go vet ./...` + `go test -race ./...` green at every commit; `make bench` + `make bench-race` green at phase gates.
3. Restart-durability acceptance test passes: a runtime instance recreated on an existing durable log resumes paused and in-flight runs to their correct terminal state.
4. No exported symbol removed; every deprecation appears in the CHANGELOG ledger with its replacement.
5. README/SPEC describe only mechanisms that exist; the "two-tier resumption" claim is gone.
6. Benchmark evidence attached for every perf commit (baseline file + benchstat tables).
