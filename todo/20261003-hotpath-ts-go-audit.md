# Task: Hot-path audit — TS→Go language-nuance problems in the execution chain

- [x] Audit the full execution chain for patterns that are natural in TypeScript but absurd/expensive/dangerous in Go
  - **Context:** Second phase of the over-abstraction audit (see `20261003-over-abstraction-audit.md`). TS has a single-threaded event loop where closures are free, locks don't exist, and "run a callback while holding state" is invisible. Go has goroutines, non-reentrant mutexes, allocation-heavy closures, and type assertions. The chain traced: event dispatch → spawnRun → executePipeline → stage (concurrent steps → mutators → atomic commit → route) → actionlog/timeline recording → pause/checkpoint → resume/replay → event emission recursion.
  - **Status:** Done — findings below.

## CRITICAL

### C1. Replay executes arbitrary step code under the store read lock
- **File:** `pkg/replay/replayer.go:780-783`
- **Pattern:** `st.Read(func(state){ mutator, actionErr = step.Action(ctx, pCtx, state) })` — the step's Action closure runs while `sync.RWMutex` read lock is held.
- **TS-ism:** JS has no locks; "run the step against live state" was safe.
- **Go cost:** Any step calling `store.Update` self-deadlocks (RLock held, Update wants write lock, RWMutex is not reentrant). Arbitrary user code (goja eval, retries) runs while all writers are blocked. This exact hazard was already fixed for normal execution in `#review-20260910-009` (`pkg/pipeline/stage.go:166-196`) but never applied to the replay path.
- **Fix:** Copy snapshot under `Read`, release, run action outside, apply mutator — reuse `executeStepAttempt` semantics.

### C2. `Register` holds the global runtime mutex across host callbacks and secret-store I/O
- **File:** `pkg/runtime/runtime.go:557-646` (lock at 557, `secrets.Has` at 565→1631, `eventSource.OnRegister` at 624)
- **Pattern:** `rt.mu.Lock(); defer rt.mu.Unlock()` wraps registration including (a) `secrets.Has` — potentially a vault network call, and (b) host `OnRegister`, which receives an `emit` closure that does `rt.bus.Emit` → `dispatch` → `rt.mu.Lock()` at 751.
- **TS-ism:** Single-threaded event loop makes "call out to host while holding state" harmless.
- **Go cost:** Latent self-deadlock if any EventSource calls `emit()` synchronously inside `OnRegister` (the interface doc explicitly invites this). Vault lookups hold the *global* runtime lock, blocking every dispatch/Resume/ListRuns process-wide.
- **Fix:** Under `rt.mu`, copy triggers and insert record; release; then call `secrets.Has` and `eventSource.OnRegister`.

### C3. Action-log append rewrites the entire run's entries array under a process-wide mutex
- **File:** `pkg/actionlog/anansi.go:187-209`; callers `pkg/pipeline/stage.go:126-141, 326-337, 343-353`
- **Pattern:** Every `Append` reads ALL prior entries, marshals the full array, writes the whole document back — two DB round trips per append. Effectful steps append twice (completion + `KindEffectCompleted` with same delta).
- **TS-ism:** JS arrays are cheap to `push`; the original TS actionlog was in-memory.
- **Go cost:** O(entries²) bytes marshaled/written per run, on the critical path of every step, all behind one package-level `mu` shared by ALL runs — cross-run contention scales with total event volume.
- **Fix:** Append-only storage (individual rows or chunked tail-append), batch per stage, drop duplicate effect entry, shard lock per `(runID, rerunIndex)`.

## HIGH

### H1. Full-state deep copy under read lock for every step attempt
- **File:** `pkg/pipeline/stage.go:192-196` (inside retry loop 152-208)
- **Pattern:** `st.Read(func(state){ stateCopy = store.DeepCopyMap(state) })` before EVERY attempt of EVERY step, whether or not the step mutates anything.
- **TS-ism:** In JS the step gets a live reference or shallow clone only where mutation is expected.
- **Go cost:** O(state) allocation per attempt × retries × steps × stages, WHILE HOLDING the store RLock. A 32 MB HTTP body in state (`pkg/nodes/http/http.go:22,257`) is memcpy'd per attempt.
- **Fix:** One snapshot per stage (steps commit atomically at `stage.go:381-393` anyway), or read-only view for no-mutator steps.

### H2. Delta computation marshals whole state twice per mutating step — even with no action log
- **File:** `pkg/pipeline/stage.go:311-319` (unconditional), `423-457` (`computeStateDelta`: 2× `DeepCopyMap`), `459-469` (`deepEqual` = 2× `json.Marshal` per key)
- **Pattern:** Diff-by-`JSON.stringify` equality over entire state — the JS `===` on serialized values reflex.
- **Go cost:** Per mutating step: 2 deep copies + 2 full marshals + up to `2×len(state)` more marshals + 1 delta marshal. Runs even when `actLog == nil` — the guard at line 320 comes AFTER the computation, so the work is discarded.
- **Fix:** Move inside `if actLog != nil`; replace `deepEqual` with `reflect.DeepEqual`; better — have mutators declare touched keys (`PatchMutator` already knows them).

### H3. Goja VM + goroutine + ~30-regex validation per expression evaluation
- **File:** `pkg/expr/expr.go:151-164` (fresh VM + interrupt goroutine), `56-118` (regex scan), invoked from `if.go:76-117`, `while.go:70,76`, switch router
- **Pattern:** Fresh JS interpreter per predicate; ~30 regexes re-run per eval. Never the Node model (evaluate against existing V8 context).
- **Go cost:** Each `if` condition = 1 VM + 1 goroutine + 1 compile + regex scan. 5-condition `if` = 5 VMs per stage. `while` re-pays per iteration. Loop-heavy workflows: interpreter setup dominates.
- **Fix:** Pool `goja.Runtime`; interpret simple `a <op> b` forms directly (no VM); cache validation on compiled config.

### H4. Config schema compiled and interpolated on every step/router execution
- **File:** `pkg/nodekit/build.go:17-18` (`prepareNodeConfig` → `CompileConfigSchema`), `config.go:244` (`Interpolate`), `typed.go` (reflection `BindToTag`)
- **Pattern:** Recompile + re-interpolate + reflection-bind config per attempt instead of once at graph-compile time.
- **TS-ism:** Object spread + template strings are free in JS.
- **Go cost:** JSON parse, schema compile, regex substitution, struct binding on hot path — for every step attempt and router eval, even when config is static (the common case).
- **Fix:** Compile once when `Step` is built; only re-interpolate per-run env refs.

### H5. Watch-service resume is re-entrant on same goroutine with unlock/relock window
- **File:** `pkg/runtime/watchservice.go` (`onEvent` releases `s.mu`, invokes `resumeCallback` → `rt.Resume` → full pipeline → `bus.Emit` → `onEvent` again); related: `runtime.go:895-897` (inline `rt.Resume` from `spawnRun`) vs `:1260` and `:801-804` (async)
- **Pattern:** Callback-driven recursion with manual lock release — the "unlock and call back in" pattern of single-threaded JS dispatch.
- **Go cost:** Unbounded same-goroutine recursion for chained pause/resume (each level adds full `Emit → handler → onEvent → resumeCallback → Resume → Run → Emit` frame chain). Unlock window lets another goroutine mutate `s.registrations` mid-resume. One refactor away from deadlock. Inconsistent: inline `Resume` at 897 runs BEFORE cron scheduling at 901-914, so stale cron can overwrite newer schedule.
- **Fix:** Uniformly async resume; registration mutation behind dedicated serializer goroutine; cron only from final paused state.

### H6. Global `rt.mu` held across DB queries and full paused-run scan on every event
- **File:** `pkg/runtime/runtime.go:751-799` (dispatch scans all `rt.paused` under lock), `428-441` and `445-458` (`ListRuns`/`GetRunMeta` do action-log queries while holding `rt.mu`)
- **Pattern:** One `sync.Mutex` guarding 9+ maps plus I/O inside critical section — direct transliteration of "all state in one place" JS object.
- **Go cost:** Every incoming event serializes through exclusive lock with O(paused × waitEvents) inner loop. ListRuns holds exclusive lock across per-run DB round trips, stalling dispatch/Resume/Register process-wide.
- **Fix:** `sync.RWMutex` for read-mostly maps; index paused runs by `waitForEvent` (`map[string][]*pausedRun`); move action-log queries outside `rt.mu`.

### H7. Persistent store `Sync` performs I/O under exclusive write lock at every stage boundary
- **File:** `pkg/store/persistent.go` (`Sync` → `s.mu.Lock()` across DB write); called from `pkg/pipeline/context.go:532, 597, 662, 845`
- **Pattern:** Persist-on-boundary with mutex held for whole flush — JS async I/O never blocked other readers.
- **Go cost:** Write-lock hold = DB/network latency; all `Read`s of that run's store queue behind it — exactly when readers are most active (stage boundaries).
- **Fix:** Dedicated flush goroutine/queue per run (last-write-wins); release data lock before DB write.

### H8. Replayer is O(stages × entries) and iterates a Go map where JS used insertion order
- **File:** `pkg/replay/replayer.go` — `allEntries` full read, linear scans in `checkpointSeq`/`resumedAfterCheckpoint`/`indexByStepID`/`indexByStage`, `walkDef` ranges over `stage.Steps` (map)
- **TS-ism:** JS object key order is deterministic (insertion order); scanning arrays fine for small N.
- **Go cost:** O(stages × entries) per replay, growing quadratically with history. Map iteration makes replay step ordering nondeterministic across runs — reproducibility hazard.
- **Fix:** Build `map[stepID]Entry` indexes once on load; iterate steps via compiled ordered slice.

### H9. Cron auto-resume timer never cancelled when run completes (LEAK)
- **File:** `pkg/runtime/runtime.go:901-914` (schedule), `1263-1282` (cancel only on re-pause), `1287-1291` (completion cleans watchService but NOT `scheduler.Cancel(runID)`); `Deregister` cancels only `workflowID:triggerID` (679-681)
- **Pattern:** Timer armed on pause, no `Stop()` on terminal path.
- **Go cost:** Every run that paused with `Checkpoint.Cron` and completed leaves a self-re-arming `time.AfterFunc` firing forever; each fire takes global `rt.mu` — permanent timer + lock-churn leak until Shutdown.
- **Fix:** `rt.scheduler.Cancel(runID)` alongside `watchService.OnRunEnded(runID)` in all completion branches.

### H10. For-each is quadratic: per-iteration array copy + full-state patch + delta storm
- **File:** `pkg/nodes/for-each/foreach.go:67` (array copy per iteration), `:90` (`PatchMutator` → `ApplyPatch`/`shallowClone` at `nodekit/config.go:80`), amplified by H2
- **Pattern:** Rebuild whole array + whole state object per iteration — the React "immutably replace state" idiom.
- **Go cost:** Per iteration: O(remaining) copy + O(state) patch clone + 2 deep copies + per-key marshal + actionlog append (C3) → O(n²) in items × state size.
- **Fix:** Accumulate results into one slice, install once (single mutator); skip delta for bookkeeping keys.

## MEDIUM

### M1. Full state exported 2-3× back-to-back on success, shipped inside event payload
- **File:** `pkg/pipeline/context.go:578` (ExportJSON → event payload 584-589), `:603` (second deep copy for FinalState); `runtime.go:1550` (third); router snapshots `context.go:295, 308, 493`; FinalState copies `236, 483, 668, 697, 841, 884`, `subpipeline.go:158`, `runtime.go:1175`
- **Go cost:** ≥3 full-state copies per successful run + megabytes copied through handler chain. Scales with 32 MB blob risk (H4).
- **Fix:** Export once, reuse for FinalState + event payload; or omit from payload.

### M2. Goroutine + closure stack per step even for single-step stages
- **File:** `pkg/pipeline/stage.go:95-100` (goroutine per step), `105-113` (recover closure), `114` (path clone), `164-165` (3 option closures per attempt), `362` (`wg.Wait`)
- **Pattern:** `Promise.all` transliteration — always fan out even when work is synchronous and trivial.
- **Go cost:** For common graph (trigger → 1 step → router): goroutine spawn, WaitGroup, mutex, path clone, 3 closures × attempts, recover closure, 2 Emit slice copies — for work faster than scheduler quantum. Parallelism often illusory while snapshot contention (H1) is real.
- **Fix:** Inline when `len(stage.Steps) <= 1` or all steps are read-only; hoist context creation out of retry loop.

### M3. `Emit` allocates handler slices and re-walks scope chain per event
- **File:** `pkg/events/events.go:149-158` (two `append` under RLock), `184-187` (recursive parent Emit), `:146` (Path.Clone on empty path)
- **Pattern:** Snapshot-the-list-then-call EventEmitter transliteration with per-emit allocation + recursive bubbling.
- **Go cost:** ≥3 emits per step (`stage.go:116, 212, 297`), each allocating 1-2 slices (even with single handler) and recursing to root.
- **Fix:** Publish immutable handler slice via `atomic.Pointer`; flatten bubbling into ancestor slice.

### M4. Switch node re-parses JSON cases on every routing evaluation
- **File:** `pkg/nodes/switch/switch.go` (`json.Unmarshal` of `cfg.Cases` per router call + `expr.EvalValue`)
- **Go cost:** Unmarshal + allocation per stage traversal, on top of H3's fresh VM.
- **Fix:** Parse at config coercion/Define time into typed `SwitchConfig.Cases` slice.

### M5. `copyStoreState` nests `src.Read` → `dst.Update` (lock-order fragility)
- **File:** `pkg/pipeline/factory.go:201-210`; also `factory.go:172-176` (checkpoint JSON round trip inside `Read`)
- **Go cost:** Two stores' locks held simultaneously — safe today only by accident of lock ordering. Any future reverse-order path deadlocks. `ReadCheckpoint` stretches RLock by full JSON round trip.
- **Fix:** Read src → snapshot → release → dst.Update; parse checkpoint outside Read.

### M6. Completed-run outcomes retain full FinalState copies until TTL
- **File:** `pkg/runtime/runtime.go:422` (`rt.outcomes[runID]`)
- **Go cost:** Resident memory = run count × state size (up to 32 MB each). Only bounded if `RunHistoryTTL` configured (default 0 = forever).
- **Fix:** Store summary + load FinalState on demand; or bounded LRU.

### M7. One `st.Update` per payload key when seeding and resuming
- **File:** `pkg/runtime/runtime.go:1499-1505` (seed), `989-1005` (resume fold)
- **TS-ism:** `Object.assign(state, payload)` in a loop was free.
- **Go cost:** One lock acquisition (+ persistent write under H7) per key instead of per payload.
- **Fix:** Single `st.Update` writing all keys in one closure.

## LOW

- **L1:** `deepEqual` via string conversion (`stage.go:459-469`) — `bytes.Equal` avoids conversions (subsumed by H2 fix).
- **L2:** Watch timeout timer assigned to arbitrary registration from map iteration (`watchservice.go` `startTimeoutLocked`) — nondeterministic owner.
- **L3:** Inconsistent sync/async resume (`runtime.go:897` inline vs `:801-804`, `:1260` async) — ordering hazard with H5/stale-cron.

## Verified-good

- Stage-atomic commit: mutators collected then applied once under `Update` (`stage.go:381-393`) — correct model.
- `#review-20260910-009`: normal step execution runs OUTSIDE the lock (pinned by `stage_snapshot_test.go`); gap is replay-only (C1).
- `Emit` releases bus RLock before handlers (`events.go:158→160`).
- No confirmed lock inversion in current code; dispatch releases `rt.mu` (799) before spawning resume goroutines (801).
- `while`/`if` fail closed on eval errors; `while` has `MaxIterations` guard.

## Suggested fix order

1. **C1, C2, H9** — correctness/leak class, small diffs.
2. **C3 + H2** — biggest allocation/IO win; mutator-declared keys kills H2 and H10's amplification.
3. **H6, H7** — lock split + paused index; Sync outside lock.
4. **H4 + H3 + M4** — compile-once (schema, expression, switch cases).
5. **H1, H8, M1-M3** — structural; needs benchmarks first (`benchstat` on 20-stage/100-step run with 1 MB state + 32 MB HTTP body).
