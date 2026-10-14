# Unreleased — 20261003 refactor (branch `refactor/20261003-audits`)

Demolition + foundation rebuild + remediation of the 20261003 audits
(architecture, hot-path, over-abstraction), per
`todo/20261003-refactor-plan.md` v3. Pre-production delete policy: the
library has no production consumers, so removed symbols are deleted
outright — no deprecation scaffolding. This section is the record of what
vanished and why; the `tests/deletion_audit_test.go` gate enforces that
nothing returns silently and that zero `// Deprecated:` stubs exist.

## Added

- **Restart durability (S.3)**: `NewWorkflowRuntime` now runs `Recover`
  when the action log implements `actionlog.RunEnumerator` — terminal
  runs are recorded and never resurrect; paused runs are re-armed from
  the log's `checkpoint` entry (waits, wait mode, timeout, received
  events, cron) when their workflow re-registers; in-flight runs are
  reported as interrupted, never auto-continued. State rebuilds lazily
  on `Resume` through the Replayer. Acceptance-tested end to end
  (`tests/restart_durability_test.go`).
- `actionlog.RunEnumerator` (`Runs`) — the optional log capability that
  opts a backend into restart recovery; implemented by `MemoryActionLog`
  and `AnansiStore`.
- `docs/RECOVERY.md` — the durable contract: the single recovery
  component, the log kinds that matter, the `RunEnumerator` backend
  contract, operational limits, and contributor rules.
- `internal/runtime` boundary (plan R.5) with an in-tree architecture map
  (`internal/runtime/doc.go`), enforced by `tests/layering_test.go`;
  `pkg/runtime` is now a thin public entry of type aliases and
  constructors.
- Characterization suite + golden replay vectors + bench baseline
  (plan Phase 0) — replay semantics and resume ordering are pinned by
  tests before and after every change.

## Changed

- **Action log storage layout (P4.1)**: per-entry rows keyed
  `(runID, rerunIndex, seq)` with O(1) tail-append and sharded seq
  counters, replacing the whole-document rewrite per append
  (O(entries²) bytes per run). **Old readers cannot read new logs** —
  the legacy single-document format and its reader are deleted
  (pre-production, no migration dual-read).
- **Canonical seq-fold replay (S.1)**: the Go Replayer now folds EVERY
  effectful output in sequence order (matching the TS `src/replay.ts`
  reference), replacing the first-effect-per-step semantics. Golden
  vectors re-encoded in the same commit.
- **Run status derives from the log (S.2)**: the log is the
  authoritative record of run-status facts; `runMetas`/`outcomes` are
  write-through caches. `GetRunMeta` reconciles from the log on cache
  miss (tier-2 eviction and shared-log runs stay queryable). The
  per-fact emit-vs-log disposition table lives in
  `internal/runtime/outcomes.go`.
- **Runtime lock behavior (C2/C4/P4.6/P4.7)**: locks never span user
  code — `Register` validates secrets and runs host callbacks outside
  `rt.mu`; the Replayer and cross-store copies run outside store locks;
  log I/O and store flushes run outside the runtime/data locks; `rt.mu`
  is an `RWMutex` with `Emit` lock-free via a copy-on-write handler map.
- **Recovery is ONE component (R.4)**: `internal/runtime/recovery.go`
  decides every recovery path; capability is decided by data (log
  entries), degradation is logged, checkpoints fold inside the same
  component.

## Performance

All measured with benchstat before/after each commit (the phase-0
baseline lives at `todo/20261003-bench-baseline.txt`):

- **Action log append** (P4.1): 7.3ms/5.9ms/22ms → ~190µs at
  10/100/1000-entry runs (36–110×, the O(n²) is gone).
- **State-delta computation** (P4.2): only computed when a log consumes
  it; mutators declare touched keys — 1MB-state stage 2278.7µs → 26.8µs
  (−98.8%).
- **Node config** (P4.3): schema compile + static coercion hoisted to
  build time — static 956.6ns → 164.1ns (−82.9%).
- **Expression evaluation** (P4.4): pooled goja runtimes, memoized
  validation, parse-once switch cases — `EvalValue` −72.7%, `EvalBody`
  −77.2%.
- **Single-step stages** (P4.5): run inline — −40.7% single-step,
  geomean −22.2%.
- **Dispatch & queries** (P4.6): `pausedByEvent` dispatch index; log
  queries out of the runtime lock; read-projections under `RLock`.
- **Store persistence** (P4.7): `Sync` deep-snapshots under the data
  lock, writes serialized per run outside it.
- **Replay indexing** (P4.8): one-pass log index — 1000-entry replay
  −8.3%, 100×10 attempts −31.3%.
- **Copy-once final state + lock-free emit** (P4.9): payload+result
  share one snapshot; `Emit` reads an atomic handler map (−22…−43%).
- **Chained re-pause depth-10** (C5): −10.0% (p=0.002) while fixing the
  ordering.

## Fixed

- **C2 (runtime)**: `Register` no longer holds `rt.mu` across host callbacks — secrets validation, cron scheduling, and `EventSource.OnRegister` run outside the critical section (a host whose `OnRegister` emits synchronously on the runtime bus previously self-deadlocked). The `OnRegister` unsubscribe func is retained and invoked on `Deregister`; failed registrations roll back idempotently under a fresh lock. `Deregister`/`Shutdown` now tear down workflow-scoped resource handles (the audit's leaked-DB-pools row).
- **C1 (replay)**: step actions run outside the store read lock during
  the rebuild walk.
- **C3 (runtime)**: a terminal outcome cancels the cron auto-resume the
  pause armed (H9 leak — every pause-with-cron run that completed leaked
  a permanent self-re-arming timer); watch timers are per-registration.
- **C4 (pipeline)**: store locks are released before cross-store state
  copies and checkpoint parses (snapshot-then-mutate; the old body held
  two store locks nested — safe only by accident of call order).
- **C5 (runtime)**: resume dispatch is serialized and every cron arm is
  gated on the CURRENT pause (chained re-pause ordering — the historical
  f1e5630 revert class); Register no longer wipes per-type event queues
  on re-registration (buffered-during-segment events were lost).

## Removed

- `runtime.Options.StoreLoader` and `WorkflowRuntime.newStoreForID` — dead recovery path: zero callers anywhere in the tree; the only "call site" was its own wiring. Restart recovery returns (plan S.3) as one startup-rebuild procedure off the action log.
- `store.AnansiStoreFactory.Loader()` — adapter for the deleted `StoreLoader`.
- `pkg/events/durable.go` entirely (`NewDurableEventBus`, `NewDurableMemoryScopedBus`, `WithDurableBackend`, `DurableBusConfig`, the in-memory "durable" store) — the bus's store was an in-memory map, `Underlying()` had zero callers, and `Subscribe` never read the backend: a durable shape with no durability.
- `events.MemoryScopedBus.Underlying()` — accessor for the deleted backend, zero callers.
- `core.Logger` interface, `core.NopLogger`, and `core.ZapLogger` adapter — one real implementation (zap) behind a hand-rolled adapter. Callers take `*zap.SugaredLogger` directly; `zap.NewNop().Sugar()` is the no-op.
- `events.ScopedEventBus` interface — exactly one implementation; `*events.MemoryScopedBus` is used directly. `Scope` returns the concrete type.
- `pipeline.PipelineContext` interface and `pipeline.RunContext` interface — each had exactly one implementation. `pipeline.PipelineContext` is now the concrete struct (was the unexported `pipelineContextImpl`); `*pipeline.RunContextImpl` is the run context.
- `pipeline.Rebuilder` interface — implemented only by `*replay.Replayer`, existed to break the pipeline→replay import cycle. Replaced by the `pipeline.RebuildFunc` function type; pass `(*replay.Replayer).Rebuild`.
- `actionlog.NopLog` — the null-object log. A nil `actionlog.Store` now means "logging disabled"; every call site nil-checks. `Options.ActionLog` defaults to nil.
- `runtime.SecretProvider` interface — zero implementations in the tree. Replaced by `Options.SecretGet` / `Options.SecretHas` func fields.
- `nodekit.TypedRunContext` embedding (closed #review-20260827-001) — the embedded `NodeRunContext` + declared `Config *C` shadowed each other; fields are now flat with the untyped map reachable as `RawConfig`. `nodekit.TypeOf` deleted (unused).

Retained (recorded against the plan's R.3 list, per the plan's own multi-impl/genuine-seam criteria):
- `store.Store` — two real implementations (MemoryStore, PersistentStore) plus test fakes; concretizing to `*MemoryStore` is a behavior trap (embedding does not make `*PersistentStore` assignable, and the `Sync` override would be lost through `&ps.MemoryStore` aliasing).
- `watch.WatchService` — cross-layer seam: the implementation lives in pkg/runtime and the pause node (pkg/nodes) resolves it from a resource handle; deleting the interface would force nodes→runtime imports, violating the target layer map (R.6).
- `pipeline.PipelineRegistry` — sits at the Compile boundary and has genuine test fakes (stubRegistry, mapPipelineRegistry).
- `actionlog.Store`, `scheduler.Scheduler`, `runtime.EventSource` — as planned.

- `core.GetLogger` — a pre-existing `// Deprecated:` stub (zero callers); superseded by `core.LoggerFromContext`. The deletion audit (`tests/deletion_audit_test.go`) now enforces zero `// Deprecated:` declarations on the branch.

- `pkg/effect` package entirely (`effect.Pure`, `effect.SideEffecting`) — a 15-LOC import-cycle workaround whose constants were hand-synced with `nodekit.Effect` (§F consistency obligation). The canonical `Effect` enum now lives in `pkg/pipeline` (next to `Step.Effect`); `pkg/nodekit` re-exports aliases at its authoring-SDK boundary. Integer values unchanged.
- `nodekit.NodeRunContext.Results` — on the step/router paths it was fed from `state["results"]`, which nothing in the tree ever wrote (always nil). The pipelines-mode router keeps its live child-results channel (`InterpolationContext.Results`, fed by `resultsByID`) and the `results []pipeline.PipelineRunResult` argument; only the dead field and the three dead reads are gone.

- `store.NewFreshStore` — one-call wrapper over `store.NewMemoryStore`; callers use the constructor directly.
- `store.AnansiStoreFactory` (and its `Create`/`Load`/`Models`/`Mint` methods) — a struct holding one ModelCollection whose methods wrapped the store constructors. Replaced by `store.NewAnansiStateCollection`, which provisions the collection; callers construct `PersistentStore` values directly.
- `actionlog.AnansiStoreFactory` (and its `NewStore` method) — same shape. Replaced by `actionlog.NewAnansiStore`, a single constructor returning the durable store.
- `pipeline.NewFactoryFromModel` — dead generic helper: called `data.StructFieldValues` and discarded the result; passed a nil schema.
- `runtime.HestiaEventSourceFunc` / `scheduler.HestiaSchedulerFunc` — adapter structs with zero callers in the tree.
- `timeline.SourceStore` / `SourceContainer` / `SourceLogger` — timeline source constants with no producer; only `SourcePipeline` is emitted.

Kept (dispositions against the OA audit's medium-impact list): the compiler helpers `flowEdges`/`dependencyEdgesTo`/`nextDefaultTarget`/`appendRequirement`/`staticResources`/`buildResourcesFor` all have live callers and named contracts (the single-caller pair carries dedup/filter logic that reads clearer named); `replay.StepAddress` and the `core` error aliases are zero-cost readability aliases, not indirection layers; `pipeline.WithRunEnv`/`WithSecretLookup`/`WithResourceResolver` option funcs stay at the public constructor boundary.

- `store.Store.Flush` (and both implementations) — no-op in both impls, zero callers in the tree; `Sync` remains the explicit persistence point.
- `pipeline.PipelineDefinition.Schema` and `pipeline.PipelineFactory`'s schema field/parameter — written, never read; the compiler never set the definition field. Node config schemas live in node definitions where they are consumed.
- `pipeline.Stage.Order` — assigned by the compiler/nodekit, asserted by one test, read by nothing at run time (slice position is the real order). `compiler.WorkflowView` now emits the slice index under its existing `"order"` key so the view shape is unchanged.
- `registry.PipelineRegistry.MarkPaused` / `FastPathResume` and the `ActiveRun` pause-expiration timer machinery (`timer`, `onExpiration`, `ExpiresAt`, `PausedAt`, `RegistryOptions`) — nothing called them (the runtime tracks paused runs itself); the README's "two-tier resumption" claim was false and has been removed.

- `actionlog.KindEffectCompleted` / `KindEffectFailed` and the duplicate write on effectful step success (S.1) — the entries re-wrote `KindStepCompleted`/`KindStepFailed` deltas byte-identically and were consumed by neither the Replayer's step fold nor the timeline. Deleted TOGETHER with the fold re-key (one coupled commit, golden vectors swapped) so no replay data is lost.
- The legacy single-document action-log reader (S.1/P4.1) — the per-entry row layout is the only layout; no production logs exist to migrate (Trap #3: no dual-read).
- `runtime`'s RAM `rerunIdx` map (S.2) — duplicated `actionlog.LatestRerunIndex` and diverged after janitor eviction; the rerun index derives from the log and now survives eviction by construction.
- `replay`'s first-effect-per-step fold branch (S.1) — replaced by the canonical seq-order fold shared with the TS reference implementation.

## [1.2.1](https://github.com/asaidimu/hermes/compare/v1.2.0...v1.2.1) (2026-09-18)


### Bug Fixes

* **replay:** rebuild from the run's full multi-attempt history ([284e8b1](https://github.com/asaidimu/hermes/commit/284e8b1c02c3460a8779aded50aa69a0adc0a807)), closes [#review-20260910-006](https://github.com/asaidimu/hermes/issues/review-20260910-006)
* revert async watch-resume callback — broke chained re-pause semantics ([f1e5630](https://github.com/asaidimu/hermes/commit/f1e5630dceed6936eba3b528bd06120bf92488bb)), closes [#review-20260910-005](https://github.com/asaidimu/hermes/issues/review-20260910-005)

# [1.2.0](https://github.com/asaidimu/hermes/compare/v1.1.1...v1.2.0) (2026-09-11)


### Bug Fixes

* **actionlog:** detach runId from document identity; durable multi-run action log ([afbf9c7](https://github.com/asaidimu/hermes/commit/afbf9c7573d0b41f20130ff12988937112d32a64)), closes [#review-20260910-024](https://github.com/asaidimu/hermes/issues/review-20260910-024)
* **engine:** resolve all P2 review findings ([5b94c0a](https://github.com/asaidimu/hermes/commit/5b94c0a542cc4d3daa2521fd7599fd16f2b1905d)), closes [#review-20260910-015](https://github.com/asaidimu/hermes/issues/review-20260910-015) [#review-20260910-003](https://github.com/asaidimu/hermes/issues/review-20260910-003) [#review-20260910-004](https://github.com/asaidimu/hermes/issues/review-20260910-004) [#review-20260910-009](https://github.com/asaidimu/hermes/issues/review-20260910-009) [#review-20260910-011](https://github.com/asaidimu/hermes/issues/review-20260910-011) [#review-20260910-012](https://github.com/asaidimu/hermes/issues/review-20260910-012) [#review-20260910-013](https://github.com/asaidimu/hermes/issues/review-20260910-013) [#review-20260910-016](https://github.com/asaidimu/hermes/issues/review-20260910-016) [#review-20260910-017](https://github.com/asaidimu/hermes/issues/review-20260910-017) [#review-20260910-021](https://github.com/asaidimu/hermes/issues/review-20260910-021) [#review-20260910-010](https://github.com/asaidimu/hermes/issues/review-20260910-010)
* **engine:** resolve P1 review findings and latent resume blockers ([fcc28af](https://github.com/asaidimu/hermes/commit/fcc28afc05fe2f95d5f96ee52d539c8fce44d630))


### Features

* **runtime:** add GetActionLog and improve test lifecycle management ([9a2c0d3](https://github.com/asaidimu/hermes/commit/9a2c0d3c2ef0dd632df87272e4e09378371f23b9))

## [Unreleased]

### Bug Fixes

* **replay:** rebuild from the run's full multi-attempt history — the Replayer now stitches every recorded attempt (rerunIndex 0..latest, in index order, with Seq re-baselined across attempts) instead of reading only the latest attempt's entries; resume advances the rerunIndex per resumed segment (#review-20260910-006), so on the second resume of a multi-pause run the rebuild saw a log starting mid-run, judged the opening stage's effect "unrecorded", resumed at the top of the pipeline, and re-paused on the FIRST pause's already-consumed event — deadlocking every workflow that pauses twice (TestResumeRepauseSingleEvent / TestResumeRepauseMultiEvent) (#review-20260910-025)
* **replay:** replay subpipeline-bearing pipelines faithfully — log entries now carry a PipelineID (schema addition to actionlog.Entry, stamped at every append site) and the Replayer partitions entries per pipeline, replays each child instance against its own definition and store (dynamic distribute children are regenerated via the stage's DynamicPipelines closure, exactly like the runtime), and merges child results with the runtime's resultKey/default-merge semantics; previously the flat step-id index collapsed concurrent subpipeline instances, silently dropping all but the first child's deltas and letting one child's route steer every child (#review-20260910-015)
* **runtime:** the replay DefinitionResolver resolves the definition the id actually names — across the compiled workflow (Workflow.FindPipeline: trigger-keyed pipelines plus subpipeline definitions embedded in stages, recursively) instead of answering every id with the run's root pipeline (#review-20260910-003)
* **runtime:** evict per-run bookkeeping — a terminal run's state store and rerun index are reclaimed by a janitor after a grace horizon (Options.RunHistoryTTL, default 10 minutes), and audit history (runMetas/outcomes) is purged when an explicit TTL is set; previously the maps only ever grew, leaking one full state map per run for the life of the host (#review-20260910-004)
* **pipeline:** run step actions on a deep-copied state snapshot OUTSIDE the store read lock — arbitrary user code (JS, HTTP retries) no longer serializes concurrent steps in a stage behind the lock, and direct mutations of the passed state map can no longer touch the store or race concurrent readers (fatal, unrecoverable concurrent-map-write); the returned mutator remains the only write path (#review-20260910-009)
* **nodes/code:** bind a deep copy of state into the goja sandbox — direct JS mutations (`state.x = …`) are discarded instead of silently writing store state behind the mutator/atomic-commit model and diverging the action log from real state; only the returned patch writes (#review-20260910-011)
* **pipeline:** fail the pause when a Persist checkpoint write fails instead of reporting "paused" with nothing durable recording where to resume; nested-checkpoint and default sub-pipeline merge update failures are now logged instead of silently discarded (#review-20260910-012)
* **scheduler:** validate cron expressions at Schedule time (ValidateCron) and reject invalid ones — a typo'd trigger ("30 * * *", "@evry 5m") previously registered fine and silently fired hourly via CronDelay's fallback (#review-20260910-013)
* **compiler:** reject the experimental query node at compile time — every execution failed, yet the README documented it as production-ready; canvases using it now fail where the graph is authored, not at runtime (#review-20260910-016)
* **ci:** `make test` now runs go vet + go test -race, `make check` adds a gofmt -s gate, and CI runs both — the quality gates the README promises can no longer drift (#review-20260910-021, #review-20260910-010)

### Documentation

* mark the `database`/`query` node pair experimental in the node catalog, definitions, and use cases — the database service node still initializes no handle (#review-20260910-017)

* **replay:** make StateAt deterministic — the projection previously depended on Go's randomized map iteration order, folding in a sibling step's recorded delta only when map order happened to allow it; it now always reconstructs "just before the target step's own delta landed" (steps in a stage run concurrently), and an unrecorded effectful sibling no longer aborts the projection (#review-20260910-024)
* **actionlog:** detach the run id from document identity — log documents now carry a deterministic UUIDv7-shaped id derived from the entry's own (RunID, RerunIndex), so ONE scope-free AnansiStore serves the multi-run runtime like MemoryActionLog; Append routes by entry identity, rejects entries without a RunID, and derives Seq as max(existing)+1; reads go straight to the document id instead of scanning the collection on the runId field; the durable event-sourced recovery path is wired end-to-end in a runtime integration test (#review-20260910-014)
* **pipeline:** recover panics in step actions, step goroutines, and sub-pipeline children so a bad node fails the step/stage instead of the host process (#review-20260910-008)
* **runtime:** track a re-paused run in full after resume — multi-event waits, wait mode, WatchService re-registration, buffered-event drain, and cron re-arm (#review-20260910-001)
* **runtime:** adopt the Replayer's rebuilt store as the canonical store after an event-sourced resume, fixing stale FinalState results and multi-pause state corruption (#review-20260910-002)
* **nodes/pause:** key watch registrations and buffered-event lookups by run id instead of node id, eliminating cross-run collisions and registration leaks (#review-20260910-007)
* **nodekit:** plumb RunID into NodeRunContext and inject built-in runtime resources (watch-service) so nodes and routers can reach them; the pause node previously never received the service and silently never paused
* **runtime/watchservice:** honor mode=all on parked multi-event registrations — resume only when every watched event type has delivered
* **replay:** resolve the "__pause__" wait-marker route through the pause checkpoint's ResumeAt instead of returning the pausing stage, which made every event-sourced resume re-pause forever (#review-20260910-023)
* **runtime:** fall back to the trigger-id Pipelines key when resolving the resume definition, fixing vacuous no-op resumes for hand-built workflows (#review-20260910-022)

## [1.1.1](https://github.com/asaidimu/hermes/compare/v1.1.0...v1.1.1) (2026-09-01)


### Bug Fixes

* fix issues ([5ac2f50](https://github.com/asaidimu/hermes/commit/5ac2f506bc543045fe76bf051a329b823f241a32))

# [1.1.0](https://github.com/asaidimu/hermes/compare/v1.0.0...v1.1.0) (2026-08-27)


### Bug Fixes

* **runtime:** implement fork and join orchestration ([6b68fae](https://github.com/asaidimu/hermes/commit/6b68fae10be6964fc1e6b2c6239da7605762abbc))


### Features

* **compiler:** implement fork-join and distribute node patterns ([ea86e00](https://github.com/asaidimu/hermes/commit/ea86e0058092c184720da4bbd90c7baa59957afd))

# 1.0.0 (2026-08-26)


### Bug Fixes

* **nodes:** add pause node and implement event-based orchestration ([3a5d8e8](https://github.com/asaidimu/hermes/commit/3a5d8e8433a7cfda4d4373e33c0b06175ca31deb))


### Features

* Initial commit ([2430aa3](https://github.com/asaidimu/hermes/commit/2430aa3d0b296d16b9faa4f8231ec1c7025da872))
