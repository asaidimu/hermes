# Task: Refactor plan v2 — demolish, rebuild the foundation, then remediate

- [ ] Execute the refactor in phases 0–6 below, on one branch, committing sequentially.
  - **Context:** Three audits, all dated 20261003 and all verified against HEAD `3957d54`, found: (1) TS→Go hot-path hazards — deadlocks, leaks, O(n²) I/O (`todo/20261003-hotpath-ts-go-audit.md`); (2) system-level complexity with no benefit — six recovery paths, none restart-safe, state duplication ×5, dual observability channels (`todo/20261003-architecture-audit.md`); (3) TS-port over-abstraction — 11 single-implementation interfaces, wrapper layers, dead code (`todo/20261003-over-abstraction-audit.md`). All three audits converge on the same root cause: **the Go runtime is a structural transliteration of the TypeScript codebase** — §H of the architecture audit names it explicitly (Promise.all fan-out, EventEmitter scope-bubbling, one JS-object-equivalent mutex over 10 maps, three simultaneous state copies per step), and the over-abstraction audit traces the interface pyramid to TS's DI habits ("derive the interface at the consumer, keep implementations concrete" — Go does the reverse).
  - **Details:** v1 of this plan (commit `5a3e7a1`) sequenced every audit finding as an in-place patch on the existing structure: nothing was deleted outright, nothing moved, the god files stayed god files, and the audits' own verdicts (collapse recovery to one path; log as source of truth; canonical representations; compile-time config; concrete internals) were deferred behind correctness and performance work that would have been done **twice** — once in the broken shape, once never. v2 supersedes it. The order is now: safety net → **demolition** (delete every dead path) → **foundation rebuild** (one recovery component, canonical representations, concrete internals, enforced layering, god-file splits, `internal/` boundary) → correctness → performance → durability → surface/docs. Every audit fix then lands in its final structural home exactly once. Revision 3 (20261003, user decision): the library has no production consumers — the delete policy replaces deprecation everywhere; the only remaining contracts are the canvas wire JSON, the frozen `.ts` catalog, and the golden replay vectors.
  - **Files:** Repo-wide; per-task file lists below. Measured at HEAD: 14,113 non-test LOC across 16 top-level `pkg/` packages + 17 `pkg/nodes/*`; god files `pkg/runtime/runtime.go` (1,913), `pkg/pipeline/context.go` (1,034), `pkg/compiler/compiler.go` (883), `pkg/replay/replayer.go` (807); 14 interface declarations of which 11 have exactly one implementation; `pkg/effect` is a 15-LOC package that exists only to break an import cycle; `pkg/registry` (192 LOC) is a disconnected subsystem whose resume machinery nothing calls.

## Why v2 supersedes v1 (revision rationale)

1. **The audits indict the shape, not just the bugs.** Architecture verdict §1–4 and §H prescribe *structural* remedies (collapse to one recovery path, single source of truth, canonical representations, concrete types). v1 kept every remedy as "deprecate in place, remove in v2" — i.e., never.
2. **Correctness fixes done first, on the wrong structure, get done twice.** C1 (step code under the store lock) and C2 (host callbacks under the global lock) are consequences of the shape: execution, persistence, and orchestration interlocked in one package with locks smeared over user-code boundaries. Fixing them inside a 1,913-line `runtime.go` + 807-line `replayer.go` bakes the broken shape deeper; the rebuilt structure makes these bugs unrepresentable (locks never span user code — enforced as an ownership rule, see Target architecture).
3. **Demolition before remediation shrinks every later diff.** Deleting dead recovery paths, wrappers, and unused fields first means the reorganization moves less code and the correctness/perf diffs touch only live code. v1 had the demolition work scattered as P3.1/P4.2/P4.3 behind 14 commits of fixes.
4. **"Idiomatic Go" is a structure, not a comment.** Concrete types, consumer-side interfaces, one obvious recovery path, packages that map to responsibilities, and a thin public surface over `internal/` machinery — none of that existed in v1's end state.

## Target architecture (the ground this refactor lays)

Import direction flows DOWN the table; a layer may import only lower layers. Enforced mechanically by `TestPackageLayering` (R.6), exceptions whitelisted with reasons.

| Layer | Package | Owns | Must not import |
|---|---|---|---|
| 0 shared | `pkg/core` | logger (concrete zap), errors, IDs | — |
| 1 persistence | `pkg/store` | run state stores (memory + persistent) | layers ≥2 |
| 1 persistence | `pkg/actionlog` | append-only run log, `RunEnumerator`, legacy dual-read | layers ≥2 |
| 2 domain | `pkg/pipeline` | graph IR types, mutators, canonical pause/route forms, checkpoint wire form | layers ≥3 (except `pkg/events` bus, whitelisted) |
| 3 fold (pure) | `pkg/replay` | the Replayer: seq-fold of log entries; no locks, no I/O, no scheduling | layers 4–6 |
| 4 compile | `pkg/compiler`, `pkg/nodekit` | graph → executable definition (one walk); build-time config compilation; node-authoring SDK | layers 5–6 |
| 5 nodes | `pkg/nodes/*` | leaf node implementations | `pkg/runtime`, `pkg/replay`, `pkg/compiler` |
| 6 orchestration | `pkg/runtime` (thin) + `internal/runtime` (implementation) | registration, dispatch, pause/resume, outcomes; **the one recovery component** (`internal/runtime/recovery.go`): dispatch + rebuild + re-arm | — (top) |
| projections | `pkg/events`, `pkg/scheduler`, `pkg/watch`, `pkg/timeline`, `pkg/registry` | bus; timers (sole owner of re-arms); watch registrations; read-only log projection; workflow registry | layer 6 internals |

**Ownership rules (correct-by-construction properties, each traceable to an audit finding):**

| Rule | Makes impossible | Enforced by |
|---|---|---|
| Locks never span user code (step Action, host callbacks, goja eval, secret lookups) | C1, C2 classes of deadlock | execution on snapshots in `pkg/pipeline` stage exec + recovery component; critical-section comments + tests |
| Every run-status fact is written ONCE — the log; RAM structures are derived caches | §B duplication, §C dual channels | S.2; `runMetas`/`outcomes` become reconcile-on-read caches |
| One component rebuilds or re-arms a run: `recovery.go` | §A six paths | R.4 + grep gate in DoD |
| Timers belong to `pkg/scheduler`; orchestration passes intents, never holds timer handles | H9, L2 leak classes | C3-fix + scheduler API |
| One canonical form per concept; other forms derived at serialization boundaries | §F hand-synced mapping code | R.2; wire round-trip tests |
| Interfaces exist at the Options boundary or at genuine multi-impl seams only | OA interface pyramid | R.3; audit note records the 3 retained exceptions |

**Public surface policy (delete policy — pre-production):** nothing in the Go surface is a compatibility contract. Dead code is deleted outright, exported or not — no deprecation stubs, no v2 removal ledger. The ONLY contracts are cross-boundary: canvas wire JSON (`compiler.DecodeWireGraph` / `compiler.CompileWire`), the frozen `.ts` catalog + `src/*.ts` (npm types), and the golden replay vectors. Implementation machinery lives under `internal/` so the surface stays deliberately thin.

## Binding constraints

| Constraint | Decision |
|---|---|
| Scope | All three audits in one plan — structural verdicts included as first-class phases (v2 change) |
| Ordering | Safety net → demolition → foundation rebuild → correctness → performance → durability → docs (v2 change; v1 was correctness-first on the old shape) |
| Restart durability | In scope: one durable log, one repopulated run index, one rebuild procedure (architecture §A verdict) |
| Breaking changes | Allowed: library has no production consumers (confirmed 20261003). Contracts: canvas wire JSON, frozen `.ts` catalog, golden replay vectors. Everything else may be deleted or reshaped; every removed exported symbol recorded in CHANGELOG `Removed` |
| TypeScript | Go-first: Go replay semantics are canonical; `src/*.ts` + node `.ts` catalog frozen as wire-format; divergence documented, golden vectors published |
| Verification | Per commit: `go vet ./...` + `go test -race ./...`. Perf commits: `make bench` before/after; `benchstat` table required for structural perf commits. Phase gates: `make bench-race`. Reorg commits additionally prove wire stability: `pkg/compiler/wire_test.go` round-trips + golden vectors green |
| Delivery | One branch `refactor/20261003-audits`; sequential commits; each commit green; ship together |

## Non-negotiable traps (read before starting any phase)

1. **Chained re-pause semantics.** Commit `f1e5630` already reverted an async watch-resume change because it broke chained re-pause. Any H5/L3 fix must preserve re-pause ordering: `pkg/runtime/repause_test.go` and `pkg/runtime/repause_bench_test.go` are the contract. Do not make resume uniformly async without proving chained re-pause still settles in order.
2. **`KindEffectCompleted` deletion is coupled to the replay fold.** The Replayer keys effectful deltas on `KindEffectCompleted | KindEffectFailed` (`pkg/replay/replayer.go:669-675`). Deleting the write without re-keying the fold loses replay data. Sequence: P4.1 changes storage only; the write removal + constant deletion + fold re-key + vector swap happen together in S.1.
3. **The only contracts left are cross-boundary.** Canvas wire JSON (`compiler.DecodeWireGraph` / `compiler.CompileWire`), the frozen `.ts` node catalog + `src/*.ts` (npm types), and the golden replay vectors. Everything else in the Go surface may change or vanish without ceremony. Consequences: the legacy single-document action-log format is DELETED, not dual-read (P4.1); the `KindEffectCompleted` constant is DELETED, not preserved (S.1); the `placeholder` edge role stays accepted-and-ignored (wire compat).
4. **Move-then-edit discipline.** A commit that relocates code (file splits, `internal/` moves, `git mv`) changes nothing else — imports and package clauses excepted. Semantic edits land as their own commits at the final location. Review aid: `git diff --find-renames=40%` must show near-pure renames for move commits.
5. **Demolition ≠ behavior change.** Phase 1 deletes only code with zero live callers (verified by grep + the P0.2 characterization suite staying green). If a "dead" symbol turns out reachable from any test or example, it moves to the live list with a disposition note — no silent resurrection.

---

## Phase 0 — Safety net (before any demolition)

- [ ] P0.1 Capture baseline: benchmarks + race status
  - **Context:** Later phases need before/after evidence, and the race detector must be green before any lock is touched. The repo already has `make bench` / `make bench-race` targets (`#improvement-bench-ci-001`) and suites: `pkg/replay/rebuild_bench_test.go`, `pkg/runtime/repause_bench_test.go`, `pkg/scheduler/cron_cache_bench_test.go`.
  - **Details:** Run `go test -race ./...` and record any failures as pre-existing (file them, do not fix inside the refactor silently). Run `make bench` and save output as the baseline. Existing benchmarks cover rebuild latency, chained re-pause, and cron parsing — the stage-execution path (H1/H2) has no benchmark yet, which P0.2 adds.
  - **Files:** artifact `todo/20261003-bench-baseline.txt`; read-only `Makefile`.

- [ ] P0.2 Characterization tests pinning current observable behavior
  - **Context:** The refactor deletes, moves, and re-keys storage. Tests must pin observable behavior first so semantic drift is caught at the offending commit, not in production. This follows the pattern of `pkg/pipeline/stage_snapshot_test.go`, which already pins `#review-20260910-009`.
  - **Details:** Table-driven tests: (a) **replay fold** — given a scripted entry stream (step start/complete, effect complete/fail, retry, rerun attempt re-baselining per `#review-20260910-025`, checkpoint mid-stream), assert rebuilt state and resume address; (b) **checkpoint/resume parity** — a deterministic graph's pause→resume end state equals its uninterrupted end state; (c) **action log seq stability** — seq numbering derives from max recorded Seq, not entry count (`pkg/actionlog/anansi.go:192-200`); (d) **cron auto-resume arming** — arms only when `Checkpoint.Cron` set; re-pause cancels the stale schedule (documents H9's missing cancel as today's behavior, then Phase 3 flips the terminal branch and the test extends).
  - **Files:** new `pkg/replay/characterization_test.go`, `tests/pause_resume_characterization_test.go`, `pkg/actionlog/characterization_test.go`.

- [ ] P0.3 Golden replay vectors (shared artifact)
  - **Context:** Phase 5 unifies replay semantics (§D: Go keeps first effect entry per step id, TS folds every output in seq order) and stops writing `KindEffectCompleted`. Golden vectors make the semantics change explicit, reviewable, and give npm consumers a reference implementation target. They also guard the Phase 2 fold-file split and R.2/R.3 representation work: the fold must behave identically before and after the rebuild.
  - **Details:** JSON files of shape `{entries: [...], expectedState: {...}, expectedResumeAddress: {...}, semantics: "baseline"}` covering: first-vs-last effect per step, route folding across loop iterations, multi-attempt rerun re-baselining, checkpoint folded mid-stream. A Go test loads and runs every vector. `src/replay.ts` gets a frozen comment block pointing at the vectors and documenting the known divergence (no behavior change — TS is frozen wire-format per constraints).
  - **Files:** new `pkg/replay/testdata/golden/*.json`, `pkg/replay/golden_test.go`; comment-only touch `src/replay.ts`.

---

## Phase 1 — Demolition (delete everything the audits proved dead)

Gate for the whole phase: characterization suite + wire round-trip tests green after every commit; grep proofs that no live caller remains for anything deleted; CHANGELOG `Removed` section updated in the same commit as each deletion.

- [ ] D.1 Delete dead recovery paths 3, 4, 6 (§A rows 3, 4, 6)
  - **Context:** Three of the six recovery paths participate in nothing: `Options.StoreLoader`/`newStoreForID` (zero callers; the single "call site" is its own wiring, `runtime.go:1673-1687`); the durable-shaped Pebble event bus (`pkg/events/durable.go` — `Underlying()` has zero callers, `Subscribe` never reads it, and its "durable" store is an in-memory map); `registry.FastPathResume`/`MarkPaused` (never called by `WorkflowRuntime`; only a test touches `MarkPaused`; README falsely advertises "two-tier resumption").
  - **Details:** Delete outright — exported or not: (a) `Options.StoreLoader` field, `newStoreForID`, the `rt.storeLoader` wiring, the `pkg/store/anansi.go:96` `Loader` adapter. (b) The whole of `pkg/events/durable.go`: `NewDurableEventBus`/`WithDurableBackend`/`DurableBusConfig` and the in-memory "durable" store behind them. (c) `pkg/registry`: `MarkPaused`/`FastPathResume`/`ActiveRun` timer machinery, plus the test that exercises `MarkPaused`. (d) README: the "two-tier resumption" claim is removed in THIS commit so code and docs never disagree within the branch (full doc pass is P6.1).
  - **Files:** `pkg/runtime/runtime.go`, `pkg/store/anansi.go`, `pkg/events/durable.go`, `pkg/events/events.go`, `pkg/registry/registry.go`, `README.md` (claim fix only).
  - **Verify:** full suite green; grep proves no references remain anywhere.

- [ ] D.2 Delete dead subsystems, fields, and write-only state (§E remainder)
  - **Context:** §E rows not covered by D.1: `store.Flush` (no-op in both impls, zero callers in tree); `PipelineFactory.schema`/`PipelineDefinition.Schema` (written, never read — `factory.go:60`, `compiler.go:720`); `Stage.Order` (assigned by compiler, asserted in tests, never read by `Run` — slice position is the real order); `wfResources` map (written at `runtime.go:574,1837`, never iterated for cleanup — delete the write-only map; REAL resource teardown arrives with C2 on live resources, not here); SPEC-promised-but-absent subsystems (`TimelineStore.Append`/snapshots never populated — `SPEC.md:309-322`; `pkg/server` "Phase 7" — `SPEC.md:345`, `runtime.go:1352`).
  - **Details:** Delete outright: `store.Flush` from the store surface and both implementations; `PipelineFactory.schema`/`PipelineDefinition.Schema`; `Stage.Order` (stop assigning, delete the field, update the tests that assert it); the `wfResources` map and its writes (it never guarded anything real). `placeholder` delete map + writes (it never guarded anything real). `placeholder` edge role: stays accepted-and-ignored (wire compat), documented in `pkg/compiler/wire.go`. SPEC.md: mark snapshot/server sections explicitly "not implemented in Go runtime" (full truth pass is P6.1; one-line markers here so the audit trail is closed).
  - **Files:** `pkg/store/store.go`, `pkg/pipeline/factory.go`, `pkg/pipeline/types.go`, `pkg/compiler/compiler.go`, `pkg/compiler/wire.go`, `pkg/runtime/runtime.go`, `SPEC.md`, node tests asserting `Stage.Order`.

- [ ] D.3 Delete wrappers, factories-of-one, and unused one-liners (OA medium/low-impact)
  - **Context:** Factories wrapping one call (`store.NewFreshStore`, `actionlog.AnansiStoreFactory`, `store.AnansiStoreFactory`), `pipeline.NewFactoryFromModel` (dead — discards its computation; single reference is its own test), trivial wrappers (`RunHandle`, `store.SetValue` — 11 call lines all internal), trivial compiler helpers (`flowEdges`, `dependencyEdgesTo`, `nextDefaultTarget`, `appendRequirement`, `staticResources`, `buildResourcesFor`).
  - **Details:** Delete outright and fix callers to use the wrapped constructor/call directly: the factories-of-one, `NewFactoryFromModel` (+ its test), `RunHandle` (collapses into `*RunContextImpl` — full collapse lands with R.3's concrete contexts). `store.SetValue` keeps (11 live internal callers — API by use, and it earns its keep). Trivial compiler helpers: inline only where the call site gets clearer — judgment per helper, no mechanical inlining. `NopLog`/`NopLogger` deletion is R.3 work (it requires the nil-check rewrite of internal call sites, not just deletion).
  - **Files:** per the OA audit's medium/low-impact sections.
  - **Verify:** suite green; grep for each deleted symbol returns nothing.

- [ ] D.4 Kill `pkg/effect`; delete `state["results"]`
  - **Context:** `pkg/effect` is a 15-LOC package that exists only as an import-cycle workaround, hand-syncing its constants with `nodekit.Effect` (§F effect-flag row). `state["results"]` is read in three places (`pkg/nodekit/build.go:83,123`, `pkg/nodekit/nodekit.go:255`) to feed `NodeRunContext.Results` and written NOWHERE in `pkg/` — node authors' `Results` is always nil unless a host seeds it manually.
  - **Details:** Move the canonical effect enum into `pkg/pipeline/types.go` (same int values — `Step.Effect int` accepts them unchanged); update the three `pkg/effect` importers; delete `pkg/effect` outright. `state["results"]`: delete the three reads and `NodeRunContext.Results` — UNLESS `handles_parity_test.go` fails because the frozen `.ts` catalog's handle signatures reference `results`; in that case keep the field with a one-line comment ("frozen wire contract") and delete only the dead reads. Publishing real results is a feature, not this refactor.
  - **Files:** `pkg/pipeline/types.go`, `pkg/nodekit/effect.go`, `pkg/effect/effect.go` (deleted), `pkg/nodekit/nodekit.go`, `pkg/nodekit/build.go` (reads deleted), `README.md`.
  - **Verify:** suite green; `go vet ./...` confirms the import cycle cannot re-form (nodes no longer have a cycle path).

- [ ] D.5 Deletion audit + §E dispositions
  - **Context:** The delete policy is only credible if auditable: everything removed in Phase 1 must be listed, and nothing deprecated-in-place may survive — half-measures re-accumulate cruft.
  - **Details:** Scripted check (small `_test.go` in `tests/`): (a) zero `// Deprecated:` comments exist on the branch — deletion is the policy, stubbing is a regression; (b) grepping the tree for each §E/OA dead symbol returns nothing (or only its CHANGELOG `Removed` entry). CHANGELOG gains a `Removed` section listing every deleted exported symbol with one-line rationale. Record §E table dispositions inline in `todo/20261003-architecture-audit.md` (a `→ D.x` disposition column), so the audit file answers item-by-item.
  - **Files:** `CHANGELOG.md` (new, `Removed` section), `todo/20261003-architecture-audit.md` (dispositions), new `tests/deletion_audit_test.go`.

---

## Phase 2 — Foundation rebuild (behavior-preserving reorganization)

Gate for the whole phase: characterization + golden vectors + wire round-trip tests green after every commit; move commits show near-pure renames under `git diff --find-renames=40%`.

- [ ] R.1 Split the god files along concern boundaries (pure moves)
  - **Context:** `pkg/runtime/runtime.go` (1,913 lines) mixes registration, dispatch, pause/resume, outcomes, recovery dispatch, and eviction. `pkg/pipeline/context.go` (1,034) mixes context types, routing interpretation (`:705-994`), and checkpoint serialization. `pkg/compiler/compiler.go` (883) mixes graph walking and wire encoding. `pkg/replay/replayer.go` (807) mixes fold, indexing, and step execution. No later task can be reviewed cleanly against these files.
  - **Details:** Within-package file splits only — no package moves, no semantic edits: `runtime.go` → `register.go` (Register/Deregister/Shutdown lifecycle), `dispatch.go` (event dispatch + paused index), `pauseresume.go` (pause/resume/watch-resume paths), `outcomes.go` (run results, metas, rerun index), `recovery_dispatch.go` (the type-assert dispatch block, moved as-is; R.4 replaces it). `context.go` → `context.go` (types), `routing_interp.go` (the `:705-994` switch — R.2 replaces it), `checkpoint.go` (serialization). `compiler.go` → `compiler.go` (walk), `wire.go` stays (encoding). `replayer.go` → `fold.go` (pure fold), `index.go` (memoized indexes), `execute.go` (step execution path). Each split is its own commit; imports only.
  - **Files:** as listed; commits 6–7 in the sequence table.
  - **Verify:** suite + golden vectors green per commit; `git diff --find-renames=40%` shows moves only.

- [ ] R.2 Canonical representations — one form per concept, derived at the boundary (§F)
  - **Context:** Every core concept exists in 3–7 hand-synced forms: Pause (7 forms), routing decision (6 forms — `RoutingInstruction` → decision string → `serializeInstruction` → `interpretationOf` → `routingHandle` → event payload fields, `context.go:705-994`), run ID (7 carriers), effect flag (3 — collapsed to 1 by D.4), node definition styles (2), two unrelated `PipelineRegistry` types with the same name. The hand-synced mapping code is the §F "consistency obligation" — a standing source of drift bugs.
  - **Details:** (1) **Routing** — internal evaluation produces ONE tagged struct; `serializeInstruction`/`interpretationOf` collapse into its methods; the `RoutingInstruction` interface is deleted and `Router` implementations (all in-tree) return the canonical struct. (2) **Pause** — `PauseInstruction` canonical; `PipelineCheckpoint` remains the serialized boundary form (it IS the wire/log format) but is produced by exactly one serializer and parsed by exactly one parser (centralize the scattered construction sites; grep `__pipeline_data__`). (3) **Run ID** — `pipeline.RunIDFromContext(ctx)` is the accessor; new code stops adding carriers (the 7 existing carriers are documented as deprecated patterns, not mechanically migrated — diminishing returns). (4) **Registry naming** — rename runtime-internal usage of `registry.PipelineRegistry` to `registry.RunRegistry` if unexported-impact only, else fix docs to disambiguate; the two same-named types must never be confused in review again. (5) **Handles** — the `Handles`/`HandlesJS` parity obligation stays test-enforced (frozen `.ts` side).
  - **Files:** `pkg/pipeline/types.go`, `pkg/pipeline/routing_interp.go` (deleted/absorbed), `pkg/pipeline/checkpoint.go`, `pkg/pipeline/context.go`, `pkg/registry/registry.go`.
  - **Verify:** wire round-trip tests (`pkg/compiler/wire_test.go`) prove boundary forms unchanged; golden vectors green.

- [ ] R.3 Concrete internals — delete the single-impl interfaces (OA high-impact)
  - **Context:** 11 single-implementation interfaces (OA table). The TS-rooted pattern — interface + single impl + DI wrapper — costs indirection at every call and forces Nop null-objects. Go idiom: concrete types internally; interfaces only where a fake is genuinely needed. Pre-production: the interfaces themselves can be deleted, not just bypassed.
  - **Details:** Delete each interface and take the concrete type at every internal field/param/call site; `Options` field types change freely (pre-production). Specifically: `store.Store` → deleted, internal `*MemoryStore` (PersistentStore embeds it already); `events.ScopedEventBus` → deleted, `*MemoryScopedBus`; `pipeline.PipelineContext`/`RunContext` → deleted, concrete structs; `core.Logger` → deleted, `*zap.Logger` + `zap.NewNop()` internally (`NopLogger`/`ZapLogger` adapters deleted); `watch.WatchService` → deleted, concrete; `runtime.SecretProvider` (zero impls) → deleted, replaced by `func(context.Context, string) (any, bool)`; `pipeline.PipelineRegistry` → deleted, concrete registry struct; `pipeline.Rebuilder` → deleted, concrete factory method; `actionlog.NopLog`/`NopLogger` deleted — internal call sites become nil checks (default `Options.ActionLog` becomes nil = disabled). Retained (recorded as `[=]` exceptions with reasoning in the OA audit file): `actionlog.Store` (two real impls + nil; test fakes), `scheduler.Scheduler` and `runtime.EventSource` (genuine fake-ability for hosts/tests). Also here: fix `typed.go` generic field shadowing (OA: `TypedRunContext[C]` construction shadows fields; flatten so `Define[C]` builds `NodeDefinition` directly — public signatures unchanged).
  - **Files:** `pkg/store/store.go`, `pkg/events/events.go`, `pkg/pipeline/types.go`, `pkg/pipeline/workflow.go`, `pkg/pipeline/factory.go`, `pkg/core/logger.go`, `pkg/core/zap_adapter.go`, `pkg/watch/watch.go`, `pkg/runtime/eventsource.go`, `pkg/nodekit/typed.go`, `pkg/nodekit/nodekit.go`.
  - **Verify:** suite + golden vectors; grep gate — no deleted interface name remains referenced anywhere; OA audit file updated with the per-row disposition.

- [ ] R.4 ONE recovery component (§A rows 1–2 — the structural verdict)
  - **Context:** After D.1, three mechanisms remain: the Replayer (`pkg/replay`), the checkpoint-fallback in `pkg/pipeline/factory.go:169-183`, and the dispatch in `runtime` that type-asserts the log implementation (`_, isNop := rt.actionLog.(actionlog.NopLog)`, `runtime.go` ~:1051-1083) and silently degrades guarantees on ANY replayer error. Recovery logic still lives in three places — nobody can reason about it.
  - **Details:** New `internal/runtime/recovery.go` (landed with R.5's `internal/` move; until then `pkg/runtime/recovery.go`): the ONLY code in the tree that decides how a run recovers. Capability decided by DATA, not type assertion: attempt event-sourced rebuild whenever the log returns any entries for the run; no entries + checkpoint present → checkpoint path, now LOGGED and counted (never silent); nothing → unknown run. `KindCheckpoint` entries stay IN the log and fold during replay (checkpoint becomes a replay optimization inside the one component, not a parallel system). The factory's checkpoint-resume code moves INTO the recovery component (move-then-edit: the move commit is pure, the absorption commit is the semantic one). Document the recovery contract in the package doc: what recovers from where, what is logged when guarantees degrade. Startup rebuild and re-arm (§A flagship) are ADDED here in Phase 5 — R.4 builds the single home they must live in.
  - **Files:** new `pkg/runtime/recovery.go` (→ `internal/runtime/recovery.go` via R.5), `pkg/pipeline/factory.go` (checkpoint path moves out), `pkg/runtime/recovery_dispatch.go` (from R.1 — deleted, absorbed), `pkg/actionlog/actionlog.go` (comment-only: capability contract).
  - **Verify:** characterization tests: MemoryActionLog → rebuild from log; NopLog + checkpoint → legacy fallback (logged); log error mid-rebuild → logged fallback + counter; golden vectors green.

- [ ] R.5 `internal/` boundary — implementation out of the public surface
  - **Context:** All 16 top-level packages are exported; every internal decision is surface API by default, which is exactly how the TS structure (where nothing is private by module convention) got ported. Idiomatic Go: a thin public entry over `internal/` machinery.
  - **Details:** Move the runtime implementation (post-R.1 files: register/dispatch/pauseresume/outcomes/recovery) to `internal/runtime`. `pkg/runtime` keeps: constructors (`NewWorkflowRuntime`), `Options`, `EventSource`/`SecretProvider` types, event/result types — thin files delegating to internal. The move commit is pure (move-then-edit). `pkg/pipeline` keeps its path (public types must stay put — node authors and hosts import them); its execution guts stay in-place (a pipeline-internal split buys less than it costs in churn). Other packages keep paths — the layer map (R.6) + recovery component deliver the structural value; a wholesale 32-package relocation would be churn without an audit finding behind it, and this plan deletes churn as a value.
  - **Files:** `pkg/runtime/*` (implementation → `internal/runtime/`), new thin `pkg/runtime` API files.
  - **Verify:** suite + wire tests green; `pkg/runtime` reads as a thin entry (constructors, Options, public types).

- [ ] R.6 Enforce the layer map with a test
  - **Context:** The Target architecture's import rules are only real if a machine checks them. Go has no compiler-enforced layering; a `tests/layering_test.go` walking `go list -deps` can assert direction. "Laying ground" means the ground is surveyed and fenced.
  - **Details:** Assert: `pkg/nodes/*` do not import `pkg/runtime`, `pkg/replay`, `pkg/compiler`; `pkg/replay` imports only `pkg/{core,pipeline,store,actionlog,effect→none after D.4}` (pure fold — no events, no scheduler, no watch); `pkg/pipeline` does not import layers ≥3 except `pkg/events` (whitelisted: bus interface); `pkg/runtime` (thin) imports nothing beyond its own internal + the public types it re-exports; `internal/runtime` is the only package importing `pkg/replay` AND `pkg/scheduler`/`pkg/watch` together; `pkg/timeline` imports only `pkg/{actionlog,events}`. Every exception needs a whitelist entry WITH the audit-finding or wire-compat reason; a whitelist entry without a reason fails the test.
  - **Files:** new `tests/layering_test.go`, new `pkg/runtime/doc.go` + `internal/runtime/doc.go` (the architecture map, so the fence and the map live in-tree).
  - **Verify:** test green; deliberately-crafted violation commit reversed as a demo (test catches it) — recorded in the commit message.

- [ ] R.7 Execution-chain slimming (§G — behavior-preserving halves)
  - **Context:** Default/advance routing is implemented FOUR times (`compiler.go:413-419`, `compiler.go:563-574`, `pkg/nodekit/build.go:194`, `pkg/pipeline/router.go:11`); the compiler traverses the graph three times (outer BFS, body BFS, fork-branch BFS) with near-identical logic plus a fourth fork-membership computation. These are structural hops, not perf — they belong in the rebuild, not the perf phase.
  - **Details:** Extract ONE `defaultTargetFor(node)` used by all four routing sites (each site's nuances — terminal leaves, pause — become parameters; pin with `pkg/compiler/wire_test.go` graphs). Factor ONE `walkGraph(seeds, enterNested func)` used three times; fork-membership folds into the same walk as a collector. The per-attempt config recompile (§G "three intermediate forms") is perf-phase work (P4.3) — here only the duplicated code disappears.
  - **Files:** `pkg/compiler/compiler.go`, `pkg/nodekit/build.go`, `pkg/pipeline/router.go`.
  - **Verify:** `go test ./pkg/compiler/... ./tests/...` incl. `samples_test.go`, `fork_while_workflow_test.go`; golden vectors green.

---

## Phase 3 — Correctness on the rebuilt foundation (hot-path audit §Suggested order 1)

These land AFTER the rebuild deliberately: C1/C2 become properties of the new boundaries (locks-never-span-user-code is an ownership rule, not a patch), and the fixes go into the R.1-split files where they will live permanently. Each fix carries its audit ID.

- [ ] C1 — Replay must not execute step code under the store read lock
  - **Context:** `pkg/replay/replayer.go:780-783` (post-R.1: `pkg/replay/execute.go`) runs `step.Action` inside `st.Read(...)`. Any step calling `store.Update` self-deadlocks (RWMutex is non-reentrant), and arbitrary user code (goja eval, HTTP) runs while all writers are blocked. The identical hazard was fixed for normal execution in `#review-20260910-009` (`pkg/pipeline/stage.go:166-196`); the replay path never got the fix. This is the first proof of the locks-never-span-user-code ownership rule applied to the fold/execute split.
  - **Details:** Apply the same shape as `executeStepAttempt`: snapshot under `Read` (or reuse `store.DeepCopyMap` on the pinned map), release, run `step.Action` on the snapshot, then apply the returned mutator through `st.Update`. Preserve the replay-specific context (`pipeline.NewReplayContext()`) and error semantics (`actionErr` vs `readErr` distinction must survive). Extend the P0.2 replay characterization test with a step whose Action calls `st.Update` to pin the deadlock fix.
  - **Files:** `pkg/replay/execute.go` (executeStep path); test in `pkg/replay/replayer_test.go`.
  - **Verify:** `go test -race ./pkg/replay/...`; new deadlock-regression test green.

- [ ] C2 — Bound `Register`'s critical section; fix the two resource leaks on the same path
  - **Context:** `pkg/runtime/register.go` (pre-R.1: `runtime.go:557-646`) holds `rt.mu` (global, non-reentrant) across (a) `secrets.Has` (vault/network I/O, via `validateRequirements` → :565), (b) `rt.scheduler.Schedule`, and (c) host `eventSource.OnRegister` (:624), whose `emit` closure calls `rt.bus.Emit → dispatch → rt.mu.Lock()` (:751) — a latent self-deadlock the EventSource docs invite. Two adjacent leaks: the `OnRegister` unsubscribe `func()` is discarded (:624, never called on Deregister), and `wfResources` was write-only (map deleted in D.2 — this task adds the REAL teardown for the resources it pointed at). This is the second proof of the ownership rule: host callbacks never inside a critical section.
  - **Details:** Restructure: under `rt.mu` — validate uniqueness, insert `workflowRecord`, update `rt.index`, snapshot the trigger set; release `rt.mu`; then run `validateRequirements` (secrets I/O), `scheduler.Schedule`, and `eventSource.OnRegister`. On failure, roll back under a fresh `rt.mu` critical section (rollback body exists at :629-646; move into a helper that locks rather than assumes the caller's lock). Call and retain the EventSource unsubscribe func in `workflowRecord`; invoke it in Deregister. Add resource teardown (close/iterate what `wfResources` tracked) in Deregister and Shutdown. Care: after releasing `rt.mu` before `OnRegister`, a concurrent Deregister of the same workflow ID becomes possible — re-validate existence before rollback and make record removal idempotent.
  - **Files:** `internal/runtime/register.go` (+ Deregister, Shutdown), tests in `internal/runtime/register_lifecycle_test.go` (new): host `emit()` inside `OnRegister` (deadlock regression), Deregister unsubscribe, resource cleanup.
  - **Verify:** `go test -race ./internal/runtime/... ./pkg/runtime/...` incl. the synchronous-emit test.

- [ ] C3-fix — Cancel cron auto-resume on terminal outcomes; deterministic watch-timer owner
  - **Context:** `internal/runtime/pauseresume.go` (pre-R.1: `runtime.go:901-914`) schedules a self-re-arming auto-resume timer on pause and cancels it ONLY on re-pause (:1281) or workflow Deregister (:681, workflow-trigger schedules only). The terminal branch (:1287-1291) cleans `watchService.OnRunEnded` but never `scheduler.Cancel(runID)` — every run that pauses with `Checkpoint.Cron` and then completes leaks a permanent timer that grabs `rt.mu` forever. This is the timers-owned-by-scheduler rule applied: orchestration must release what it asked the scheduler to hold. L2: `pkg/watch` `startTimeoutLocked` assigns the shared timeout timer to an arbitrary registration from map iteration.
  - **Details:** Add `rt.scheduler.Cancel(runID)` next to `watchService.OnRunEnded(runID)` in ALL terminal branches (completion, failed, aborted — grep `OnRunEnded` for the full set). Extend P0.2's cron test: pause with cron → force-complete → assert no further timer fires (counting scheduler fake — `scheduler.Scheduler` keeps its interface, R.3 recorded it as a genuine seam). For L2, assign the timer to the earliest-deadline registration (deterministic) or store per-registration timers; behavior otherwise identical.
  - **Files:** `internal/runtime/pauseresume.go`, `pkg/watch/watch.go`; tests `internal/runtime/runtime_test.go` / eviction area.
  - **Verify:** leak test green under `-race`; `make bench-race` unaffected.

- [ ] C4-fix — Fix lock-order fragility in `copyStoreState` and `ReadCheckpoint` (M5)
  - **Context:** `pkg/pipeline/factory.go:201-210` nests `src.Read` → `dst.Update` (two stores' locks held; safe today only by accident of ordering). `factory.go:172-176` runs a full checkpoint JSON round trip inside `Read`, stretching the RLock. Post-R.4 the checkpoint parse that remains in factory must not hold locks across I/O-shaped work.
  - **Details:** `copyStoreState`: snapshot src under `Read` into a local map, return from `Read`, then apply a single mutator on dst. `ReadCheckpoint`: copy raw checkpoint bytes out under `Read`, parse outside. No behavior change intended; pin with a unit test that runs copy between two stores concurrently with reverse-direction copies to prove no inversion.
  - **Files:** `pkg/pipeline/factory.go` (:169-215).
  - **Verify:** `go test -race ./pkg/pipeline/...`.

- [ ] C5-fix — Serialize watch-service resume without breaking chained re-pause (H5 + L3)
  - **Context:** `pkg/watch` `onEvent` releases `s.mu` and calls `resumeCallback` → `rt.Resume` → full pipeline → `bus.Emit` → `onEvent` again: unbounded same-goroutine recursion with an unlock/relock window on `s.registrations`. Resume is inconsistently sync/async: inline at `runtime.go:895-897` (runs BEFORE cron scheduling at :901-914, so a stale cron can overwrite the newer schedule), async at :801-804 and :1260 (L3). **Trap #1 applies in full:** commit `f1e5630` reverted an async watch-resume callback because it broke chained re-pause — this task must beat that history, not repeat it.
  - **Details:** Two moves, kept minimal: (1) make resume dispatch uniform — ONE code path through a resume serializer (ordered task queue; the watch callback posts a task and returns; registration mutation happens only on the serializer). (2) Order fix: cron scheduling for a newly paused run must read the FINAL pause state (move the inline `rt.Resume` :895-897 out of the pre-cron position, or schedule cron after the resume attempt settles) so a stale schedule can never overwrite a newer wait. Before writing code, write the failing test from `repause_test.go` scenarios at depth ≥ 3 chained re-pauses; run it against the new implementation AND against pre-refactor code to confirm it captures the ordering contract. If the full serializer proves riskier than the window it closes, land only the cron-ordering half and mark the serializer half `[X]` with the reason — the unlock window is real but the re-pause regression is worse.
  - **Files:** `pkg/watch/watch.go`, `internal/runtime/pauseresume.go` (spawn/dispatch resume paths); tests `internal/runtime/repause_test.go` (extend), new `internal/runtime/watch_resume_order_test.go`.
  - **Verify:** `go test -race ./internal/runtime/... ./pkg/watch/...` with repause suite at depth 10; `BenchmarkChainedRePause` no regression.

---

## Phase 4 — Hot-path performance (hot-path audit §Suggested order 2–5)

Every commit carries `make bench` before/after; the structural batch (P4.5, P4.7, P4.8) additionally requires a `benchstat` table against the P0.1 baseline. These land after the rebuild so each fix is local to its layer (storage in `pkg/actionlog`, deltas in `pkg/pipeline`, locking in `internal/runtime`).

- [ ] P4.1 C3 — Action log append becomes O(1) tail writes with per-run lock sharding
  - **Context:** `pkg/actionlog/anansi.go:180-214` `Append` loads ALL prior entries, re-marshals the full array, and rewrites the whole document — two DB round trips per append, O(entries²) bytes per run, behind one package mutex shared by ALL runs. Callers: `pkg/pipeline/stage.go:126-141, 326-337, 343-353` (hot path of every step). **Trap #2:** this commit changes STORAGE ONLY; dropping the duplicate `KindEffectCompleted` write is S.1 (coupled to replay semantics).
  - **Details:** New storage layout: per-entry rows or chunked tail-append (choose during implementation; per-entry rows under `(runID, rerunIndex, seq)` keying is simplest and makes `All` a range scan). Provide `RunEnumerator` (optional interface on `actionlog.Store`: `Runs(ctx) ([]RunRef, error)` + `LatestRerunIndex(ctx, runID)`) — the capability probe S.3 needs; implement for `AnansiStore` and `MemoryActionLog`; a nil/absent log enumerates nothing. No legacy dual-read (Trap #3 — no production data): the new layout is the only layout; delete the legacy single-document reader and its tests. Lock sharding: per-`(runID, rerunIndex)` mutexes replace the store-wide `mu`. Benchmark: append benchmark at 10/100/1000-entry runs.
  - **Files:** `pkg/actionlog/anansi.go`, `pkg/actionlog/actionlog.go`, `pkg/actionlog/actionlog_test.go` (legacy reader deleted); new `pkg/actionlog/append_bench_test.go`.
  - **Verify:** characterization green on the new format; append benchmark flat, not quadratic.

- [ ] P4.2 H2 + H10-amplification — Guard delta computation; mutators declare touched keys
  - **Context:** `pkg/pipeline/stage.go:311-319` computes the state delta UNCONDITIONALLY, then checks `actLog == nil` at :320 — the work (2× `DeepCopyMap` + 2× full marshal + up to 2×len(state) per-key marshals in `computeStateDelta`/`deepEqual` at :423-469) is discarded whenever there is no action log. `deepEqual` marshals both sides to JSON per key (the JS `JSON.stringify` equality reflex — §H). H10's for-each cost is mostly this delta storm amplified.
  - **Details:** Move the delta block inside `if actLog != nil`. Replace `deepEqual` with `reflect.DeepEqual` (L1 subsumed). Then the real fix: optional interface `TouchedKeys(ctx) []string` checked via type assertion — `store.PatchMutator` already knows its keys; compute the delta only over touched keys. Mutators without the interface fall back to whole-state diff (compat preserved).
  - **Files:** `pkg/pipeline/stage.go` (:300-470), `pkg/store/store.go`, `pkg/nodes/for-each/foreach.go` (adopt TouchedKeys).
  - **Verify:** new stage benchmark with 1 MB state + NopLog shows delta cost ≈ 0; action-log run produces byte-identical log entries vs pre-change (characterization).

- [ ] P4.3 H4 + §G-config — Compile node config once at Step/Router build time
  - **Context:** `pkg/nodekit/build.go:17-18` runs `prepareNodeConfig` → `CompileConfigSchema`, `pkg/nodekit/config.go:244` `Interpolate`, and reflection binding (`typed.go`) on EVERY step attempt and router eval — JSON parse + schema compile + regex substitution + struct bind per attempt (architecture verdict 4: "move to compile time — graph compilation already exists, it's the natural place").
  - **Details:** Precompute at `BuildStep`/`BuildRouter` time: compiled schema + static-resolved config. Per-run work shrinks to env/secret interpolation ONLY when the config actually references dynamic sources (detect at compile time by scanning for `${...}`/env markers; static configs skip interpolation entirely). Keep `prepareNodeConfig` behavior identical for dynamic configs (characterization: same coerced `*C` output for a config matrix before/after).
  - **Files:** `pkg/nodekit/build.go`, `pkg/nodekit/config.go`, `pkg/nodekit/typed.go`; tests `pkg/nodekit/typed_test.go`, `pkg/compiler/compiler_test.go`.
  - **Verify:** stage benchmark (from P4.2) shows config cost removed from the per-attempt path.

- [ ] P4.4 H3 + M4 — Pool goja VMs; parse switch cases at Define time
  - **Context:** `pkg/expr/expr.go:151-164` constructs a fresh `goja.Runtime` + interrupt goroutine per evaluation and re-runs ~30 validation regexes (:56-118); invoked per condition from `pkg/nodes/if/if.go:76-117`, `while.go:70,76`, switch router. `pkg/nodes/switch/switch.go` additionally `json.Unmarshal`s `cfg.Cases` on every routing call.
  - **Details:** Pool `goja.Runtime` (per-call site or `sync.Pool`; interrupt goroutine per pooled VM lifecycle, not per eval). Memoize validation on first compile (pure function of the expression string). Switch: parse cases once into a typed slice at config coercion, routing evaluates pre-parsed values. Pin behavior with `pkg/expr/expr_test.go` + `pkg/nodes/switch/switch_test.go` — eval results byte-identical (JS semantics quirks included).
  - **Files:** `pkg/expr/expr.go`, `pkg/nodes/switch/switch.go`, `pkg/nodes/if/if.go`, `pkg/nodes/while/while.go` (call sites).
  - **Verify:** expression eval benchmark (pool vs fresh) + `go test -race ./pkg/nodes/... ./pkg/expr/...`.

- [ ] P4.5 H1 + M2 — One snapshot per stage; inline single-step stages (§H: three-copies and Promise.all)
  - **Context:** `pkg/pipeline/stage.go:192-196` deep-copies the FULL state under the store RLock for EVERY attempt of EVERY step (32 MB HTTP bodies memcpy'd per attempt — `pkg/nodes/http/http.go:22,257`). Steps commit atomically at stage end (:381-393), so per-attempt full copies over-serialize — this is §H's "three simultaneous state copies" transliterated from JS. M2: goroutine + WaitGroup + recover closure per step even for single-step stages (:95-113, :362) — §H's `Promise.all` transliteration.
  - **Details:** Snapshot once per stage execution (before the step fan-out), hand each attempt the same immutable snapshot; mutators still apply atomically at commit — semantics unchanged because attempts never see each other's mutations today either (pin this claim in the P0.2 suite FIRST). Inline the synchronous path when `len(stage.Steps) <= 1` (keep the goroutine path for >1). Hoist context/closure creation out of the retry loop (:164-165). **Requires P0.1 baseline + `benchstat` table** (20-stage/100-step graph, 1 MB state).
  - **Files:** `pkg/pipeline/stage.go` (:95-208, :380-393); tests `pkg/pipeline/stage_snapshot_test.go` (extend: concurrent steps still isolated).
  - **Verify:** benchstat: allocations/op and ns/op on the stage benchmark; race detector; snapshot-isolation test green.

- [ ] P4.6 H6 — Split the global runtime lock; index paused runs; move I/O out of critical sections (§H: single-mutex transliteration)
  - **Context:** One `rt.mu sync.Mutex` guards 9+ maps — §H: "direct transliteration of 'all runtime state in one JS object'". Dispatch scans all `rt.paused` under the lock per event (post-R.1: `internal/runtime/dispatch.go`, `runtime.go:751-799`); `ListRuns`/`GetRunMeta` (:428-441, :445-458) hold the exclusive lock across per-run action-log DB queries, stalling dispatch/Resume/Register process-wide.
  - **Details:** Convert `rt.mu` to `sync.RWMutex`; split by concern where contention warrants (workflows/index = read-mostly → RLock in dispatch; paused/outcomes/runMetas keep write path under Lock). Build `pausedByEvent map[string][]*pausedRun` index maintained on pause/resume/cancel; dispatch consults the index (O(matching) not O(paused × waitEvents)) — the `PauseInstruction` `waitForEvent` match rule applied identically (pin with a test on wildcard/exact matching before switching). `ListRuns`/`GetRunMeta`: snapshot run IDs under lock, log queries outside. Keep `rt.mu` as the single lock for the simple maps — do not over-shard in one commit.
  - **Files:** `internal/runtime/dispatch.go`, `outcomes.go`, `register.go`; tests `internal/runtime/runtime_test.go` (dispatch matching characterization).
  - **Verify:** race detector under `make bench-race`; existing concurrency tests green.

- [ ] P4.7 H7 — Persistent store `Sync` flushes off the data lock
  - **Context:** `pkg/store/persistent.go` `Sync` holds the exclusive write lock across the DB write; called at every stage boundary from `pkg/pipeline/context.go:532, 597, 662, 845`. Readers of the run's state queue behind DB/network latency exactly when they are busiest.
  - **Details:** Serial flush per run: snapshot the state under lock, mark `dirty=false`, release, write outside the lock (last-write-wins). Keep `Sync` synchronous-in-contract where durability matters: pause/checkpoint paths DO want durability-before-return (`pkg/pipeline/pause_persist_test.go` is the contract); stage-boundary syncs can tolerate eventual flush — if the distinction matters, add `SyncBlocking` vs async `Flush` semantics rather than changing pause durability.
  - **Files:** `pkg/store/persistent.go`, `pkg/pipeline/context.go` (call sites if semantics split).
  - **Verify:** `pkg/store/persistent_test.go` extended: concurrent reads during flush don't block; pause-persist test still proves durability at pause.

- [ ] P4.8 H8 — Replayer: index once, iterate in order
  - **Context:** `pkg/replay` does full-log reads plus linear scans (`checkpointSeq`, `resumedAfterCheckpoint`, `indexByStepID`, `indexByStage`) — O(stages × entries) per replay — and `walkDef` ranges over `stage.Steps` (Go map), making step order nondeterministic across runs (reproducibility hazard; JS objects guaranteed insertion order — §H-adjacent).
  - **Details:** Build `map[stepID]Entry` and stage indexes ONCE on load (post-R.1 they live in `pkg/replay/index.go` — memoize there). Replace `walkDef` map iteration with the compiled definition's ordered step slice (verify `PipelineDefinition` ordering is slice-based; any remaining map iteration that defines observable order gets sorted deterministically and pinned by golden vectors). **benchstat against `pkg/replay/rebuild_bench_test.go` required.**
  - **Files:** `pkg/replay/index.go`, `pkg/replay/fold.go`.
  - **Verify:** golden vectors + characterization green; benchstat on rebuild benchmark (10/100/1000 entries × 1 and 10 attempts).

- [ ] P4.9 M1 + M3 + M6 + M7 — Copy-once export, cheaper emits, bounded outcomes, single Update
  - **Context:** M1: ≥3 full-state exports per successful run (`context.go:578,603`, `runtime.go:1550`) and the full state ships inside the success event payload. M3: every `Emit` allocates handler slices + re-walks the scope chain (`pkg/events/events.go:146-158,184-187` — §H's EventEmitter transliteration). M6: `rt.outcomes[runID]` retains full FinalState until TTL (default forever). M7: seeding/resume does one `st.Update` per payload key (`runtime.go:1499-1505`, `:989-1005`).
  - **Details:** M1: export once on success, reuse for FinalState + event payload — check consumers of `pipeline:success` payload first (timeline/UI); if the payload can be a summary + reference, shrink it, otherwise keep payload but stop the duplicate copies. M3: publish handler slices via `atomic.Pointer` on mutation; `Emit` reads the pointer; scope-chain handlers flatten at registration time per scope, not per emit. M6: store a summary + load FinalState on demand from the log when `RunResult` needs it (TTL=0 default stays non-leaking). M7: single `st.Update` writing all keys per payload.
  - **Files:** `pkg/pipeline/context.go` (:570-610), `internal/runtime/outcomes.go` (:1550, :1499-1505, :989-1005, outcomes struct ~:422), `pkg/events/events.go`.
  - **Verify:** benchstat on the structural parts; `pkg/events/events_test.go` parity; success-path benchmark.

---

## Phase 5 — The log is the source of truth; restart durability (architecture §A–§D flagship)

- [ ] S.1 Canonical seq-fold replay; stop writing `KindEffectCompleted` (§D — ONE coupled commit)
  - **Context:** Go `pkg/replay` and TS `src/replay.ts:67-135` disagree: Go keeps the FIRST effect entry per step id and the LAST route per stage; TS folds EVERY output in seq order. For looping pipelines with effectful steps they produce different state. Meanwhile `KindEffectCompleted` re-writes `KindStepCompleted`'s delta byte-identically (`stage.go:343-353`) and is consumed by neither the Replayer's step fold (it keys effect entries separately, `replayer.go:669-675`) nor the timeline — **Trap #2: these two facts are fixed in the same commit or replay data is lost.**
  - **Details:** Canonical semantics = **seq-order folding** (matches TS, is the natural event-sourcing semantic, and makes first-vs-last moot once the duplicate write stops). Update the Go Replayer fold (`pkg/replay/fold.go`); stop writing `KindEffectCompleted` on effectful step success and DELETE the constant and the fold's duplicate-key branch (`KindStepCompleted` with its `Effect` flag becomes the only completion record; no legacy entries exist in production — Trap #3). Golden vectors: the P0.3 `baseline` set is swapped to `semantics: "seq-fold"` in the SAME commit; update `src/replay.ts` frozen comment (comment only). Before merging this commit: verify the timeline never reads `KindEffectCompleted` (`pkg/timeline/timeline.go`).
  - **Files:** `pkg/replay/fold.go`, `pkg/pipeline/stage.go` (:343-353), `pkg/replay/testdata/golden/*`, `src/replay.ts` (comment only).
  - **Verify:** all vectors green; characterization updated to canonical fold; `pkg/replay/replay_subpipeline_test.go` green; log-size benchmark shows per-run entry count drop.

- [ ] S.2 Derived run status — RAM structures become caches of the log (§B, §C)
  - **Context:** One run-status fact is recorded 4× (log entries + `runMetas` + `outcomes` + `RunResult`); every lifecycle site does `bus.Emit` AND `actionlog.Append` (15 sites); `rt.rerunIdx` duplicates `actionLog.LatestRerunIndex` and diverges after janitor eviction.
  - **Details:** (1) `setRunStatus` stays as a write-through RAM cache but is no longer authoritative — reconcile-on-read from the log on miss (S.3 does the startup half). (2) Delete `rt.rerunIdx`; derive from `RunEnumerator.LatestRerunIndex` (P4.1). (3) Audit the 15 dual-write lifecycle sites: emit OR log per fact type — events for subscribers, log for durability; facts that are both stay dual-written but only the log is authoritative (document per site in `internal/runtime/outcomes.go`). (4) Timeline stays a pure projection (already correct) — verified in S.1.
  - **Files:** `internal/runtime/outcomes.go`, `internal/runtime/register.go`, `pkg/actionlog/actionlog.go` (comment: authority contract).
  - **Verify:** suite green; eviction test proves rerun index survives janitor eviction (was divergent before).

- [ ] S.3 Startup rebuild — restart durability (§A the flagship fix)
  - **Context:** Every recovery path funneled through `rt.paused[runID]` — a RAM-only map never repopulated from disk. The system paid for six recovery designs and delivered zero restart durability. The audit's verdict: durable log + repopulated run index + one rebuild procedure. R.4 built the single home; P4.1 built the enumeration capability; this is the payoff.
  - **Details:** Startup recovery in `internal/runtime/recovery.go`: `rt.Recover(ctx)` called by `NewWorkflowRuntime` when the action log implements `RunEnumerator`: (1) enumerate `(runID, rerunIndex)` refs; (2) last entry `KindPipelineCompleted`/`KindPipelineFailed` → record meta as terminal (rebuild `runMetas` lazily — see S.2); (3) non-terminal runs → rebuild state via the Replayer (R.4 path) and repopulate the paused registry from the folded `KindCheckpoint` entry; (4) re-arm: cron auto-resume timers from `Checkpoint.Cron`, watch-service registrations from checkpoint wait conditions, event-source subscriptions as needed. Costs bounded: enumeration is a key scan; full rebuild deferred per-run to first Resume (lazy) EXCEPT re-arming data, which must be eager. Acceptance test: runtime A + MemoryActionLog (durable-backed), run a workflow that pauses mid-graph; create runtime B on the SAME log; resume the run in B; drive to the same terminal state as an uninterrupted run. Also cover: in-flight (never-paused) run recovered from log; rerun-index continuation via `LatestRerunIndex`; janitor interplay (terminal runs must NOT resurrect).
  - **Files:** `internal/runtime/recovery.go`, new `tests/restart_durability_test.go`.
  - **Verify:** the acceptance test under `-race`; docs updated (P6.1).

---

## Phase 6 — Surface, docs, release

- [ ] P6.1 Documentation truth pass
  - **Context:** README documented mechanisms that did not exist ("two-tier resumption" — removed in D.1; restart recovery — real as of S.3); SPEC promises snapshots and a server phase that were never built (`SPEC.md:309-322`, `SPEC.md:345`). Phase 2 changed the architecture; the docs must describe the system that now exists.
  - **Details:** README: rewrite pause/resume around the single recovery path + startup rebuild; add an "Operational limits" note (NopLog = no restart durability; MemoryActionLog = process-lifetime durability; AnansiStore = restart durability); add the architecture map from `pkg/runtime/doc.go`. SPEC: mark unimplemented sections explicitly. Add `docs/RECOVERY.md`: the durable contract, the log kinds that matter for recovery, the `RunEnumerator` contract for backends, and the locks-never-span-user-code rule for contributors.
  - **Files:** `README.md`, `SPEC.md`, `docs/RECOVERY.md`.
  - **Verify:** doc claims match code behavior — walk each claim to its file:line once.

- [ ] P6.2 CHANGELOG finalized
  - **Context:** With deletions instead of deprecations, the CHANGELOG is the only record of what vanished and why — it replaces the v1 plan's deprecation ledger and v2 removal list entirely.
  - **Details:** Finalize `CHANGELOG.md` per phase: `Removed` (every deleted exported symbol with rationale), `Changed` (action log storage layout — old readers cannot read new logs; canonical seq-fold replay; runtime lock behavior), `Added` (restart durability, `RunEnumerator`, architecture map + internal/ boundary, layer map test). Scripted check: zero `// Deprecated:` comments on the branch (D.5 test keeps enforcing).
  - **Files:** `CHANGELOG.md`.
  - **Verify:** every deleted exported symbol from Phases 1–5 appears in `Removed`; no deprecation stubs exist.

- [ ] P6.3 Final gate + version decision
  - **Context:** One branch ships together; the gate must prove the whole plan, phase by phase.
  - **Details:** Run: `go vet ./...`, `go test -race ./...`, `make bench`, `make bench-race`, `benchstat` summary vs P0.1 baseline (attach table to the branch description), layering test, deletion-audit test. Confirm the restart-durability acceptance test runs in the default suite. Version: recommend `v2.0.0` — exported symbols are removed and replay semantics change; even with no production consumers, semver honesty costs nothing pre-adoption (repo is at v1.2.1). Bun/TS package untouched except comments — no npm release needed.
  - **Files:** branch `refactor/20261003-audits` final state.
  - **Verify:** all green; branch ready to merge as one unit.

---

## Commit sequence (branch `refactor/20261003-audits`)

| # | Commit | Tasks | Gate |
|---|---|---|---|
| 1 | `test: characterization suite, golden replay vectors, bench baseline` | P0.1–P0.3 | race + baseline |
| 2 | `refactor(runtime): delete dead recovery paths (StoreLoader, durable bus, two-tier resume)` | D.1 | race + wire tests |
| 3 | `refactor: delete dead subsystems, write-only fields, promised-but-absent notes` | D.2 | race + wire tests |
| 4 | `refactor: delete wrappers and factories-of-one; nil-checks for internal Nops` | D.3 | race + wire tests |
| 5 | `refactor(effect): canonical flag in pipeline; delete pkg/effect and state["results"]` | D.4 | race + vet |
| 6 | `chore: deletion audit, §E dispositions, Removed section` | D.5 | audit test |
| 7 | `refactor(runtime): split runtime.go into concern files (pure move)` | R.1a | rename-diff |
| 8 | `refactor(pipeline,compiler,replay): split god files (pure move)` | R.1b | rename-diff |
| 9 | `refactor(pipeline): canonical representations — one form per concept` | R.2 | vectors + wire tests |
| 10 | `refactor: delete single-implementation interfaces; concrete internals; typed.go shadowing` | R.3 | race + wire tests |
| 11 | `refactor(runtime): one recovery component; capability by data, not type assertion` | R.4 | characterization |
| 12 | `refactor(runtime): move implementation to internal/runtime (pure move)` | R.5 | wire tests + suite |
| 13 | `test: enforce the layer map; architecture doc in-tree` | R.6 | layering test |
| 14 | `refactor(compiler): one graph walk, one default-target helper` | R.7 | compiler + samples tests |
| 15 | `fix(replay): run step actions outside the store read lock` | C1 | race |
| 16 | `fix(runtime): bound Register critical section; unsubscribe + resource teardown` | C2 | race |
| 17 | `fix(runtime): cancel cron auto-resume on terminal outcomes; deterministic watch timer` | C3-fix | race |
| 18 | `fix(pipeline): release store locks before cross-store copy and checkpoint parse` | C4-fix (M5) | race |
| 19 | `fix(runtime): serialize watch-service resume; cron reads final pause state` | C5-fix (H5, L3) | race + repause bench |
| 20 | `perf(actionlog): append-only entry storage, per-run sharding, legacy dual-read, RunEnumerator` | P4.1 (C3) | bench |
| 21 | `perf(pipeline): guard delta computation; mutator-declared touched keys` | P4.2 (H2) | bench |
| 22 | `perf(nodekit): compile node config once at build time` | P4.3 (H4) | bench |
| 23 | `perf(expr): pooled goja VMs; pre-parsed switch cases` | P4.4 (H3, M4) | bench |
| 24 | `perf(pipeline): stage-level snapshot, inline single-step stages` | P4.5 (H1, M2) | benchstat |
| 25 | `perf(runtime): split runtime lock, index paused runs, unblock ListRuns` | P4.6 (H6) | race + bench |
| 26 | `perf(store): flush persistent state off the data lock` | P4.7 (H7) | race + bench |
| 27 | `perf(replay): index once, deterministic ordered iteration` | P4.8 (H8) | benchstat |
| 28 | `perf: copy-once export, atomic emit slices, bounded outcomes, single update` | P4.9 (M1/M3/M6/M7) | benchstat |
| 29 | `feat(replay): canonical seq-fold; stop writing KindEffectCompleted` | S.1 | golden vectors (coupled) |
| 30 | `refactor(runtime): run status derived from the log` | S.2 | race |
| 31 | `feat(runtime): startup rebuild — restart durability from the action log` | S.3 | race + acceptance test |
| 32 | `docs: architecture map, recovery semantics, README/SPEC truth pass` | P6.1 | doc walk |
| 33 | `chore: changelog finalized; release v2.0.0` | P6.2–P6.3 | full gate |

Ordering constraints: 2–6 (demolition) precede 7–14 (rebuild) so the rebuild moves only live code; 11 (recovery component) precedes 12 (internal/ move) so the component lands in its final home once; 15–19 land after the rebuild so fixes go into split files; 20 precedes 31 (RunEnumerator); 29 must not precede 27 (fold must be indexed and deterministic before semantics change); 31 requires 11 + 20 + 30.

## Risk register

| Risk | Likelihood | Mitigation |
|---|---|---|
| Reorg churn obscures semantic changes in review | High | Move-then-edit discipline (Trap #4); move commits must pass `git diff --find-renames=40%` as near-pure renames; semantic commits are small and reference their audit ID |
| Deleted symbol turns out to have a hidden caller | Low | Pre-production: no external consumers (confirmed 20261003); grep + characterization suite gate every deletion (Trap #5); D.5 audit test |
| `internal/` move breaks deep-path importers | Low | No external importers pre-production; only implementation files move; wire tests + suite gate commit 12 |
| Async-resume change re-breaks chained re-pause (history: `f1e5630`) | High | C5-fix ordering tests first at depth ≥3 against pre-refactor code; land cron-ordering half alone if needed; `[X]` marker allowed with reasoning |
| `KindEffectCompleted` deletion loses replay data | High if sequenced wrong | Coupled commit S.1 with golden vectors swapped in the SAME commit; fold re-keyed in test first, then write + constant removed; S.1 scheduled after H8 so fold is deterministic |
| Action log layout incompatible with old logs | Low (moot) | No production runs exist (Trap #3): legacy format deleted, new layout is the only layout |
| Replay semantics change (first-effect → seq-fold) | Low | No production log consumers; golden vectors re-encoded in the same commit (S.1); decision documented in CHANGELOG; npm package unchanged (frozen) |
| Register restructure introduces Deregister race (lock released before OnRegister) | Medium | Re-validate + idempotent rollback (C2); race detector + lifecycle test |
| Layering test too strict / false positives block unrelated work | Low | Whitelist with per-entry reason required; violations fail with the rule text, not a bare assert |
| Stage snapshot change breaks step isolation assumptions | Medium | Pin current semantics in P0.2 BEFORE P4.5; `stage_snapshot_test.go` extension |
| Compiler walk unification regresses nested-graph compilation | Medium | R.7 late in rebuild; `samples_test.go` + fork/while workflow tests are the net |
| Phase 2 lands, then a Phase 3 fix reveals a structural mistake | Medium | Revert-by-commit granularity; characterization + golden vectors catch drift at the offending commit; the plan explicitly sequences fixes after the rebuild for exactly this feedback loop |
| Benchmarks regress instead of improving on small-state workloads | Low | benchstat per structural commit; revert-by-commit granularity preserved |

## Definition of done

1. Every audit finding has a disposition: fixed, deleted, documented, or `[=]`/`[X]` with reasoning recorded in the respective audit file (§E table carries dispositions from D.5; OA table from R.3).
2. Zero dead symbols and zero deprecated stubs: grep over the §E and OA dead-weight lists returns nothing; no `// Deprecated:` comment exists on the branch (deletion audit test).
3. The Target architecture is real: `internal/runtime` boundary exists; `tests/layering_test.go` enforces the import map with reason-carrying whitelist.
4. ONE code path recovers a run: `internal/runtime/recovery.go`; grep gate — no other file decides rebuild-vs-checkpoint or re-arms timers.
5. Locks never span user code: C1/C2 regression tests green; the rule is documented in `docs/RECOVERY.md` and enforced by the ownership rules table.
6. Restart-durability acceptance test passes: a runtime instance recreated on an existing durable log resumes paused and in-flight runs to their correct terminal state.
7. `go vet ./...` + `go test -race ./...` green at every commit; `make bench` + `make bench-race` green at phase gates; benchstat evidence for every perf commit.
8. Every removed exported symbol is recorded in CHANGELOG `Removed` with rationale; the wire contract (canvas JSON, frozen `.ts` catalog) is untouched, proven by wire round-trip tests.
9. README/SPEC describe only mechanisms that exist; the architecture map in-tree matches `tests/layering_test.go`.
10. Canonical seq-fold replay documented with golden vectors as the semantics reference; the frozen-TS divergence note updated.

