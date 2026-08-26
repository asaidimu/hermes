# Hermes — MVP Feature Inventory

Reference for code review. Every claim is verifiable against the cited package.
Scope: what ships today; explicit non-goals at the end.

## 1. Architecture

Polyglot workflow engine:

- **Go runtime** (`pkg/…`) — compiles node graphs into stage pipelines and executes them.
- **TS SDK** (`src/`, published as `@asaidimu/hermes`) — node catalog (`NODE_DEFS`, `HANDLES`, `CATALOG`), wire types (`types.ts`), graph serialization (`serialize.ts`). Shared contract with the Go compiler's wire format (`pkg/server/server.go` `wireNode`/`wireEdge`).
- **Anansi** (`github.com/asaidimu/go-anansi/v8`) — schema-driven persistence layer backing run-state durability.

```
graph (nodes+edges) → compiler → pipeline.PipelineDefinition
                    → runtime.WorkflowRuntime → RunContextImpl.Run()
                    → stages → steps/subpipelines → store → events/timeline
```

## 2. Compiler (`pkg/compiler`)

- Compiles `{nodes, edges}` wire graphs into `PipelineDefinition`s keyed by trigger ID.
- **BFS stage ordering** from each trigger; flat stage list with jump-based routing.
- **Routing children** (if / switch / while / for-each): child node becomes the stage router via `nodekit.BuildRouter`; container nodes adopt their routing child's router.
- **Bounded nodes** (try-catch): body-handle edge compiles a `<id>__body` subpipeline with optional setup step; results routed by the node's pipelines-router.
- **Pipeline-ref nodes**: referenced sub-workflows compile as nested `PipelineDefinition`s with `resultKey` result merging.
- Compile-time config validation via anansi schema `ValidateConfig` (full validation, coerces defaults).
- Compile errors for missing edges (e.g., bounded node without body edge), unknown kinds, unresolvable targets.

## 3. Pipeline engine (`pkg/pipeline`)

- **Stages** run in steps-mode or pipelines-mode (pipelines win when both present).
- **Steps within a stage** execute concurrently; each step gets retries + per-step timeout; failures aggregate ("N step(s) failed in <label>:<id>").
- Step mutators commit atomically via one `store.Update` after the stage's steps settle.
- **Routing instructions**: `Advance`, `Jump(stageID)`, `JumpTo(EntryAddress)` (stage+step+subpipeline index), `Terminate`, `Pause`.
- Routers receive a deep-copied state snapshot (`stateSnapshot`); engine evaluates instruction → advance/jump/terminate/pause.
- **Pause**: checkpoint written through the store (`WriteCheckpoint` inside `store.Update` — locking + durability), optional full state `Snapshot`, resume address persisted under `__pipeline_data__.checkpoints[pipelineID]` in state.
- **Subpipelines** (`ExecuteSubPipelines`): fork/join semantics, join events, per-child fresh stores (`NewFreshStore`), result merging into parent under `resultKey[:childID]`, nested pause bubbling with sub-addresses.
- Events emitted at every boundary: `pipeline:start/success/failure/pause`, `stage:start/success/failure`, `step:start/success/retry/failure`, `subpipeline:fork/join`, `router:evaluated`.

## 4. Node catalog (`pkg/nodes`)

| Kind | Capabilities |
|---|---|
| `trigger` | Seeds initial state; string coercion of initialState (bool/number passthrough) |
| `arithmetic` | add/subtract/multiply/divide/modulo/power/min/max; operands as literals or state refs; dotted result keys |
| `code` | JS via goja sandbox (`expr.RunSandbox`): script receives `state`, returns patch object; ctx-cancelled interpreter (loop guard) |
| `transformer` | ~20 actions incl. EXTRACT, MAP_FIELD, FILTER_LIST, APPEND_LIST, COUNT, CONCAT, CASE_TRANSFORM (upper/lower), COALESCE, MERGE_OBJECTS, FLATTEN_OBJECT, GROUP_BY, KEY_BY, CAST_TYPE, DEFAULT_IF_EMPTY, SET_VALUE, FORMAT_DATE, DATE_DIFF, DATE_ADD, SORT_LIST, UNIQUE_LIST, SLICE_LIST, DELETE_FIELD |
| `if` | true/false handles; predicate DSL (`equals`, `not_equals`, `greater_than`, `less_than`, `greater_equals`, `less_equals`, `contains`, `starts_with`, `ends_with`); legacy key/predicate/value or condition-object config |
| `switch` | multi-way branch on value/handle |
| `while` | condition loop (simple predicate or condition object); re-evaluates per iteration via router |
| `for-each` | array/object iterator state machine at `__$<nodeId>__items__`; do/done handles |
| `delay` | wall-clock ms sleep in `Run`; cron pause via `PauseForCron("__cron_delay__", …)`; no-cron follows outgoing edge, terminates at leaves |
| `pause` | pause-until-event via WatchService registration; onResume/onTimeout handles; any/all modes; timeout ms |
| `try-catch` | bounded body execution; error capture to state (`SystemErrorJSON`); retry routing back into body |
| `http` | GET/POST/etc.; headers/params arrays; body; responseType json/text; throwOnError (default true); timeoutMs (default 30s); output under `http_<nodeId>` or custom key; SSRF guard blocks private/loopback IP literals |
| `gemini` | Google Gemini generateContent call: apiKey, model (default gemini-2.5-flash), prompt, systemInstruction, temperature, jsonMode; output under `gemini_response` or custom key |
| `query` | STUB — requires database resource; returns "not yet implemented" (Phase 5 WIP) |
| `pipeline-ref` | Invoke registered sub-workflow; own fresh state; `initialState` interpolation from parent; result merged under `resultKey`; compile-time config validation against target trigger |

Node definitions carry: kind, label, description, ConfigSchema (anansi JSON), Handles (+ `HandlesJS` parity for the client), Run, Router/RouterFunc, BodyHandle, ValidateConfig.

## 5. Nodekit (`pkg/nodekit`)

- Registry (`Register`/`Get`/`Registry`) of node definitions.
- **Config pipeline**: deep interpolation of `${state.path}` style expressions against live state per execution (loops see updated state), then anansi-schema coercion + defaults.
- `BuildStep` / `BuildStage` / `BuildBoundedStage` / `BuildRouter` / `buildRouterFunc`: compile node defs into engine primitives.
- `PatchMutator` + `ApplyPatch`: flat dotted-key patches, ExpandPatch → MergeMaps (arrays replace, objects merge recursively, `nodekit.Delete` sentinel removes keys).
- `Lookup(state, "a.b.c")`: dotted-path read.
- Resource resolution: dependency edges → `resource:<sourceNodeId>` handles injected into NodeRunContext.
- `ValidateConfig` on NodeDefinition: full anansi validation at compile time.

## 6. Runtime (`pkg/runtime`)

- **WorkflowRuntime**: registers workflows (compiled or raw graphs), routes trigger events to pipelines, spawns runs.
- **Options**: Bus, StoreFactory (`func() (Store, error)`), StoreLoader (`func(runID) (Store, error)`), Timeline, Logger, Env, Services, EventSource, Scheduler, Mode (execution gate: concurrency semaphore, default 10).
- **Triggering**: event-bus dispatch (spawnRun, async) and direct `Invoke` (sync).
- **Run lifecycle**: `RunHandle` (OnPrepare/OnComplete hooks, Timeout), outcomes map, stores map, active-run registry.
- **Pause/resume**: pausedRun bookkeeping; single-event and multi-event (any/all) waits with `ReceivedEvents` tracking; timeout handling; cron auto-resume via Scheduler; WatchService pre-pause buffering resumes immediately if an event arrived before pausing.
- **Abort**: bus subscription (`run:abort`) → AbortRun cancels active context; aborted results distinguished from failures.
- **Crash recovery**: `resumeFromPersistence(runID)` loads store via StoreLoader, reads `__run_meta__` linkage (workflowId/triggerId seeded at spawn) for direct workflow resolution (legacy scan fallback), rebuilds checkpoint, delegates to Resume.
- **EventSource IoC**: pluggable external-event wiring; `ManualEventSource` default.
- Timeline recording attached per run when Timeline configured.

## 7. Persistence & run identity (`pkg/store`)

- **Run identity = state document identity**: minted in memory via anansi's `document.New(&PipelineState{})` (UUIDv7 `_id_`), zero DB round-trips; first write-through inserts lazily preserving the pre-minted ID; recovery looks up by `_id_ == runID`. System owns `_id_`/`_metadata_` — hermes never writes them.
- **MemoryStore**: plain `map[string]any` + UUIDv7 id; RWMutex; Read (live view under lock) / Update (mutator) / ExportJSON / Clone. Zero anansi dependency.
- **PersistentStore**: write-through over `anansi ModelCollection[*PipelineState]`; translates flat engine state ⇄ typed row (`state` column = pipeline state, `metadata` column = run linkage `RunMetadata{workflowId,triggerId,pipelineId}`); updates by `_id_` filter; Clone preserves identity + insertion state.
- **Engine/store contract**: state is plain `map[string]any` end-to-end (`Mutator func(map[string]any) error`); documents exist only inside the persistence boundary.
- **Checkpoints** live in state under `__pipeline_data__` (persisted inside the `state` column) — not in system metadata.
- **AnansiStoreFactory**: derives runs-collection schema from the `PipelineState` struct (`ExtractDTOSchemaDirect`), provisions collection if absent, configures anansi document-factory singleton; exposes `Create()` (mint) / `Load(id)` (recover) + `Mint()`/`Loader()` adapters for runtime Options.

## 8. Events (`pkg/events`)

- `MemoryScopedBus` over go-events: hierarchical `EventPath` scoping (`Scope(prefix)` — child buses bubble to parents), wildcard subscription (`"*"`), unsubscribe handles.
- `PipelineEvent`: run/pipeline IDs, path, payload, duration.

## 9. Scheduling (`pkg/scheduler`)

- `Scheduler` interface: `Schedule(id, cron, callback)` / shutdown; in-memory implementation used for delay-node cron pauses and recurring triggers.

## 10. Watch service (`pkg/watch`, `pkg/runtime/watchservice.go`)

- `WatchDescriptor{EventTypes, Mode(any|all), Timeout}` registrations per run.
- Pre-pause buffering: events arriving between Register and OnRunPaused resume immediately.
- Payload conditions (`Field/Op/Value`) matched against events; matched events produce `WatchEvent{Payload, Patch}`; Patch merged into state on resume.
- Timeout watchdogs emit `__resume_reason__: timeout` patches (pause node routes to onTimeout).

## 11. Timeline (`pkg/timeline`)

- `TimelineRecorder` attaches to a run's bus + store, persisting events.
- `TimelineStore` interface with memory implementation; snapshots + deltas strategy.
- `GetRunMeta`, `GetEvents(fromSeq,toSeq)`; `TimelinePlayer.Seek(seq)` reconstructs state snapshot+deltas replay (time-travel debugging).

## 12. HTTP API (`pkg/server`) + facade (`pipelines.go`)

REST surface (net/http stdlib mux, CORS enabled):

| Route | Purpose |
|---|---|
| `POST /run` | Submit raw `{nodes, edges}` graph; compiles, registers, runs, awaits outcome |
| `POST /compile` | Compile-only (validation/dry run) |
| `GET /registry` | Node catalog for clients |
| `GET /handles.js` | JS handle specs for the editor |
| `POST /register` / `POST /deregister` | Long-lived workflow registration |
| `POST /events` | Emit trigger events |
| `GET /runs` | List runs |
| `GET /runs/{id}/outcome` · `/events` · `/store` | Poll outcome, event log, final state |
| `POST /runs/{id}/abort` | Abort a run |

`pipelines.go` re-exports core types as a stable embedding surface.

## 13. Expression sandbox (`pkg/expr`)

- goja-based: `EvalBody` (condition bodies), `EvalValue`, `RunSandbox` (code node function-sandbox with `state` global), `ValidateExpression`, `StatePathExpr` (safe `state["a"]["b"]` path rendering).
- Runtime bound to context cancellation — infinite loops die with stage timeout/abort.

## 14. Registry (`pkg/registry`)

- Active-run tracking: Register/Get/Deregister/List, MarkPaused with expiry callbacks, FastPathResume for in-memory paused runs.

## 15. Testing inventory

- Unit tests per node (arithmetic/code/delay/foreach/http/if/pause/query/switch/transformer/trigger/try-catch/while).
- Compiler tests (routing children, containers, samples).
- Runtime tests (pause/resume incl. payloads & hard failures, watch flows).
- Integration suite (`tests/`): event wiring, pause/resume, subpipelines (incl. stress fan-out), timeline playback.
- Store round-trip test against real in-memory sqlite via anansi (mint → seed/checkpoint/linkage → reload-by-ID → update-in-place, no duplicate rows; row shape assertions).
- End-to-end calc sample run (while loop → delay → if chain).

## Known MVP gaps / review focus

1. **Query node stub** — needs database resource wiring (Phase 5).
2. **Gemini apiKey in config** — secret handling should move to resources/env before production.
3. **HTTP SSRF guard is regex-based** — DNS-rebinding and redirect-following not covered.
4. **InMemorySchedulerReplace flake** — timing-sensitive scheduler test (passes solo; fails under `-count>1` sometimes).
5. **Review @notes** — `@note #review-*` markers across the codebase track open P1–P3 findings (silent error discards, interface nits, concurrency comments).
6. **Single-process scope** — memory timeline/scheduler/bus; persistence only covers run state, not timeline events or registry.
7. **No auth/TLS** on the REST surface.
8. **While-loop safety** relies on ctx cancellation; no iteration cap.
