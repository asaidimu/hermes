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
- **Fork-join**: fork nodes compile to pipelines-mode stages with sub-pipelines per branch; compiler validates all branches converge at the same join node; branch stages excluded from flat stage list.
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
| `query` | STUB — requires database resource; returns "not yet implemented" (Phase 5 WIP) |
| `pipeline-ref` | Invoke registered sub-workflow; own fresh state; `initialState` interpolation from parent; result merged under `resultKey`; compile-time config validation against target trigger |
| `fork` | Split execution into parallel branches (N source handles); each branch runs as a sub-pipeline; converges at a single Join node |
| `join` | Synchronization point after Fork; 1 target, 1 source; waits for all fork branches to complete before advancing |
| `distribute` | Parallel for-each: execute the body concurrently for each element in an array; each iteration gets its own sub-pipeline with the element injected; results merged under `resultKey` |

Node definitions carry: kind, label, description, ConfigSchema (anansi JSON), Handles (+ `HandlesJS` parity for the client), Run, Router/RouterFunc, BodyHandle, ValidateConfig, **Requirements**.

### Generic node configs (`pkg/nodekit/typed.go`)

Node definitions are **generic over their configuration structs**. One struct per node kind — the struct IS the schema:

- `Define[C any](TypedDefinition[C])` → erased `NodeDefinition` ready for `Register`.
- **Schema derived once** at registration via `data.ExtractDTOSchemaDirectWithTag((*C)(nil), "config")` — no hand-written `ConfigSchema` JSON needed.
- **Binding**: raw config map → `document.NewRecordView(map).BindToTag(&cfg, "config")` — anansi binder fills the struct.
- **Run/Router/BodyHandle callbacks** receive `*TypedRunContext[C]` with typed `Config *C` — no more `cfg["method"].(string)` extraction.
- Struct tags drive everything: `config:"fieldName"` for name resolution, `anansi:"required=true,default=GET"` for metadata.
- `ValidateConfig func(*C) error` for custom validation beyond derived schema.
- Handles callbacks receive `*C` — dynamic handles (switch/cases) read typed fields directly.
- Zero node changes required: engine never sees `C`, only the erased `NodeDefinition`.

Convention: `config:"fieldName"` for anansi binding + `anansi:"required,default,nullable,type,values"` for metadata. The struct replaces: hand-written ConfigSchema JSON, manual field extraction in Run/Router, ValidateConfig functions.

### Node requirements (env/secrets)

Nodes declare external capabilities via `NodeDefinition.Requirements []Requirement` (`Kind: env|secret`, `Key`, `Required`, `Description`):

- The **compiler aggregates** them onto `Workflow.Requirements` (deduped by kind+key).
- The **runtime validates before registration/run**: required env keys must exist in `Options.Env`; required secrets must satisfy `Options.Secrets.Has`. Unsatisfied → registration refused with a descriptive error listing missing keys. Hosts can pre-check via `rt.ValidateWorkflowRequirements(wf)` at definition-save time.
- At execution, steps read values from context: `NodeRunContext.Env map[string]any` and `NodeRunContext.Secret(key)`. Secret values are function-resolved — they never persist into state, checkpoints, or events.
- `SecretProvider` interface (`Get`/`Has`) is defined hermes-side; hosts (e.g. hestia) implement it against their credential store.

Design rule: **configs carry references; context carries credentials.** Secrets must never appear in node configs, which persist verbatim inside workflow definitions.

## 5. Nodekit (`pkg/nodekit`)

- Registry (`Register`/`Get`/`Registry`) of node definitions.
- **Config pipeline**: deep interpolation of `${state.path}` style expressions against live state per execution (loops see updated state), then anansi-schema coercion + defaults.
- `BuildStep` / `BuildStage` / `BuildBoundedStage` / `BuildDistributeStage` / `BuildRouter` / `buildRouterFunc`: compile node defs into engine primitives.
- `DynamicPipelines func(state map[string]any) []PipelineDefinition`: stage field for runtime-generated sub-pipelines (used by distribute). If set, the engine calls it instead of using static `Pipelines`.
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
2. **HTTP SSRF guard is regex-based** — DNS-rebinding and redirect-following not covered.
3. **InMemorySchedulerReplace flake** — timing-sensitive scheduler test (passes solo; fails under `-count>1` sometimes).
4. **Review @notes** — `@note #review-*` markers across the codebase track open P1–P3 findings (silent error discards, interface nits, concurrency comments).
5. **Single-process scope** — memory timeline/scheduler/bus; persistence only covers run state, not timeline events or registry.
6. **No auth/TLS** on the REST surface.
7. **While-loop safety** relies on ctx cancellation; no iteration cap.
