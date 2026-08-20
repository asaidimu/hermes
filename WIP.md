# WIP — Hermes Parity Plan

Polyglot restructure of the Go RSP engine (`hermes`) to reach full parity with the
TS `@asaidimu/workflows` package and the hedwig flows client, with a new npm package
`@asaidimu/hermes` published from the repo root.

The packages in the `utils` monorepo will be **deprecated once parity is achieved**;
`hermes` (Go) + `@asaidimu/hermes` (JS) become the new source of truth.

---

## Target repo layout (`~/projects/hermes`)

```
hermes/
  go.mod  go.sum  pipelines.go
  package.json                  # name: @asaidimu/hermes  <- published from ROOT
  tsconfig.json
  .gitignore                    # add: dist/, node_modules/, src/generated.ts
  src/                          # shared TS (package's own sources)
    types.ts                    #   HandleSpec, NodeCatalogEntry + wire types
    serialize.ts                #   buildHandlesJS(), buildRegistryJSON()
    index.ts
  scripts/
    build.ts                    # npm toolchain aggregator (bun/esbuild)
  pkg/
    nodekit/   nodekit.go       # shared node types + registry (cycle-free)
    nodes/
      nodes.go                  # aggregator: imports every node pkg, registers
      trigger/     trigger.go trigger.ts
      if/          if.go if.ts
      arithmetic/  arithmetic.go arithmetic.ts
      code/        code.go code.ts
      delay/       delay.go delay.ts
      for-each/    for-each.go for-each.ts
      gemini/      gemini.go gemini.ts
      http/        http.go http.ts
      query/       query.go query.ts
      switch/      switch.go switch.ts
      transformer/ transformer.go transformer.ts
      try-catch/   try-catch.go try-catch.ts
      while/       while.go while.ts
      database/    database.go database.ts        # resource
    compiler/  runtime/  pipeline/  server/  store/  events/  timeline/  registry/
  tests/  examples/
```

**Why `pkg/nodekit`**: each `pkg/nodes/<kind>` imports `nodekit` for
`NodeDefinition`/`HandleSpec`/`NodeRunner`; the aggregator `pkg/nodes` imports every
`pkg/nodes/<kind>` to register their `var Node`. Subpackages never import `pkg/nodes`,
so there is no import cycle. The `pipelines.go` facade keeps working.

Each language's toolchain aggregates its own source:
- **Go**: `go build ./...` naturally compiles every `*.go`; `pkg/nodes` imports each
  node subpackage so their registrations run in `init()`.
- **TS**: `scripts/build.ts` globs `pkg/nodes/*/*.ts` and aggregates them into the
  published package + generated wire assets.

---

## The `@asaidimu/hermes` npm package (root)

- `package.json` at repo root, `files: ["dist"]`, bundled via **esbuild** with `tsc`
  for `.d.ts`. Coexists with `go.mod`.
- `src/types.ts` — canonical wire/type layer: `HandleSpec`, `NodeCatalogEntry`,
  `WorkflowNode`/`WorkflowEdge`, `PipelineEvent`, `RunOutcome`, `RunTimelineMeta`,
  `WorkflowTrigger`, `WorkflowState`. (Hedwig adopts these after parity.)
- Per-kind `pkg/nodes/<kind>/<kind>.ts` exports **`nodeDef`**:
  `{ kind, label, description, configSchema, scope?, bodyHandle?,
     type: "executable" | "resource", handles(config), defaults }`
  — mirroring the exact handle specs in the current utils node defs:
  - static for most kinds;
  - `switch` parses `config.cases` dynamically;
  - `query` carries the `service` resource target handle;
  - `database` carries the `db` resource source handle.
- `scripts/build.ts` (the TS toolchain aggregator):
  1. globs `pkg/nodes/*/*.ts` -> writes `src/generated.ts` (imports each `nodeDef`,
     aggregates `NODE_DEFS` + `HANDLES`); `src/generated.ts` is gitignored.
  2. bundles `dist/` for publish (`Bun.build` esm + cjs, `--dts` types).
  - `serialize.ts` exports `buildHandlesJS` / `buildRegistryJSON` helpers for any
    consumer that wants to host the definitions (e.g. a server); not wired into Go.

---

## Go integration with the package

- The Go binary is **fully self-contained**: `go build ./...` requires no JS
  artifacts, no generated files, and no npm step. JS definitions exist only in the
  `@asaidimu/hermes` npm package; consumers (e.g. hedwig) install it for handles and
  catalog metadata.
- No `//go:embed`, no committed `assets/`, no regenerate step.
- Each node declares its `ConfigSchema` as a **`json.RawMessage` anansi schema**
  (`nodekit.NodeDefinition`), compiled through the anansi schema compiler
  (`nodekit.CompileConfigSchema` → `definition.FromJSON` + `definition.Compile`).
  Field-type coercion and defaults derive from the compiled `ResolvedSchema` — no
  hand-rolled field parser. Free-form object fields use `record` so they compile.
  The TS `configSchema` in `pkg/nodes/<kind>/<kind>.ts` is the authoritative
  editor/catalog source and must stay in sync with the Go schema JSON.

---

## Runtime parity work

### 1. `pkg/nodekit` + `pkg/nodes` restructure
Move the current `pkg/nodes/nodes.go` shared types (HandleSpec, NodeDefinition,
NodeRunner, NodeRouter, registry) into `pkg/nodekit`; split node implementations into
per-kind packages; `go build ./...` green.

### 2. Go node implementations (per kind)
| Kind | Behavior |
|---|---|
| `trigger` | initial-state coercion (string->bool/number), emits as patch; trigger def on MANUAL_EVENT (`__manual__`) |
| `arithmetic` | `Number()` coerce + 8 ops + key/div-zero errors |
| `delay` | `time.Sleep(config.ms)` |
| `transformer` | all 26 transform actions incl. date/sort/group ops |
| `http` | `net/http` with SSRF private-IP block, timeout, headers/params/body, responseType, `http_<nodeId>` key fallback |
| `gemini` | `generateContent` API call, jsonMode, usage metadata |
| `code` | goja sandboxed user code (with interrupt/timeout) |
| `if` | conditions + AND/OR combinators via goja; legacy key/predicate/value path |
| `switch` | goja-eval `value` + JSON cases match -> handle id |
| `for-each` | internal `__$<nodeId>__items__` iterator state machine, `itemKey` patch, `do`/`done` router |
| `while` | simple/complex predicate via goja, `do`/`done` router |
| `try-catch` | `bodyHandle: "try"`, bounded builder -> body subpipeline, `errorKey` capture, `catch`/`done` router |
| `query` | `find`/`filter`/`create`/`update`/`delete` against resource collection, JSON-parse `query`/`data` |
| `database` (resource) | `init`/`cleanup` wrapping a go-anansi collection/store, `resource:*` events |

Config interpolation runs per node (`deepInterpolate` of `{{ state.* }}`,
`{{ $res.* }}`, `{{ $results.* }}`) before each run/router, mirroring
`schema.ts`.

### 3. `pkg/expr` — goja evaluator
- Wrapper that injects state as a JS object and evaluates
  `(function(state){ return (<expr>); })` for if/while/switch simple + complex modes
  and the sandboxed code body for the `code` node.
- `SetInterruptHandler` + timeout/abort wiring so workflow abort/interrupts stop
  evaluation (replicates AbortSignal).
- Result conversion: goja `Export()` -> Go `any`; object -> patch map.

### 4. `pkg/compiler`
Port `compiler.ts`:
- Node/edge parsing (`byId`, `childrenOf`, `parentId`, `flowEdges`,
  `dependencyEdgesTo`), `makeResolveHandle`, `nextDefaultTarget`.
- `bfsStages` + `bfsBodyNodes` (respect `bodyHandle`, routing-def fan-out, skip
  resource/parented/output nodes).
- `compileStages` -> `[]pipeline.Stage`:
  - container ("stage") -> grouped child steps + <=1 routing child
  - "pause" node -> validation error (matches TS)
  - "pipeline-ref" -> `Stage.Pipelines` + router
  - bounded (`bodyHandle`) -> `Stage{Pipelines:[body]}` + `PipelinesRouter`
    (loop-back -> `Jump(boundedID)`, else resolve handle)
  - routing node -> single-step stage with router
  - plain node -> single-step stage, default router (terminate-on-error unless
    `__terminateOnFailure__: false`)
- Trigger collection -> `Workflow{ID, Label, Triggers, Pipelines, Services}`.

**Status: done.** `pkg/compiler` (+ `pkg/pipeline/workflow.go`, `pkg/nodekit/build.go`)
implemented and green; see `todo/phase4-compiler.md`. `NodeRunContext.Store` and
`PipelineContext.ResolveResource` are wired so the Phase 5 runtime can inject
run-scoped resource handles into steps.

### 5. `pkg/runtime`
Port `engine.ts` + `@core/runtime` orchestration:
- `WorkflowRuntime`: event bus, store registry (runID -> store), timeline, outcome map.
- `Register(workflow, opts)` — subscribe each trigger event with predicate; on fire ->
  fresh store, `RunContextImpl`, attach `TimelineRecorder`, background `Run`, record
  outcome.
- `Run(nodes, edges)` — compile -> register -> emit MANUAL_EVENT -> resolve `{runId}`
  via onPrepare (the TS run path).
- `AbortRun` (ABORT_EVENT -> `RunContext.Abort`), `GetRunOutcome`, `ListRuns`.
- Resource manager: init resource handles at run scope, inject `resources[kind]` into
  steps, cleanup on complete.

**Status: done.** `pkg/runtime` (+ resource-resolver plumbing through the engine,
`core.SystemErrorJSON` for `*:failure` payloads, abort-wired cancellable run context)
implemented and green; see `todo/phase5-runtime.md`. Engine resource keys
(`resource:<nodeId>`) resolve into initialized handles via the run context resolver
(`resolveStepResources` -> `pcxt.ResolveResource`), and `Run(nodes, edges)` is the
Phase 7 server entry point.

### 6. Event wire parity (client-visible runtime contract)
The client's `translations.ts`/`context.tsx` reconstruct node statuses and log rows
from these payloads; currently missing/wrong in `pkg/pipeline`:
- `stage:start` -> add `mode: "steps" | "pipelines"` (currently emits `stepCount`).
- `stage:success` -> add `nextInstruction`.
- Emit `router:evaluated` (`instruction`, `interpretation`) — not emitted today.
- Emit `subpipeline:fork` / `join` (`subPipelineIds`, `results`).
- Emit `resource:init/ready/init:failure/cleanup/cleanup:failure`.
- Ensure `error.message` is present for all `*:failure` (client reads
  `payload.error.message`).
- Fix `subpipeline.go`: child runs use `childDef.ID` as runID — must bubble with the
  parent runID.
- Runtime semantic fix: subpipeline failure must not hard-abort the parent; errors
  flow into `PipelinesRouter` so try-catch can catch (mirrors TS `errors` record).

**Status: done.** `pkg/pipeline` Run loop now emits the full wire (`stage:start` mode,
`stage:success` nextInstruction, `router:evaluated` instruction/interpretation,
`subpipeline:fork/join`); `pkg/runtime` emits resource lifecycle events with the TS
payloads; `ExecuteStageSteps` aggregates failures with the TS message format; child
subpipelines bubble under the parent runID and failures flow into the router instead of
aborting the parent. Verified green (build/vet/test -race, bun build, tsc) with
`tests/eventwire_test.go` + `TestResourceLifecycleEvents`; see `todo/phase6-event-wire.md`.

### 7. Thin server adapter
Adapt existing `pkg/server`:
- `POST /run` accepts `{nodes, edges}` -> `runtime.Run` (currently expects
  `{pipelineId}`).
- Add CORS (`Access-Control-Allow-Origin: *` + OPTIONS) — currently absent, would
  block the hedwig dev client.
- Runtime endpoints only: `/run`, `/runs`, `/runs/:id`, `/outcome`, `/events`,
  `/store`, `/abort`. No `/handles.js` / catalog endpoints — the client gets those
  from the `@asaidimu/hermes` package. *(Revised mid-plan: per user directive the
  `~/projects/utils/src/workflows/server.ts` contract IS the spec, so the Go
  server now also serves `/registry`, `/handles.js`, `/compile`, `/register`,
  `/deregister`, `/events`; see Phase 8.)*

**Status: done.** `pkg/server` is now a thin runtime adapter: `POST /run` accepts
the TS wire graph (`{nodes, edges}` with `data.kind`/`data.config`/`data.role`),
returns `{runId}` via `OnPrepare`, and run data is served from the runtime
(`/runs`, `/runs/:id`, `/outcome`, `/events`, `/store`, `/abort`); CORS enabled.
`examples/server/main.go` and `tests/frontend_api_test.go` updated for the graph
API; runtime gained `GetRunMeta`/`GetEvents`. Verified green; see
`todo/phase7-server.md`.

### 8. Verification
- Port the 3 TS vitest suites to Go table-driven tests: `compiler.test.ts`,
  `runtime.test.ts`, `database-workflow.test.ts`.
- End-to-end test running the client's `calc` sample graph (trigger -> query ->
  database resource) asserting document state, event sequence, and outcome.
- `go build ./...`, `go vet ./...`, `go test -race ./...`.
- Manual: run Go server on `:3001`, point hedwig at it, run the sample flow.

**Status: done.** All 22 `compiler.test.ts` cases map 1:1 to
`pkg/compiler/compiler_test.go`; every `runtime.test.ts` scenario is covered in
`pkg/runtime/runtime_test.go` (completion/no-[object Object], abort, step:failure
error serialization, resource lifecycle). `database-workflow.test.ts` is held per
user directive (database resource is metadata-only, query Run is a guarded stub).
The calc sample flow is verified end-to-end via the live server smoke test
(trigger -> outcome/events-with-path/store). Full gate green: `go build ./...`,
`go vet ./...`, `go test ./...`, `go test -race ./...`, `bun run build`,
`tsc --noEmit`; see `todo/phase8-verification.md`.

**Phase 8.5 — server surface parity (server.ts contract):** per user directive,
`~/projects/utils/src/workflows/server.ts` is the spec. Added to `pkg/server`:
`GET /registry` (nodekit registry JSON), `GET /handles.js` (`{ kind: (config) =>
<specs> }` JS literal; hand-written switch handler mirroring
`pkg/nodes/switch/switch.ts`), `POST /compile` (JSON-safe metadata view),
`POST /register` (raw nodes/edges -> compile + register), `POST /deregister`,
`POST /events` (bus emit). `TimelineEvent` now carries `path` for client
translations. Live smoke test verified all endpoints on `:3001`.

---

## Locked decisions
- **goja** (`github.com/dop251/goja`) for the `code` node + if/while/switch expression
  evaluation.
- **Database resource -> go-anansi collections** (functional parity for the sample
  workflow).
- **Runtime-first**: runtime/compiler/nodes parity is the priority; the HTTP adapter
  is a thin final layer.
- **`@asaidimu/hermes`** = handles + catalog + wire types (single package owns all
  JS/wire artifacts).
- **No embed / no committed assets.** The npm package is the only source of JS definitions; Go is fully self-contained.
- **esbuild** bundler (with `tsc` types) for the npm package.

## Not in this package / out of scope for now
- The server API client (fetch-based) — implemented later.
- UI components/editors — stay in hedwig's flows.
- Runtime execution semantics (run/router/goja) — Go-side.

## Deprecation path
`@asaidimu/workflows` + related `utils` packages stay until parity lands; then:
- `@asaidimu/hermes` becomes the JS-side source of truth.
- Hedwig imports `RunTimelineMeta`, handles, and catalog from `@asaidimu/hermes` and
  points at the Go server (drops the `/handles.js` runtime fetch).
- `utils` packages are deprecated.