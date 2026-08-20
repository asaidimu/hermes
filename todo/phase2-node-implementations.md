# Phase 2 — Go node implementations (Run/Router) + expr

**Context:** Phase 1 (polyglot scaffold) is complete: `pkg/nodekit` holds shared node
types, `pkg/nodes/<kind>/` holds polyglot pairs (`.go` + `.ts`), `ConfigSchema` is a
`json.RawMessage` anansi schema compiled via `nodekit.CompileConfigSchema`
(`definition.FromJSON` + `definition.Compile`), and `go build`/`go vet`/tests are
green. This phase gives every executable node its `Run`/`Router` and implements the
goja evaluator. Parity target: `~/projects/utils/src/workflows/nodes/*/index.ts`.

Execution contract to mirror (from `schema.ts` `buildStep`/`buildRouter`):
- config is `coerceConfig`d from configSchema defaults then `deepInterpolate`d
  (`{{ state.* }}`, `{{ $res.* }}`, `{{ $results.* }}`); interpolated AFTER coercion.
- `run` reads `state` (full document map), returns a flat patch `{ dottedKey: value }`
  applied via `document.Set` (dotted-path aware); `results` live at `state.results`.
- `router` returns a handle id resolved by the compiler to a target stage.

---

^- [*] Extend `pkg/nodekit` NodeRunContext for full parity inputs
  - **Context:** Current context only has NodeID/Config/Document/Resources/Logger.
    Nodes need `state` (map), `results` (map), `errors` (map), and a write path for
    routers (try-catch router writes `errorKey`).
  - **Details:** Add `State map[string]any`, `Results map[string]any`,
    `Errors map[string]any`, and `Store store.Store` (or a `Write(map[string]any)`
    helper). NodeRunner/NodeRouter signatures unchanged. `BuildStep` populates the
    new fields from the pipeline context/document; routers that write go through
    the store.
  - **Files:** `pkg/nodekit/nodekit.go`

^- [*] Add `pkg/expr` — goja evaluator
  - **Context:** The `code` node, if/while/switch simple+complex conditions, and the
    legacy if key/predicate/value path all evaluate JS against `state`. Parity:
    `new Function("state", "return (expr);")` and the sandbox wrapper.
  - **Details:** `expr.Eval(ctx, code string, state map[string]any) (any, error)`
    wrapping `(function(state){ <code> })`; `expr.Condition(...)` building the
    predicate/operator eval strings (===, !==, >, <, >=, <=, includes, startsWith,
    endsWith) with the utils `String(x).includes(...)` semantics; `expr.RunSandbox`
    for the code node (injects state clone, masks fetch/window/global). Set
    `SetInterruptHandler` + honor ctx cancellation for abort/timeout.
  - **Files:** create `pkg/expr/expr.go` (+ `expr_test.go`)
  - **Dependency:** `github.com/dop251/goja` (add to `go.mod`)

^- [*] Implement `trigger`, `arithmetic`, `delay`, `code` nodes
  - **Context:** Straightforward linear nodes. Trigger: coerce `initialState`
    string->bool/number (utils `run`), emit patch. Arithmetic: `Number()` operands,
    8 ops, errors on missing key / non-numeric / div/mod zero. Delay:
    `time.Sleep(ms)` respecting ctx. Code: `expr.RunSandbox`, return object patch.
  - **Files:** `pkg/nodes/{trigger,arithmetic,delay,code}/*.go`

^- [*] Implement `if`, `switch`, `while`, `for-each` nodes (run + router)
  - **Context:** Routing nodes. If: conditions array + combinators, legacy
    key/predicate/value, complex `condition` string via goja. Switch: eval `value`,
    JSON cases match -> case id, else `defaultHandle`. While: simple/complex
    predicate via goja -> "do"/"done". For-each: iterator state machine at
    `__$<nodeId>__items__` (array/object entries), patch `itemKey`, router -> do/done.
  - **Details:** All evaluation via `pkg/expr`; routers return handle ids
    (`if`/`else`, `do`/`done`, case id / defaultHandle).
  - **Files:** `pkg/nodes/{if,switch,while,for-each}/*.go`

^- [*] Implement `try-catch` node
  - **Context:** bodyHandle "try"; run + router aggregate `errors` into a single
    SystemError JSON at `errorKey` (single vs parallel-tracks messages), router
    writes it to store and returns "catch" / "done". Bounded-stage wiring (body
    subpipeline) is the compiler's job (Phase 4); here implement run/router against
    the errors record.
  - **Files:** `pkg/nodes/try-catch/trycatch.go`

^- [*] Implement `http` and `gemini` nodes
  - **Context:** External calls. http: SSRF private-IP block, params/headers,
    auto content-type for JSON bodies, timeout via ctx, `key` fallback
    `http_<nodeId>`, responseType json/text; store `{data,status,statusText,headers}`.
    gemini: `generateContent` POST, jsonMode parse, usage metadata, key default
    `gemini_response`.
  - **Files:** `pkg/nodes/{http,gemini}/*.go`

^- [*] Implement `transformer` node
  - **Context:** Apply rule list to state: DELETE_FIELD pending deletes, otherwise
    `getValueByPath` -> `executeTransform(action, value, actionParam, state, target)`
    -> merge patch. Port all 26 transform actions from
    `~/projects/utils/src/workflows/nodes/transformer/transforms.ts` (incl. date /
    sort / group ops).
  - **Files:** `pkg/nodes/transformer/transformer.go`, `transforms.go`

^- [*] Implement `query` node (database resource HELD)
  - **Context:** Runtime parity for the `calc` sample. The `database` resource is
    **on hold** (not fully thought through) — its underlying go-anansi store
    wiring and `ResourceInit`/`ResourceEnd` are deferred to Phase 5. The `query`
    node's Run guards on `resources["database"]` and returns the "requires a
    database service" error when absent, matching the TS node; the handle
    contract + collection ops (find/filter/create/update/delete, serialize with
    `$id/$created/$updated/$version`) will be defined when the resource lands.
  - **Files:** `pkg/nodes/query/query.go` (Run stub with guard)

^- [*] Node unit tests
  - **Context:** Table-driven tests per node mirroring the utils behavior (trigger
    coercion, arithmetic ops/errors, if/switch/while routing, for-each iteration,
    try-catch error aggregation, transformer rule application, http SSRF block).
  - **Files:** `pkg/nodes/*/*_test.go`

^- [*] Verification: `go build ./...`, `go vet ./...`, `go test -race ./...`, npm build + typecheck
  - **Context:** Keep both toolchains green; re-run `bun run build` after any `.ts`
    change.

---

## Post-Phase-2 follow-ups (not in this task)
`pkg/compiler` (Phase 4), `pkg/runtime` orchestration (Phase 5), event-wire parity
(Phase 6), server adapter (Phase 7), verification vs TS suites (Phase 8). See
`WIP.md`.