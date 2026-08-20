# Phase 7 — Thin server adapter

## Scope

- [*] `POST /run` accepts `{nodes, edges}` in the TS wire format
  - **Context:** previously expected `{pipelineId}` and ran a pre-registered
    `PipelineDefinition`. Now the client submits the raw graph and the runtime
    compiles + runs it.
  - **Details:** `pkg/server/server.go` decodes `WorkflowNode`/`WorkflowEdge`
    wire shapes (`data.kind`/`data.config`, top-level `type`, `data.role`) into
    `compiler.Node`/`compiler.Edge`, then calls `runtime.Run` in a goroutine and
    returns `{runId}` via the `OnPrepare` hook (compile failures surface as 400
    instead of hanging). Example: `examples/server/main.go` rewritten to the
    graph-based API.
- [*] Add CORS (`Access-Control-Allow-Origin: *` + OPTIONS)
  - **Context:** previously absent — blocked the hedwig dev client.
  - **Details:** `withCORS` middleware wraps the mux; OPTIONS preflight returns
    204 with allow headers.
- [*] Runtime endpoints only: `/run`, `/runs`, `/runs/:id`, `/outcome`, `/events`,
      `/store`, `/abort`
  - **Context:** no `/handles.js` / catalog endpoints — the client gets handles
    from the `@asaidimu/hermes` package.
  - **Details:** removed `/registry`, `/register`, `/deregister`, `/handles.js`;
    `/runs/:id` = timeline meta, `/outcome` = `GetRunOutcome`, `/events` =
    `GetEvents`, `/store` = live run store, `/runs/:id/abort` (POST) = `AbortRun`.
- [*] Runtime additions: `GetRunMeta`, `GetEvents`
  - **Details:** added to `pkg/runtime/runtime.go` delegating to the runtime's
    timeline store; the server owns a default `MemoryTimelineStore` when no
    runtime is injected.
- [*] Verify: `go build/vet/test -race`; update WIP.md
  - **Details:** green. `tests/frontend_api_test.go` rewritten for the graph API:
    POST `/run` with wire graph, poll outcome, `/runs/:id`, `/events`, `/store`,
    `/runs` list, abort, 404 for removed endpoints, CORS preflight, and a
    bad-graph 400 case.

## Reference

- TS wire shapes: `src/types.ts` (`WorkflowNode`, `WorkflowEdge`, `EdgeRole`).
- `pkg/runtime` API: `Run(nodes, edges, RunOptions{OnPrepare})`, `AbortRun`,
  `GetRunOutcome`, `ListRuns`, `GetRunMeta`, `GetEvents`, `Store`.