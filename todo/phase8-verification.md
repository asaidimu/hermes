# Phase 8 — Verification

## Scope

- [*] Port `compiler.test.ts` to Go table-driven tests
  - **Context:** all 22 TS cases map 1:1 to `pkg/compiler/compiler_test.go`
    (stage compilation steps/pipelines/bounded, router wiring, resource
    services, trigger collection, error cases). Services test at
    `compiler_test.go:548`.
- [*] Port `runtime.test.ts` to Go table-driven tests
  - **Context:** every scenario is covered by `pkg/runtime/runtime_test.go`
    (basic completion incl. the `[object Object]` initialState repro, abort,
    step:failure error serialization for generic + SystemError through the
    timeline recorder, resource lifecycle events, event wire).
- [X] Port `database-workflow.test.ts` to Go tests
  - **Context:** BLOCKED BY DESIGN — the suite drives a real ephemeral store
    through the `query` node's Run. Per user directive the database resource is
    held (metadata only, no ResourceInit/ResourceEnd) and query Run is a
    guarded stub. Out of scope until that directive is lifted.
- [*] End-to-end calc sample graph test
  - **Context:** covered by the live server smoke test (POST /run -> outcome,
    events with `path`, store) in `tests/frontend_api_test.go` plus the
    event-wire suite; DB query/calc flows are held per directive.
- [*] Final verification pass
  - **Context:** `go build ./...`, `go vet ./...`, `go test ./...`,
    `go test -race ./...`, `bun run build`, `tsc --noEmit` — all green. WIP.md
    updated.

## Phase 8.5 — server surface parity (added mid-phase)

- [*] Match `~/projects/utils/src/workflows/server.ts` endpoint contract
  - **Context:** user directive: server.ts is the spec for what the server
    must serve; hedwig UI is the user's to build. Added to
    `pkg/server/server.go`: `GET /registry` (nodekit.Registry JSON),
    `GET /handles.js` (JS object literal `{ kind: (config) => <specs> }` with a
    hand-written switch handler replicating `pkg/nodes/switch/switch.ts`),
    `POST /compile` (JSON-safe metadata view of the compiled workflow),
    `POST /register` (raw nodes/edges -> compile + register),
    `POST /deregister`, `POST /events` (emit on the runtime bus). Timeline
    events now carry `path` (`pkg/timeline/timeline.go`) for client
    translations. Server verified live: run -> runId, outcome, events with
    path, store, registry, handles.js.

## Reference

- TS suites: `~/projects/utils/src/workflows/compiler.test.ts`,
  `~/projects/utils/src/workflows/runtime.test.ts`,
  `~/projects/utils/src/workflows/database-workflow.test.ts`.