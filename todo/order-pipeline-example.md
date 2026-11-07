# Task: Build a runnable example for Hermes

- [*] Explain what Hermes does (answer in chat from README/architecture)
  - **Context:** User asked "what does this library do and build me an example".
- [*] Create `examples/order-pipeline/main.go`
  - **Context:** `examples/` was empty; the library needed a self-contained, runnable demo.
  - **Details:** Two demos in one program:
    1. Canvas wire JSON → `compiler.DecodeWireGraph` → `rt.Run`: order-triage graph
       (trigger → code compute-total → if branch ≥100 → express/standard code nodes
       → delay → receipt code nodes), with bus subscription and timeline dump via
       `rt.GetEvents` (requires `runtime.Options{ActionLog: actionlog.NewMemoryActionLog()}`).
    2. Human-in-the-loop: `compiler.Compile` + `rt.Register` (NOT `rt.Run`, which
       deregisters on return) with a `pause` node (waitForEvent "approval"), wait for
       paused status, then resume by emitting the matching event on `rt.Bus()`
       (`"approval"`) — WatchService routes it to `rt.Resume` with the payload as patch.
       Note: pause is a bounded node — it requires an outgoing `"do"` edge to a body
       subgraph (at least one node), plus the `"onResume"` edge to the continuation.
  - **Files:** `examples/order-pipeline/main.go`
- [*] Verify: `go build ./...` and `go run ./examples/order-pipeline` succeed
  - **Context:** Example compiles and runs cleanly end-to-end (both demos print
    expected status/final state). Blank-import `pkg/nodes` is required for node kinds.
  - **Notes:** `gofmt -w` applied; `go vet ./examples/...` passes.
