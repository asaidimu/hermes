# Fix Backlog Issues in Sequence

- [*] Task 1: Fix server goroutine leak, unbuffered channel block, and timer leak in `pkg/server/server.go` (#review-20260822-041, #review-20260822-042)
  - **Context:** In `pkg/server/server.go:handlePostRun`, `rt.Run` runs in a background goroutine. If the HTTP client disconnects or times out, the `prep` channel send can block indefinitely, the context is not cancelled, and `time.After` leaks a timer.
  - **Details:** Use a buffered channel `make(chan prepResult, 1)`, derive and propagate request cancellation context, use `time.NewTimer` with explicit `defer timer.Stop()`, and update devnotes.
  - **Files:** `pkg/server/server.go`, `pkg/server/server_test.go`

- [*] Task 2: Fix timer leak in `WorkflowRuntime.Run` in `pkg/runtime/runtime.go` (#review-20260822-038)
  - **Context:** `WorkflowRuntime.Run` uses `case <-time.After(timeout):` in a select, leaking an active timer in memory if `done` arrives before timeout.
  - **Details:** Replace with `timer := time.NewTimer(timeout)` and `defer timer.Stop()`. Update devnotes.
  - **Files:** `pkg/runtime/runtime.go`

- [*] Task 3: Fix TOCTOU race in dispatch between lock acquisitions in `pkg/runtime/runtime.go` (#review-20260822-039)
  - **Context:** In `dispatch()`, `rt.mu` is unlocked after collecting resume candidates and re-locked for trigger candidates, leaving a TOCTOU window where `rt.paused` / `rt.index` can be modified.
  - **Details:** Collect all candidates (toResume and trigger matches) under a single lock acquisition. Update devnotes.
  - **Files:** `pkg/runtime/runtime.go`

- [*] Task 4: Fix unknown predicate operator returning true in `pkg/runtime/watchservice.go` (#review-20260822-044)
  - **Context:** In `matchesPredicate`, unrecognized comparison operators return `true`, silently matching all events.
  - **Details:** Make unknown operators return `false` (and log/flag if needed). Update devnotes and tests.
  - **Files:** `pkg/runtime/watchservice.go`, `pkg/runtime/watchservice_test.go`

- [*] Verify full test suite
  - **Context:** Ensure all unit and integration tests pass with zero regressions.
  - **Details:** Run `go test ./...`.
