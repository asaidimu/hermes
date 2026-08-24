# Code Review — 2026-08-22

**Reviewer:** opencode  
**Scope:** Staged changes across 50+ files (pause node, runtime, scheduler, events, store, server, pipeline, nodes, tests, README)

---

## Pre-Review Automated Checks

| Check | Result |
|-------|--------|
| `go vet ./...` | Pass |
| `go test ./...` | **FAIL** — `TestInMemorySchedulerReplace` (expected callCount=10, got 20) |
| `go test -race ./...` | **FAIL** — race on `js.timer` in `pkg/scheduler/inmemory.go:55` |
| `devnotes check` | N/A (notes already in code) |

---

## P0 — Must Fix Before Merge

### 1. Race condition in `InMemoryScheduler.scheduleNextLocked`
**File:** `pkg/scheduler/inmemory.go:50-71`  
**Devnote:** `#review-20260822-052` (related)

The timer callback at line 55-70 calls `s.scheduleNextLocked(id, js, cron)` at line 68 **without holding the lock**. Concurrently, `Schedule()` calls the same function **under the lock** at line 46. Both write to `js.timer` at line 55, creating a data race detected by `-race`.

**Fix:** Acquire `s.mu.Lock()` before the reschedule call at line 67-69:
```go
if still {
    s.mu.Lock()
    s.scheduleNextLocked(id, js, cron)
    s.mu.Unlock()
}
```

**Test:** `TestInMemorySchedulerReplace` fails because the race causes double-firing (callCount=20 instead of 10).

---

### 2. Incorrect func pointer comparison in `Subscribe` unsubscribe
**File:** `pkg/events/events.go:187`  
**Devnote:** `#review-20260822-004`

`&h == &handler` compares addresses of loop variable `h` vs captured `handler` — this will **never** match. Handlers are never removed, causing a permanent leak.

**Fix:** Use `reflect.ValueOf(h).Pointer() == reflect.ValueOf(handler).Pointer()`, or return an index-based token from `Subscribe`.

---

### 3. Unsafe type assertion panics on bad input
**File:** `pkg/pipeline/context.go:367`  
**Devnote:** `#review-20260822-034`

`instruction.(PauseInstruction)` panics if `instruction` is not a `PauseInstruction`. Use comma-ok assertion.

---

## P1 — Should Fix Before Merge

### 4. `Document()` returns mutable pointer after releasing lock
**File:** `pkg/store/store.go:64-77`  
**Devnote:** `#review-20260822-005`

Callers can mutate the returned `*document.Document` without any lock, creating data races with concurrent `Read`/`Update`/`Transact`.

### 5. `Clone` double-acquires RLock (potential deadlock)
**File:** `pkg/store/store.go:149-162`  
**Devnote:** `#review-20260822-008`

`Clone()` holds RLock and calls `ExportJSON()` which also acquires RLock. A pending writer between the two lock acquisitions will deadlock.

### 6. Unbounded HTTP response body read
**File:** `pkg/nodes/http/http.go:177`  
**Devnote:** `#review-20260822-037`

`io.ReadAll(resp.Body)` reads unbounded response into memory. Use `io.LimitReader`.

### 7. Timer leak in `Run` select
**File:** `pkg/runtime/runtime.go:891`  
**Devnote:** `#review-20260822-038`

`time.After(timeout)` leaks a timer if `done` or `ctx.Done()` fires first. Use `time.NewTimer` with `defer timer.Stop()`.

### 8. TOCTOU race in `dispatch`
**File:** `pkg/runtime/runtime.go:498-544`  
**Devnote:** `#review-20260822-039`

Lock is released between snapshotting `toResume` and `entries`. Take both under a single lock.

### 9. Goroutine leak when HTTP client disconnects
**File:** `pkg/server/server.go:188-231`  
**Devnote:** `#review-20260822-041`

If the HTTP handler times out, the goroutine running `rt.Run` continues. The prep channel send blocks forever on the success path.

### 10. Timer leak in `handlePostRun`
**File:** `pkg/server/server.go:229`  
**Devnote:** `#review-20260822-042`

`time.After(10s)` leaks if prep channel fires first. Use `time.NewTimer`.

### 11. Unknown operator returns `true` in watch conditions
**File:** `pkg/runtime/watchservice.go:227`  
**Devnote:** `#review-20260822-044`

The switch has no default case. Unknown operators fall through and return `true`, treating invalid conditions as satisfied.

### 12. Code injection vulnerability in `EvalValue`
**File:** `pkg/expr/expr.go:86-101`  
**Devnote:** `#review-20260822-049`

User input is wrapped in `"(" + expr + ")"` with no validation. Break-out injection possible.

### 13. Store update errors silently discarded (multiple locations)
- `pkg/pipeline/context.go:84` — `#review-20260822-033`
- `pkg/nodes/pause/pause.go:108` — `#review-20260822-046`
- `pkg/runtime/runtime.go:746` — `#review-20260822-048`
- `pkg/runtime/runtime.go:937` — `#review-20260822-047`

### 14. Double registration of pause node
**File:** `pkg/nodes/pause/pause.go:130`  
**Devnote:** `#review-20260822-045`

`nodes.go` already registers `pause.Node`. The `init()` in `pause.go` registers again (harmless but confusing).

### 15. `Get` returns mutable internal state pointer
**File:** `pkg/registry/registry.go:79`  
**Devnote:** `#review-20260822-043`

Returns pointer to internal `ActiveRun` under `RLock`. Caller holds mutable reference after unlock.

### 16. `Write` discards store update error
**File:** `pkg/pipeline/context.go:84`  
**Devnote:** `#review-20260822-033`

### 17. `Schedule` error discarded
**File:** `pkg/runtime/runtime.go:353`  
**Devnote:** `#review-20260822-040`

### 18. Goroutine leak on normal path (abortChan bridge)
**File:** `pkg/pipeline/context.go:116`  
**Devnote:** `#review-20260822-055`

Use `context.AfterFunc` (Go 1.21+) instead of dedicated goroutine.

### 19. `Emit` silently discards all handler errors
**File:** `pkg/events/events.go:146`  
**Devnote:** `#review-20260822-006`

---

## P2 — Can Fix in Follow-Up

### 20. `Store.Transact` is identical to `Update`
**File:** `pkg/store/store.go:28-35`  
**Devnote:** `#review-20260822-019`

No rollback, retry, or isolation beyond `Update`. Either add real semantics or remove.

### 21. `ExportJSON` performs wasteful Marshal/Unmarshal round-trip
**File:** `pkg/store/store.go:126-141`  
**Devnote:** `#review-20260822-020`

### 22. `WriteCheckpoint` Marshal/Unmarshal round-trip
**File:** `pkg/pipeline/checkpoint.go:84`  
**Devnote:** `#review-20260822-054`

### 23. Wildcard CORS origin
**File:** `pkg/server/server.go:339`  
**Devnote:** `#review-20260822-050`

`Access-Control-Allow-Origin: *` is a security risk in production.

### 24. `WatchService` has no error returns
**File:** `pkg/watch/watch.go:53`  
**Devnote:** `#review-20260822-013`

`Register` returns nothing; failures silently dropped.

### 25. Untyped strings for operator/field/mode
**File:** `pkg/watch/watch.go:24`  
**Devnote:** `#review-20260822-011`

### 26. `Timeout` is `int64` without documented units
**File:** `pkg/watch/watch.go:40`  
**Devnote:** `#review-20260822-012`

### 27. `Underlying()` leaks concrete type through interface
**File:** `pkg/events/events.go:66`  
**Devnote:** `#review-20260822-015`

### 28. `CronDelay` silently masks invalid cron expressions
**File:** `pkg/scheduler/cron.go:22`  
**Devnote:** `#review-20260822-053`

### 29. Context never propagated to scheduler callback
**File:** `pkg/scheduler/inmemory.go:35`  
**Devnote:** `#review-20260822-052`

### 30. `Document()` returns concrete pointer, unmockable
**File:** `pkg/store/store.go:19`  
**Devnote:** `#review-20260822-018`

---

## P3 — Minor / Optional

### 31-35. Documentation/naming issues
- `#review-20260822-009`: Package doc misdescribes contents
- `#review-20260822-010`: `Payload`/`Patch` lack doc comments
- `#review-20260822-014`: `EventPath` lacks doc comment
- `#review-20260822-021`: Blank import side-effect unexplained
- `#review-20260822-030`: `NopLogger` lacks doc comment

---

## README (Unstaged)

The README.md changes (343 lines added) are well-structured and accurate. No code review issues found — this is documentation-only.

---

## Summary

| Priority | Count | Status |
|----------|-------|--------|
| P0 | 3 | Must fix — includes race condition, infinite loop in unsubscribe, panic |
| P1 | 16 | Should fix — timer leaks, goroutine leaks, error swallowing, security |
| P2 | 11 | Follow-up — interface bloat, wasted allocations, CORS, docs |
| P3 | 5 | Minor — naming, documentation |

**Total open devnotes:** 55 (including pre-existing from earlier reviews)

**Recommended next step:** Fix the 3 P0 items first (scheduler race, unsubscribe comparison, unsafe type assertion), then re-run `go test -race ./...` to confirm the scheduler race is resolved.
