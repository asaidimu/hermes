# TODO: Pause Node Redesign

## Completed
- [x] Create `pkg/watch/watch.go` - shared types for WatchService interface
- [x] Create `pkg/runtime/watchservice.go` - WatchService implementation with pre-pause buffering
- [x] Update `pkg/runtime/runtime.go` - integrate WatchService into WorkflowRuntime
- [x] Update `pkg/nodes/pause/pause.go` - redesign with three handles (do, onResume, onTimeout)
- [x] Update `pkg/nodes/pause/pause_test.go` - update tests for new handle design
- [x] Run full test suite - all tests pass

## Summary

Implemented the pre-pause buffering pattern from the TS PauseService:

1. **WatchService** (`pkg/runtime/watchservice.go`):
   - Manages watch registrations for all active runs
   - Pre-pause buffering: events arriving before pause are buffered
   - `Register()` - called by pause node's Run method BEFORE pausing
   - `OnRunPaused()` - returns buffered event or parks the run
   - `OnRunEnded()` - cleans up watches when run completes
   - Timeout handling with context-cancel-safe timers

2. **Runtime Integration** (`pkg/runtime/runtime.go`):
   - Added `watchService` field to WorkflowRuntime
   - WatchService initialized in constructor with resume callback
   - `spawnRun()` uses WatchService for event watching (replaces manual subscription)
   - `Resume()` calls `OnRunEnded()` when run completes
   - `Shutdown()` cleans up all active watches
   - WatchService exposed as resource `resource:watch-service` for nodes

3. **Pause Node** (`pkg/nodes/pause/pause.go`):
   - Three handles: `do` (input), `onResume` (output), `onTimeout` (output)
   - `Run()` registers watch with WatchService (pre-pause buffering)
   - `Router()` routes to `onResume` or `onTimeout` based on `__resume_reason__`

4. **Shared Types** (`pkg/watch/watch.go`):
   - `WatchDescriptor` - describes what events to watch for
   - `WatchService` - interface for the watch service

## Test Results
- All pause node tests pass
- All runtime tests pass
- Full test suite passes
