# Run lifecycle hooks: OnPause / OnResume (and rt.Run pause fix)

## Context

The runtime's registration lifecycle callbacks are incomplete. `RegisterOptions` has
`OnPrepare` / `OnComplete` / `OnCleanup`, but:

- **Pause fires no callback and no bus event.** `spawnRun` (`internal/runtime/dispatch.go:174-243`)
  registers the paused run, writes the fold snapshot, and `return`s — skipping
  `rt.callOnComplete`. `Invoke` and `Resume` guard the same way (`dispatch.go:270-272`,
  `pauseresume.go:359-371`). Hosts must poll `GetRunMeta` for `status=paused` —
  the exact workaround `examples/external-events` uses.
- **Resume fires no notification** (the action log records `KindResumed`; RAM has nothing).
- **Bug (verified):** `rt.Run` funnels completion through an internal `OnComplete` that
  never fires on pause, so a pausing workflow through `Run` burns the full timeout and
  returns an opaque `TIMEOUT` error — the paused RunID is lost.
  Repro: trigger → pause → delay with `RunOptions{Timeout: 3s}` → `ELAPSED=3.003s err=TIMEOUT`.

## Plan

- [ ] Add `OnPause` / `OnResume func(RunResult)` to `RegisterOptions` and `RunOptions`
      (`internal/runtime/runtime.go`; public via alias in `pkg/runtime/runtime.go`).
      `OnPause`: run parked at a checkpoint (RunResult carries RunID, WaitForEvent(s),
      WaitMode, Checkpoint, FinalState at pause, Status="paused").
      `OnResume`: resumed segment about to execute (Status="resumed", Checkpoint = the
      wait being resumed from; fires after all Resume gates pass — payload fold, drift
      policy, rebuild — so refused resumes never notify).
- [ ] Wire call sites:
  - `spawnRun` pause branch: `callOnPause` after fold snapshot, before buffered-event
    immediate resume (ordering: OnPause → possible immediate OnResume → possible
    re-pause/OnComplete). Also for paused results with no tracked wait.
  - `Invoke`: `callOnPause` on paused results (symmetry with `callOnComplete` firing
    there for terminal results).
  - `pauseresume.go` Resume: enrich the paused `result` with WaitForEvent(s)/WaitMode/
    Checkpoint (spawn-path parity) before the outcomes write; `callOnResume` right
    before the resumed segment executes; `callOnPause` in the re-pause branch (same
    position as spawnRun: after fold snapshot, before buffered delivery).
- [ ] Fix `rt.Run`: return the paused result as soon as `OnPause` fires (non-blocking
      done-chans), skip the deferred `Deregister` when the result is paused so the
      registration survives for a later `rt.Resume(workflowID, payload)`.
- [ ] Tests (`internal/runtime/lifecycle_hooks_test.go`):
  - hook order OnPause → OnResume → OnComplete with expected fields on each;
  - `rt.Run` returns `Status=="paused"` well under timeout, then `rt.Resume` +
    RunOptions.OnComplete observes the terminal outcome; Deregister cleans up.
- [ ] Validate in the real cross-process scenario: update `examples/external-events`
      child to push an IPC "paused" message from `OnPause` (replaces the parent's
      200ms `GetRunMeta` polling for `status=paused`); re-run both scenarios.
- [ ] Verify: `go build ./...`, `go vet`, `go test ./internal/runtime/... ./tests/...`,
      both external-events scenarios PASS.

## Status

- [*] Add `OnPause` / `OnResume` to `RegisterOptions` and `RunOptions` (public via
      `pkg/runtime` type aliases). `OnPause` carries the wait, Checkpoint and state
      as of the pause; `OnResume` (Status `"resumed"`) fires only after all Resume
      gates pass.
- [*] Wired: `spawnRun` pause (tracked + untracked), `Invoke` paused results,
      `Resume` → `OnResume` before segment execution, re-pause branch of `Resume`
      (same position as spawnRun). Paused `Resume` results now also carry
      WaitForEvent(s)/WaitMode/Checkpoint (spawn-path parity for the outcomes map).
- [*] `rt.Run` returns the paused result as soon as `OnPause` fires; the deferred
      `Deregister` is now conditional (skipped when paused) so
      `rt.Resume(runID, payload)` can continue the run; caller owns Deregister.
      Verified: pausing graph returns in <10ms instead of burning the timeout.
- [*] Tests: `internal/runtime/lifecycle_hooks_test.go` — hook order
      (prepare→pause→resume→complete), Run-pause-return + resume continuation,
      OnResume-not-fired-on-refused-resume. 3/3 stable over -count=3.
- [*] `examples/external-events` child now pushes the paused IPC message from
      `OnPause` (the 20ms `GetRunMeta` polling loop is gone); parent protocol
      unchanged. Both scenarios 2/2 PASS.
- [*] Verified: `go build ./...`, `go vet ./...`, `gofmt` clean,
      `go test ./tests/ ./pkg/...` all ok, `./internal/runtime` ok.
- [*] Fixed pre-existing `TestFoldSnapshotGateDegradesToWalk` flake via
      `awaitFoldSnapshot` (see Notes); remaining rare chained-parity flake
      documented below (pre-existing, replay-side).

## Notes for future sessions

- FIXED flake: `TestFoldSnapshotGateDegradesToWalk` failed ~50% of runs at
  `foldsnapshot_test.go:231` (nil snapshot) — `awaitPaused` observes `rt.paused`
  a few lines before `writeFoldSnapshot` lands. Fixed test-side with
  `awaitFoldSnapshot` (waits for a snapshot whose Seq == last log entry, so a
  chained re-pause's previous snapshot doesn't satisfy it either); wired into
  all four pause-then-load sites in foldsnapshot_test.go. 500 runs clean.
- REMAINING pre-existing flake (NOT from this change; HEAD fails 5/8 batches of
  25 identically): `TestFoldSnapshotParityChainedRepause` intermittently fails
  its address parity (`foldsnapshot_test.go:211`): walk reports
  `Step:"step:b"` while the snapshot fast path reports `Step:""` (stage-level,
  what the checkpoint's ResumeAt holds). State parity still holds; only the
  reported EntryAddress diverges — a replay.Rebuild address-derivation
  inconsistency for router-pauses after a chained re-pause, independent of the
  hooks work. Rare (~2%/CI run). Fix belongs in pkg/replay's walkDef address
  derivation or the test's address contract; do not chase it as a hooks
  regression.
- The pause node books the runtime-level wait as `"__pause__"`
  (`pkg/nodes/pause/pause.go:177`) regardless of the configured
  `waitForEvent` (that name only scopes WatchService pre-buffering), so
  paused `RunResult.WaitForEvent == "__pause__"` is the correct contract.
- `Resume` takes the **runID**, not the workflowID.
- `OnPause`/`OnResume` are RAM notifications only — no new log facts (the log's
  checkpoint/KindResumed entries remain the authority; RECOVERY.md unchanged).
- The leftover `stash@{0}` ("WIP on main: cb844e2") was dropped during
  post-merge cleanup; its stash commit was 90c89b34 if it is ever needed.
