# Task: Re-paused runs deadlock on their second resume (`make test` failures in pkg/runtime)

- [*] Clone `github.com/asaidimu/hermes` (branch `fixes`) and install the toolchain (Go 1.27.1; bun 1.3.14 already present)
- [*] Run `make test` (`go clean -testcache && go vet ./... && go test -race ./...`) and capture failures
  - **Context:** `make test` is the project's full quality gate (README "Testing & Quality Standard").
  - **Result:** 30+ packages pass; 2 failures in `pkg/runtime`:
    - `TestResumeRepauseSingleEvent` — repause_test.go:134 "timed out waiting for multi-pause run completion"
    - `TestResumeRepauseMultiEvent` — repause_test.go:187 "timed out waiting for multi-event re-pause completion"
- [*] Diagnose
  - **Context:** Both tests build a two-pause workflow: set a → pause(evt:1) → set b → pause(evt:2 or evt:2a/2b) → set c. The first resume works; the second resume (after the re-pause) never completes.
  - **Instrumented trace** (temporary WS_DEBUG logging in runtime.go/watchservice.go, reverted before commit):
    `Resume (2nd) → segment done status="paused" waitEvent="evt:1"` — the run re-paused on the FIRST pause's event, i.e. the rebuild resumed at the opening stage instead of the final one.
  - **Root cause:** `Replayer.latestEntries` (pkg/replay/replayer.go) fed `Rebuild` ONLY the highest `rerunIndex`'s entries. Resume deliberately advances the rerunIndex per segment (resolved #review-20260910-006), so a multi-pause run's history is spread across attempts: attempt 0 = first segment (effects + pause #1 checkpoint), attempt 1 = first resumed segment (effects + pause #2 checkpoint). On the second resume the Replayer saw only attempt 1: no record of stage:a's completed effect and no first-pause checkpoint, so `walkDef` returned "first unrecorded effectful step" = the opening stage. The run re-executed it and re-paused on an already-consumed event — permanent deadlock. Only the event-sourced path was broken; the legacy in-memory checkpoint path carried the right address.
- [*] Fix (pkg/replay/replayer.go)
  - **Details:** Replace `latestEntries` with `allEntries`: stitch attempts 0..latest in index order (chronological by construction — `nextRerunIndex` assigns strictly increasing indexes and each segment appends only after the bump) and re-baseline Seq 1..N, because both shipped stores derive Seq per (runID, rerunIndex) — MemoryActionLog per (run,index) pair, AnansiStore per document — while `checkpointSeq`/`resumedAfterCheckpoint` compare Seq across attempts once stitched. `StateAt` shares the stitched view so projections match recovery semantics.
- [*] Verify
  - Both previously failing tests pass; full `make test` (vet + race) exits 0; `make check` (gofmt -s + vet) exits 0.
- [*] Deliverable: commit on `fixes` + git bundle (all refs) at `/home/z/my-project/download/hermes-fixes.bundle`
