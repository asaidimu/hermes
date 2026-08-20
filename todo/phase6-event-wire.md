# Phase 6 — Event wire parity

Port the client-visible runtime event contract from the TS engine into `pkg/pipeline`.
The client's `translations.ts`/`context.tsx` reconstruct node statuses and log rows from
these payloads; parity is required for the Phase 8 verification and the Phase 7 server.

## Scope

- [*] `stage:start` carries `mode: "steps" | "pipelines"` (today emits `stepCount` only)
  - **Context:** TS stage router emits the mode so the client knows whether to render
    steps or subpipeline blocks.
  - **Details:** Run loop in `pkg/pipeline/context.go` emits mode/stepCount/subPipelineCount;
    removed the duplicate stage:start in `stage.go`.
- [*] `stage:success` carries `nextInstruction`
  - **Context:** TS serializes the routing instruction chosen after the stage.
  - **Details:** Run loop serializes via `serializeInstruction`; removed duplicate
    stage:success in `stage.go`.
- [*] Emit `router:evaluated` (`instruction`, `interpretation`)
  - **Context:** TS emits this after step/pipeline routers evaluate; the client uses it
    to render routing decisions.
  - **Details:** `interpretationOf` maps Pause→pause, Terminate→terminate, Jump/JumpTo→jump,
    nil/Advance→advance|natural-end.
- [*] Emit `subpipeline:fork` / `subpipeline:join` (`subPipelineIds`, `results`)
  - **Context:** parent stage around `Stage.Pipelines`.
- [*] Emit `resource:init` / `resource:ready` / `resource:init:failure` /
      `resource:cleanup` / `resource:cleanup:failure`
  - **Context:** resource lifecycle events in `pkg/runtime` (initResources/cleanup).
  - **Details:** `initResources` now takes the run bus + runID + pipelineID and emits the
    TS payload (`resourceId`/`resourceKind`/`resourceLabel`, `errorMessage` on failures);
    `pipeline.Service` gained Kind/Label, populated by the compiler.
- [*] `error.message` present on all `*:failure` payloads
  - **Context:** client reads `payload.error.message`. `SystemErrorJSON` already provides
    message/code/stack; ensure every failure event uses it.
  - **Details:** Added `core.CauseMessage`; `ExecuteStageSteps` now aggregates step errors
    with the TS message format `"N step(s) failed in <label>:<id>: causes"`.
- [*] Fix `subpipeline.go`: child runs must bubble with the parent runID
  - **Context:** child `RunContextImpl` currently uses `childDef.ID` as runID — the client
    keys events by the parent run. Child runIDs should derive from the parent runID.
- [*] Subpipeline failure must not hard-abort the parent
  - **Context:** currently `ExecuteSubPipelines` returns the error → parent fails. TS
    routes errors into `PipelinesRouter` via the `errors` record so try-catch can catch.
    Subpipeline errors should be captured per child (result.Error) and surfaced to the
    router; the parent continues.
  - **Details:** `ExecuteSubPipelines` uses `sync.WaitGroup` (siblings no longer cancel),
    captures per-child failures; error reserved for store-clone/cancellation.
- [*] Verify: `go build/vet/test -race`, `bun run build`, `tsc --noEmit`; update WIP.md.
  - **Details:** green; new `tests/eventwire_test.go` (4 tests) + `TestResourceLifecycleEvents`.

## Reference

- TS engine event emission: `utils/src/runtime/pipeline/**`
- Client consumption: `translations.ts` / `context.tsx` (hedwig flows client)