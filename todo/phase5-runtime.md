# Phase 5 — pkg/runtime (WorkflowRuntime)

Port of `@core/runtime` `WorkflowRuntime` (utils/src/runtime/runtime/runtime.ts) + `WorkflowsEngine`
(utils/src/workflows/engine.ts). Delivers run orchestration: trigger dispatch, per-run store,
resource lifecycle, outcomes, abort.

## Scope

- [*] Plumb run-scoped resource resolver through the engine
  - **Context:** Steps resolve `resource:<nodeID>` handles via `pcxt.ResolveResource`, but
    `ExecuteStageSteps`/`ExecuteSubPipelines` never pass a resolver, and child factories don't
    carry one. The runtime needs to inject `map[string]any` resource handles per run.
  - **Details:** Add `resourceResolver` field + `SetResourceResolver` to `RunContextImpl`
    (pkg/pipeline/context.go); add `ResourceResolver` to `FactoryOptions`
    (pkg/pipeline/factory.go) and attach in `Prepare`/`PrepareWithEntry`; thread the resolver
    through `ExecuteStageSteps` (pkg/pipeline/stage.go) into `NewPipelineContext(WithResourceResolver)`
    and through `ExecuteSubPipelines` (pkg/pipeline/subpipeline.go) into child factories.
- [*] Implement `pkg/runtime/runtime.go` — `WorkflowRuntime`
  - **Context:** Core orchestrator. TS reference:
    `utils/src/runtime/runtime/runtime.ts` (dispatch → spawnRun → executePipeline → onComplete),
    `utils/src/workflows/engine.ts`.
  - **Details:** `Options{Bus, StoreFactory, Timeline, Logger, Env}`; `RegisterOptions{Mode,
    OnPrepare, OnComplete, OnCleanup}`; `Mode{Type, Concurrency, Capacity}`. Registry:
    `workflows` map + event-type dispatch index (ref-counted bus subscriptions), `outcomes`
    map, `active` run-context map. Methods: `NewWorkflowRuntime`, `Register`, `Deregister`,
    `HasWorkflow`, `ListWorkflows`, `Invoke` (direct awaitable run), `Run` (compile→register→
    emit `__manual__`→await outcome), `AbortRun`, `GetRunOutcome`, `ListRuns`, `Store(runID)`.
    Resources: init workflow-scoped once (cached), run/transient per run; cleanup on end.
    Seed `__trigger_event__` into the run store; attach `timeline.TimelineRecorder` when a
    timeline store is configured.
- [*] Write `pkg/runtime/runtime_test.go`
  - **Context:** Port runtime.test.ts behavior: manual-trigger run completes and writes final
    state; abort marks outcome; duplicate register errors; deregister stops dispatch; error
    nodes serialize into timeline store (step:failure carries message/stack/code).
- [*] Verify: `go build ./...`, `go vet ./...`, `go test ./...`, `go test -race ./...`,
  `bun run build`, `tsc --noEmit`; update `WIP.md`.

## Notes

- JS-number convention: numeric results are float64; use float64 literals in test input state.
- Abort event constant `run:abort` (TS `ABORT_EVENT`); manual event `__manual__`.
- Run outcome statuses: "succeeded" | "failed" | "aborted" | "paused".
- Server adapter (Phase 7) will use `Run(nodes, edges)`.
- Additional engine fixes made while porting: `RunContextImpl.Run` now derives a cancellable
  context from the abort channel so in-flight steps observe aborts; `SystemErrorJSON` moved to
  `pkg/core` (stage/pipeline failure event payloads now serialize errors in the TS
  `SystemError.toJSON()` shape with code/message/stack).