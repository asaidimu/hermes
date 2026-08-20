# Phase 4 — `pkg/compiler` (workflow graph → compiled pipelines)

**Context:** Phases 1–2 are complete: polyglot node packages with Go Run/Router
implementations, `pkg/expr` (goja), node unit tests, and the engine (`pkg/pipeline`)
all green. This phase ports `~/projects/utils/src/workflows/compiler.ts` into
`pkg/compiler`, turning a flat node/edge graph into a compiled `pipeline.Workflow`
(trigger-bound pipelines + run-scoped resource services). The next phase
(`pkg/runtime`) consumes the compiled `Workflow`.

Parity reference: `compiler.ts` + `compiler.test.ts` + `schema.ts` (builders).

- [*] Add `Workflow` / `WorkflowTrigger` / `Service` / `PipelineRegistry` types
  - **Details:** Live in `pkg/pipeline/workflow.go` (base package so `nodekit`
    can reference `WorkflowTrigger` without an import cycle). `Service.Init` /
    `Service.Cleanup` closures are built by the compiler (capture the node def).
  - **Files:** `pkg/pipeline/workflow.go`

- [*] Extend `PipelineContext` with `ResolveResource(key) (any, bool)`
  - **Details:** `BuildStep` resolves the compiler's `{kind: "resource:<id>"}`
    artifact-key map into live handles at action time via the optional
    `pipelineContextImpl.resourceResolver` (default no-op). Added
    `pipeline.WithResourceResolver` for the runtime (Phase 5) to inject handles.
  - **Files:** `pkg/pipeline/types.go`

- [*] Add `pkg/nodekit/build.go` builders
  - **Details:** `prepareNodeConfig` (shared interpolate+coerce), `BuildTrigger`
    (only kind "trigger" → `__manual__` event), `BuildRouter` (re-interpolates per
    invocation, resolves handle → `pipeline.Jump(target)`), `defaultStageRouter`
    (advance via `resolveHandle("")`), `BuildStage` (single-step + router),
    `BuildBoundedStage` (try-catch: `<id>__body` subpipeline with optional
    `<id>__setup` step, `PipelinesRouter` builds the errors/results records and
    loops back via `Jump(nodeID)` when the handle equals the body handle).
  - **Refactor:** `BuildStep` action now uses `prepareNodeConfig` +
    `resolveStepResources`; `NodeRunContext.Store` populated from the context.
  - **Files:** `pkg/nodekit/build.go`, `pkg/nodekit/nodekit.go`

- [*] Implement `pkg/compiler/compiler.go`
  - **Details:** `Node`/`Edge` structs + `Compile(nodes, edges, registry)`. Ports
    `edgeRole/flowEdges/dependencyEdgesTo`, `makeResolveHandle` (skips edges into
    `output` nodes), `nextDefaultTarget`, `bfsStages` (bodyHandle / routing
    fan-out / default), `bfsBodyNodes`, `compileStageNode` (container steps +
    ≤1 routing child, error on >1), `compileStages` (container / pause error /
    pipeline-ref / bounded / standard), `buildResourceServices`, and the trigger
    collection → pipelines. Workflow ID via `google/uuid`.
  - **Files:** `pkg/compiler/compiler.go`

- [*] Port `compiler.test.ts` to `pkg/compiler/compiler_test.go`
  - **Details:** All TS cases (linear workflow, ascending orders, missing/child
    trigger errors, flow-only BFS, resource/container-child skipping, diamond
    tail dedupe, routing fan-out, container default + routing-child routers,
    multiple-routing error, pipeline-ref success/failure routing, resource
    injection + services). Registers stub `output` / `pipeline-ref` kinds and
    blank-imports `pkg/nodes` for the real registry.
  - **Files:** `pkg/compiler/compiler_test.go`

- [*] Verification
  - **Details:** `go build ./...`, `go vet ./...`, `go test ./...`,
    `go test -race ./...`, `bun run build` + `tsc --noEmit` all green.
  - **Notes:** Real switch/if nodes are pure routing (no `run`), so a routing
    child inside a container is not added to steps — matches the real TS
    registry (only the test mock gave switch a buildStep).

---

## Post-Phase-4 follow-ups (not in this task)
`pkg/runtime` (Phase 5): `WorkflowRuntime` — event bus, store registry, resource
manager (init services + `WithResourceResolver` injection), `Register`, `Run`
(compile → register → emit MANUAL_EVENT), `AbortRun`, outcomes. Then Phase 6
event-wire parity, Phase 7 server adapter, Phase 8 verification. See `WIP.md`.