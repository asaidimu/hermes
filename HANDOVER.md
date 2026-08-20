# Agent Handover Document: `pipelines` (Go RSP Engine)

**Target Repository**: `/home/augustine/projects/pipelines`  
**Go Module**: `github.com/asaidimu/pipelines`  
**Date**: August 19, 2026  
**Status**: Ready for Implementation

---

## 1. Project Overview & Architectural Mission

The goal of this project is to implement a Go-native, production-grade **Routing Sequential Pipeline (RSP)** engine that serves as an **embeddable Go library** with full feature and wire parity with `@asaidimu/utils-pipeline`.

### Core Mandate: Embeddable Library First
* **Zero Bloat Core**: The core library (`pkg/pipeline`, `pkg/store`, `pkg/events`, `pkg/timeline`, `pkg/registry`) must be completely self-contained and embeddable directly as a Go package in any service or daemon.
* **Decoupled Server Adapter**: The HTTP REST API and WebSocket/SSE layer (maintaining frontend UI canvas parity) is an optional adapter located under `pkg/server`, ensuring the core engine has zero mandatory HTTP/web server dependencies.

---

## 2. Key Specifications & Context Files

Before starting implementation, review the following documents in this directory:
1. **[`SPEC.md`](./SPEC.md)**: Complete RSP engine specification, types, state machine execution loop, routing rules, checkpointing format, and frontend wire contracts.
2. **[`SCHEMA_AND_DURABILITY.md`](./SCHEMA_AND_DURABILITY.md)**: Deep dive on Anansi schema-backed pooled documents, zero-boilerplate struct schemas, and crash-safe atomic stage commits.
3. **[`go.mod`](./go.mod)**: Module definition configured with local replace directives for `go-anansi` and `go-events`.

---

## 3. Foundational Dependencies

The project builds upon two sibling Go repositories:
1. **`go-anansi`** (`/home/augustine/projects/go-anansi` -> `github.com/asaidimu/go-anansi/v8`):
   - Data container: `*document.Document` (implementing `data.Documenter`).
   - Memory pooling: `container.Pool` / `DocumentPool` for zero-allocation steady-state performance.
   - Struct-derived DTO schemas and typed model binding (`doc.Bind(&model)`).
   - Sanitization engine for PII masking in timeline events.
2. **`go-events`** (`/home/augustine/projects/go-events` -> `github.com/asaidimu/go-events/v2`):
   - `SimpleEventBus[PipelineEvent]` for live in-memory event fanout.
   - Durable Pebble LSM storage for crash-proof event sourcing and audit logs.

---

## 4. Implementation Roadmap (Phased Execution)

### Phase 1: Core Primitives & Anansi Store Adapter
* **Package**: `pkg/core`, `pkg/store`
* **Deliverables**:
  - `SystemError` with error codes (`NOT_FOUND`, `INVALID_COMMAND`, `EXECUTION_FAILED`, `TIMEOUT`, `CANCELLED`).
  - Structured `Logger` interface with no-op fallback.
  - `Store` interface wrapping Anansi `*document.Document` with `Transaction(ctx, fn)` and `ExportJSON()`.
  - In-memory / ephemeral Anansi store implementation.

### Phase 2: Event System & Ancestry Tracking
* **Package**: `pkg/events`
* **Deliverables**:
  - `PathNode` and `EventPath` tracking hierarchical ancestry (`pipeline -> stage -> subpipeline -> step`).
  - `ScopedEventBus` wrapping `go-events` `SimpleEventBus[PipelineEvent]`, supporting child-to-parent event bubbling.

### Phase 3: Pipeline Definition & State Machine Execution Loop
* **Package**: `pkg/pipeline`
* **Deliverables**:
  - `PipelineDefinition`, `Stage`, `Step`, `PipelineContext` types.
  - `PipelineFactory` with `Prepare(ctx, entry, runID)`.
  - `RunContextImpl`: Stage execution loop, concurrent step runner using `golang.org/x/sync/errgroup`, per-step timeout and retries.
  - Atomic stage commit: Accumulating `DocumentMutator` functions from steps and committing them atomically to `Store` at stage boundaries.

### Phase 4: Dynamic Routing & Subpipelines
* **Package**: `pkg/pipeline`
* **Deliverables**:
  - Router evaluator for `Advance`, `Jump`, `JumpTo`, `Terminate`, and `Pause`.
  - Subpipeline Fork/Join runner: Concurrent execution of child pipelines via `errgroup`, aggregating child results into `PipelinesRouter`.

### Phase 5: Checkpoints, Registry & Two-Tier Resumption
* **Package**: `pkg/pipeline`, `pkg/registry`
* **Deliverables**:
  - `PipelineCheckpoint` serialization into Anansi document metadata under `__pipeline_data__.checkpoints`.
  - `PipelineRegistry`: Thread-safe in-memory run tracking with `time.AfterFunc` pause expiration timers and `OnExpired` hooks.
  - `PipelineFactory.Resume(ctx, runID)`:
    - Fast-Path: In-memory live context reset and immediate execution.
    - Cold-Storage Path: Document reload from Anansi storage, checkpoint verification, and fresh context initialization at `checkpoint.ResumeAt`.

### Phase 6: Timeline Engine (Recording & Time-Travel Playback)
* **Package**: `pkg/timeline`
* **Deliverables**:
  - `TimelineRecorder`: Channel-buffered event capture, monotonic sequence counter (`Seq`), periodic `doc.Clone()` snapshots, and Anansi field sanitization.
  - `TimelineStore`: In-memory and Pebble/storage-backed event and snapshot repository.
  - `TimelinePlayer`: Time-travel replay with `Seek(seq)` (snapshot restore + delta replay) and `StepForward`/`StepBackward`.

### Phase 7: Embeddable Facade & Optional HTTP Server Adapter
* **Package**: `pkg/server`, `pipelines.go`
* **Deliverables**:
  - Clean top-level embeddable API in root `pipelines` package.
  - Optional `pkg/server` implementing the 100% wire-compatible REST API (`/run`, `/compile`, `/runs/:runId/events`, `/runs/:runId/store`, etc.).

### Phase 8: Comprehensive Test Suite & Parity Verification
* **Package**: `tests/`
* **Deliverables**:
  - Port all 12 Vitest suites into Go table-driven tests (`testing.T`).
  - Stress testing with race detector: `go test -race -v ./...`.
  - Concurrency validation with 1,000+ simultaneous subpipelines and paused workflows.

---

## 5. Development & Bug Hunting Guidelines

* **Principle**: Always start bug fixes with a **repro test** before modifying implementation.
* **Thread Safety**: All stateful components (`RunContext`, `Registry`, `TimelineStore`, `ScopedEventBus`) must be strictly concurrent-safe.
* **Go Idioms**:
  - Use `context.Context` for cancellation and timeouts.
  - Use `(T, error)` returns rather than panics.
  - Preserve clear struct field comments and docstrings.
