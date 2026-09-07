# Hermes

> Polyglot workflow engine — a Go-native **Routing Sequential Pipeline (RSP)** runtime paired with a JavaScript node catalog, handles, and wire types for visual workflow canvases.

[![Go Version](https://img.shields.io/badge/go-1.27-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![npm](https://img.shields.io/badge/npm-@asaidimu%2Fhermes-CB3837?logo=npm&logoColor=white)](https://www.npmjs.com/package/@asaidimu/hermes)
[![Tests](https://github.com/asaidimu/hermes/actions/workflows/test.yaml/badge.svg)](https://github.com/asaidimu/hermes/actions/workflows/test.yaml)
[![License](https://img.shields.io/badge/license-MIT-green)](./LICENSE.md)

## Quick Links
- [Overview & Features](#overview--features)
- [Installation & Setup](#installation--setup)
- [Usage Documentation](#usage-documentation)
- [Project Architecture](#project-architecture)
- [Development & Contributing](#development--contributing)
- [Troubleshooting & FAQ](#troubleshooting--faq)
- [License](#license)

---

## Overview & Features

### Detailed Description

Hermes is an asynchronous, state-machine-driven workflow orchestrator written in Go. It compiles flat workflow graphs (`{nodes, edges}`) into trigger-bound pipelines and executes them through a Routing Sequential Pipeline (RSP) engine: stages run concurrent steps, merge their document mutations atomically at stage boundaries, and route dynamically based on step settlements. Workflow state lives inside schema-backed Anansi documents (`*document.Document`), giving typed path resolution, validation, and fast pooled access.

The project is deliberately **polyglot**. Every built-in node kind lives in its own package containing *both* a Go implementation (`<kind>.go`) and a TypeScript definition (`<kind>.ts`). The Go toolchain aggregates the `.go` files into a self-contained binary with zero `//go:embed` assets; the Bun-based toolchain aggregates the `.ts` files into the [`@asaidimu/hermes`](https://www.npmjs.com/package/@asaidimu/hermes) npm package, which ships the node catalog (`CATALOG`), evaluatable handle functions (`HANDLES`), and canonical wire types consumed by frontend UI canvases.

### Key Features
- **Routing State Machine**: Stages evaluate routing instructions — `Advance`, `Terminate`, `Jump`, `JumpTo`, `Pause` — driven by document queries and step outcomes.
- **Atomic Stage Commits**: Concurrent steps produce isolated mutations that are merged and committed atomically to the Anansi-backed document store at stage boundaries.
- **Checkpoint Pause & Resume**: Suspend workflows at stage boundaries with serialized resume addresses; restore from the in-memory registry cache or reload from cold storage via two-tier resumption.
- **Time-Travel Timeline**: Sequence-indexed event recording with periodic snapshots, playable forward/backward and consumable directly by the frontend timeline scrubber.
- **Subpipeline Fork/Join**: Stages can fan out into concurrent child pipelines whose results are aggregated by a pipelines router.
- **Event-Driven Runtime**: A bus-driven orchestrator dispatches external events to registered workflows, supports per-workflow concurrency modes (`transient`, `serialized`, `exclusive`, `loop`), and resumes paused runs on matching events via a pre-buffering watch service.
- **Scheduling**: Pluggable cron scheduler (`"30 * * * *"`, `"@every 5m"`, `"@daily"`) plus one-shot delays, backed by an in-memory implementation.
- **Built-in Node Catalog**: 15 production-ready kinds including sandboxed JavaScript (`code`, powered by goja), HTTP requests, Gemini AI prompts, control flow (`if`/`switch`/`while`/`for-each`/`try-catch`/`pause`/`delay`), transforms, queries, and resource nodes.
- **Canvas Wire Format**: `compiler.DecodeWireGraph` / `compiler.CompileWire` translate canvas JSON documents (`{nodes, edges}`) into compiled workflows — no HTTP layer required.
- **Embeddable Facade**: Import the root `pipelines` package for a clean, dependency-light API.

---

## Installation & Setup

### Prerequisites
- **Go >= 1.27** (the module targets `go 1.27rc1`; CI uses `1.27.0-rc.1`)
- **Bun >= 1.2** — only required when building/publishing the JS package (`bun.lock` present)
- Optional: Node.js >= 18 for consuming the published npm artifacts

### Installation
```bash
# Clone the repository
git clone https://github.com/asaidimu/hermes.git
cd hermes

# Fetch Go dependencies
go mod download

# (Optional, for the @asaidimu/hermes JS package) install TS dependencies
bun install
```

As a Go library:
```bash
go get github.com/asaidimu/hermes
```

### Configuration
No environment variables are required — the runtime defaults to in-memory stores, an isolated event bus, and an in-memory action log. Typical configuration points:

| Concern | Mechanism |
| :--- | :--- |
| Event bus / store factory / action log | `runtime.Options{Bus, StoreFactory, ActionLog}` |
| Cron scheduling | `runtime.Options.Scheduler` (defaults to `scheduler.InMemoryScheduler`) |
| External trigger wiring | `runtime.Options.EventSource` (defaults to a manual source) |
| Gemini API key | Passed per-node via the node `config.apiKey` field |

> Programs that build workflows must register all node kinds via a blank import of `pkg/nodes` (see [Troubleshooting](#troubleshooting--faq)).

### Verification
```bash
# Compile everything and run the suite
go build ./...
go test ./...
```

---

## Usage Documentation

### Basic Usage

Run a workflow graph directly against the embedded runtime:

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/asaidimu/hermes/pkg/compiler"
	"github.com/asaidimu/hermes/pkg/nodes" // registers all node kinds
	"github.com/asaidimu/hermes/pkg/runtime"
)

func main() {
	rt := runtime.NewWorkflowRuntime(runtime.Options{})

	nodes := []compiler.Node{
		{
			ID: "trigger", Type: compiler.NodeExecutable, Kind: "trigger",
			Config: map[string]any{"initialState": map[string]any{"text": "hello"}},
		},
		{
			ID: "delay", Type: compiler.NodeExecutable, Kind: "delay",
			Config: map[string]any{"ms": float64(250)},
		},
	}
	edges := []compiler.Edge{
		{ID: "e1", Source: "trigger", Target: "delay", Role: compiler.EdgeFlow},
	}

	result, err := rt.Run(context.Background(), nodes, edges,
		runtime.RunOptions{Timeout: 10 * time.Second})
	if err != nil {
		panic(err)
	}

	fmt.Printf("status: %s\n", result.Status)     // status: succeeded
	fmt.Printf("state: %v\n", result.FinalState)  // state: map[text:hello]
}
```

Run a canvas JSON document directly against the embedded runtime:

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/asaidimu/hermes/pkg/compiler"
	"github.com/asaidimu/hermes/pkg/nodes" // registers all node kinds
	"github.com/asaidimu/hermes/pkg/runtime"
)

var canvasDoc = []byte(`{
  "nodes": [
    { "id": "n1", "type": "executable",
      "data": { "kind": "trigger", "config": { "initialState": { "count": 0 } } },
      "position": { "x": 0, "y": 0 } },
    { "id": "n2", "type": "executable",
      "data": { "kind": "code",
                "config": { "code": "return { count: state.count + 1 };" } },
      "position": { "x": 120, "y": 0 } }
  ],
  "edges": [
    { "id": "e1", "source": "n1", "target": "n2", "data": { "role": "flow" } }
  ]
}`)

func main() {
	rt := runtime.NewWorkflowRuntime(runtime.Options{})

	nodes, edges, err := compiler.DecodeWireGraph(canvasDoc)
	if err != nil {
		panic(err)
	}
	result, err := rt.Run(context.Background(), nodes, edges,
		runtime.RunOptions{Timeout: 10 * time.Second})
	if err != nil {
		panic(err)
	}

	fmt.Printf("status: %s\n", result.Status) // status: succeeded

	// Every step/stage/lifecycle event is in the unified action log.
	evs, _ := rt.GetEvents(context.Background(), result.RunID, 0, 0)
	fmt.Printf("events: %d\n", len(evs))
}
```

### Go API Reference

#### Runtime & Observability

| Symbol | Purpose |
| :--- | :--- |
| `compiler.DecodeWireGraph(doc)` | Canvas JSON → `([]Node, []Edge)` |
| `compiler.CompileWire(doc, registry)` | Canvas JSON → compiled `*pipeline.Workflow` |
| `compiler.WorkflowView(wf)` | Compiled workflow metadata as JSON-compatible maps |
| `rt.Run(ctx, nodes, edges, opts)` | Compile + register + execute; returns `RunResult` |
| `rt.GetEvents(ctx, runID, from, to)` | Action-log entries projected onto timeline wire shapes |
| `rt.ListRuns(ctx)` / `rt.GetRunMeta(ctx, runID)` | Run history from the unified log |
| `rt.Resume(runID, payload)` | Resume a paused run (event-sourced recovery when configured) |

#### Built-in Node Catalog

| Kind | Type | Description |
| :--- | :--- | :--- |
| `trigger` | executable | Entry point; seeds `initialState` and fires on trigger events |
| `arithmetic` | executable | Numeric operations on state values |
| `code` | executable | Sandboxed JavaScript transformation of workflow state (goja VM) |
| `http` | executable | Outbound HTTP request (method, url, headers, params, body, timeout) |
| `gemini` | executable | Structured prompt execution against Google Gemini models |
| `query` | executable | Executes queries against a connected service resource |
| `transformer` | executable | Declarative state transformations |
| `if` | executable | Conditional branch routed by handle |
| `switch` | executable | Multi-way branch over configured cases |
| `while` | executable | Condition-gated loop |
| `for-each` | executable | Iterates a collection with bounded concurrency |
| `try-catch` | executable | Error containment for subgraphs |
| `delay` | executable | Timed pause between steps |
| `pause` | executable | Pauses the run until a watched event or timeout (with pre-pause buffering) |
| `database` | resource | Database connection exposed as a run-scoped resource |

#### Core Go Types

| Symbol | Purpose |
| :--- | :--- |
| `pipeline.PipelineDefinition / Stage / Step` | Pipeline structure; steps carry `Action(ctx, PipelineContext, doc) (DocumentMutator, error)` |
| `pipeline.RoutingInstruction` | `Advance()`, `Terminate()`, `Jump(id)`, `JumpTo(addr)`, `Pause(id, timeout)` |
| `store.Store` / `NewMemoryStore` | Document persistence with `Read`/`Update`/`ExportJSON` |
| `events.ScopedEventBus` | Hierarchical pipeline → stage → step event fan-out (`PipelineEvent`) |
| `actionlog.Store` | Unified append-only log keyed by `(runID, rerunIndex)`; source of truth for recovery and history |
| `timeline.TimelineEvent` | Frontend wire projection derived from log entries |
| `registry.PipelineRegistry` | Thread-safe run tracking with pause-expiration timers |
| `compiler.Compile(nodes, edges)` | Graph → compiled workflow (trigger-bound pipelines + resources) |

### Common Use Cases

1. **API orchestration backend**: Embed the runtime in a Go service, register workflows compiled from user-drawn graphs, dispatch webhook payloads with `rt.Invoke(workflowID, triggerID, evt)`, and expose results through your own handlers.
2. **Visual builder preview & debugging**: Decode a frontend canvas with `compiler.DecodeWireGraph`, execute via `rt.Run`, then drive inspection from `rt.GetEvents` (unified log) while reading intermediate state via the run store.
3. **Scheduled data pipelines**: Register a workflow whose trigger is bound to a cron schedule (`"@daily"`); combine `http`, `gemini`, and `database` nodes to fetch, enrich, and persist data on recurring intervals, using `pause` + event watches for human-in-the-loop gates.

---

## Project Architecture

```
┌───────────────────────────────────────────────────────────────────┐
│  Frontend (Hedwig canvas JSON) ──▶ compiler.DecodeWireGraph ──▶  │
│                                    compiler.Compile ──▶ runtime   │
│                                                        │          │
│  Embedded Go consumers ────────▶ pipelines.go facade ──┤          │
│                                                        ▼          │
│              Workflow (trigger-bound pipelines + services)        │
│                                                        │          │
│                     pkg/pipeline (RSP engine: stages, steps,      │
│                     routers, checkpoints, subpipelines)           │
│                          │                    │                   │
│                 pkg/store (Anansi      pkg/events (scoped bus,    │
│                 documents, atomic      hierarchical paths)        │
│                 stage commits)               │                    │
│                 pkg/actionlog (unified append-only log:           │
│                 recovery + observability, keyed by                │
│                 runID + rerunIndex)                               │
│                                              ▼                    │
│                          pkg/timeline (recorder, snapshots,       │
│                          player)   pkg/scheduler   pkg/watch     │
└───────────────────────────────────────────────────────────────────┘
```

### Core Components
- **RSP Engine (`pkg/pipeline`)**: Definitions (`PipelineDefinition`, `Stage`, `Step`), the run context state machine executing stages with concurrent steps, the routing evaluator, checkpoint envelopes, and the subpipeline fork/join runner. Steps emit `DocumentMutator` closures which are merged and committed atomically at each stage boundary.
- **Document Store (`pkg/store`)**: Wraps Anansi `*document.Document` behind a transactional interface; the in-memory implementation pools documents for low-allocation steady-state performance.
- **Event System (`pkg/events`)**: `ScopedEventBus` with ancestor-aware `EventPath`s (`pipeline → stage → step`) so every emitted `PipelineEvent` carries full execution ancestry; supports child-to-parent bubbling.
- **Compiler (`pkg/compiler`)**: Turns a flat `{nodes, edges}` graph into a compiled workflow — trigger-bound pipelines, container scoping (`parentId` nesting), edge roles (`flow`, `dependency`, `placeholder`), and run-scoped resource services.
- **Runtime (`pkg/runtime`)**: Bus-driven orchestrator. Dispatches trigger events, spawns per-run stores, enforces per-workflow concurrency modes, records the unified action log, tracks outcomes, parks/resumes paused runs (single- or multi-event waits, any/all semantics), and integrates the scheduler.
- **Node Kit & Catalog (`pkg/nodekit`, `pkg/nodes/*`): Shared registration types (`NodeDefinition`, `HandleSpec`, runners/routers/resource hooks). Each kind's package registers itself via `init()`; the `pkg/nodes` aggregator pulls them all in. Every kind also declares a TypeScript twin consumed by the npm build.
- **Observability Log (`pkg/actionlog`)**: Append-only log keyed by `(runID, rerunIndex)` recording every step/stage/lifecycle event with state deltas. Source of truth for crash recovery (via `pkg/replay`) and run history; `pkg/timeline` holds only the frontend wire projection.
- **Registry (`pkg/registry`)**: Thread-safe tracking of active runs with `time.AfterFunc` pause-expiration timers and expiry hooks powering two-tier resumption.
- **Scheduler (`pkg/scheduler`)**: Pluggable cron/delay interface with an in-memory reference implementation suitable for single-process deployments.
- **Watch Service (`pkg/watch`, `pkg/runtime/watchservice.go`)**: Registers event watchers on behalf of `pause` nodes, buffers events arriving before the pause settles, and resolves waits by payload conditions.
- **Expression Sandbox (`pkg/expr`)**: goja-based JavaScript evaluation used by the `code` node and expression handling.
- **JS Package (`src/`, `scripts/build.ts`)**: Canonical wire types (`WorkflowNode`, `WorkflowEdge`, `PipelineEvent`, `RunOutcome`…), serialization helpers (`buildHandlesJS`, `buildRegistryJSON`), and the aggregated exports `NODE_DEFS`, `HANDLES`, `CATALOG`.

### Extension Points
- **Custom nodes**: Implement a `nodekit.NodeDefinition` (runner, optional router/router-func, resource init/cleanup, handles) and call `nodekit.Register(def)` — typically from a new `pkg/nodes/<kind>/` package imported by the aggregator. Add the `.ts` twin so it appears in `CATALOG`/`HANDLES`.
- **Pluggable infrastructure**: Inject alternative `events.ScopedEventBus`, `store.StoreFactory` (e.g., SQLite/disk-backed Anansi collections), `actionlog.Store` (e.g., durable collections), `scheduler.Scheduler`, and `runtime.EventSource` implementations through `runtime.Options`.
- **Generic state models**: `pipelines.NewFactoryFromModel[T]` derives factories reflecting arbitrary Go struct state schemas with zero boilerplate.
- **Routing policies**: Bounded nodes can supply `PipelinesRouterFunc` to inspect subpipeline results and decide follow-up instructions (used by `pause` to resume immediately on buffered events).
- **JS aggregation**: `scripts/build.ts` auto-discovers every `pkg/nodes/*/*.ts` definition — dropping a new file in a node directory is enough for it to ship in the npm package.

---

## Development & Contributing

### Available Scripts
- `go build ./...`: Compiles the entire Go workspace.
- `go test ./...`: Runs all unit and integration tests.
- `go test -race ./...`: Concurrency validation under the race detector.
- `bun run build`: Aggregates per-kind TS defs into `src/generated.ts`, bundles `dist/index.mjs` + `dist/index.cjs`, and emits declarations.
- `bun run typecheck`: Type-checks the TypeScript sources with `tsc --noEmit`.

Releases are automated with `semantic-release` (conventional commits → changelog → GitHub release).

### Testing & Quality Standard
```bash
go test -race ./...
bun run typecheck   # when touching TS sources
```
The suite covers the core engine (`tests/pipeline_test.go`), subpipelines, pause/resume, unified-log recording and state derivation (`tests/unified_log_test.go`), the wire translation layer (`pkg/compiler/wire_test.go`), if-node routing on a user workflow graph (`tests/if_node_repro_test.go`), and handle parity between Go and TS definitions (`pkg/nodekit/handles_parity_test.go`). CI runs tests and a build on every push/PR to `main`.

### Contributing Guidelines
1. Fork the project repository.
2. Create a feature branch (`git checkout -b feature/amazing-feature`).
3. Commit changes (`git commit -m 'feat: add amazing feature'`).
4. Push to branch (`git push origin feature/amazing-feature`).
5. Open a Pull Request.

When adding a node kind, include both `<kind>.go` and `<kind>.ts` in its package, register it in `pkg/nodes/nodes.go`, and extend the parity tests.

---

## Troubleshooting & FAQ

### Troubleshooting
- **Issue**: `unknown node kind "<kind>"` when compiling a graph  
  **Solution**: Blank-import the catalog so `init()` registrations run: `_ "github.com/asaidimu/hermes/pkg/nodes"`.

- **Issue**: `src/generated.ts` missing after cloning  
  **Solution**: It is intentionally gitignored and regenerated by `bun run build`; never edit it by hand.

- **Issue**: A run never leaves `paused` status
  **Solution**: Paused runs wait for a watched event (`rt.Resume(runID, payload)` or an event-source dispatch) or expire per the pause timeout; verify the event type and payload conditions match the `pause` node's watch descriptor.

### FAQ
- **Q**: How do frontend canvases reach the engine?
  **A**: Through `compiler.DecodeWireGraph` / `compiler.CompileWire`, which translate canvas JSON (`{nodes, edges}`) into compiled workflows — no HTTP layer involved.

- **Q**: Where does workflow state live?
  **A**: In an Anansi `*document.Document` created per run. Steps mutate it through `DocumentMutator` functions applied atomically at stage boundaries; `rt.Store(runID)` exports it as JSON.

- **Q**: Is user-supplied JavaScript safe to execute?  
  **A**: The `code` node evaluates scripts inside a sandboxed goja VM (no host I/O), rather than spawning processes.

- **Q**: How do Go and TypeScript stay in sync?  
  **A**: Each node kind carries paired implementations, and a dedicated handle-parity test asserts the Go and TS handle specs agree, preventing drift.

- **Q**: Does pausing survive process restarts?  
  **A**: Yes — checkpoints record the resume address (`EntryAddress`) in document metadata; resumption prefers the live registry cache and falls back to cold-storage document reload.

---

## License
Distributed under MIT-style permissive terms. See [`LICENSE.md`](./LICENSE.md) for the full text (author/date placeholders pending finalization; the npm manifest is currently marked `UNLICENSED`/private until first publication).

## Acknowledgments
- [`go-anansi`](https://github.com/asaidimu/go-anansi) — schema-backed document containers, pooling, and storage engine
- [`go-events`](https://github.com/asaidimu/go-events) — scoped event buses and durable Pebble LSM event sourcing
- [`goja`](https://github.com/dop251/goja) — ECMAScript runtime embedded for sandboxed code nodes
- [`pebble`](https://github.com/cockroachdb/pebble) — LSM storage backing durable event logs
- The **Hedwig** UI canvas — the frontend contract that drives the wire-parity guarantees
