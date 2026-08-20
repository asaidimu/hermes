# Routing Sequential Pipeline (RSP) Engine for Go

**Module**: `github.com/asaidimu/pipelines`  
**Specification Version**: `2.1.0`  
**Target Language**: Go 1.22+  
**Foundational Ecosystem**:
- **Data Container & Storage**: `github.com/asaidimu/go-anansi/v8` (`core/document.Document`, `data.Documenter`, and Anansi collection/storage engine)
- **Event Bus & Event Sourcing**: `github.com/asaidimu/go-events/v2` (`SimpleEventBus`, durable Pebble LSM event log)

---

## 1. Overview & Architectural Goals

The `pipelines` library is an asynchronous, state-machine-driven workflow orchestrator for Go. Built natively on **Go-Anansi** for schema-backed, container-addressed state management and **Go-Events** for durable, ordered event distribution, it provides **100% wire and protocol parity with the existing frontend / UI ecosystem**.

### Core Guarantees:
1. **Frontend Wire Parity**: Exact JSON-level compatibility for all REST APIs, timeline events, metadata envelopes, and run outcomes used by the UI canvas and inspection panels.
2. **Schema-Addressed Data Container (`*document.Document`)**: Workflow state lives inside a high-performance `*document.Document` (implementing `data.Documenter`), providing typed path resolution, schema validation, metadata separation, model binding, and fast pooling.
3. **Anansi-Backed Store Engine (`Store`)**: Pipeline stores wrap Anansi storage layers (ephemeral memory, SQLite, or disk collections) with atomic stage transaction boundaries.
4. **Durable & Live Event Bus (`go-events`)**: All pipeline, stage, router, and step events publish to `go-events`, supporting live in-memory fanout and persistent Pebble LSM event logs.
5. **Conditional State Machine Routing**: Stages dynamically jump, advance, terminate, or pause based on document queries and step settlements.
6. **Atomic Stage Boundaries**: Concurrent step operations produce isolated mutations that are merged and committed atomically to the Anansi document container.
7. **Checkpoint-Based Pause & Resume**: Suspend workflows at stage boundaries, serialize document state and resume addresses, and restore from live registry cache or cold storage.
8. **Time-Travel Observability (Timeline)**: Sequence-indexed recording, snapshotting, and playback directly consumable by the frontend timeline player.

---

## 2. Frontend Protocol & Wire Compatibility Contract

To ensure the existing frontend / UI visual canvas, timeline slider, and node debugger work with zero client modifications, the Go engine and its HTTP server adhere strictly to the following wire contracts.

### 2.1 Timeline Event Wire Schema (`TimelineEvent`)

```go
type TimelineEventSource string

const (
	SourcePipeline  TimelineEventSource = "pipeline"
	SourceStore     TimelineEventSource = "store"
	SourceContainer TimelineEventSource = "container"
	SourceLogger    TimelineEventSource = "logger"
)

// TimelineEvent matches the exact JSON schema expected by the frontend timeline scrubber.
type TimelineEvent struct {
	RunID     string                 `json:"runId"`
	Seq       int64                  `json:"seq"`
	Timestamp int64                  `json:"timestamp"` // Epoch Milliseconds
	Source    TimelineEventSource    `json:"source"`
	Type      string                 `json:"type"`
	Payload   map[string]any         `json:"payload"`
	Delta     map[string]any         `json:"delta,omitempty"`
	Snapshot  map[string]any         `json:"snapshot,omitempty"`
}
```

### 2.2 Run Metadata Wire Schema (`RunTimelineMeta`)

```go
type RunTimelineStatus string

const (
	StatusRecording RunTimelineStatus = "recording"
	StatusComplete  RunTimelineStatus = "complete"
	StatusFailed    RunTimelineStatus = "failed"
	StatusPaused    RunTimelineStatus = "paused"
)

// RunTimelineMeta matches the response of GET /runs/:runId
type RunTimelineMeta struct {
	RunID            string            `json:"runId"`
	PipelineID       string            `json:"pipelineId"`
	StartTime        int64             `json:"startTime"`          // Epoch Milliseconds
	EndTime          *int64            `json:"endTime,omitempty"`   // Epoch Milliseconds
	EventCount       int64             `json:"eventCount"`
	Status           RunTimelineStatus `json:"status"`
	SnapshotInterval int               `json:"snapshotInterval"`
	SnapshotSeqs     []int64           `json:"snapshotSeqs"`
	Metadata         map[string]any    `json:"metadata,omitempty"`
}
```

### 2.3 Run Outcome Wire Schema (`RunOutcome`)

```go
// RunOutcome matches the response of GET /runs/:runId/outcome
type RunOutcome struct {
	OK              bool     `json:"ok"`
	Status          string   `json:"status"` // "success" | "failed" | "paused"
	ExecutedNodeIDs []string `json:"executedNodeIds,omitempty"`
	Error           *string  `json:"error,omitempty"`
}
```

### 2.4 Complete Frontend HTTP REST API Surface

| Method | Route | Request Body | Response Body / Status | Description |
| :--- | :--- | :--- | :--- | :--- |
| `GET` | `/registry` | - | `map[string]NodeDefinition` | Returns registered visual node descriptors |
| `POST` | `/run` | `{"nodes": [...], "edges": [...]}` | `{"runId": "uuid"}` | Dev convenience: compile DAG + trigger run |
| `POST` | `/compile` | `{"nodes": [...], "edges": [...]}` | `CompiledWorkflowJSON` | Compiles visual graph to pipeline definition |
| `POST` | `/register` | `{"workflow": {...}}` | `{"ok": true}` | Registers a compiled workflow |
| `POST` | `/deregister` | `{"workflowId": "id"}` | `{"ok": true}` | Deregisters workflow from runtime |
| `POST` | `/events` | `{"type": "...", "payload": {...}}` | `{"ok": true}` | Dispatches external trigger event |
| `GET` | `/runs` | - | `[]RunTimelineMeta` | Lists all workflow execution runs |
| `GET` | `/runs/:runId` | - | `RunTimelineMeta` (404 if not found) | Returns metadata for a run |
| `GET` | `/runs/:runId/outcome` | - | `RunOutcome` (404 if not found) | Returns settlement status of run |
| `GET` | `/runs/:runId/events` | - | `[]TimelineEvent` | Returns chronological event log for timeline slider |
| `GET` | `/runs/:runId/store` | - | `map[string]any` | Returns Anansi Document JSON state |
| `GET` | `/handles.js` | - | `application/javascript` | Evaluatable handle functions for UI canvas ports |
| `POST` | `/runs/:runId/abort` | - | `{"ok": true}` | Signals run cancellation via context |

---

## 3. Data Container & Storage Integration (`go-anansi`)

### 3.1 `*document.Document` as Pipeline State

Workflow state is encapsulated in Anansi's `*document.Document`. Steps read and modify state via path-based accessors, typed bindings, or document mutators. When serialized for `GET /runs/:runId/store`, it emits standard JSON matching frontend expectations.

```go
package pipeline

import (
	"context"
	"time"

	"github.com/asaidimu/go-anansi/v8/core/data"
	"github.com/asaidimu/go-anansi/v8/core/document"
	"github.com/asaidimu/go-anansi/v8/core/schema/definition"
)

// DocumentMutator applies atomic modifications to an Anansi Document.
type DocumentMutator func(doc *document.Document) error

// Store manages the persistence and atomic transaction lifecycle of an Anansi Document.
type Store interface {
	Document() *document.Document
	Read(fn func(doc *document.Document) error) error
	Update(ctx context.Context, mutator DocumentMutator) error
	Transaction(ctx context.Context, fn func(txDoc *document.Document) error) error
	Ready(ctx context.Context) error
	ExportJSON() (map[string]any, error)
}
```

---

## 4. Event Bus Integration (`go-events`)

Pipelines use `github.com/asaidimu/go-events/v2` (`SimpleEventBus[PipelineEvent]`) for event dissemination, UI streaming, and timeline persistence.

```go
import "github.com/asaidimu/go-events/v2"

// PathNode represents an ancestor in the hierarchical execution path.
type PathNode struct {
	Kind  string `json:"kind"`  // "pipeline" | "stage" | "step"
	ID    string `json:"id"`
	Label string `json:"label"`
}

type EventPath []PathNode

// PipelineEvent is emitted across all stage/step boundaries.
type PipelineEvent struct {
	Type       string         `json:"type"`
	RunID      string         `json:"runId"`
	PipelineID string         `json:"pipelineId"`
	Path       EventPath      `json:"path"`
	Timestamp  int64          `json:"timestamp"` // Epoch ms
	Duration   int64          `json:"duration,omitempty"` // ms
	Payload    map[string]any `json:"payload,omitempty"`
}

type ScopedEventBus interface {
	Emit(ctx context.Context, eventType string, evt PipelineEvent)
	Subscribe(eventType string, handler func(ctx context.Context, evt PipelineEvent) error) (unsubscribe func())
}
```

---

## 5. Pipeline & Stage Definitions

```go
type PipelineDefinition struct {
	ID          string
	Label       string
	Schema      *definition.CompiledSchema
	Stages      []Stage
}

type Stage struct {
	ID              string
	Order           int
	Label           string
	Timeout         time.Duration
	Steps           map[string]Step
	Router          StepStageRouter
	Pipelines       []PipelineDefinition
	PipelinesRouter PipelineStageRouter
}

type Step struct {
	ID      string
	Label   string
	Timeout time.Duration
	Retries int
	Action  func(ctx context.Context, pcxt PipelineContext, doc *document.Document) (DocumentMutator, error)
}

type PipelineContext interface {
	RunID() string
	PipelineID() string
	StageID() string
	StepID() string
	Logger() Logger
}
```

---

## 6. Routing Engine & Instruction Set

```go
type RoutingInstruction interface {
	isRoutingInstruction()
}

type (
	AdvanceInstruction   struct{}
	TerminateInstruction struct{}
	JumpInstruction      struct{ StageID string }
	JumpToInstruction    struct{ Address EntryAddress }
	PauseInstruction     struct {
		StageID string
		Timeout time.Duration
		Persist bool
	}
)

func Advance() RoutingInstruction                 { return AdvanceInstruction{} }
func Terminate() RoutingInstruction               { return TerminateInstruction{} }
func Jump(stageID string) RoutingInstruction      { return JumpInstruction{StageID: stageID} }
func JumpTo(addr EntryAddress) RoutingInstruction { return JumpToInstruction{Address: addr} }
func Pause(stageID string, timeout time.Duration) RoutingInstruction {
	return PauseInstruction{StageID: stageID, Timeout: timeout, Persist: true}
}
```

---

## 7. Execution Engine & Lifecycle (`RunContext`)

```go
type PipelineRunResult struct {
	Status     string              `json:"status"` // "succeeded" | "paused" | "failed"
	RunID      string              `json:"runId"`
	FinalDoc   *document.Document  `json:"-"`
	Checkpoint *PipelineCheckpoint `json:"checkpoint,omitempty"`
	Error      error               `json:"error,omitempty"`
}

type RunContext interface {
	ID() string
	PipelineID() string
	Store() Store
	Run(ctx context.Context) (PipelineRunResult, error)
	Abort(err error)
	Write(mutator DocumentMutator)
	On(event string, handler func(ctx context.Context, evt PipelineEvent) error) (unsubscribe func())
}
```

---

## 8. Checkpoints & Resumption Parity

### 8.1 Checkpoint Envelope
Checkpoints are stored in Anansi document metadata under `__pipeline_data__.checkpoints[pipelineID]`:

```go
type EntryAddress struct {
	Stage    string               `json:"stage"`
	Step     string               `json:"step,omitempty"`
	Pipeline *SubPipelineAddress  `json:"pipeline,omitempty"`
}

type SubPipelineAddress struct {
	Index int    `json:"index"`
	Stage string `json:"stage"`
	Step  string `json:"step,omitempty"`
}

type PipelineCheckpoint struct {
	RunID              string       `json:"runId"`
	PipelineID         string       `json:"pipelineId"`
	ResumeAt           EntryAddress `json:"resumeAt"`
	PausedAtStageID    string       `json:"pausedAtStageId"`
	PausedAtStageLabel string       `json:"pausedAtStageLabel"`
	PausedOn           string       `json:"pausedOn"` // ISO-8601
}
```

---

## 9. Timeline Engine (Recording & Time-Travel Playback)

The `TimelineStore` implements chronological storage and snapshotting, matching the frontend's expected format:

```go
type TimelineStore interface {
	Append(ctx context.Context, event TimelineEvent) error
	GetRunMeta(ctx context.Context, runID string) (*RunTimelineMeta, error)
	ListRuns(ctx context.Context) ([]RunTimelineMeta, error)
	GetEvents(ctx context.Context, runID string, fromSeq, toSeq int64) ([]TimelineEvent, error)
	GetNearestSnapshot(ctx context.Context, runID string, seq int64) (*TimelineEvent, error)
}
```

---

## 10. Project Directory Layout

```
pipelines/
├── go.mod
├── go.sum
├── SPEC.md
├── SCHEMA_AND_DURABILITY.md
├── pkg/
│   ├── core/                  # Errors and logger interfaces
│   ├── store/                 # Anansi Store implementation & JSON exporter
│   ├── pipeline/              # Core RSP Engine
│   │   ├── types.go           # PipelineDefinition, Stage, Step, Instructions
│   │   ├── factory.go         # PipelineFactory
│   │   ├── context.go         # RunContextImpl & Execution State Machine
│   │   ├── stage.go           # Concurrent step runner & atomic commit
│   │   ├── router.go          # Routing evaluator
│   │   ├── checkpoint.go      # Checkpoint envelope & serialization
│   │   ├── events.go          # Event definitions & ScopedEventBus
│   │   └── subpipeline.go     # Subpipeline Fork/Join engine
│   ├── registry/              # Run registry & auto-expiration timers
│   ├── timeline/              # Timeline recorder, store & playback engine
│   └── server/                # Frontend HTTP/REST API Server (100% Wire Parity)
└── tests/
    ├── pipeline_test.go
    ├── subpipeline_test.go
    ├── pause_resume_test.go
    ├── timeline_test.go
    └── frontend_api_test.go   # End-to-end HTTP parity tests
```
