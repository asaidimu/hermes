package pipeline

import (
	"context"
	"time"

	"github.com/asaidimu/go-anansi/v8/core/document"
	"github.com/asaidimu/go-anansi/v8/core/schema/definition"
	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/store"
)

// PipelineContext provides execution metadata to running steps.
type PipelineContext interface {
	RunID() string
	PipelineID() string
	StageID() string
	StepID() string
	Path() events.EventPath
	Logger() core.Logger
	// ResolveResource resolves a run-scoped resource artifact key (e.g.
	// "resource:db-1") into the initialized handle. Implementations without a
	// resource manager return (nil, false).
	ResolveResource(key string) (any, bool)
}

type pipelineContextImpl struct {
	runID            string
	pipelineID       string
	stageID          string
	stepID           string
	path             events.EventPath
	logger           core.Logger
	resourceResolver func(key string) (any, bool)
}

func (c *pipelineContextImpl) RunID() string          { return c.runID }
func (c *pipelineContextImpl) PipelineID() string     { return c.pipelineID }
func (c *pipelineContextImpl) StageID() string        { return c.stageID }
func (c *pipelineContextImpl) StepID() string         { return c.stepID }
func (c *pipelineContextImpl) Path() events.EventPath { return c.path }
func (c *pipelineContextImpl) Logger() core.Logger    { return c.logger }

func (c *pipelineContextImpl) ResolveResource(key string) (any, bool) {
	if c.resourceResolver != nil {
		return c.resourceResolver(key)
	}
	return nil, false
}

// WithResourceResolver attaches a run-scoped resource resolver to a context
// built by NewPipelineContext. The runtime (pkg/runtime) uses this to inject
// initialized resource handles into steps that declare resource dependencies.
func WithResourceResolver(resolver func(key string) (any, bool)) func(*pipelineContextImpl) {
	return func(c *pipelineContextImpl) {
		c.resourceResolver = resolver
	}
}

func NewPipelineContext(runID, pipelineID, stageID, stepID string, path events.EventPath, logger core.Logger, opts ...func(*pipelineContextImpl)) PipelineContext {
	if logger == nil {
		logger = core.NopLogger{}
	}
	c := &pipelineContextImpl{
		runID:      runID,
		pipelineID: pipelineID,
		stageID:    stageID,
		stepID:     stepID,
		path:       path,
		logger:     logger,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// StepAction executes user logic on the Anansi document and returns a mutator to commit.
type StepAction func(ctx context.Context, pcxt PipelineContext, state *document.Document) (store.DocumentMutator, error)

// Step represents a single concurrent unit of execution within a Stage.
type Step struct {
	ID      string
	Label   string
	Timeout time.Duration
	Retries int
	Action  StepAction
}

// RoutingInstruction defines state transitions after stage completion.
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

func (AdvanceInstruction) isRoutingInstruction()   {}
func (TerminateInstruction) isRoutingInstruction() {}
func (JumpInstruction) isRoutingInstruction()      {}
func (JumpToInstruction) isRoutingInstruction()    {}
func (PauseInstruction) isRoutingInstruction()     {}

func Advance() RoutingInstruction                 { return AdvanceInstruction{} }
func Terminate() RoutingInstruction               { return TerminateInstruction{} }
func Jump(stageID string) RoutingInstruction      { return JumpInstruction{StageID: stageID} }
func JumpTo(addr EntryAddress) RoutingInstruction { return JumpToInstruction{Address: addr} }
func Pause(stageID string, timeout time.Duration) RoutingInstruction {
	return PauseInstruction{StageID: stageID, Timeout: timeout, Persist: true}
}

// StepStageRouter evaluates routing instructions based on the document after stage steps complete.
type StepStageRouter func(ctx context.Context, doc *document.Document, st store.Store) (RoutingInstruction, error)

// PipelineStageRouter evaluates routing instructions after subpipelines settle.
type PipelineStageRouter func(ctx context.Context, doc *document.Document, results []PipelineRunResult, st store.Store) (RoutingInstruction, error)

// Stage represents a sequential step/subpipeline block within a Pipeline.
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

// PipelineDefinition declares the static DAG/pipeline structure.
type PipelineDefinition struct {
	ID          string
	Label       string
	Schema      *definition.CompiledSchema
	Stages      []Stage
}

// PipelineRunResult represents the terminal or paused state of a pipeline execution.
type PipelineRunResult struct {
	Status     string              `json:"status"` // "succeeded" | "paused" | "failed" | "aborted"
	RunID      string              `json:"runId"`
	PipelineID string              `json:"pipelineId"`
	FinalDoc   *document.Document  `json:"-"`
	Checkpoint *PipelineCheckpoint `json:"checkpoint,omitempty"`
	Error      error               `json:"error,omitempty"`
}

// RunContext manages the execution lifecycle of a pipeline.
type RunContext interface {
	ID() string
	PipelineID() string
	Store() store.Store
	EventBus() events.ScopedEventBus
	Run(ctx context.Context) (PipelineRunResult, error)
	Abort(err error)
	Write(mutator store.DocumentMutator)
	On(eventType string, handler events.EventHandler) (unsubscribe func())
}
