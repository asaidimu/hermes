package pipeline

import (
	"context"
	"time"

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
	// Env returns the run's environment layers (non-secret configuration from
	// the host). May be nil/empty when the host provides none.
	Env() map[string]any
	// ResolveSecret resolves a credential by key through the host-supplied
	// secret provider. Values are for immediate use only — implementations
	// must never persist them into state. Returns (nil, false) when the key
	// is unknown or no provider is configured.
	ResolveSecret(key string) (any, bool)
}

type pipelineContextImpl struct {
	runID            string
	pipelineID       string
	stageID          string
	stepID           string
	path             events.EventPath
	logger           core.Logger
	resourceResolver func(key string) (any, bool)
	runEnv           map[string]any
	secretLookup     func(key string) (any, bool)
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

func (c *pipelineContextImpl) Env() map[string]any { return c.runEnv }

func (c *pipelineContextImpl) ResolveSecret(key string) (any, bool) {
	if c.secretLookup == nil {
		return nil, false
	}
	return c.secretLookup(key)
}

// WithRunEnv attaches the run's environment layers to a context built by
// NewPipelineContext.
func WithRunEnv(env map[string]any) func(*pipelineContextImpl) {
	return func(c *pipelineContextImpl) { c.runEnv = env }
}

// WithSecretLookup attaches the run's credential lookup to a context built by
// NewPipelineContext.
func WithSecretLookup(lookup func(key string) (any, bool)) func(*pipelineContextImpl) {
	return func(c *pipelineContextImpl) { c.secretLookup = lookup }
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

// StepAction executes user logic against a read-only view of the run state
// and returns a mutator to commit. Mutate the passed map only via the
// returned mutator.
type StepAction func(ctx context.Context, pcxt PipelineContext, state map[string]any) (store.Mutator, error)

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
		StageID       string
		Timeout       time.Duration
		Persist       bool
		WaitForEvent  string   // single event (backward compat)
		WaitForEvents []string // multiple events to wait for
		WaitMode      string   // "any" (default) or "all"
		Cron          string   // cron expression for auto-resume (e.g. "@every 5m")
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

// PauseForEvent returns a PauseInstruction that waits for a specific event type.
func PauseForEvent(eventType string, timeout time.Duration) RoutingInstruction {
	return PauseInstruction{StageID: "", Timeout: timeout, Persist: true, WaitForEvent: eventType}
}

// PauseForEvents returns a PauseInstruction that waits for multiple event types.
// mode should be "any" (resume when any event arrives) or "all" (resume when all arrive).
func PauseForEvents(events []string, mode string, timeout time.Duration) RoutingInstruction {
	if mode == "" {
		mode = "any"
	}
	return PauseInstruction{StageID: "", Timeout: timeout, Persist: true, WaitForEvents: events, WaitMode: mode}
}

// PauseForCron returns a PauseInstruction that auto-resumes after a cron delay.
// The cron expression supports "@every 5m", "30 9 * * *", etc.
func PauseForCron(eventType string, cron string) RoutingInstruction {
	return PauseInstruction{StageID: "", Timeout: 0, Persist: true, WaitForEvent: eventType, Cron: cron}
}

// StepStageRouter evaluates routing instructions based on a state snapshot
// taken after stage steps complete.
type StepStageRouter func(ctx context.Context, state map[string]any, st store.Store) (RoutingInstruction, error)

// PipelineStageRouter evaluates routing instructions after subpipelines settle.
type PipelineStageRouter func(ctx context.Context, state map[string]any, results []PipelineRunResult, st store.Store) (RoutingInstruction, error)

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
	Config          map[string]any
}

// PipelineDefinition declares the static DAG/pipeline structure.
type PipelineDefinition struct {
	ID     string
	Label  string
	Schema *definition.CompiledSchema
	Stages []Stage
}

// PipelineRunResult represents the terminal or paused state of a pipeline execution.
type PipelineRunResult struct {
	Status        string              `json:"status"` // "succeeded" | "paused" | "failed" | "aborted"
	RunID         string              `json:"runId"`
	PipelineID    string              `json:"pipelineId"`
	FinalState    map[string]any      `json:"-"`
	Checkpoint    *PipelineCheckpoint `json:"checkpoint,omitempty"`
	WaitForEvent  string              `json:"waitForEvent,omitempty"`  // single event (backward compat)
	WaitForEvents []string            `json:"waitForEvents,omitempty"` // multiple events
	WaitMode      string              `json:"waitMode,omitempty"`      // "any" or "all"
	Error         error               `json:"error,omitempty"`
}

// RunContext manages the execution lifecycle of a pipeline.
type RunContext interface {
	ID() string
	PipelineID() string
	Store() store.Store
	EventBus() events.ScopedEventBus
	Run(ctx context.Context) (PipelineRunResult, error)
	Abort(err error)
	Write(mutator store.Mutator)
	On(eventType string, handler events.EventHandler) (unsubscribe func())
}
