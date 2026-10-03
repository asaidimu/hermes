package pipeline

import (
	"context"
	"time"

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

// NewReplayContext creates a minimal PipelineContext suitable for replay.
// During replay, steps only need to compute their mutator — they don't
// need event bus, logger, or resource resolution. All optional methods
// return safe defaults.
func NewReplayContext() PipelineContext {
	return &pipelineContextImpl{
		logger: core.NopLogger{},
	}
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
	// Effect classifies this step for event-sourced replay purposes.
	// EffectSideEffecting steps have their outcomes recorded in the action
	// log; EffectPure steps are re-executed on replay. Set by
	// nodekit.BuildStep from the node definition's classification.
	// A zero value (EffectUnspecified) means "unknown" — the step is
	// treated conservatively as effectful for safety.
	Effect int
}

// RoutingKind discriminates a RoutingInstruction (R.2: ONE canonical form —
// the former interface + five marker structs collapsed into a tagged struct;
// the kind switch in evaluateInstruction was already the interpreter).
type RoutingKind int

const (
	// RouteNone is the zero value: "no instruction". A stage router
	// returning the zero struct means "advance"; a node RouterFunc
	// returning it means "follow the default edge" — exactly what a nil
	// interface meant before the tagged struct.
	RouteNone RoutingKind = iota
	RouteAdvance
	RouteTerminate
	RouteJump
	RouteJumpTo
	RoutePause
)

// RoutingInstruction defines state transitions after stage completion.
// Zero value is RouteNone ("no instruction"), never a valid decision by
// itself; constructors below set the kind explicitly.
type RoutingInstruction struct {
	Kind    RoutingKind
	StageID string // Jump target / explicit pause resume stage
	Address EntryAddress // JumpTo

	// Pause-only fields.
	Timeout       time.Duration
	Persist       bool
	WaitForEvent  string   // single event (backward compat)
	WaitForEvents []string // multiple events to wait for
	WaitMode      string   // "any" (default) or "all"
	Cron          string   // cron expression for auto-resume (e.g. "@every 5m")
}

func Advance() RoutingInstruction                 { return RoutingInstruction{Kind: RouteAdvance} }
func Terminate() RoutingInstruction               { return RoutingInstruction{Kind: RouteTerminate} }
func Jump(stageID string) RoutingInstruction      { return RoutingInstruction{Kind: RouteJump, StageID: stageID} }
func JumpTo(addr EntryAddress) RoutingInstruction { return RoutingInstruction{Kind: RouteJumpTo, Address: addr} }
func Pause(stageID string, timeout time.Duration) RoutingInstruction {
	return RoutingInstruction{Kind: RoutePause, StageID: stageID, Timeout: timeout, Persist: true}
}

// PauseForEvent returns a RoutingInstruction that waits for a specific event type.
func PauseForEvent(eventType string, timeout time.Duration) RoutingInstruction {
	return RoutingInstruction{Kind: RoutePause, Timeout: timeout, Persist: true, WaitForEvent: eventType}
}

// PauseForEvents returns a RoutingInstruction that waits for multiple event types.
// mode should be "any" (resume when any event arrives) or "all" (resume when all arrive).
func PauseForEvents(events []string, mode string, timeout time.Duration) RoutingInstruction {
	if mode == "" {
		mode = "any"
	}
	return RoutingInstruction{Kind: RoutePause, Timeout: timeout, Persist: true, WaitForEvents: events, WaitMode: mode}
}

// PauseForCron returns a RoutingInstruction that auto-resumes after a cron delay.
// The cron expression supports "@every 5m", "30 9 * * *", etc.
func PauseForCron(eventType string, cron string) RoutingInstruction {
	return RoutingInstruction{Kind: RoutePause, Persist: true, WaitForEvent: eventType, Cron: cron}
}

// StepStageRouter evaluates routing instructions based on a state snapshot
// taken after stage steps complete.
type StepStageRouter func(ctx context.Context, state map[string]any, st store.Store) (RoutingInstruction, error)

// PipelineStageRouter evaluates routing instructions after subpipelines settle.
type PipelineStageRouter func(ctx context.Context, state map[string]any, results []PipelineRunResult, st store.Store) (RoutingInstruction, error)

// Stage represents a sequential step/subpipeline block within a Pipeline.
//
// Immutability contract: once a Stage is compiled (e.g. returned from the
// distribute node's DynamicPipelines fan-out in pkg/nodekit/build.go), the
// engine treats it — and everything it transitively owns (the Steps map,
// Router/PipelinesRouter closures, nested Pipelines) — as read-only for the
// rest of that run. Concurrent children of a distribute/fork are handed
// shallow copies of the SAME underlying Stage values (see
// #review-20260910-018): this is safe only because nothing writes into a
// Stage's fields, its Steps map, or values captured by its closures at
// runtime. context.go's DynamicPipelines patch path mutates a
// per-iteration struct copy, never the shared original. Any new code that
// writes to a Stage or its Steps map after compilation turns concurrent
// distribute/fork children into a data race — build a fresh Stage (or a
// real deep clone) instead of mutating a shared one.
type Stage struct {
	ID              string
	Label           string
	Timeout         time.Duration
	Steps           map[string]Step
	Router          StepStageRouter
	Pipelines       []PipelineDefinition
	PipelinesRouter PipelineStageRouter
	Config          map[string]any
	// DynamicPipelines generates sub-pipelines at runtime from the current state.
	// Used by distribute (parallel for-each): each array element becomes a
	// sub-pipeline. If set, Pipelines is ignored and DynamicPipelines is called
	// during execution to produce the sub-pipeline list.
	DynamicPipelines func(state map[string]any) []PipelineDefinition
}

// PipelineDefinition declares the static DAG/pipeline structure.
type PipelineDefinition struct {
	ID     string
	Label  string
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

// Effect classifies whether a node's execution has real-world consequences
// (or other non-deterministic behavior) that must be durably recorded rather
// than safely re-derived by re-running the node, versus being a pure,
// deterministic function of its inputs that's safe to re-execute freely.
//
// This classification exists to support event-sourced recovery: a run can be
// reconstructed by replaying its pipeline definition, re-executing every
// EffectPure step fresh, while EffectSideEffecting steps have their prior
// outcome injected from a durable record instead of being re-run. See
// EXECUTION_ENGINE_REDESIGN_V2.md §2 for the full design this supports.
//
// The zero value, EffectUnspecified, is deliberately invalid — nodekit
// Register panics if a node is registered without an explicit
// classification. There is no default in either direction: assuming Pure is
// unsafe (a side-effecting node could be silently re-executed on replay,
// duplicating a real-world action like an HTTP call), and silently assuming
// SideEffecting for everything defeats the purpose of classifying at all
// (nothing would ever be cheaply re-derivable). Node authors must decide,
// and the registry enforces that they did.
//
// 20261003 refactor (plan D.4): the canonical enum lives here — pipeline
// reads Step.Effect and nodekit authors it — and the pkg/effect
// import-cycle workaround is deleted.
type Effect int

const (
	// EffectUnspecified is the zero value. Never a valid classification for
	// a registered node — Register rejects it.
	EffectUnspecified Effect = iota

	// EffectPure marks a node as a deterministic function of its config and
	// state, with no observable effect outside the pipeline run. Safe to
	// re-execute on replay/recovery; its outcome is never durably recorded.
	EffectPure

	// EffectSideEffecting marks a node as having consequences beyond this
	// pipeline run (a network call, a database write) or non-deterministic
	// behavior (wall-clock time, randomness) that must not be silently
	// duplicated or diverged on replay. Its outcome is durably recorded
	// before the step is considered complete, and a recorded outcome is
	// injected rather than the node being re-executed during recovery.
	EffectSideEffecting
)

// String returns a human-readable name, used in panic/error messages and in
// the registry's JSON representation (Effect is exposed to registry
// consumers, e.g. the visual editor, as this string via its json tag).
func (e Effect) String() string {
	switch e {
	case EffectPure:
		return "Pure"
	case EffectSideEffecting:
		return "SideEffecting"
	default:
		return "Unspecified"
	}
}

// Valid reports whether e is a real classification (not the zero value, and
// not an out-of-range value).
func (e Effect) Valid() bool {
	return e == EffectPure || e == EffectSideEffecting
}

// MarshalJSON renders Effect as its string name, so serialized node
// definitions expose a stable, readable value ("Pure" / "SideEffecting")
// rather than a bare integer that would be meaningless to consumers and
// would silently renumber if constants are ever reordered.
func (e Effect) MarshalJSON() ([]byte, error) {
	return []byte(`"` + e.String() + `"`), nil
}
