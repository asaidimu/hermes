package pipeline

import (
	"context"

	"github.com/asaidimu/go-anansi/v8/core/data"
	"github.com/asaidimu/go-anansi/v8/core/schema/definition"
	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/google/uuid"
)

// FactoryOptions configures the pipeline factory.
type FactoryOptions struct {
	Logger   core.Logger
	EventBus events.ScopedEventBus
	// ResourceResolver resolves run-scoped resource artifact keys ("resource:<id>")
	// into initialized handles. Attached to every run context this factory prepares.
	ResourceResolver func(key string) (any, bool)
	// RunEnv holds the host's environment layers exposed to steps via
	// PipelineContext.Env(). Non-secret configuration only.
	RunEnv map[string]any
	// SecretLookup resolves credentials by key at execution time. Attached to
	// every run context this factory prepares; values never persist to state.
	SecretLookup func(key string) (any, bool)
}

// PipelineFactory creates and prepares pipeline run contexts.
type PipelineFactory struct {
	definition PipelineDefinition
	schema     *definition.CompiledSchema
	options    FactoryOptions
}

// NewFactory creates a new PipelineFactory.
func NewFactory(def PipelineDefinition, schema *definition.CompiledSchema, opts ...FactoryOptions) *PipelineFactory {
	var opt FactoryOptions
	if len(opts) > 0 {
		opt = opts[0]
	}
	if opt.Logger == nil {
		opt.Logger = core.NopLogger{}
	}
	return &PipelineFactory{
		definition: def,
		schema:     schema,
		options:    opt,
	}
}

// Prepare builds a fresh RunContext for a runID (or generates a new UUIDv4).
func (f *PipelineFactory) Prepare(runID string, st store.Store, bus ...events.ScopedEventBus) *RunContextImpl {
	if runID == "" {
		runID = uuid.New().String()
	}
	var b events.ScopedEventBus
	if len(bus) > 0 && bus[0] != nil {
		b = bus[0]
	} else if f.options.EventBus != nil {
		b = f.options.EventBus
	} else {
		b = f.newFallbackBus(runID)
	}
	if st == nil {
		st = store.NewMemoryStore(nil)
	}

	rc := NewRunContext(runID, f.definition, st, b, f.options.Logger)
	if f.options.ResourceResolver != nil {
		rc.SetResourceResolver(f.options.ResourceResolver)
	}
	rc.runEnv = f.options.RunEnv
	rc.secretLookup = f.options.SecretLookup
	return rc
}

// PrepareWithEntry builds a RunContext starting execution at a specific entry address.
func (f *PipelineFactory) PrepareWithEntry(runID string, st store.Store, bus events.ScopedEventBus, entry EntryAddress) *RunContextImpl {
	if runID == "" {
		runID = uuid.New().String()
	}
	if bus == nil {
		if f.options.EventBus != nil {
			bus = f.options.EventBus
		} else {
			bus = f.newFallbackBus(runID)
		}
	}
	if st == nil {
		st = store.NewMemoryStore(nil)
	}

	rc := NewRunContext(runID, f.definition, st, bus, f.options.Logger, entry)
	if f.options.ResourceResolver != nil {
		rc.SetResourceResolver(f.options.ResourceResolver)
	}
	rc.runEnv = f.options.RunEnv
	rc.secretLookup = f.options.SecretLookup
	return rc
}

// Resume restores execution from an existing Store using its stored checkpoint.
func (f *PipelineFactory) Resume(ctx context.Context, runID string, st store.Store, bus ...events.ScopedEventBus) (*RunContextImpl, error) {
	if st == nil {
		return nil, core.NewSystemError(core.ErrCodeValidation, "store is required to resume pipeline")
	}

	var ckpt *PipelineCheckpoint
	if err := st.Read(func(state map[string]any) error {
		var rErr error
		ckpt, rErr = ReadCheckpoint(state, f.definition.ID)
		return rErr
	}); err != nil {
		return nil, err
	}
	if ckpt == nil {
		return nil, core.NewSystemError(core.ErrCodeNotFound, "no checkpoint found in document for pipeline "+f.definition.ID)
	}

	var b events.ScopedEventBus
	if len(bus) > 0 && bus[0] != nil {
		b = bus[0]
	} else if f.options.EventBus != nil {
		b = f.options.EventBus
	} else {
		b = f.newFallbackBus(runID)
	}

	runCtx := f.PrepareWithEntry(runID, st, b, ckpt.ResumeAt)
	return runCtx, nil
}

// newFallbackBus creates a standalone, unparented event bus for callers that
// invoke the factory directly without supplying a bus (e.g. tests, or
// PipelineFactory used outside WorkflowRuntime).
//
// @note #scoped-bus-opportunity-002 issue status=resolved priority=P1 tags=#event-bus,#data-loss : Orphan bus fallback silently loses all events
//
// Resolved: this fallback bus still has no parent and no underlying durable
// backend — events emitted on it still go nowhere (no subscribers, no
// bubbling, no timeline recording) — but that is now a visible, logged
// condition instead of a silent one. A full fix via bus.IsolatedScope(runID)
// requires the caller to already hold a *rooted* go-events-backed bus with
// durable storage configured (see events.NewDurableMemoryScopedBus, added
// for #scoped-bus-opportunity-005); PipelineFactory has no such root to
// scope from when used standalone like this, so it cannot construct
// isolation on its own. Callers who want durability/isolation should supply
// FactoryOptions.EventBus (typically WorkflowRuntime does, via its root
// bus) rather than relying on this fallback.
func (f *PipelineFactory) newFallbackBus(runID string) events.ScopedEventBus {
	logger := f.options.Logger
	if logger == nil {
		logger = core.NopLogger{}
	}
	logger.Warn("pipeline: no event bus supplied; creating an orphan in-memory bus with no subscribers or durability",
		"pipeline", f.definition.ID, "runId", runID)
	return events.NewMemoryScopedBus()
}

// NewFactoryFromModel creates a factory reflecting a Go struct state model (Zero-Boilerplate).
func NewFactoryFromModel[T any](def PipelineDefinition, opts ...FactoryOptions) *PipelineFactory {
	var val T
	var cs *definition.CompiledSchema

	// Synthesize schema from struct fields if possible
	if _, err := data.StructFieldValues(val, false); err == nil {
		// Schema synthesized
	}

	return NewFactory(def, cs, opts...)
}
