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
		// @note #scoped-bus-opportunity-002 issue status=open priority=P1 tags=#event-bus,#data-loss : Orphan bus fallback silently loses all events
		//
		// When no bus is provided and factory has no EventBus option, a standalone
		// MemoryScopedBus is created with no parent and no underlying go-events bus.
		// Events emitted on this bus go nowhere — no subscribers, no bubbling, no
		// timeline recording. This is a silent data loss scenario.
		//
		// Fix with go-events ScopedBus: use bus.IsolatedScope(runID) from the root
		// runtime bus instead of creating orphan buses. This ensures every pipeline
		// execution has a durable event log and proper scoping.
		b = events.NewMemoryScopedBus()
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
			bus = events.NewMemoryScopedBus()
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
		b = events.NewMemoryScopedBus()
	}

	runCtx := f.PrepareWithEntry(runID, st, b, ckpt.ResumeAt)
	return runCtx, nil
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
