package pipeline

import (
        "context"

        "github.com/asaidimu/hermes/pkg/actionlog"
        "github.com/asaidimu/hermes/pkg/core"
        "github.com/asaidimu/hermes/pkg/events"
        "github.com/asaidimu/hermes/pkg/store"
        "github.com/google/uuid"

        "go.uber.org/zap"
)

// FactoryOptions configures the pipeline factory.
type FactoryOptions struct {
        Logger   *zap.SugaredLogger
        EventBus *events.MemoryScopedBus
        // ActionLog records every significant event during execution for
        // event-sourced recovery and observability. When nil, no entries are
        // recorded (logging disabled).
        ActionLog actionlog.Store
        // RerunIndex identifies which attempt of a run this factory prepares.
        // 0 is the first execution; each forced recovery increments it so
        // retries get separate log documents for audit purposes.
        RerunIndex int
        // Rebuild, when set, replaces the checkpoint-based Resume path with
        // event-sourced recovery (§4.2 of EXECUTION_ENGINE_REDESIGN_V2.md):
        // the factory calls it to reconstruct state from the action log
        // instead of reading a stored checkpoint.
        //
        // 20261003 refactor (plan R.3): was the Rebuilder interface (implemented
        // only by *replay.Replayer, existing to break the pipeline->replay
        // import cycle). A func type breaks the cycle the same way with less
        // ceremony — pass (*replay.Replayer).Rebuild.
        Rebuild RebuildFunc
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

// RebuildFunc performs event-sourced recovery: it receives the run's root
// pipeline id so the replayer can partition the log per pipeline (entries
// carry a PipelineID since #review-20260910-015) and resolve the definition
// to replay (#review-20260910-003). (R.3: was the Rebuilder interface.)
type RebuildFunc func(ctx context.Context, runID, rootPipelineID string) (store.Store, EntryAddress, error)

// PipelineFactory creates and prepares pipeline run contexts.
//
// 20261003 refactor (plan D.2): the schema field is gone — it was written
// at construction and read nowhere, and PipelineDefinition.Schema (also
// deleted) was never set by the compiler. Node config schemas live in the
// node definitions (pkg/nodekit) where they are actually consumed.
type PipelineFactory struct {
        definition PipelineDefinition
        options    FactoryOptions
}

// NewFactory creates a new PipelineFactory.
func NewFactory(def PipelineDefinition, opts ...FactoryOptions) *PipelineFactory {
        var opt FactoryOptions
        if len(opts) > 0 {
                opt = opts[0]
        }
        if opt.Logger == nil {
                opt.Logger = zap.NewNop().Sugar()
        }
        // ActionLog nil = logging disabled (R.3 deleted NopLog; every call
        // site nil-checks).
        return &PipelineFactory{
                definition: def,
                options:    opt,
        }
}

// Prepare builds a fresh RunContext for a runID (or generates a new UUIDv4).
func (f *PipelineFactory) Prepare(runID string, st store.Store, bus ...*events.MemoryScopedBus) *RunContextImpl {
        if runID == "" {
                runID = uuid.New().String()
        }
        var b *events.MemoryScopedBus
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
        rc.actionLog = f.options.ActionLog
        rc.rerunIndex = f.options.RerunIndex
        if f.options.ResourceResolver != nil {
                rc.SetResourceResolver(f.options.ResourceResolver)
        }
        rc.runEnv = f.options.RunEnv
        rc.secretLookup = f.options.SecretLookup
        return rc
}

// PrepareWithEntry builds a RunContext starting execution at a specific entry address.
func (f *PipelineFactory) PrepareWithEntry(runID string, st store.Store, bus *events.MemoryScopedBus, entry EntryAddress) *RunContextImpl {
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
        rc.actionLog = f.options.ActionLog
        rc.rerunIndex = f.options.RerunIndex
        if f.options.ResourceResolver != nil {
                rc.SetResourceResolver(f.options.ResourceResolver)
        }
        rc.runEnv = f.options.RunEnv
        rc.secretLookup = f.options.SecretLookup
        return rc
}

// Resume restores execution from an existing Store. When a Rebuilder is
// configured (FactoryOptions.Rebuilder), it uses event-sourced recovery
// (§4.2) to reconstruct state from the action log instead of reading a
// stored checkpoint.
func (f *PipelineFactory) Resume(ctx context.Context, runID string, st store.Store, bus ...*events.MemoryScopedBus) (*RunContextImpl, error) {
        if st == nil {
                return nil, core.NewSystemError(core.ErrCodeValidation, "store is required to resume pipeline")
        }

        var entryAddr EntryAddress

        if f.options.Rebuild != nil {
                // Event-sourced recovery path: reconstruct state from action log.
                rebuiltStore, addr, err := f.options.Rebuild(ctx, runID, f.definition.ID)
                if err != nil {
                        return nil, core.NewSystemError(core.ErrCodeExecutionFailed,
                                "replayer rebuild failed for run "+runID).WithCause(err)
                }
                if addr.Stage == "" {
                        // Run already completed during replay — nothing to resume.
                        return nil, core.NewSystemError(core.ErrCodeNotFound,
                                "run "+runID+" already completed (no resume needed)")
                }
                // Use the rebuilt store and resume address. Copy existing state
                // from the original store (which may have additional fields
                // seeded by the runtime) into the rebuilt store.
                if err := copyStoreState(st, rebuiltStore); err != nil {
                        return nil, err
                }
                st = rebuiltStore
                entryAddr = addr
        } else {
                // Legacy checkpoint path. C4-fix (M5): extract the raw checkpoint
                // value under the store read lock, parse OUTSIDE it — the JSON round
                // trip used to stretch the RLock for I/O-shaped work.
                var ckptRaw any
                if err := st.Read(func(state map[string]any) error {
                        ckptRaw = ExtractCheckpointRaw(state, f.definition.ID)
                        return nil
                }); err != nil {
                        return nil, err
                }
                ckpt, perr := ParseCheckpoint(ckptRaw)
                if perr != nil {
                        return nil, perr
                }
                if ckpt == nil {
                        return nil, core.NewSystemError(core.ErrCodeNotFound, "no checkpoint found in document for pipeline "+f.definition.ID)
                }
                entryAddr = ckpt.ResumeAt
        }

        var b *events.MemoryScopedBus
        if len(bus) > 0 && bus[0] != nil {
                b = bus[0]
        } else if f.options.EventBus != nil {
                b = f.options.EventBus
        } else {
                b = f.newFallbackBus(runID)
        }

        runCtx := f.PrepareWithEntry(runID, st, b, entryAddr)
        return runCtx, nil
}

// copyStoreState copies all state from src into dst. Used when the replayer
// produces a rebuilt store that needs the runtime-seeded fields (run metadata,
// trigger event, etc.) from the original store.
//
// C4-fix (M5, 20261003): the old body nested dst.Update INSIDE src.Read,
// holding both stores' locks at once — safe only by accident of call-site
// ordering, and a deadlock waiting for the first reverse-direction copy.
// Now: snapshot src under its read lock into a local map, release, then
// apply a single mutator on dst. Only one store lock is ever held.
func copyStoreState(src, dst store.Store) error {
        snapshot := make(map[string]any)
        if err := src.Read(func(srcState map[string]any) error {
                for k, v := range srcState {
                        snapshot[k] = v
                }
                return nil
        }); err != nil {
                return err
        }
        return dst.Update(context.Background(), func(dstState map[string]any) error {
                for k, v := range snapshot {
                        dstState[k] = v
                }
                return nil
        })
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
func (f *PipelineFactory) newFallbackBus(runID string) *events.MemoryScopedBus {
        logger := f.options.Logger
        if logger == nil {
                logger = zap.NewNop().Sugar()
        }
        logger.Warn("pipeline: no event bus supplied; creating an orphan in-memory bus with no subscribers or durability",
                "pipeline", f.definition.ID, "runId", runID)
        return events.NewMemoryScopedBus()
}
