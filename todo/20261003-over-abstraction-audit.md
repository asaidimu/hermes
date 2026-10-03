# Task: Over-abstraction audit — TS→Go port artifacts

- [x] Audit the codebase for over-abstraction patterns carried over from TypeScript
  - **Context:** Hermes was ported from TypeScript. TS requires interfaces for DI/mocking/polymorphism; Go does not. A single-implementation interface is a function call with extra steps. This audit identifies indirection that adds complexity without behavioral value.
  - **Status:** Done — findings below.

## Findings by Impact

### High-Impact: Single-Implementation Interfaces

These are interfaces with one (or zero) implementations and no behavioral value.
In Go, interfaces are valuable when there are multiple implementations or when
mocking is needed. None of these qualify.

| Interface | File | Implementations | Simpler Alternative |
|---|---|---|---|
| `store.Store` | `pkg/store/store.go:19` | `MemoryStore` (primary), `PersistentStore` (embeds `MemoryStore`) | Use `*MemoryStore` directly; add `Sync`/`Flush` method if persistence needed |
| `events.ScopedEventBus` | `pkg/events/events.go:59` | `MemoryScopedBus` only | Use `*MemoryScopedBus` directly |
| `pipeline.PipelineContext` | `pkg/pipeline/types.go:14` | `pipelineContextImpl` only | Use concrete struct directly |
| `pipeline.RunContext` | `pkg/pipeline/types.go:253` | `RunContextImpl` only | Use `*RunContextImpl` directly |
| `actionlog.Store` | `pkg/actionlog/actionlog.go:90` | `NopLog`, `MemoryActionLog`, `AnansiStore` | Use concrete type + nil check for no-op |
| `core.Logger` | `pkg/core/logger.go:6` | `NopLogger`, `ZapLogger` | Use `*zap.Logger` directly; `zap.NewNop()` for no-op |
| `scheduler.Scheduler` | `pkg/scheduler/scheduler.go:10` | `InMemoryScheduler`, `HestiaSchedulerFunc` | Use `*InMemoryScheduler` directly |
| `watch.WatchService` | `pkg/watch/watch.go:92` | one impl in runtime | Use concrete type |
| `runtime.EventSource` | `pkg/runtime/eventsource.go:37` | `ManualEventSource`, `HestiaEventSourceFunc` | Use function types directly |
| `runtime.SecretProvider` | `pkg/runtime/runtime.go:55` | **zero implementations** | Use `func(context.Context, string) (any, bool)` |
| `pipeline.PipelineRegistry` | `pkg/pipeline/workflow.go:73` | one impl | Use `func(string) (*PipelineDefinition, bool)` |

### Medium-Impact: Unnecessary Layers

- **Factory functions that wrap one call:**
  - `store.NewFreshStore` (`store.go:70`) — just calls `NewMemoryStore`
  - `actionlog.AnansiStoreFactory` (`anansi.go:38`) — holds one `*ModelCollection` field
  - `store.AnansiStoreFactory` (`anansi.go:22`) — same pattern
  - `events.NewDurableEventBus` / `WithDurableBackend` — wrap two params in a config struct
- **Adapter structs delegating to function fields:**
  - `runtime.HestiaEventSourceFunc` (`hestia_eventsource.go:10`)
  - `scheduler.HestiaSchedulerFunc` (`hestia.go:9`)
  - `core.ZapLogger` (`zap_adapter.go:6`) — wraps `*zap.Logger` which already has the same methods
- **Option functions that just set a field:**
  - `pipeline.WithRunEnv`, `WithSecretLookup`, `WithResourceResolver` (`types.go:72-89`)
- **Type aliases / pure re-exports:**
  - `core.SystemError` = `common.SystemError` (`errors.go:8`)
  - `core.NewSystemError` / `SystemErrorFrom` — delegate to `common.*`
  - `replay.StepAddress` = `pipeline.EntryAddress` (`replayer.go:24`)
  - `replay.DefinitionResolver` — function type alias (`replayer.go:36`)
  - `nodekit.SystemErrorJSON` = `core.SystemErrorJSON` (`errors.go:5`)
- **No-op "null object" types replaceable by nil checks:**
  - `actionlog.NopLog` (`actionlog.go:119`)
  - `core.NopLogger` (`logger.go:21`)
- **Wrapper with no transformation:**
  - `runtime.RunHandle` (`runtime.go:156`) — wraps `RunContextImpl`, delegates everything
  - `store.SetValue` (`store.go:168`) — returns trivial closure
  - `store.DeepCopyMap` (`store.go:138`) — exported wrapper for unexported `deepCopyMap`

### Low-Impact: Dead Weight

- **`pkg/effect` package** — exists solely to break an import cycle; two constants
  (`Pure = 1`, `SideEffecting = 2`) could live in `pkg/pipeline`
- **Generic types immediately type-erased:**
  - `nodekit.TypedRunContext[C]` / `TypedDefinition[C]` / `Define[C]` / `TypeOf[C]`
    (`typed.go`) — generic parameter only used during construction; type erasure
    happens immediately. `TypedRunContext` also causes field shadowing.
- **`pipeline.RoutingInstruction`** (`types.go:138`) — marker interface porting a
  TS union type; a tagged struct or type switch is more idiomatic (the type switch
  already exists in `evaluateInstruction`)
- **`runtime.Options`** (`runtime.go:70`) — 11 fields, most always set to defaults
  (`StoreFactory`, `StoreLoader`, `EventSource`, `Scheduler`, `ActionLog`)
- **`pipeline.NewFactoryFromModel`** (`factory.go:240`) — calls
  `data.StructFieldValues` but discards the result; passes nil schema. Dead code.
- **`timeline.TimelineEventSource`** — 4 constants, only `SourcePipeline` is used
- **Trivial compiler helpers** — `flowEdges`, `dependencyEdgesTo`, `nextDefaultTarget`,
  `appendRequirement`, `staticResources`, `buildResourcesFor` are simple loops that
  could be inlined at call sites

## Root Cause

TypeScript's type system requires interfaces for DI, mocking, and polymorphism.
Go doesn't. Every interface listed above should be evaluated against the Go idiom:
*derive the interface at the consumer, keep implementations concrete*.
