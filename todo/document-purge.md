# Purge documents from the engine — state is just a map

## Context
With `PipelineState` (struct-derived schema) the persisted shape is typed, and the pipeline-visible state is `RunData = map[string]any`. Yet `*anansi document.Document` still threads through the entire execution layer: Store interface, router signatures, Step.Action, NodeRunContext, checkpoint functions, PipelineRunResult.FinalDoc. Audit showed every consumer only reads/mutates state keys:
- Nodes already get `State map[string]any`; ONE line in all of pkg/nodes touches `nCtx.Document` (arithmetic.go:59)
- Routers read state; FinalDoc consumers immediately json.Marshal it to merge maps; checkpoints are one body key; ApplyPatch's core is already pure map math
- All `_id_`/`_metadata_` hazards stem from this leakage

User decisions: collapse Transact into Update; DELETE NodeRunContext.Document outright; full purge in one pass.

## Target shape
```go
// pkg/store
type Mutator func(state map[string]any) error   // replaces DocumentMutator
type Store interface {
    ID() string
    Read(fn func(state map[string]any) error) error   // view under lock, do not retain
    Update(ctx context.Context, m Mutator) error      // write-through (PersistentStore)
    ExportJSON() (map[string]any, error)
    Clone() (Store, error)
    Ready(ctx) error
    Flush(ctx) error
}
```
- MemoryStore: plain map + id string internally — zero anansi dependency
- PersistentStore: unchanged contract; ModelCollection[PipelineState] translation stays
- Checkpoint funcs: signature swap doc → map (stay in pkg/pipeline, no import cycle); going through store.Update FIXES the latent bug where WriteCheckpoint mutated via Document() bypassing lock + write-through
- FinalDoc → FinalState map[string]any (deep copy); sub-pipeline merge loses marshal round-trip

## Tasks
- [*] store.go: interface reshape + map-based MemoryStore + SetValue/DeleteValue mutators; drop SetMetadata
- [*] persistent.go: adapt embed usage (typedView from s.state, idLocked semantics)
- [*] pipeline/types.go: Step.Action, router sigs, FinalState
- [*] pipeline/checkpoint.go: map signatures
- [*] pipeline/router.go, stage.go, context.go, factory.go, subpipeline.go: swap doc→snapshot
- [*] nodekit: drop NodeRunContext.Document; ApplyPatch(state, flat); build.go context assembly
- [*] nodes/arithmetic.go:59: state lookup with dotted-path helper if needed
- [*] timeline.Seek returns map[string]any (+ consumers)
- [*] runtime resumeFromPersistence map-based
- [*] tests: .Document() seeding → NewFreshStore(map); persistent_test Transact→Update
- [*] go build ./... && vet && full suite green (calc run test end-to-end)

## Notes
- WriteCheckpoint-via-Document() previously bypassed store lock AND write-through persistence; routing it through store.Update fixes a latent durability bug.
- No dotted-path Set reliance found at call sites (ApplyPatch pre-expands; runtime/pause/trycatch use flat keys). arithmetic fieldKey may be dotted → verify.
