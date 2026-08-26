# Run Identity = State Document Identity

## Context
A run's state lives in exactly one document (`factory.Prepare(runID, st, bus)` in `pkg/runtime/runtime.go`). Two violations of the anansi contract broke persistence:

1. **Composite runID jammed into `_id_`** — `generateRunID()` produced `<wf>:<trigger>:<uuid4>` and `PersistentStore.persist()` force-wrote it into `_id_`, a system-managed UUIDv7 primary key (32-char hex). Lookups by that composite string could never match.
2. **Checkpoints stored in `_metadata_`** — `WriteCheckpoint`/`ReadCheckpoint` used `SetMetadataValue`/`Metadata()`, but `_metadata_` is reserved for SYSTEM metadata (request tracing, versioning). Run metadata is application data.

## Design decision (user-approved)
- **Document creation IS run creation**: mint the state doc in memory (`document.NewRecordView` + UUIDv7-hex `_id_`, mirroring anansi's `newUUID()` format); NO DB call. The minted `_id_` IS the runID. Insert (first `persist`) preserves the valid pre-set ID.
- **Lazy insert**: no tombstones; first write-through creates the row.
- **Two-option factory API**: `Options.StoreFactory func() (store.Store, error)` (mint) + `Options.StoreLoader func(runID string) (store.Store, error)` (recovery).
- **Don't use `core/data` Document** constructors; stay on `core/document`.
- Run linkage (`workflowId`/`triggerId`) persisted in body under `__run_meta__` so crash-recovery resolves directly instead of scanning all workflows × pipelines.

## Tasks
- [*] `pkg/store/store.go`: add `ID() string` to `Store`; MemoryStore mints/returns `_id_` (UUIDv7 hex); lock-free `idLocked()` for reentrant use
- [*] `pkg/store/state.go`: `RunMetadata` + `RunData` + `PipelineState` struct (DocumentModel embed, `anansi:"state"` / `anansi:"metadata"` tags) — schema is DERIVED from this struct, never hand-written
- [*] `pkg/store/persistent.go`: backed by `collection.ModelCollection[*PipelineState]`; mint via `document.New(&PipelineState{})` (no DB); `NewPersistentStoreForID` = `FindByID`; persist translates flat body ⇄ typed shape (state/metadata columns)
- [*] `pkg/store/anansi.go`: factory derives schema via `data.ExtractDTOSchemaDirect`, self-provisions collection (HasCollection → CreateCollection), configures document factory singleton; `Create()`/`Load()` + `Mint()`/`Loader()` adapters
- [*] `pkg/pipeline/checkpoint.go`: `__pipeline_data__` moved from `_metadata_` to document body (`doc.Get`/`doc.Set`)
- [*] `pkg/runtime/runtime.go`: `Options.StoreFactory func() (store.Store, error)` + `Options.StoreLoader`; `executePipeline` derives `runID := st.ID()`; `generateRunID` deleted; seeds `store.RunMetaKey`; `resumeFromPersistence` resolves workflow directly from linkage (legacy scan kept as fallback)
- [*] Tests: `pkg/store/persistent_test.go` — real in-memory sqlite round-trip (mint → seed/checkpoint/linkage → reload by ID → update-in-place no duplicates); full suite green

## Gotchas discovered (also in AGENTS.md)
- Schema field IDs must be UUIDv7s — deriving from structs handles it; hand-writing requires minting them.
- `Document.Set("_id_"/"_metadata_")` → readonly error; seed system fields in the record map BEFORE constructing the view.
- Container-bound numeric values aren't always `float64` — coerce defensively after persistence round-trips.
- `MemoryStore.ID()` under held lock deadlocks — use `idLocked()` internally.
- Document factory must be configured before any persistence op that mints documents.

## Notes
- Feature not wired in production yet → no data migration needed.
- Known-flaky unrelated test: `TestInMemorySchedulerReplace` (fails under `-count>1`, passes solo).
