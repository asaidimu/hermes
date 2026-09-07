package store

import (
	"context"

	"github.com/asaidimu/go-anansi/v8/core/document"
	"github.com/asaidimu/go-anansi/v8/core/persistence/collection"
	"github.com/asaidimu/hermes/pkg/core"
)

// PersistentStore is an in-memory Store backed by an anansi ModelCollection
// for optional queryability. It delegates all state management to the embedded
// MemoryStore — no write-through occurs on Update or Flush.
//
// The Action Log (pkg/actionlog) is the durable source of truth for crash
// recovery. PersistentStore exists solely for external queryability —
// it is an explicitly non-authoritative read projection.
// Callers that need the database to reflect current state can call Sync()
// to push the in-memory state to the collection.
//
// Identity model: a run IS its state document. NewPersistentStore mints the
// document identity (a UUIDv7 _id_, via anansi's own document.New) in memory
// with no database round-trip; Store.ID() returns it as the run identifier.
type PersistentStore struct {
	*MemoryStore
	models *collection.ModelCollection[*PipelineState]
	exists bool // whether the document has been inserted into the collection
}

var _ Store = (*PersistentStore)(nil)

// NewPersistentStore creates a run document in memory only. The identity is
// minted immediately through anansi's struct-model pipeline but nothing is
// written to the collection. Call Sync() to push state to the database for
// queryability. initialState may be nil.
func NewPersistentStore(models *collection.ModelCollection[*PipelineState], initialState map[string]any) *PersistentStore {
	ps := document.New(&PipelineState{Data: RunData(initialState)})
	ms := NewMemoryStore(initialState)
	ms.id = ps.GetID()
	return &PersistentStore{
		MemoryStore: ms,
		models:      models,
	}
}

// NewPersistentStoreForID loads an existing run document from the collection
// by its identifier. It returns a NotFound error when no document exists for
// runID.
func NewPersistentStoreForID(ctx context.Context, models *collection.ModelCollection[*PipelineState], runID string) (*PersistentStore, error) {
	ps, err := models.FindByID(ctx, runID)
	if err != nil {
		return nil, core.SystemErrorFrom(err, core.ErrCodeNotFound)
	}

	body := make(map[string]any, len(ps.Data)+1)
	for k, v := range ps.Data {
		body[k] = v
	}
	if info := ps.RunInfo; info != (RunMetadata{}) {
		body[RunMetaKey] = info.Map()
	}

	ms := NewMemoryStore(body)
	ms.id = ps.GetID()
	return &PersistentStore{
		MemoryStore: ms,
		models:      models,
		exists:      true,
	}, nil
}

// Update applies the mutator to in-memory state only. No write-through
// occurs — the Action Log is the durable record. Call Sync() to push
// state to the database for queryability.
func (s *PersistentStore) Update(ctx context.Context, mutator Mutator) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if mutator != nil {
		return mutator(s.state)
	}
	return nil
}

// Flush is a no-op. The Action Log handles durability. Call Sync() to
// explicitly push state to the database for queryability.
func (s *PersistentStore) Flush(_ context.Context) error { return nil }

// Sync pushes the current in-memory state to the database collection for
// external queryability. This is optional — the Action Log is the durable
// source of truth. Sync is useful when an HTTP API needs to serve the
// current state of a run from the database rather than from memory.
func (s *PersistentStore) Sync(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.persist(ctx)
}

// persist writes the current in-memory state to the collection. On the first
// call the document is inserted with its pre-minted _id_; subsequent calls
// update by _id_.
func (s *PersistentStore) persist(ctx context.Context) error {
	state, info := s.typedView()

	if !s.exists {
		full := &PipelineState{Data: state, RunInfo: info}
		full.ID = s.MemoryStore.id
		if _, err := s.models.Create(ctx, full); err != nil {
			return core.SystemErrorFrom(err, core.ErrCodeExecutionFailed)
		}
		s.exists = true
		return nil
	}

	if _, err := s.models.Update(ctx, s.MemoryStore.id, &PipelineState{Data: state, RunInfo: info}); err != nil {
		return core.SystemErrorFrom(err, core.ErrCodeExecutionFailed)
	}
	return nil
}

// typedView converts the flat state into the persisted PipelineState fields.
func (s *PersistentStore) typedView() (RunData, RunMetadata) {
	metaRaw, _ := s.state[RunMetaKey].(map[string]any)
	info := RunInfoFromMap(metaRaw)
	state := make(RunData, len(s.state))
	for k, v := range s.state {
		if k == RunMetaKey {
			continue
		}
		state[k] = v
	}
	return state, info
}

// Clone creates a deep copy backed by the same model collection, preserving
// the run identity and insertion state.
func (s *PersistentStore) Clone() (Store, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cloned, err := s.MemoryStore.Clone()
	if err != nil {
		return nil, err
	}
	return &PersistentStore{
		MemoryStore: cloned.(*MemoryStore),
		models:      s.models,
		exists:      s.exists,
	}, nil
}
