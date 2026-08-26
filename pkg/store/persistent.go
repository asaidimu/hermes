package store

import (
	"context"

	"github.com/asaidimu/go-anansi/v8/core/document"
	"github.com/asaidimu/go-anansi/v8/core/persistence/collection"
	"github.com/asaidimu/hermes/pkg/core"
)

// PersistentStore is a write-through Store backed by an anansi ModelCollection
// of PipelineState. Every Update persists to the collection immediately, and
// Flush ensures the current in-memory state is persisted.
//
// Identity model: a run IS its state document. NewPersistentStore mints the
// document identity (a UUIDv7 _id_, via anansi's own document.New) in memory
// with no database round-trip; Store.ID() returns it as the run identifier.
// The first write-through inserts the document, preserving the pre-minted
// _id_. Recovery loads by that id via NewPersistentStoreForID. The _id_ itself
// is never overwritten by hermes — it is owned by the system.
type PersistentStore struct {
	*MemoryStore
	models *collection.ModelCollection[*PipelineState]
	exists bool // whether the document has been inserted into the collection
}

var _ Store = (*PersistentStore)(nil)

// NewPersistentStore creates a run document in memory only: the identity is
// minted immediately through anansi's struct-model pipeline but nothing is
// written to the collection until the first write-through. initialState may
// be nil.
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
// runID — recovery of an unknown run must fail loudly rather than silently
// fabricate empty state.
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

// persist writes the current in-memory state to the collection. On the first
// call the document is inserted with its pre-minted _id_; subsequent calls
// update by _id_. The flat state is persisted as the PipelineState shape:
// pipeline state under "state", run linkage under "metadata". System fields
// (_id_, _metadata_) remain under anansi's control.
//
// Caller must hold the store lock (invoked from Update/Flush).
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
// Assumes the caller holds the store lock.
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

// Update applies the mutator under lock and writes through to the collection.
func (s *PersistentStore) Update(ctx context.Context, mutator Mutator) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if mutator != nil {
		if err := mutator(s.state); err != nil {
			return err
		}
	}
	return s.persist(ctx)
}

// Flush persists the current in-memory state to the collection.
func (s *PersistentStore) Flush(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.persist(ctx)
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
