package store

import (
	"context"
	"sync"

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

	// flushMu serializes the DB write (one in-flight flush per run).
	// P4.7 (H7): the DATA lock (MemoryStore.mu) is never held across
	// I/O — Sync snapshots under it and writes outside, so state
	// readers and mutators no longer queue behind DB latency at every
	// stage-boundary milestone sync.
	flushMu sync.Mutex
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
	return applyMutator(s.state, mutator)
}

// Sync pushes the current in-memory state to the database collection for
// external queryability. This is optional — the Action Log is the durable
// source of truth. Sync is useful when an HTTP API needs to serve the
// current state of a run from the database rather than from memory.
//
// Synchronous in contract (pause/checkpoint paths want durability before
// return) but NOT under the data lock: P4.7 (H7) snapshots the state
// under MemoryStore.mu, releases it, and writes to the DB outside —
// concurrent readers and mutators of the run state never queue behind
// DB latency. Writes are serialized per store (flushMu) and land
// last-write-wins: a flush always persists a state at least as fresh as
// its snapshot.
func (s *PersistentStore) Sync(ctx context.Context) error {
	// Deep snapshot under the data lock: persist marshals the state
	// OUTSIDE the lock, so nested maps/slices must not alias live
	// state a concurrent Update could mutate mid-marshal.
	s.mu.Lock()
	state, info := s.typedView()
	snapshot := make(RunData, len(state))
	for k, v := range state {
		snapshot[k] = v
	}
	s.mu.Unlock()

	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	// Re-read the insertion flag under the data lock INSIDE the flush
	// mutex: a concurrent Sync may have created the document between
	// our snapshot and acquiring the write slot.
	s.mu.RLock()
	exists := s.exists
	s.mu.RUnlock()

	if !exists {
		full := &PipelineState{Data: snapshot, RunInfo: info}
		full.ID = s.MemoryStore.id
		if _, err := s.models.Create(ctx, full); err != nil {
			return core.SystemErrorFrom(err, core.ErrCodeExecutionFailed)
		}
		s.mu.Lock()
		s.exists = true
		s.mu.Unlock()
		return nil
	}

	if _, err := s.models.Update(ctx, s.MemoryStore.id, &PipelineState{Data: snapshot, RunInfo: info}); err != nil {
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
