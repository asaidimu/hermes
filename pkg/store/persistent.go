package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/asaidimu/go-anansi/v8/core/data"
	"github.com/asaidimu/go-anansi/v8/core/document"
	"github.com/asaidimu/go-anansi/v8/core/persistence/base"
	"github.com/asaidimu/go-anansi/v8/core/query"
	"github.com/asaidimu/hermes/pkg/core"
)

// PersistentStore is a write-through Store backed by an anansi collection.
// Every Update/Transact persists to the collection immediately, and Flush
// ensures the current in-memory state is persisted.
type PersistentStore struct {
	*MemoryStore
	coll   base.Collection
	runID  string
	exists bool // whether the document was loaded from the collection
}

// NewPersistentStore creates a PersistentStore backed by the given collection.
// It attempts to load an existing document for runID from the collection.
func NewPersistentStore(ctx context.Context, coll base.Collection, runID string) (*PersistentStore, error) {
	s := &PersistentStore{
		MemoryStore: NewMemoryStore(nil),
		coll:        coll,
		runID:       runID,
	}

	if err := s.load(ctx); err != nil {
		return nil, fmt.Errorf("persistent store: load failed: %w", err)
	}

	return s, nil
}

// load attempts to load an existing document from the collection by runID.
func (s *PersistentStore) load(ctx context.Context) error {
	q := query.NewQueryBuilder().Where(data.DocumentIDField).Eq(s.runID).Build()

	result, err := s.coll.Read(ctx, &q)
	if err != nil {
		return core.SystemErrorFrom(err, core.ErrCodeExecutionFailed)
	}

	if result.Count == 0 {
		s.doc = document.NewRecordView(map[string]any{
			data.DocumentIDField: s.runID,
		})
		s.exists = false
		return nil
	}

	docs := result.Data
	if len(docs) == 0 {
		s.doc = document.NewRecordView(map[string]any{
			data.DocumentIDField: s.runID,
		})
		s.exists = false
		return nil
	}

	raw, err := json.Marshal(docs[0])
	if err != nil {
		return core.SystemErrorFrom(err, core.ErrCodeExecutionFailed)
	}

	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return core.SystemErrorFrom(err, core.ErrCodeExecutionFailed)
	}

	s.doc = document.NewRecordView(m)
	s.exists = true
	return nil
}

// persist writes the current in-memory state to the anansi collection.
func (s *PersistentStore) persist(ctx context.Context) error {
	m := s.doc.ToMap()
	m[data.DocumentIDField] = s.runID

	d := data.MustNewDocument(m, ctx)

	if s.exists {
		filter := query.NewQueryBuilder().Where(data.DocumentIDField).Eq(s.runID).Build().Filters

		cu := base.NewCollectionUpdate().WithFilter(filter)
		for k, v := range m {
			cu.SetField(k, v)
		}

		_, err := s.coll.Update(ctx, cu)
		if err != nil {
			return core.SystemErrorFrom(err, core.ErrCodeExecutionFailed)
		}
	} else {
		_, err := s.coll.CreateOne(ctx, d)
		if err != nil {
			return core.SystemErrorFrom(err, core.ErrCodeExecutionFailed)
		}
		s.exists = true
	}
	return nil
}

// Update mutates the document and persists to the collection.
func (s *PersistentStore) Update(ctx context.Context, mutator DocumentMutator) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if mutator == nil {
		return nil
	}
	if err := mutator(s.doc); err != nil {
		return err
	}
	return s.persist(ctx)
}

// Transact executes a function and persists to the collection.
func (s *PersistentStore) Transact(ctx context.Context, fn func(txDoc *document.Document) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if fn == nil {
		return nil
	}
	if err := fn(s.doc); err != nil {
		return err
	}
	return s.persist(ctx)
}

// Flush persists the current in-memory state to the collection.
func (s *PersistentStore) Flush(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.persist(ctx)
}

// Clone creates a deep copy backed by the same collection.
func (s *PersistentStore) Clone() (Store, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	exported, err := s.exportJSONLocked()
	if err != nil {
		return nil, err
	}

	clonedDoc := document.NewRecordView(exported)
	ms := NewMemoryStore(clonedDoc, s.schema)
	return &PersistentStore{
		MemoryStore: ms,
		coll:        s.coll,
		runID:       s.runID,
		exists:      s.exists,
	}, nil
}

// exportJSONLocked is an internal helper that assumes the lock is held.
func (s *PersistentStore) exportJSONLocked() (map[string]any, error) {
	if s.doc == nil {
		return make(map[string]any), nil
	}
	dataBytes, err := json.Marshal(s.doc)
	if err != nil {
		return nil, core.SystemErrorFrom(err, core.ErrCodeExecutionFailed)
	}

	var res map[string]any
	if err := json.Unmarshal(dataBytes, &res); err != nil {
		return nil, core.SystemErrorFrom(err, core.ErrCodeExecutionFailed)
	}
	if res == nil {
		res = make(map[string]any)
	}
	return res, nil
}

var _ Store = (*PersistentStore)(nil)
