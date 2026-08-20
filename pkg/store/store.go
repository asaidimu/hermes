package store

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/asaidimu/go-anansi/v8/core/data"
	"github.com/asaidimu/go-anansi/v8/core/document"
	"github.com/asaidimu/go-anansi/v8/core/schema/definition"
	"github.com/asaidimu/hermes/pkg/core"
)

// DocumentMutator applies modifications to an Anansi Document.
type DocumentMutator func(doc *document.Document) error

// Store manages persistence and transaction lifecycle of an Anansi Document.
type Store interface {
	Document() *document.Document
	Read(fn func(doc *document.Document) error) error
	Update(ctx context.Context, mutator DocumentMutator) error
	Transact(ctx context.Context, fn func(txDoc *document.Document) error) error
	Ready(ctx context.Context) error
	ExportJSON() (map[string]any, error)
	Clone() (Store, error)
}

// MemoryStore is an in-memory thread-safe implementation of Store.
type MemoryStore struct {
	mu     sync.RWMutex
	doc    *document.Document
	schema *definition.CompiledSchema
}

// NewMemoryStore creates a store with an optional initial document or empty record view.
func NewMemoryStore(doc *document.Document, schema ...*definition.CompiledSchema) *MemoryStore {
	var cs *definition.CompiledSchema
	if len(schema) > 0 {
		cs = schema[0]
	}
	if doc == nil {
		doc = document.NewRecordView(make(map[string]any))
	}
	return &MemoryStore{
		doc:    doc,
		schema: cs,
	}
}

// Document returns the active document pointer.
func (s *MemoryStore) Document() *document.Document {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.doc
}

// Read executes a read function under a shared read lock.
func (s *MemoryStore) Read(fn func(doc *document.Document) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if fn == nil {
		return nil
	}
	return fn(s.doc)
}

// Update executes a single mutator function under an exclusive write lock.
func (s *MemoryStore) Update(ctx context.Context, mutator DocumentMutator) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if mutator == nil {
		return nil
	}
	return mutator(s.doc)
}

// Transact executes a transaction function against the document.
func (s *MemoryStore) Transact(ctx context.Context, fn func(txDoc *document.Document) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if fn == nil {
		return nil
	}
	return fn(s.doc)
}

// Ready verifies the store is initialized.
func (s *MemoryStore) Ready(ctx context.Context) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.doc == nil {
		return core.NewSystemError(core.ErrCodeExecutionFailed, "store document is nil")
	}
	return nil
}

// ExportJSON exports the document to a standard map[string]any for wire compatibility.
func (s *MemoryStore) ExportJSON() (map[string]any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
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

// Clone creates a deep copy of the store.
func (s *MemoryStore) Clone() (Store, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	exported, err := s.ExportJSON()
	if err != nil {
		return nil, err
	}

	// Create a new clone document as record view or pool
	clonedDoc := document.NewRecordView(exported)
	return NewMemoryStore(clonedDoc, s.schema), nil
}

// Ensure DocumentMutator helper for simple key-value sets
func SetValue(key string, val any) DocumentMutator {
	return func(doc *document.Document) error {
		return doc.Set(key, val)
	}
}

// Ensure DocumentMutator helper for metadata sets
func SetMetadata(key string, val any) DocumentMutator {
	return func(doc *document.Document) error {
		return doc.SetMetadataValue(key, val)
	}
}

var _ Store = (*MemoryStore)(nil)
var _ = data.DocumentIDField
