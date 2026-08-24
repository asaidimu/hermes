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
	// @note #review-20260822-018 issue status=open priority=P2 tags=#review,#interface : Document() returns concrete pointer, unmockable
	//
	// Store.Document() returns *document.Document, a concrete struct from the anansi
	// library. This makes it impossible to mock Store in tests without importing the
	// real document package. Consider returning an interface or removing Document() from
	// the Store interface entirely (callers can use Read() for safe access).
	Document() *document.Document
	Read(fn func(doc *document.Document) error) error
	Update(ctx context.Context, mutator DocumentMutator) error
	// @note #review-20260822-019 issue status=open priority=P2 tags=#review,#interface : Transact is identical to Update
	//
	// Transact and Update have the same lock pattern, same signature shape, and same
	// semantics. Transact provides no rollback, retry, or isolation guarantees beyond
	// Update. This violates the Interface Segregation Principle and confuses callers about
	// which to use. Either add real transactional semantics (retry on conflict, nested
	// transactions, rollback) or remove Transact from the interface.
	Transact(ctx context.Context, fn func(txDoc *document.Document) error) error
	Ready(ctx context.Context) error
	ExportJSON() (map[string]any, error)
	Clone() (Store, error)
	// Flush persists the current in-memory state to the backing store.
	// For MemoryStore this is a no-op. For PersistentStore this writes
	// through to the anansi collection.
	Flush(ctx context.Context) error
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
	// @note #review-20260822-005 issue status=open priority=P1 tags=#review,#bug,#concurrency : Document() returns mutable pointer after releasing lock
	//
	// This method acquires RLock, obtains a raw *document.Document pointer, releases
	// the lock via defer RUnlock, and returns the pointer. Callers can then mutate the
	// document freely without any lock held, creating a data race with concurrent
	// Read/Update/Transact calls. The mutex is effectively pointless for this method.
	//
	// Fix by returning a defensive copy, returning an interface, or documenting that
	// the caller must not mutate the returned value.
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
	// @note #review-20260822-020 issue status=open priority=P2 tags=#review,#bug : ExportJSON performs wasteful Marshal/Unmarshal round-trip
	//
	// ExportJSON marshals the document to []byte via json.Marshal, then immediately
	// unmarshals back into map[string]any. This is a redundant encode/decode cycle.
	//
	// Either json.Unmarshal directly into map[string]any from the document's internal
	// representation, or add a ToMap() method to the document type.
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

// Flush is a no-op for in-memory stores.
func (s *MemoryStore) Flush(_ context.Context) error { return nil }

// Clone creates a deep copy of the store.
func (s *MemoryStore) Clone() (Store, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// @note #review-20260822-008 issue status=open priority=P1 tags=#review,#concurrency,#bug : Clone acquires RLock then calls ExportJSON which also acquires RLock
	//
	// Clone() holds RLock and calls s.ExportJSON(), which itself acquires RLock.
	// While Go allows multiple concurrent readers, a pending writer between the two
	// lock acquisitions will cause deadlock. The RLock is not reentrant in the presence
	// of a writer — if ExportJSON is ever changed to use Lock, or if a writer is queued,
	// this deadlocks.
	//
	// Fix by having Clone call an internal unexported exportJSONLocked method that
	// assumes the lock is already held.
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
// @note #review-20260822-021 issue status=open priority=P3 tags=#review,#naming : Blank import side-effect is unexplained
//
// `var _ = data.DocumentIDField` is a blank variable assignment that asserts nothing
// about types. Unlike `var _ Interface = (*Type)(nil)`, it has no compile-time
// verification purpose. If it exists to force an import for side effects, it should use
// `_ "package/path"` import syntax instead.
var _ = data.DocumentIDField
