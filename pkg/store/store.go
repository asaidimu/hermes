package store

import (
	"context"
	"strings"
	"sync"

	"github.com/asaidimu/hermes/pkg/core"
	"github.com/google/uuid"
)

// Mutator applies modifications to run state. The state map is the live
// store state under lock: mutate it in place, do not retain references.
type Mutator func(state map[string]any) error

// Store manages the lifecycle of a run's state — an arbitrary JSON-like map.
// Persistence shape (columns, system fields) is an implementation detail of
// concrete stores; the engine only ever sees plain maps.
type Store interface {
	// ID returns the run identifier: a system-minted UUIDv7 shared with the
	// run's persisted state document (when persistence is configured).
	ID() string
	// Read executes fn under a shared read lock with the live state map.
	// fn must not retain or mutate the map.
	Read(fn func(state map[string]any) error) error
	// Update applies a mutator under an exclusive lock. Persistent stores
	// write through to their backing collection.
	Update(ctx context.Context, m Mutator) error
	Ready(ctx context.Context) error
	ExportJSON() (map[string]any, error)
	Clone() (Store, error)
	// Flush persists current in-memory state. No-op for MemoryStore.
	Flush(ctx context.Context) error
}

// MemoryStore is an in-memory thread-safe Store backed by a plain map.
type MemoryStore struct {
	mu    sync.RWMutex
	state map[string]any
	id    string
}

// newState seeds the internal map, minting nothing — identity lives on the
// struct, never inside user state.
func newState(initial map[string]any) map[string]any {
	state := make(map[string]any, len(initial)+1)
	for k, v := range initial {
		state[k] = v
	}
	return state
}

// newID mints a UUIDv7 in anansi's canonical 32-char hex form.
func newID() string {
	return strings.ReplaceAll(uuid.Must(uuid.NewV7()).String(), "-", "")
}

// NewMemoryStore creates an in-memory store with the given initial state.
func NewMemoryStore(initialState map[string]any) *MemoryStore {
	return &MemoryStore{
		state: newState(initialState),
		id:    newID(),
	}
}

// NewFreshStore creates a new in-memory store with the given initial state,
// completely independent of any parent store. Unlike Clone(), which copies
// the parent's state, this starts from scratch. If initialState is nil, an
// empty store is created.
func NewFreshStore(initialState map[string]any) *MemoryStore {
	return NewMemoryStore(initialState)
}

// ID returns the run identifier: a system-minted UUIDv7 created with the
// store — no database round-trip involved.
func (s *MemoryStore) ID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.id
}

// Read executes fn with the live state map under a shared read lock. fn must
// not retain or mutate the map; use Update for mutations.
func (s *MemoryStore) Read(fn func(state map[string]any) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if fn == nil {
		return nil
	}
	return fn(s.state)
}

// Update applies a single mutator function under an exclusive write lock.
func (s *MemoryStore) Update(_ context.Context, mutator Mutator) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if mutator == nil {
		return nil
	}
	return mutator(s.state)
}

// Ready verifies the store is initialized.
func (s *MemoryStore) Ready(_ context.Context) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.state == nil {
		return core.NewSystemError(core.ErrCodeExecutionFailed, "store state is nil")
	}
	return nil
}

// ExportJSON exports a deep copy of the state for wire compatibility.
func (s *MemoryStore) ExportJSON() (map[string]any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return deepCopyMap(s.state), nil
}

// Flush is a no-op for in-memory stores.
func (s *MemoryStore) Flush(_ context.Context) error { return nil }

// Clone creates a deep copy of the store with the same identity semantics:
// clones share the run identifier but have fully independent state.
func (s *MemoryStore) Clone() (Store, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &MemoryStore{
		state: deepCopyMap(s.state),
		id:    s.id,
	}, nil
}

// DeepCopyMap returns a deep copy of a state-shaped map: nested maps and
// slices are cloned recursively so the copy shares no references with src.
// Use it when handing state to consumers that must not observe later
// mutations (routers, snapshots, result records).
func DeepCopyMap(src map[string]any) map[string]any {
	return deepCopyMap(src)
}

// deepCopyMap deep-copies map[string]any and []any values recursively,
// leaving scalars untouched.
func deepCopyMap(src map[string]any) map[string]any {
	out := make(map[string]any, len(src))
	for k, v := range src {
		out[k] = deepCopyAny(v)
	}
	return out
}

func deepCopyAny(v any) any {
	switch t := v.(type) {
	case map[string]any:
		return deepCopyMap(t)
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = deepCopyAny(e)
		}
		return out
	default:
		return v
	}
}

// SetValue returns a Mutator setting a top-level state key.
func SetValue(key string, val any) Mutator {
	return func(state map[string]any) error {
		state[key] = val
		return nil
	}
}

var _ Store = (*MemoryStore)(nil)
