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

// TouchedKeysKey is the engine-reserved state key a mutator may set
// during apply (via Touch) to declare the top-level keys it modifies.
// It never reaches real state: every Update pops it after the mutator
// runs, and the action-log delta path (plan P4.2) consumes it on its
// working copy. User code must not read or write it directly.
const TouchedKeysKey = "__hermes_touched_keys__"

// Touch declares, from inside a mutator body, the top-level state keys
// the mutator is about to modify. The stage runner's action-log delta
// path scopes its diff to the declared keys instead of deep-comparing
// the whole state (plan P4.2 — the H2 delta storm on large states).
// Mutators that never call Touch keep the whole-state diff: behavior
// and log entries stay identical (compat preserved).
func Touch(state map[string]any, keys ...string) {
        if state != nil && len(keys) > 0 {
                state[TouchedKeysKey] = append([]string(nil), keys...)
        }
}

// TakeTouchedKeys reads and removes a Touch declaration from a state
// copy the delta path applied a mutator to. Returns nil when the
// mutator declared nothing.
func TakeTouchedKeys(state map[string]any) []string {
        if state == nil {
                return nil
        }
        v, ok := state[TouchedKeysKey]
        if !ok {
                return nil
        }
        delete(state, TouchedKeysKey)
        keys, _ := v.([]string)
        return keys
}

// popTouchedKeys strips a Touch declaration after a mutator ran against
// REAL state (the central Update guard) so the reserved key never
// reaches durable or readable state.
func popTouchedKeys(state map[string]any) {
        if state != nil {
                delete(state, TouchedKeysKey)
        }
}

// applyMutator runs m against state and strips any Touch declaration —
// the single choke point both Store implementations use for Update so
// the reserved key can never leak into committed state.
func applyMutator(state map[string]any, m Mutator) error {
        if m == nil {
                return nil
        }
        if err := m(state); err != nil {
                return err
        }
        popTouchedKeys(state)
        return nil
}

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
        // Update applies a mutator under an exclusive lock. The Action Log
        // (pkg/actionlog) is the durable source of truth for crash recovery.
        Update(ctx context.Context, m Mutator) error
        Ready(ctx context.Context) error
        ExportJSON() (map[string]any, error)
        Clone() (Store, error)
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
        return applyMutator(s.state, mutator)
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
