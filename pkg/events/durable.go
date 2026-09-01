package events

import (
	"sync"

	gevents "github.com/asaidimu/go-events/v2"
)

// memoryStore is a trivial in-memory implementation of gevents.Store, used so
// a durable-shaped go-events EventBus can be constructed without touching
// disk. It satisfies the Store contract (Set/Get/Delete/Close) but keeps
// everything in a map guarded by a mutex; nothing survives process restart.
type memoryStore struct {
	mu   sync.RWMutex
	vals map[string][]byte
}

func newMemoryStore() *memoryStore {
	return &memoryStore{vals: make(map[string][]byte)}
}

func (s *memoryStore) Set(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]byte, len(value))
	copy(cp, value)
	s.vals[key] = cp
	return nil
}

func (s *memoryStore) Get(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.vals[key]
	if !ok {
		return nil, gevents.ErrStoreKeyNotFound
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp, nil
}

func (s *memoryStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.vals, key)
	return nil
}

func (s *memoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.vals = nil
	return nil
}

// DurableBusConfig configures the go-events backend wired in behind a
// MemoryScopedBus by NewDurableMemoryScopedBus / WithDurableBackend.
type DurableBusConfig struct {
	// BaseDir is the root directory for the durable Pebble-backed event log
	// and state store. Required when Durable is true; ignored otherwise.
	BaseDir string
	// BusKey namespaces this bus's on-disk subdirectory and go-events
	// bus identity. Required when Durable is true; ignored otherwise.
	BusKey string
	// Durable selects a real, disk-backed Pebble event log at
	// {BaseDir}/{BusKey}/events/. When false (the default), the backing
	// go-events bus still exists and is fully wired — Emit/Subscribe work
	// end-to-end — but its log and metadata store are in-memory only
	// (gevents.NewMemoryLog + this package's memoryStore), so nothing
	// touches disk and nothing survives a restart. Use this to exercise or
	// test the durable wiring path without provisioning storage.
	Durable bool
}

// NewDurableEventBus constructs a real go-events *gevents.EventBus per cfg
// and returns it along with a closer. Callers typically pass the result to
// gevents.NewSimple[PipelineEvent](bus) and then NewMemoryScopedBus(simple)
// (or WithDurableBackend, which does exactly that).
//
// @note #scoped-bus-opportunity-005 issue status=resolved priority=P2 tags=#event-bus,#durability : Durable event backend designed but never wired
//
// Resolved: NewMemoryScopedBus's `underlying` parameter — present since the
// type was first written, per review-20260822 — is now actually
// constructible and wired, via this function plus WithDurableBackend below.
// It stops short of making durability the default for every
// WorkflowRuntime, deliberately: flipping the default would mean every
// caller who doesn't otherwise configure storage suddenly gets Pebble files
// written under a directory they didn't choose. Instead this is opt-in:
// pass Options{Bus: events.WithDurableBackend(events.DurableBusConfig{
// Durable: true, BaseDir: "...", BusKey: "..."})} to WorkflowRuntime to get
// a durable, replayable event log; the zero-config in-memory default is
// unchanged for everyone else. Note that even the non-durable path here now
// exercises the real go-events dispatch/retry/checkpoint machinery (via
// MemoryLog + memoryStore) rather than being unreachable dead code.
func NewDurableEventBus(cfg DurableBusConfig) (bus *gevents.EventBus, closeFn func() error, err error) {
	baseDir := cfg.BaseDir
	busKey := cfg.BusKey
	if busKey == "" {
		busKey = "hermes"
	}

	gcfg := gevents.DefaultConfig(baseDir, busKey)
	if !cfg.Durable {
		gcfg.Log = gevents.NewMemoryLog()
		gcfg.Store = newMemoryStore()
		if baseDir == "" {
			// validate() requires a non-empty BaseDir even though neither
			// the in-memory Log nor Store above ever read or write it.
			gcfg.BaseDir = "memory"
		}
	}

	b, err := gevents.NewEventBus(gcfg)
	if err != nil {
		return nil, nil, err
	}
	return b, b.Close, nil
}

// WithDurableBackend builds a root MemoryScopedBus backed by a real
// go-events bus constructed from cfg (see NewDurableEventBus), and returns
// it along with a closer the caller must invoke on shutdown to release the
// underlying resources (Pebble handles when Durable is true).
//
// On any construction error, falls back to a plain, non-durable
// NewMemoryScopedBus() with a nil closer, so callers can use this as a
// drop-in for NewMemoryScopedBus() without a separate error-handling path
// for the common case; check the returned error if that fallback matters to
// the caller.
func WithDurableBackend(cfg DurableBusConfig) (bus *MemoryScopedBus, closeFn func() error, err error) {
	gbus, closer, err := NewDurableEventBus(cfg)
	if err != nil {
		return NewMemoryScopedBus(), nil, err
	}
	simple := gevents.NewSimple[PipelineEvent](gbus)
	return NewMemoryScopedBus(simple), closer, nil
}
