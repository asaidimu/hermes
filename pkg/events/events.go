package events

import (
	"context"
	"reflect"
	"sync"
	"time"

	gevents "github.com/asaidimu/go-events/v2"
)

// PathNode represents an ancestor in the hierarchical execution path.
type PathNode struct {
	Kind  string `json:"kind"` // "pipeline" | "stage" | "step"
	ID    string `json:"id"`
	Label string `json:"label"`
}

// EventPath represents the hierarchical execution path within a workflow run.
// It tracks the pipeline, stage, and step ancestry for an event, enabling
// scoped event routing and bubbling. Paths are ordered from root to leaf
// (e.g., [pipeline, stage, step]).
// @note #review-20260822-014 issue status=resolved priority=P3 tags=#review,#documentation : EventPath type lacks doc comment
//
// Fixed by adding doc comment explaining EventPath's purpose, ordering
// semantics, and relationship to hierarchical execution paths.
type EventPath []PathNode

func (p EventPath) Clone() EventPath {
	if p == nil {
		return nil
	}
	cp := make(EventPath, len(p))
	copy(cp, p)
	return cp
}

func (p EventPath) Append(kind, id, label string) EventPath {
	cp := p.Clone()
	return append(cp, PathNode{Kind: kind, ID: id, Label: label})
}

// PipelineEvent is emitted across all stage/step/pipeline boundaries.
type PipelineEvent struct {
	Type       string         `json:"type"`
	RunID      string         `json:"runId"`
	PipelineID string         `json:"pipelineId"`
	Path       EventPath      `json:"path"`
	Timestamp  int64          `json:"timestamp"`          // Epoch ms
	Duration   int64          `json:"duration,omitempty"` // ms
	Payload    map[string]any `json:"payload,omitempty"`
}

// EventHandler is a callback for pipeline events.
type EventHandler func(ctx context.Context, evt PipelineEvent) error

// ScopedEventBus provides event emission and hierarchical bubbling.
type ScopedEventBus interface {
	Emit(ctx context.Context, eventType string, evt PipelineEvent)
	Subscribe(eventType string, handler EventHandler) (unsubscribe func())
	Scope(prefix EventPath) ScopedEventBus
}

// MemoryScopedBus is a concurrent-safe implementation of ScopedEventBus.
type MemoryScopedBus struct {
	mu         sync.RWMutex
	parent     *MemoryScopedBus
	path       EventPath
	handlers   map[string][]EventHandler
	underlying gevents.SimpleEventBus[PipelineEvent]
}

// NewMemoryScopedBus creates a root scoped bus with an optional backing go-events bus.
func NewMemoryScopedBus(underlying ...gevents.SimpleEventBus[PipelineEvent]) *MemoryScopedBus {
	var ub gevents.SimpleEventBus[PipelineEvent]
	if len(underlying) > 0 {
		ub = underlying[0]
	}
	return &MemoryScopedBus{
		path:       make(EventPath, 0),
		handlers:   make(map[string][]EventHandler),
		underlying: ub,
	}
}

// Scope returns a child bus with the specified path prefix appended.
func (b *MemoryScopedBus) Scope(prefix EventPath) ScopedEventBus {
	return &MemoryScopedBus{
		parent:     b,
		path:       prefix.Clone(),
		handlers:   make(map[string][]EventHandler),
		underlying: b.underlying,
	}
}

// Underlying returns the underlying go-events SimpleEventBus if any.
func (b *MemoryScopedBus) Underlying() gevents.SimpleEventBus[PipelineEvent] {
	return b.underlying
}

// Emit broadcasts the event to local subscribers, forwards to go-events if present, and bubbles up to parent.
func (b *MemoryScopedBus) Emit(ctx context.Context, eventType string, evt PipelineEvent) {
	// @note #review-20260822-016 issue status=open priority=P2 tags=#review,#concurrency : Emit reads parent and underlying without lock protection
	//
	// Emit reads b.parent and b.underlying without holding any lock. While these fields
	// are only set during Scope() construction and are structurally immutable after that,
	// there is no enforcement or documentation of this immutability contract. A future
	// refactor adding post-construction mutation would silently introduce a data race.
	//
	// Add a doc comment stating immutability, or protect with a lock.
	if evt.Type == "" {
		evt.Type = eventType
	}
	if evt.Timestamp == 0 {
		evt.Timestamp = time.Now().UnixMilli()
	}
	if len(evt.Path) == 0 && len(b.path) > 0 {
		evt.Path = b.path.Clone()
	}

	// 1. Dispatch locally to exact match and wildcard "*"
	var toCall []EventHandler
	b.mu.RLock()
	if hList, ok := b.handlers[eventType]; ok {
		toCall = append(toCall, hList...)
	}
	if hList, ok := b.handlers["*"]; ok {
		toCall = append(toCall, hList...)
	}
	b.mu.RUnlock()

	for _, h := range toCall {
		// @note #review-20260822-006 issue status=open priority=P1 tags=#review,#error-handling : Emit silently discards all handler errors
		//
		// Every handler error is discarded via `_ = h(ctx, evt)`. If a subscriber returns
		// an error indicating a failed write, dropped event, or context cancellation, the
		// caller has no way to know. This defeats Go's error propagation idiom.
		//
		// go-events ScopedBus.Publish() returns errors. Migrating to ScopedBus would
		// fix this at the call site — callers can handle or propagate errors.
		// At minimum, log the error; ideally, collect errors from all handlers and return
		// them or pass them through a provided error channel.
		_ = h(ctx, evt)
	}

	// 2. Underlying go-events bus dispatch (only once at root or if present)
	if b.parent == nil && b.underlying != nil {
		b.underlying.Emit(ctx, eventType, evt)
	}

	// 3. Bubble up to parent
	if b.parent != nil {
		b.parent.Emit(ctx, eventType, evt)
	}
}

// Subscribe registers an event handler for the given eventType or "*" for all.
func (b *MemoryScopedBus) Subscribe(eventType string, handler EventHandler) (unsubscribe func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// @note #review-20260822-017 issue status=open priority=P2 tags=#review,#concurrency : Subscribe holds write lock for append-only operation
	//
	// Subscribe acquires b.mu.Lock() (exclusive write lock) but only appends to a slice.
	// For a read-heavy system where subscriptions are infrequent but Emit is called
	// constantly, a copy-on-write pattern with RLock for Emit and atomic slice
	// replacement for Subscribe would reduce contention. Not a correctness issue, but a
	// performance consideration at scale.
	b.handlers[eventType] = append(b.handlers[eventType], handler)

	return func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		list := b.handlers[eventType]
		// @note #review-20260822-004 issue status=resolved priority=P1 tags=#review,#bug : Incorrect func pointer comparison in unsubscribe closure
		//
		// Fixed by using reflect.ValueOf to compare function pointers instead of
		// comparing addresses of loop variables. The original `&h == &handler`
		// compared the address of the loop variable `h` with the address of the
		// captured `handler` parameter, which would never match.
		handlerPtr := reflect.ValueOf(handler).Pointer()
		for i, h := range list {
			if reflect.ValueOf(h).Pointer() == handlerPtr {
				b.handlers[eventType] = append(list[:i], list[i+1:]...)
				break
			}
		}
	}
}

// @note #scoped-bus-opportunity-001 todo status=open priority=P1 tags=#event-bus,#architecture : Replace MemoryScopedBus with go-events ScopedBus for topic isolation
//
// The go-events/v2 package now provides ScopedBus (scoped.go) which gives
// real topic-level namespace isolation via topic prefixing. MemoryScopedBus
// uses hierarchical bubbling but no actual topic isolation — all scopes
// share the same flat event type namespace.
//
// go-events ScopedBus benefits for hermes:
//   - bus.Scope("run:"+runID) gives per-run topic isolation (no cross-run interference)
//   - bus.Scope("workflow:"+wfID) gives per-workflow isolation
//   - IsolatedScope() creates a separate Pebble store for true data isolation
//   - Publish() returns errors (current Emit discards all handler errors — P1 review note)
//   - Durable event log via Pebble (the underlying field is currently never wired)
//
// Migration path:
//   1. Replace NewMemoryScopedBus() with NewEventBus() + ScopedBus usage
//   2. Replace Emit() calls with scoped.Publish() (error-returning)
//   3. Remove manual parent-bubbling logic (ScopedBus handles it internally)
//   4. Wire the durable backend that was designed but never connected
var _ ScopedEventBus = (*MemoryScopedBus)(nil)
