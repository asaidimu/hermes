package events

import (
	"context"
	"reflect"
	"sync"
	"time"

	gevents "github.com/asaidimu/go-events/v2"
	"github.com/asaidimu/hermes/pkg/core"
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
//
// @note #review-20260822-016 issue status=resolved priority=P2 tags=#review,#concurrency : Emit reads parent and underlying without lock protection
//
// Resolved: documenting the immutability contract Emit already relied on.
// parent, underlying, and logger are set exactly once, at construction
// (NewMemoryScopedBus or Scope), and never reassigned afterward. Emit reads
// them without holding b.mu because there is nothing to race with — no
// method mutates them post-construction. path and handlers are the only
// fields that change after construction, and both are already guarded by
// b.mu where they're touched. A future change that introduces
// post-construction mutation of parent/underlying/logger must add locking
// around those specific fields.
type MemoryScopedBus struct {
	mu         sync.RWMutex
	parent     *MemoryScopedBus
	path       EventPath
	handlers   map[string][]EventHandler
	underlying gevents.SimpleEventBus[PipelineEvent]
	logger     core.Logger
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
		logger:     core.NopLogger{},
	}
}

// WithLogger returns a copy of the root bus's construction settings with the
// given logger attached, used to surface handler errors that Emit would
// otherwise discard (see review-20260822-006). Only meaningful when called
// on a root bus, before any Scope() children are created from it, since
// Scope() copies the logger onto every descendant at creation time.
func (b *MemoryScopedBus) WithLogger(logger core.Logger) *MemoryScopedBus {
	if logger == nil {
		logger = core.NopLogger{}
	}
	b.logger = logger
	return b
}

// Scope returns a child bus with the specified path prefix appended.
func (b *MemoryScopedBus) Scope(prefix EventPath) ScopedEventBus {
	return &MemoryScopedBus{
		parent:     b,
		path:       prefix.Clone(),
		handlers:   make(map[string][]EventHandler),
		underlying: b.underlying,
		logger:     b.logger,
	}
}

// Underlying returns the underlying go-events SimpleEventBus if any.
func (b *MemoryScopedBus) Underlying() gevents.SimpleEventBus[PipelineEvent] {
	return b.underlying
}

// Emit broadcasts the event to local subscribers, forwards to go-events if present, and bubbles up to parent.
func (b *MemoryScopedBus) Emit(ctx context.Context, eventType string, evt PipelineEvent) {
	// @note #review-20260822-016 issue status=resolved priority=P2 tags=#review,#concurrency : Emit reads parent and underlying without lock protection
	//
	// Resolved: see the fuller resolution note on the MemoryScopedBus
	// struct's doc comment above (this was a duplicate of the same note
	// posted at two locations). Summary: parent/underlying/logger are set
	// once at construction and never reassigned, so reading them here
	// without b.mu is safe; that immutability contract is now documented.
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
		// @note #review-20260822-006 issue status=resolved priority=P1 tags=#review,#error-handling : Emit silently discards all handler errors
		//
		// Resolved (partial, without the full ScopedBus migration): handler
		// errors are now logged instead of silently discarded via `_ = h(...)`.
		// Emit's signature is `Emit(ctx, eventType, evt)` with no return value,
		// used pervasively across the runtime as a fire-and-forget call —
		// changing it to return an error (or an aggregated slice of errors)
		// is a breaking API change to every call site in this repo and is
		// exactly the kind of change the go-events ScopedBus migration
		// (scoped-bus-opportunity-001, deliberately deferred) would need to
		// make properly, together with a decision on how callers should
		// react to a failed subscriber. Logging closes the "no way to know"
		// gap without that wider signature change.
		if err := h(ctx, evt); err != nil && b.logger != nil {
			b.logger.Error("event handler error", "eventType", eventType, "runId", evt.RunID, "error", err)
		}
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
	// @note #review-20260822-017 issue status=wontfix priority=P2 tags=#review,#concurrency : Subscribe holds write lock for append-only operation
	//
	// Investigated and declined: b.handlers is a plain map[string][]EventHandler,
	// not just a slice. Concurrent access to a Go map must be synchronized
	// even for what looks like a single "append" — Subscribe both reads
	// b.handlers[eventType] and (on first subscriber for that eventType,
	// implicitly) writes a new map entry, which races with Emit's read
	// unless serialized. The exclusive Lock here isn't overcautious, it's
	// the actual safety requirement for the map. A copy-on-write scheme
	// (RLock for Emit, atomic slice/map replacement for Subscribe) could
	// reduce contention, but is nontrivial to get right (needs
	// atomic.Pointer to the whole handlers map, replaced wholesale on every
	// Subscribe/unsubscribe) and there's no profiling evidence this lock is
	// a real bottleneck — Subscribe is called at pipeline/watch
	// registration time, not per-event. Not implementing a nontrivial
	// concurrency redesign against a suspected-but-unmeasured performance
	// concern.
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

// @note #scoped-bus-opportunity-001 todo status=wontfix priority=P1 tags=#event-bus,#architecture : Replace MemoryScopedBus with go-events ScopedBus for topic isolation
//
// Investigated and declined for now (not implemented blind). go-events/v2's
// ScopedBus/EventBus (checked directly against the upstream source) is
// fundamentally async: Publish() appends to a durable log and
// SubscribeWithOptions() reads back via a polling/draining goroutine against
// checkpoints — there is no synchronous "call the handler now" path. Two
// things in this codebase depend on synchronous, in-process dispatch that a
// wholesale swap would break:
//
//  1. MemoryScopedBus.Emit's parent-bubbling happens inline, in the calling
//     goroutine, before Emit returns — pipeline/stage/step code relies on
//     handlers (e.g. the timeline recorder) having already observed an
//     event by the time execution moves on. Async delivery would reorder or
//     delay that relative to pipeline progress.
//  2. WatchService (see scoped-bus-opportunity-004) subscribes at the root
//     and resolves pauses by matching arbitrary payload conditions, not by
//     run identity — a design that depends on every subscriber seeing every
//     event on the shared bus, which per-run topic isolation would remove.
//
// The safer, additive half of this note — wiring the durable go-events
// backend that MemoryScopedBus already has a slot for (the `underlying`
// field) — is real and has been implemented: see
// events.NewDurableMemoryScopedBus (scoped-bus-opportunity-005). That gives
// durability/replay without replacing the synchronous dispatch model this
// code depends on. A full ScopedBus migration remains open as future work,
// but needs a design pass on run-identity-based subscription filtering
// first (see the wontfix note on scoped-bus-opportunity-004), not just a
// mechanical swap.
var _ ScopedEventBus = (*MemoryScopedBus)(nil)
