package events

import (
	"context"
	"reflect"
	"sync/atomic"
	"time"

	gevents "github.com/asaidimu/go-events/v2"

	"go.uber.org/zap"
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

// 20261003 refactor (plan R.3): the former MemoryScopedBus interface is
// deleted — it had exactly one implementation. *MemoryScopedBus is the bus
// type used throughout; if a second bus implementation ever becomes real,
// re-derive an interface at that consumer.

// MemoryScopedBus provides event emission and hierarchical bubbling.
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
// handlerMap is the immutable snapshot of a bus's subscriptions. P4.9
// (M3): published via atomic.Pointer and replaced wholesale on every
// Subscribe/unsubscribe, so Emit reads subscriptions LOCK-FREE (the old
// path RLock'd and copy-allocated the handler slice on every emit).
type handlerMap map[string][]EventHandler

type MemoryScopedBus struct {
	parent     *MemoryScopedBus
	path       EventPath
	handlers   atomic.Pointer[handlerMap]
	underlying gevents.SimpleEventBus[PipelineEvent]
	logger     *zap.SugaredLogger
}

// NewMemoryScopedBus creates a root scoped bus with an optional backing go-events bus.
func NewMemoryScopedBus(underlying ...gevents.SimpleEventBus[PipelineEvent]) *MemoryScopedBus {
	var ub gevents.SimpleEventBus[PipelineEvent]
	if len(underlying) > 0 {
		ub = underlying[0]
	}
	b := &MemoryScopedBus{
		path:       make(EventPath, 0),
		underlying: ub,
		logger:     zap.NewNop().Sugar(),
	}
	b.handlers.Store(func() *handlerMap { h := handlerMap{}; return &h }())
	return b
}

// WithLogger returns a copy of the root bus's construction settings with the
// given logger attached, used to surface handler errors that Emit would
// otherwise discard (see review-20260822-006). Only meaningful when called
// on a root bus, before any Scope() children are created from it, since
// Scope() copies the logger onto every descendant at creation time.
func (b *MemoryScopedBus) WithLogger(logger *zap.SugaredLogger) *MemoryScopedBus {
	if logger == nil {
		logger = zap.NewNop().Sugar()
	}
	b.logger = logger
	return b
}

// Scope returns a child bus with the specified path prefix appended.
func (b *MemoryScopedBus) Scope(prefix EventPath) *MemoryScopedBus {
	child := &MemoryScopedBus{
		parent:     b,
		path:       prefix.Clone(),
		underlying: b.underlying,
		logger:     b.logger,
	}
	child.handlers.Store(&handlerMap{})
	return child
}

// Emit broadcasts the event to local subscribers, forwards to go-events if present, and bubbles up to parent.
func (b *MemoryScopedBus) Emit(ctx context.Context, eventType string, evt PipelineEvent) {
	// @note #review-20260822-016 issue P2 resolved status=resolved priority=P2 tags=#review,#concurrency : Emit reads parent and underlying without lock protection
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

	// 1. Dispatch locally to exact match and wildcard "*". P4.9 (M3):
	// the subscription snapshot is read via atomic pointer — no lock, no
	// per-emit slice concatenation. Exact-match handlers run before
	// wildcard handlers, same order as before.
	if handlers := b.handlers.Load(); handlers != nil {
		for _, h := range (*handlers)[eventType] {
			callHandler(ctx, b, h, eventType, evt)
		}
		for _, h := range (*handlers)["*"] {
			callHandler(ctx, b, h, eventType, evt)
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

// callHandler invokes one event handler, logging (never discarding)
// errors — the resolved #review-20260822-006 semantics, extracted so
// both the exact-match and wildcard dispatch paths share it.
func callHandler(ctx context.Context, b *MemoryScopedBus, h EventHandler, eventType string, evt PipelineEvent) {
	if err := h(ctx, evt); err != nil && b.logger != nil {
		b.logger.Error("event handler error", "eventType", eventType, "runId", evt.RunID, "error", err)
	}
}

// Subscribe registers an event handler for the given eventType or "*" for all.
func (b *MemoryScopedBus) Subscribe(eventType string, handler EventHandler) (unsubscribe func()) {
	// P4.9 (M3): copy-on-write publication. The review-20260822-017
	// "wontfix" note sketched exactly this scheme and declined it for
	// lack of profiling evidence; H3's emit-storm finding (handler slice
	// allocation + lock per emit on every bus event) is that evidence,
	// and the scheme also removes the lock from the read path entirely.
	for {
		old := b.handlers.Load()
		next := handlerMap{}
		if old != nil {
			for k, v := range *old {
				next[k] = v
			}
		}
		next[eventType] = append(next[eventType], handler)
		if b.handlers.CompareAndSwap(old, &next) {
			break
		}
	}

	return func() {
		// reflect-based function-pointer identity: see the resolved
		// #review-20260822-004 (comparing loop-variable addresses never
		// matches).
		handlerPtr := reflect.ValueOf(handler).Pointer()
		for {
			old := b.handlers.Load()
			next := handlerMap{}
			if old != nil {
				for k, v := range *old {
					next[k] = v
				}
			}
			list := next[eventType]
			for i, h := range list {
				if reflect.ValueOf(h).Pointer() == handlerPtr {
					next[eventType] = append(list[:i:i], list[i+1:]...)
					break
				}
			}
			if b.handlers.CompareAndSwap(old, &next) {
				return
			}
		}
	}
}

// @note #scoped-bus-opportunity-001 todo P1 wontfix status=wontfix priority=P1 tags=#event-bus,#architecture : Replace MemoryScopedBus with go-events ScopedBus for topic isolation
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
// The safer, additive half of this note — wiring a durable go-events
// backend behind MemoryScopedBus's `underlying` field — was implemented
// (scoped-bus-opportunity-005) and later DELETED by the 20261003 refactor
// (plan D.1): its "durable" store was an in-memory map, `Underlying()` had
// zero callers, and Subscribe never read the backend, so the shape promised
// durability it did not have. A durable event stream should come back as a
// real implementation or not at all. A full ScopedBus migration remains
// open as future work, but needs a design pass on run-identity-based
// subscription filtering first (see the wontfix note on
// scoped-bus-opportunity-004), not just a mechanical swap.
