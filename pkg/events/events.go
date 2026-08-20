package events

import (
	"context"
	"sync"
	"time"

	gevents "github.com/asaidimu/go-events/v2"
)

// PathNode represents an ancestor in the hierarchical execution path.
type PathNode struct {
	Kind  string `json:"kind"`  // "pipeline" | "stage" | "step"
	ID    string `json:"id"`
	Label string `json:"label"`
}

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
	Underlying() gevents.SimpleEventBus[PipelineEvent]
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

	b.handlers[eventType] = append(b.handlers[eventType], handler)

	return func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		list := b.handlers[eventType]
		for i, h := range list {
			// Compare func pointers
			if &h == &handler {
				b.handlers[eventType] = append(list[:i], list[i+1:]...)
				break
			}
		}
	}
}

var _ ScopedEventBus = (*MemoryScopedBus)(nil)
