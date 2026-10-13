package events

import (
	"context"
	"testing"
)

// BenchmarkEmit measures the hot dispatch path (plan P4.9, M3): pre-COW
// every emit RLock'd the bus and copy-allocated a handler slice; the
// copy-on-write snapshot makes the read path lock-free and
// allocation-free for the exact-match case.
func BenchmarkEmit_OneHandler(b *testing.B) {
	bus := NewMemoryScopedBus()
	handler := func(ctx context.Context, evt PipelineEvent) error { return nil }
	bus.Subscribe("bench:event", handler)
	evt := PipelineEvent{RunID: "bench-run", Payload: map[string]any{"k": "v"}}
	ctx := context.Background()
	for b.Loop() {
		bus.Emit(ctx, "bench:event", evt)
	}
}

// BenchmarkEmit_WithWildcard includes a "*" subscriber — the second
// slice Emit iterates in place.
func BenchmarkEmit_WithWildcard(b *testing.B) {
	bus := NewMemoryScopedBus()
	handler := func(ctx context.Context, evt PipelineEvent) error { return nil }
	bus.Subscribe("bench:event", handler)
	bus.Subscribe("*", handler)
	evt := PipelineEvent{RunID: "bench-run", Payload: map[string]any{"k": "v"}}
	ctx := context.Background()
	for b.Loop() {
		bus.Emit(ctx, "bench:event", evt)
	}
}

// BenchmarkEmit_Bubbled: emit on a scoped child that bubbles to a parent
// bus with its own subscriptions — the scope-chain walk.
func BenchmarkEmit_Bubbled(b *testing.B) {
	root := NewMemoryScopedBus()
	handler := func(ctx context.Context, evt PipelineEvent) error { return nil }
	root.Subscribe("bench:event", handler)
	child := root.Scope(EventPath{}.Append("pipeline", "p1", "P1"))
	evt := PipelineEvent{RunID: "bench-run", Payload: map[string]any{"k": "v"}}
	ctx := context.Background()
	for b.Loop() {
		child.Emit(ctx, "bench:event", evt)
	}
}
