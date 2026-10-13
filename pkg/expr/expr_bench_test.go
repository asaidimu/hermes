package expr_test

import (
	"context"
	"testing"

	"github.com/asaidimu/hermes/pkg/expr"
)

// BenchmarkEvalValue measures the per-evaluation cost of the switch
// node's value expression (plan P4.4, H3): pre-fix every eval built a
// fresh goja runtime plus an interrupt watcher goroutine; post-fix VMs
// are pooled.
func BenchmarkEvalValue(b *testing.B) {
	state := map[string]any{"value": "case-2", "n": float64(7)}
	ctx := context.Background()
	for b.Loop() {
		if _, err := expr.EvalValue(ctx, "state.value + '-' + String(state.n)", state); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEvalBody measures the per-evaluation cost of the if/while
// predicate bodies (plan P4.4, H3).
func BenchmarkEvalBody(b *testing.B) {
	state := map[string]any{"count": float64(3), "total": float64(9)}
	ctx := context.Background()
	for b.Loop() {
		ok, err := expr.EvalBody(ctx, "return state.count < state.total", state)
		if err != nil {
			b.Fatal(err)
		}
		if !ok {
			b.Fatal("expected true")
		}
	}
}
