package while

import (
	"context"
	"testing"

	"github.com/asaidimu/hermes/pkg/nodekit"
)

func route(t *testing.T, cfg map[string]any, state map[string]any) string {
	t.Helper()
	handle, err := Node.Router(context.Background(), nodekit.NodeRunContext{Config: cfg, State: state, NodeID: "wh1"})
	if err != nil {
		t.Fatalf("router error: %v", err)
	}
	return handle
}

func TestSimplePredicate(t *testing.T) {
	cfg := map[string]any{"mode": "simple", "key": "state.index", "predicate": "<", "value": "5"}
	if got := route(t, cfg, map[string]any{"index": 3}); got != "do" {
		t.Errorf("in range: got %q", got)
	}
	if got := route(t, cfg, map[string]any{"index": 5}); got != "done" {
		t.Errorf("boundary: got %q", got)
	}
}

func TestComplexCondition(t *testing.T) {
	cfg := map[string]any{"mode": "complex", "condition": "return state.count < 3;"}
	if got := route(t, cfg, map[string]any{"count": 2}); got != "do" {
		t.Errorf("true: got %q", got)
	}
	if got := route(t, cfg, map[string]any{"count": 3}); got != "done" {
		t.Errorf("false: got %q", got)
	}
}

func TestErrorFallsToDone(t *testing.T) {
	cfg := map[string]any{"mode": "complex", "condition": "return state.a.b + 1;"}
	if got := route(t, cfg, map[string]any{}); got != "done" {
		t.Errorf("error fallback: got %q", got)
	}
}

// Bare-key tests: field without state. prefix resolves via StatePathExpr.

func TestBareKeySimple(t *testing.T) {
	cfg := map[string]any{"mode": "simple", "key": "index", "predicate": "<", "value": "5"}
	if got := route(t, cfg, map[string]any{"index": 3}); got != "do" {
		t.Errorf("bare key true: got %q", got)
	}
	if got := route(t, cfg, map[string]any{"index": 5}); got != "done" {
		t.Errorf("bare key false: got %q", got)
	}
}

func TestBareKeyNested(t *testing.T) {
	cfg := map[string]any{"mode": "simple", "key": "loop.counter", "predicate": "<", "value": "3"}
	state := map[string]any{"loop": map[string]any{"counter": float64(1)}}
	if got := route(t, cfg, state); got != "do" {
		t.Errorf("nested bare key true: got %q", got)
	}
	state["loop"].(map[string]any)["counter"] = float64(3)
	if got := route(t, cfg, state); got != "done" {
		t.Errorf("nested bare key false: got %q", got)
	}
}