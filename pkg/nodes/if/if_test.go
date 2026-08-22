package ifnode

import (
	"context"
	"testing"

	"github.com/asaidimu/hermes/pkg/nodekit"
)

func route(t *testing.T, cfg map[string]any, state map[string]any) string {
	t.Helper()
	handle, err := Node.Router(context.Background(), nodekit.NodeRunContext{Config: cfg, State: state, NodeID: "if1"})
	if err != nil {
		t.Fatalf("router error: %v", err)
	}
	return handle
}

func TestLegacySimple(t *testing.T) {
	cfg := map[string]any{"mode": "simple", "key": "state.value", "predicate": "===", "value": "10"}
	if got := route(t, cfg, map[string]any{"value": 10}); got != "if" {
		t.Errorf("equal true: got %q", got)
	}
	if got := route(t, cfg, map[string]any{"value": 20}); got != "else" {
		t.Errorf("equal false: got %q", got)
	}
}

func TestLegacyIncludes(t *testing.T) {
	cfg := map[string]any{"mode": "simple", "key": "state.name", "predicate": "includes", "value": "\"her\""}
	if got := route(t, cfg, map[string]any{"name": "hermes"}); got != "if" {
		t.Errorf("includes true: got %q", got)
	}
	if got := route(t, cfg, map[string]any{"name": "atlas"}); got != "else" {
		t.Errorf("includes false: got %q", got)
	}
}

func TestComplexCondition(t *testing.T) {
	cfg := map[string]any{"mode": "complex", "condition": "return state.value > 5 && state.value < 20;"}
	if got := route(t, cfg, map[string]any{"value": 10}); got != "if" {
		t.Errorf("complex true: got %q", got)
	}
	if got := route(t, cfg, map[string]any{"value": 100}); got != "else" {
		t.Errorf("complex false: got %q", got)
	}
}

func TestConditionsArrayWithCombinators(t *testing.T) {
	cfg := map[string]any{
		"conditions": []any{
			map[string]any{"field": "state.value", "operator": "greater_than", "value": "5"},
			map[string]any{"field": "state.value", "operator": "less_than", "value": "20"},
		},
		"combinators": []any{"and"},
	}
	if got := route(t, cfg, map[string]any{"value": 10}); got != "if" {
		t.Errorf("AND true: got %q", got)
	}
	if got := route(t, cfg, map[string]any{"value": 30}); got != "else" {
		t.Errorf("AND false: got %q", got)
	}

	cfgOr := map[string]any{
		"conditions": []any{
			map[string]any{"field": "state.value", "operator": "equals", "value": "1"},
			map[string]any{"field": "state.value", "operator": "equals", "value": "2"},
		},
		"combinators": []any{"or"},
	}
	if got := route(t, cfgOr, map[string]any{"value": 2}); got != "if" {
		t.Errorf("OR true: got %q", got)
	}
	if got := route(t, cfgOr, map[string]any{"value": 3}); got != "else" {
		t.Errorf("OR false: got %q", got)
	}
}

func TestEvaluationErrorFallsToElse(t *testing.T) {
	cfg := map[string]any{"mode": "complex", "condition": "return state.boom.nothere + 1;"}
	if got := route(t, cfg, map[string]any{}); got != "else" {
		t.Errorf("error fallback: got %q", got)
	}
}

// Bare-key tests: field without state. prefix should resolve via StatePathExpr.

func TestBareKeyConditionsArray(t *testing.T) {
	cfg := map[string]any{
		"conditions": []any{
			map[string]any{"field": "total", "operator": "less_than", "value": "70"},
		},
	}
	// 76 < 70 is false
	if got := route(t, cfg, map[string]any{"total": float64(76)}); got != "else" {
		t.Errorf("bare key less_than false: got %q", got)
	}
	// 50 < 70 is true
	if got := route(t, cfg, map[string]any{"total": float64(50)}); got != "if" {
		t.Errorf("bare key less_than true: got %q", got)
	}
}

func TestBareKeyGreaterEquals(t *testing.T) {
	cfg := map[string]any{
		"conditions": []any{
			map[string]any{"field": "score", "operator": "greater_equals", "value": "90"},
		},
	}
	if got := route(t, cfg, map[string]any{"score": float64(95)}); got != "if" {
		t.Errorf("bare key greater_equals true: got %q", got)
	}
	if got := route(t, cfg, map[string]any{"score": float64(80)}); got != "else" {
		t.Errorf("bare key greater_equals false: got %q", got)
	}
}

func TestBareKeyLegacySimple(t *testing.T) {
	cfg := map[string]any{"mode": "simple", "key": "value", "predicate": "===", "value": "10"}
	if got := route(t, cfg, map[string]any{"value": 10}); got != "if" {
		t.Errorf("bare legacy simple true: got %q", got)
	}
	if got := route(t, cfg, map[string]any{"value": 20}); got != "else" {
		t.Errorf("bare legacy simple false: got %q", got)
	}
}

func TestBareKeyNestedPath(t *testing.T) {
	cfg := map[string]any{
		"conditions": []any{
			map[string]any{"field": "entry.value", "operator": "equals", "value": "true"},
		},
	}
	state := map[string]any{"entry": map[string]any{"value": true}}
	if got := route(t, cfg, state); got != "if" {
		t.Errorf("nested bare key true: got %q", got)
	}
	if got := route(t, cfg, map[string]any{"entry": map[string]any{"value": false}}); got != "else" {
		t.Errorf("nested bare key false: got %q", got)
	}
}

func TestBareKeyMultipleConditions(t *testing.T) {
	cfg := map[string]any{
		"conditions": []any{
			map[string]any{"field": "a", "operator": "greater_than", "value": "1"},
			map[string]any{"field": "b", "operator": "less_than", "value": "10"},
		},
		"combinators": []any{"and"},
	}
	if got := route(t, cfg, map[string]any{"a": float64(5), "b": float64(3)}); got != "if" {
		t.Errorf("bare key AND true: got %q", got)
	}
	if got := route(t, cfg, map[string]any{"a": float64(5), "b": float64(15)}); got != "else" {
		t.Errorf("bare key AND false: got %q", got)
	}
}