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