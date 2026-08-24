package switchnode

import (
	"context"
	"testing"

	"github.com/asaidimu/hermes/pkg/nodekit"
)

func route(t *testing.T, cfg map[string]any, state map[string]any) string {
	t.Helper()
	handle, err := Node.Router(context.Background(), nodekit.NodeRunContext{Config: cfg, State: state, NodeID: "sw1"})
	if err != nil {
		t.Fatalf("router error: %v", err)
	}
	return handle
}

func TestSwitchArrayCases(t *testing.T) {
	cfg := map[string]any{
		"value":         "state.value",
		"cases":         `[{"match":"admin","id":"admin_case","label":"Admin"},{"match":"user","id":"user_case","label":"User"}]`,
		"defaultHandle": "default",
	}
	if got := route(t, cfg, map[string]any{"value": "admin"}); got != "admin_case" {
		t.Errorf("match admin: got %q", got)
	}
	if got := route(t, cfg, map[string]any{"value": "user"}); got != "user_case" {
		t.Errorf("match user: got %q", got)
	}
	if got := route(t, cfg, map[string]any{"value": "guest"}); got != "default" {
		t.Errorf("default: got %q", got)
	}
}

func TestSwitchNumericValue(t *testing.T) {
	cfg := map[string]any{
		"value":         "state.value",
		"cases":         `[{"match":"1","id":"one","label":"One"}]`,
		"defaultHandle": "default",
	}
	if got := route(t, cfg, map[string]any{"value": 1}); got != "one" {
		t.Errorf("numeric match: got %q", got)
	}
}

func TestSwitchEvalErrorFallsToDefault(t *testing.T) {
	cfg := map[string]any{
		"value":         "state.missing.field",
		"cases":         "[]",
		"defaultHandle": "fallback",
	}
	if got := route(t, cfg, map[string]any{}); got != "fallback" {
		t.Errorf("error fallback: got %q", got)
	}
}

// Bare-key tests: value without state. prefix resolves via StatePathExpr.

func TestSwitchBareKey(t *testing.T) {
	cfg := map[string]any{
		"value":         "role",
		"cases":         `[{"match":"admin","id":"admin_case","label":"Admin"},{"match":"user","id":"user_case","label":"User"}]`,
		"defaultHandle": "default",
	}
	if got := route(t, cfg, map[string]any{"role": "admin"}); got != "admin_case" {
		t.Errorf("bare key match: got %q", got)
	}
	if got := route(t, cfg, map[string]any{"role": "guest"}); got != "default" {
		t.Errorf("bare key default: got %q", got)
	}
}

func TestSwitchBareKeyNested(t *testing.T) {
	cfg := map[string]any{
		"value":         "user.role",
		"cases":         `[{"match":"editor","id":"editor_case","label":"Editor"}]`,
		"defaultHandle": "default",
	}
	state := map[string]any{"user": map[string]any{"role": "editor"}}
	if got := route(t, cfg, state); got != "editor_case" {
		t.Errorf("nested bare key match: got %q", got)
	}
}
