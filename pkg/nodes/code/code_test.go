package code

import (
	"context"
	"testing"

	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/store"
)

func runCode(code string, state map[string]any) (store.Mutator, error) {
	return Node.Run(context.Background(), nodekit.NodeRunContext{
		Config: map[string]any{"code": code},
		State:  state,
	})
}

func TestCodeEmptyReturnsNil(t *testing.T) {
	result, err := runCode("", nil)
	if result != nil {
		t.Error("expected nil for empty code")
	}
	if err != nil {
		t.Errorf("expected no error: %v", err)
	}
}

func TestCodeSimpleObjectReturn(t *testing.T) {
	result, err := runCode("return { x: 42 };", map[string]any{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil mutator")
	}
}

func TestCodeReadsState(t *testing.T) {
	state := map[string]any{"name": "hermes"}
	result, err := runCode("return { upper: state.name.toUpperCase() };", state)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil mutator")
	}
}

func TestCodeReturnsUndefined(t *testing.T) {
	result, err := runCode("return undefined;", map[string]any{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// return undefined → SandboxExport returns nil → no patch
	if result != nil {
		t.Errorf("expected nil result, got %v", result)
	}
}

func TestCodeSyntaxErrorReturnsError(t *testing.T) {
	_, err := runCode("return { {{{broken", map[string]any{})
	if err == nil {
		t.Error("expected error for syntax error")
	}
}

func TestCodeNonObjectReturnReturnsNil(t *testing.T) {
	result, err := runCode("return 42;", map[string]any{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// non-object return → no patch
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

// TestCodeDirectStateMutationDiscarded pins the resolved
// #review-20260910-011: the sandbox binds a DEEP COPY of state, so JS that
// mutates `state` directly must not affect the caller's map — only the
// returned patch may write state. Previously goja bound the live map by
// reference, so `state.x = …` mutated store state (or the caller's map)
// behind the mutator/atomic-commit model, and the recorded delta diverged
// from real state.
func TestCodeDirectStateMutationDiscarded(t *testing.T) {
	state := map[string]any{"x": int64(1), "nested": map[string]any{"k": "v"}}
	result, err := runCode(`
		state.x = 999;
		state.brandNew = "nope";
		state.nested.k = "hacked";
		return { ok: true };
	`, state)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil mutator")
	}

	// The caller's map (at runtime: the store's live state or the engine's
	// snapshot) must be untouched by the direct mutations.
	if state["x"] != int64(1) {
		t.Errorf("direct mutation leaked into caller state: x = %v", state["x"])
	}
	if _, exists := state["brandNew"]; exists {
		t.Error("direct mutation leaked: brandNew key created in caller state")
	}
	if nested, ok := state["nested"].(map[string]any); !ok || nested["k"] != "v" {
		t.Errorf("direct mutation leaked into nested map: %v", state["nested"])
	}

	// The returned patch is the only write path: applying it must set ok
	// and nothing else.
	if err := result(map[string]any{}); err != nil {
		t.Fatalf("mutator failed: %v", err)
	}
}
