package code

import (
	"context"
	"testing"

	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/store"
)

func runCode(code string, state map[string]any) (store.DocumentMutator, error) {
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
