package trycatch

import (
	"context"
	"testing"

	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/store"
)

func TestRouterDoneWithoutErrors(t *testing.T) {
	handle, err := Node.Router(context.Background(), nodekit.NodeRunContext{
		NodeID: "tc1",
		Config: map[string]any{"errorKey": "error"},
		Errors: map[string]any{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if handle != "done" {
		t.Errorf("handle = %q, want done", handle)
	}
}

func TestRouterCatchWithErrorsWritesStore(t *testing.T) {
	st := store.NewMemoryStore(nil)
	handle, err := Node.Router(context.Background(), nodekit.NodeRunContext{
		NodeID: "tc1",
		Config: map[string]any{"errorKey": "error"},
		Errors: map[string]any{
			"nodeA": core.NewSystemError("INTERNAL_ERROR", "boom"),
		},
		Store: st,
	})
	if err != nil {
		t.Fatal(err)
	}
	if handle != "catch" {
		t.Errorf("handle = %q, want catch", handle)
	}

	data, _ := st.ExportJSON()
	errVal, ok := data["error"].(map[string]any)
	if !ok {
		t.Fatalf("errorKey not written to store: %v", data)
	}
	if errVal["message"] != "boom" {
		t.Errorf("error message = %v, want boom", errVal["message"])
	}
}

func TestRouterMultipleErrorsAggregates(t *testing.T) {
	handle, err := Node.Router(context.Background(), nodekit.NodeRunContext{
		NodeID: "tc1",
		Config: map[string]any{"errorKey": "error"},
		Errors: map[string]any{
			"a": core.NewSystemError("INTERNAL_ERROR", "e1"),
			"b": core.NewSystemError("INTERNAL_ERROR", "e2"),
		},
		Store: store.NewMemoryStore(nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	if handle != "catch" {
		t.Errorf("handle = %q, want catch", handle)
	}
}
