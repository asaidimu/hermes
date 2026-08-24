package trigger

import (
	"context"
	"testing"

	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/store"
)

func TestRunCoercesInitialState(t *testing.T) {
	doc := store.NewMemoryStore(nil).Document()
	ctx := context.Background()

	nCtx := nodekit.NodeRunContext{
		Config: map[string]any{
			"initialState": map[string]any{
				"flag":    "true",
				"off":     "false",
				"count":   "42",
				"ratio":   "3.5",
				"zero":    "0",
				"text":    "hello",
				"untrust": "TRUE",
			},
		},
	}

	mut, err := run(ctx, nCtx)
	if err != nil {
		t.Fatal(err)
	}
	if mut == nil {
		t.Fatal("expected a mutator")
	}
	if err := mut(doc); err != nil {
		t.Fatal(err)
	}

	state := doc.Data()
	if state["flag"] != true {
		t.Errorf("flag = %v, want true", state["flag"])
	}
	if state["off"] != false {
		t.Errorf("off = %v, want false", state["off"])
	}
	if state["count"] != float64(42) {
		t.Errorf("count = %v, want 42", state["count"])
	}
	if state["ratio"] != float64(3.5) {
		t.Errorf("ratio = %v, want 3.5", state["ratio"])
	}
	if state["zero"] != float64(0) {
		t.Errorf("zero = %v, want 0", state["zero"])
	}
	if state["text"] != "hello" {
		t.Errorf("text = %v, want hello", state["text"])
	}
	if state["untrust"] != true {
		t.Errorf("untrust = %v, want true (case-insensitive)", state["untrust"])
	}
}
