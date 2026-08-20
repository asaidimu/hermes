package foreach

import (
	"context"
	"testing"

	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/store"
)

// runStep applies the for-each run mutator and returns the resulting state.
func runStep(t *testing.T, cfg map[string]any, state map[string]any) map[string]any {
	t.Helper()
	doc := store.NewMemoryStore(nil).Document()
	for k, v := range state {
		if err := doc.Set(k, v); err != nil {
			t.Fatal(err)
		}
	}
	mut, err := run(context.Background(), nodekit.NodeRunContext{
		NodeID: "loop1",
		Config: cfg,
		State:  state,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := mut(doc); err != nil {
		t.Fatal(err)
	}
	return doc.Data()
}

func TestIterateArray(t *testing.T) {
	cfg := map[string]any{"itemsKey": "items", "itemKey": "item"}
	state := map[string]any{"items": []any{"a", "b", "c"}}

	internalKey := "__$loop1__items__"
	states := []map[string]any{state}
	for i := 0; i < 3; i++ {
		next := runStep(t, cfg, states[i])
		states = append(states, next)
		internal := next[internalKey].(map[string]any)
		if internal["hasValue"] != true {
			t.Fatalf("step %d: expected hasValue, got %v", i+1, internal["hasValue"])
		}
		if next["item"] != []any{"a", "b", "c"}[i] {
			t.Errorf("step %d: item = %v", i+1, next["item"])
		}
	}

	final := runStep(t, cfg, states[3])
	if _, ok := final[internalKey]; ok {
		t.Errorf("internal state should be deleted when exhausted, got %v", final[internalKey])
	}
	if _, ok := final["item"]; ok {
		t.Errorf("item key should be deleted when exhausted")
	}

	if got, err := Node.Router(context.Background(), nodekit.NodeRunContext{
		NodeID: "loop1", State: states[1],
	}); err != nil {
		t.Fatal(err)
	} else if got != "do" {
		t.Errorf("router with pending value: got %q", got)
	}
	if got, err := Node.Router(context.Background(), nodekit.NodeRunContext{
		NodeID: "loop1", State: final,
	}); err != nil {
		t.Fatal(err)
	} else if got != "done" {
		t.Errorf("router exhausted: got %q", got)
	}
}

func TestObjectEntries(t *testing.T) {
	cfg := map[string]any{"itemsKey": "users", "itemKey": "user"}
	state := map[string]any{"users": map[string]any{"alice": "a", "bob": "b"}}

	next := runStep(t, cfg, state)
	internal := next["__$loop1__items__"].(map[string]any)
	if internal["type"] != "object" {
		t.Errorf("type = %v, want object", internal["type"])
	}
	if internal["hasValue"] != true {
		t.Errorf("expected a value")
	}
	if _, ok := internal["entries"].([]any); !ok {
		t.Errorf("entries should be array of object values")
	}
}