package pipeline

import (
	"context"
	"testing"

	"github.com/asaidimu/hermes/pkg/store"
)

// TestStateSnapshotDeepCopy guards the fix for review-20260826-002: the
// snapshot handed to routers must share no references with live store state.
func TestStateSnapshotDeepCopy(t *testing.T) {
	st := store.NewFreshStore(map[string]any{
		"nested": map[string]any{"a": float64(1)},
		"list":   []any{map[string]any{"b": float64(1)}},
	})

	snap := stateSnapshot(st)

	// Mutate the store after taking the snapshot — both top-level and nested.
	err := st.Update(context.Background(), func(state map[string]any) error {
		nested := state["nested"].(map[string]any)
		nested["a"] = float64(2)
		nested["added"] = true
		list := state["list"].([]any)
		list[0].(map[string]any)["b"] = float64(2)
		state["new"] = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	snapNested := snap["nested"].(map[string]any)
	if snapNested["a"] != float64(1) {
		t.Fatalf("nested value aliased live state: got %v, want 1", snapNested["a"])
	}
	if _, ok := snapNested["added"]; ok {
		t.Fatal("nested key added post-snapshot leaked into snapshot")
	}
	snapList := snap["list"].([]any)
	if snapList[0].(map[string]any)["b"] != float64(1) {
		t.Fatalf("slice element aliased live state: got %v", snapList[0])
	}
	if _, ok := snap["new"]; ok {
		t.Fatal("top-level key added post-snapshot leaked into snapshot")
	}

	// The reverse direction too: mutating the snapshot must not touch the store.
	snapNested["a"] = float64(99)
	var live float64
	_ = st.Read(func(state map[string]any) error {
		live = state["nested"].(map[string]any)["a"].(float64)
		return nil
	})
	if live != float64(2) {
		t.Fatalf("snapshot mutation leaked into live state: got %v, want 2", live)
	}
}
