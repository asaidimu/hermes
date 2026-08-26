package transformer

import (
	"context"
	"testing"

	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/store"
)

func runRules(t *testing.T, state map[string]any, rules []any) (map[string]any, error) {
	t.Helper()
	st := store.NewFreshStore(state)
	mut, err := run(context.Background(), nodekit.NodeRunContext{
		Config: map[string]any{"rules": rules},
		State:  state,
	})
	if err != nil {
		return nil, err
	}
	if mut != nil {
		if err := st.Update(context.Background(), mut); err != nil {
			return nil, err
		}
	}
	return st.ExportJSON()
}

func TestSetValueAndExtract(t *testing.T) {
	state, err := runRules(t, map[string]any{"a": float64(5)}, []any{
		map[string]any{"targetKey": "out", "sourceKey": "a", "action": "EXTRACT", "actionParam": ""},
		map[string]any{"targetKey": "fixed", "sourceKey": "", "action": "SET_VALUE", "actionParam": "42"},
		map[string]any{"targetKey": "flag", "sourceKey": "", "action": "SET_VALUE", "actionParam": "true"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if state["out"] != float64(5) {
		t.Errorf("out = %v, want 5", state["out"])
	}
	if state["fixed"] != float64(42) {
		t.Errorf("fixed = %v, want 42", state["fixed"])
	}
	if state["flag"] != true {
		t.Errorf("flag = %v, want true", state["flag"])
	}
}

func TestDeleteField(t *testing.T) {
	state, err := runRules(t, map[string]any{"a": map[string]any{"b": float64(1), "c": float64(2)}}, []any{
		map[string]any{"targetKey": "a.b", "sourceKey": "", "action": "DELETE_FIELD", "actionParam": ""},
	})
	if err != nil {
		t.Fatal(err)
	}
	a := state["a"].(map[string]any)
	if _, ok := a["b"]; ok {
		t.Errorf("a.b should be deleted: %v", a)
	}
	if a["c"] != float64(2) {
		t.Errorf("a.c should remain: %v", a)
	}
}

func TestDeleteTopLevelField(t *testing.T) {
	state, err := runRules(t, map[string]any{"remove": float64(1), "keep": float64(2)}, []any{
		map[string]any{"targetKey": "remove", "sourceKey": "", "action": "DELETE_FIELD", "actionParam": ""},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := state["remove"]; ok {
		t.Errorf("remove should be deleted: %v", state)
	}
	if state["keep"] != float64(2) {
		t.Errorf("keep should remain: %v", state)
	}
}

func TestMapFieldAndCount(t *testing.T) {
	state, err := runRules(t, map[string]any{"users": []any{
		map[string]any{"name": "alice"}, map[string]any{"name": "bob"},
	}}, []any{
		map[string]any{"targetKey": "names", "sourceKey": "users", "action": "MAP_FIELD", "actionParam": "name"},
		map[string]any{"targetKey": "count", "sourceKey": "users", "action": "COUNT", "actionParam": ""},
	})
	if err != nil {
		t.Fatal(err)
	}
	names := state["names"].([]any)
	if len(names) != 2 || names[0] != "alice" || names[1] != "bob" {
		t.Errorf("names = %v", names)
	}
	if state["count"] != float64(2) {
		t.Errorf("count = %v, want 2", state["count"])
	}
}

func TestFilterList(t *testing.T) {
	state, err := runRules(t, map[string]any{"items": []any{
		map[string]any{"kind": "a", "v": float64(1)}, map[string]any{"kind": "b", "v": float64(2)},
	}}, []any{
		map[string]any{"targetKey": "filtered", "sourceKey": "items", "action": "FILTER_LIST", "actionParam": "kind=a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	filtered := state["filtered"].([]any)
	if len(filtered) != 1 {
		t.Fatalf("filtered = %v", filtered)
	}
	if filtered[0].(map[string]any)["v"] != float64(1) {
		t.Errorf("wrong item filtered: %v", filtered[0])
	}
}

func TestCoalesceAndDefaultIfEmpty(t *testing.T) {
	state, err := runRules(t, map[string]any{"fallback": "fb"}, []any{
		map[string]any{"targetKey": "res", "sourceKey": "missing", "action": "COALESCE", "actionParam": "fallback"},
		map[string]any{"targetKey": "d", "sourceKey": "emptyStr", "action": "DEFAULT_IF_EMPTY", "actionParam": "99"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if state["res"] != "fb" {
		t.Errorf("res = %v, want fb", state["res"])
	}
	if state["d"] != float64(99) {
		t.Errorf("d = %v, want 99", state["d"])
	}
}

func TestGroupBy(t *testing.T) {
	state, err := runRules(t, map[string]any{"rows": []any{
		map[string]any{"cat": "x"}, map[string]any{"cat": "y"}, map[string]any{"cat": "x"},
	}}, []any{
		map[string]any{"targetKey": "groups", "sourceKey": "rows", "action": "GROUP_BY", "actionParam": "cat"},
	})
	if err != nil {
		t.Fatal(err)
	}
	groups := state["groups"].(map[string]any)
	if len(groups["x"].([]any)) != 2 || len(groups["y"].([]any)) != 1 {
		t.Errorf("groups = %v", groups)
	}
}

func TestDateFormatAndDiff(t *testing.T) {
	state, err := runRules(t, map[string]any{"date": "2026-08-20T10:30:00Z"}, []any{
		map[string]any{"targetKey": "fmt", "sourceKey": "date", "action": "FORMAT_DATE", "actionParam": "YYYY-MM-DD"},
		map[string]any{"targetKey": "days", "sourceKey": "date", "action": "DATE_DIFF", "actionParam": "system.now|days"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if state["fmt"] != "2026-08-20" {
		t.Errorf("fmt = %v, want 2026-08-20", state["fmt"])
	}
	if _, ok := state["days"].(float64); !ok {
		t.Errorf("days should be a number: %v", state["days"])
	}
}

func TestMergeAndFlatten(t *testing.T) {
	state, err := runRules(t, map[string]any{
		"base":   map[string]any{"x": float64(1)},
		"extra":  map[string]any{"y": float64(2)},
		"nested": map[string]any{"a": map[string]any{"b": float64(3)}, "c": float64(4)},
	}, []any{
		map[string]any{"targetKey": "merged", "sourceKey": "base", "action": "MERGE_OBJECTS", "actionParam": "extra"},
		map[string]any{"targetKey": "flat", "sourceKey": "nested", "action": "FLATTEN_OBJECT", "actionParam": ""},
	})
	if err != nil {
		t.Fatal(err)
	}
	m := state["merged"].(map[string]any)
	if m["x"] != float64(1) || m["y"] != float64(2) {
		t.Errorf("merged = %v", m)
	}
	f := state["flat"].(map[string]any)
	if f["a.b"] != float64(3) || f["c"] != float64(4) {
		t.Errorf("flat = %v", f)
	}
}

func TestSortUniqueSlice(t *testing.T) {
	state, err := runRules(t, map[string]any{"list": []any{float64(3), float64(1), float64(2), float64(2)}}, []any{
		map[string]any{"targetKey": "sorted", "sourceKey": "list", "action": "SORT_LIST", "actionParam": ":asc"},
		map[string]any{"targetKey": "unique", "sourceKey": "sorted", "action": "UNIQUE_LIST", "actionParam": ""},
		map[string]any{"targetKey": "sliced", "sourceKey": "sorted", "action": "SLICE_LIST", "actionParam": "0:2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	sorted := state["sorted"].([]any)
	if len(sorted) != 4 || sorted[0] != float64(1) {
		t.Errorf("sorted = %v", sorted)
	}
	unique := state["unique"].([]any)
	if len(unique) != 3 {
		t.Errorf("unique = %v", unique)
	}
	sliced := state["sliced"].([]any)
	if len(sliced) != 2 {
		t.Errorf("sliced = %v", sliced)
	}
}

func TestCastAndCase(t *testing.T) {
	state, err := runRules(t, map[string]any{"num": "5", "txt": "hello"}, []any{
		map[string]any{"targetKey": "n", "sourceKey": "num", "action": "CAST_TYPE", "actionParam": "number"},
		map[string]any{"targetKey": "up", "sourceKey": "txt", "action": "CASE_TRANSFORM", "actionParam": "upper"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if state["n"] != float64(5) {
		t.Errorf("n = %v, want 5", state["n"])
	}
	if state["up"] != "HELLO" {
		t.Errorf("up = %v, want HELLO", state["up"])
	}
}

func TestReductionAndFind(t *testing.T) {
	state, err := runRules(t, map[string]any{"nums": []any{float64(1), float64(2), float64(3)}, "objs": []any{
		map[string]any{"id": float64(1), "v": "a"}, map[string]any{"id": float64(2), "v": "b"},
	}}, []any{
		map[string]any{"targetKey": "sum", "sourceKey": "nums", "action": "REDUCE_SUM", "actionParam": ""},
		map[string]any{"targetKey": "match", "sourceKey": "objs", "action": "FIND_MATCH", "actionParam": "id=2"},
		map[string]any{"targetKey": "obj", "sourceKey": "objs", "action": "ARRAY_TO_OBJECT", "actionParam": "id:v"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if state["sum"] != float64(6) {
		t.Errorf("sum = %v, want 6", state["sum"])
	}
	if state["match"].(map[string]any)["v"] != "b" {
		t.Errorf("match = %v", state["match"])
	}
	obj := state["obj"].(map[string]any)
	if obj["2"] != "b" {
		t.Errorf("obj = %v", obj)
	}
}

func TestKeyByAndAppend(t *testing.T) {
	state, err := runRules(t, map[string]any{"items": []any{
		map[string]any{"id": "a", "v": float64(1)}, map[string]any{"id": "b", "v": float64(2)},
	}, "existing": []any{float64(0)}}, []any{
		map[string]any{"targetKey": "keyed", "sourceKey": "items", "action": "KEY_BY", "actionParam": "id"},
		map[string]any{"targetKey": "existing", "sourceKey": "items", "action": "APPEND_LIST", "actionParam": ""},
	})
	if err != nil {
		t.Fatal(err)
	}
	keyed := state["keyed"].(map[string]any)
	if keyed["a"].(map[string]any)["v"] != float64(1) {
		t.Errorf("keyed = %v", keyed)
	}
	existing := state["existing"].([]any)
	if len(existing) != 3 {
		t.Errorf("existing = %v", existing)
	}
}
