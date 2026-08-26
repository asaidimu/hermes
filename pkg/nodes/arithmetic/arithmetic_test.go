package arithmetic

import (
	"context"
	"strings"
	"testing"

	"github.com/asaidimu/hermes/pkg/nodekit"
)

func runWith(t *testing.T, cfg map[string]any) (map[string]any, error) {
	t.Helper()
	state := map[string]any{}
	mut, err := run(context.Background(), nodekit.NodeRunContext{Config: cfg})
	if err != nil {
		return nil, err
	}
	if err := mut(state); err != nil {
		return nil, err
	}
	return state, nil
}

func TestRunOperations(t *testing.T) {
	cases := []struct {
		op   string
		l, r string
		want float64
	}{
		{"add", "2", "3", 5},
		{"subtract", "10", "4", 6},
		{"multiply", "3", "7", 21},
		{"divide", "10", "4", 2.5},
		{"modulo", "10", "3", 1},
		{"power", "2", "8", 256},
		{"min", "3", "9", 3},
		{"max", "3", "9", 9},
	}
	for _, c := range cases {
		state, err := runWith(t, map[string]any{
			"operation": c.op, "left": c.l, "right": c.r, "key": "result",
		})
		if err != nil {
			t.Errorf("%s: %v", c.op, err)
			continue
		}
		if state["result"] != c.want {
			t.Errorf("%s: result = %v, want %v", c.op, state["result"], c.want)
		}
	}
}

func TestRunErrors(t *testing.T) {
	if _, err := runWith(t, map[string]any{"left": "1", "right": "2"}); err == nil || !strings.Contains(err.Error(), "'key' is required") {
		t.Errorf("missing key: want error about key, got %v", err)
	}
	if _, err := runWith(t, map[string]any{"left": "abc", "right": "2", "key": "r"}); err == nil || !strings.Contains(err.Error(), "must be numbers") {
		t.Errorf("non-numeric: want operands error, got %v", err)
	}
	if _, err := runWith(t, map[string]any{"operation": "divide", "left": "1", "right": "0", "key": "r"}); err == nil || !strings.Contains(err.Error(), "division by zero") {
		t.Errorf("divide zero: got %v", err)
	}
	if _, err := runWith(t, map[string]any{"operation": "modulo", "left": "1", "right": "0", "key": "r"}); err == nil || !strings.Contains(err.Error(), "modulo by zero") {
		t.Errorf("modulo zero: got %v", err)
	}
	if _, err := runWith(t, map[string]any{"operation": "sqrt", "left": "1", "right": "2", "key": "r"}); err == nil || !strings.Contains(err.Error(), "unsupported operation") {
		t.Errorf("unsupported op: got %v", err)
	}
}

func TestRunNestedKey(t *testing.T) {
	state, err := runWith(t, map[string]any{
		"operation": "add", "left": "1", "right": "2", "key": "math.result",
	})
	if err != nil {
		t.Fatal(err)
	}
	math, ok := state["math"].(map[string]any)
	if !ok || math["result"] != float64(3) {
		t.Errorf("nested key not expanded: %v", state)
	}
}
