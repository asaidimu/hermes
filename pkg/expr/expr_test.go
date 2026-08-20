package expr

import (
	"context"
	"testing"
)

func TestEvalBody(t *testing.T) {
	state := map[string]any{"value": 10, "name": "hermes", "items": []any{1, 2, 3}}

	cases := []struct {
		body string
		want bool
	}{
		{"return (state.value) === (10);", true},
		{"return (state.value) > (5);", true},
		{"return String(state.name).includes(\"her\");", true},
		{"return String(state.name).startsWith(\"her\");", true},
		{"return state.items.length > 2;", true},
		{"return (state.value) === (20);", false},
		{"return state.missing;", false},
		{"throw new Error('boom');", false}, // EvalBody returns err, caller defaults to else
	}

	for _, c := range cases {
		got, err := EvalBody(context.Background(), c.body, state)
		if c.body == "throw new Error('boom');" {
			if err == nil {
				t.Errorf("expected error for %q", c.body)
			}
			continue
		}
		if err != nil {
			t.Errorf("EvalBody(%q) unexpected error: %v", c.body, err)
			continue
		}
		if got != c.want {
			t.Errorf("EvalBody(%q) = %v, want %v", c.body, got, c.want)
		}
	}
}

func TestEvalValue(t *testing.T) {
	state := map[string]any{"value": "admin"}
	got, err := EvalValue(context.Background(), "state.value", state)
	if err != nil {
		t.Fatal(err)
	}
	if got != "admin" {
		t.Errorf("EvalValue = %v, want admin", got)
	}
}

func TestRunSandbox(t *testing.T) {
	state := map[string]any{"text": "hello"}
	code := `return { upper: state.text.toUpperCase() };`
	res, err := RunSandbox(context.Background(), code, state)
	if err != nil {
		t.Fatal(err)
	}
	m, ok := res.(map[string]any)
	if !ok {
		t.Fatalf("expected map result, got %T", res)
	}
	if m["upper"] != "HELLO" {
		t.Errorf("upper = %v, want HELLO", m["upper"])
	}
}

func TestRunSandboxMasksGlobals(t *testing.T) {
	code := `return { hasFetch: typeof fetch, hasWindow: typeof window };`
	res, err := RunSandbox(context.Background(), code, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	m := res.(map[string]any)
	if m["hasFetch"] != "undefined" || m["hasWindow"] != "undefined" {
		t.Errorf("globals not masked: %v", m)
	}
}

func TestRunSandboxSyntaxError(t *testing.T) {
	if _, err := RunSandbox(context.Background(), "return {{ bad", map[string]any{}); err == nil {
		t.Fatal("expected syntax error")
	}
}

func TestString(t *testing.T) {
	cases := []struct {
		in   any
		want string
	}{
		{"abc", "abc"},
		{float64(5), "5"},
		{float64(5.5), "5.5"},
		{true, "true"},
		{map[string]any{}, "[object Object]"},
		{[]any{}, "[object Object]"},
	}
	for _, c := range cases {
		if got := String(c.in); got != c.want {
			t.Errorf("String(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}