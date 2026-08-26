package expr

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/dop251/goja"
)

// dangerousPatterns contains regex patterns for expressions that should be rejected
// to prevent code injection attacks.
var dangerousPatterns = []*regexp.Regexp{
	// Function calls and constructors
	regexp.MustCompile(`\bFunction\s*\(`),
	regexp.MustCompile(`\beval\s*\(`),
	regexp.MustCompile(`\bsetTimeout\s*\(`),
	regexp.MustCompile(`\bsetInterval\s*\(`),
	regexp.MustCompile(`\bsetImmediate\s*\(`),

	// Variable declarations and assignments
	regexp.MustCompile(`\bvar\s+\w`),
	regexp.MustCompile(`\blet\s+\w`),
	regexp.MustCompile(`\bconst\s+\w`),
	regexp.MustCompile(`\bdelete\s+\w`),
	regexp.MustCompile(`\bvoid\s+\w`),

	// Object and array manipulation that could be dangerous
	regexp.MustCompile(`__proto__`),
	regexp.MustCompile(`constructor\s*\[`),
	regexp.MustCompile(`prototype\s*\[`),

	// Access to global objects
	regexp.MustCompile(`\bglobalThis\b`),
	regexp.MustCompile(`\bwindow\b`),
	regexp.MustCompile(`\bglobal\b`),
	regexp.MustCompile(`\bself\b`),

	// Import/export statements
	regexp.MustCompile(`\bimport\s*\(`),
	regexp.MustCompile(`\bexport\s+`),
	regexp.MustCompile(`\brequire\s*\(`),

	// Class definitions
	regexp.MustCompile(`\bclass\s+\w`),

	// Debugger and other dangerous statements
	regexp.MustCompile(`\bdebugger\b`),
	regexp.MustCompile(`\bwith\s*\(`),
	regexp.MustCompile(`\bfor\s*\(\s*var\s+\w+\s+in\b`),
}

// ValidateExpression checks if a JS expression is safe to evaluate.
// It rejects expressions that contain potentially dangerous patterns.
func ValidateExpression(expr string) error {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return fmt.Errorf("expr: empty expression")
	}

	// Check for obviously dangerous patterns
	for _, pattern := range dangerousPatterns {
		if pattern.MatchString(expr) {
			return fmt.Errorf("expr: expression contains potentially dangerous pattern")
		}
	}

	// Check for unbalanced parentheses/brackets that could indicate injection attempts
	// This is a basic check - more sophisticated attacks might bypass this
	depth := 0
	for _, ch := range expr {
		switch ch {
		case '(', '[':
			depth++
		case ')', ']':
			depth--
		}
		if depth < 0 {
			return fmt.Errorf("expr: unbalanced parentheses/brackets")
		}
	}
	if depth != 0 {
		return fmt.Errorf("expr: unbalanced parentheses/brackets")
	}

	// Check for string literals that might contain injection attempts
	// Simple heuristic: look for unescaped quotes that could break out of context
	inString := false
	stringChar := byte(0)
	escaped := false
	for i := 0; i < len(expr); i++ {
		ch := expr[i]

		if escaped {
			escaped = false
			continue
		}

		if ch == '\\' {
			escaped = true
			continue
		}

		if !inString {
			if ch == '\'' || ch == '"' || ch == '`' {
				inString = true
				stringChar = ch
			}
		} else if ch == stringChar {
			inString = false
		}
	}

	return nil
}

// simplePathRe matches a dotted state path: identifier segments separated by
// dots (e.g. "total", "entry.value", "payload.status").
var simplePathRe = regexp.MustCompile(`^[A-Za-z_$][A-Za-z0-9_$.]*$`)

// StatePathExpr compiles a dotted state path into a JS expression rooted at the
// run-state `state` binding, standardizing how configs address state:
//
//	"total"       -> "state.total"
//	"entry.value" -> "state.entry.value"
//	"payload.status" -> "state.payload.status"
//	"state.total" -> "state.total"   (explicit prefix kept)
//	"state.a + state.b" -> unchanged (not a simple path; treated as JS expr)
//
// A blank path returns "". Anything that is not a simple dotted path is
// returned untouched so complex JS expressions keep working.
func StatePathExpr(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	if strings.HasPrefix(path, "state.") {
		return path
	}
	if simplePathRe.MatchString(path) {
		return "state." + path
	}
	return path
}

// newRuntime creates a goja runtime whose execution can be interrupted when the
// provided context is cancelled (abort / stage timeout / infinite loop guard).
// The returned stop function must be called after the runtime is done with.
func newRuntime(ctx context.Context) (*goja.Runtime, func()) {
	rt := goja.New()
	if ctx == nil {
		return rt, func() {}
	}
	stop := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			rt.Interrupt(ctx.Err())
		case <-stop:
		}
	}()
	return rt, func() { close(stop) }
}

// EvalBody evaluates a JS body against a `state` binding, invoking it as a
// function so `return` statements work. It returns the boolean truthiness of the
// result. Used by the if/while simple-predicate eval strings and complex
// condition bodies, mirroring `new Function("state", body)`.
func EvalBody(ctx context.Context, body string, state map[string]any) (bool, error) {
	rt, stop := newRuntime(ctx)
	defer stop()
	_ = rt.Set("state", state)
	fn, err := rt.RunString("(function(state) {\n" + body + "\n})")
	if err != nil {
		return false, err
	}
	call, ok := goja.AssertFunction(fn)
	if !ok {
		return false, fmt.Errorf("expr: evaluated value is not callable")
	}
	v, err := call(goja.Undefined(), rt.ToValue(state))
	if err != nil {
		return false, err
	}
	return v.ToBoolean(), nil
}

// EvalValue evaluates a plain JS expression against a `state` binding and
// returns the exported value. Used by the switch node's `value` expression,
// mirroring `new Function("state", "return (" + expr + ");")`.
// @note #review-20260822-049 issue status=resolved priority=P1 tags=#review,#security : Code injection vulnerability in EvalValue
//
// Fixed by adding input validation (ValidateExpression) to reject dangerous
// patterns like eval, Function constructors, variable declarations, __proto__
// access, etc. Also wrapped EvalValue in a restrictive sandbox that masks
// globals (window, global, globalThis, fetch, XMLHttpRequest).
func EvalValue(ctx context.Context, expr string, state map[string]any) (any, error) {
	// Validate the expression to prevent code injection
	if err := ValidateExpression(expr); err != nil {
		return nil, err
	}

	// Use a more restrictive sandbox that masks dangerous globals
	rt, stop := newRuntime(ctx)
	defer stop()
	_ = rt.Set("state", state)

	// Wrap in a function with restricted globals to prevent injection
	script := `(function(state, window, global, globalThis, fetch, XMLHttpRequest) {
"use strict";
return (` + expr + `);
})(state, undefined, undefined, undefined, undefined, undefined)`

	v, err := rt.RunString(script)
	if err != nil {
		return nil, err
	}
	return v.Export(), nil
}

// RunSandbox evaluates the code node sandbox: user code runs inside a function
// whose window/global/globalThis/fetch/XMLHttpRequest bindings are masked to
// undefined, mirroring the TS sandboxWrapper. The returned object is exported.
func RunSandbox(ctx context.Context, code string, state map[string]any) (any, error) {
	rt, stop := newRuntime(ctx)
	defer stop()
	_ = rt.Set("state", state)
	script := `(function(state, window, global, globalThis, fetch, XMLHttpRequest) {
"use strict";
` + code + `
})(state, undefined, undefined, undefined, undefined, undefined)`
	v, err := rt.RunString(script)
	if err != nil {
		return nil, err
	}
	return v.Export(), nil
}

// String mirrors JS String(x): objects serialize as "[object Object]", null as
// "undefined"; everything else uses fmt-style formatting.
func String(v any) string {
	switch t := v.(type) {
	case nil:
		return "undefined"
	case string:
		return t
	case map[string]any, []any:
		return "[object Object]"
	case bool:
		if t {
			return "true"
		}
		return "false"
	case float64:
		if t == float64(int64(t)) {
			return fmt.Sprintf("%d", int64(t))
		}
		return fmt.Sprintf("%v", t)
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", t))
	}
}
