package ifnode

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/asaidimu/hermes/pkg/expr"
	"github.com/asaidimu/hermes/pkg/nodekit"
)

var Node = nodekit.NodeDefinition{
	Kind:        "if",
	Label:       "If / Condition",
	Description: "Branch to 'true' or 'false' output based on conditions with AND/OR combinators.",
	Type:        "executable",
	ConfigSchema: json.RawMessage(`{
		"version": "1.0.0",
		"name": "if",
		"fields": {
			"mode":        { "name": "mode", "type": "string", "default": "simple", "required": true },
			"key":         { "name": "key", "type": "string", "default": "state.value", "required": true },
			"predicate":   { "name": "predicate", "type": "string", "default": "===", "required": true },
			"value":       { "name": "value", "type": "string", "default": "10", "required": true },
			"conditions":  { "name": "conditions", "type": "array", "required": false },
			"combinators": { "name": "combinators", "type": "array", "required": false },
			"condition": {
				"name": "condition",
				"type": "union",
				"schema": [ { "id": "simpleCondition" }, { "id": "complexCondition" } ],
				"required": true
			}
		},
		"schemas": {
			"simpleCondition": {
				"name": "simpleCondition",
				"fields": {
					"key":       { "name": "key", "type": "string", "required": true },
					"predicate": { "name": "predicate", "type": "string", "required": true },
					"value":     { "name": "value", "type": "string", "required": true }
				}
			},
			"complexCondition": { "name": "complexCondition", "type": "string" }
		}
	}`),
	Handles: func(config map[string]any) []nodekit.HandleSpec {
		return []nodekit.HandleSpec{
			{Type: nodekit.HandleTarget, ID: ""},
			{Type: nodekit.HandleSource, ID: "if", Label: "true"},
			{Type: nodekit.HandleSource, ID: "else", Label: "false"},
		}
	},
	HandlesJS: `() => [{"type":"target","id":"","kind":"executable"},{"type":"source","id":"if","label":"true","kind":"executable"},{"type":"source","id":"else","label":"false","kind":"executable"}]`,
	Router: router,
}

var operatorMap = map[string]string{
	"equals":         "===",
	"not_equals":     "!==",
	"greater_than":   ">",
	"less_than":      "<",
	"greater_equals": ">=",
	"less_equals":    "<=",
	"contains":       "includes",
	"starts_with":    "startsWith",
	"ends_with":      "endsWith",
}

func conditionEvalString(field, operator, value string) string {
	jsOp := operator
	if mapped, ok := operatorMap[operator]; ok {
		jsOp = mapped
	}
	resolvedField := field
	if resolvedField == "" {
		resolvedField = "state.value"
	}
	resolvedField = expr.StatePathExpr(resolvedField)
	resolvedValue := value
	if resolvedValue == "" {
		resolvedValue = "undefined"
	}
	switch jsOp {
	case "includes":
		return fmt.Sprintf("return String(%s).includes(%s);", resolvedField, resolvedValue)
	case "startsWith":
		return fmt.Sprintf("return String(%s).startsWith(%s);", resolvedField, resolvedValue)
	case "endsWith":
		return fmt.Sprintf("return String(%s).endsWith(%s);", resolvedField, resolvedValue)
	default:
		return fmt.Sprintf("return (%s) %s (%s);", resolvedField, jsOp, resolvedValue)
	}
}


func evalCondition(ctx context.Context, cond map[string]any, state map[string]any) (bool, error) {
	field, _ := cond["field"].(string)
	operator, _ := cond["operator"].(string)
	value, _ := cond["value"].(string)
	return expr.EvalBody(ctx, conditionEvalString(field, operator, value), state)
}

// router mirrors the TS if node: evaluates the conditions array with
// combinators (new path) or the legacy key/predicate/value / complex condition
// paths, returning "if" / "else". Any evaluation error routes to "else".
func router(ctx context.Context, nCtx nodekit.NodeRunContext) (string, error) {
	cfg := nCtx.Config
	state := nCtx.State

	if conditions, ok := cfg["conditions"].([]any); ok && len(conditions) > 0 {
		var combinators []any
		if c, ok := cfg["combinators"].([]any); ok {
			combinators = c
		}
		result, err := evalCondition(ctx, asMap(conditions[0]), state)
		if err != nil {
			return "else", nil
		}
		for i := 1; i < len(conditions); i++ {
			combinator := "and"
			if i-1 < len(combinators) {
				if s, ok := combinators[i-1].(string); ok && s != "" {
					combinator = s
				}
			}
			condResult, err := evalCondition(ctx, asMap(conditions[i]), state)
			if err != nil {
				return "else", nil
			}
			if combinator == "or" {
				result = result || condResult
			} else {
				result = result && condResult
			}
		}
		if result {
			return "if", nil
		}
		return "else", nil
	}

	mode, _ := cfg["mode"].(string)
	if mode == "" {
		mode = "simple"
	}

	ok, err := evalLegacy(ctx, cfg, state, mode)
	if err != nil || !ok {
		return "else", nil
	}
	return "if", nil
}

func evalLegacy(ctx context.Context, cfg map[string]any, state map[string]any, mode string) (bool, error) {
	if mode == "simple" {
		key, _ := cfg["key"].(string)
		predicate, _ := cfg["predicate"].(string)
		value, _ := cfg["value"].(string)
		return expr.EvalBody(ctx, conditionEvalString(key, predicate, value), state)
	}
	condition, _ := cfg["condition"].(string)
	return expr.EvalBody(ctx, condition, state)
}

func asMap(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return map[string]any{}
}