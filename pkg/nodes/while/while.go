package while

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/asaidimu/hermes/pkg/expr"
	"github.com/asaidimu/hermes/pkg/nodekit"
)

var Node = nodekit.NodeDefinition{
	Kind:        "while",
	Label:       "While Loop",
	Description: "Repeatedly execute the 'do' branch as long as the condition remains true.",
	Type:        "executable",
	ConfigSchema: json.RawMessage(`{
		"version": "1.0.0",
		"name": "while",
		"fields": {
			"mode":      { "name": "mode", "type": "string", "default": "simple", "required": true },
			"key":       { "name": "key", "type": "string", "default": "state.index", "required": true },
			"predicate": { "name": "predicate", "type": "string", "default": "<", "required": true },
			"value":     { "name": "value", "type": "string", "default": "5", "required": true },
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
			{Type: nodekit.HandleSource, ID: "done", Label: "done"},
			{Type: nodekit.HandleSource, ID: "do", Label: "do"},
		}
	},
	HandlesJS: `() => [{"type":"target","id":"","kind":"executable"},{"type":"source","id":"done","label":"done","kind":"executable"},{"type":"source","id":"do","label":"do","kind":"executable"}]`,
	Router:    router,
}

// router mirrors the TS while node: evaluates the simple predicate or the
// complex condition body, returning "do" / "done". Any evaluation error routes
// to "done".
func router(ctx context.Context, nCtx nodekit.NodeRunContext) (string, error) {
	cfg := nCtx.Config
	mode, _ := cfg["mode"].(string)
	if mode == "" {
		mode = "simple"
	}

	var ok bool
	var err error
	if mode == "simple" {
		key, _ := cfg["key"].(string)
		predicate, _ := cfg["predicate"].(string)
		value, _ := cfg["value"].(string)
		ok, err = expr.EvalBody(ctx, evalString(key, predicate, value), nCtx.State)
	} else {
		condition, _ := cfg["condition"].(string)
		ok, err = expr.EvalBody(ctx, condition, nCtx.State)
	}
	if err != nil || !ok {
		return "done", nil
	}
	return "do", nil
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

func evalString(key, predicate, value string) string {
	jsOp := predicate
	if mapped, ok := operatorMap[predicate]; ok {
		jsOp = mapped
	}
	resolvedKey := key
	if resolvedKey == "" {
		resolvedKey = "state.index"
	}
	resolvedKey = expr.StatePathExpr(resolvedKey)
	resolvedValue := value
	if resolvedValue == "" {
		resolvedValue = "undefined"
	}
	switch jsOp {
	case "includes":
		return fmt.Sprintf("return String(%s).includes(%s);", resolvedKey, resolvedValue)
	case "startsWith":
		return fmt.Sprintf("return String(%s).startsWith(%s);", resolvedKey, resolvedValue)
	case "endsWith":
		return fmt.Sprintf("return String(%s).endsWith(%s);", resolvedKey, resolvedValue)
	default:
		return fmt.Sprintf("return (%s) %s (%s);", resolvedKey, jsOp, resolvedValue)
	}
}
