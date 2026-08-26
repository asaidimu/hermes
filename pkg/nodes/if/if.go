package ifnode

import (
	"context"
	"fmt"

	"github.com/asaidimu/hermes/pkg/expr"
	"github.com/asaidimu/hermes/pkg/nodekit"
)

type IfConfig struct {
	Mode        string `config:"mode" anansi:"default=simple"`
	Key         string `config:"key" anansi:"default=state.value"`
	Predicate   string `config:"predicate" anansi:"default==="`
	Value       string `config:"value" anansi:"default=10"`
	Conditions  any    `config:"conditions"`
	Combinators any    `config:"combinators"`
	Condition   any    `config:"condition"`
}

var Node = nodekit.Define(nodekit.TypedDefinition[IfConfig]{
	Kind:        "if",
	Label:       "If / Condition",
	Description: "Branch to 'true' or 'false' output based on conditions with AND/OR combinators.",
	Type:        "executable",
	Handles: func(cfg *IfConfig) []nodekit.HandleSpec {
		return []nodekit.HandleSpec{
			{Type: nodekit.HandleTarget, ID: ""},
			{Type: nodekit.HandleSource, ID: "if", Label: "true"},
			{Type: nodekit.HandleSource, ID: "else", Label: "false"},
		}
	},
	HandlesJS: `() => [{"type":"target","id":"","kind":"executable"},{"type":"source","id":"if","label":"true","kind":"executable"},{"type":"source","id":"else","label":"false","kind":"executable"}]`,
	Router:    router,
})

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

func evalConditionItem(ctx context.Context, cond map[string]any, state map[string]any) (bool, error) {
	field, _ := cond["field"].(string)
	operator, _ := cond["operator"].(string)
	value, _ := cond["value"].(string)
	return expr.EvalBody(ctx, conditionEvalString(field, operator, value), state)
}

func router(ctx context.Context, nCtx *nodekit.TypedRunContext[IfConfig]) (string, error) {
	cfg := nCtx.Config
	state := nCtx.State

	// New path: conditions array with combinators.
	if conditions, ok := cfg.Conditions.([]any); ok && len(conditions) > 0 {
		var combinators []any
		if c, ok := cfg.Combinators.([]any); ok {
			combinators = c
		}
		result, err := evalConditionItem(ctx, asMap(conditions[0]), state)
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
			condResult, err := evalConditionItem(ctx, asMap(conditions[i]), state)
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

	mode := cfg.Mode
	if mode == "" {
		mode = "simple"
	}

	ok, err := evalLegacy(ctx, cfg, state, mode)
	if err != nil || !ok {
		return "else", nil
	}
	return "if", nil
}

func evalLegacy(ctx context.Context, cfg *IfConfig, state map[string]any, mode string) (bool, error) {
	if mode == "simple" {
		return expr.EvalBody(ctx, conditionEvalString(cfg.Key, cfg.Predicate, cfg.Value), state)
	}
	// complex mode: condition is a raw JS expression
	switch c := cfg.Condition.(type) {
	case string:
		return expr.EvalBody(ctx, c, state)
	default:
		return false, fmt.Errorf("invalid condition type")
	}
}

func asMap(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return map[string]any{}
}
