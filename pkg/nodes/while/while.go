package while

import (
	"context"
	"fmt"

	"github.com/asaidimu/hermes/pkg/expr"
	"github.com/asaidimu/hermes/pkg/nodekit"
)

type WhileConfig struct {
	Mode      string `config:"mode" anansi:"default=simple"`
	Condition any    `config:"condition"`
}

var Node = nodekit.Define(nodekit.TypedDefinition[WhileConfig]{
	Kind:        "while",
	Label:       "While Loop",
	Description: "Repeatedly execute the 'do' branch as long as the condition remains true.",
	Type:        "executable",
	Handles: func(cfg *WhileConfig) []nodekit.HandleSpec {
		return []nodekit.HandleSpec{
			{Type: nodekit.HandleTarget, ID: ""},
			{Type: nodekit.HandleSource, ID: "done", Label: "done"},
			{Type: nodekit.HandleSource, ID: "do", Label: "do"},
		}
	},
	HandlesJS: `() => [{"type":"target","id":"","kind":"executable"},{"type":"source","id":"done","label":"done","kind":"executable"},{"type":"source","id":"do","label":"do","kind":"executable"}]`,
	Router:    router,
})

// @note #review-20260826-008 observation status=open priority=P3 tags=#review,#robustness : Loop safety relies solely on context cancellation — no iteration cap
// @author ox-alpha
//
// A predicate that never turns false loops until the run's context is
// cancelled (stage timeout, run abort). Each iteration still emits stage/step
// events, so an unbounded loop grows the timeline and event log without
// bound. Consider an optional maxIterations config that fails the stage when
// exceeded, mirroring defensive guards in similar engines.
//
// Router mirrors the TS while node: evaluates the simple predicate or the
// complex condition body, returning "do" / "done". Any evaluation error routes
// to "done".
func router(ctx context.Context, nCtx *nodekit.TypedRunContext[WhileConfig]) (string, error) {
	cfg := nCtx.Config
	mode := cfg.Mode
	if mode == "" {
		mode = "simple"
	}

	var ok bool
	var err error
	if mode == "simple" {
		condition, _ := cfg.Condition.(map[string]any)
		if condition == nil {
			return "done", nil
		}
		key, _ := condition["key"].(string)
		predicate, _ := condition["predicate"].(string)
		value, _ := condition["value"].(string)
		ok, err = expr.EvalBody(ctx, evalString(key, predicate, value), nCtx.State)
	} else {
		condition, _ := cfg.Condition.(string)
		if condition == "" {
			return "done", nil
		}
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
