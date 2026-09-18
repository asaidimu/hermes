package while

import (
	"context"
	"fmt"

	"github.com/asaidimu/hermes/pkg/expr"
	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/store"
)

type WhileConfig struct {
	Mode      string `config:"mode" anansi:"default=simple"`
	Condition any    `config:"condition"`
	// MaxIterations caps how many times the "do" branch may fire before the
	// stage fails outright, instead of relying solely on the run's context
	// (stage timeout, run abort) to eventually cut an unbounded loop off.
	// 0 (the default) keeps today's unlimited behavior.
	MaxIterations int `config:"maxIterations" anansi:"default=0"`
}

var Node = nodekit.Define(nodekit.TypedDefinition[WhileConfig]{
	Kind:        "while",
	Effect:      nodekit.EffectPure,
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

// @note #review-20260826-008 observation P3 resolved status=resolved priority=P3 tags=#review,#robustness : Loop safety relies solely on context cancellation — no iteration cap
// @author ox-alpha
//
// Resolved: added an optional MaxIterations config (0 = unlimited, the
// previous behavior). When set, the router persists an iteration counter
// into the run store keyed by node id and fails the stage outright once
// the cap is exceeded, instead of letting an unbounded loop keep emitting
// stage/step events until the run's context is eventually cancelled.
//
// Router mirrors the TS while node: evaluates the simple predicate or the
// complex condition body, returning "do" / "done". Any predicate evaluation
// error routes to "done"; exceeding MaxIterations instead fails the stage
// with an error, since that is a configuration guard rail rather than a
// normal "condition became false" outcome.
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

	if cfg.MaxIterations > 0 {
		next, failErr := nextIterationCount(ctx, nCtx, cfg.MaxIterations)
		if failErr != nil {
			return "", failErr
		}
		_ = next
	}

	return "do", nil
}

// nextIterationCount reads this node's persisted iteration count (0 if
// unset), returns an error once incrementing it would exceed max, and
// otherwise persists the incremented count back to the run store so the
// next router invocation sees it.
func nextIterationCount(ctx context.Context, nCtx *nodekit.TypedRunContext[WhileConfig], max int) (int, error) {
	iterKey := "__while_" + nCtx.NodeID + "_iterations"
	count := 0
	switch v := nCtx.State[iterKey].(type) {
	case int:
		count = v
	case int64:
		count = int(v)
	case float64:
		count = int(v)
	}
	next := count + 1
	if next > max {
		return count, fmt.Errorf("while node %q exceeded maxIterations (%d)", nCtx.NodeID, max)
	}
	if nCtx.Store != nil {
		_ = nCtx.Store.Update(ctx, store.SetValue(iterKey, next))
	}
	return next, nil
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
