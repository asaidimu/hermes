package arithmetic

import (
	"context"
	"fmt"
	"math"

	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/store"
)

type ArithmeticConfig struct {
	Operation string `config:"operation" anansi:"default=add"`
	Left      any    `config:"left"`
	Right     any    `config:"right"`
	Key       string `config:"key"`
}

var Node = nodekit.Define(nodekit.TypedDefinition[ArithmeticConfig]{
	Kind:        "arithmetic",
	Label:       "Arithmetic",
	Description: "Performs a mathematical operation on two values.",
	Type:        "executable",
	Handles: func(cfg *ArithmeticConfig) []nodekit.HandleSpec {
		return []nodekit.HandleSpec{
			{Type: nodekit.HandleTarget, ID: ""},
			{Type: nodekit.HandleSource, ID: ""},
		}
	},
	HandlesJS: `() => [{"type":"target","id":"","kind":"executable"},{"type":"source","id":"","kind":"executable"}]`,
	Run:       run,
})

func run(ctx context.Context, nCtx *nodekit.TypedRunContext[ArithmeticConfig]) (store.Mutator, error) {
	cfg := nCtx.Config
	operation := cfg.Operation
	if operation == "" {
		operation = "add"
	}
	rawLeft := cfg.Left
	rawRight := cfg.Right
	key := cfg.Key

	if key == "" {
		return nil, fmt.Errorf("Arithmetic node: 'key' is required (where to store the result).")
	}

	resolve := func(raw any) (float64, bool) {
		if num, ok := nodekit.Number(raw); ok {
			return num, true
		}
		if fieldKey, ok := raw.(string); ok && fieldKey != "" {
			if docVal, ok := nodekit.Lookup(nCtx.State, fieldKey); ok {
				return nodekit.Number(docVal)
			}
		}
		return 0, false
	}

	leftVal, leftOK := resolve(rawLeft)
	rightVal, rightOK := resolve(rawRight)
	if !leftOK || !rightOK {
		return nil, fmt.Errorf(
			"Arithmetic node: Operands must be numbers or valid document field references. Received: left=%v, right=%v",
			rawLeft, rawRight,
		)
	}

	var result float64
	switch operation {
	case "add":
		result = leftVal + rightVal
	case "subtract":
		result = leftVal - rightVal
	case "multiply":
		result = leftVal * rightVal
	case "divide":
		if rightVal == 0 {
			return nil, fmt.Errorf("Arithmetic node: division by zero.")
		}
		result = leftVal / rightVal
	case "modulo":
		if rightVal == 0 {
			return nil, fmt.Errorf("Arithmetic node: modulo by zero.")
		}
		result = math.Mod(leftVal, rightVal)
	case "power":
		result = math.Pow(leftVal, rightVal)
	case "min":
		result = math.Min(leftVal, rightVal)
	case "max":
		result = math.Max(leftVal, rightVal)
	default:
		return nil, fmt.Errorf("Arithmetic node: unsupported operation %q.", operation)
	}

	return nodekit.PatchMutator(map[string]any{key: result}), nil
}
