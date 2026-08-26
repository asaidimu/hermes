package arithmetic

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/store"
)

var Node = nodekit.NodeDefinition{
	Kind:        "arithmetic",
	Label:       "Arithmetic",
	Description: "Performs a mathematical operation on two values.",
	Type:        "executable",
	ConfigSchema: json.RawMessage(`{
		"version": "1.0.0",
		"name": "arithmetic",
		"fields": {
			"operation": { "name": "operation", "type": "string", "default": "add" },
			"left":      { "name": "left", "type": "string", "default": "" },
			"right":     { "name": "right", "type": "string", "default": "" },
			"key":       { "name": "key", "type": "string", "default": "", "required": true }
		}
	}`),
	Handles: func(config map[string]any) []nodekit.HandleSpec {
		return []nodekit.HandleSpec{
			{Type: nodekit.HandleTarget, ID: ""},
			{Type: nodekit.HandleSource, ID: ""},
		}
	},
	HandlesJS: `() => [{"type":"target","id":"","kind":"executable"},{"type":"source","id":"","kind":"executable"}]`,
	Run:       run,
}

func run(ctx context.Context, nCtx nodekit.NodeRunContext) (store.Mutator, error) {
	cfg := nCtx.Config
	operation, _ := cfg["operation"].(string)
	if operation == "" {
		operation = "add"
	}
	rawLeft := cfg["left"]
	rawRight := cfg["right"]
	key, _ := cfg["key"].(string)

	if key == "" {
		return nil, fmt.Errorf("Arithmetic node: 'key' is required (where to store the result).")
	}

	// Helper to resolve an operand: tries literal conversion first, then looks up state keys
	resolve := func(raw any) (float64, bool) {
		if num, ok := nodekit.Number(raw); ok {
			return num, true
		}
		// Fallback: look up key in state if operand is a state field reference
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
