package trigger

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/store"
)

var Node = nodekit.NodeDefinition{
	Kind:        "trigger",
	Label:       "Trigger",
	Description: "Starts the state machine workflow with injectables/initial state context.",
	Type:        "executable",
	ConfigSchema: json.RawMessage(`{
		"version": "1.0.0",
		"name": "trigger",
		"fields": {
			"event": { "name": "event", "type": "string", "default": "__manual__" },
			"initialState": { "name": "initialState", "type": "record" },
			"cron": { "name": "cron", "type": "string", "description": "Cron expression for recurring triggers (e.g. '@every 5m', '30 9 * * *'). When set, the trigger fires automatically on schedule." }
		}
	}`),
	Handles: func(config map[string]any) []nodekit.HandleSpec {
		return []nodekit.HandleSpec{{Type: nodekit.HandleSource, ID: ""}}
	},
	HandlesJS: `() => [{"type":"source","id":"","kind":"executable"}]`,
	Run:       run,
}

// run coerces initialState values from strings to boolean/number, mirroring the
// TS trigger node, and returns the coerced state as the initial patch.
func run(ctx context.Context, nCtx nodekit.NodeRunContext) (store.Mutator, error) {
	raw, _ := nCtx.Config["initialState"].(map[string]any)
	if raw == nil {
		raw = map[string]any{}
	}
	patch := make(map[string]any, len(raw))
	for k, v := range raw {
		trimmed := strings.TrimSpace(stringify(v))
		lower := strings.ToLower(trimmed)
		switch {
		case lower == "true":
			patch[k] = true
		case lower == "false":
			patch[k] = false
		default:
			if trimmed != "" {
				if f, ok := nodekit.Number(trimmed); ok {
					patch[k] = f
					continue
				}
			}
			patch[k] = v
		}
	}
	return nodekit.PatchMutator(patch), nil
}

func stringify(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}
