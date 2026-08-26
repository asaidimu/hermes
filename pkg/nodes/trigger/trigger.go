package trigger

import (
	"context"
	"fmt"
	"strings"

	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/store"
)

type TriggerConfig struct {
	Event        string         `config:"event" anansi:"default=__manual__"`
	InitialState map[string]any `config:"initialState"`
	Cron         string         `config:"cron"`
}

var Node = nodekit.Define(nodekit.TypedDefinition[TriggerConfig]{
	Kind:        "trigger",
	Label:       "Trigger",
	Description: "Starts the state machine workflow with injectables/initial state context.",
	Type:        "executable",
	Handles: func(cfg *TriggerConfig) []nodekit.HandleSpec {
		return []nodekit.HandleSpec{{Type: nodekit.HandleSource, ID: ""}}
	},
	HandlesJS: `() => [{"type":"source","id":"","kind":"executable"}]`,
	Run:       run,
})

func run(ctx context.Context, nCtx *nodekit.TypedRunContext[TriggerConfig]) (store.Mutator, error) {
	raw := nCtx.Config.InitialState
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
