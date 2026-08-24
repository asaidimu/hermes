package delay

import (
	"context"
	"encoding/json"
	"time"

	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
)

var Node = nodekit.NodeDefinition{
	Kind:        "delay",
	Label:       "Delay",
	Description: "Wait for a given number of milliseconds, or until a cron schedule fires.",
	Type:        "executable",
	ConfigSchema: json.RawMessage(`{
		"version": "1.0.0",
		"name": "delay",
		"fields": {
			"ms": { "name": "ms", "type": "number", "default": 1000 },
			"cron": { "name": "cron", "type": "string", "description": "Cron expression (e.g. '@every 5m', '30 9 * * *'). When set, ms is ignored." }
		}
	}`),
	Handles: func(config map[string]any) []nodekit.HandleSpec {
		return []nodekit.HandleSpec{
			{Type: nodekit.HandleTarget, ID: ""},
			{Type: nodekit.HandleSource, ID: ""},
		}
	},
	HandlesJS:  `() => [{"type":"target","id":"","kind":"executable"},{"type":"source","id":"","kind":"executable"}]`,
	Run:        run,
	RouterFunc: routerFunc,
}

// routerFunc is used when cron is configured — pauses the pipeline and
// schedules a resume via the scheduler. When no cron is set, it returns
// Terminate to match the default leaf-node behavior (prevents advancing
// past branch boundaries in if/switch/try-catch).
func routerFunc(ctx context.Context, nCtx nodekit.NodeRunContext) (pipeline.RoutingInstruction, error) {
	cron, _ := nCtx.Config["cron"].(string)
	if cron == "" {
		return pipeline.Terminate(), nil
	}
	return pipeline.PauseForCron("__cron_delay__", cron), nil
}

func run(ctx context.Context, nCtx nodekit.NodeRunContext) (store.DocumentMutator, error) {
	// If cron is configured, skip the blocking delay — the router will pause.
	cron, _ := nCtx.Config["cron"].(string)
	if cron != "" {
		return nil, nil
	}

	ms, ok := nodekit.Number(nCtx.Config["ms"])
	if !ok {
		ms = 0
	}
	if ms <= 0 {
		return nil, nil
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(time.Duration(ms) * time.Millisecond):
		return nil, nil
	}
}
