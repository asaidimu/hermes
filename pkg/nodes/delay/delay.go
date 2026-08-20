package delay

import (
	"context"
	"encoding/json"
	"time"

	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/store"
)

var Node = nodekit.NodeDefinition{
	Kind:        "delay",
	Label:       "Delay",
	Description: "Wait for a given number of milliseconds.",
	Type:        "executable",
	ConfigSchema: json.RawMessage(`{
		"version": "1.0.0",
		"name": "delay",
		"fields": {
			"ms": { "name": "ms", "type": "number", "default": 1000, "required": true }
		}
	}`),
	Handles: func(config map[string]any) []nodekit.HandleSpec {
		return []nodekit.HandleSpec{
			{Type: nodekit.HandleTarget, ID: ""},
			{Type: nodekit.HandleSource, ID: ""},
		}
	},
	Run: run,
}

func run(ctx context.Context, nCtx nodekit.NodeRunContext) (store.DocumentMutator, error) {
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