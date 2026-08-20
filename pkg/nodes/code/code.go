package code

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/asaidimu/hermes/pkg/expr"
	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/store"
)

var Node = nodekit.NodeDefinition{
	Kind:        "code",
	Label:       "JavaScript Code",
	Description: "Execute custom JS transformations on the workflow state.",
	Type:        "executable",
	ConfigSchema: json.RawMessage(`{
		"version": "1.0.0",
		"name": "code",
		"fields": {
			"code": {
				"name": "code",
				"type": "string",
				"default": "// Example: Transform text to uppercase\nreturn {\n  text: state.text?.toUpperCase()\n};",
				"required": true
			}
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
	code, _ := nCtx.Config["code"].(string)
	if strings.TrimSpace(code) == "" {
		return nil, nil
	}

	result, err := expr.RunSandbox(ctx, code, nCtx.State)
	if err != nil {
		return nil, err
	}

	patch, ok := result.(map[string]any)
	if !ok {
		return nil, nil
	}
	return nodekit.PatchMutator(patch), nil
}