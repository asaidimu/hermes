package code

import (
	"context"
	"strings"

	"github.com/asaidimu/hermes/pkg/expr"
	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/store"
)

type CodeConfig struct {
	Code string `config:"code" anansi:"default=// Example: Transform text to uppercase\nreturn {\n  text: state.text?.toUpperCase()\n};"`
}

var Node = nodekit.Define(nodekit.TypedDefinition[CodeConfig]{
	Kind:        "code",
	Label:       "JavaScript Code",
	Description: "Execute custom JS transformations on the workflow state.",
	Type:        "executable",
	Handles: func(cfg *CodeConfig) []nodekit.HandleSpec {
		return []nodekit.HandleSpec{
			{Type: nodekit.HandleTarget, ID: ""},
			{Type: nodekit.HandleSource, ID: ""},
		}
	},
	HandlesJS: `() => [{"type":"target","id":"","kind":"executable"},{"type":"source","id":"","kind":"executable"}]`,
	Run:       run,
})

func run(ctx context.Context, nCtx *nodekit.TypedRunContext[CodeConfig]) (store.Mutator, error) {
	code := nCtx.Config.Code
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
