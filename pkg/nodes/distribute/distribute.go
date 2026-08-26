package distribute

import (
	"context"

	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/store"
)

type DistributeConfig struct {
	ItemsKey string `config:"itemsKey" anansi:"default=items"`
	ItemKey  string `config:"itemKey"  anansi:"default=item"`
}

var Node = nodekit.Define(nodekit.TypedDefinition[DistributeConfig]{
	Kind:        "distribute",
	Label:       "Distribute (Parallel For-Each)",
	Description: "Execute the body concurrently for each element in an array. Each iteration gets its own sub-pipeline with the element injected.",
	Type:        "executable",
	BodyHandle:  "do",
	Handles: func(cfg *DistributeConfig) []nodekit.HandleSpec {
		return []nodekit.HandleSpec{
			{Type: nodekit.HandleTarget, ID: ""},
			{Type: nodekit.HandleSource, ID: "done", Label: "done"},
			{Type: nodekit.HandleSource, ID: "do", Label: "do"},
		}
	},
	HandlesJS: `() => [{"type":"target","id":"","kind":"executable"},{"type":"source","id":"done","label":"done","kind":"executable"},{"type":"source","id":"do","label":"do","kind":"executable"}]`,
	Run: func(ctx context.Context, nCtx *nodekit.TypedRunContext[DistributeConfig]) (store.Mutator, error) {
		items, _ := nCtx.State[nCtx.Config.ItemsKey]
		key := "__$" + nCtx.NodeID + "__items__"
		return nodekit.PatchMutator(map[string]any{key: items}), nil
	},
	Router: nil,
})
