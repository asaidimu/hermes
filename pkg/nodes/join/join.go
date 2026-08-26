package join

import (
	"context"

	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/store"
)

type JoinConfig struct{}

var Node = nodekit.Define(nodekit.TypedDefinition[JoinConfig]{
	Kind:        "join",
	Label:       "Join",
	Description: "Synchronization point: waits for all parallel branches from a Fork to complete before proceeding.",
	Type:        "executable",
	Handles: func(cfg *JoinConfig) []nodekit.HandleSpec {
		return []nodekit.HandleSpec{
			{Type: nodekit.HandleTarget, ID: "", Label: "in"},
			{Type: nodekit.HandleSource, ID: "", Label: "out"},
		}
	},
	HandlesJS: `() => [{"type":"target","id":"","label":"in","kind":"executable"},{"type":"source","id":"","label":"out","kind":"executable"}]`,
	Run:       run,
})

func run(ctx context.Context, nCtx *nodekit.TypedRunContext[JoinConfig]) (store.Mutator, error) {
	// Join is a synchronization marker. The fork's PipelinesRouter ensures
	// all branches complete before jumping here. Nothing to execute.
	return nil, nil
}
