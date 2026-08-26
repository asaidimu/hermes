package fork

import (
	"context"

	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/store"
)

type ForkConfig struct{}

var Node = nodekit.Define(nodekit.TypedDefinition[ForkConfig]{
	Kind:        "fork",
	Label:       "Fork",
	Description: "Split execution into parallel branches. Multiple edges from the \"do\" handle each become a concurrent sub-pipeline. All branches must converge at the same Join node.",
	Type:        "executable",
	Handles: func(cfg *ForkConfig) []nodekit.HandleSpec {
		return []nodekit.HandleSpec{
			{Type: nodekit.HandleTarget, ID: "", Label: "in"},
			{Type: nodekit.HandleSource, ID: "do", Label: "do"},
		}
	},
	HandlesJS: `() => [
		{ type: "target", id: "", label: "in", kind: "executable" },
		{ type: "source", id: "do", label: "do", kind: "executable" },
	]`,
	Run: run,
})

func run(ctx context.Context, nCtx *nodekit.TypedRunContext[ForkConfig]) (store.Mutator, error) {
	return nil, nil
}
