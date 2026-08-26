package fork

import (
	"context"

	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/store"
)

type ForkConfig struct {
	Branches []string `config:"branches"`
}

var Node = nodekit.Define(nodekit.TypedDefinition[ForkConfig]{
	Kind:        "fork",
	Label:       "Fork",
	Description: "Split execution into parallel branches. Each branch runs concurrently and must converge at the same Join node.",
	Type:        "executable",
	Handles: func(cfg *ForkConfig) []nodekit.HandleSpec {
		specs := []nodekit.HandleSpec{{Type: nodekit.HandleTarget, ID: "", Label: "in"}}
		branches := cfg.Branches
		if len(branches) == 0 {
			branches = []string{"a", "b"}
		}
		for _, b := range branches {
			specs = append(specs, nodekit.HandleSpec{Type: nodekit.HandleSource, ID: b, Label: b})
		}
		return specs
	},
	HandlesJS: `(config) => {
  const specs = [{ type: "target", id: "", label: "in", kind: "executable" }];
  const branches = config.branches || ["a", "b"];
  for (const b of branches) {
    specs.push({ type: "source", id: String(b), label: String(b), kind: "executable" });
  }
  return specs;
}`,
	Run: run,
})

func run(ctx context.Context, nCtx *nodekit.TypedRunContext[ForkConfig]) (store.Mutator, error) {
	return nil, nil
}
