package delay

import (
	"context"
	"time"

	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
)

type DelayConfig struct {
	Ms   float64 `config:"ms" anansi:"default=1000"`
	Cron string  `config:"cron"`
}

var Node = nodekit.Define(nodekit.TypedDefinition[DelayConfig]{
	Kind:        "delay",
	Label:       "Delay",
	Description: "Wait for a given number of milliseconds, or until a cron schedule fires.",
	Type:        "executable",
	Handles: func(cfg *DelayConfig) []nodekit.HandleSpec {
		return []nodekit.HandleSpec{
			{Type: nodekit.HandleTarget, ID: ""},
			{Type: nodekit.HandleSource, ID: ""},
		}
	},
	HandlesJS:  `() => [{"type":"target","id":"","kind":"executable"},{"type":"source","id":"","kind":"executable"}]`,
	Run:        run,
	RouterFunc: routerFunc,
})

func routerFunc(ctx context.Context, nCtx *nodekit.TypedRunContext[DelayConfig]) (pipeline.RoutingInstruction, error) {
	if nCtx.Config.Cron == "" {
		return nil, nil
	}
	return pipeline.PauseForCron("__cron_delay__", nCtx.Config.Cron), nil
}

func run(ctx context.Context, nCtx *nodekit.TypedRunContext[DelayConfig]) (store.Mutator, error) {
	if nCtx.Config.Cron != "" {
		return nil, nil
	}
	ms := nCtx.Config.Ms
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
