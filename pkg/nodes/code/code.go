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
	Effect:      nodekit.EffectSideEffecting,
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

	// @note #review-20260910-011 issue status=open priority=P2 tags=#review,#concurrency,#sandbox : Sandbox receives the live state map — JS can mutate store state directly
	// @author hermes-review
	//
	// nCtx.State is the live map handed out by store.Read (not a copy), and
	// goja binds Go maps by reference — so user JS like `state.count = 999`
	// mutates the run store immediately, bypassing the mutator/atomic-stage-
	// commit model entirely. Two consequences: (1) the step's recorded delta
	// and returned patch miss direct mutations, so the action log (and the
	// Replayer's injected deltas) diverge from real state; (2) since
	// ExecuteStageSteps runs the action under the store's read lock, a
	// direct mutation races any concurrent reader of the same map (e.g.
	// another step's sandbox in a multi-step stage) — Go's fatal
	// concurrent-map-write panic, unrecoverable by design. Consider
	// deep-copying state before binding it into the VM (store.DeepCopyMap)
	// and merging only the returned patch.
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
