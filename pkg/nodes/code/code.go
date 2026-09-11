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

	// @note #review-20260910-011 issue status=resolved priority=P2 tags=#review,#concurrency,#sandbox : Sandbox received the live state map — JS could mutate store state directly
	// @author hermes-review
	//
	// Resolved: the sandbox now binds a DEEP COPY of state (store.DeepCopyMap)
	// and only the returned patch reaches the store. Previously nCtx.State
	// was bound into the goja VM by reference, so user JS like
	// `state.count = 999` mutated whatever map the caller handed over,
	// bypassing the mutator/atomic-stage-commit model: the step's recorded
	// delta missed direct mutations (action log / Replayer deltas diverged
	// from real state), and when the caller was ExecuteStageSteps' read
	// path the mutation raced concurrent readers — Go's fatal
	// concurrent-map-write panic, unrecoverable by design.
	//
	// The node-level copy is deliberately independent of the step engine's
	// own snapshot (#review-20260910-009): the replay path (replayStep) and
	// any other caller that passes a live map are covered here too, and the
	// contract is now local to this node rather than an assumption about
	// every caller. Direct JS mutations hit the private copy and are
	// discarded; the returned patch is the only write path.
	vmState := store.DeepCopyMap(nCtx.State)
	result, err := expr.RunSandbox(ctx, code, vmState)
	if err != nil {
		return nil, err
	}

	patch, ok := result.(map[string]any)
	if !ok {
		return nil, nil
	}
	return nodekit.PatchMutator(patch), nil
}
