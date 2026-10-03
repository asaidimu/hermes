// Step execution during replay. Split out of replayer.go by R.1 (pure
// move); C1 fixes the lock discipline here.
package replay

import (
	"context"

	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
)

// replayStep re-executes a pure step's Action against the store. The
// step's Action closure captures the node logic; we call it with a
// read-only state snapshot and apply the returned mutator.
func replayStep(ctx context.Context, st store.Store, step pipeline.Step) error {
	if step.Action == nil {
		return nil
	}
	// Build a minimal PipelineContext for the step. During replay,
	// the step doesn't need bus/logger/resource resolution — it just
	// needs to compute its mutator.
	pCtx := pipeline.NewReplayContext()
	var mutator store.Mutator
	var actionErr error
	readErr := st.Read(func(state map[string]any) error {
		mutator, actionErr = step.Action(ctx, pCtx, state)
		return nil
	})
	if readErr != nil {
		return readErr
	}
	if actionErr != nil {
		return actionErr
	}
	if mutator != nil {
		return st.Update(ctx, mutator)
	}
	return nil
}
