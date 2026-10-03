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

	// C1 (20261003 hotpath audit): the action runs on a DEEP-COPIED
	// snapshot, OUTSIDE the store read lock — the same shape
	// #review-20260910-009 gave the live execution path. Previously the
	// action ran inside st.Read's callback: a step calling st.Update
	// self-deadlocked (RWMutex is non-reentrant), and arbitrary user code
	// ran while all writers were blocked. The replay-specific context and
	// the actionErr-vs-readErr distinction are preserved.
	var stateCopy map[string]any
	if readErr := st.Read(func(state map[string]any) error {
		stateCopy = store.DeepCopyMap(state)
		return nil
	}); readErr != nil {
		return readErr
	}

	mutator, actionErr := step.Action(ctx, pCtx, stateCopy)
	if actionErr != nil {
		return actionErr
	}
	if mutator != nil {
		return st.Update(ctx, mutator)
	}
	return nil
}
