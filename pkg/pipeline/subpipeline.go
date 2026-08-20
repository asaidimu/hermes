package pipeline

import (
	"context"
	"sync"

	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/store"
)

// ExecuteSubPipelines executes child pipelines concurrently and aggregates
// their results. Child failures are captured per-result (Status "failed" with
// Error set) and do NOT hard-fail the parent: they flow into the stage's
// PipelinesRouter so bounded stages (try-catch) can catch them — mirroring the
// TS `errors` record. The returned error is reserved for infrastructural
// failures (store clone) and parent-run cancellation (abort).
//
// resolver (optional) resolves run-scoped resource keys into handles; it
// propagates to the child run contexts so steps in subpipelines can resolve
// resource dependencies. Children share the parent's runID so their events
// bubble under the parent run (client keys events by runId).
func ExecuteSubPipelines(
	ctx context.Context,
	runID string,
	parentPipelineID string,
	stage Stage,
	path events.EventPath,
	parentStore store.Store,
	bus events.ScopedEventBus,
	logger core.Logger,
	subAddr *SubPipelineAddress,
	resolver func(key string) (any, bool),
) ([]PipelineRunResult, error) {
	if len(stage.Pipelines) == 0 {
		return nil, nil
	}

	results := make([]PipelineRunResult, len(stage.Pipelines))
	var mu sync.Mutex

	var wg sync.WaitGroup
	for idx, def := range stage.Pipelines {
		childIdx := idx
		childDef := def

		// Check if we need to target a specific child pipeline index on resume
		var childEntry *EntryAddress
		if subAddr != nil && subAddr.Index == childIdx {
			childEntry = &EntryAddress{
				Stage: subAddr.Stage,
				Step:  subAddr.Step,
			}
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			childBus := bus.Scope(path.Append("pipeline", childDef.ID, childDef.Label))

			// Clone parent store for child pipeline isolation
			childStore, err := parentStore.Clone()
			if err != nil {
				mu.Lock()
				results[childIdx] = PipelineRunResult{
					Status:     "failed",
					RunID:      runID,
					PipelineID: childDef.ID,
					Error:      core.NewSystemError(core.ErrCodeExecutionFailed, "failed to clone store for subpipeline").WithCause(err),
				}
				mu.Unlock()
				return
			}

			childFactory := NewFactory(childDef, childDef.Schema, FactoryOptions{
				Logger:           logger,
				ResourceResolver: resolver,
			})

			var childRunContext *RunContextImpl
			if childEntry != nil {
				childRunContext = childFactory.PrepareWithEntry(runID, childStore, childBus, *childEntry)
			} else {
				childRunContext = childFactory.Prepare(runID, childStore, childBus)
			}

			res, runErr := childRunContext.Run(ctx)

			if runErr != nil && res.Status != "failed" {
				res = PipelineRunResult{
					Status:     "failed",
					RunID:      runID,
					PipelineID: childDef.ID,
					FinalDoc:   childStore.Document(),
					Error:      runErr,
				}
			}

			mu.Lock()
			results[childIdx] = res
			mu.Unlock()
		}()
	}
	wg.Wait()

	if err := ctx.Err(); err != nil {
		return results, err
	}
	return results, nil
}