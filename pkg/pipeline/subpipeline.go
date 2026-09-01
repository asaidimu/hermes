package pipeline

import (
	"context"
	"encoding/json"
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
// If initialState is non-nil, each child gets a fresh store seeded with that
// state instead of a clone of the parent store. This provides true isolation:
// the child starts from scratch with only the specified initial state.
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
	runEnv map[string]any,
	secretLookup func(key string) (any, bool),
) ([]PipelineRunResult, error) {
	if len(stage.Pipelines) == 0 {
		return nil, nil
	}

	// Resolve initialState from stage config if present.
	var initialState map[string]any
	if stage.Config != nil {
		if is, ok := stage.Config["initialState"].(map[string]any); ok {
			initialState = is
		}
	}

	// @note #review-20260825-002 issue status=resolved priority=P2 tags=#review,#concurrency : Shared initialState across concurrent goroutines
	//
	// Resolved: each child now gets its own deep copy of initialState
	// (deepCopyJSONValue below) instead of the same shared map. Confirmed
	// this was a real, not just theoretical, gap: store.NewFreshStore ->
	// NewMemoryStore -> newState only shallow-copies the top-level map —
	// nested maps/slices inside initialState (which, coming from stage
	// config, are typically JSON-shaped and often nested) were shared by
	// reference across every child goroutine. A JSON round-trip is used for
	// the deep copy rather than a hand-rolled recursive copier, since
	// initialState's value space is already constrained to what JSON can
	// represent (it comes from parsed stage config), making round-tripping
	// both correct and simple.

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

			// Create fresh store with initialState, or clone parent if no initialState.
			var childStore store.Store
			var err error
			if initialState != nil {
				childStore = store.NewFreshStore(deepCopyJSONMap(initialState))
			} else {
				childStore, err = parentStore.Clone()
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
			}

			childFactory := NewFactory(childDef, childDef.Schema, FactoryOptions{
				RunEnv:           runEnv,
				SecretLookup:     secretLookup,
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
					FinalState: stateSnapshot(childStore),
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

// deepCopyJSONMap returns a deep copy of m via a JSON encode/decode
// round-trip. Used to give each concurrent sub-pipeline child its own
// independent copy of stage-config-derived initial state (see
// review-20260825-002); safe because m's value space is already
// JSON-compatible (it comes from parsed stage config: maps, slices,
// strings, numbers, bools, nil).
func deepCopyJSONMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		// Fall back to the original map; a failed round-trip on
		// JSON-derived data would indicate a deeper bug (e.g. a
		// non-serializable value smuggled into stage config), and losing
		// isolation is preferable to losing the child's initial state
		// entirely.
		return m
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return m
	}
	return out
}
