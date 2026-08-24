package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/asaidimu/go-anansi/v8/core/document"
	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/store"
)

// ExecuteStageSteps runs all steps in a stage concurrently and applies their mutators atomically on success.
// resolver (optional) resolves run-scoped resource keys ("resource:<id>") into handles.
func ExecuteStageSteps(
	ctx context.Context,
	runID string,
	pipelineID string,
	stage Stage,
	path events.EventPath,
	st store.Store,
	bus events.ScopedEventBus,
	logger core.Logger,
	skipStepID string,
	resolver func(key string) (any, bool),
) error {
	if len(stage.Steps) == 0 {
		return nil
	}

	stagePath := path.Append("stage", stage.ID, stage.Label)

	stageCtx := ctx
	var cancel context.CancelFunc
	if stage.Timeout > 0 {
		stageCtx, cancel = context.WithTimeout(ctx, stage.Timeout)
		defer cancel()
	}

	var mu sync.Mutex
	mutators := make([]store.DocumentMutator, 0, len(stage.Steps))
	var errsMu sync.Mutex
	stepErrs := make([]error, 0, len(stage.Steps))

	var wg sync.WaitGroup
	for stepID, stepDef := range stage.Steps {
		step := stepDef
		sID := stepID

		wg.Add(1)
		go func() {
			defer wg.Done()
			stepPath := stagePath.Append("step", sID, step.Label)

			bus.Emit(stageCtx, "step:start", events.PipelineEvent{
				RunID:      runID,
				PipelineID: pipelineID,
				Path:       stepPath,
				Payload: map[string]any{
					"stepId":    sID,
					"stepLabel": step.Label,
				},
			})

			stepStart := time.Now()
			var mutator store.DocumentMutator
			var stepErr error

			retries := step.Retries
			if retries < 0 {
				retries = 0
			}

			for attempt := 0; attempt <= retries; attempt++ {
				if stageCtx.Err() != nil {
					stepErr = stageCtx.Err()
					break
				}

				stepAttemptCtx := stageCtx
				var stepCancel context.CancelFunc
				if step.Timeout > 0 {
					stepAttemptCtx, stepCancel = context.WithTimeout(stageCtx, step.Timeout)
				}

				pCtx := NewPipelineContext(runID, pipelineID, stage.ID, sID, stepPath, logger, WithResourceResolver(resolver))
				mutator, stepErr = executeStepAttempt(stepAttemptCtx, pCtx, step, st.Document())
				if stepCancel != nil {
					stepCancel()
				}

				if stepErr == nil {
					break
				}

				if attempt < retries {
					logger.Warn(fmt.Sprintf("Step %s failed (attempt %d/%d), retrying: %v", sID, attempt+1, retries+1, stepErr))
					bus.Emit(stageCtx, "step:retry", events.PipelineEvent{
						RunID:      runID,
						PipelineID: pipelineID,
						Path:       stepPath,
						Payload: map[string]any{
							"stepId":  sID,
							"attempt": attempt + 1,
							"error":   stepErr.Error(),
						},
					})
				}
			}

			duration := time.Since(stepStart).Milliseconds()
			if stepErr != nil {
				bus.Emit(stageCtx, "step:failure", events.PipelineEvent{
					RunID:      runID,
					PipelineID: pipelineID,
					Path:       stepPath,
					Duration:   duration,
					Payload: map[string]any{
						"stepId":     sID,
						"stepLabel":  step.Label,
						"durationMs": duration,
						"error":      core.SystemErrorJSON(stepErr),
					},
				})
				errsMu.Lock()
				stepErrs = append(stepErrs, stepErr)
				errsMu.Unlock()
				return
			}

			bus.Emit(stageCtx, "step:success", events.PipelineEvent{
				RunID:      runID,
				PipelineID: pipelineID,
				Path:       stepPath,
				Duration:   duration,
				Payload: map[string]any{
					"stepId":     sID,
					"stepLabel":  step.Label,
					"durationMs": duration,
				},
			})

			if mutator != nil {
				mu.Lock()
				mutators = append(mutators, mutator)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	// Mirror the TS aggregate stage failure: "N step(s) failed in <label>:<id>: causes"
	if len(stepErrs) > 0 {
		msgs := make([]string, 0, len(stepErrs))
		for _, e := range stepErrs {
			msgs = append(msgs, core.CauseMessage(e))
		}
		suffix := ""
		if len(msgs) > 1 {
			suffix = "s"
		}
		return core.NewSystemError(
			core.ErrCodeExecutionFailed,
			fmt.Sprintf("%d step%s failed in %s:%s: %s", len(stepErrs), suffix, stage.Label, stage.ID, strings.Join(msgs, ", ")),
		)
	}

	// Atomic commit of all step mutators to store
	if len(mutators) > 0 {
		commitErr := st.Transact(ctx, func(txDoc *document.Document) error {
			for _, m := range mutators {
				if err := m(txDoc); err != nil {
					return err
				}
			}
			return nil
		})
		if commitErr != nil {
			return core.NewSystemError(core.ErrCodeExecutionFailed, "failed to commit stage mutators").WithCause(commitErr)
		}
	}

	return nil
}

func executeStepAttempt(ctx context.Context, pCtx PipelineContext, step Step, doc *document.Document) (store.DocumentMutator, error) {
	if step.Action == nil {
		return nil, nil
	}
	return step.Action(ctx, pCtx, doc)
}
