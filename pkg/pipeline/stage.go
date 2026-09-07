package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/effect"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/store"
)

// ExecuteStageSteps runs all steps in a stage concurrently and applies their mutators atomically on success.
// resolver (optional) resolves run-scoped resource keys ("resource:<id>") into handles.
// actLog (optional) records every step event for the unified observability log.
// rerunIndex stamps each entry so retries get separate audit trails.
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
	runEnv map[string]any,
	secretLookup func(key string) (any, bool),
	actLog actionlog.Store,
	rerunIndex int,
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
	mutators := make([]store.Mutator, 0, len(stage.Steps))
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

			if actLog != nil {
				payload, _ := json.Marshal(map[string]any{
					"stepId":    sID,
					"stepLabel": step.Label,
				})
				actLog.Append(stageCtx, actionlog.Entry{
					RunID:      runID,
					RerunIndex: rerunIndex,
					Kind:       actionlog.KindStepStarted,
					Type:       "step:start",
					StageID:    stage.ID,
					StepID:     sID,
					Payload:    payload,
				})
			}

			stepStart := time.Now()
			var mutator store.Mutator
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

				pCtx := NewPipelineContext(runID, pipelineID, stage.ID, sID, stepPath, logger,
					WithResourceResolver(resolver), WithRunEnv(runEnv), WithSecretLookup(secretLookup))
				snapshotErr := st.Read(func(state map[string]any) error {
					mutator, stepErr = executeStepAttempt(stepAttemptCtx, pCtx, step, state)
					return nil
				})
				if snapshotErr != nil && stepErr == nil {
					stepErr = snapshotErr
				}
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
					if actLog != nil {
						payload, _ := json.Marshal(map[string]any{
							"stepId":  sID,
							"attempt": attempt + 1,
						})
						actLog.Append(stageCtx, actionlog.Entry{
							RunID:      runID,
							RerunIndex: rerunIndex,
							Kind:       actionlog.KindStepRetry,
							Type:       "step:retry",
							StageID:    stage.ID,
							StepID:     sID,
							Payload:    payload,
							Error:      stepErr.Error(),
							Attempt:    attempt + 1,
						})
					}
				}
			}

			duration := time.Since(stepStart).Milliseconds()
			if stepErr != nil {
			// Record the failure for every step (observability).
			if actLog != nil {
				payload, _ := json.Marshal(map[string]any{
					"stepId":     sID,
					"stepLabel":  step.Label,
					"durationMs": duration,
					"error":      core.SystemErrorJSON(stepErr),
				})
					actLog.Append(stageCtx, actionlog.Entry{
						RunID:      runID,
						RerunIndex: rerunIndex,
						Kind:       actionlog.KindStepFailed,
						Type:       "step:failure",
						StageID:    stage.ID,
						StepID:     sID,
						Payload:    payload,
						Error:      stepErr.Error(),
						Duration:   duration,
					})
				}
				// Record effectful step failure for the replayer (§4.1).
				if step.Effect == int(effect.SideEffecting) && actLog != nil {
					actLog.Append(stageCtx, actionlog.Entry{
						RunID:      runID,
						RerunIndex: rerunIndex,
						Kind:       actionlog.KindEffectFailed,
						StageID:    stage.ID,
						StepID:     sID,
						Error:      stepErr.Error(),
					})
				}

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

			// Record the completion for every step (observability): the
			// state delta (what the mutator changes) plus duration.
			var output json.RawMessage
			if mutator != nil {
				// Compute the state delta by applying the mutator
				// to a temporary copy of the current state.
				delta := computeStateDelta(st, mutator)
				if delta != nil {
					output, _ = json.Marshal(delta)
				}
			}
			if actLog != nil {
				payload, _ := json.Marshal(map[string]any{
					"stepId":     sID,
					"stepLabel":  step.Label,
					"durationMs": duration,
				})
				actLog.Append(stageCtx, actionlog.Entry{
					RunID:      runID,
					RerunIndex: rerunIndex,
					Kind:       actionlog.KindStepCompleted,
					Type:       "step:success",
					StageID:    stage.ID,
					StepID:     sID,
					Payload:    payload,
					Output:     output,
					Duration:   duration,
				})
			}

			// Record effectful step outcome for the replayer (§4.1):
			// same delta, so the Replayer can inject it during replay
			// without re-executing the step's side effects.
			if step.Effect == int(effect.SideEffecting) && actLog != nil {
				actLog.Append(stageCtx, actionlog.Entry{
					RunID:      runID,
					RerunIndex: rerunIndex,
					Kind:       actionlog.KindEffectCompleted,
					StageID:    stage.ID,
					StepID:     sID,
					Output:     output,
				})
			}

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
		commitErr := st.Update(ctx, func(state map[string]any) error {
			for _, m := range mutators {
				if err := m(state); err != nil {
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

func executeStepAttempt(ctx context.Context, pCtx PipelineContext, step Step, state map[string]any) (store.Mutator, error) {
	if step.Action == nil {
		return nil, nil
	}
	return step.Action(ctx, pCtx, state)
}

// computeStateDelta applies a mutator to a deep copy of the current store
// state and returns only the keys that changed. Used to record the state
// delta of an effectful step in the action log so the Replayer can inject
// it during replay without re-executing the step's side effects.
func computeStateDelta(st store.Store, mutator store.Mutator) map[string]any {
	// Snapshot current state.
	var before map[string]any
	_ = st.Read(func(state map[string]any) error {
		before = store.DeepCopyMap(state)
		return nil
	})
	if before == nil {
		before = make(map[string]any)
	}

	// Apply the mutator to a copy.
	after := store.DeepCopyMap(before)
	if err := mutator(after); err != nil {
		return nil
	}

	// Diff: only keys that changed.
	delta := make(map[string]any)
	for k, v := range after {
		if oldV, ok := before[k]; !ok || !deepEqual(oldV, v) {
			delta[k] = v
		}
	}
	// Keys deleted.
	for k := range before {
		if _, ok := after[k]; !ok {
			delta[k] = nil
		}
	}
	if len(delta) == 0 {
		return nil
	}
	return delta
}

// deepEqual is a simple equality check for JSON-compatible values.
func deepEqual(a, b any) bool {
	aj, err := json.Marshal(a)
	if err != nil {
		return false
	}
	bj, err := json.Marshal(b)
	if err != nil {
		return false
	}
	return string(aj) == string(bj)
}
