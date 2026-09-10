package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/effect"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/store"
)

// @note #review-20260910-010 todo status=open priority=P2 tags=#review,#style : gofmt -s was not clean — 12 files failed gofmt -s -l at review time
// @author hermes-review
//
// gofmt -s -l . (go1.27rc1) listed 12 files before this review's mechanical
// pass: pkg/actionlog/actionlog.go, pkg/effect/effect.go,
// pkg/nodekit/typed_test.go, pkg/nodes/pause/pause_test.go,
// pkg/pipeline/checkpoint.go, pkg/pipeline/stage.go,
// pkg/replay/replayer_test.go, pkg/runtime/requirements_test.go,
// pkg/runtime/runtime.go, pkg/runtime/runtime_test.go,
// tests/actionlog_test.go, tests/fork_while_workflow_test.go. This file's
// step-failure branch (the actLog block inside `if stepErr != nil`) was
// visibly misindented, and review edits had drifted files to space
// indentation. The tree has since been normalized with `gofmt -s -w .`;
// this note now tracks the missing PREVENTION: add a gofmt/goimports (or
// gofumpt) gate to CI so the drift cannot recur — see #review-20260910-021.
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

	// @note #review-20260910-008 issue status=resolved priority=P1 tags=#review,#robustness,#panic : Step goroutines have no panic recovery — a panicking action kills the host process
	// @author hermes-review
	//
	// Resolved: panics are recovered at three layers, each converting into
	// the existing failure path instead of unwinding into the runtime:
	//
	// 1. executeStepAttempt recovers action panics (Go node runners,
	//    panics inside resource handles, third-party NodeRunners — goja
	//    already recovers JS internally) and returns them as
	//    ErrCodeExecutionFailed step errors with the stack logged via the
	//    step's logger, so the existing retry loop, step:failure event,
	//    action log entries, and stage failure aggregation apply unchanged.
	// 2. The step goroutine itself carries a safety-net recover for panics
	//    outside the action (bus event handlers, delta computation): it
	//    logs the stack and appends to stepErrs so the stage still fails
	//    cleanly without killing the host process or the concurrent
	//    siblings mid-stage.
	// 3. ExecuteSubPipelines' child goroutines recover panics from child
	//    pipeline execution into a failed PipelineRunResult, which the
	//    bounded stage's PipelinesRouter (try-catch) can catch like any
	//    child failure.
	var wg sync.WaitGroup
	for stepID, stepDef := range stage.Steps {
		step := stepDef
		sID := stepID

		wg.Add(1)
		go func() {
			defer wg.Done()
			// Safety net: a panic anywhere in this goroutine outside the
			// action (which has its own recovery in executeStepAttempt)
			// must fail the step, not the process.
			defer func() {
				if r := recover(); r != nil {
					logger.Error("step goroutine panicked", "stepId", sID, "stageId", stage.ID, "runId", runID, "panic", r, "stack", string(debug.Stack()))
					errsMu.Lock()
					stepErrs = append(stepErrs, core.NewSystemError(core.ErrCodeExecutionFailed,
						fmt.Sprintf("step %s panicked: %v", sID, r)))
					errsMu.Unlock()
				}
			}()
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
				// @note #review-20260910-009 issue status=open priority=P2 tags=#review,#concurrency,#api : Actions execute while the store read lock is held — Store.Update from an action self-deadlocks
				// @author hermes-review
				//
				// step.Action runs inside st.Read's callback, i.e. under MemoryStore's
				// RLock (Read hands out the LIVE state map, not a copy). Two hazards:
				//
				// 1. NodeRunContext.Store is exposed to runners; any custom node that
				//    calls nCtx.Store.Update (or pcxt.Write) while executing
				//    deadlocks itself 100% of the time — RWMutex cannot upgrade a read
				//    lock to a write lock in the same goroutine. Today's built-in nodes
				//    only Update from ROUTERS (called outside st.Read), so the engine
				//    passes its own test suite, but the API invites extension authors
				//    into a guaranteed deadlock with no documentation of the constraint.
				// 2. Arbitrary user code (JS sandbox, HTTP retries) runs while the lock
				//    is held, serializing all concurrent steps in the stage for the
				//    action's whole duration and stretching the lock hold time.
				//
				// Fix direction: snapshot a deep copy for the action (stateSnapshot
				// already exists), or document + enforce that actions must not touch
				// the store, and hide Store from NodeRunContext during Run.
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

// executeStepAttempt runs one attempt of a step's action. A panic inside the
// action (node runner bug, nil-map write, panicking resource handle or
// third-party NodeRunner — see #review-20260910-008) is recovered and
// converted into the normal step error path, so it participates in retries
// and the standard failure reporting instead of terminating the host process.
func executeStepAttempt(ctx context.Context, pCtx PipelineContext, step Step, state map[string]any) (mutator store.Mutator, err error) {
	if step.Action == nil {
		return nil, nil
	}
	defer func() {
		if r := recover(); r != nil {
			err = core.NewSystemError(core.ErrCodeExecutionFailed,
				fmt.Sprintf("step %s panicked: %v", step.ID, r))
			if lg := pCtx.Logger(); lg != nil {
				lg.Error("step action panicked", "stepId", step.ID, "panic", r, "stack", string(debug.Stack()))
			}
		}
	}()
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
