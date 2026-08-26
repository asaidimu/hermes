package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/store"
)

// RunContextImpl is the state machine execution engine for a pipeline run.
type RunContextImpl struct {
	mu           sync.RWMutex
	runID        string
	definition   PipelineDefinition
	store        store.Store
	eventBus     events.ScopedEventBus
	logger       core.Logger
	entryAddress *EntryAddress

	// runEnv holds the host's environment layers exposed to steps via the
	// PipelineContext. Non-secret configuration only.
	runEnv map[string]any
	// secretLookup resolves credentials at execution time; values are for
	// immediate use and never persist to state.
	secretLookup func(key string) (any, bool)

	abortChan chan struct{}
	abortErr  error
	aborted   bool
	paused    bool

	// resourceResolver resolves run-scoped resource artifact keys ("resource:<id>")
	// into initialized handles. The runtime (pkg/runtime) injects it per run so
	// steps and child pipelines can resolve resource dependencies.
	resourceResolver func(key string) (any, bool)
}

func NewRunContext(
	runID string,
	def PipelineDefinition,
	st store.Store,
	bus events.ScopedEventBus,
	logger core.Logger,
	entry ...EntryAddress,
) *RunContextImpl {
	var entryAddr *EntryAddress
	if len(entry) > 0 && entry[0].Stage != "" {
		entryAddr = &entry[0]
	}
	return &RunContextImpl{
		runID:        runID,
		definition:   def,
		store:        st,
		eventBus:     bus,
		logger:       logger,
		entryAddress: entryAddr,
		abortChan:    make(chan struct{}),
	}
}

func (r *RunContextImpl) ID() string                      { return r.runID }
func (r *RunContextImpl) PipelineID() string              { return r.definition.ID }
func (r *RunContextImpl) Store() store.Store              { return r.store }
func (r *RunContextImpl) EventBus() events.ScopedEventBus { return r.eventBus }

// SetResourceResolver attaches a run-scoped resource resolver to this context.
// It is propagated to child pipelines created during subpipeline execution.
func (r *RunContextImpl) SetResourceResolver(resolver func(key string) (any, bool)) {
	r.resourceResolver = resolver
}

func (r *RunContextImpl) ResolveResource(key string) (any, bool) {
	if r.resourceResolver != nil {
		return r.resourceResolver(key)
	}
	return nil, false
}

func (r *RunContextImpl) Write(mutator store.Mutator) {
	// @note #review-20260822-033 issue status=open priority=P1 tags=#review,#error-handling : Write discards store update error
	//
	// The error from r.store.Update is discarded with `_ =`. If the store update fails
	// (e.g., validation error, key collision), the pipeline continues with stale state.
	// This defeats Go's error propagation idiom. At minimum, log the error; ideally,
	// propagate it or return it to the caller.
	_ = r.store.Update(context.Background(), mutator)
}

func (r *RunContextImpl) On(eventType string, handler events.EventHandler) func() {
	return r.eventBus.Subscribe(eventType, handler)
}

func (r *RunContextImpl) Abort(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.aborted {
		r.aborted = true
		if err == nil {
			err = core.NewSystemError(core.ErrCodeAbort, "pipeline execution aborted")
		}
		r.abortErr = err
		close(r.abortChan)
	}
}

// Run executes the pipeline state machine.
func (r *RunContextImpl) Run(ctx context.Context) (PipelineRunResult, error) {
	if r.eventBus == nil {
		r.eventBus = events.NewMemoryScopedBus()
	}
	if r.logger == nil {
		r.logger = core.NopLogger{}
	}
	// Derive a cancellable context from the abort channel so in-flight steps
	// (e.g. delay) observe aborts via ctx.Done() instead of only between stages.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	// @note #review-20260822-055 issue status=open priority=P1 tags=#review,#performance,#memory-leak : Goroutine leak on normal path
	//
	// A goroutine is spawned per Run call to bridge abortChan -> cancel(). On the normal
	// (non-aborted) path, this goroutine blocks on select until runCtx.Done() fires. The
	// defer cancel() ensures the context is cancelled, so the goroutine will eventually
	// drain — but there is a brief window where the goroutine outlives the Run return. If
	// Run is called in a tight loop, these goroutines accumulate transiently. Consider
	// using context.AfterFunc (Go 1.21+) instead of a dedicated goroutine.
	go func() {
		select {
		case <-r.abortChan:
			cancel()
		case <-runCtx.Done():
		}
	}()

	pipePath := events.EventPath{
		{Kind: "pipeline", ID: r.definition.ID, Label: r.definition.Label},
	}

	r.eventBus.Emit(ctx, "pipeline:start", events.PipelineEvent{
		RunID:      r.runID,
		PipelineID: r.definition.ID,
		Path:       pipePath,
		Payload: map[string]any{
			"pipelineId": r.definition.ID,
			"label":      r.definition.Label,
		},
	})

	startTime := time.Now()

	// Locate start stage
	currentIdx := 0
	var currentSubAddr *SubPipelineAddress
	var currentStepID string

	if r.entryAddress != nil && r.entryAddress.Stage != "" {
		idx := FindStageIndex(r.definition.Stages, r.entryAddress.Stage)
		if idx >= 0 {
			currentIdx = idx
			currentStepID = r.entryAddress.Step
			currentSubAddr = r.entryAddress.Pipeline
		}
	}

	for currentIdx < len(r.definition.Stages) {
		r.mu.RLock()
		aborted := r.aborted
		abortErr := r.abortErr
		r.mu.RUnlock()

		if aborted {
			return r.abortedResult(ctx, pipePath, startTime, abortErr)
		}

		if runCtx.Err() != nil {
			return PipelineRunResult{
				Status:     "failed",
				RunID:      r.runID,
				PipelineID: r.definition.ID,
				FinalState: stateSnapshot(r.store),
				Error:      runCtx.Err(),
			}, runCtx.Err()
		}

		stage := r.definition.Stages[currentIdx]
		stageStart := time.Now()
		stagePath := pipePath.Append("stage", stage.ID, stage.Label)

		// A stage runs either steps or sub-pipelines (pipelines win, mirroring TS).
		mode := "steps"
		if len(stage.Pipelines) > 0 || stage.DynamicPipelines != nil {
			mode = "pipelines"
		}

		// stage:start
		r.eventBus.Emit(ctx, "stage:start", events.PipelineEvent{
			RunID:      r.runID,
			PipelineID: r.definition.ID,
			Path:       stagePath,
			Payload: map[string]any{
				"stageId":          stage.ID,
				"stageLabel":       stage.Label,
				"mode":             mode,
				"stepCount":        len(stage.Steps),
				"subPipelineCount": len(stage.Pipelines),
			},
		})

		var instruction RoutingInstruction
		var stageErr error

		if mode == "steps" {
			// 1. Run stage steps if defined
			if len(stage.Steps) > 0 {
				err := ExecuteStageSteps(runCtx, r.runID, r.definition.ID, stage, pipePath, r.store, r.eventBus, r.logger, currentStepID, r.resourceResolver, r.runEnv, r.secretLookup)
				currentStepID = "" // reset step resume targeting after first stage
				if err != nil {
					return r.failStage(ctx, pipePath, stage, stageStart, startTime, err)
				}
			}

			// 2. Evaluate step stage router
			router := stage.Router
			if router == nil {
				router = DefaultStepRouter
			}
			instruction, stageErr = router(runCtx, stateSnapshot(r.store), r.store)
		} else {
			// 3. Pipelines-mode stage: fork children, join, route.
			// Resolve pipelines: use DynamicPipelines if set, else static Pipelines.
			resolvedPipelines := stage.Pipelines
			if stage.DynamicPipelines != nil {
				resolvedPipelines = stage.DynamicPipelines(stateSnapshot(r.store))
				// Patch the stage so ExecuteSubPipelines sees the resolved list.
				stage.Pipelines = resolvedPipelines
			}

			subPipelineIDs := make([]string, 0, len(resolvedPipelines))
			for _, sp := range resolvedPipelines {
				subPipelineIDs = append(subPipelineIDs, sp.ID)
			}
			r.eventBus.Emit(ctx, "subpipeline:fork", events.PipelineEvent{
				RunID:      r.runID,
				PipelineID: r.definition.ID,
				Path:       stagePath,
				Payload: map[string]any{
					"stageId":        stage.ID,
					"stageLabel":     stage.Label,
					"subPipelineIds": subPipelineIDs,
				},
			})

			subResults, subErr := ExecuteSubPipelines(
				runCtx, r.runID, r.definition.ID, stage, pipePath, r.store, r.eventBus, r.logger, currentSubAddr, r.resourceResolver, r.runEnv, r.secretLookup,
			)
			currentSubAddr = nil // reset subpipeline address after first run

			// subpipeline:join
			joinResults := map[string]any{}
			for i, sp := range resolvedPipelines {
				if i >= len(subResults) {
					continue
				}
				entry := map[string]any{
					"pipelineId": sp.ID,
					"status":     subResults[i].Status,
					"ok":         subResults[i].Status == "succeeded",
				}
				if subResults[i].Error != nil {
					entry["error"] = core.SystemErrorJSON(subResults[i].Error)
				}
				joinResults[sp.ID] = entry
			}
			r.eventBus.Emit(ctx, "subpipeline:join", events.PipelineEvent{
				RunID:      r.runID,
				PipelineID: r.definition.ID,
				Path:       stagePath,
				Duration:   time.Since(stageStart).Milliseconds(),
				Payload: map[string]any{
					"stageId":    stage.ID,
					"stageLabel": stage.Label,
					"results":    joinResults,
				},
			})

			if subErr != nil {
				r.mu.RLock()
				abortedNow := r.aborted
				abortErrNow := r.abortErr
				r.mu.RUnlock()
				if abortedNow {
					return r.abortedResult(ctx, pipePath, startTime, abortErrNow)
				}
				return r.failStage(ctx, pipePath, stage, stageStart, startTime, subErr)
			}

			// Merge child results into parent under resultKey if configured,
			// otherwise merge mutated application state fields into the parent store (e.g. fork branches).
			if stage.Config != nil && stage.Config["resultKey"] != nil {
				if resultKey, ok := stage.Config["resultKey"].(string); ok && resultKey != "" {
					for i, sRes := range subResults {
						if i >= len(resolvedPipelines) {
							continue
						}
						pipelineID := resolvedPipelines[i].ID
						var mergeVal any
						if sRes.Status == "succeeded" && sRes.FinalState != nil {
							// Merge child's final state
							mergeVal = sRes.FinalState
						} else {
							// Child failed — merge error details
							errDetail := map[string]any{"pipelineId": pipelineID, "status": sRes.Status}
							if sRes.Error != nil {
								errDetail["error"] = core.SystemErrorJSON(sRes.Error)
							}
							mergeVal = errDetail
						}

						if mergeVal != nil {
							key := resultKey
							if len(resolvedPipelines) > 1 {
								key = resultKey + ":" + pipelineID
							}
							// @note #review-20260825-001 issue status=open priority=P1 tags=#review,#error-handling : Discarded store update error in result merge
							//
							// The error from r.store.Update is discarded with `_ =`. If the store
							// update fails (e.g., document corruption, write conflict), the parent
							// silently loses the child's result with no indication. At minimum,
							// log the error; ideally, return it or fail the stage.
							_ = r.store.Update(ctx, store.SetValue(key, mergeVal))
						}
					}
				}
			} else {
				// Default merge: merge application keys from successful sub-pipelines (e.g. fork branches)
				for _, sRes := range subResults {
					if sRes.Status == "succeeded" && sRes.FinalState != nil {
						_ = r.store.Update(ctx, func(state map[string]any) error {
							for k, v := range sRes.FinalState {
								if !strings.HasPrefix(k, "__") {
									state[k] = v
								}
							}
							return nil
						})
					}
				}
			}

			// Check if any subpipeline paused
			for subIdx, sRes := range subResults {
				if sRes.Status == "paused" && sRes.Checkpoint != nil {
					// Bubble up nested checkpoint
					snap, _ := r.store.ExportJSON()
					nestedCkpt := PipelineCheckpoint{
						RunID:              r.runID,
						PipelineID:         r.definition.ID,
						PausedAtStageID:    stage.ID,
						PausedAtStageLabel: stage.Label,
						ResumeAt: EntryAddress{
							Stage: stage.ID,
							Pipeline: &SubPipelineAddress{
								Index: subIdx,
								Stage: sRes.Checkpoint.ResumeAt.Stage,
								Step:  sRes.Checkpoint.ResumeAt.Step,
							},
						},
						Snapshot: snap,
					}
					_ = r.store.Update(ctx, func(state map[string]any) error {
						return WriteCheckpoint(state, nestedCkpt)
					})
					_ = r.store.Flush(ctx)
					return PipelineRunResult{
						Status:     "paused",
						RunID:      r.runID,
						PipelineID: r.definition.ID,
						FinalState: stateSnapshot(r.store),
						Checkpoint: &nestedCkpt,
					}, nil
				}
			}

			pipeRouter := stage.PipelinesRouter
			if pipeRouter == nil {
				pipeRouter = DefaultPipelineStageRouter
			}
			instruction, stageErr = pipeRouter(runCtx, stateSnapshot(r.store), subResults, r.store)
		}

		if stageErr != nil {
			return r.failStage(ctx, pipePath, stage, stageStart, startTime, stageErr)
		}

		// Pause routing stops the stage; no stage:success / router:evaluated.
		if pauseInst, ok := instruction.(PauseInstruction); ok {
			return r.handlePause(ctx, stage, pauseInst, currentIdx)
		}

		// stage:success carries the nextInstruction the client routes on.
		stageDuration := time.Since(stageStart).Milliseconds()
		r.eventBus.Emit(ctx, "stage:success", events.PipelineEvent{
			RunID:      r.runID,
			PipelineID: r.definition.ID,
			Path:       stagePath,
			Duration:   stageDuration,
			Payload: map[string]any{
				"stageId":         stage.ID,
				"stageLabel":      stage.Label,
				"durationMs":      stageDuration,
				"nextInstruction": serializeInstruction(instruction),
			},
		})

		// router:evaluated — the client renders the routing decision.
		hasNext := currentIdx+1 < len(r.definition.Stages)
		r.eventBus.Emit(ctx, "router:evaluated", events.PipelineEvent{
			RunID:      r.runID,
			PipelineID: r.definition.ID,
			Path:       stagePath,
			Duration:   stageDuration,
			Payload: map[string]any{
				"stageId":        stage.ID,
				"stageLabel":     stage.Label,
				"instruction":    serializeInstruction(instruction),
				"interpretation": interpretationOf(instruction, hasNext),
			},
		})

		// Apply the routing instruction
		decision, shouldPause, nextIdx, evalErr := r.evaluateInstruction(ctx, instruction, currentIdx, stage)
		if evalErr != nil {
			return r.failStage(ctx, pipePath, stage, stageStart, startTime, evalErr)
		}
		if shouldPause {
			// @note #review-20260822-034 issue status=resolved priority=P1 tags=#review,#bug : Unsafe type assertion without comma-ok
			//
			// Fixed by using comma-ok type assertion to prevent runtime panics
			// when instruction is not a PauseInstruction.
			pi, ok := instruction.(PauseInstruction)
			if !ok {
				return r.failStage(ctx, pipePath, stage, stageStart, startTime,
					fmt.Errorf("expected PauseInstruction, got %T", instruction))
			}
			return r.handlePause(ctx, stage, pi, currentIdx)
		}
		if decision == "terminate" {
			break
		}
		if decision == "jump" {
			currentIdx = nextIdx
			continue
		}

		currentIdx++
	}

	duration := time.Since(startTime).Milliseconds()
	finalState, _ := r.store.ExportJSON()
	r.eventBus.Emit(ctx, "pipeline:success", events.PipelineEvent{
		RunID:      r.runID,
		PipelineID: r.definition.ID,
		Path:       pipePath,
		Duration:   duration,
		Payload: map[string]any{
			"pipelineId":    r.definition.ID,
			"pipelineLabel": r.definition.Label,
			"status":        "succeeded",
			"finalState":    finalState,
		},
	})

	return PipelineRunResult{
		Status:     "succeeded",
		RunID:      r.runID,
		PipelineID: r.definition.ID,
		FinalState: stateSnapshot(r.store),
	}, nil

}

// failStage emits stage:failure + pipeline:failure and returns the failed run result.
func (r *RunContextImpl) failStage(ctx context.Context, pipePath events.EventPath, stage Stage, stageStart, runStart time.Time, stageErr error) (PipelineRunResult, error) {
	duration := time.Since(stageStart).Milliseconds()

	// Check if the run was aborted — if so, return the aborted result instead
	// of treating the stage failure as a generic failure.
	r.mu.RLock()
	aborted := r.aborted
	abortErr := r.abortErr
	r.mu.RUnlock()
	if aborted {
		return r.abortedResult(ctx, pipePath, runStart, abortErr)
	}

	r.eventBus.Emit(ctx, "stage:failure", events.PipelineEvent{
		RunID:      r.runID,
		PipelineID: r.definition.ID,
		Path:       pipePath.Append("stage", stage.ID, stage.Label),
		Duration:   duration,
		Payload: map[string]any{
			"stageId":    stage.ID,
			"stageLabel": stage.Label,
			"durationMs": duration,
			"error":      core.SystemErrorJSON(stageErr),
		},
	})

	r.eventBus.Emit(ctx, "pipeline:failure", events.PipelineEvent{
		RunID:      r.runID,
		PipelineID: r.definition.ID,
		Path:       pipePath,
		Duration:   time.Since(runStart).Milliseconds(),
		Payload: map[string]any{
			"pipelineId":    r.definition.ID,
			"pipelineLabel": r.definition.Label,
			"stageId":       stage.ID,
			"error":         core.SystemErrorJSON(stageErr),
		},
	})

	return PipelineRunResult{
		Status:     "failed",
		RunID:      r.runID,
		PipelineID: r.definition.ID,
		FinalState: stateSnapshot(r.store),
		Error:      stageErr,
	}, stageErr
}

// abortedResult emits pipeline:failure (aborted) and returns the aborted result.
func (r *RunContextImpl) abortedResult(ctx context.Context, pipePath events.EventPath, runStart time.Time, abortErr error) (PipelineRunResult, error) {
	r.eventBus.Emit(ctx, "pipeline:failure", events.PipelineEvent{
		RunID:      r.runID,
		PipelineID: r.definition.ID,
		Path:       pipePath,
		Duration:   time.Since(runStart).Milliseconds(),
		Payload: map[string]any{
			"pipelineId":    r.definition.ID,
			"pipelineLabel": r.definition.Label,
			"error":         core.SystemErrorJSON(abortErr),
		},
	})
	return PipelineRunResult{
		Status:     "aborted",
		RunID:      r.runID,
		PipelineID: r.definition.ID,
		FinalState: stateSnapshot(r.store),
		Error:      abortErr,
	}, abortErr
}

// serializeInstruction renders a RoutingInstruction into the wire shape used by
// the client: a stage id string (jump), an entry-address object (jump-to), or
// null for advance/terminate/pause.
func serializeInstruction(inst RoutingInstruction) any {
	switch v := inst.(type) {
	case JumpInstruction:
		return v.StageID
	case JumpToInstruction:
		addr := v.Address
		m := map[string]any{"stage": addr.Stage}
		if addr.Step != "" {
			m["step"] = addr.Step
		}
		if addr.Pipeline != nil {
			m["pipeline"] = map[string]any{
				"index": addr.Pipeline.Index,
				"stage": addr.Pipeline.Stage,
				"step":  addr.Pipeline.Step,
			}
		}
		return m
	case PauseInstruction:
		return nil
	default:
		return nil
	}
}

// interpretationOf maps a RoutingInstruction to the TS router:evaluated
// interpretation: pause | terminate | jump | advance | natural-end.
func interpretationOf(inst RoutingInstruction, hasNext bool) string {
	switch inst.(type) {
	case PauseInstruction:
		return "pause"
	case TerminateInstruction:
		return "terminate"
	case JumpInstruction, JumpToInstruction:
		return "jump"
	default:
		if hasNext {
			return "advance"
		}
		return "natural-end"
	}
}

func (r *RunContextImpl) evaluateInstruction(
	ctx context.Context,
	inst RoutingInstruction,
	currentIdx int,
	stage Stage,
) (decision string, pause bool, nextIdx int, err error) {
	if inst == nil {
		return "advance", false, currentIdx + 1, nil
	}

	switch v := inst.(type) {
	case AdvanceInstruction:
		return "advance", false, currentIdx + 1, nil
	case TerminateInstruction:
		return "terminate", false, len(r.definition.Stages), nil
	case JumpInstruction:
		targetIdx := FindStageIndex(r.definition.Stages, v.StageID)
		if targetIdx < 0 {
			return "", false, 0, core.NewSystemError(core.ErrCodeNotFound, fmt.Sprintf("target stage %s not found", v.StageID))
		}
		return "jump", false, targetIdx, nil
	case JumpToInstruction:
		targetIdx := FindStageIndex(r.definition.Stages, v.Address.Stage)
		if targetIdx < 0 {
			return "", false, 0, core.NewSystemError(core.ErrCodeNotFound, fmt.Sprintf("target stage %s not found", v.Address.Stage))
		}
		return "jump", false, targetIdx, nil
	case PauseInstruction:
		return "pause", true, currentIdx, nil
	default:
		return "advance", false, currentIdx + 1, nil
	}
}

func (r *RunContextImpl) handlePause(ctx context.Context, stage Stage, pauseInst PauseInstruction, currentIdx int) (PipelineRunResult, error) {
	r.mu.Lock()
	r.paused = true
	r.mu.Unlock()

	// Determine the resume stage: use the explicit StageID if provided,
	// otherwise advance to the next stage after the current one.
	resumeStageID := pauseInst.StageID
	if resumeStageID == "" {
		nextIdx := currentIdx + 1
		if nextIdx < len(r.definition.Stages) {
			resumeStageID = r.definition.Stages[nextIdx].ID
		} else {
			resumeStageID = stage.ID // last stage — will terminate on resume
		}
	}

	ckpt := PipelineCheckpoint{
		RunID:              r.runID,
		PipelineID:         r.definition.ID,
		PausedAtStageID:    stage.ID,
		PausedAtStageLabel: stage.Label,
		ResumeAt: EntryAddress{
			Stage: resumeStageID,
		},
		WaitForEvent:  pauseInst.WaitForEvent,
		WaitForEvents: pauseInst.WaitForEvents,
		WaitMode:      pauseInst.WaitMode,
		Timeout:       pauseInst.Timeout.Milliseconds(),
		Cron:          pauseInst.Cron,
	}

	if pauseInst.Persist {
		snap, _ := r.store.ExportJSON()
		ckpt.Snapshot = snap
		_ = r.store.Update(ctx, func(state map[string]any) error {
			return WriteCheckpoint(state, ckpt)
		})
		_ = r.store.Flush(ctx)
	}

	r.eventBus.Emit(ctx, "pipeline:pause", events.PipelineEvent{
		RunID:      r.runID,
		PipelineID: r.definition.ID,
		Path: events.EventPath{
			{Kind: "pipeline", ID: r.definition.ID, Label: r.definition.Label},
			{Kind: "stage", ID: stage.ID, Label: stage.Label},
		},
		Payload: map[string]any{
			"pausedAtStageId": stage.ID,
			"resumeAtStageId": resumeStageID,
			"timeout":         pauseInst.Timeout.Milliseconds(),
			"waitForEvent":    pauseInst.WaitForEvent,
			"waitForEvents":   pauseInst.WaitForEvents,
			"waitMode":        pauseInst.WaitMode,
		},
	})

	return PipelineRunResult{
		Status:        "paused",
		RunID:         r.runID,
		PipelineID:    r.definition.ID,
		FinalState:    stateSnapshot(r.store),
		Checkpoint:    &ckpt,
		WaitForEvent:  pauseInst.WaitForEvent,
		WaitForEvents: pauseInst.WaitForEvents,
		WaitMode:      pauseInst.WaitMode,
	}, nil
}

var _ RunContext = (*RunContextImpl)(nil)

// @note #review-20260826-002 issue status=resolved priority=P1 tags=#review,#concurrency,#bug : stateSnapshot is a shallow copy — nested maps alias live store state
// @author ox-alpha
//
// Only top-level keys were copied; nested values (results, __pipeline_data__,
// user objects) were shared references into the live store map. Routers
// receiving the snapshot could observe concurrent step mutations mid-read
// (map concurrent read/write panic) and, if they ever mutated nested data,
// corrupt live state while bypassing the store lock.
//
// Fixed by delegating to store.DeepCopyMap, which clones maps/slices
// recursively; covered by TestStateSnapshotDeepCopy.
//
// stateSnapshot returns a deep copy of the run state for read-only consumers
// (routers), safe to retain beyond the store lock.
func stateSnapshot(st store.Store) map[string]any {
	var snap map[string]any
	_ = st.Read(func(state map[string]any) error {
		snap = store.DeepCopyMap(state)
		return nil
	})
	if snap == nil {
		snap = make(map[string]any)
	}
	return snap
}
