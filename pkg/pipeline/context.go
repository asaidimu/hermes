package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/asaidimu/go-anansi/v8/core/document"
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
	if bus == nil {
		bus = events.NewMemoryScopedBus()
	}
	if logger == nil {
		logger = core.NopLogger{}
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

func (r *RunContextImpl) ID() string                     { return r.runID }
func (r *RunContextImpl) PipelineID() string             { return r.definition.ID }
func (r *RunContextImpl) Store() store.Store             { return r.store }
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

func (r *RunContextImpl) Write(mutator store.DocumentMutator) {
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
	// Derive a cancellable context from the abort channel so in-flight steps
	// (e.g. delay) observe aborts via ctx.Done() instead of only between stages.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
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
				FinalDoc:   r.store.Document(),
				Error:      runCtx.Err(),
			}, runCtx.Err()
		}

		stage := r.definition.Stages[currentIdx]
		stageStart := time.Now()
		stagePath := pipePath.Append("stage", stage.ID, stage.Label)

		// A stage runs either steps or sub-pipelines (pipelines win, mirroring TS).
		mode := "steps"
		if len(stage.Pipelines) > 0 {
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
				err := ExecuteStageSteps(runCtx, r.runID, r.definition.ID, stage, pipePath, r.store, r.eventBus, r.logger, currentStepID, r.resourceResolver)
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
			instruction, stageErr = router(runCtx, r.store.Document(), r.store)
		} else {
			// 3. Pipelines-mode stage: fork children, join, route.
			subPipelineIDs := make([]string, 0, len(stage.Pipelines))
			for _, sp := range stage.Pipelines {
				subPipelineIDs = append(subPipelineIDs, sp.ID)
			}
			r.eventBus.Emit(ctx, "subpipeline:fork", events.PipelineEvent{
				RunID:      r.runID,
				PipelineID: r.definition.ID,
				Path:       stagePath,
				Payload: map[string]any{
					"stageId":       stage.ID,
					"stageLabel":    stage.Label,
					"subPipelineIds": subPipelineIDs,
				},
			})

			subResults, subErr := ExecuteSubPipelines(
				runCtx, r.runID, r.definition.ID, stage, pipePath, r.store, r.eventBus, r.logger, currentSubAddr, r.resourceResolver,
			)
			currentSubAddr = nil // reset subpipeline address after first run

			// subpipeline:join
			joinResults := map[string]any{}
			for i, sp := range stage.Pipelines {
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

			// Check if any subpipeline paused
			for subIdx, sRes := range subResults {
				if sRes.Status == "paused" && sRes.Checkpoint != nil {
					// Bubble up nested checkpoint
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
					}
					_ = WriteCheckpoint(r.store.Document(), nestedCkpt)
					return PipelineRunResult{
						Status:     "paused",
						RunID:      r.runID,
						PipelineID: r.definition.ID,
						FinalDoc:   r.store.Document(),
						Checkpoint: &nestedCkpt,
					}, nil
				}
			}

			pipeRouter := stage.PipelinesRouter
			if pipeRouter == nil {
				pipeRouter = DefaultPipelineStageRouter
			}
			instruction, stageErr = pipeRouter(runCtx, r.store.Document(), subResults, r.store)
		}

		if stageErr != nil {
			return r.failStage(ctx, pipePath, stage, stageStart, startTime, stageErr)
		}

		// Pause routing stops the stage; no stage:success / router:evaluated.
		if pauseInst, ok := instruction.(PauseInstruction); ok {
			return r.handlePause(ctx, stage, pauseInst)
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
			return r.handlePause(ctx, stage, instruction.(PauseInstruction))
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
		FinalDoc:   r.store.Document(),
	}, nil

}

// failStage emits stage:failure + pipeline:failure and returns the failed run result.
func (r *RunContextImpl) failStage(ctx context.Context, pipePath events.EventPath, stage Stage, stageStart, runStart time.Time, stageErr error) (PipelineRunResult, error) {
	duration := time.Since(stageStart).Milliseconds()
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
		FinalDoc:   r.store.Document(),
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
		FinalDoc:   r.store.Document(),
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

func (r *RunContextImpl) handlePause(ctx context.Context, stage Stage, pauseInst PauseInstruction) (PipelineRunResult, error) {
	r.mu.Lock()
	r.paused = true
	r.mu.Unlock()

	nextStageID := stage.ID
	if pauseInst.StageID != "" {
		nextStageID = pauseInst.StageID
	}

	ckpt := PipelineCheckpoint{
		RunID:              r.runID,
		PipelineID:         r.definition.ID,
		PausedAtStageID:    stage.ID,
		PausedAtStageLabel: stage.Label,
		ResumeAt: EntryAddress{
			Stage: nextStageID,
		},
	}

	if pauseInst.Persist {
		_ = WriteCheckpoint(r.store.Document(), ckpt)
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
			"resumeAtStageId": nextStageID,
			"timeout":         pauseInst.Timeout.Milliseconds(),
		},
	})

	return PipelineRunResult{
		Status:     "paused",
		RunID:      r.runID,
		PipelineID: r.definition.ID,
		FinalDoc:   r.store.Document(),
		Checkpoint: &ckpt,
	}, nil
}

var _ RunContext = (*RunContextImpl)(nil)
var _ = document.NewRecordView
