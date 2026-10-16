package pipeline

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/store"

	"go.uber.org/zap"
)

// RunContextImpl is the state machine execution engine for a pipeline run.
type RunContextImpl struct {
	mu           sync.RWMutex
	runID        string
	definition   PipelineDefinition
	store        store.Store
	eventBus     *events.MemoryScopedBus
	logger       *zap.SugaredLogger
	entryAddress *EntryAddress

	// actionLog records every significant event during execution for
	// event-sourced recovery and observability.
	actionLog actionlog.Store

	// rerunIndex identifies which attempt of this run is executing.
	// Stamped on every log entry so retries get separate audit trails.
	rerunIndex int

	// runEnv holds the host's environment layers exposed to steps via the
	// PipelineContext. Non-secret configuration only.
	runEnv map[string]any
	// secretLookup resolves credentials at execution time; values are for
	// immediate use and never persist to state.
	secretLookup func(key string) (any, bool)

	abortErr error
	aborted  bool
	paused   bool
	// cancelRun is set by Run() to the cancel func of its derived,
	// abort-aware context. Abort() calls it directly instead of relying on
	// a background goroutine bridging an abort channel to cancel() (see the
	// note on review-20260822-055 in Run for why).
	cancelRun context.CancelFunc

	// resourceResolver resolves run-scoped resource artifact keys ("resource:<id>")
	// into initialized handles. The runtime (pkg/runtime) injects it per run so
	// steps and child pipelines can resolve resource dependencies.
	resourceResolver func(key string) (any, bool)

	// wfHash (v2.1 H1.2): the workflow-level structural fingerprint this run
	// executes under, stamped onto identity entries by appendLog and inherited
	// by subpipeline children so their checkpoints carry the same workflow
	// identity. Empty = unstamped.
	wfHash string
}

func NewRunContext(
	runID string,
	def PipelineDefinition,
	st store.Store,
	bus *events.MemoryScopedBus,
	logger *zap.SugaredLogger,
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
	}
}

func (r *RunContextImpl) ID() string                        { return r.runID }
func (r *RunContextImpl) PipelineID() string                { return r.definition.ID }
func (r *RunContextImpl) Store() store.Store                { return r.store }
func (r *RunContextImpl) EventBus() *events.MemoryScopedBus { return r.eventBus }

// ActionLog returns the run's action log store, or nil when logging is
// disabled (R.3 deleted NopLog; callers nil-check).
func (r *RunContextImpl) ActionLog() actionlog.Store {
	return r.actionLog
}

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
	// @note #review-20260822-033 issue P1 resolved status=resolved priority=P1 tags=#review,#error-handling : Write discards store update error
	//
	// Resolved: log the error instead of discarding it. Write's signature
	// (no return value) is part of the PipelineContext interface used
	// pervasively by node implementations as a fire-and-forget call;
	// changing it to return an error would ripple across every node in the
	// repo, which isn't safe to do blind in this environment (no working
	// compiler to catch every call site). Logging closes the "pipeline
	// continues with stale state and nobody finds out" gap without that
	// wider, riskier signature change.
	if err := r.store.Update(context.Background(), mutator); err != nil && r.logger != nil {
		r.logger.Error("pipeline: store update failed", "runId", r.runID, "pipelineId", r.definition.ID, "error", err)
	}
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
		if r.cancelRun != nil {
			r.cancelRun()
		}
	}
}

// Run executes the pipeline state machine.
func (r *RunContextImpl) Run(ctx context.Context) (PipelineRunResult, error) {
	if r.eventBus == nil {
		r.eventBus = events.NewMemoryScopedBus()
	}
	if r.logger == nil {
		r.logger = zap.NewNop().Sugar()
	}
	// Derive a cancellable context from the abort channel so in-flight steps
	// (e.g. delay) observe aborts via ctx.Done() instead of only between stages.
	runCtx, cancel := context.WithCancel(ctx)
	// Stamp run identity and the run-scoped resource resolver onto the
	// execution context. Stage routers and node callbacks receive only a bare
	// context.Context (no PipelineContext), so this is how the pause node's
	// PipelinesRouterFunc learns the run id it registers watches under
	// (#review-20260910-007) and reaches built-in runtime resources.
	runCtx = context.WithValue(runCtx, runIDContextKey{}, r.runID)
	if r.resourceResolver != nil {
		runCtx = context.WithValue(runCtx, resourceResolverContextKey{}, r.resourceResolver)
	}
	defer cancel()
	// @note #review-20260822-055 issue P1 resolved status=resolved priority=P1 tags=#review,#performance,#memory-leak : Goroutine leak on normal path
	//
	// Resolved: removed the dedicated bridging goroutine entirely, rather
	// than swapping it for context.AfterFunc (which still needs something
	// to bridge Abort's signal into a context in the first place — it
	// doesn't take a channel). Abort() now holds a direct reference to this
	// run's cancel func (r.cancelRun, set below under r.mu) and calls it
	// itself under lock; there is no longer a channel or a goroutine
	// watching one; Abort() called before Run() starts is now handled by
	// checking r.aborted immediately after registering cancelRun, instead
	// of relying on a goroutine that hadn't started its select yet.
	r.mu.Lock()
	alreadyAborted := r.aborted
	r.cancelRun = cancel
	r.mu.Unlock()
	if alreadyAborted {
		cancel()
	}

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

	r.appendLog(ctx, actionlog.Entry{
		RunID: r.runID,
		Kind:  actionlog.KindPipelineStarted,
		Type:  "pipeline:start",
		Payload: mustMarshal(map[string]any{
			"pipelineId": r.definition.ID,
			"label":      r.definition.Label,
		}),
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

		r.appendLog(ctx, actionlog.Entry{
			RunID:   r.runID,
			Kind:    actionlog.KindStageStarted,
			Type:    "stage:start",
			StageID: stage.ID,
			Payload: mustMarshal(map[string]any{
				"stageId":    stage.ID,
				"stageLabel": stage.Label,
				"mode":       mode,
			}),
		})

		var instruction RoutingInstruction
		var stageErr error

		if mode == "steps" {
			// 1. Run stage steps if defined
			if len(stage.Steps) > 0 {
				err := ExecuteStageSteps(runCtx, r.runID, r.definition.ID, stage, pipePath, r.store, r.eventBus, r.logger, currentStepID, r.resourceResolver, r.runEnv, r.secretLookup, r.actionLog, r.rerunIndex)
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

			// Record routing decision in the action log (§2.3: always
			// recorded regardless of router purity — cheap, and prevents
			// replay divergence from accidentally impure routers).
			if stageErr == nil {
				r.recordRouting(ctx, stage.ID, instruction)
			}
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
			r.appendLog(ctx, actionlog.Entry{
				RunID:   r.runID,
				Kind:    actionlog.KindSubpipelineForked,
				Type:    "subpipeline:fork",
				StageID: stage.ID,
				Payload: mustMarshal(map[string]any{
					"stageId":        stage.ID,
					"subPipelineIds": subPipelineIDs,
				}),
			})

			subResults, subErr := ExecuteSubPipelines(
				runCtx, r.runID, r.definition.ID, stage, pipePath, r.store, r.eventBus, r.logger, currentSubAddr, r.resourceResolver, r.runEnv, r.secretLookup, r.actionLog, r.rerunIndex, r.wfHash,
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
			r.appendLog(ctx, actionlog.Entry{
				RunID:    r.runID,
				Kind:     actionlog.KindSubpipelineJoined,
				Type:     "subpipeline:join",
				StageID:  stage.ID,
				Payload:  mustMarshal(map[string]any{"stageId": stage.ID}),
				Duration: time.Since(stageStart).Milliseconds(),
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
							// @note #review-20260825-001 issue P1 resolved status=resolved priority=P1 tags=#review,#error-handling : Discarded store update error in result merge
							//
							// Resolved: log the error instead of discarding it.
							// Continuing rather than failing the stage on a
							// single merge-key failure matches the same
							// best-effort-but-now-visible choice made for the
							// sibling notes in this file (review-20260822-033)
							// and pause.go (review-20260822-046).
							if err := r.store.Update(ctx, store.SetValue(key, mergeVal)); err != nil && r.logger != nil {
								r.logger.Error("pipeline: failed to merge sub-pipeline result into state", "runId", r.runID, "pipelineId", pipelineID, "key", key, "error", err)
							}
						}
					}
				}
			} else {
				// Default merge: merge application keys from successful sub-pipelines (e.g. fork branches)
				for _, sRes := range subResults {
					if sRes.Status == "succeeded" && sRes.FinalState != nil {
						// Best-effort merge, now visible (#review-20260910-012
						// sibling pattern): a failed merge means the parent state
						// silently misses a child's application keys.
						if err := r.store.Update(ctx, func(state map[string]any) error {
							for k, v := range sRes.FinalState {
								if !strings.HasPrefix(k, "__") {
									state[k] = v
								}
							}
							return nil
						}); err != nil && r.logger != nil {
							r.logger.Error("pipeline: failed to merge sub-pipeline state into parent", "runId", r.runID, "pipelineId", r.definition.ID, "error", err)
						}
					}
				}
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
					// Best-effort like the sibling sites: the child's own
					// checkpoint (returned below) still records the resume
					// address, but a failed bubble-up write is now VISIBLE
					// (#review-20260910-012 sibling pattern).
					if err := r.store.Update(ctx, func(state map[string]any) error {
						return WriteCheckpoint(state, nestedCkpt)
					}); err != nil && r.logger != nil {
						r.logger.Error("pipeline: failed to persist nested subpipeline checkpoint", "runId", r.runID, "pipelineId", r.definition.ID, "stageId", stage.ID, "error", err)
					}
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

			// Record routing decision in the action log (§2.3).
			if stageErr == nil {
				r.recordRouting(ctx, stage.ID, instruction)
			}
		}

		if stageErr != nil {
			return r.failStage(ctx, pipePath, stage, stageStart, startTime, stageErr)
		}

		// Pause routing stops the stage; no stage:success / router:evaluated.
		if instruction.Kind == RoutePause {
			return r.handlePause(ctx, stage, instruction, currentIdx)
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
				"nextInstruction": instruction.Serialize(),
			},
		})
		r.appendLog(ctx, actionlog.Entry{
			RunID:    r.runID,
			Kind:     actionlog.KindStageCompleted,
			Type:     "stage:success",
			StageID:  stage.ID,
			Payload:  mustMarshal(map[string]any{"stageId": stage.ID, "stageLabel": stage.Label}),
			Duration: stageDuration,
		})
		r.syncStateMilestone(ctx)

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
				"instruction":    instruction.Serialize(),
				"interpretation": instruction.Interpretation(hasNext),
			},
		})

		// Apply the routing instruction
		decision, shouldPause, nextIdx, evalErr := r.evaluateInstruction(ctx, instruction, currentIdx, stage)
		if evalErr != nil {
			return r.failStage(ctx, pipePath, stage, stageStart, startTime, evalErr)
		}
		if shouldPause {
			// @note #review-20260822-034 issue P1 resolved status=resolved priority=P1 tags=#review,#bug : Unsafe type assertion without comma-ok
			//
			// Superseded by R.2 (20261003): the instruction is a tagged
			// struct, so the former comma-ok type assertion is a kind
			// check — the panic class it guarded no longer exists.
			return r.handlePause(ctx, stage, instruction, currentIdx)
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
	// M1 (plan P4.9): export the final state ONCE and share the snapshot
	// between the pipeline:success payload and the run result — the
	// success path used to deep-copy the full state three times (payload
	// export, result snapshot, and the runtime's own re-export downstream).
	finalState := stateSnapshot(r.store)
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
	r.appendLog(ctx, actionlog.Entry{
		RunID:    r.runID,
		Kind:     actionlog.KindPipelineCompleted,
		Type:     "pipeline:success",
		Duration: duration,
	})
	r.syncStateMilestone(ctx)

	return PipelineRunResult{
		Status:     "succeeded",
		RunID:      r.runID,
		PipelineID: r.definition.ID,
		FinalState: finalState,
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
	r.appendLog(ctx, actionlog.Entry{
		RunID:    r.runID,
		Kind:     actionlog.KindStageFailed,
		Type:     "stage:failure",
		StageID:  stage.ID,
		Error:    core.CauseMessage(stageErr),
		Duration: duration,
	})
	r.appendLog(ctx, actionlog.Entry{
		RunID:    r.runID,
		Kind:     actionlog.KindPipelineFailed,
		Type:     "pipeline:failure",
		Error:    core.CauseMessage(stageErr),
		Duration: time.Since(runStart).Milliseconds(),
	})
	r.syncStateMilestone(ctx)

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
	r.appendLog(ctx, actionlog.Entry{
		RunID:    r.runID,
		Kind:     actionlog.KindPipelineFailed,
		Type:     "pipeline:failure",
		Error:    core.CauseMessage(abortErr),
		Duration: time.Since(runStart).Milliseconds(),
	})
	return PipelineRunResult{
		Status:     "aborted",
		RunID:      r.runID,
		PipelineID: r.definition.ID,
		FinalState: stateSnapshot(r.store),
		Error:      abortErr,
	}, abortErr
}

// Context keys for values RunContextImpl.Run stamps onto the execution
// context. Unexported: values are read through the accessors below.
type runIDContextKey struct{}
type resourceResolverContextKey struct{}

// RunIDFromContext returns the run id stamped onto ctx by
// RunContextImpl.Run, or "" when ctx did not originate from a pipeline run
// (replay, standalone factory use). Stage routers and node callbacks use
// this to identify the run they execute under without a signature change.
func RunIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(runIDContextKey{}).(string); ok {
		return v
	}
	return ""
}

// ResourceResolverFromContext returns the run-scoped resource resolver
// stamped onto ctx by RunContextImpl.Run, or nil when absent. Node wiring
// uses it to resolve resource handles in call paths that receive no
// PipelineContext (e.g. bounded nodes' PipelinesRouterFunc).
func ResourceResolverFromContext(ctx context.Context) func(key string) (any, bool) {
	if v, ok := ctx.Value(resourceResolverContextKey{}).(func(string) (any, bool)); ok {
		return v
	}
	return nil
}

// appendLog appends an entry to the run's action log, filling RunID,
// RerunIndex and PipelineID (the pipeline whose execution produced the
// entry — the root def for this run context, or the child def when a
// subpipeline's own RunContextImpl logs; see #review-20260910-015).
// Nil-safe: returns silently when no log is configured.
func (r *RunContextImpl) appendLog(ctx context.Context, entry actionlog.Entry) {
	if r.actionLog == nil {
		return
	}
	entry.RunID = r.runID
	entry.RerunIndex = r.rerunIndex
	entry.PipelineID = r.definition.ID
	// v2.1 H1.2: stamp the workflow fingerprint on the two identity kinds
	// the drift checks read — attempt start and pause. Other kinds stay
	// unstamped (bytes on every entry buy nothing).
	if entry.DefHash == "" && r.wfHash != "" &&
		(entry.Kind == actionlog.KindPipelineStarted || entry.Kind == actionlog.KindCheckpoint) {
		entry.DefHash = r.wfHash
	}
	r.actionLog.Append(ctx, entry)
}

// mustMarshal renders a payload map to JSON for a log entry. Returns nil
// on marshaling failure — log enrichment must never fail execution.
func mustMarshal(payload map[string]any) []byte {
	b, err := json.Marshal(payload)
	if err != nil {
		return nil
	}
	return b
}

// storeSyncer is implemented by stores that can project in-memory state to
// a queryable backend (e.g. *store.PersistentStore.Sync). The engine calls
// it at milestones — stage boundaries, checkpoints, completion — so the
// state document tracks the log without a write per entry.
type storeSyncer interface {
	Sync(context.Context) error
}

// syncStateMilestone projects current state for queryability. Best-effort:
// the action log is the durable source of truth, so a sync failure must
// never fail execution.
func (r *RunContextImpl) syncStateMilestone(ctx context.Context) {
	if s, ok := r.store.(storeSyncer); ok {
		if err := s.Sync(ctx); err != nil && r.logger != nil {
			r.logger.Error("pipeline: milestone state sync failed", "runId", r.runID, "error", err)
		}
	}
}
