// Routing interpretation: serialization of routing decisions, the
// instruction evaluator, pause handling, and the log/route helpers that
// feed them. Split out of context.go by R.1 (pure move); R.2 replaces the
// instruction forms with one canonical representation.
package pipeline

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/store"
)

// Serialize renders a RoutingInstruction into the wire shape used by the
// client: a stage id string (jump), an entry-address object (jump-to), or
// null for advance/terminate/pause. (R.2: was serializeInstruction.)
func (i RoutingInstruction) Serialize() any {
	switch i.Kind {
	case RouteJump:
		return i.StageID
	case RouteJumpTo:
		addr := i.Address
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
	default:
		return nil
	}
}

// Interpretation maps a RoutingInstruction to the TS router:evaluated
// interpretation: pause | terminate | jump | advance | natural-end.
// (R.2: was interpretationOf.)
func (i RoutingInstruction) Interpretation(hasNext bool) string {
	switch i.Kind {
	case RoutePause:
		return "pause"
	case RouteTerminate:
		return "terminate"
	case RouteJump, RouteJumpTo:
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
	switch inst.Kind {
	case RouteAdvance, RouteNone:
		return "advance", false, currentIdx + 1, nil
	case RouteTerminate:
		return "terminate", false, len(r.definition.Stages), nil
	case RouteJump:
		targetIdx := FindStageIndex(r.definition.Stages, inst.StageID)
		if targetIdx < 0 {
			return "", false, 0, core.NewSystemError(core.ErrCodeNotFound, fmt.Sprintf("target stage %s not found", inst.StageID))
		}
		return "jump", false, targetIdx, nil
	case RouteJumpTo:
		targetIdx := FindStageIndex(r.definition.Stages, inst.Address.Stage)
		if targetIdx < 0 {
			return "", false, 0, core.NewSystemError(core.ErrCodeNotFound, fmt.Sprintf("target stage %s not found", inst.Address.Stage))
		}
		return "jump", false, targetIdx, nil
	case RoutePause:
		return "pause", true, currentIdx, nil
	default:
		return "advance", false, currentIdx + 1, nil
	}
}

func (r *RunContextImpl) handlePause(ctx context.Context, stage Stage, pauseInst RoutingInstruction, currentIdx int) (PipelineRunResult, error) {
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

	// @note #review-20260910-012 issue P2 resolved status=resolved priority=P2 tags=#review,#error-handling : Checkpoint persistence failure is silently discarded on pause
	// @author hermes-review
	// @see #review-20260822-033
	//
	// Resolved: a failed Persist checkpoint write now FAILS THE PAUSE.
	// Everything downstream (the pipeline:pause event, the "paused"
	// result, the runtime's pausedRun registration) used to proceed as
	// if persistence had succeeded, so a run whose store write failed
	// reported paused with nothing durable recording where to resume —
	// the legacy checkpoint resume path then found no checkpoint and
	// recovery dead-ended. A lost checkpoint invalidates the whole
	// paused-run contract, so the error surfaces as a failed stage/run
	// instead of a fake pause. The sibling discarded-update sites in
	// this file (nested-checkpoint bubbling below, default sub-pipeline
	// merge) are best-effort by design and now LOG failures like
	// review-20260822-033 does.
	if pauseInst.Persist {
		if err := r.store.Update(ctx, func(state map[string]any) error {
			return WriteCheckpoint(state, ckpt)
		}); err != nil {
			if r.logger != nil {
				r.logger.Error("pipeline: failed to persist pause checkpoint — failing the pause", "runId", r.runID, "pipelineId", r.definition.ID, "stageId", stage.ID, "error", err)
			}
			return PipelineRunResult{
				Status:     "failed",
				RunID:      r.runID,
				PipelineID: r.definition.ID,
				FinalState: stateSnapshot(r.store),
				Error:      core.NewSystemError(core.ErrCodeExecutionFailed, "failed to persist pause checkpoint").WithCause(err),
			}, core.NewSystemError(core.ErrCodeExecutionFailed, "failed to persist pause checkpoint").WithCause(err)
		}
		r.syncStateMilestone(ctx)
	}

	// Record the checkpoint in the action log so the Replayer can recover
	// pause metadata (wait-for-event, timeout, cron) from the log alone.
	if r.actionLog != nil {
		ckptJSON, _ := json.Marshal(ckpt)
		r.actionLog.Append(ctx, actionlog.Entry{
			RunID:      r.runID,
			RerunIndex: r.rerunIndex,
			PipelineID: r.definition.ID,
			Kind:       actionlog.KindCheckpoint,
			Type:       "pipeline:pause",
			StageID:    stage.ID,
			Output:     ckptJSON,
		})
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

// recordRouting appends a KindRouted entry to the action log for the given
// stage's routing decision. Per §2.3 of the design doc, routing decisions are
// always recorded regardless of router purity — cheap (a stage ID and handle),
// and it eliminates replay divergence if a router is ever accidentally impure.
func (r *RunContextImpl) recordRouting(ctx context.Context, stageID string, inst RoutingInstruction) {
	if r.actionLog == nil {
		return
	}
	handle := inst.WireHandle()
	if handle == "" {
		return // advance/terminate — nothing to record
	}
	r.appendLog(ctx, actionlog.Entry{
		Kind:    actionlog.KindRouted,
		Type:    "router:evaluated",
		StageID: stageID,
		Handle:  handle,
	})
}

// PauseRouteHandle is the routing handle recorded for pause instructions
// without an explicit resume stage (PauseForEvent / PauseForEvents /
// PauseForCron). It is a WAIT MARKER, not a jump target: replay consumers
// (pkg/replay) must resolve it through the stage's pause checkpoint
// (ResumeAt) instead of re-entering the pausing stage.
const PauseRouteHandle = "__pause__"

// WireHandle extracts the target handle from a routing instruction for
// action log recording. Returns empty for advance/terminate (no route target).
// (R.2: was routingHandle.)
func (i RoutingInstruction) WireHandle() string {
	switch i.Kind {
	case RouteJump:
		return i.StageID
	case RouteJumpTo:
		return i.Address.Stage
	case RoutePause:
		if i.StageID != "" {
			return i.StageID
		}
		return PauseRouteHandle
	default:
		return ""
	}
}

// @note #review-20260826-002 issue P1 resolved status=resolved priority=P1 tags=#review,#concurrency,#bug : stateSnapshot is a shallow copy — nested maps alias live store state
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
