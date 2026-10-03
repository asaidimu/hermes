// Recovery (R.4, architecture audit §A): this file is the ONLY code in the
// tree that decides how a run recovers. Before R.4 the decision lived in two
// places (the runtime's Resume type-asserted the log implementation; the
// factory carried its own rebuild-vs-checkpoint branch) and the checkpoint
// fallback degraded guarantees SILENTLY.
//
// The recovery contract:
//
//   - Capability is decided by DATA, not type assertion: a run is
//     event-sourced-rebuildable iff the action log holds entries for it
//     (rebuildCapability probes LatestRerunIndex).
//   - No log entries + a checkpoint in the run store → checkpoint path,
//     LOGGED (never silent).
//   - A failed rebuild degrades loudly: logged fallback to the checkpoint
//     address.
//   - KindCheckpoint entries stay IN the log and fold during replay — the
//     checkpoint is a replay optimization inside this one component, not a
//     parallel recovery system.
//
// Startup rebuild and re-arm (plan S.3) are added here — this is the single
// home they must live in.
package runtime

import (
	"context"

	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/replay"
	"github.com/asaidimu/hermes/pkg/store"
)

// rebuildCapability reports — from DATA — whether the action log can support
// an event-sourced rebuild of runID, and returns the Rebuild func when it
// can. No log, no entries: no rebuild capability.
func (rt *WorkflowRuntime) rebuildCapability(ctx context.Context, runID string, record *workflowRecord, pdef pipeline.PipelineDefinition) pipeline.RebuildFunc {
	if rt.actionLog == nil {
		return nil
	}
	latest, err := rt.actionLog.LatestRerunIndex(ctx, runID)
	if err != nil || latest < 0 {
		return nil
	}
	// @note #review-20260910-003 issue P2 resolved status=resolved priority=P2 tags=#review,#replay : DefinitionResolver answered every id with the root pipeline definition
	// @author hermes-review
	//
	// Resolved: the resolver answers with the definition the id actually
	// names, looked up across the whole compiled workflow
	// (Workflow.FindPipeline), falling back to the run's root definition.
	// Combined with per-pipeline entry partitioning (#review-20260910-015),
	// event-sourced recovery is trustworthy for subpipeline-bearing
	// pipelines. Dynamic distribute children are runtime-generated and are
	// never resolved here — the Replayer regenerates them from the parent
	// stage's DynamicPipelines closure.
	definitionResolver := func(resolvedID string) (*pipeline.PipelineDefinition, bool) {
		if d, ok := record.workflow.FindPipeline(resolvedID); ok {
			return d, true
		}
		if resolvedID == pdef.ID {
			d := pdef
			return &d, true
		}
		return nil, false
	}
	return replay.NewReplayer(rt.actionLog, definitionResolver).Rebuild
}

// resumeRunContext produces the run context a resume executes on. When
// rebuild is non-nil, event-sourced recovery is attempted through the
// factory; any failure logs and falls back to the checkpoint address
// (guarantees degrade loudly, never silently). Returns the context and
// whether the rebuilt store is canonical.
func (rt *WorkflowRuntime) resumeRunContext(
	ctx context.Context,
	runID string,
	pdef pipeline.PipelineDefinition,
	st store.Store,
	bus *events.MemoryScopedBus,
	ckpt *pipeline.PipelineCheckpoint,
	rebuild pipeline.RebuildFunc,
	rerunIndex int,
) (*pipeline.RunContextImpl, bool) {
	factory := pipeline.NewFactory(pdef, pipeline.FactoryOptions{
		Logger:       rt.logger,
		ActionLog:    rt.actionLog,
		Rebuild:      rebuild,
		RerunIndex:   rerunIndex,
		RunEnv:       rt.env,
		SecretLookup: rt.secretLookup(),
	})

	if rebuild != nil {
		runCtx, resumeErr := factory.Resume(ctx, runID, st, bus)
		if resumeErr != nil {
			// Rebuild failed — fall back to the legacy checkpoint path,
			// loudly.
			rt.logger.Warn("resume: event-sourced rebuild failed, falling back to checkpoint",
				"runId", runID, "error", resumeErr)
			return factory.PrepareWithEntry(runID, st, bus, ckpt.ResumeAt), false
		}
		return runCtx, true
	}

	// No rebuild data (no action log, or no entries for this run): the
	// checkpoint in the run store is the recovery source. Logged — the
	// pre-R.4 code degraded silently here.
	rt.logger.Warn("resume: no action-log entries for run — recovering from the checkpoint in run state",
		"runId", runID, "pipelineId", pdef.ID)
	return factory.PrepareWithEntry(runID, st, bus, ckpt.ResumeAt), false
}
