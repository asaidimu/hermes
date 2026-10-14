// The fold: replaying one pipeline definition forward against a store —
// the per-pipeline entry partitioning, the definition walk, subpipeline
// replay + merge, and delta injection. Split out of replayer.go by R.1
// (pure move).
package replay

import (
        "context"
        "encoding/json"
        "fmt"
        "os"
        "sort"
        "strings"

        "github.com/asaidimu/hermes/pkg/actionlog"
        "github.com/asaidimu/hermes/pkg/pipeline"
        "github.com/asaidimu/hermes/pkg/store"
)

// pipelineGroups holds a run's action log entries partitioned by the
// pipeline that produced them. Entries without a PipelineID (written
// before the field existed) are treated as root entries.
type pipelineGroups map[string][]actionlog.Entry

// partitionByPipeline splits entries per PipelineID. Root entries are those
// whose PipelineID equals rootPipelineID or is empty (backward compatibility
// with logs written before #review-20260910-015 stamped the field).
func partitionByPipeline(entries []actionlog.Entry, rootPipelineID string) pipelineGroups {
        groups := pipelineGroups{}
        for _, e := range entries {
                key := e.PipelineID
                if key == "" {
                        key = rootPipelineID
                }
                groups[key] = append(groups[key], e)
        }
        return groups
}

// isPipelinesModeStage reports whether a stage runs subpipelines (mirrors
// RunContextImpl.Run's mode detection).
func isPipelinesModeStage(stage pipeline.Stage) bool {
        return len(stage.Pipelines) > 0 || stage.DynamicPipelines != nil
}

// resolveChildPipelines resolves a pipelines-mode stage's child definitions
// exactly like the runtime does: DynamicPipelines (if set) wins over the
// static list, evaluated against the reconstructed state snapshot.
func resolveChildPipelines(stage pipeline.Stage, st store.Store) ([]pipeline.PipelineDefinition, error) {
        resolved := stage.Pipelines
        if stage.DynamicPipelines != nil {
                state, err := exportState(st)
                if err != nil {
                        return nil, err
                }
                resolved = stage.DynamicPipelines(state)
        }
        return resolved, nil
}

// newChildStore seeds a child pipeline store like ExecuteSubPipelines does:
// a fresh store from stage.Config["initialState"] when configured, else a
// clone of the parent store at this point of the replay.
func newChildStore(stage pipeline.Stage, parent store.Store) (store.Store, error) {
        if stage.Config != nil {
                if is, ok := stage.Config["initialState"].(map[string]any); ok && is != nil {
                        return store.NewMemoryStore(store.DeepCopyMap(is)), nil
                }
        }
        cloned, err := parent.Clone()
        if err != nil {
                return nil, fmt.Errorf("clone parent store for subpipeline: %w", err)
        }
        return cloned, nil
}

// stageForked reports whether the parent's own entries contain a
// KindSubpipelineForked marker for the stage — proof the stage actually ran.
func stageForked(own []actionlog.Entry, stageID string) bool {
        for _, e := range own {
                if e.Kind == actionlog.KindSubpipelineForked && e.StageID == stageID {
                        return true
                }
        }
        return false
}

// walkDef replays one pipeline definition forward against st, consuming the
// entries produced by THAT pipeline (groups[pipelineID]); subpipeline
// entries are consumed recursively when their fork stage is reached.
// Returns the resume address: empty when the definition completed, or the
// stage/step (optionally nested in a subpipeline via EntryAddress.Pipeline)
// where forward execution should pick up.
func (rp *Replayer) walkDef(ctx context.Context, def *pipeline.PipelineDefinition, groups pipelineGroups, st store.Store) (StepAddress, error) {
        own := groups[def.ID]
        // P4.8: one pass over this pipeline's entries memoizes every lookup
        // the stage loop needs (pre-P4.8 the per-stage helpers re-scanned the
        // slice per stage — O(stages x entries)).
        idx := buildLogIndex(own)
        byStep := idx.byStep
        routes := idx.routes
        checkpoints := idx.checkpoints

        // Build a stage-indexed lookup for jump resolution.
        stageIdx := make(map[string]int, len(def.Stages))
        for i, s := range def.Stages {
                stageIdx[s.ID] = i
        }

        currentIdx := 0
        for currentIdx < len(def.Stages) {
                stage := def.Stages[currentIdx]

                // Pipelines-mode stage: replay each child instance against its
                // own entries and definition, then merge like the runtime does.
                if isPipelinesModeStage(stage) {
                        if !idx.forked[stage.ID] {
                                // No fork entry — the stage never ran. Forward
                                // execution resumes at this stage.
                                return StepAddress{Stage: stage.ID}, nil
                        }

                        childResume, err := rp.replaySubpipelineStage(ctx, stage, groups, st)
                        if err != nil {
                                return StepAddress{}, err
                        }
                        if os.Getenv("REPLAY_DEBUG") != "" {
                                fmt.Printf("DEBUG parent=%s stage=%s childResume=%+v\n", def.ID, stage.ID, childResume)
                        }
                        if childResume.Stage != "" {
                                // A child paused mid-run: bubble the nested resume
                                // address (mirrors the runtime's nested checkpoint).
                                return StepAddress{
                                        Stage: stage.ID,
                                        Step:  childResume.Step,
                                        Pipeline: &pipeline.SubPipelineAddress{
                                                Index: childResume.index,
                                                Stage: childResume.Stage,
                                        },
                                }, nil
                        }
                        // Children settled — fall through to routing.
                } else {
                        // Steps-mode stage: process all steps in DETERMINISTIC order
                        // (P4.8: Go map iteration is randomized; the first unrecorded
                        // effectful step defines the resume address, so the order must
                        // not depend on map luck — sorted step IDs, pinned by golden
                        // vectors).
                        for _, step := range sortedSteps(stage.Steps) {
                                if step.Effect == int(pipeline.EffectSideEffecting) {
                                        recorded := byStep[step.ID]
                                        if len(recorded) > 0 {
                                                // Effectful step has recorded outcomes: inject ALL
                                                // of them in seq order (S.1 canonical seq-fold —
                                                // retries and loop iterations each appended a
                                                // delta; replaying the sequence reproduces the
                                                // final state). Don't re-execute — side effects
                                                // must not be duplicated.
                                                if err := applyRecordedDeltas(st, recorded); err != nil {
                                                        return StepAddress{}, fmt.Errorf(
                                                                "apply recorded deltas for step %s: %w", step.ID, err)
                                                }
                                                continue
                                        }
                                        // Not yet recorded — this is where forward
                                        // execution resumes.
                                        return StepAddress{Stage: stage.ID, Step: step.ID}, nil
                                }
                                // Pure: re-execute for real against reconstructed state.
                                if err := replayStep(ctx, st, step); err != nil {
                                        return StepAddress{}, fmt.Errorf(
                                                "replay pure step %s: %w", step.ID, err)
                                }
                        }
                }

                // All steps/children in this stage processed. Check routing.
                if handle, routed := routes[stage.ID]; routed && handle != "" {
                        // Pause routes: either the "__pause__" wait marker (pause
                        // without an explicit resume stage) or an explicit resume
                        // stage that matches this stage's own pause checkpoint.
                        // Both mean execution WAITED here instead of jumping.
                        if ckptResume, isCkpt := checkpoints[stage.ID]; isCkpt &&
                                (handle == pipeline.PauseRouteHandle || handle == ckptResume) {
                                if !idx.resumedAfter(stage.ID) {
                                        // The latest pause has not been resumed yet — the
                                        // log ends inside the wait. Forward execution
                                        // starts at the checkpoint's resume stage (the
                                        // walk there decides whether anything is left to
                                        // do, so an empty resume stage still yields a
                                        // resumable address instead of a silent
                                        // "completed").
                                        return StepAddress{Stage: ckptResume}, nil
                                }
                                // The resumed segment is recorded in the log: continue
                                // from the resume stage.
                                if targetIdx, found := stageIdx[ckptResume]; found {
                                        if targetIdx == currentIdx {
                                                // Degenerate pause on the last stage: the
                                                // recorded resume address is the pausing
                                                // stage itself. Forward execution re-enters
                                                // it (the pause router re-evaluates buffered
                                                // events); returning the address here keeps
                                                // the walk from spinning on itself.
                                                return StepAddress{Stage: stage.ID}, nil
                                        }
                                        currentIdx = targetIdx
                                        continue
                                }
                        }
                        // Routing decision recorded — follow it. Handle maps to
                        // a stage ID for jump instructions.
                        if targetIdx, found := stageIdx[handle]; found {
                                currentIdx = targetIdx
                                continue
                        }
                        // @note #review-20260910-023 issue P1 resolved status=resolved priority=P1 tags=#review,#replay,#resume : Rebuild returns the pausing stage as the resume address, so every resumed run re-pauses forever
                        // @author hermes-review
                        // @see #review-20260910-001
                        //
                        // Resolved: a recorded pause route (PauseRouteHandle, "__pause__")
                        // is a WAIT MARKER, not a jump target. Previously the lookup below
                        // failed (no stage is named "__pause__") and fell through to
                        // "unknown target — treat as resume point", returning the PAUSING
                        // stage as the resume address. The resumed run then re-entered the
                        // pause stage, whose router paused again — an infinite pause loop
                        // that made event-sourced resume unusable for any workflow that
                        // actually pauses (masked until now: the pause node was a silent
                        // no-op, see #review-20260910-007, and hand-built runtime tests
                        // hit the vacuous zero-definition path, see
                        // #review-20260910-022). A pause checkpoint records where execution
                        // should continue (ResumeAt) — resolve the marker through it.
                        if handle == pipeline.PauseRouteHandle {
                                if resumeAt, ok := checkpoints[stage.ID]; ok {
                                        if targetIdx, found := stageIdx[resumeAt]; found {
                                                if targetIdx == currentIdx {
                                                        // Degenerate pause on the last stage: the
                                                        // recorded resume address is the pausing
                                                        // stage itself. Forward execution re-enters
                                                        // it (the pause router re-evaluates buffered
                                                        // events); returning the address here keeps
                                                        // the walk from spinning on itself.
                                                        return StepAddress{Stage: stage.ID}, nil
                                                }
                                                currentIdx = targetIdx
                                                continue
                                        }
                                }
                        }
                        // Unknown target — treat as resume point.
                        return StepAddress{Stage: stage.ID}, nil
                }

                // No routing entry or advance — natural progression to next stage.
                currentIdx++
        }

        // Run completed — all stages replayed without hitting an unrecorded
        // effectful step or missing routing decision.
        return StepAddress{}, nil
}

// childResume is the resume address produced by a child pipeline walk, plus
// the child's index within the parent stage's pipeline list (for the nested
// SubPipelineAddress).
type childResume struct {
        index int
        Stage string
        Step  string
}

// replaySubpipelineStage replays a pipelines-mode stage's children: each
// child's entries are replayed against its own store (seeded/cloned like
// ExecuteSubPipelines does) and merged into the parent store with the same
// resultKey/default-merge semantics as RunContextImpl.Run. The caller must
// check stageForked before invoking.
//
// Returns a resume with Stage != "" when a child paused mid-run (the caller
// bubbles it as a nested EntryAddress.Pipeline address); a zero childResume
// when all children settled.
func (rp *Replayer) replaySubpipelineStage(ctx context.Context, stage pipeline.Stage, groups pipelineGroups, st store.Store) (childResume, error) {
        resolved, err := resolveChildPipelines(stage, st)
        if err != nil {
                return childResume{}, err
        }

        for i := range resolved {
                child := &resolved[i]

                childStore, err := newChildStore(stage, st)
                if err != nil {
                        return childResume{}, fmt.Errorf("subpipeline %s: %w", child.ID, err)
                }

                addr, err := rp.walkDef(ctx, child, groups, childStore)
                if err != nil {
                        return childResume{}, fmt.Errorf("replay subpipeline %s: %w", child.ID, err)
                }
                if addr.Stage != "" {
                        return childResume{index: i, Stage: addr.Stage, Step: addr.Step}, nil
                }

                if err := mergeChildResult(st, child, childStore, stage, i, len(resolved), groups[child.ID]); err != nil {
                        return childResume{}, err
                }
        }
        return childResume{}, nil
}

// mergeChildResult applies the runtime's post-join merge semantics for one
// child: resultKey-configured stages store the child's final state (or error
// detail) under resultKey[":" + childID]; default merge folds application
// keys (non "__" prefixed) from successful children into the parent state.
func mergeChildResult(
        parent store.Store,
        child *pipeline.PipelineDefinition,
        childStore store.Store,
        stage pipeline.Stage,
        index, total int,
        childEntries []actionlog.Entry,
) error {
        // Infer the child's terminal status from its own entries: a failed or
        // aborted child run appends KindPipelineFailed; anything else that
        // completed is a success. (The runtime's join EVENT carries per-child
        // statuses, but the durable join log entry records only the stage id —
        // inference from entries is the replay-faithful source.)
        succeeded := true
        for _, e := range childEntries {
                if e.Kind == actionlog.KindPipelineFailed {
                        succeeded = false
                        break
                }
        }

        if stage.Config != nil {
                if resultKey, ok := stage.Config["resultKey"].(string); ok && resultKey != "" {
                        var mergeVal any
                        if succeeded {
                                final, err := exportState(childStore)
                                if err != nil {
                                        return fmt.Errorf("export subpipeline %s state: %w", child.ID, err)
                                }
                                mergeVal = final
                        } else {
                                mergeVal = map[string]any{"pipelineId": child.ID, "status": "failed"}
                        }
                        key := resultKey
                        if total > 1 {
                                key = resultKey + ":" + child.ID
                        }
                        return parent.Update(context.Background(), store.SetValue(key, mergeVal))
                }
        }

        if !succeeded {
                return nil
        }
        final, err := exportState(childStore)
        if err != nil {
                return fmt.Errorf("export subpipeline %s state: %w", child.ID, err)
        }
        return parent.Update(context.Background(), func(state map[string]any) error {
                for k, v := range final {
                        if !strings.HasPrefix(k, "__") {
                                state[k] = v
                        }
                }
                return nil
        })
}

// foldDef folds every stage of a definition into st for StateAt projections:
// recorded deltas for effectful steps, re-execution for pure ones, recursive
// subpipeline folding + merging for pipelines-mode stages. When addr names a
// stage/step of THIS definition, folding stops there per the resolved
// #review-20260910-024 semantics: siblings of the target step are folded,
// the target's own contribution is skipped, and an empty StepAddress.Stage
// match ("state at stage entry") returns before the stage's steps.
func (rp *Replayer) foldDef(ctx context.Context, def *pipeline.PipelineDefinition, groups pipelineGroups, st store.Store, addr StepAddress) error {
        own := groups[def.ID]
        idx := buildLogIndex(own)
        byStep := idx.byStep

        for _, stage := range def.Stages {
                if isPipelinesModeStage(stage) {
                        if !idx.forked[stage.ID] {
                                // Stage never ran — contributes nothing.
                                if stage.ID == addr.Stage {
                                        return nil
                                }
                                continue
                        }
                        resolved, err := resolveChildPipelines(stage, st)
                        if err != nil {
                                return err
                        }
                        for i := range resolved {
                                child := &resolved[i]
                                childStore, err := newChildStore(stage, st)
                                if err != nil {
                                        return fmt.Errorf("subpipeline %s: %w", child.ID, err)
                                }
                                if err := rp.foldDef(ctx, child, groups, childStore, StepAddress{}); err != nil {
                                        return fmt.Errorf("replay subpipeline %s: %w", child.ID, err)
                                }
                                if err := mergeChildResult(st, child, childStore, stage, i, len(resolved), groups[child.ID]); err != nil {
                                        return err
                                }
                        }
                        if stage.ID == addr.Stage {
                                return nil
                        }
                        continue
                }

                // Stop at the target stage before processing any steps.
                if stage.ID == addr.Stage && addr.Step == "" {
                        return nil
                }

                if stage.ID == addr.Stage {
                        // Step-level target: fold in every sibling's contribution
                        // regardless of map iteration order, skip the target's own.
                        for _, step := range sortedSteps(stage.Steps) {
                                if step.ID == addr.Step {
                                        continue
                                }
                                if err := foldStep(ctx, st, byStep, step); err != nil {
                                        return err
                                }
                        }
                        return nil
                }

                // Stages before the target: fold in every step.
                for _, step := range sortedSteps(stage.Steps) {
                        if err := foldStep(ctx, st, byStep, step); err != nil {
                                return err
                        }
                }
        }
        return nil
}

// foldStep folds one step's contribution into a replayed store: the
// recorded delta for an effectful step (a no-op when the step has no
// recording yet), or re-execution for a pure step.
func foldStep(ctx context.Context, st store.Store, byStep map[string][]actionlog.Entry, step pipeline.Step) error {
        if step.Effect == int(pipeline.EffectSideEffecting) {
                recorded := byStep[step.ID]
                if len(recorded) == 0 {
                        return nil
                }
                return applyRecordedDeltas(st, recorded)
        }
        return replayStep(ctx, st, step)
}

// sortedSteps returns a definition's steps ordered by ID — a
// deterministic iteration order over the Steps map (plan P4.8). The
// runtime executes a stage's steps concurrently, so replay order is a
// canonicalization choice, not a semantic one; sorting makes replays
// reproducible run to run.
func sortedSteps(steps map[string]pipeline.Step) []pipeline.Step {
        ids := make([]string, 0, len(steps))
        for id := range steps {
                ids = append(ids, id)
        }
        sort.Strings(ids)
        out := make([]pipeline.Step, 0, len(ids))
        for _, id := range ids {
                out = append(out, steps[id])
        }
        return out
}

// --- helpers ---

// applyRecordedDeltas injects a step's recorded outcomes into the store
// IN SEQ ORDER (S.1 canonical seq-fold). Entries with no Output (step
// failures) are no-ops; each recorded delta is applied on top of the
// previous one, reproducing the state the run actually reached.
//
// All of the step's deltas are parsed first, then applied inside ONE
// store Update: the closure applies them in slice order, which is seq
// order, so the result is identical to per-entry Updates — but a log
// with N recorded outcomes for a step costs one lock acquisition, not
// N (the S.1 seq-fold made every delta real work; per-entry Updates
// measured 4× on the 1000-entry rebuild bench). A malformed delta fails
// the whole fold BEFORE any application — the rebuild aborts and the
// rebuilt store is discarded either way.
func applyRecordedDeltas(st store.Store, entries []actionlog.Entry) error {
        deltas := make([]map[string]any, 0, len(entries))
        for _, entry := range entries {
                if len(entry.Output) == 0 {
                        continue // no delta recorded (step had no state effect)
                }
                var delta map[string]any
                if err := json.Unmarshal(entry.Output, &delta); err != nil {
                        return fmt.Errorf("unmarshal state delta: %w", err)
                }
                deltas = append(deltas, delta)
        }
        return st.Update(context.Background(), func(state map[string]any) error {
                for _, delta := range deltas {
                        for k, v := range delta {
                                state[k] = v
                        }
                }
                return nil
        })
}

// exportState returns a deep copy of the store's current state.
func exportState(st store.Store) (map[string]any, error) {
        var state map[string]any
        err := st.Read(func(s map[string]any) error {
                state = store.DeepCopyMap(s)
                return nil
        })
        if state == nil {
                state = make(map[string]any)
        }
        return state, err
}
