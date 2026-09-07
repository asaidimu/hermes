// Package replay implements event-sourced recovery for pipeline runs.
// It reads the Action Log (pkg/actionlog) and reconstructs state by
// replaying the pipeline definition forward: re-executing pure steps,
// injecting recorded state deltas for effectful steps, and following
// recorded routing decisions. See EXECUTION_ENGINE_REDESIGN_V2.md §4.2.
package replay

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/effect"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
)

// StepAddress is an alias for pipeline.EntryAddress, pointing to a
// specific stage (and optionally step) within a pipeline.
type StepAddress = pipeline.EntryAddress

// DefinitionResolver returns the PipelineDefinition for a run. Called
// by the Replayer during Rebuild to obtain the definition to replay.
type DefinitionResolver func(runID string) (*pipeline.PipelineDefinition, bool)

// Replayer reconstructs pipeline state from the Action Log by replaying
// the pipeline definition forward. It is the consumer side of the
// event-sourced recovery design (§4.2).
type Replayer struct {
	log     actionlog.Store
	resolve DefinitionResolver
}

// NewReplayer creates a Replayer that reads from the given action log
// and resolves pipeline definitions via the provided resolver.
func NewReplayer(log actionlog.Store, resolve DefinitionResolver) *Replayer {
	if log == nil {
		log = actionlog.NopLog{}
	}
	return &Replayer{
		log:     log,
		resolve: resolve,
	}
}

// Rebuild reconstructs the run's state by replaying the pipeline
// definition forward, consulting the Action Log at effectful-step
// boundaries and routing decision points.
//
// Returns the reconstructed Store and the resume address:
//   - Empty StepAddress (Stage=="") means the run completed — no
//     forward execution needed.
//   - Non-empty StepAddress means forward execution should resume at
//     that step (the first unrecorded effectful step, or the first
//     stage whose routing decision wasn't recorded).
func (rp *Replayer) Rebuild(ctx context.Context, runID string) (store.Store, StepAddress, error) {
	entries, err := rp.latestEntries(ctx, runID)
	if err != nil {
		return nil, StepAddress{}, fmt.Errorf("actionlog.All: %w", err)
	}

	byStep := indexByStepID(entries)
	routes := indexByStage(entries)

	def, ok := rp.resolve(runID)
	if !ok {
		return nil, StepAddress{}, core.NewSystemError(core.ErrCodeNotFound,
			"no pipeline definition for run "+runID)
	}

	st := store.NewMemoryStore(nil)

	// Build a stage-indexed lookup for jump resolution.
	stageIdx := make(map[string]int, len(def.Stages))
	for i, s := range def.Stages {
		stageIdx[s.ID] = i
	}

	currentIdx := 0
	for currentIdx < len(def.Stages) {
		stage := def.Stages[currentIdx]

		// Process all steps in this stage.
		for _, step := range stage.Steps {
			if step.Effect == int(effect.SideEffecting) {
				entry, done := byStep[step.ID]
				if done {
					// Effectful step completed: inject its recorded
					// state delta. Don't re-execute — side effects
					// must not be duplicated.
					if err := applyRecordedDelta(st, entry); err != nil {
						return nil, StepAddress{}, fmt.Errorf(
							"apply recorded delta for step %s: %w", step.ID, err)
					}
					continue
				}
				// Not yet recorded — this is where forward
				// execution resumes.
				return st, StepAddress{Stage: stage.ID, Step: step.ID}, nil
			}
			// Pure: re-execute for real against reconstructed state.
			if err := replayStep(ctx, st, step); err != nil {
				return nil, StepAddress{}, fmt.Errorf(
					"replay pure step %s: %w", step.ID, err)
			}
		}

		// All steps in this stage processed. Check routing.
		if handle, routed := routes[stage.ID]; routed && handle != "" {
			// Routing decision recorded — follow it. Handle maps to
			// a stage ID for jump instructions.
			if targetIdx, found := stageIdx[handle]; found {
				currentIdx = targetIdx
				continue
			}
			// Unknown target — treat as resume point.
			return st, StepAddress{Stage: stage.ID}, nil
		}

		// No routing entry or advance — natural progression to next stage.
		currentIdx++
	}

	// Run completed — all stages replayed without hitting an unrecorded
	// effectful step or missing routing decision.
	return st, StepAddress{}, nil
}

// StateAt reconstructs the exact state map as it existed immediately
// after stepAddr completed, by replaying the Action Log up to (and
// including) that step. Used for debugging, audit, and "why did this
// run take the branch it took" questions — without the engine having
// ever separately stored a snapshot at that point.
func (rp *Replayer) StateAt(ctx context.Context, runID string, addr StepAddress) (map[string]any, error) {
	entries, err := rp.latestEntries(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("actionlog.All: %w", err)
	}

	byStep := indexByStepID(entries)

	def, ok := rp.resolve(runID)
	if !ok {
		return nil, core.NewSystemError(core.ErrCodeNotFound,
			"no pipeline definition for run "+runID)
	}

	st := store.NewMemoryStore(nil)

	for _, stage := range def.Stages {
		// Stop at the target stage before processing any steps.
		if stage.ID == addr.Stage && addr.Step == "" {
			return exportState(st)
		}

		for _, step := range stage.Steps {
			// Stop after reaching the target step.
			if stage.ID == addr.Stage && step.ID == addr.Step {
				return exportState(st)
			}

			if step.Effect == int(effect.SideEffecting) {
				entry, done := byStep[step.ID]
				if done {
					if err := applyRecordedDelta(st, entry); err != nil {
						return nil, err
					}
					continue
				}
				// Unrecorded effectful step before target — shouldn't
				// happen if addr is valid, but handle gracefully.
				return exportState(st)
			}
			if err := replayStep(ctx, st, step); err != nil {
				return nil, err
			}
		}
	}

	return exportState(st)
}

// --- helpers ---

// latestEntries reads the run's most recent attempt (highest rerunIndex).
// Returns nil when no log exists — the caller then resumes from the start.
func (rp *Replayer) latestEntries(ctx context.Context, runID string) ([]actionlog.Entry, error) {
	idx, err := rp.log.LatestRerunIndex(ctx, runID)
	if err != nil {
		return nil, err
	}
	if idx < 0 {
		return nil, nil
	}
	return rp.log.All(ctx, runID, idx)
}

// indexByStepID builds a map from StepID to the first completed entry.
// For effectful steps, this is the KindEffectCompleted or KindEffectFailed
// entry that records the step's outcome.
func indexByStepID(entries []actionlog.Entry) map[string]actionlog.Entry {
	m := make(map[string]actionlog.Entry)
	for _, e := range entries {
		if e.StepID != "" && (e.Kind == actionlog.KindEffectCompleted || e.Kind == actionlog.KindEffectFailed) {
			if _, exists := m[e.StepID]; !exists {
				m[e.StepID] = e
			}
		}
	}
	return m
}

// indexByStage builds a map from StageID to the routing handle of the
// most recent KindRouted entry for that stage.
func indexByStage(entries []actionlog.Entry) map[string]string {
	m := make(map[string]string)
	for _, e := range entries {
		if e.Kind == actionlog.KindRouted && e.StageID != "" {
			m[e.StageID] = e.Handle
		}
	}
	return m
}

// applyRecordedDelta injects the state delta recorded in an action log
// entry into the store. The delta is a flat map of key→value changes
// that were applied when the effectful step originally ran.
func applyRecordedDelta(st store.Store, entry actionlog.Entry) error {
	if len(entry.Output) == 0 {
		return nil // no delta recorded (step had no state effect)
	}
	var delta map[string]any
	if err := json.Unmarshal(entry.Output, &delta); err != nil {
		return fmt.Errorf("unmarshal state delta: %w", err)
	}
	return st.Update(context.Background(), func(state map[string]any) error {
		for k, v := range delta {
			state[k] = v
		}
		return nil
	})
}

// replayStep re-executes a pure step's Action against the store. The
// step's Action closure captures the node logic; we call it with a
// read-only state snapshot and apply the returned mutator.
func replayStep(ctx context.Context, st store.Store, step pipeline.Step) error {
	if step.Action == nil {
		return nil
	}
	// Build a minimal PipelineContext for the step. During replay,
	// the step doesn't need bus/logger/resource resolution — it just
	// needs to compute its mutator.
	pCtx := pipeline.NewReplayContext()
	var mutator store.Mutator
	var actionErr error
	readErr := st.Read(func(state map[string]any) error {
		mutator, actionErr = step.Action(ctx, pCtx, state)
		return nil
	})
	if readErr != nil {
		return readErr
	}
	if actionErr != nil {
		return actionErr
	}
	if mutator != nil {
		return st.Update(ctx, mutator)
	}
	return nil
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
