package replay

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/stretchr/testify/require"
)

// Characterization tests: they pin the CURRENT observable behavior of the
// Replayer's fold (refactor plan P0.2) so the Phase 2 rebuild (R.1/R.2) and
// the Phase 5 semantics change (S.1 seq-fold) can each be judged against a
// recorded baseline rather than against intent. These are not aspirational
// specs — assertions here describe what the code does today, including the
// quirks S.1 will later replace (first-effect-wins folding).

// charLog is a MemoryActionLog plus the resolver the Replayer needs, keyed
// by pipeline id — the minimum fixture for a scripted entry stream.
type charFixture struct {
	log   *actionlog.MemoryActionLog
	rp    *Replayer
	defID string
}

func newCharFixture(def *pipeline.PipelineDefinition) *charFixture {
	log := actionlog.NewMemoryActionLog()
	defs := map[string]*pipeline.PipelineDefinition{def.ID: def}
	rp := NewReplayer(log, func(id string) (*pipeline.PipelineDefinition, bool) {
		d, ok := defs[id]
		return d, ok
	})
	return &charFixture{log: log, rp: rp, defID: def.ID}
}

// append stamps run identity (the store does not) and appends an entry to
// the given attempt.
func (f *charFixture) append(t *testing.T, rerunIndex int, e actionlog.Entry) {
	t.Helper()
	e.RunID = charRunID
	e.RerunIndex = rerunIndex
	e.PipelineID = f.defID
	_, err := f.log.Append(context.Background(), e)
	require.NoError(t, err)
}

const charRunID = "char-run-1"

func charStepAction(key string, val any, called *int) pipeline.StepAction {
	return func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
		if called != nil {
			*called++
		}
		return store.SetValue(key, val), nil
	}
}

// TestReplayFold_EffectDeltaInjectedNotReexecuted pins §4.1: a completed
// effectful step contributes its RECORDED delta; its action is never
// re-executed during replay (side effects must not be duplicated).
func TestReplayFold_EffectDeltaInjectedNotReexecuted(t *testing.T) {
	calls := 0
	def := &pipeline.PipelineDefinition{
		ID: "p-char-1",
		Stages: []pipeline.Stage{
			{
				ID: "stage-1",
				Steps: map[string]pipeline.Step{
					"s1": {ID: "s1", Effect: 2, Action: charStepAction("k", "recorded", &calls)},
				},
			},
		},
	}
	f := newCharFixture(def)
	delta, _ := json.Marshal(map[string]any{"k": "recorded"})
	f.append(t, 0, actionlog.Entry{Kind: actionlog.KindStepStarted, StageID: "stage-1", StepID: "s1"})
	f.append(t, 0, actionlog.Entry{Kind: actionlog.KindEffectCompleted, StageID: "stage-1", StepID: "s1", Output: delta})

	st, addr, err := f.rp.Rebuild(context.Background(), charRunID, f.defID)
	require.NoError(t, err)
	require.Equal(t, StepAddress{}, addr, "run completed during replay")
	var state map[string]any
	require.NoError(t, st.Read(func(s map[string]any) error { state = store.DeepCopyMap(s); return nil }))
	require.Equal(t, "recorded", state["k"])
	require.Equal(t, 0, calls, "effectful action must not re-run during replay")
}

// TestReplayFold_FirstEffectEntryWins pins the BASELINE fold semantics the
// architecture audit §D flags as divergent from TS: indexByStepID keeps the
// FIRST effect entry per step id, so a second (later, higher-Seq) entry for
// the same step is ignored. S.1 replaces this with seq-order folding and
// swaps this test's expectation — the flip is intentional and reviewed.
func TestReplayFold_FirstEffectEntryWins(t *testing.T) {
	def := &pipeline.PipelineDefinition{
		ID: "p-char-2",
		Stages: []pipeline.Stage{
			{
				ID: "stage-1",
				Steps: map[string]pipeline.Step{
					"s1": {ID: "s1", Effect: 2, Action: charStepAction("k", "ignored", nil)},
				},
			},
		},
	}
	f := newCharFixture(def)
	first, _ := json.Marshal(map[string]any{"k": "first"})
	second, _ := json.Marshal(map[string]any{"k": "second"})
	f.append(t, 0, actionlog.Entry{Kind: actionlog.KindEffectCompleted, StageID: "stage-1", StepID: "s1", Output: first})
	f.append(t, 0, actionlog.Entry{Kind: actionlog.KindEffectCompleted, StageID: "stage-1", StepID: "s1", Output: second})

	st, _, err := f.rp.Rebuild(context.Background(), charRunID, f.defID)
	require.NoError(t, err)
	var state map[string]any
	require.NoError(t, st.Read(func(s map[string]any) error { state = store.DeepCopyMap(s); return nil }))
	require.Equal(t, "first", state["k"], "baseline: first effect entry per step id wins")
}

// TestReplayFold_LastRouteWins pins indexByStage: the most recent
// KindRouted entry for a stage decides where the walk continues.
func TestReplayFold_LastRouteWins(t *testing.T) {
	def := &pipeline.PipelineDefinition{
		ID: "p-char-3",
		Stages: []pipeline.Stage{
			{ID: "stage-1", Steps: map[string]pipeline.Step{}},
			{ID: "stage-2", Steps: map[string]pipeline.Step{
				"s2": {ID: "s2", Effect: 2, Action: charStepAction("end", true, nil)},
			}},
			{ID: "stage-3", Steps: map[string]pipeline.Step{}},
		},
	}
	f := newCharFixture(def)
	f.append(t, 0, actionlog.Entry{Kind: actionlog.KindRouted, StageID: "stage-1", Handle: "stage-3"})
	f.append(t, 0, actionlog.Entry{Kind: actionlog.KindRouted, StageID: "stage-1", Handle: "stage-2"})
	f.append(t, 0, actionlog.Entry{Kind: actionlog.KindEffectCompleted, StageID: "stage-2", StepID: "s2",
		Output: mustJSON(map[string]any{"end": true})})

	st, addr, err := f.rp.Rebuild(context.Background(), charRunID, f.defID)
	require.NoError(t, err)
	require.Equal(t, StepAddress{}, addr, "walk followed the LAST route and completed")
	var state map[string]any
	require.NoError(t, st.Read(func(s map[string]any) error { state = store.DeepCopyMap(s); return nil }))
	require.Equal(t, true, state["end"])
}

// TestReplayFold_PureStepReexecuted pins §4.1: pure steps re-run against
// reconstructed state during replay.
func TestReplayFold_PureStepReexecuted(t *testing.T) {
	calls := 0
	def := &pipeline.PipelineDefinition{
		ID: "p-char-4",
		Stages: []pipeline.Stage{
			{
				ID: "stage-1",
				Steps: map[string]pipeline.Step{
					"pure-1": {ID: "pure-1", Effect: 1, Action: charStepAction("derived", "computed", &calls)},
					"fx-1":   {ID: "fx-1", Effect: 2, Action: charStepAction("k", "v", nil)},
				},
			},
		},
	}
	f := newCharFixture(def)
	f.append(t, 0, actionlog.Entry{Kind: actionlog.KindEffectCompleted, StageID: "stage-1", StepID: "fx-1",
		Output: mustJSON(map[string]any{"k": "v"})})

	st, addr, err := f.rp.Rebuild(context.Background(), charRunID, f.defID)
	require.NoError(t, err)
	require.Equal(t, StepAddress{}, addr)
	var state map[string]any
	require.NoError(t, st.Read(func(s map[string]any) error { state = store.DeepCopyMap(s); return nil }))
	require.Equal(t, "computed", state["derived"], "pure step re-executed")
	require.Equal(t, "v", state["k"], "effectful delta injected")
	require.GreaterOrEqual(t, calls, 1, "pure action ran during replay")
}

// TestReplayFold_ResumeAtUnrecordedEffect pins the resume address: the first
// effectful step without a recorded completion is where forward execution
// picks up.
func TestReplayFold_ResumeAtUnrecordedEffect(t *testing.T) {
	def := &pipeline.PipelineDefinition{
		ID: "p-char-5",
		Stages: []pipeline.Stage{
			{
				ID: "stage-1",
				Steps: map[string]pipeline.Step{
					"s1": {ID: "s1", Effect: 2, Action: charStepAction("a", 1, nil)},
					"s2": {ID: "s2", Effect: 2, Action: charStepAction("b", 2, nil)},
				},
			},
		},
	}
	f := newCharFixture(def)
	f.append(t, 0, actionlog.Entry{Kind: actionlog.KindEffectCompleted, StageID: "stage-1", StepID: "s1",
		Output: mustJSON(map[string]any{"a": float64(1)})})

	_, addr, err := f.rp.Rebuild(context.Background(), charRunID, f.defID)
	require.NoError(t, err)
	require.Equal(t, StepAddress{Stage: "stage-1", Step: "s2"}, addr)
}

// TestReplayFold_PauseCheckpointMidStream pins pause handling: a
// KindCheckpoint whose stage's route is the "__pause__" wait marker and
// which has no following KindResumed means the run is STILL WAITING —
// replay must return the checkpoint's ResumeAt address, not "completed".
func TestReplayFold_PauseCheckpointMidStream(t *testing.T) {
	def := &pipeline.PipelineDefinition{
		ID: "p-char-6",
		Stages: []pipeline.Stage{
			{ID: "stage-1", Steps: map[string]pipeline.Step{}},
			{ID: "stage-2", Steps: map[string]pipeline.Step{}},
		},
	}
	f := newCharFixture(def)
	ckpt := pipeline.PipelineCheckpoint{
		PipelineID:      f.defID,
		PausedAtStageID: "stage-1",
		ResumeAt:        pipeline.EntryAddress{Stage: "stage-2"},
	}
	f.append(t, 0, actionlog.Entry{Kind: actionlog.KindCheckpoint, StageID: "stage-1",
		Output: mustJSON(ckpt)})
	f.append(t, 0, actionlog.Entry{Kind: actionlog.KindRouted, StageID: "stage-1", Handle: pipeline.PauseRouteHandle})

	_, addr, err := f.rp.Rebuild(context.Background(), charRunID, f.defID)
	require.NoError(t, err)
	require.Equal(t, StepAddress{Stage: "stage-2"}, addr, "unresumed checkpoint → resume at ResumeAt")
}

// TestReplayFold_ResumeRecordedContinuesFromResumeStage pins the resumed
// half: once a KindResumed entry follows the checkpoint, the walk continues
// from the checkpoint's resume stage instead of stopping there.
func TestReplayFold_ResumeRecordedContinuesFromResumeStage(t *testing.T) {
	def := &pipeline.PipelineDefinition{
		ID: "p-char-7",
		Stages: []pipeline.Stage{
			{ID: "stage-1", Steps: map[string]pipeline.Step{}},
			{ID: "stage-2", Steps: map[string]pipeline.Step{}},
		},
	}
	f := newCharFixture(def)
	ckpt := pipeline.PipelineCheckpoint{
		PipelineID:      f.defID,
		PausedAtStageID: "stage-1",
		ResumeAt:        pipeline.EntryAddress{Stage: "stage-2"},
	}
	f.append(t, 0, actionlog.Entry{Kind: actionlog.KindCheckpoint, StageID: "stage-1",
		Output: mustJSON(ckpt)})
	f.append(t, 0, actionlog.Entry{Kind: actionlog.KindRouted, StageID: "stage-1", Handle: pipeline.PauseRouteHandle})
	f.append(t, 0, actionlog.Entry{Kind: actionlog.KindResumed, StageID: "stage-2", Type: "pipeline:resumed"})

	_, addr, err := f.rp.Rebuild(context.Background(), charRunID, f.defID)
	require.NoError(t, err)
	require.Equal(t, StepAddress{}, addr, "resumed segment recorded → run completed")
}

// TestReplayFold_MultiAttemptRebaseline pins #review-20260910-025: a
// multi-attempt run's history is stitched across rerunIndexes in attempt
// order with re-baselined Seq values, so a checkpoint written in attempt 0
// and the resume recorded in attempt 1 are seen in the right order — the
// run does NOT re-pause at the attempt-0 checkpoint on rebuild.
func TestReplayFold_MultiAttemptRebaseline(t *testing.T) {
	def := &pipeline.PipelineDefinition{
		ID: "p-char-8",
		Stages: []pipeline.Stage{
			{ID: "stage-1", Steps: map[string]pipeline.Step{
				"s1": {ID: "s1", Effect: 2, Action: charStepAction("a", 1, nil)},
			}},
			{ID: "stage-2", Steps: map[string]pipeline.Step{
				"s2": {ID: "s2", Effect: 2, Action: charStepAction("b", 2, nil)},
			}},
		},
	}
	f := newCharFixture(def)

	// Attempt 0: step a completes, run pauses at stage-1's router (resume
	// continues at stage-2 — the realistic multi-stage shape).
	f.append(t, 0, actionlog.Entry{Kind: actionlog.KindEffectCompleted, StageID: "stage-1", StepID: "s1",
		Output: mustJSON(map[string]any{"a": float64(1)})})
	ckpt := pipeline.PipelineCheckpoint{
		PipelineID:      f.defID,
		PausedAtStageID: "stage-1",
		ResumeAt:        pipeline.EntryAddress{Stage: "stage-2"},
	}
	f.append(t, 0, actionlog.Entry{Kind: actionlog.KindCheckpoint, StageID: "stage-1",
		Output: mustJSON(ckpt)})
	f.append(t, 0, actionlog.Entry{Kind: actionlog.KindRouted, StageID: "stage-1", Handle: pipeline.PauseRouteHandle})

	// Attempt 1 (post-resume): KindResumed, then step b completes.
	f.append(t, 1, actionlog.Entry{Kind: actionlog.KindResumed, StageID: "stage-1", Type: "pipeline:resumed"})
	f.append(t, 1, actionlog.Entry{Kind: actionlog.KindEffectCompleted, StageID: "stage-2", StepID: "s2",
		Output: mustJSON(map[string]any{"b": float64(2)})})

	st, addr, err := f.rp.Rebuild(context.Background(), charRunID, f.defID)
	require.NoError(t, err)
	require.Equal(t, StepAddress{}, addr, "stitched attempts → run completed, no re-pause")
	var state map[string]any
	require.NoError(t, st.Read(func(s map[string]any) error { state = store.DeepCopyMap(s); return nil }))
	require.Equal(t, float64(1), state["a"])
	require.Equal(t, float64(2), state["b"])
}

// TestStateAtParityWithRebuild pins that StateAt (debug projection) and
// Rebuild agree on state up to a completed step: the projection at the
// unrecorded second step carries exactly the same keys the rebuild produced
// before hitting that address.
func TestStateAtParityWithRebuild(t *testing.T) {
	def := &pipeline.PipelineDefinition{
		ID: "p-char-9",
		Stages: []pipeline.Stage{
			{ID: "stage-1", Steps: map[string]pipeline.Step{
				"s1": {ID: "s1", Effect: 2, Action: charStepAction("a", 1, nil)},
			}},
			{ID: "stage-2", Steps: map[string]pipeline.Step{
				"s2": {ID: "s2", Effect: 2, Action: charStepAction("b", 2, nil)},
			}},
		},
	}
	f := newCharFixture(def)
	f.append(t, 0, actionlog.Entry{Kind: actionlog.KindEffectCompleted, StageID: "stage-1", StepID: "s1",
		Output: mustJSON(map[string]any{"a": float64(1)})})

	state, err := f.rp.StateAt(context.Background(), charRunID, f.defID, StepAddress{Stage: "stage-2", Step: "s2"})
	require.NoError(t, err)
	require.Equal(t, float64(1), state["a"])
	_, hasB := state["b"]
	require.False(t, hasB, "state at s2 excludes s2's own (unrecorded) delta")
}

// TestRebuildUnknownRunYieldsEmptyStore pins the no-log case: a run with no
// entries rebuilds an empty store and resumes at the FIRST unrecorded
// effectful step of the first stage.
func TestRebuildUnknownRunYieldsEmptyStore(t *testing.T) {
	def := &pipeline.PipelineDefinition{
		ID: "p-char-10",
		Stages: []pipeline.Stage{
			{ID: "stage-1", Steps: map[string]pipeline.Step{
				"s1": {ID: "s1", Effect: 2, Action: charStepAction("a", 1, nil)},
			}},
		},
	}
	f := newCharFixture(def)
	_, addr, err := f.rp.Rebuild(context.Background(), "never-seen-run", f.defID)
	require.NoError(t, err)
	require.Equal(t, StepAddress{Stage: "stage-1", Step: "s1"}, addr)
}

// TestReplayFold_DegenerateSameStageResumeShortCircuits pins the
// keep-the-walk-from-spinning branch: when a checkpoint's ResumeAt is the
// pausing stage itself AND the resumed segment is recorded, the walk returns
// the pausing stage as the resume address instead of re-entering the pause
// router's stage in a loop. Documents today's behavior verbatim.
func TestReplayFold_DegenerateSameStageResumeShortCircuits(t *testing.T) {
	def := &pipeline.PipelineDefinition{
		ID: "p-char-11",
		Stages: []pipeline.Stage{
			{ID: "stage-1", Steps: map[string]pipeline.Step{}},
			{ID: "stage-2", Steps: map[string]pipeline.Step{}},
		},
	}
	f := newCharFixture(def)
	ckpt := pipeline.PipelineCheckpoint{
		PipelineID:      f.defID,
		PausedAtStageID: "stage-1",
		ResumeAt:        pipeline.EntryAddress{Stage: "stage-1"},
	}
	f.append(t, 0, actionlog.Entry{Kind: actionlog.KindCheckpoint, StageID: "stage-1",
		Output: mustJSON(ckpt)})
	f.append(t, 0, actionlog.Entry{Kind: actionlog.KindRouted, StageID: "stage-1", Handle: pipeline.PauseRouteHandle})
	f.append(t, 0, actionlog.Entry{Kind: actionlog.KindResumed, StageID: "stage-1", Type: "pipeline:resumed"})

	_, addr, err := f.rp.Rebuild(context.Background(), charRunID, f.defID)
	require.NoError(t, err)
	require.Equal(t, StepAddress{Stage: "stage-1"}, addr, "degenerate same-stage resume short-circuits the walk")
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// Compile-time guard: keep the time import if fixtures grow timestamps.
var _ = time.Now
