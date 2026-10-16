package runtime

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/scheduler"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/stretchr/testify/require"
)

// C5-fix (20261003) ordering contract, per plan task C5-fix (H5 + L3):
//
//      When a run chains through pause → resume → re-pause (depth ≥ 3), the
//      cron auto-resume armed for an OLDER pause must never be armed after a
//      NEWER pause has replaced it. Pre-fix, every resume frame armed its
//      pause's cron unconditionally after spawning the buffered-event resume,
//      so a stale schedule could land on top of (or fire during) a newer wait
//      and resume it as if cron-authorized.
//
// The chain is driven deterministically: each resumed segment blocks on a
// gate step, and a helper goroutine emits the chain event mid-segment so
// it is buffered against the still-registered wait and drains at the next
// re-pause. Gate-step ordering makes every schedule/cancel land at a
// known point in the chain.

// seqScheduler records Schedule/Cancel calls in order (cron value with each).
type seqScheduler struct {
	mu    sync.Mutex
	seq   []string // "S:<cron>" / "C"
	live  map[string]string
	calls int
}

func newSeqScheduler() *seqScheduler { return &seqScheduler{live: map[string]string{}} }

func (s *seqScheduler) Schedule(id string, cron string, cb func(context.Context)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq = append(s.seq, "S:"+cron)
	s.live[id] = cron
	s.calls++
	return nil
}

func (s *seqScheduler) Cancel(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.live, id)
	s.seq = append(s.seq, "C")
	return nil
}

func (s *seqScheduler) Shutdown(context.Context) error { return nil }

func (s *seqScheduler) snapshot() (seq []string, live map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.seq))
	copy(out, s.seq)
	liveCopy := make(map[string]string, len(s.live))
	for k, v := range s.live {
		liveCopy[k] = v
	}
	return out, liveCopy
}

var _ scheduler.Scheduler = (*seqScheduler)(nil)

// chainGate is a step that signals entry and blocks until released, giving
// the test a deterministic handle on mid-segment timing.
type chainGate struct{ entered, release chan struct{} }

func newChainGate() *chainGate {
	return &chainGate{entered: make(chan struct{}, 1), release: make(chan struct{}, 1)}
}

func (g *chainGate) step() pipeline.StepAction {
	return func(ctx context.Context, pcxt *pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
		g.entered <- struct{}{}
		<-g.release
		return store.SetValue("gate", true), nil
	}
}

// buildChainWorkflow: wait1(cron c1) → gate2 → wait2(cron c2) →
// gate3 → wait3(no cron) → final. Every wait pauses on event "chain".
// Gates sit inside each resumed segment so the helper goroutine can emit
// mid-segment (buffering the chain event against the live wait) and then
// release the segment deterministically. Returns the workflow and its
// gates in chain order.
func buildChainWorkflow(c1, c2 string) (*pipeline.Workflow, []*chainGate) {
	g2, g3 := newChainGate(), newChainGate()
	gates := []*chainGate{g2, g3}

	pauseStage := func(id string, cron string) pipeline.Stage {
		return pipeline.Stage{
			ID: id, Label: id,
			Router: func(ctx context.Context, state map[string]any, st store.Store) (pipeline.RoutingInstruction, error) {
				return pipeline.PauseForCron("chain", cron), nil
			},
		}
	}
	gateStage := func(id string, g *chainGate) pipeline.Stage {
		return pipeline.Stage{
			ID: id, Label: id,
			Steps: map[string]pipeline.Step{
				id + "-step": {ID: id + "-step", Effect: int(pipeline.EffectSideEffecting), Action: g.step()},
			},
		}
	}
	final := pipeline.Stage{
		ID: "stage:final", Label: "Final",
		Steps: map[string]pipeline.Step{
			"final-step": {ID: "final-step", Effect: int(pipeline.EffectSideEffecting), Action: func(ctx context.Context, pcxt *pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
				return store.SetValue("done", float64(3)), nil
			}},
		},
	}
	def := pipeline.PipelineDefinition{
		ID:    "chain-pipeline",
		Label: "Chain",
		Stages: []pipeline.Stage{
			pauseStage("stage:wait1", c1),
			gateStage("stage:gate2", g2),
			pauseStage("stage:wait2", c2),
			gateStage("stage:gate3", g3),
			pauseStage("stage:wait3", ""),
			final,
		},
	}
	return &pipeline.Workflow{
		ID:    "wf-chain-cron",
		Label: "Chain Cron",
		Pipelines: map[string]pipeline.PipelineDefinition{
			"trigger:manual:Run": def,
		},
		Triggers: map[string]pipeline.WorkflowTrigger{
			"trigger:manual:Run": {ID: "trigger:manual:Run", Event: ManualEvent},
		},
	}, gates
}

// TestChainedRepauseArmsOnlyCurrentWaitCron: after a depth-3 chained
// re-pause, the scheduler must hold ONLY the newest wait's auto-resume
// state. Wait1 arms c1 at the first pause (plain case). The three chained
// buffered-event resumes (each driven by a mid-segment emit) must NOT arm
// c2 on top of wait3: wait2's arm is superseded by wait3's no-cron drop,
// and wait3's re-pause path owns the schedule from there.
func TestChainedRepauseArmsOnlyCurrentWaitCron(t *testing.T) {
	const (
		c1 = "@every 1h"
		c2 = "@every 2h"
	)
	wf, gates := buildChainWorkflow(c1, c2)

	ms := NewManualEventSource()
	fake := newSeqScheduler()
	done := make(chan RunResult, 1)
	rt := NewWorkflowRuntime(Options{
		ActionLog:   actionlog.NewMemoryActionLog(),
		EventSource: ms,
		Scheduler:   fake,
	})
	defer rt.Shutdown(context.Background())

	prepared := make(chan string, 1)
	require.NoError(t, rt.Register(wf, RegisterOptions{
		Mode:       Mode{Type: "transient"},
		OnPrepare:  func(h *RunHandle) error { prepared <- h.RunID; return nil },
		OnComplete: func(r RunResult) { done <- r },
	}))
	rt.Bus().Emit(context.Background(), ManualEvent, events.PipelineEvent{Payload: map[string]any{}})
	runID := <-prepared

	// The run pauses on wait1 and spawnRun arms c1 (plain first pause —
	// no buffered event, gate must pass). NB: the cron arm happens AFTER
	// the paused-registry write awaitPaused observes (fold-snapshot save
	// and buffered delivery sit between them), so the arm is polled for —
	// asserting immediately after awaitPaused was a scheduling race the
	// H2.2 snapshot write widened into a real flake.
	awaitPaused(t, rt, 1)
	require.Eventually(t, func() bool {
		seq, _ := fake.snapshot()
		return len(seq) == 1 && seq[0] == "S:"+c1
	}, 5*time.Second, 5*time.Millisecond, "first pause must arm exactly c1")
	seq, live := fake.snapshot()
	require.Equal(t, c1, live[runID], "c1 must be the live auto-resume schedule")

	// Helper: release each gate after emitting "chain" mid-segment so the
	// event is buffered against the still-registered wait and drains at
	// the next re-pause, chaining the next resume.
	go func() {
		for _, g := range gates { // gate2, gate3 in chain order
			<-g.entered
			rt.Bus().Emit(context.Background(), "chain", events.PipelineEvent{Payload: map[string]any{}})
			g.release <- struct{}{}
		}
	}()

	// Drive the chain: resume wait1 externally; gate2 emits + releases →
	// wait2 re-pause → buffered drain → nested resume#2 → gate3 → wait3
	// re-pause → buffered drain → nested resume#3 → final stage completes.
	ms.Emit("chain", map[string]any{"start": true})

	res := awaitResult(t, done)
	require.Equal(t, "succeeded", res.Status, "chained run must complete: %v", res.Error)
	require.Equal(t, float64(3), res.FinalState["done"], "final stage must have run")

	// THE ORDERING CONTRACT: no schedule may outlive its own wait. c2 was
	// armed (pre-fix) by resume#1's frame AFTER the chain had already
	// re-paused on wait3 — a stale timer that could fire during wait3 and
	// resume it as if cron-authorized. Post-fix the arm is gated on the
	// run still being paused on that same checkpoint, so the scheduler
	// must never see c2 at all.
	seq, live = fake.snapshot()
	for _, entry := range seq {
		require.NotEqual(t, "S:"+c2, entry,
			"stale c2 arm for wait2 landed after a newer wait replaced it (H5 ordering)")
	}
	require.NotContains(t, live, runID,
		"no auto-resume schedule may survive a completed run")
}

// awaitResult waits for a completion result.
func awaitResult(t *testing.T, done chan RunResult) RunResult {
	t.Helper()
	select {
	case r := <-done:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for run completion")
		return RunResult{}
	}
}
