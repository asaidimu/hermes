// Package runtime — benchmark for chained re-pause cycles, the
// synchronous-bus dispatch path that #review-20260910-005 documented
// as load-bearing.
//
// runtime.Resume runs synchronously in the emitter's goroutine: a
// chained re-pause cycle (where a resumed run immediately re-pauses on
// another event) accumulates latency linearly in the chain depth. The
// benchmark measures the cost of N sequential pause→resume cycles on a
// single run, varying N to quantify the linear accumulation.
//
// The workflow under test is a single pipeline with N+1 stages: stage 0
// sets a=0, then N stages each pause on a distinct event "evt:N" before
// incrementing a counter, then a final stage finalizes. Each Emit
// synchronously resumes the run, which executes one stage and re-pauses
// at the next — exactly the ordering chained-re-pause tests depend on.
package runtime

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
)

// chainedRepauseWorkflow builds a workflow with `chainDepth` pause→resume
// cycles. The pipeline shape is:
//
//	stage:set-a      → set a = 0
//	stage:pause-1    → pause(evt:1)
//	stage:inc-1      → set a = a + 1
//	stage:pause-2    → pause(evt:2)
//	stage:inc-2      → set a = a + 1
//	… (chainDepth times)
//	stage:final      → set done = true
//
// Each Emit of "evt:N" synchronously resumes the run, which executes
// stage:inc-N and re-pauses at stage:pause-(N+1). The final Emit
// executes stage:final and the run completes.
func chainedRepauseWorkflow(chainDepth int) *pipeline.Workflow {
	stages := make([]pipeline.Stage, 0, chainDepth*2+2)

	// stage:set-a — initial state.
	stages = append(stages, pipeline.Stage{
		ID: "stage:set-a", Label: "Set A",
		Steps: map[string]pipeline.Step{
			"step:set-a": {
				ID: "step:set-a", Effect: int(pipeline.EffectSideEffecting),
				Action: func(_ context.Context, _ pipeline.PipelineContext, _ map[string]any) (store.Mutator, error) {
					return store.SetValue("a", float64(0)), nil
				},
			},
		},
	})

	// chainDepth cycles of pause → increment.
	for i := 1; i <= chainDepth; i++ {
		evtID := fmt.Sprintf("evt:%d", i)
		stageID := fmt.Sprintf("stage:pause-%d", i)
		incStageID := fmt.Sprintf("stage:inc-%d", i)
		incStepID := fmt.Sprintf("step:inc-%d", i)
		incVal := float64(i)

		// Pause stage.
		stages = append(stages, pipeline.Stage{
			ID: stageID, Label: stageID,
			Router: func(_ context.Context, _ map[string]any, _ store.Store) (pipeline.RoutingInstruction, error) {
				return pipeline.PauseForEvent(evtID, 0), nil
			},
		})

		// Increment stage.
		stages = append(stages, pipeline.Stage{
			ID: incStageID, Label: incStageID,
			Steps: map[string]pipeline.Step{
				incStepID: {
					ID: incStepID, Effect: int(pipeline.EffectSideEffecting),
					Action: func(_ context.Context, _ pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
						return func(s map[string]any) error {
							s["a"] = incVal
							return nil
						}, nil
					},
				},
			},
		})
	}

	// Final stage.
	stages = append(stages, pipeline.Stage{
		ID: "stage:final", Label: "Final",
		Steps: map[string]pipeline.Step{
			"step:final": {
				ID: "step:final", Effect: int(pipeline.EffectSideEffecting),
				Action: func(_ context.Context, _ pipeline.PipelineContext, _ map[string]any) (store.Mutator, error) {
					return store.SetValue("done", true), nil
				},
			},
		},
	})

	def := pipeline.PipelineDefinition{
		ID:     "chained-repause-pipeline",
		Label:  "Chained Re-Pause",
		Stages: stages,
	}
	return &pipeline.Workflow{
		ID:    "wf-chained-repause",
		Label: "Chained Re-Pause",
		Pipelines: map[string]pipeline.PipelineDefinition{
			"trigger:manual:Run": def,
		},
		Triggers: map[string]pipeline.WorkflowTrigger{
			"trigger:manual:Run": {ID: "trigger:manual:Run", Event: ManualEvent},
		},
	}
}

// benchChainedRepause runs one full chained re-pause cycle of the given
// depth. The setup (workflow registration, initial emit) is excluded
// from the timer; the timer covers the chainDepth sequential Emit calls
// that drive the run through each pause→resume→re-pause→…→complete
// cycle.
func benchChainedRepause(b *testing.B, chainDepth int) {
	b.Helper()
	ctx := context.Background()
	wf := chainedRepauseWorkflow(chainDepth)

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		ms := NewManualEventSource()
		done := make(chan RunResult, 1)
		rt := NewWorkflowRuntime(Options{
			ActionLog:   actionlog.NewMemoryActionLog(),
			EventSource: ms,
		})
		prepared := make(chan string, 1)
		if err := rt.Register(wf, RegisterOptions{
			Mode:       Mode{Type: "transient"},
			OnPrepare:  func(h *RunHandle) error { prepared <- h.RunID; return nil },
			OnComplete: func(r RunResult) { done <- r },
		}); err != nil {
			b.Fatalf("Register: %v", err)
		}

		// Kick off the run — it will pause at stage:pause-1.
		rt.Bus().Emit(ctx, ManualEvent, events.PipelineEvent{Payload: map[string]any{}})
		<-prepared
		awaitPausedB(b, rt, 1)
		b.StartTimer()

		// Drive the chain: each Emit resumes the run, which increments
		// and re-pauses (except the last, which finalizes).
		for j := 1; j <= chainDepth; j++ {
			ms.Emit(fmt.Sprintf("evt:%d", j), map[string]any{"n": j})
			if j < chainDepth {
				awaitPausedB(b, rt, 1)
			}
		}

		// Wait for completion.
		select {
		case res := <-done:
			if !res.OK || res.Status != "succeeded" {
				b.Fatalf("run did not succeed: %+v", res)
			}
		case <-time.After(10 * time.Second):
			b.Fatal("timed out waiting for chained re-pause run completion")
		}
		b.StopTimer()
		rt.Shutdown(ctx)
	}
}

// awaitPausedB is the benchmark variant of awaitPaused (no *testing.T).
func awaitPausedB(b *testing.B, rt *WorkflowRuntime, want int) {
	b.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		rt.mu.Lock()
		got := len(rt.paused)
		rt.mu.Unlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	b.Fatalf("expected %d paused runs, never reached", want)
}

// BenchmarkChainedRepause_Depth2 measures 2 sequential pause→resume
// cycles on a single run — the baseline.
func BenchmarkChainedRepause_Depth2(b *testing.B) {
	benchChainedRepause(b, 2)
}

// BenchmarkChainedRepause_Depth5 measures 5 sequential cycles.
// Each Emit synchronously resumes + re-pauses; the cost accumulates
// linearly in the chain depth.
func BenchmarkChainedRepause_Depth5(b *testing.B) {
	benchChainedRepause(b, 5)
}

// BenchmarkChainedRepause_Depth10 measures 10 sequential cycles —
// the stress test. A chain of 10 re-pauses pays ~10× the per-cycle
// latency in synchronous dispatch time.
func BenchmarkChainedRepause_Depth10(b *testing.B) {
	benchChainedRepause(b, 10)
}
