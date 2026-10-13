package pipeline_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
)

// benchmarkStageExec measures full-run execution of a multi-stage graph
// (plan P4.5, H1 + M2): per-attempt full-state snapshots under the store
// read lock, and goroutine+WaitGroup machinery even for single-step
// stages. The state carries nested structure so the structural deep
// copy is visible.
func benchmarkStageExec(b *testing.B, stages, stepsPerStage int) {
	b.Helper()
	ctx := context.Background()

	initial := map[string]any{
		"counter": float64(0),
		"nested":  map[string]any{"a": map[string]any{"b": []any{float64(1), float64(2), map[string]any{"c": "d"}}}},
		"blob":    fmt.Sprintf("%1000d", 1), // ~1KB string, shared by the copy
	}

	stages_ := make([]pipeline.Stage, 0, stages)
	for s := 0; s < stages; s++ {
		steps := map[string]pipeline.Step{}
		for i := 0; i < stepsPerStage; i++ {
			id := fmt.Sprintf("s%d-step%d", s, i)
			steps[id] = pipeline.Step{
				ID:     id,
				Effect: int(pipeline.EffectPure),
				Action: func(ctx context.Context, pcxt *pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
					_ = state["nested"]
					return store.SetValue(pcxt.StepID(), true), nil
				},
			}
		}
		stages_ = append(stages_, pipeline.Stage{ID: fmt.Sprintf("stage-%d", s), Label: fmt.Sprintf("Stage %d", s), Steps: steps})
	}

	def := pipeline.PipelineDefinition{
		ID:     "exec-bench-pipeline",
		Label:  "Exec Bench",
		Stages: stages_,
	}

	for b.Loop() {
		st := store.NewMemoryStore(initial)
		bus := events.NewMemoryScopedBus()
		rc := pipeline.NewRunContext("exec-bench-run", def, st, bus, nil)
		res, err := rc.Run(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if res.Status != "succeeded" {
			b.Fatalf("status = %s", res.Status)
		}
	}
}

// BenchmarkStageExec_SingleStep_20x1: twenty single-step stages — the
// M2 inline path removes per-stage goroutine machinery.
func BenchmarkStageExec_SingleStep_20x1(b *testing.B) {
	benchmarkStageExec(b, 20, 1)
}

// BenchmarkStageExec_MultiStep_20x5: twenty five-step stages (100 steps)
// — the H1 snapshot path.
func BenchmarkStageExec_MultiStep_20x5(b *testing.B) {
	benchmarkStageExec(b, 20, 5)
}
