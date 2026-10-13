package pipeline_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
)

// BenchmarkStageDeltaNoActionLog pins the P4.2 (H2) fix: with no action
// log configured, the per-step state-delta computation (2× deep copy +
// full diff of the state) must not run. The stage executes a step whose
// mutator touches one key of a ~1 MB state; pre-fix the delta was
// computed and discarded on every attempt.
//
// Run as: go test ./pkg/pipeline/ -bench BenchmarkStageDeltaNoActionLog
func BenchmarkStageDeltaNoActionLog_1MB(b *testing.B) {
	ctx := context.Background()

	// ~1 MB state: one large blob plus scalar keys.
	blob := strings.Repeat("x", 1<<20)
	initial := map[string]any{"blob": blob}
	for i := 0; i < 16; i++ {
		initial[fmt.Sprintf("k%d", i)] = float64(i)
	}

	def := pipeline.PipelineDefinition{
		ID:    "delta-bench-pipeline",
		Label: "Delta Bench",
		Stages: []pipeline.Stage{
			{
				ID: "stage-1", Label: "Patch",
				Steps: map[string]pipeline.Step{
					"patch-step": {
						ID:     "patch-step",
						Effect: int(pipeline.EffectSideEffecting),
						Action: func(ctx context.Context, pcxt *pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return nodekit.PatchMutator(map[string]any{"k0": float64(42)}), nil
						},
					},
				},
			},
		},
	}

	for b.Loop() {
		st := store.NewMemoryStore(initial)
		bus := events.NewMemoryScopedBus()
		rc := pipeline.NewRunContext("delta-bench-run", def, st, bus, nil)
		res, err := rc.Run(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if res.Status != "succeeded" {
			b.Fatalf("status = %s", res.Status)
		}
	}
}
