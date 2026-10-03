// Package replay — benchmark for Replayer.Rebuild, the event-sourced
// recovery path (pkg/replay/replayer.go:73–96).
//
// Rebuild is called synchronously inside runtime.Resume on every resume
// when a Rebuilder is configured, so its latency directly bounds resume
// throughput. The benchmark covers the three log shapes the replayer
// is designed for:
//
//   - 10 entries, 1 rerun attempt  — small run, fast resume
//   - 100 entries, 1 rerun attempt — medium run
//   - 1000 entries, 1 rerun attempt — long run, stress test
//   - 100 entries, 10 rerun attempts — multi-attempt recovery
//     (exercises allEntries' cross-attempt Seq re-baselining)
//
// The pipeline is a 5-stage definition with one effectful step per stage.
// Each effectful step has a recorded delta in the log, so the replayer
// injects the delta instead of re-executing the step — the steady-state
// recovery path.
package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
)

// benchRebuildPipeline returns a 5-stage pipeline with one effectful
// step per stage. Each stage's step is named "step-<stage>" and writes
// a key "k<stage>" with a stage-specific value.
func benchRebuildPipeline() *pipeline.PipelineDefinition {
	stages := make([]pipeline.Stage, 5)
	for i := 0; i < 5; i++ {
		stageID := fmt.Sprintf("stage-%d", i+1)
		stepID := fmt.Sprintf("step-%d", i+1)
		key := fmt.Sprintf("k%d", i+1)
		val := int64(i + 1)
		stages[i] = pipeline.Stage{
			ID:    stageID,
			Label: stageID,
			Steps: map[string]pipeline.Step{
				stepID: {
					ID:     stepID,
					Effect: 2, // SideEffecting — replayer injects the recorded delta
					Action: func(ctx context.Context, pcxt *pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
						return store.SetValue(key, val), nil
					},
				},
			},
		}
	}
	return &pipeline.PipelineDefinition{
		ID:     "bench-rebuild-pipeline",
		Label:  "Bench Rebuild",
		Stages: stages,
	}
}

// populateLog appends `entryCount` effect-completed entries to the log
// for the given runID and rerunIndex, distributed across the pipeline's
// 5 stages. Each entry's Output is a one-key state delta matching the
// step's mutator.
func populateLog(ctx context.Context, log actionlog.Store, runID string, rerunIndex, entryCount int, def *pipeline.PipelineDefinition) error {
	for i := 0; i < entryCount; i++ {
		stageIdx := i % len(def.Stages)
		stage := def.Stages[stageIdx]
		// Pick the first (and only) step in the stage.
		var stepID string
		for sID := range stage.Steps {
			stepID = sID
			break
		}
		key := fmt.Sprintf("k%d", stageIdx+1)
		delta, _ := json.Marshal(map[string]any{key: float64(stageIdx + 1)})
		if _, err := log.Append(ctx, actionlog.Entry{
			RunID:      runID,
			RerunIndex: rerunIndex,
			PipelineID: def.ID,
			Kind:       actionlog.KindEffectCompleted,
			StageID:    stage.ID,
			StepID:     stepID,
			Output:     delta,
		}); err != nil {
			return err
		}
	}
	return nil
}

// benchRebuild runs Rebuild against a pre-populated log and pipeline.
// The log setup is excluded from the timer; only Rebuild is measured.
func benchRebuild(b *testing.B, entryCount, rerunCount int) {
	b.Helper()
	ctx := context.Background()
	def := benchRebuildPipeline()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		log := actionlog.NewMemoryActionLog()
		runID := fmt.Sprintf("run-bench-%d", i)
		// Populate each rerun attempt with entryCount entries.
		for r := 0; r < rerunCount; r++ {
			if err := populateLog(ctx, log, runID, r, entryCount, def); err != nil {
				b.Fatalf("populateLog: %v", err)
			}
		}
		rp := NewReplayer(log, func(id string) (*pipeline.PipelineDefinition, bool) {
			if id == def.ID {
				return def, true
			}
			return nil, false
		})
		b.StartTimer()

		if _, _, err := rp.Rebuild(ctx, runID, def.ID); err != nil {
			b.Fatalf("Rebuild: %v", err)
		}
	}
}

// BenchmarkRebuild_10Entries_1Attempt measures Rebuild on a small log:
// 10 effect-completed entries, 1 rerun attempt.
func BenchmarkRebuild_10Entries_1Attempt(b *testing.B) {
	benchRebuild(b, 10, 1)
}

// BenchmarkRebuild_100Entries_1Attempt measures Rebuild on a medium log.
func BenchmarkRebuild_100Entries_1Attempt(b *testing.B) {
	benchRebuild(b, 100, 1)
}

// BenchmarkRebuild_1000Entries_1Attempt measures Rebuild on a long log
// — the stress test. The replayer walks every entry, partitions by
// pipeline, and applies recorded deltas in Seq order.
func BenchmarkRebuild_1000Entries_1Attempt(b *testing.B) {
	benchRebuild(b, 1000, 1)
}

// BenchmarkRebuild_100Entries_10Attempts measures Rebuild with multiple
// rerun attempts — exercises allEntries' cross-attempt Seq re-baselining
// (per-attempt counters restart at 1, so cross-attempt comparisons would
// be wrong without renumbering — #review-20260910-025).
func BenchmarkRebuild_100Entries_10Attempts(b *testing.B) {
	benchRebuild(b, 100, 10)
}
