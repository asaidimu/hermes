package tests

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/stretchr/testify/require"
)

func TestUnifiedLogRecordingAndStateDerivation(t *testing.T) {
	ctx := context.Background()

	log := actionlog.NewMemoryActionLog()

	def := pipeline.PipelineDefinition{
		ID:    "timeline-pipeline",
		Label: "Timeline Pipeline",
		Stages: []pipeline.Stage{
			{
				ID: "s1",
				Steps: map[string]pipeline.Step{
					"step1": {
						ID: "step1",
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("status", "in_progress"), nil
						},
					},
				},
			},
			{
				ID: "s2",
				Steps: map[string]pipeline.Step{
					"step2": {
						ID: "step2",
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("status", "done"), nil
						},
					},
				},
			},
		},
	}

	st := store.NewMemoryStore(nil)
	factory := pipeline.NewFactory(def, nil, pipeline.FactoryOptions{ActionLog: log})
	runCtx := factory.Prepare("run-timeline-1", st)

	res, err := runCtx.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, "succeeded", res.Status)

	// The unified log records the full lifecycle.
	entries, err := log.All(ctx, "run-timeline-1", 0)
	require.NoError(t, err)
	require.NotEmpty(t, entries)

	kinds := make(map[actionlog.EntryKind]int)
	for _, e := range entries {
		kinds[e.Kind]++
	}
	require.Equal(t, 1, kinds[actionlog.KindPipelineStarted])
	require.Equal(t, 2, kinds[actionlog.KindStageStarted])
	require.Equal(t, 2, kinds[actionlog.KindStepStarted])
	require.Equal(t, 2, kinds[actionlog.KindStepCompleted])
	require.Equal(t, 1, kinds[actionlog.KindPipelineCompleted])

	// State is derivable by replaying step_completed outputs forward.
	state := make(map[string]any)
	for _, e := range entries {
		if e.Kind != actionlog.KindStepCompleted || len(e.Output) == 0 {
			continue
		}
		var delta map[string]any
		require.NoError(t, json.Unmarshal(e.Output, &delta))
		for k, v := range delta {
			state[k] = v
		}
	}
	require.Equal(t, "done", state["status"])

	// Single attempt recorded.
	idx, err := log.LatestRerunIndex(ctx, "run-timeline-1")
	require.NoError(t, err)
	require.Equal(t, 0, idx)
}
