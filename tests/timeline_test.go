package tests

import (
	"context"
	"testing"

	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/asaidimu/hermes/pkg/timeline"
	"github.com/stretchr/testify/require"
)

func TestTimelineRecordingAndPlayback(t *testing.T) {
	ctx := context.Background()

	tStore := timeline.NewMemoryTimelineStore()

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
	factory := pipeline.NewFactory(def, nil)
	runCtx := factory.Prepare("run-timeline-1", st)

	recorder := timeline.NewTimelineRecorder("run-timeline-1", def.ID, tStore, 1)
	recorder.Attach(runCtx.EventBus(), runCtx.Store())

	res, err := runCtx.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, "succeeded", res.Status)

	// Check recorded events
	events, err := tStore.GetEvents(ctx, "run-timeline-1", 0, 0)
	require.NoError(t, err)
	require.NotEmpty(t, events)

	meta, err := tStore.GetRunMeta(ctx, "run-timeline-1")
	require.NoError(t, err)
	require.Equal(t, timeline.StatusComplete, meta.Status)
	require.Equal(t, int64(len(events)), meta.EventCount)

	// Time-travel replay
	player := timeline.NewTimelinePlayer(tStore, "run-timeline-1")
	state, err := player.Seek(ctx, meta.EventCount)
	require.NoError(t, err)
	require.NotNil(t, state)

	require.Equal(t, "done", state["status"])
}
