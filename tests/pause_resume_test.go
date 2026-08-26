package tests

import (
	"context"
	"testing"
	"time"

	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/registry"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/stretchr/testify/require"
)

func TestPauseAndResumeWorkflow(t *testing.T) {
	ctx := context.Background()

	def := pipeline.PipelineDefinition{
		ID:    "pausable-pipeline",
		Label: "Pausable Pipeline",
		Stages: []pipeline.Stage{
			{
				ID:    "stage-a",
				Order: 1,
				Steps: map[string]pipeline.Step{
					"step-a": {
						ID: "step-a",
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("stage_a_done", true), nil
						},
					},
				},
				Router: func(ctx context.Context, state map[string]any, _ store.Store) (pipeline.RoutingInstruction, error) {
					// Pause at stage-a and resume at stage-b
					return pipeline.Pause("stage-b", 5*time.Second), nil
				},
			},
			{
				ID:    "stage-b",
				Order: 2,
				Steps: map[string]pipeline.Step{
					"step-b": {
						ID: "step-b",
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("stage_b_done", true), nil
						},
					},
				},
			},
		},
	}

	st := store.NewMemoryStore(nil)
	factory := pipeline.NewFactory(def, nil)
	runCtx := factory.Prepare("run-pause-1", st)

	reg := registry.NewPipelineRegistry()
	_ = reg.Register(&registry.ActiveRun{
		RunID:      "run-pause-1",
		PipelineID: def.ID,
		RunContext: runCtx,
		Store:      st,
	})

	// 1. First execution pauses
	res1, err := runCtx.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, "paused", res1.Status)
	require.NotNil(t, res1.Checkpoint)
	require.Equal(t, "stage-b", res1.Checkpoint.ResumeAt.Stage)

	var valA any
	err = st.Read(func(state map[string]any) error { valA = state["stage_a_done"]; return nil })
	require.NoError(t, err)
	require.Equal(t, true, valA)

	var valB any
	_ = st.Read(func(state map[string]any) error { valB = state["stage_b_done"]; return nil })
	require.Nil(t, valB)

	// 2. Mark paused in registry
	_ = reg.MarkPaused("run-pause-1", 5*time.Second)

	// 3. Cold-Storage Resumption (simulate reloading document from store)
	resumedCtx, err := factory.Resume(ctx, "run-pause-1", st)
	require.NoError(t, err)

	res2, err := resumedCtx.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, "succeeded", res2.Status)

	var valBAfter any
	err = st.Read(func(state map[string]any) error { valBAfter = state["stage_b_done"]; return nil })
	require.NoError(t, err)
	require.Equal(t, true, valBAfter)
}
