package tests

import (
	"context"
	"testing"
	"time"

	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/stretchr/testify/require"
)

func TestSequentialPipelineExecution(t *testing.T) {
	ctx := context.Background()

	def := pipeline.PipelineDefinition{
		ID:    "test-pipeline",
		Label: "Test Pipeline",
		Stages: []pipeline.Stage{
			{
				ID:    "stage-1",
				Order: 1,
				Label: "Stage 1",
				Steps: map[string]pipeline.Step{
					"step-1": {
						ID:    "step-1",
						Label: "Step 1",
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("count", int64(10)), nil
						},
					},
				},
			},
			{
				ID:    "stage-2",
				Order: 2,
				Label: "Stage 2",
				Steps: map[string]pipeline.Step{
					"step-2": {
						ID:    "step-2",
						Label: "Step 2",
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							cur := toInt64(state["count"])
							return store.SetValue("count", cur+5), nil
						},
					},
				},
			},
		},
	}

	factory := pipeline.NewFactory(def, nil)
	runCtx := factory.Prepare("run-123", nil)

	var emittedEvents []string
	runCtx.On("*", func(ctx context.Context, evt events.PipelineEvent) error {
		emittedEvents = append(emittedEvents, evt.Type)
		return nil
	})

	res, err := runCtx.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, "succeeded", res.Status)

	val := res.FinalState["count"]
	_ = val
	require.NoError(t, err)
	require.Equal(t, int64(15), val)

	require.Contains(t, emittedEvents, "pipeline:start")
	require.Contains(t, emittedEvents, "stage:start")
	require.Contains(t, emittedEvents, "step:start")
	require.Contains(t, emittedEvents, "step:success")
	require.Contains(t, emittedEvents, "stage:success")
	require.Contains(t, emittedEvents, "pipeline:success")
}

func TestPipelineJumpRouting(t *testing.T) {
	ctx := context.Background()

	def := pipeline.PipelineDefinition{
		ID:    "jump-pipeline",
		Label: "Jump Pipeline",
		Stages: []pipeline.Stage{
			{
				ID:    "start",
				Order: 1,
				Steps: map[string]pipeline.Step{
					"init": {
						ID: "init",
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("jumped", true), nil
						},
					},
				},
				Router: func(ctx context.Context, state map[string]any, _ store.Store) (pipeline.RoutingInstruction, error) {
					return pipeline.Jump("finish"), nil
				},
			},
			{
				ID:    "skipped",
				Order: 2,
				Steps: map[string]pipeline.Step{
					"never": {
						ID: "never",
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("skipped_executed", true), nil
						},
					},
				},
			},
			{
				ID:    "finish",
				Order: 3,
				Steps: map[string]pipeline.Step{
					"done": {
						ID: "done",
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return store.SetValue("completed", true), nil
						},
					},
				},
			},
		},
	}

	factory := pipeline.NewFactory(def, nil)
	runCtx := factory.Prepare("run-jump", nil)

	res, err := runCtx.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, "succeeded", res.Status)

	skippedVal := res.FinalState["skipped_executed"]
	require.Nil(t, skippedVal)

	completedVal := res.FinalState["completed"]
	require.NoError(t, err)
	require.Equal(t, true, completedVal)

}

func TestPipelineStepRetryAndTimeout(t *testing.T) {
	ctx := context.Background()
	attempts := 0

	def := pipeline.PipelineDefinition{
		ID:    "retry-pipeline",
		Label: "Retry Pipeline",
		Stages: []pipeline.Stage{
			{
				ID: "retry-stage",
				Steps: map[string]pipeline.Step{
					"flaky-step": {
						ID:      "flaky-step",
						Retries: 2,
						Timeout: 50 * time.Millisecond,
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							attempts++
							if attempts < 3 {
								return nil, core.NewSystemError(core.ErrCodeExecutionFailed, "temporary glitch")
							}
							return store.SetValue("success_after_retries", true), nil
						},
					},
				},
			},
		},
	}

	factory := pipeline.NewFactory(def, nil)
	runCtx := factory.Prepare("run-retry", nil)

	res, err := runCtx.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, "succeeded", res.Status)
	require.Equal(t, 3, attempts)
}

func toInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	default:
		return 0
	}
}
