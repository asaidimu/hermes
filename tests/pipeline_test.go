package tests

import (
	"context"
	"testing"
	"time"

	"github.com/asaidimu/go-anansi/v8/core/document"
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
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, doc *document.Document) (store.DocumentMutator, error) {
							return func(d *document.Document) error {
								return d.Set("count", int64(10))
							}, nil
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
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, doc *document.Document) (store.DocumentMutator, error) {
							return func(d *document.Document) error {
								cntRaw, _ := d.Get("count")
								var cnt int64
								if c, ok := cntRaw.(int64); ok {
									cnt = c
								} else if c, ok := cntRaw.(int); ok {
									cnt = int64(c)
								}
								return d.Set("count", cnt+5)
							}, nil
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

	val, err := res.FinalDoc.Get("count")
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
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, doc *document.Document) (store.DocumentMutator, error) {
							return func(d *document.Document) error {
								return d.Set("jumped", true)
							}, nil
						},
					},
				},
				Router: func(ctx context.Context, doc *document.Document, _ store.Store) (pipeline.RoutingInstruction, error) {
					return pipeline.Jump("finish"), nil
				},
			},
			{
				ID:    "skipped",
				Order: 2,
				Steps: map[string]pipeline.Step{
					"never": {
						ID: "never",
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, doc *document.Document) (store.DocumentMutator, error) {
							return func(d *document.Document) error {
								return d.Set("skipped_executed", true)
							}, nil
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
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, doc *document.Document) (store.DocumentMutator, error) {
							return func(d *document.Document) error {
								return d.Set("completed", true)
							}, nil
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

	skippedVal, _ := res.FinalDoc.Get("skipped_executed")
	require.Nil(t, skippedVal)

	completedVal, err := res.FinalDoc.Get("completed")
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
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, doc *document.Document) (store.DocumentMutator, error) {
							attempts++
							if attempts < 3 {
								return nil, core.NewSystemError(core.ErrCodeExecutionFailed, "temporary glitch")
							}
							return func(d *document.Document) error {
								return d.Set("success_after_retries", true)
							}, nil
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
