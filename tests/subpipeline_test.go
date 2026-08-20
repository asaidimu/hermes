package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/asaidimu/go-anansi/v8/core/document"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/stretchr/testify/require"
)

func TestConcurrentSubPipelines(t *testing.T) {
	ctx := context.Background()

	child1 := pipeline.PipelineDefinition{
		ID:    "child-1",
		Label: "Child 1",
		Stages: []pipeline.Stage{
			{
				ID: "c1-stage",
				Steps: map[string]pipeline.Step{
					"c1-step": {
						ID: "c1-step",
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, doc *document.Document) (store.DocumentMutator, error) {
							return func(d *document.Document) error {
								return d.Set("c1_done", true)
							}, nil
						},
					},
				},
			},
		},
	}

	child2 := pipeline.PipelineDefinition{
		ID:    "child-2",
		Label: "Child 2",
		Stages: []pipeline.Stage{
			{
				ID: "c2-stage",
				Steps: map[string]pipeline.Step{
					"c2-step": {
						ID: "c2-step",
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, doc *document.Document) (store.DocumentMutator, error) {
							return func(d *document.Document) error {
								return d.Set("c2_done", true)
							}, nil
						},
					},
				},
			},
		},
	}

	parent := pipeline.PipelineDefinition{
		ID:    "parent-pipeline",
		Label: "Parent Pipeline",
		Stages: []pipeline.Stage{
			{
				ID:        "subpipeline-stage",
				Label:     "Subpipelines Stage",
				Pipelines: []pipeline.PipelineDefinition{child1, child2},
				PipelinesRouter: func(ctx context.Context, doc *document.Document, results []pipeline.PipelineRunResult, _ store.Store) (pipeline.RoutingInstruction, error) {
					require.Len(t, results, 2)
					require.Equal(t, "succeeded", results[0].Status)
					require.Equal(t, "succeeded", results[1].Status)
					return pipeline.Advance(), nil
				},
			},
		},
	}

	factory := pipeline.NewFactory(parent, nil)
	runCtx := factory.Prepare("parent-run-1", nil)

	res, err := runCtx.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, "succeeded", res.Status)
}


func TestHighConcurrencySubPipelines(t *testing.T) {
	ctx := context.Background()
	const numChildren = 100

	children := make([]pipeline.PipelineDefinition, numChildren)
	for i := 0; i < numChildren; i++ {
		idx := i
		children[i] = pipeline.PipelineDefinition{
			ID:    fmt.Sprintf("stress-child-%d", idx),
			Label: fmt.Sprintf("Stress Child %d", idx),
			Stages: []pipeline.Stage{
				{
					ID: "s-stage",
					Steps: map[string]pipeline.Step{
						"s-step": {
							ID: "s-step",
							Action: func(ctx context.Context, pcxt pipeline.PipelineContext, doc *document.Document) (store.DocumentMutator, error) {
								return func(d *document.Document) error {
									return d.Set(fmt.Sprintf("key_%d", idx), idx)
								}, nil
							},
						},
					},
				},
			},
		}
	}

	parent := pipeline.PipelineDefinition{
		ID:    "stress-parent",
		Label: "Stress Parent",
		Stages: []pipeline.Stage{
			{
				ID:        "stress-stage",
				Pipelines: children,
				PipelinesRouter: func(ctx context.Context, doc *document.Document, results []pipeline.PipelineRunResult, _ store.Store) (pipeline.RoutingInstruction, error) {
					require.Len(t, results, numChildren)
					for _, r := range results {
						require.Equal(t, "succeeded", r.Status)
					}
					return pipeline.Advance(), nil
				},
			},
		},
	}

	factory := pipeline.NewFactory(parent, nil)
	runCtx := factory.Prepare("stress-run-1", nil)

	res, err := runCtx.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, "succeeded", res.Status)
}

