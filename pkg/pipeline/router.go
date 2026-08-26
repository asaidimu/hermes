package pipeline

import (
	"context"

	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/store"
)

// DefaultStepRouter advances to the next stage by default.
func DefaultStepRouter(ctx context.Context, state map[string]any, st store.Store) (RoutingInstruction, error) {
	return Advance(), nil
}

// DefaultPipelineStageRouter checks if any child failed or paused, otherwise advances.
func DefaultPipelineStageRouter(ctx context.Context, state map[string]any, results []PipelineRunResult, st store.Store) (RoutingInstruction, error) {
	for _, res := range results {
		if res.Status == "failed" || res.Status == "aborted" {
			if res.Error != nil {
				return nil, res.Error
			}
			return nil, core.NewSystemError(core.ErrCodeExecutionFailed, "subpipeline failed")
		}
		if res.Status == "paused" {
			return nil, nil // Child pause handled directly by subpipeline runner
		}
	}
	return Advance(), nil
}

// FindStageIndex locates the slice index of a stage ID in stages.
func FindStageIndex(stages []Stage, stageID string) int {
	for i, s := range stages {
		if s.ID == stageID {
			return i
		}
	}
	return -1
}
