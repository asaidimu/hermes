package tests

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/stretchr/testify/require"
)

// collectEvents subscribes to "*" on a root bus and returns a thread-safe
// collector that snapshots every emitted event (child events bubble up).
func collectEvents(t *testing.T, bus events.ScopedEventBus) func() []events.PipelineEvent {
	t.Helper()
	var mu sync.Mutex
	var seen []events.PipelineEvent
	unsub := bus.Subscribe("*", func(_ context.Context, evt events.PipelineEvent) error {
		mu.Lock()
		seen = append(seen, evt)
		mu.Unlock()
		return nil
	})
	t.Cleanup(unsub)
	return func() []events.PipelineEvent {
		mu.Lock()
		defer mu.Unlock()
		out := make([]events.PipelineEvent, len(seen))
		copy(out, seen)
		return out
	}
}

func eventsOf(evts []events.PipelineEvent, typ string) []events.PipelineEvent {
	var out []events.PipelineEvent
	for _, e := range evts {
		if e.Type == typ {
			out = append(out, e)
		}
	}
	return out
}

func stepStage(id, label string) pipeline.Stage {
	return pipeline.Stage{
		ID:    id,
		Label: label,
		Steps: map[string]pipeline.Step{
			"step-" + id: {
				ID: "step-" + id,
				Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
					return store.SetValue("touched", id), nil
				},
			},
		},
	}
}

func TestEventWireStepsMode(t *testing.T) {
	ctx := context.Background()
	def := pipeline.PipelineDefinition{
		ID:     "wire-pipe",
		Label:  "Wire Pipe",
		Stages: []pipeline.Stage{stepStage("s1", "Stage 1"), stepStage("s2", "Stage 2")},
	}

	bus := events.NewMemoryScopedBus()
	snapshot := collectEvents(t, bus)
	factory := pipeline.NewFactory(def, nil)
	runCtx := factory.Prepare("run-wire-steps", nil, bus)

	res, err := runCtx.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, "succeeded", res.Status)

	evts := snapshot()

	starts := eventsOf(evts, "stage:start")
	require.Len(t, starts, 2)
	for _, s := range starts {
		require.Equal(t, "run-wire-steps", s.RunID)
		require.Equal(t, "steps", s.Payload["mode"])
		require.EqualValues(t, 1, s.Payload["stepCount"])
	}

	successes := eventsOf(evts, "stage:success")
	require.Len(t, successes, 2)
	for _, s := range successes {
		_, hasNext := s.Payload["nextInstruction"]
		require.True(t, hasNext, "stage:success must carry nextInstruction")
	}

	evals := eventsOf(evts, "router:evaluated")
	require.Len(t, evals, 2)
	require.Equal(t, "advance", evals[0].Payload["interpretation"])
	require.Equal(t, "natural-end", evals[1].Payload["interpretation"])
	for i, e := range evals {
		require.Equal(t, []string{"s1", "s2"}[i], e.Payload["stageId"])
		require.Nil(t, e.Payload["instruction"])
	}
}

func TestEventWireSubPipelines(t *testing.T) {
	ctx := context.Background()
	child1 := pipeline.PipelineDefinition{
		ID:     "wire-child-1",
		Label:  "Wire Child 1",
		Stages: []pipeline.Stage{stepStage("wc1", "WC1")},
	}
	child2 := pipeline.PipelineDefinition{
		ID:     "wire-child-2",
		Label:  "Wire Child 2",
		Stages: []pipeline.Stage{stepStage("wc2", "WC2")},
	}
	parent := pipeline.PipelineDefinition{
		ID:    "wire-parent",
		Label: "Wire Parent",
		Stages: []pipeline.Stage{
			{
				ID:        "wire-sub-stage",
				Label:     "Wire Sub Stage",
				Pipelines: []pipeline.PipelineDefinition{child1, child2},
				PipelinesRouter: func(ctx context.Context, state map[string]any, results []pipeline.PipelineRunResult, _ store.Store) (pipeline.RoutingInstruction, error) {
					require.Len(t, results, 2)
					return pipeline.Advance(), nil
				},
			},
		},
	}

	bus := events.NewMemoryScopedBus()
	snapshot := collectEvents(t, bus)
	factory := pipeline.NewFactory(parent, nil)
	runCtx := factory.Prepare("run-wire-sub", nil, bus)

	res, err := runCtx.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, "succeeded", res.Status)

	evts := snapshot()

	// Parent stage runs in pipelines mode.
	starts := eventsOf(evts, "stage:start")
	require.Len(t, starts, 3) // parent + 2 children
	parentStart := starts[0]
	require.Equal(t, "pipelines", parentStart.Payload["mode"])
	require.EqualValues(t, 2, parentStart.Payload["subPipelineCount"])

	// subpipeline:fork carries child pipeline ids.
	forks := eventsOf(evts, "subpipeline:fork")
	require.Len(t, forks, 1)
	ids, ok := forks[0].Payload["subPipelineIds"].([]string)
	require.True(t, ok)
	require.ElementsMatch(t, []string{"wire-child-1", "wire-child-2"}, ids)

	// subpipeline:join carries per-child results.
	joins := eventsOf(evts, "subpipeline:join")
	require.Len(t, joins, 1)
	results, ok := joins[0].Payload["results"].(map[string]any)
	require.True(t, ok)
	for _, id := range ids {
		entry, ok := results[id].(map[string]any)
		require.True(t, ok)
		require.Equal(t, "succeeded", entry["status"])
		require.Equal(t, true, entry["ok"])
	}

	// Child events bubble under the parent runId.
	for _, e := range evts {
		require.Equal(t, "run-wire-sub", e.RunID)
	}
}

func TestEventWireSubPipelineSoftFailure(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("child exploded")
	child := pipeline.PipelineDefinition{
		ID:    "wire-fail-child",
		Label: "Wire Fail Child",
		Stages: []pipeline.Stage{
			{
				ID:    "wire-fail-stage",
				Label: "Wire Fail Stage",
				Steps: map[string]pipeline.Step{
					"wire-fail-step": {
						ID: "wire-fail-step",
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return nil, core.NewSystemError(core.ErrCodeExecutionFailed, boom.Error()).WithCause(boom)
						},
					},
				},
			},
		},
	}
	parent := pipeline.PipelineDefinition{
		ID:    "wire-soft-parent",
		Label: "Wire Soft Parent",
		Stages: []pipeline.Stage{
			{
				ID:        "wire-soft-stage",
				Label:     "Soft Stage",
				Pipelines: []pipeline.PipelineDefinition{child},
				PipelinesRouter: func(ctx context.Context, state map[string]any, results []pipeline.PipelineRunResult, _ store.Store) (pipeline.RoutingInstruction, error) {
					require.Len(t, results, 1)
					require.Equal(t, "failed", results[0].Status)
					require.NotNil(t, results[0].Error)
					return pipeline.Advance(), nil // recover: try-catch "try" leg
				},
			},
		},
	}

	bus := events.NewMemoryScopedBus()
	snapshot := collectEvents(t, bus)
	factory := pipeline.NewFactory(parent, nil)
	runCtx := factory.Prepare("run-wire-soft", nil, bus)

	res, err := runCtx.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, "succeeded", res.Status)

	evts := snapshot()

	// The child's failure is captured, not propagated.
	joins := eventsOf(evts, "subpipeline:join")
	require.Len(t, joins, 1)
	results, ok := joins[0].Payload["results"].(map[string]any)
	require.True(t, ok)
	entry, ok := results["wire-fail-child"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "failed", entry["status"])
	require.Equal(t, false, entry["ok"])
	errObj, ok := entry["error"].(map[string]any)
	require.True(t, ok)
	require.NotEmpty(t, errObj["message"])
	require.NotEmpty(t, errObj["code"])

	// The child failed, but the parent router swallowed it: no parent stage:failure.
	for _, sf := range eventsOf(evts, "stage:failure") {
		require.NotEqual(t, "wire-soft-parent", sf.PipelineID)
	}

	// Child emitted its own failure under the parent runId.
	childFails := eventsOf(evts, "pipeline:failure")
	require.Len(t, childFails, 1)
	require.Equal(t, "run-wire-soft", childFails[0].RunID)
	errObj, ok = childFails[0].Payload["error"].(map[string]any)
	require.True(t, ok)
	require.Contains(t, errObj["message"], "child exploded")
	require.Contains(t, errObj["message"], "Wire Fail Stage:wire-fail-stage")
}

func TestEventWireSubPipelineHardFailure(t *testing.T) {
	ctx := context.Background()
	child := pipeline.PipelineDefinition{
		ID:    "wire-hard-child",
		Label: "Wire Hard Child",
		Stages: []pipeline.Stage{
			{
				ID:    "wire-hard-stage",
				Label: "Hard Stage",
				Steps: map[string]pipeline.Step{
					"wire-hard-step": {
						ID: "wire-hard-step",
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
							return nil, core.NewSystemError(core.ErrCodeExecutionFailed, "hard failure")
						},
					},
				},
			},
		},
	}
	parent := pipeline.PipelineDefinition{
		ID:    "wire-hard-parent",
		Label: "Wire Hard Parent",
		Stages: []pipeline.Stage{
			{
				ID:        "wire-hard-stage",
				Label:     "Hard Stage",
				Pipelines: []pipeline.PipelineDefinition{child},
			},
		},
	}

	bus := events.NewMemoryScopedBus()
	snapshot := collectEvents(t, bus)
	factory := pipeline.NewFactory(parent, nil)
	runCtx := factory.Prepare("run-wire-hard", nil, bus)

	res, err := runCtx.Run(ctx)
	require.Error(t, err)
	require.Equal(t, "failed", res.Status)

	evts := snapshot()

	// DefaultPipelineStageRouter propagates the child failure: parent stage fails.
	stageFails := eventsOf(evts, "stage:failure")
	require.Len(t, stageFails, 2) // child stage:failure + parent stage:failure
	parentFail := stageFails[0]
	if parentFail.PipelineID != "wire-hard-parent" {
		parentFail = stageFails[1]
	}
	require.Equal(t, "wire-hard-parent", parentFail.PipelineID)
	require.Equal(t, "wire-hard-stage", parentFail.Payload["stageId"])
	errObj, ok := parentFail.Payload["error"].(map[string]any)
	require.True(t, ok)
	require.Contains(t, errObj["message"], "hard failure")
	require.Contains(t, errObj["message"], "Hard Stage:wire-hard-stage")
}
