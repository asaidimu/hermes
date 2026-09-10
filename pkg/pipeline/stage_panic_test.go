package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/stretchr/testify/require"
)

// TestStepPanicRecovered guards the fix for review-20260910-008: a panicking
// step action must surface as a normal step error (participating in retries
// and stage failure aggregation), not kill the host process.
func TestStepPanicRecovered(t *testing.T) {
	committed := false
	stage := Stage{
		ID:    "stage:panic",
		Label: "Panic Stage",
		Steps: map[string]Step{
			"step:boom": {
				ID: "step:boom",
				Action: func(ctx context.Context, pcxt PipelineContext, state map[string]any) (store.Mutator, error) {
					panic("boom: nil map write")
				},
			},
			// A healthy concurrent sibling: its mutator must NOT be committed
			// when the stage fails — the stage commit is all-or-nothing.
			"step:ok": {
				ID: "step:ok",
				Action: func(ctx context.Context, pcxt PipelineContext, state map[string]any) (store.Mutator, error) {
					return store.SetValue("ok", true), nil
				},
			},
		},
	}

	st := store.NewMemoryStore(nil)
	err := ExecuteStageSteps(
		context.Background(), "run-1", "pipe-1", stage, events.EventPath{}, st,
		events.NewMemoryScopedBus(), core.NopLogger{}, "", nil, nil, nil,
		nil, 0,
	)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "panicked"), "error should report the panic, got: %v", err)
	require.False(t, committed)

	// The stage failed atomically: the healthy sibling's mutation was not
	// committed to the store.
	var ok any
	_ = st.Read(func(state map[string]any) error { ok = state["ok"]; return nil })
	require.Nil(t, ok, "failed stage must not commit sibling mutators")

	// The process obviously survived — prove the engine still works by
	// running a healthy stage through the same entry point.
	healthy := Stage{
		ID: "stage:ok", Label: "OK Stage",
		Steps: map[string]Step{
			"step:ok": {ID: "step:ok", Action: func(ctx context.Context, pcxt PipelineContext, state map[string]any) (store.Mutator, error) {
				return store.SetValue("ok", true), nil
			}},
		},
	}
	require.NoError(t, ExecuteStageSteps(
		context.Background(), "run-1", "pipe-1", healthy, events.EventPath{}, st,
		events.NewMemoryScopedBus(), core.NopLogger{}, "", nil, nil, nil,
		nil, 0,
	))
	_ = st.Read(func(state map[string]any) error { ok = state["ok"]; return nil })
	require.Equal(t, true, ok)
}

// TestStepPanicRetries guards that a recovered panic behaves like any other
// step error for the retry loop: a step that panics on the first attempt and
// succeeds on the second is retried and completes.
func TestStepPanicRetries(t *testing.T) {
	attempts := 0
	stage := Stage{
		ID: "stage:retry", Label: "Retry Stage",
		Steps: map[string]Step{
			"step:flaky": {
				ID:      "step:flaky",
				Retries: 1,
				Action: func(ctx context.Context, pcxt PipelineContext, state map[string]any) (store.Mutator, error) {
					attempts++
					if attempts == 1 {
						panic("first attempt blows up")
					}
					return store.SetValue("done", true), nil
				},
			},
		},
	}

	st := store.NewMemoryStore(nil)
	require.NoError(t, ExecuteStageSteps(
		context.Background(), "run-1", "pipe-1", stage, events.EventPath{}, st,
		events.NewMemoryScopedBus(), core.NopLogger{}, "", nil, nil, nil,
		nil, 0,
	))
	require.Equal(t, 2, attempts, "panicking attempt must be retried")
	var done any
	_ = st.Read(func(state map[string]any) error { done = state["done"]; return nil })
	require.Equal(t, true, done)
}

// TestSubpipelinePanicRecovered guards the ExecuteSubPipelines half of
// review-20260910-008: a panic inside a child pipeline must surface as a
// failed child result (catchable by a bounded stage's PipelinesRouter), not
// kill the host process with sibling children mid-flight.
func TestSubpipelinePanicRecovered(t *testing.T) {
	stage := Stage{
		ID: "stage:children", Label: "Children",
		Pipelines: []PipelineDefinition{
			{
				ID: "child:boom", Label: "Boom Child",
				Stages: []Stage{
					{
						ID: "child:boom:stage", Label: "Boom",
						Router: func(ctx context.Context, state map[string]any, st store.Store) (RoutingInstruction, error) {
							panic("child router explosion")
						},
					},
				},
			},
			{
				ID: "child:ok", Label: "OK Child",
				Stages: []Stage{
					{
						ID: "child:ok:stage", Label: "OK",
						Steps: map[string]Step{
							"child:ok:step": {ID: "child:ok:step", Action: func(ctx context.Context, pcxt PipelineContext, state map[string]any) (store.Mutator, error) {
								return store.SetValue("ok", true), nil
							}},
						},
					},
				},
			},
		},
	}

	parent := store.NewMemoryStore(nil)
	results, err := ExecuteSubPipelines(
		context.Background(), "run-1", "parent", stage, events.EventPath{}, parent,
		events.NewMemoryScopedBus(), core.NopLogger{}, nil, nil, nil, nil,
		nil, 0,
	)
	// The panic is a per-child failure, not an infrastructural error.
	require.NoError(t, err)
	require.Len(t, results, 2)

	var boom, okChild bool
	for _, r := range results {
		if r.PipelineID == "child:boom" {
			boom = true
			require.Equal(t, "failed", r.Status)
			require.Error(t, r.Error)
			require.True(t, strings.Contains(core.CauseMessage(r.Error), "panicked"), "child error should report the panic, got: %v", r.Error)
		}
		if r.PipelineID == "child:ok" {
			okChild = true
			require.Equal(t, "succeeded", r.Status)
		}
	}
	require.True(t, boom, "panicking child must report a failed result")
	require.True(t, okChild, "sibling child must be unaffected")

	// Sanity: CauseMessage on a plain error still works (guards the helper usage above).
	require.True(t, strings.Contains(core.CauseMessage(errors.New("plain")), "plain"))
	_ = time.Now // keep time import if unused later
}
