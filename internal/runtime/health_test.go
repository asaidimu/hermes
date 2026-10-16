package runtime

// H4 acceptance (v2.1 hardening plan): the Health() introspection snapshot.

import (
	"context"
	"testing"
	"time"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/stretchr/testify/require"
)

func TestHealthCountersAndPosture(t *testing.T) {
	ms := NewManualEventSource()
	rt := NewWorkflowRuntime(Options{
		ActionLog:        actionlog.NewMemoryActionLog(),
		EventSource:      ms,
		StoreGrace:       3 * time.Minute,
		MaxLogAge:        24 * time.Hour,
		DefinitionPolicy: "require-same",
	})
	defer rt.Shutdown(context.Background())

	h := rt.Health(context.Background())
	require.Equal(t, 0, h.ActiveRuns)
	require.True(t, h.JanitorRunning)
	require.Equal(t, 3*time.Minute, h.StoreGrace)
	require.Equal(t, 24*time.Hour, h.MaxLogAge)
	require.Equal(t, "require-same", h.DefinitionPolicy)
	require.True(t, h.FoldSnapshotsEnabled)
	require.Equal(t, "memory", h.LogBackend)
	require.NotNil(t, h.ActionLog, "memory log reports usage")

	// Drive one run to pause: active → 0, paused → 1.
	def := pipeline.PipelineDefinition{
		ID: "h-pipeline",
		Stages: []pipeline.Stage{
			{ID: "s1", Steps: map[string]pipeline.Step{
				"a": {ID: "a", Effect: int(pipeline.EffectSideEffecting), Action: func(ctx context.Context, pcxt *pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
					return store.SetValue("x", float64(1)), nil
				}},
			}},
			{ID: "wait", Router: func(ctx context.Context, state map[string]any, st store.Store) (pipeline.RoutingInstruction, error) {
				return pipeline.PauseForEvent("go", 0), nil
			}},
		},
	}
	require.NoError(t, rt.Register(&pipeline.Workflow{
		ID:        "h-wf",
		Pipelines: map[string]pipeline.PipelineDefinition{"trigger:manual:Run": def},
		Triggers:  map[string]pipeline.WorkflowTrigger{"trigger:manual:Run": {ID: "trigger:manual:Run", Event: ManualEvent}},
	}, RegisterOptions{Mode: Mode{Type: "transient"}}))
	rt.Bus().Emit(context.Background(), ManualEvent, events.PipelineEvent{Payload: map[string]any{}})
	awaitPaused(t, rt, 1)

	h = rt.Health(context.Background())
	require.Equal(t, 1, h.PausedRuns)
	require.Equal(t, 0, h.ActiveRuns)
	require.Equal(t, 1, h.CachedRunMetas)
	require.Equal(t, 1, h.LiveStores)
	require.Positive(t, h.ActionLog.Entries)
	require.Positive(t, h.Uptime)
}

func TestHealthWithoutActionLog(t *testing.T) {
	rt := NewWorkflowRuntime(Options{})
	defer rt.Shutdown(context.Background())
	h := rt.Health(context.Background())
	require.Nil(t, h.ActionLog)
	require.Empty(t, h.LogBackend)
	require.True(t, h.FoldSnapshotsEnabled)
}
