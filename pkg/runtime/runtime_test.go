package runtime

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/asaidimu/go-anansi/v8/core/document"
	"github.com/asaidimu/hermes/pkg/compiler"
	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/asaidimu/hermes/pkg/timeline"
	"github.com/stretchr/testify/require"

	_ "github.com/asaidimu/hermes/pkg/nodes"
)

const emptySchema = `{"version":"1.0.0","name":"test","fields":{}}`

func execNode(id, kind string, config map[string]any) compiler.Node {
	return compiler.Node{ID: id, Type: compiler.NodeExecutable, Kind: kind, Config: config}
}

func flowEdge(id, src, dst string) compiler.Edge {
	return compiler.Edge{ID: id, Source: src, Target: dst, Role: compiler.EdgeFlow}
}

func depEdge(id, src, dst string) compiler.Edge {
	return compiler.Edge{ID: id, Source: src, Target: dst, Role: compiler.EdgeDependency}
}

func mustCompile(t *testing.T, nodes []compiler.Node, edges []compiler.Edge) *pipeline.Workflow {
	t.Helper()
	wf, err := compiler.Compile(nodes, edges, nil)
	require.NoError(t, err)
	return wf
}

func awaitDone(t *testing.T, ch chan RunResult) RunResult {
	t.Helper()
	select {
	case res := <-ch:
		return res
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for run completion")
		return RunResult{}
	}
}

func TestRunCompilesAndWritesFinalState(t *testing.T) {
	rt := NewWorkflowRuntime(Options{})
	done := make(chan RunResult, 1)

	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{"initialState": map[string]any{"foo": "bar"}}),
		execNode("delay-1", "delay", map[string]any{"ms": float64(1)}),
	}
	edges := []compiler.Edge{flowEdge("e1", "trigger-1", "delay-1")}

	var runID string
	go func() {
		_, err := rt.Run(context.Background(), nodes, edges, RunOptions{
			OnPrepare: func(h *RunHandle) error {
				runID = h.RunID
				return nil
			},
			OnComplete: func(r RunResult) { done <- r },
		})
		if err != nil {
			done <- RunResult{OK: false, Error: err}
		}
	}()

	res := awaitDone(t, done)
	require.True(t, res.OK, "run should succeed: %v", res.Error)
	require.Equal(t, "succeeded", res.Status)
	require.Equal(t, "bar", res.FinalState["foo"])
	// Trigger initialState must not spread into numeric keys ([object Object] repro).
	for k := range res.FinalState {
		require.NotRegexp(t, `^\d+$`, k, "state must not contain numeric keys from spread of a stringified object")
	}
	require.NotEmpty(t, runID)
	outcome, ok := rt.GetRunOutcome(runID)
	require.True(t, ok)
	require.Equal(t, "succeeded", outcome.Status)
}

func TestDuplicateRegisterFails(t *testing.T) {
	rt := NewWorkflowRuntime(Options{})
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{"initialState": map[string]any{}}),
		execNode("delay-1", "delay", map[string]any{"ms": float64(1)}),
	}
	edges := []compiler.Edge{flowEdge("e1", "trigger-1", "delay-1")}

	wf := mustCompile(t, nodes, edges)
	require.NoError(t, rt.Register(wf, RegisterOptions{}))
	err := rt.Register(wf, RegisterOptions{})
	require.Error(t, err)
	se := core.SystemErrorFrom(err)
	require.Equal(t, core.ErrCodeConflict, se.Code)
}

func TestDeregisterStopsDispatch(t *testing.T) {
	rt := NewWorkflowRuntime(Options{})
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{"initialState": map[string]any{}}),
		execNode("delay-1", "delay", map[string]any{"ms": float64(1)}),
	}
	edges := []compiler.Edge{flowEdge("e1", "trigger-1", "delay-1")}

	wf := mustCompile(t, nodes, edges)
	completed := make(chan struct{}, 1)
	require.NoError(t, rt.Register(wf, RegisterOptions{
		OnComplete: func(RunResult) { completed <- struct{}{} },
	}))

	rt.Deregister(wf.ID)
	require.False(t, rt.HasWorkflow(wf.ID))

	rt.Bus().Emit(context.Background(), ManualEvent, events.PipelineEvent{Payload: map[string]any{}})

	select {
	case <-completed:
		t.Fatal("onComplete should not fire after deregister")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestAbortRun(t *testing.T) {
	rt := NewWorkflowRuntime(Options{})
	done := make(chan RunResult, 1)

	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{"initialState": map[string]any{}}),
		execNode("delay-1", "delay", map[string]any{"ms": float64(300)}),
	}
	edges := []compiler.Edge{flowEdge("e1", "trigger-1", "delay-1")}

	var handle *RunHandle
	prepared := make(chan struct{}, 1)
	require.NoError(t, rt.Register(mustCompile(t, nodes, edges), RegisterOptions{
		OnPrepare: func(h *RunHandle) error {
			handle = h
			prepared <- struct{}{}
			return nil
		},
		OnComplete: func(r RunResult) { done <- r },
	}))

	rt.Bus().Emit(context.Background(), ManualEvent, events.PipelineEvent{Payload: map[string]any{}})

	select {
	case <-prepared:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for onPrepare")
	}
	require.NotNil(t, handle)
	handle.Abort(core.NewSystemError(core.ErrCodeAbort, "user abort"))

	res := awaitDone(t, done)
	require.False(t, res.OK)
	require.Equal(t, "aborted", res.Status)
}

func TestTimelineRecordsStepFailure(t *testing.T) {
	// Register a node kind that always throws, mirroring the TS test's
	// "test-generic-error" node.
	nodekit.Register(nodekit.NodeDefinition{
		Kind:         "test-generic-error",
		Label:        "Test Generic Error",
		ConfigSchema: json.RawMessage(emptySchema),
		Type:         "executable",
		Handles: func(config map[string]any) []nodekit.HandleSpec {
			return []nodekit.HandleSpec{{Type: nodekit.HandleTarget, ID: ""}, {Type: nodekit.HandleSource, ID: ""}}
		},
		Run: func(ctx context.Context, nCtx nodekit.NodeRunContext) (store.DocumentMutator, error) {
			return nil, core.NewSystemError(core.ErrCodeExecutionFailed, "generic error message")
		},
	})

	ts := timeline.NewMemoryTimelineStore()
	rt := NewWorkflowRuntime(Options{Timeline: ts})
	done := make(chan struct{}, 1)
	var runID string

	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{"initialState": map[string]any{}}),
		execNode("error-1", "test-generic-error", map[string]any{}),
	}
	edges := []compiler.Edge{flowEdge("e1", "trigger-1", "error-1")}

	require.NoError(t, rt.Register(mustCompile(t, nodes, edges), RegisterOptions{
		OnPrepare: func(h *RunHandle) error {
			runID = h.RunID
			return nil
		},
		OnComplete: func(RunResult) { done <- struct{}{} },
	}))

	rt.Bus().Emit(context.Background(), ManualEvent, events.PipelineEvent{Payload: map[string]any{}})

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for run completion")
	}

	require.NotEmpty(t, runID)
	evts, err := ts.GetEvents(context.Background(), runID, 0, 0)
	require.NoError(t, err)

	var found bool
	for _, ev := range evts {
		if ev.Type != "step:failure" {
			continue
		}
		found = true
		errObj, ok := ev.Payload["error"].(map[string]any)
		require.True(t, ok, "step:failure must carry an error payload")
		require.Contains(t, errObj["message"], "generic error message")
		_, hasCode := errObj["code"]
		require.True(t, hasCode, "step:failure error must carry a code")
		_, hasStack := errObj["stack"]
		require.True(t, hasStack, "step:failure error must carry a stack field")
	}
	require.True(t, found, "timeline should contain a step:failure event")
}

func TestResourceResolverInjection(t *testing.T) {
	// Resource node kind: initializes a handle at run scope.
	nodekit.Register(nodekit.NodeDefinition{
		Kind:         "dbref",
		Label:        "DB Ref",
		ConfigSchema: json.RawMessage(emptySchema),
		Type:         "resource",
		Handles: func(config map[string]any) []nodekit.HandleSpec {
			return []nodekit.HandleSpec{{Type: nodekit.HandleSource, ID: ""}}
		},
		ResourceInit: func(ctx context.Context, nCtx nodekit.NodeRunContext) (any, error) {
			return "db://live", nil
		},
		ResourceEnd: func(ctx context.Context, nCtx nodekit.NodeRunContext, handle any) error {
			return nil
		},
	})

	// Consumer node: references the resource via interpolation.
	nodekit.Register(nodekit.NodeDefinition{
		Kind:         "consumer",
		Label:        "Consumer",
		ConfigSchema: json.RawMessage(`{"version":"1.0.0","name":"consumer","fields":{"conn":{"name":"conn","type":"string","required":true}}}`),
		Type:         "executable",
		Handles: func(config map[string]any) []nodekit.HandleSpec {
			return []nodekit.HandleSpec{{Type: nodekit.HandleTarget, ID: ""}, {Type: nodekit.HandleSource, ID: ""}}
		},
		Run: func(ctx context.Context, nCtx nodekit.NodeRunContext) (store.DocumentMutator, error) {
			return nodekit.PatchMutator(map[string]any{"conn": nCtx.Config["conn"]}), nil
		},
	})

	rt := NewWorkflowRuntime(Options{})
	done := make(chan struct{}, 1)
	var runID string

	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{"initialState": map[string]any{}}),
		{ID: "res-1", Type: compiler.NodeResource, Kind: "dbref", Config: map[string]any{}},
		execNode("consumer-1", "consumer", map[string]any{"conn": "{{ $res.dbref }}"}),
	}
	edges := []compiler.Edge{
		flowEdge("e1", "trigger-1", "consumer-1"),
		depEdge("e2", "res-1", "consumer-1"),
	}

	go func() {
		_, err := rt.Run(context.Background(), nodes, edges, RunOptions{
			OnPrepare: func(h *RunHandle) error {
				runID = h.RunID
				return nil
			},
			OnComplete: func(r RunResult) { done <- struct{}{} },
		})
		require.NoError(t, err)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for run completion")
	}

	require.NotEmpty(t, runID)
	outcome, ok := rt.GetRunOutcome(runID)
	require.True(t, ok)
	require.Equal(t, "succeeded", outcome.Status)
	require.Equal(t, "db://live", outcome.FinalState["conn"])
}

func TestResourceLifecycleEvents(t *testing.T) {
	nodekit.Register(nodekit.NodeDefinition{
		Kind:         "liferes",
		Label:        "Life Res",
		ConfigSchema: json.RawMessage(emptySchema),
		Type:         "resource",
		Handles: func(config map[string]any) []nodekit.HandleSpec {
			return []nodekit.HandleSpec{{Type: nodekit.HandleSource, ID: ""}}
		},
		ResourceInit: func(ctx context.Context, nCtx nodekit.NodeRunContext) (any, error) {
			return "handle-1", nil
		},
		ResourceEnd: func(ctx context.Context, nCtx nodekit.NodeRunContext, handle any) error {
			return nil
		},
	})
	nodekit.Register(nodekit.NodeDefinition{
		Kind:         "liferes-consumer",
		Label:        "Life Res Consumer",
		ConfigSchema: json.RawMessage(emptySchema),
		Type:         "executable",
		Handles: func(config map[string]any) []nodekit.HandleSpec {
			return []nodekit.HandleSpec{{Type: nodekit.HandleTarget, ID: ""}, {Type: nodekit.HandleSource, ID: ""}}
		},
		Run: func(ctx context.Context, nCtx nodekit.NodeRunContext) (store.DocumentMutator, error) {
			return nodekit.PatchMutator(map[string]any{"consumed": true}), nil
		},
	})

	rt := NewWorkflowRuntime(Options{})
	var mu sync.Mutex
	var seen []events.PipelineEvent
	unsub := rt.Bus().Subscribe("*", func(_ context.Context, evt events.PipelineEvent) error {
		mu.Lock()
		seen = append(seen, evt)
		mu.Unlock()
		return nil
	})
	defer unsub()

	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{"initialState": map[string]any{}}),
		{ID: "res-1", Type: compiler.NodeResource, Kind: "liferes", Config: map[string]any{}},
		execNode("consumer-1", "liferes-consumer", map[string]any{}),
	}
	edges := []compiler.Edge{
		flowEdge("e1", "trigger-1", "consumer-1"),
		depEdge("e2", "res-1", "consumer-1"),
	}

	res, err := rt.Run(context.Background(), nodes, edges)
	require.NoError(t, err)
	require.Equal(t, "succeeded", res.Status)

	mu.Lock()
	defer mu.Unlock()

	byType := map[string][]events.PipelineEvent{}
	for _, ev := range seen {
		byType[ev.Type] = append(byType[ev.Type], ev)
	}

	require.Len(t, byType["resource:init"], 1)
	require.Len(t, byType["resource:ready"], 1)
	require.Len(t, byType["resource:cleanup"], 1)

	for _, typ := range []string{"resource:init", "resource:ready", "resource:cleanup"} {
		ev := byType[typ][0]
		require.Equal(t, "resource:res-1", ev.Payload["resourceId"])
		require.Equal(t, "liferes", ev.Payload["resourceKind"])
		require.Equal(t, "Life Res", ev.Payload["resourceLabel"])
	}
}

// ---------------------------------------------------------------------------
// Pause/Resume tests
// ---------------------------------------------------------------------------

func TestPauseResumeEventSource(t *testing.T) {
	// Create a pipeline that pauses waiting for an event, then resumes.
	// We construct the pipeline definition directly since there's no
	// "pause" node kind registered.
	pauseStageID := "stage:pause:Pause"
	codeStageID := "stage:code:Run Code"

	def := pipeline.PipelineDefinition{
		ID:    "pause-resume-test",
		Label: "Pause Resume Test",
		Stages: []pipeline.Stage{
			{
				ID:    pauseStageID,
				Label: "Pause",
				Steps: map[string]pipeline.Step{},
				Router: func(ctx context.Context, doc *document.Document, st store.Store) (pipeline.RoutingInstruction, error) {
					return pipeline.PauseForEvent("user:approved", 0), nil
				},
			},
			{
				ID:    codeStageID,
				Label: "Run Code",
				Steps: map[string]pipeline.Step{
					"step:code:Run Code": {
						ID:    "step:code:Run Code",
						Label: "Run Code",
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state *document.Document) (store.DocumentMutator, error) {
							state.Set("total", float64(10))
							state.Set("resumed", true)
							return nil, nil
						},
					},
				},
				Router: func(ctx context.Context, doc *document.Document, st store.Store) (pipeline.RoutingInstruction, error) {
					return pipeline.Advance(), nil
				},
			},
		},
	}

	wf := &pipeline.Workflow{
		ID:    "wf-pause-test",
		Label: "Pause Test",
		Pipelines: map[string]pipeline.PipelineDefinition{
			"trigger:manual:Run": def,
		},
		Triggers: map[string]pipeline.WorkflowTrigger{
			"trigger:manual:Run": {
				ID:    "trigger:manual:Run",
				Event: ManualEvent,
			},
		},
	}

	ms := NewManualEventSource()

	done := make(chan RunResult, 1)
	rt := NewWorkflowRuntime(Options{
		Timeline:    timeline.NewMemoryTimelineStore(),
		EventSource: ms,
	})

	err := rt.Register(wf, RegisterOptions{
		Mode:       Mode{Type: "transient"},
		OnComplete: func(r RunResult) { done <- r },
	})
	require.NoError(t, err)

	// Emit the manual trigger to start the run.
	rt.Bus().Emit(context.Background(), ManualEvent, events.PipelineEvent{
		Payload: map[string]any{},
	})

	// Give the pipeline time to start and pause.
	time.Sleep(100 * time.Millisecond)

	// The run should be paused waiting for "user:approved".
	rt.mu.Lock()
	pausedCount := len(rt.paused)
	rt.mu.Unlock()
	require.Equal(t, 1, pausedCount, "expected 1 paused run")

	// Emit the resume event via the ManualEventSource.
	ms.Emit("user:approved", map[string]any{"approved": true})

	res := awaitDone(t, done)
	require.True(t, res.OK)
	require.Equal(t, "succeeded", res.Status)
}

func TestResumeWithPayload(t *testing.T) {
	pauseStageID := "stage:pause:Pause"
	codeStageID := "stage:code:Run Code"

	def := pipeline.PipelineDefinition{
		ID:    "resume-payload-test",
		Label: "Resume Payload Test",
		Stages: []pipeline.Stage{
			{
				ID:    pauseStageID,
				Label: "Pause",
				Steps: map[string]pipeline.Step{},
				Router: func(ctx context.Context, doc *document.Document, st store.Store) (pipeline.RoutingInstruction, error) {
					return pipeline.PauseForEvent("data:arrived", 0), nil
				},
			},
			{
				ID:    codeStageID,
				Label: "Run Code",
				Steps: map[string]pipeline.Step{
					"step:code:Run Code": {
						ID:    "step:code:Run Code",
						Label: "Run Code",
						Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state *document.Document) (store.DocumentMutator, error) {
							// The resume payload should be available in state.
							state.Set("fromPayload", "hello from event")
							return nil, nil
						},
					},
				},
				Router: func(ctx context.Context, doc *document.Document, st store.Store) (pipeline.RoutingInstruction, error) {
					return pipeline.Advance(), nil
				},
			},
		},
	}

	wf := &pipeline.Workflow{
		ID:    "wf-resume-payload-test",
		Label: "Resume Payload Test",
		Pipelines: map[string]pipeline.PipelineDefinition{
			"trigger:manual:Run": def,
		},
		Triggers: map[string]pipeline.WorkflowTrigger{
			"trigger:manual:Run": {
				ID:    "trigger:manual:Run",
				Event: ManualEvent,
			},
		},
	}

	ms := NewManualEventSource()

	done := make(chan RunResult, 1)
	rt := NewWorkflowRuntime(Options{
		Timeline:    timeline.NewMemoryTimelineStore(),
		EventSource: ms,
	})

	err := rt.Register(wf, RegisterOptions{
		Mode:       Mode{Type: "transient"},
		OnComplete: func(r RunResult) { done <- r },
	})
	require.NoError(t, err)

	rt.Bus().Emit(context.Background(), ManualEvent, events.PipelineEvent{Payload: map[string]any{}})
	time.Sleep(100 * time.Millisecond)

	// Resume with a payload.
	ms.Emit("data:arrived", map[string]any{"value": "hello from event"})

	res := awaitDone(t, done)
	require.Equal(t, "succeeded", res.Status)
}

func TestShutdownEventSource(t *testing.T) {
	ms := NewManualEventSource()
	rt := NewWorkflowRuntime(Options{
		EventSource: ms,
	})

	err := rt.Shutdown(context.Background())
	require.NoError(t, err)
	require.True(t, ms.ShutdownCalled)
}

// ---------------------------------------------------------------------------
// Custom event trigger tests
// ---------------------------------------------------------------------------

func TestCustomEventTrigger(t *testing.T) {
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{
			"event":        "order:created",
			"initialState": map[string]any{},
		}),
		execNode("code-1", "code", map[string]any{
			"code": "state.received = true;",
		}),
	}
	edges := []compiler.Edge{flowEdge("e1", "trigger-1", "code-1")}

	wf := mustCompile(t, nodes, edges)

	// Verify the trigger event is "order:created", not "__manual__"
	trigger, ok := wf.Triggers["trigger-1"]
	require.True(t, ok)
	require.Equal(t, "order:created", trigger.Event)

	done := make(chan RunResult, 1)
	rt := NewWorkflowRuntime(Options{
		Timeline: timeline.NewMemoryTimelineStore(),
	})

	err := rt.Register(wf, RegisterOptions{
		Mode:       Mode{Type: "transient"},
		OnComplete: func(r RunResult) { done <- r },
	})
	require.NoError(t, err)

	// Emit the custom event — not __manual__
	rt.Bus().Emit(context.Background(), "order:created", events.PipelineEvent{
		Payload: map[string]any{"orderId": "12345"},
	})

	res := awaitDone(t, done)
	require.True(t, res.OK)
	require.Equal(t, "succeeded", res.Status)
}

func TestCustomEventTriggerWithPayload(t *testing.T) {
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{
			"event":        "user:registered",
			"initialState": map[string]any{},
		}),
		execNode("code-1", "code", map[string]any{
			"code": "state.received = (state.userName === 'alice' && state.email === 'alice@example.com');",
		}),
	}
	edges := []compiler.Edge{flowEdge("e1", "trigger-1", "code-1")}

	wf := mustCompile(t, nodes, edges)

	done := make(chan RunResult, 1)
	rt := NewWorkflowRuntime(Options{
		Timeline: timeline.NewMemoryTimelineStore(),
	})

	err := rt.Register(wf, RegisterOptions{
		Mode:       Mode{Type: "transient"},
		OnComplete: func(r RunResult) { done <- r },
	})
	require.NoError(t, err)

	// Emit with payload — keys fold into state
	rt.Bus().Emit(context.Background(), "user:registered", events.PipelineEvent{
		Payload: map[string]any{"userName": "alice", "email": "alice@example.com"},
	})

	res := awaitDone(t, done)
	require.True(t, res.OK)
	require.Equal(t, "succeeded", res.Status)
}

func TestDefaultEventTrigger(t *testing.T) {
	// Trigger without "event" field should default to __manual__
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{
			"initialState": map[string]any{},
		}),
		execNode("code-1", "code", map[string]any{
			"code": "state.ok = true;",
		}),
	}
	edges := []compiler.Edge{flowEdge("e1", "trigger-1", "code-1")}

	wf := mustCompile(t, nodes, edges)

	trigger, ok := wf.Triggers["trigger-1"]
	require.True(t, ok)
	require.Equal(t, "__manual__", trigger.Event)
}

func TestDelayCronPauseResume(t *testing.T) {
	// Delay node with cron should pause and auto-resume after the cron delay.
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{
			"initialState": map[string]any{},
		}),
		execNode("delay-1", "delay", map[string]any{
			"cron": "@every 100ms",
		}),
		execNode("code-1", "code", map[string]any{
			"code": "return { done: true };",
		}),
	}
	edges := []compiler.Edge{
		flowEdge("e1", "trigger-1", "delay-1"),
		flowEdge("e2", "delay-1", "code-1"),
	}

	wf := mustCompile(t, nodes, edges)
	done := make(chan RunResult, 1)

	rt := NewWorkflowRuntime(Options{})
	defer rt.Shutdown(context.Background())

	err := rt.Register(wf, RegisterOptions{
		OnComplete: func(r RunResult) { done <- r },
	})
	require.NoError(t, err)

	// Emit trigger — should pause at delay, then auto-resume via cron
	rt.Bus().Emit(context.Background(), "__manual__", events.PipelineEvent{
		Payload: map[string]any{},
	})

	res := awaitDone(t, done)
	require.True(t, res.OK)
	require.Equal(t, "succeeded", res.Status)
	require.Equal(t, true, res.FinalState["done"])
}