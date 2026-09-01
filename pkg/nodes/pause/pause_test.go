package pause

import (
	"context"
	"testing"
	"time"

	"github.com/asaidimu/hermes/pkg/compiler"
	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/nodes/code"
	"github.com/asaidimu/hermes/pkg/nodes/trigger"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/runtime"
	"github.com/asaidimu/hermes/pkg/timeline"
	"github.com/stretchr/testify/require"
)

func init() {
	nodekit.Register(Node)
	nodekit.Register(trigger.Node)
	nodekit.Register(code.Node)
}

func execNode(id, kind string, config map[string]any) compiler.Node {
	return compiler.Node{ID: id, Type: compiler.NodeExecutable, Kind: kind, Config: config}
}

func flowEdge(id, src, dst string) compiler.Edge {
	return compiler.Edge{ID: id, Source: src, Target: dst, Role: compiler.EdgeFlow}
}

func flowEdgeWithHandle(id, src, srcHandle, dst string) compiler.Edge {
	return compiler.Edge{ID: id, Source: src, Target: dst, SourceHandle: srcHandle, Role: compiler.EdgeFlow}
}

func mustCompile(t *testing.T, nodes []compiler.Node, edges []compiler.Edge) *pipeline.Workflow {
	t.Helper()
	wf, err := compiler.Compile(nodes, edges, nil)
	require.NoError(t, err)
	return wf
}

func awaitDone(t *testing.T, ch chan runtime.RunResult) runtime.RunResult {
	t.Helper()
	select {
	case res := <-ch:
		return res
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for run completion")
		return runtime.RunResult{}
	}
}

func TestPauseNodeRegistered(t *testing.T) {
	def, ok := nodekit.Get("pause")
	require.True(t, ok, "pause node should be registered")
	require.Equal(t, "pause", def.Kind)
	require.NotNil(t, def.Run, "pause node should have Run")
	require.NotNil(t, def.PipelinesRouterFunc, "pause node should have PipelinesRouterFunc")
}

func TestPauseNodeEndToEnd(t *testing.T) {
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{
			"initialState": map[string]any{},
		}),
		execNode("pause-1", "pause", map[string]any{
			"waitForEvent": "user:approved",
			"timeout":      float64(0),
		}),
		execNode("code-inside", "code", map[string]any{
			"code": "state.insideBody = true;",
		}),
		execNode("code-after", "code", map[string]any{
			"code": "state.afterPause = true;",
		}),
	}
	edges := []compiler.Edge{
		flowEdge("e1", "trigger-1", "pause-1"),
		flowEdgeWithHandle("e2", "pause-1", "do", "code-inside"),
		flowEdgeWithHandle("e3", "pause-1", "onResume", "code-after"),
	}

	wf := mustCompile(t, nodes, edges)

	ms := runtime.NewManualEventSource()
	done := make(chan runtime.RunResult, 1)
	rt := runtime.NewWorkflowRuntime(runtime.Options{
		Timeline:    timeline.NewMemoryTimelineStore(),
		EventSource: ms,
		Logger:      core.NopLogger{},
	})

	err := rt.Register(wf, runtime.RegisterOptions{
		Mode:       runtime.Mode{Type: "transient"},
		OnComplete: func(r runtime.RunResult) { done <- r },
	})
	require.NoError(t, err)

	// Start the run — it should execute the body, then pause.
	rt.Bus().Emit(context.Background(), "__manual__", events.PipelineEvent{
		Payload: map[string]any{},
	})

	// Resume after a short delay.
	go func() {
		time.Sleep(100 * time.Millisecond)
		ms.Emit("user:approved", map[string]any{"approved": true})
	}()

	res := awaitDone(t, done)
	require.True(t, res.OK)
	require.Equal(t, "succeeded", res.Status)
}

func TestPauseNodeTimeout(t *testing.T) {
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{
			"initialState": map[string]any{},
		}),
		execNode("pause-1", "pause", map[string]any{
			"waitForEvent": "user:approved",
			"timeout":      float64(50),
		}),
		execNode("code-inside", "code", map[string]any{
			"code": "state.insideBody = true;",
		}),
		execNode("code-after", "code", map[string]any{
			"code": "state.afterTimeout = true;",
		}),
	}
	edges := []compiler.Edge{
		flowEdge("e1", "trigger-1", "pause-1"),
		flowEdgeWithHandle("e2", "pause-1", "do", "code-inside"),
		flowEdgeWithHandle("e3", "pause-1", "onTimeout", "code-after"),
	}

	wf := mustCompile(t, nodes, edges)

	ms := runtime.NewManualEventSource()
	done := make(chan runtime.RunResult, 1)
	rt := runtime.NewWorkflowRuntime(runtime.Options{
		Timeline:    timeline.NewMemoryTimelineStore(),
		EventSource: ms,
		Logger:      core.NopLogger{},
	})

	err := rt.Register(wf, runtime.RegisterOptions{
		Mode:       runtime.Mode{Type: "transient"},
		OnComplete: func(r runtime.RunResult) { done <- r },
	})
	require.NoError(t, err)

	rt.Bus().Emit(context.Background(), "__manual__", events.PipelineEvent{
		Payload: map[string]any{},
	})

	// Don't resume — let it timeout. The pipeline should continue.
	res := awaitDone(t, done)
	require.True(t, res.OK)
	require.Equal(t, "succeeded", res.Status)
}

// ---------------------------------------------------------------------------
// Multi-event pause tests
// ---------------------------------------------------------------------------

func TestPauseMultiEventAny(t *testing.T) {
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{
			"initialState": map[string]any{},
		}),
		execNode("pause-1", "pause", map[string]any{
			"waitForEvents": []any{"approval:manager", "approval:finance"},
			"mode":          "any",
			"timeout":       float64(0),
		}),
		execNode("code-inside", "code", map[string]any{
			"code": "state.insideBody = true;",
		}),
		execNode("code-after", "code", map[string]any{
			"code": "state.received = true;",
		}),
	}
	edges := []compiler.Edge{
		flowEdge("e1", "trigger-1", "pause-1"),
		flowEdgeWithHandle("e2", "pause-1", "do", "code-inside"),
		flowEdgeWithHandle("e3", "pause-1", "onResume", "code-after"),
	}

	wf := mustCompile(t, nodes, edges)

	ms := runtime.NewManualEventSource()
	done := make(chan runtime.RunResult, 1)
	rt := runtime.NewWorkflowRuntime(runtime.Options{
		Timeline:    timeline.NewMemoryTimelineStore(),
		EventSource: ms,
		Logger:      core.NopLogger{},
	})

	err := rt.Register(wf, runtime.RegisterOptions{
		Mode:       runtime.Mode{Type: "transient"},
		OnComplete: func(r runtime.RunResult) { done <- r },
	})
	require.NoError(t, err)

	rt.Bus().Emit(context.Background(), "__manual__", events.PipelineEvent{
		Payload: map[string]any{},
	})

	time.Sleep(100 * time.Millisecond)

	// Emit one of the two events — should resume with "any" mode.
	ms.Emit("approval:manager", map[string]any{"approved": true})

	res := awaitDone(t, done)
	require.True(t, res.OK)
	require.Equal(t, "succeeded", res.Status)
}

func TestPauseMultiEventAll(t *testing.T) {
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{
			"initialState": map[string]any{},
		}),
		execNode("pause-1", "pause", map[string]any{
			"waitForEvents": []any{"approval:manager", "approval:finance"},
			"mode":          "all",
			"timeout":       float64(0),
		}),
		execNode("code-inside", "code", map[string]any{
			"code": "state.insideBody = true;",
		}),
		execNode("code-after", "code", map[string]any{
			"code": "state.received = true;",
		}),
	}
	edges := []compiler.Edge{
		flowEdge("e1", "trigger-1", "pause-1"),
		flowEdgeWithHandle("e2", "pause-1", "do", "code-inside"),
		flowEdgeWithHandle("e3", "pause-1", "onResume", "code-after"),
	}

	wf := mustCompile(t, nodes, edges)

	ms := runtime.NewManualEventSource()
	done := make(chan runtime.RunResult, 1)
	rt := runtime.NewWorkflowRuntime(runtime.Options{
		Timeline:    timeline.NewMemoryTimelineStore(),
		EventSource: ms,
		Logger:      core.NopLogger{},
	})

	err := rt.Register(wf, runtime.RegisterOptions{
		Mode:       runtime.Mode{Type: "transient"},
		OnComplete: func(r runtime.RunResult) { done <- r },
	})
	require.NoError(t, err)

	rt.Bus().Emit(context.Background(), "__manual__", events.PipelineEvent{
		Payload: map[string]any{},
	})

	time.Sleep(100 * time.Millisecond)

	// Emit first event — should NOT resume yet.
	ms.Emit("approval:manager", map[string]any{"approved": true})
	time.Sleep(100 * time.Millisecond)

	// Emit second event — should resume now.
	ms.Emit("approval:finance", map[string]any{"approved": true})

	res := awaitDone(t, done)
	require.True(t, res.OK)
	require.Equal(t, "succeeded", res.Status)
}
