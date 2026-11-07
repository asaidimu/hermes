package runtime

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/compiler"
	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/stretchr/testify/require"
)

type hookEvent struct {
	name   string
	runID  string
	status string
	err    error
}

func pauseResumeNodesEdges() ([]compiler.Node, []compiler.Edge) {
	nodes := []compiler.Node{
		execNode("trigger-1", "trigger", map[string]any{"initialState": map[string]any{"x": float64(1)}}),
		execNode("pause-1", "pause", map[string]any{"waitForEvent": "go"}),
		execNode("body-1", "delay", map[string]any{"ms": float64(1)}),
		execNode("after-1", "delay", map[string]any{"ms": float64(1)}),
	}
	edges := []compiler.Edge{
		flowEdge("e1", "trigger-1", "pause-1"),
		{ID: "e2", Source: "pause-1", Target: "body-1", SourceHandle: "do", Role: compiler.EdgeFlow},
		{ID: "e3", Source: "pause-1", Target: "after-1", SourceHandle: "onResume", Role: compiler.EdgeFlow},
	}
	return nodes, edges
}

func TestLifecycleHooksPauseResumeOrder(t *testing.T) {
	rt := NewWorkflowRuntime(Options{ActionLog: actionlog.NewMemoryActionLog()})
	defer rt.Shutdown(context.Background())

	nodes, edges := pauseResumeNodesEdges()
	wf := mustCompile(t, nodes, edges)

	var mu sync.Mutex
	var order []hookEvent
	ch := make(chan struct{}, 8)
	record := func(e hookEvent) {
		mu.Lock()
		order = append(order, e)
		mu.Unlock()
		ch <- struct{}{}
	}

	paused := make(chan RunResult, 1)
	completed := make(chan RunResult, 1)

	err := rt.Register(wf, RegisterOptions{
		Mode:      Mode{Type: "transient"},
		OnPrepare: func(h *RunHandle) error { record(hookEvent{"prepare", h.RunID, "", nil}); return nil },
		OnPause: func(r RunResult) {
			record(hookEvent{"pause", r.RunID, r.Status, nil})
			paused <- r
		},
		OnResume: func(r RunResult) { record(hookEvent{"resume", r.RunID, r.Status, nil}) },
		OnComplete: func(r RunResult) {
			record(hookEvent{"complete", r.RunID, r.Status, r.Error})
			completed <- r
		},
	})
	require.NoError(t, err)

	rt.bus.Emit(context.Background(), ManualEvent, events.PipelineEvent{Payload: map[string]any{}})

	var pauseRes RunResult
	select {
	case pauseRes = <-paused:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for OnPause")
	}
	require.Equal(t, "paused", pauseRes.Status)
	require.NotEmpty(t, pauseRes.RunID)
	require.Equal(t, "__pause__", pauseRes.WaitForEvent)
	require.NotNil(t, pauseRes.Checkpoint)
	require.NotNil(t, pauseRes.FinalState)
	require.Equal(t, float64(1), pauseRes.FinalState["x"])

	<-ch // drain the pause record notification

	rt.Resume(pauseRes.RunID, map[string]any{"approver": "grace"})

	select {
	case res := <-completed:
		require.True(t, res.OK, "resumed run should succeed: %v", res.Error)
		require.Equal(t, "succeeded", res.Status)
		require.Equal(t, pauseRes.RunID, res.RunID)
		require.Equal(t, "grace", res.FinalState["approver"])
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for OnComplete after resume")
	}

	// Drain remaining record notifications (resume + complete).
	for i := 0; i < 2; i++ {
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatal("missing hook notification")
		}
	}

	mu.Lock()
	defer mu.Unlock()
	names := make([]string, 0, len(order))
	for _, e := range order {
		names = append(names, e.name)
	}
	require.Equal(t, []string{"prepare", "pause", "resume", "complete"}, names)
	require.Equal(t, "resumed", order[2].status)
	require.Equal(t, "succeeded", order[3].status)
	require.Equal(t, pauseRes.RunID, order[2].runID)
}

func TestRunReturnsPausedResultAndResumeContinues(t *testing.T) {
	rt := NewWorkflowRuntime(Options{ActionLog: actionlog.NewMemoryActionLog()})
	defer rt.Shutdown(context.Background())

	nodes, edges := pauseResumeNodesEdges()

	final := make(chan RunResult, 1)
	start := time.Now()
	res, err := rt.Run(context.Background(), nodes, edges, RunOptions{
		Timeout:    10 * time.Second,
		OnComplete: func(r RunResult) { final <- r },
	})
	require.NoError(t, err)
	require.Less(t, time.Since(start), 2*time.Second, "Run must not wait out the timeout on pause")
	require.Equal(t, "paused", res.Status)
	require.NotEmpty(t, res.RunID)
	require.NotEmpty(t, res.WorkflowID)
	require.Equal(t, "__pause__", res.WaitForEvent)

	require.True(t, rt.HasWorkflow(res.WorkflowID), "paused Run must keep the workflow registered for Resume")

	rt.Resume(res.RunID, map[string]any{"approver": "grace"})

	select {
	case term := <-final:
		require.True(t, term.OK, "resumed run should succeed: %v", term.Error)
		require.Equal(t, "succeeded", term.Status)
		require.Equal(t, res.RunID, term.RunID)
		require.Equal(t, "grace", term.FinalState["approver"])
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for terminal OnComplete after Resume")
	}

	rt.Deregister(res.WorkflowID)
	require.False(t, rt.HasWorkflow(res.WorkflowID))
}

func TestResumeFiresOnResumeOnlyAfterGatesPass(t *testing.T) {
	rt := NewWorkflowRuntime(Options{ActionLog: actionlog.NewMemoryActionLog()})
	defer rt.Shutdown(context.Background())

	nodes, edges := pauseResumeNodesEdges()
	wf := mustCompile(t, nodes, edges)

	paused := make(chan RunResult, 1)
	resumed := make(chan RunResult, 1)
	err := rt.Register(wf, RegisterOptions{
		Mode:       Mode{Type: "transient"},
		OnPause:    func(r RunResult) { paused <- r },
		OnResume:   func(r RunResult) { resumed <- r },
		OnComplete: func(r RunResult) {},
	})
	require.NoError(t, err)

	rt.bus.Emit(context.Background(), ManualEvent, events.PipelineEvent{Payload: map[string]any{}})

	var pauseRes RunResult
	select {
	case pauseRes = <-paused:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for OnPause")
	}

	bad := rt.Resume("nonexistent-run", map[string]any{"k": "v"})
	require.False(t, bad.OK)
	var se *core.SystemError
	require.ErrorAs(t, bad.Error, &se)
	require.Equal(t, core.ErrCodeNotFound, se.Code)
	select {
	case <-resumed:
		t.Fatal("OnResume must not fire for a refused resume")
	case <-time.After(200 * time.Millisecond):
	}

	rt.Resume(pauseRes.RunID, map[string]any{"k": "v"})
	select {
	case r := <-resumed:
		require.Equal(t, "resumed", r.Status)
		require.Equal(t, pauseRes.RunID, r.RunID)
		require.NotNil(t, r.Checkpoint)
	case <-time.After(5 * time.Second):
		t.Fatal("OnResume should fire for an accepted resume")
	}
}
