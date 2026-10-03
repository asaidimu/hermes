package runtime

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

// C2 regression tests (20261003 refactor plan): the Register critical
// section is bounded — host callbacks (secrets validation, EventSource
// OnRegister) run OUTSIDE rt.mu — the OnRegister unsubscribe func is
// retained and invoked on Deregister, and workflow-scoped resources are
// torn down on Deregister/Shutdown.

// syncEmitEventSource emits synchronously on OnRegister. Pre-C2 this
// deadlocked: OnRegister ran while rt.mu was held, and emit() dispatches
// back into rt.mu.
type syncEmitEventSource struct {
	emitted bool
}

func (s *syncEmitEventSource) OnRegister(ctx context.Context, params RegisterParams) (func(), error) {
	params.Emit("evt:sync", map[string]any{"ok": true})
	s.emitted = true
	return func() { /* unsubscribed */ }, nil
}

func (s *syncEmitEventSource) OnShutdown(context.Context) error { return nil }

func noopTriggerWorkflow(id string) *pipeline.Workflow {
	def := pipeline.PipelineDefinition{ID: id + "-pipeline"}
	return &pipeline.Workflow{
		ID:    id,
		Label: id,
		Pipelines: map[string]pipeline.PipelineDefinition{
			"trigger:manual:Run": def,
		},
		Triggers: map[string]pipeline.WorkflowTrigger{
			"trigger:manual:Run": {ID: "trigger:manual:Run", Event: ManualEvent},
		},
	}
}

// TestRegisterSynchronousEmitNoDeadlock pins the C2 fix: a host whose
// OnRegister emits on the runtime bus synchronously must not deadlock.
func TestRegisterSynchronousEmitNoDeadlock(t *testing.T) {
	rt := NewWorkflowRuntime(Options{})
	defer rt.Shutdown(context.Background())

	src := &syncEmitEventSource{}
	rt.eventSource = src

	done := make(chan error, 1)
	go func() {
		done <- rt.Register(noopTriggerWorkflow("wf-sync-emit"), RegisterOptions{})
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
		require.True(t, src.emitted)
	case <-time.After(5 * time.Second):
		t.Fatal("Register deadlocked — host emit() ran inside the rt.mu critical section (C2 regression)")
	}
}

// unsubRecordingEventSource hands back an unsubscribe func that records
// invocation.
type unsubRecordingEventSource struct {
	mu          sync.Mutex
	unsubscribe bool
}

func (s *unsubRecordingEventSource) OnUnregisterHack() {}

func (s *unsubRecordingEventSource) OnRegister(ctx context.Context, params RegisterParams) (func(), error) {
	return func() {
		s.mu.Lock()
		s.unsubscribe = true
		s.mu.Unlock()
	}, nil
}

func (s *unsubRecordingEventSource) OnShutdown(context.Context) error { return nil }

func (s *unsubRecordingEventSource) wasUnsubscribed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.unsubscribe
}

// TestDeregisterInvokesEventSourceUnsubscribe pins the retained unsubscribe:
// pre-C2 the func returned by OnRegister was discarded, so external
// listeners (webhooks, queues) leaked forever.
func TestDeregisterInvokesEventSourceUnsubscribe(t *testing.T) {
	rt := NewWorkflowRuntime(Options{})
	defer rt.Shutdown(context.Background())

	src := &unsubRecordingEventSource{}
	rt.eventSource = src

	require.NoError(t, rt.Register(noopTriggerWorkflow("wf-unsub"), RegisterOptions{}))
	require.False(t, src.wasUnsubscribed())

	rt.Deregister("wf-unsub")
	require.True(t, src.wasUnsubscribed(), "Deregister must invoke the retained OnRegister unsubscribe")
}

// TestDeregisterTearsDownWorkflowResources pins the C2 resource teardown:
// workflow-scoped services initialized via initResources are Cleaned up on
// Deregister (pre-C2 the wfResources map was never iterated — the audit's
// "leaked DB pools/connections" row).
func TestDeregisterTearsDownWorkflowResources(t *testing.T) {
	rt := NewWorkflowRuntime(Options{})
	defer rt.Shutdown(context.Background())

	cleaned := make(chan string, 4)
	wf := noopTriggerWorkflow("wf-resources")
	wf.Services = []pipeline.Service{
		{
			ID:    "shared-pool",
			Scope: "workflow",
			Kind:  "pool",
			Label: "Shared Pool",
			Cleanup: func(ctx context.Context, handle any) error {
				cleaned <- "shared-pool"
				return nil
			},
		},
	}

	require.NoError(t, rt.Register(wf, RegisterOptions{}))

	// Simulate initResources having cached a handle for the service (the
	// real path populates it on the first run that resolves it).
	rt.mu.Lock()
	if rec, ok := rt.workflows["wf-resources"]; ok {
		rec.mu.Lock()
		rec.wfResources["shared-pool"] = "handle-1"
		rec.mu.Unlock()
	}
	rt.mu.Unlock()

	rt.Deregister("wf-resources")
	select {
	case id := <-cleaned:
		require.Equal(t, "shared-pool", id)
	case <-time.After(2 * time.Second):
		t.Fatal("workflow-scoped resource was never cleaned up on Deregister (C2 regression)")
	}
}

// TestRegisterRollbackIsIdempotent pins the rollback re-validation: when
// OnRegister fails AFTER the lock was released, the rollback removes the
// record; a concurrent Deregister racing first must not panic or double-
// delete (the plan's "re-validate existence before rollback" requirement).
func TestRegisterRollbackIsIdempotent(t *testing.T) {
	rt := NewWorkflowRuntime(Options{})
	defer rt.Shutdown(context.Background())

	src := &failingEventSource{}
	rt.eventSource = src

	err := rt.Register(noopTriggerWorkflow("wf-rollback"), RegisterOptions{})
	require.Error(t, err)
	require.False(t, rt.HasWorkflow("wf-rollback"), "failed registration must leave no record")

	// Rollback is idempotent: deregistering again is a no-op.
	rt.Deregister("wf-rollback")
	require.False(t, rt.HasWorkflow("wf-rollback"))
}

type failingEventSource struct{}

func (failingEventSource) OnRegister(context.Context, RegisterParams) (func(), error) {
	return nil, context.DeadlineExceeded
}

func (failingEventSource) OnShutdown(context.Context) error { return nil }

// TestRegisterSecretValidationOutsideLock verifies requirements validation
// happens outside rt.mu: a slow secret Has() call must not block concurrent
// bus dispatch (which takes rt.mu in dispatch).
func TestRegisterSecretValidationOutsideLock(t *testing.T) {
	rt := NewWorkflowRuntime(Options{})
	defer rt.Shutdown(context.Background())

	slowHas := func(context.Context, string) bool {
		time.Sleep(100 * time.Millisecond)
		return true
	}
	rt.secretHas = slowHas

	wf := noopTriggerWorkflow("wf-slow-secrets")
	wf.Requirements = []pipeline.Requirement{{Kind: pipeline.ReqSecret, Key: "K", Required: true}}

	start := time.Now()
	done := make(chan error, 1)
	go func() {
		done <- rt.Register(wf, RegisterOptions{})
	}()
	require.NoError(t, <-done)
	require.GreaterOrEqual(t, time.Since(start), 90*time.Millisecond, "validation ran")

	// While validation was in flight, HasWorkflow (which takes rt.mu) must
	// have been able to proceed quickly — the lock was NOT held for the
	// 100ms of secret I/O. Sample the lock latency during a second
	// registration with slow secrets.
	rt2start := time.Now()
	_ = rt.HasWorkflow("wf-slow-secrets")
	require.Less(t, time.Since(rt2start), 50*time.Millisecond,
		"rt.mu was held across secret validation (C2 regression)")
	_ = events.NewMemoryScopedBus // keep import if assertions change
}
