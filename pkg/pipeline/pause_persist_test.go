package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/store"
)

// failingUpdateStore wraps a MemoryStore and fails every Update once armed.
// Used to prove pause-checkpoint persistence failures are no longer
// discarded (#review-20260910-012).
type failingUpdateStore struct {
	store.Store
	armed bool
}

func (f *failingUpdateStore) Update(ctx context.Context, mutator store.Mutator) error {
	if f.armed {
		return errors.New("injected disk failure")
	}
	return f.Store.Update(ctx, mutator)
}

// TestPauseFailsWhenCheckpointPersistFails pins the resolved
// #review-20260910-012: a Persist=true pause whose checkpoint write fails
// must FAIL THE PAUSE (surface the error) instead of reporting "paused" with
// nothing durable recording where to resume — the legacy checkpoint resume
// path would otherwise dead-end ("no checkpoint found in document").
func TestPauseFailsWhenCheckpointPersistFails(t *testing.T) {
	ctx := context.Background()

	inner := store.NewMemoryStore(map[string]any{})
	st := &failingUpdateStore{Store: inner}
	// Arm the failure only after the run is underway (the trigger/seed
	// writes must succeed; the pause checkpoint write must not).
	defer func() { st.armed = false }()

	def := PipelineDefinition{
		ID:    "persist-fail-pipeline",
		Label: "Persist Fail Pipeline",
		Stages: []Stage{
			{
				ID:    "pause-stage",
				Order: 1,
				Steps: map[string]Step{
					"noop": {
						ID:     "noop",
						Effect: 1, // pure — re-executed on replay, no log needed
						Action: func(ctx context.Context, pcxt PipelineContext, state map[string]any) (store.Mutator, error) {
							st.armed = true
							return nil, nil
						},
					},
				},
				Router: func(ctx context.Context, state map[string]any, st store.Store) (RoutingInstruction, error) {
					return Pause("resume-here", 0), nil
				},
			},
			{ID: "resume-here", Order: 2},
		},
	}

	rc := NewRunContext("run-persist-fail", def, st, events.NewMemoryScopedBus(), nil)
	res, runErr := rc.Run(ctx)

	if runErr == nil {
		t.Fatalf("expected the pause to fail when checkpoint persistence fails, got status=%s", res.Status)
	}
	if res.Status == "paused" {
		t.Fatal("run reported 'paused' despite a failed checkpoint write — the paused-run contract is broken")
	}
	if res.Status != "failed" {
		t.Fatalf("status = %s, want failed", res.Status)
	}
	if res.Error == nil {
		t.Fatal("error not surfaced in the run result")
	}
	if !strings.Contains(runErr.Error(), "failed to persist pause checkpoint") {
		t.Errorf("error should name the checkpoint persistence failure, got: %v", runErr)
	}
}
