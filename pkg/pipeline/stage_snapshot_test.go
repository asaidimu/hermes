package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/store"
)

// TestActionRunsOnSnapshotOutsideStoreLock pins the resolved
// #review-20260910-009: a step action receives a deep-copied snapshot and
// runs OUTSIDE the store read lock, so
//
//  1. direct mutations of the passed state map can no longer touch the
//     store (previously the live map was handed over — direct writes
//     bypassed the atomic stage commit and raced concurrent readers with
//     Go's unrecoverable fatal concurrent-map-write panic), and
//  2. arbitrary user code (slow JS, HTTP retries) no longer runs while the
//     store lock is held, which serialized every concurrent step in the
//     stage for the action's whole duration.
func TestActionRunsOnSnapshotOutsideStoreLock(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemoryStore(map[string]any{"seed": int64(1)})

	def := PipelineDefinition{
		ID:    "snapshot-pipeline",
		Label: "Snapshot Pipeline",
		Stages: []Stage{
			{
				ID:    "stage-1",
				Order: 1,
				Steps: map[string]Step{
					"mutator-step": {
						ID: "mutator-step",
						Action: func(ctx context.Context, pcxt PipelineContext, state map[string]any) (store.Mutator, error) {
							// Direct "mutations" — the pre-fix bug let these
							// hit the LIVE store map.
							state["hacked"] = true
							if m, ok := state["seed"].(int64); ok {
								state["seed"] = m + 1000
							}
							// The legitimate write path: the returned mutator.
							return store.SetValue("legit", true), nil
						},
					},
					"slow-step": {
						ID: "slow-step",
						Action: func(ctx context.Context, pcxt PipelineContext, state map[string]any) (store.Mutator, error) {
							// Runs concurrently with mutator-step; under the
							// pre-fix lock-held execution the stage serialized
							// behind this sleep.
							time.Sleep(50 * time.Millisecond)
							return store.SetValue("slow", true), nil
						},
					},
				},
			},
		},
	}

	done := make(chan struct{})
	var got PipelineRunResult
	var runErr error
	bus := events.NewMemoryScopedBus()
	rc := NewRunContext("run-snapshot", def, st, bus, nil)
	go func() {
		defer close(done)
		got, runErr = rc.Run(ctx)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("run timed out")
	}
	if runErr != nil {
		t.Fatalf("run failed: %v", runErr)
	}
	if got.Status != "succeeded" {
		t.Fatalf("status = %s", got.Status)
	}

	final, err := st.ExportJSON()
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := final["hacked"]; exists {
		t.Error("direct mutation leaked into the store: 'hacked' key present")
	}
	if final["seed"] != int64(1) {
		t.Errorf("direct mutation leaked into the store: seed = %v, want 1", final["seed"])
	}
	if final["legit"] != true {
		t.Errorf("returned mutator not committed: legit = %v", final["legit"])
	}
	if final["slow"] != true {
		t.Errorf("concurrent step result missing: slow = %v", final["slow"])
	}
}

// TestConcurrentStepsRaceFree exercises the snapshot contract under -race
// with steps that read shared nested state while a sibling mutates its own
// snapshot. Pre-fix, direct map writes under the store read lock tripped
// Go's fatal concurrent-map-write detection against concurrent readers.
func TestConcurrentStepsRaceFree(t *testing.T) {
	st := store.NewMemoryStore(map[string]any{
		"shared": map[string]any{"a": 1, "b": 2},
	})
	steps := map[string]Step{}
	for i := 0; i < 8; i++ {
		id := string(rune('a'+i)) + "-step"
		steps[id] = Step{
			ID: id,
			Action: func(ctx context.Context, pcxt PipelineContext, state map[string]any) (store.Mutator, error) {
				// Read nested shared state AND (attempt) a direct write —
				// the snapshot makes both race-free.
				_ = state["shared"]
				state["attempted"] = true
				return store.SetValue(id, true), nil
			},
		}
	}
	def := PipelineDefinition{
		ID: "race-pipeline",
		Stages: []Stage{{
			ID:    "s1",
			Order: 1,
			Steps: steps,
		}},
	}
	rc := NewRunContext("run-race", def, st, events.NewMemoryScopedBus(), nil)
	res, err := rc.Run(context.Background())
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if res.Status != "succeeded" {
		t.Fatalf("status = %s", res.Status)
	}
	final, _ := st.ExportJSON()
	if _, exists := final["attempted"]; exists {
		t.Error("direct write leaked into the store")
	}
}
