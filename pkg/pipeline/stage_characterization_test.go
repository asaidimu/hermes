package pipeline

import (
	"context"
	"strings"
	"testing"

	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/stretchr/testify/require"
)

// TestConcurrentSiblingsDoNotSeeEachOthersMutations pins the isolation
// invariant the P4.5 stage-snapshot change depends on: concurrent steps in
// one stage execute against snapshots taken before any sibling's mutator
// commits — a sibling's mutation is never visible mid-stage, regardless of
// interleaving. Mutators land atomically at stage end, so every step's view
// is "state as of stage entry". P4.5 (one snapshot per stage) must preserve
// exactly this visibility; the action below fails the stage if a sibling's
// write ever shows up in another step's view.
func TestConcurrentSiblingsDoNotSeeEachOthersMutations(t *testing.T) {
	const siblings = 6

	steps := map[string]Step{}
	for i := 0; i < siblings; i++ {
		letter := string(rune('a' + i))
		id := letter + "-step"
		steps[id] = Step{
			ID: id,
			Action: func(ctx context.Context, pcxt PipelineContext, state map[string]any) (store.Mutator, error) {
				// Any sibling's write ("<letter>-wrote" for a different
				// letter) visible in this snapshot is an isolation breach.
				for k := range state {
					if strings.HasSuffix(k, "-wrote") && !strings.HasPrefix(k, letter+"-") {
						return nil, errLeak
					}
				}
				return store.SetValue(letter+"-wrote", true), nil
			},
		}
	}

	def := PipelineDefinition{
		ID:    "sibling-isolation",
		Label: "Sibling Isolation",
		Stages: []Stage{{
			ID:    "stage-1",
			Steps: steps,
		}},
	}

	// Repeat to shake out goroutine ordering; every attempt must isolate.
	for attempt := 0; attempt < 20; attempt++ {
		st := store.NewMemoryStore(nil)
		rc := NewRunContext("run-siblings", def, st, events.NewMemoryScopedBus(), nil)
		res, err := rc.Run(context.Background())
		require.NoError(t, err, "attempt %d", attempt)
		require.Equal(t, "succeeded", res.Status, "attempt %d", attempt)
		final, _ := st.ExportJSON()
		// All sibling writes committed atomically at stage end.
		for i := 0; i < siblings; i++ {
			require.Equal(t, true, final[string(rune('a'+i))+"-wrote"], "attempt %d", attempt)
		}
	}
}

// TestStageCommitIsAtomic pins the all-or-nothing commit shape P4.5 keeps:
// every sibling's mutator applies inside ONE store.Update at stage end —
// a reader that observes the first sibling's key observes them all.
func TestStageCommitIsAtomic(t *testing.T) {
	const siblings = 4
	steps := map[string]Step{}
	for i := 0; i < siblings; i++ {
		letter := string(rune('a' + i))
		steps[letter+"-step"] = Step{
			ID: letter + "-step",
			Action: func(ctx context.Context, pcxt PipelineContext, state map[string]any) (store.Mutator, error) {
				return store.SetValue(letter+"-wrote", true), nil
			},
		}
	}
	def := PipelineDefinition{
		ID:     "atomic-commit",
		Label:  "Atomic Commit",
		Stages: []Stage{{ID: "stage-1", Steps: steps}},
	}
	st := store.NewMemoryStore(nil)
	rc := NewRunContext("run-atomic", def, st, events.NewMemoryScopedBus(), nil)
	res, err := rc.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, "succeeded", res.Status)

	seen := 0
	require.NoError(t, st.Read(func(state map[string]any) error {
		for i := 0; i < siblings; i++ {
			if v, ok := state[string(rune('a'+i))+"-wrote"]; ok && v == true {
				seen++
			}
		}
		return nil
	}))
	require.Equal(t, siblings, seen, "all sibling mutators committed")
}

type leakError struct{}

func (*leakError) Error() string { return "sibling mutation leaked into another step's snapshot" }

var errLeak = &leakError{}
