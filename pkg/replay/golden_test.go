package replay

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/stretchr/testify/require"
)

// Golden replay vectors (refactor plan P0.3): JSON files under
// testdata/golden/ that pin the Replayer's fold semantics as executable,
// cross-implementation reference data. Each vector scripts an entry stream
// plus a materializable definition and asserts the rebuilt state and resume
// address.
//
// The "semantics" field records which fold rules the expectations follow:
//
//      "baseline" — Go behavior at plan start: FIRST effect entry per step id,
//                   LAST route per stage (indexByStepID / indexByStage).
//      "seq-fold" — the Phase 5 (S.1) canonical: fold every entry in seq order.
//
// S.1 swaps the baseline vectors' expectations to seq-fold in the same
// commit that changes the fold — a vector failure after S.1 without a
// semantics swap is a real regression, not a spec update.

// goldenStepDef is the JSON-materializable step: effect classification plus
// the writes its Action performs (SetValue per key) when re-executed.
type goldenStepDef struct {
	ID     string         `json:"id"`
	Effect int            `json:"effect"`
	Writes map[string]any `json:"writes,omitempty"`
	// Fails, when set, makes the re-executed action return an error (used
	// only by vectors that pin failure handling).
	Fails string `json:"fails,omitempty"`
}

type goldenStageDef struct {
	ID    string          `json:"id"`
	Steps []goldenStepDef `json:"steps,omitempty"`
}

type goldenDef struct {
	ID     string           `json:"id"`
	Stages []goldenStageDef `json:"stages"`
}

type goldenVector struct {
	Name        string    `json:"name"`
	Semantics   string    `json:"semantics"`
	Description string    `json:"description"`
	Definition  goldenDef `json:"definition"`
	// Entries are actionlog.Entry JSON objects; RunID/RerunIndex/PipelineID
	// are stamped by the harness so vectors stay minimal.
	Entries []actionlog.Entry `json:"entries"`
	// ExpectedResumeAddress {"stage","step"} — empty stage means completed.
	ExpectedResumeAddress pipeline.EntryAddress `json:"expectedResumeAddress"`
	ExpectedState         map[string]any        `json:"expectedState"`
}

func materializeDefinition(gd goldenDef) *pipeline.PipelineDefinition {
	def := &pipeline.PipelineDefinition{ID: gd.ID}
	for _, gs := range gd.Stages {
		stage := pipeline.Stage{ID: gs.ID, Steps: map[string]pipeline.Step{}}
		for _, gstep := range gs.Steps {
			writes := gstep.Writes
			stepID := gstep.ID
			fails := gstep.Fails
			step := pipeline.Step{
				ID:     stepID,
				Effect: gstep.Effect,
			}
			if gstep.Effect == 1 || fails != "" {
				// Pure steps re-execute during replay: materialize a real
				// Action that performs the declared writes.
				step.Action = func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
					if fails != "" {
						return nil, &goldenActionError{msg: fails}
					}
					return func(state map[string]any) error {
						for k, v := range writes {
							state[k] = v
						}
						return nil
					}, nil
				}
			}
			stage.Steps[stepID] = step
		}
		def.Stages = append(def.Stages, stage)
	}
	return def
}

type goldenActionError struct{ msg string }

func (e *goldenActionError) Error() string { return e.msg }

func runGoldenVector(t *testing.T, dir string, name string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, name))
	require.NoError(t, err, "reading vector %s", name)

	var v goldenVector
	require.NoError(t, json.Unmarshal(data, &v), "parsing vector %s", name)
	require.Contains(t, []string{"baseline", "seq-fold"}, v.Semantics,
		"vector %s must declare known semantics", name)

	def := materializeDefinition(v.Definition)
	log := actionlog.NewMemoryActionLog()
	for _, e := range v.Entries {
		e.RunID = goldenRunID
		// RerunIndex comes from the vector entry itself (defaults to 0).
		e.PipelineID = def.ID
		_, err := log.Append(context.Background(), e)
		require.NoError(t, err, "appending entry in %s", name)
	}

	rp := NewReplayer(log, func(id string) (*pipeline.PipelineDefinition, bool) {
		if id == def.ID {
			return def, true
		}
		return nil, false
	})

	st, addr, err := rp.Rebuild(context.Background(), goldenRunID, def.ID)
	require.NoError(t, err, "rebuild for %s", name)

	require.Equal(t, v.ExpectedResumeAddress, addr, "resume address for %s", name)

	state, err := exportState(st)
	require.NoError(t, err, "exporting state for %s", name)

	// Compare only keys the vector declares — internal bookkeeping keys the
	// definition writes are out of scope for the assertion.
	require.Equal(t, sortedKeys(v.ExpectedState), sortedKeys(pick(state, keysOf(v.ExpectedState))),
		"state for %s", name)
	for k, want := range v.ExpectedState {
		require.Equal(t, want, state[k], "state[%s] for %s", k, name)
	}
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func pick(m map[string]any, keys []string) map[string]any {
	out := make(map[string]any, len(keys))
	for _, k := range keys {
		out[k] = m[k]
	}
	return out
}

func sortedKeys(m map[string]any) []string {
	out := keysOf(m)
	sort.Strings(out)
	return out
}

const goldenRunID = "golden-run"

// TestGoldenReplayVectors runs every vector in testdata/golden.
func TestGoldenReplayVectors(t *testing.T) {
	dir := filepath.Join("testdata", "golden")
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	run := false
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		run = true
		name := e.Name()
		t.Run(name, func(t *testing.T) {
			runGoldenVector(t, dir, name)
		})
	}
	require.True(t, run, "no golden vectors found under %s", dir)
}
