package pipeline

import (
	"testing"
	"time"
)

// fpDef builds a definition for fingerprint tests. Steps live in a map, so
// two instances of the "same" definition are always distinct objects — the
// fingerprint must still agree.
func fpDef() *PipelineDefinition {
	return &PipelineDefinition{
		ID: "root",
		Stages: []Stage{
			{
				ID:      "stage-1",
				Timeout: 5 * time.Second,
				Config:  map[string]any{"expr": "a > 1", "limit": float64(10)},
				Steps: map[string]Step{
					"step-a": {ID: "step-a", Timeout: time.Second, Retries: 2, Effect: int(EffectPure)},
					"step-b": {ID: "step-b", Retries: 0, Effect: int(EffectSideEffecting)},
				},
			},
			{
				ID: "stage-2",
				Steps: map[string]Step{
					"step-c": {ID: "step-c", Effect: int(EffectPure)},
				},
				Pipelines: []PipelineDefinition{
					{ID: "child",
						Stages: []Stage{
							{ID: "child-stage", Config: map[string]any{"k": "v"}},
						}},
				},
			},
		},
	}
}

func TestFingerprintStableAcrossInstances(t *testing.T) {
	a := Fingerprint(fpDef())
	b := Fingerprint(fpDef())
	if a == "" || len(a) != 32 {
		t.Fatalf("fingerprint should be 32 hex chars, got %q", a)
	}
	if a != b {
		t.Fatalf("identical structure must fingerprint identically: %s vs %s", a, b)
	}
	if again := Fingerprint(fpDef()); again != a {
		t.Fatalf("third instance drifted: %s", again)
	}
}

func TestFingerprintIgnoresLabels(t *testing.T) {
	base := Fingerprint(fpDef())
	def := fpDef()
	def.Label = "renamed root"
	def.Stages[0].Label = "renamed stage"
	s := def.Stages[0].Steps["step-a"]
	s.Label = "renamed step"
	def.Stages[0].Steps["step-a"] = s
	if Fingerprint(def) != base {
		t.Fatal("label-only changes must not change the fingerprint")
	}
}

func TestFingerprintDetectsDrift(t *testing.T) {
	base := Fingerprint(fpDef())
	cases := map[string]func(*PipelineDefinition){
		"stage reorder": func(d *PipelineDefinition) { d.Stages[0], d.Stages[1] = d.Stages[1], d.Stages[0] },
		"stage added":   func(d *PipelineDefinition) { d.Stages = append(d.Stages, Stage{ID: "stage-3"}) },
		"stage id":      func(d *PipelineDefinition) { d.Stages[0].ID = "stage-1b" },
		"stage timeout": func(d *PipelineDefinition) { d.Stages[0].Timeout = time.Hour },
		"stage config": func(d *PipelineDefinition) {
			d.Stages[0].Config = map[string]any{"expr": "a > 2", "limit": float64(10)}
		},
		"step added":   func(d *PipelineDefinition) { d.Stages[0].Steps["step-x"] = Step{ID: "step-x", Effect: int(EffectPure)} },
		"step removed": func(d *PipelineDefinition) { delete(d.Stages[0].Steps, "step-b") },
		"step effect class": func(d *PipelineDefinition) {
			s := d.Stages[0].Steps["step-b"]
			s.Effect = int(EffectPure)
			d.Stages[0].Steps["step-b"] = s
		},
		"step retries": func(d *PipelineDefinition) {
			s := d.Stages[0].Steps["step-a"]
			s.Retries = 9
			d.Stages[0].Steps["step-a"] = s
		},
		"step timeout": func(d *PipelineDefinition) {
			s := d.Stages[0].Steps["step-a"]
			s.Timeout = time.Minute
			d.Stages[0].Steps["step-a"] = s
		},
		"pipeline id": func(d *PipelineDefinition) { d.ID = "root-renamed" },
		"nested def change": func(d *PipelineDefinition) {
			d.Stages[1].Pipelines[0].Stages[0].Config = map[string]any{"k": "changed"}
		},
		"nested def added": func(d *PipelineDefinition) {
			d.Stages[1].Pipelines = append(d.Stages[1].Pipelines, PipelineDefinition{ID: "child-2"})
		},
		"nested def removed": func(d *PipelineDefinition) { d.Stages[1].Pipelines = nil },
	}
	for name, mutate := range cases {
		def := fpDef()
		mutate(def)
		if got := Fingerprint(def); got == base {
			t.Errorf("%s: fingerprint did not change", name)
		}
	}
}

func TestFingerprintConfigTypes(t *testing.T) {
	// Value-type changes in config must register; deeply equal configs must
	// agree regardless of map construction order.
	a := Fingerprint(&PipelineDefinition{ID: "p", Stages: []Stage{{ID: "s", Config: map[string]any{
		"b": []any{float64(1), "x", true},
		"a": map[string]any{"y": float64(2)},
	}}}})
	b := Fingerprint(&PipelineDefinition{ID: "p", Stages: []Stage{{ID: "s", Config: map[string]any{
		"a": map[string]any{"y": float64(2)},
		"b": []any{float64(1), "x", true},
	}}}})
	if a != b {
		t.Fatal("config map construction order must not matter")
	}
	c := Fingerprint(&PipelineDefinition{ID: "p", Stages: []Stage{{ID: "s", Config: map[string]any{
		"b": []any{float64(1), "x", false},
		"a": map[string]any{"y": float64(2)},
	}}}})
	if c == a {
		t.Fatal("config value change must change the fingerprint")
	}
	// Array order is semantic.
	d := Fingerprint(&PipelineDefinition{ID: "p", Stages: []Stage{{ID: "s", Config: map[string]any{
		"b": []any{"x", float64(1), true},
		"a": map[string]any{"y": float64(2)},
	}}}})
	if d == a {
		t.Fatal("array reorder must change the fingerprint")
	}
}

func TestFingerprintNilDef(t *testing.T) {
	if Fingerprint(nil) != "nil" {
		t.Fatalf("nil def should use the sentinel, got %q", Fingerprint(nil))
	}
	if Fingerprint(nil) == Fingerprint(&PipelineDefinition{}) {
		t.Fatal("nil sentinel must differ from an empty definition")
	}
}
