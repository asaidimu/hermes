package nodekit

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/asaidimu/hermes/pkg/store"
)

// --- Test config structs ---

type SimpleConfig struct {
	Name  string `config:"name" anansi:"required=true"`
	Count int    `config:"count" anansi:"default=1"`
}

type NestedConfig struct {
	Outer struct {
		Inner string `config:"inner" anansi:"required=true"`
	} `config:"outer"`
}

type OptionalConfig struct {
	Label string `config:"label"`
}

// --- Helpers ---

func schemaFieldNames(raw json.RawMessage) []string {
	var schema struct {
		Fields map[string]struct {
			Name string `json:"name"`
		} `json:"fields"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		return nil
	}
	var names []string
	for _, f := range schema.Fields {
		names = append(names, f.Name)
	}
	return names
}

func mustDefine[C any](t *testing.T, def TypedDefinition[C]) NodeDefinition {
	t.Helper()
	nd := Define(def)
	if nd.ConfigSchema == nil {
		t.Fatal("Define produced nil ConfigSchema")
	}
	return nd
}

// --- Tests ---

func TestDefineSchemaDerivation(t *testing.T) {
	nd := mustDefine(t, TypedDefinition[SimpleConfig]{
		Kind:  "test-simple",
		Label: "Simple",
	})

	// Schema should contain fields from the struct.
	names := schemaFieldNames(nd.ConfigSchema)
	if len(names) == 0 {
		t.Fatal("schema has no fields")
	}
	for _, want := range []string{"name", "count"} {
		found := false
		for _, n := range names {
			if n == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("schema missing field %q; got %v", want, names)
		}
	}
}

func TestDefineErasedFields(t *testing.T) {
	nd := Define(TypedDefinition[NestedConfig]{
		Kind:        "test-nested",
		Label:       "Nested",
		Description: "desc",
		Icon:        "icon",
		Type:        "executable",
		Scope:       "pipeline",
		BodyHandle:  "body-out",
		HandlesJS:   `() => []`,
	})
	if nd.Kind != "test-nested" {
		t.Errorf("Kind = %q", nd.Kind)
	}
	if nd.Label != "Nested" {
		t.Errorf("Label = %q", nd.Label)
	}
	if nd.Description != "desc" {
		t.Errorf("Description = %q", nd.Description)
	}
	if nd.Icon != "icon" {
		t.Errorf("Icon = %q", nd.Icon)
	}
	if nd.Type != "executable" {
		t.Errorf("Type = %q", nd.Type)
	}
	if nd.Scope != "pipeline" {
		t.Errorf("Scope = %q", nd.Scope)
	}
	if nd.BodyHandle != "body-out" {
		t.Errorf("BodyHandle = %q", nd.BodyHandle)
	}
	if nd.HandlesJS != `() => []` {
		t.Errorf("HandlesJS = %q", nd.HandlesJS)
	}
}

func TestDefineRunReceivesTypedConfig(t *testing.T) {
	var got *SimpleConfig
	nd := Define(TypedDefinition[SimpleConfig]{
		Kind: "test-run",
		Run: func(ctx context.Context, nCtx *TypedRunContext[SimpleConfig]) (store.Mutator, error) {
			got = nCtx.Config
			return store.SetValue("out", nCtx.Config.Name), nil
		},
	})

	nCtx := NodeRunContext{
		NodeID: "n1",
		Config: map[string]any{"name": "hello", "count": 5},
		State:  map[string]any{},
	}
	_, err := nd.Run(context.Background(), nCtx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got == nil {
		t.Fatal("Config was nil")
	}
	if got.Name != "hello" {
		t.Errorf("Name = %q, want %q", got.Name, "hello")
	}
	if got.Count != 5 {
		t.Errorf("Count = %d, want 5", got.Count)
	}
}

func TestDefineRouterReceivesTypedConfig(t *testing.T) {
	var got *SimpleConfig
	nd := Define(TypedDefinition[SimpleConfig]{
		Kind: "test-router",
		Router: func(ctx context.Context, nCtx *TypedRunContext[SimpleConfig]) (string, error) {
			got = nCtx.Config
			return nCtx.Config.Name, nil
		},
	})

	nCtx := NodeRunContext{
		NodeID: "n1",
		Config: map[string]any{"name": "branch-a"},
	}
	handle, err := nd.Router(context.Background(), nCtx)
	if err != nil {
		t.Fatalf("Router: %v", err)
	}
	if got == nil {
		t.Fatal("Config was nil")
	}
	if handle != "branch-a" {
		t.Errorf("handle = %q, want %q", handle, "branch-a")
	}
}

func TestDefineHandlesReceivesTypedConfig(t *testing.T) {
	nd := Define(TypedDefinition[OptionalConfig]{
		Kind: "test-handles",
		Handles: func(cfg *OptionalConfig) []HandleSpec {
			if cfg.Label == "" {
				return []HandleSpec{{Type: HandleTarget, ID: ""}}
			}
			return []HandleSpec{
				{Type: HandleTarget, ID: ""},
				{Type: HandleSource, ID: cfg.Label, Label: cfg.Label},
			}
		},
	})

	// Empty label → 1 handle.
	specs := nd.Handles(map[string]any{})
	if len(specs) != 1 {
		t.Errorf("empty label: got %d handles, want 1", len(specs))
	}

	// With label → 2 handles.
	specs = nd.Handles(map[string]any{"label": "out"})
	if len(specs) != 2 {
		t.Errorf("with label: got %d handles, want 2", len(specs))
	}
	if specs[1].ID != "out" {
		t.Errorf("handle ID = %q, want %q", specs[1].ID, "out")
	}
}

func TestDefineCustomValidation(t *testing.T) {
	nd := Define(TypedDefinition[SimpleConfig]{
		Kind: "test-vc",
		ValidateConfig: func(cfg *SimpleConfig) error {
			if cfg.Count < 0 {
				return errors.New("count must be non-negative")
			}
			return nil
		},
	})

	// Valid config.
	if err := nd.ValidateConfig(map[string]any{"name": "x", "count": 1}); err != nil {
		t.Errorf("valid config: %v", err)
	}

	// Invalid: negative count.
	if err := nd.ValidateConfig(map[string]any{"name": "x", "count": -1}); err == nil {
		t.Error("expected validation error for negative count")
	}
}

func TestDefineBuiltinValidation(t *testing.T) {
	nd := Define(TypedDefinition[SimpleConfig]{
		Kind: "test-required",
	})

	// Missing required field "name" → validation error.
	if err := nd.ValidateConfig(map[string]any{"count": 1}); err == nil {
		t.Error("expected validation error for missing required field")
	}

	// With required field.
	if err := nd.ValidateConfig(map[string]any{"name": "ok"}); err != nil {
		t.Errorf("valid config: %v", err)
	}
}

func TestDefineSchemaRoundTrip(t *testing.T) {
	nd := Define(TypedDefinition[SimpleConfig]{
		Kind: "test-rt",
	})

	// Schema JSON should be valid and parseable.
	var parsed map[string]any
	if err := json.Unmarshal(nd.ConfigSchema, &parsed); err != nil {
		t.Fatalf("schema not valid JSON: %v", err)
	}
	if parsed["name"] == nil && parsed["fields"] == nil {
		t.Errorf("schema doesn't contain expected fields: %v", parsed)
	}
}

func TestDefineRequirementPassThrough(t *testing.T) {
	nd := Define(TypedDefinition[SimpleConfig]{
		Kind: "test-reqs",
		Requirements: []Requirement{
			{Kind: ReqEnv, Key: "MY_KEY", Required: true},
			{Kind: ReqSecret, Key: "MY_SECRET", Required: true},
		},
	})
	if len(nd.Requirements) != 2 {
		t.Fatalf("Requirements: got %d, want 2", len(nd.Requirements))
	}
	if nd.Requirements[0].Key != "MY_KEY" {
		t.Errorf("Requirements[0].Key = %q", nd.Requirements[0].Key)
	}
}

func TestDefineEmptyConfigStruct(t *testing.T) {
	type EmptyConfig struct{}
	nd := Define(TypedDefinition[EmptyConfig]{
		Kind: "test-empty",
		Run: func(ctx context.Context, nCtx *TypedRunContext[EmptyConfig]) (store.Mutator, error) {
			return store.SetValue("ok", true), nil
		},
	})
	if nd.ConfigSchema == nil {
		t.Fatal("schema is nil for empty config struct")
	}

	// Run with nil config map — should not panic.
	_, err := nd.Run(context.Background(), NodeRunContext{
		NodeID: "n1",
		Config: nil,
		State:  map[string]any{},
	})
	if err != nil {
		t.Errorf("Run with nil config: %v", err)
	}
}

func TestDefineContextPassedThrough(t *testing.T) {
	type Cfg struct {
		Value string `config:"value"`
	}
	nd := Define(TypedDefinition[Cfg]{
		Kind: "test-ctx",
		Run: func(ctx context.Context, nCtx *TypedRunContext[Cfg]) (store.Mutator, error) {
			// Verify context and surrounding state are available.
			if nCtx.NodeID != "ctx-node" {
				t.Errorf("NodeID = %q, want %q", nCtx.NodeID, "ctx-node")
			}
			if nCtx.Config.Value != "x" {
				t.Errorf("Config.Value = %q, want %q", nCtx.Config.Value, "x")
			}
			if nCtx.State["foo"] != "bar" {
				t.Errorf("State[foo] = %v", nCtx.State["foo"])
			}
			return nil, nil
		},
	})

	_, err := nd.Run(context.Background(), NodeRunContext{
		NodeID: "ctx-node",
		Config: map[string]any{"value": "x"},
		State:  map[string]any{"foo": "bar"},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestDefineDynamicHandles(t *testing.T) {
	type SwitchConfig struct {
		Cases          string `config:"cases"`
		DefaultHandle  string `config:"defaultHandle"`
	}

	nd := Define(TypedDefinition[SwitchConfig]{
		Kind: "test-switch",
		Handles: func(cfg *SwitchConfig) []HandleSpec {
			specs := []HandleSpec{{Type: HandleTarget, ID: ""}}
			if cfg.Cases != "" {
				var cases []struct {
					ID    string `json:"id"`
					Label string `json:"label"`
				}
				if err := json.Unmarshal([]byte(cfg.Cases), &cases); err == nil {
					for _, c := range cases {
						specs = append(specs, HandleSpec{
							Type:  HandleSource,
							ID:    c.ID,
							Label: c.Label,
						})
					}
				}
			}
			if cfg.DefaultHandle != "" {
				specs = append(specs, HandleSpec{
					Type:  HandleSource,
					ID:    cfg.DefaultHandle,
					Label: "default",
				})
			}
			return specs
		},
	})

	// No cases → just target.
	specs := nd.Handles(map[string]any{})
	if len(specs) != 1 {
		t.Errorf("empty cases: got %d, want 1", len(specs))
	}

	// With cases.
	casesJSON := `[{"id":"a","label":"Case A"},{"id":"b","label":"Case B"}]`
	specs = nd.Handles(map[string]any{"cases": casesJSON, "defaultHandle": "def"})
	if len(specs) != 4 {
		t.Errorf("with cases: got %d, want 4", len(specs))
	}
	ids := make([]string, len(specs))
	for i, s := range specs {
		ids[i] = s.ID
	}
	joined := strings.Join(ids, ",")
	if !strings.Contains(joined, "a") || !strings.Contains(joined, "b") || !strings.Contains(joined, "def") {
		t.Errorf("unexpected IDs: %v", ids)
	}
}

func TestDefineBindingTypes(t *testing.T) {
	type TypedConfig struct {
		Name    string            `config:"name"`
		Tags    []string          `config:"tags"`
		Options map[string]string `config:"options"`
		Active  bool              `config:"active"`
		Score   float64           `config:"score"`
	}
	var got *TypedConfig
	nd := Define(TypedDefinition[TypedConfig]{
		Kind: "test-types",
		Run: func(ctx context.Context, nCtx *TypedRunContext[TypedConfig]) (store.Mutator, error) {
			got = nCtx.Config
			return nil, nil
		},
	})

	_, err := nd.Run(context.Background(), NodeRunContext{
		Config: map[string]any{
			"name":    "typed",
			"tags":    []any{"go", "anansi"},
			"options": map[string]any{"key": "val"},
			"active":  true,
			"score":   9.5,
		},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got == nil {
		t.Fatal("Config nil")
	}
	if got.Name != "typed" {
		t.Errorf("Name = %q", got.Name)
	}
	if len(got.Tags) != 2 || got.Tags[0] != "go" {
		t.Errorf("Tags = %v", got.Tags)
	}
	if got.Options["key"] != "val" {
		t.Errorf("Options = %v", got.Options)
	}
	if !got.Active {
		t.Error("Active = false")
	}
	if got.Score != 9.5 {
		t.Errorf("Score = %f", got.Score)
	}
}
