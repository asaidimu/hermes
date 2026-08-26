package nodes

import (
	"encoding/json"
	"sort"
	"testing"

	"github.com/asaidimu/hermes/pkg/nodekit"
)

// TestRegisteredKinds asserts every expected node kind is registered by the
// aggregator, and that all node types are present.
func TestRegisteredKinds(t *testing.T) {
	got := nodekit.Registry()
	if len(got) == 0 {
		t.Fatal("registry is empty; aggregator init did not register nodes")
	}

	want := []string{
		"arithmetic", "code", "database", "delay", "distribute", "for-each", "fork", "http",
		"if", "join", "pause", "pipeline-ref", "query", "switch", "transformer", "trigger", "try-catch", "while",
	}
	for _, kind := range want {
		if _, ok := got[kind]; !ok {
			t.Errorf("missing registered node kind %q", kind)
		}
	}

	keys := make([]string, 0, len(got))
	for k := range got {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if len(keys) != len(want) {
		t.Errorf("registry has %d kinds, want %d: %v", len(keys), len(want), keys)
	}
}

// TestConfigSchemasCompile asserts every node's ConfigSchema is a valid anansi
// schema that compiles through the schema compiler (field descriptors resolved).
// Nodes with no config fields (e.g. join) may have an empty derived schema.
func TestConfigSchemasCompile(t *testing.T) {
	for kind, def := range nodekit.Registry() {
		if len(def.ConfigSchema) == 0 {
			t.Errorf("node %q has empty ConfigSchema", kind)
			continue
		}
		if !json.Valid(def.ConfigSchema) {
			t.Errorf("node %q ConfigSchema is not valid JSON: %s", kind, def.ConfigSchema)
			continue
		}
		rs, err := nodekit.CompileConfigSchema(def.ConfigSchema)
		if err != nil {
			t.Errorf("node %q ConfigSchema failed to compile: %v", kind, err)
			continue
		}
		// Some nodes (e.g. join) have no config fields — that's valid.
		_ = rs
	}
}

// TestHandlesAlwaysReturn asserts handles functions produce at least one spec.
func TestHandlesAlwaysReturn(t *testing.T) {
	for kind, def := range nodekit.Registry() {
		if def.Handles == nil {
			t.Errorf("node %q has nil Handles", kind)
			continue
		}
		specs := def.Handles(map[string]any{})
		if len(specs) == 0 {
			t.Errorf("node %q Handles returned no specs for empty config", kind)
		}
	}
}
