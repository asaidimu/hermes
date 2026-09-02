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

// TestEffectClassification locks in the intended Effect classification for
// every built-in node kind (see EXECUTION_ENGINE_REDESIGN_V2.md §2). This is
// the test that actually catches classification drift: if a node's Effect
// changes without this table being updated deliberately, that's exactly the
// silent-divergence risk the classification scheme exists to prevent, so it
// fails the build rather than failing a replay in production.
//
// Every registered node MUST appear in this table — TestNoUnclassifiedNodes
// below fails if a new node kind is added without a corresponding, reviewed
// entry here.
func TestEffectClassification(t *testing.T) {
	want := map[string]nodekit.Effect{
		// Deterministic, no I/O: safe to re-execute freely on replay.
		"arithmetic":  nodekit.EffectPure,
		"distribute":  nodekit.EffectPure, // control-flow only; its steps carry their own classification
		"for-each":    nodekit.EffectPure, // control-flow only
		"fork":        nodekit.EffectPure, // control-flow only
		"if":          nodekit.EffectPure,
		"join":        nodekit.EffectPure,
		"switch":      nodekit.EffectPure,
		"transformer": nodekit.EffectPure,
		"try-catch":   nodekit.EffectPure, // aggregates/routes on sub-pipeline errors; no I/O of its own
		"while":       nodekit.EffectPure, // control-flow only

		// Has real-world consequences, or is otherwise non-deterministic:
		// outcome must be durably recorded, never silently re-executed.
		"code":     nodekit.EffectSideEffecting, // arbitrary user code; cannot be safely assumed pure
		"database": nodekit.EffectSideEffecting, // writes to external storage
		"delay":    nodekit.EffectSideEffecting, // wall-clock time is a source of replay divergence
		"http":     nodekit.EffectSideEffecting, // real network call
		"query":    nodekit.EffectSideEffecting, // reads external state that can change between replays
		"trigger":  nodekit.EffectSideEffecting, // represents an external event having occurred
		"pause":    nodekit.EffectSideEffecting, // registers stateful watch descriptors; not safely re-runnable
		// Conservative default: pipeline-ref's true classification depends
		// on the referenced pipeline's own steps, which can't be known
		// statically at this node's registration time. Composite
		// classification (inherit from the referenced definition) is not
		// implemented — see EXECUTION_ENGINE_REDESIGN_V2.md §2's table note.
		// SideEffecting is the safe direction to be wrong in.
		"pipeline-ref": nodekit.EffectSideEffecting,
	}

	registry := nodekit.Registry()
	for kind, wantEffect := range want {
		def, ok := registry[kind]
		if !ok {
			t.Errorf("classification table references unregistered kind %q", kind)
			continue
		}
		if def.Effect != wantEffect {
			t.Errorf("node %q Effect = %v, want %v", kind, def.Effect, wantEffect)
		}
	}

	// TestNoUnclassifiedNodes: every registered kind must have an entry
	// above. A new node landing without updating this table is exactly the
	// "someone forgot to think about replay safety" case worth catching in
	// review, not at runtime.
	for kind := range registry {
		if _, ok := want[kind]; !ok {
			t.Errorf("node %q has no entry in the Effect classification table; add one deliberately", kind)
		}
	}
}
