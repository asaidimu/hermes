package nodekit

import (
	"github.com/asaidimu/go-anansi/v8/core/schema/definition"
	"github.com/asaidimu/hermes/pkg/store"
)

// nodeConfigPlan is the compile-time form of a node's config pipeline
// (plan P4.3, H4 + §G-config): the schema compiles ONCE at
// BuildStep/BuildRouter time instead of on every step attempt and
// router eval, and configs without interpolation tokens are coerced
// once at build time — per-attempt work for a static config shrinks to
// a defensive copy.
//
// Dynamic configs (any string containing {{ ... }} anywhere) keep the
// full per-attempt interpolate + coerce pipeline against live state,
// with only the schema compile hoisted. Behavior is identical to the
// old per-attempt prepareNodeConfig for both classes.
type nodeConfigPlan struct {
	def       NodeDefinition
	raw       map[string]any
	rs        *definition.ResolvedSchema
	schemaErr error
	dynamic   bool
	static    map[string]any // coerced config, valid only when !dynamic
}

// newNodeConfigPlan builds the plan. A schema error is recorded, not
// returned: BuildStep/BuildRouter have no error return, and the old
// behavior failed the STEP attempt with that error — the plan preserves
// the exact failure timing (see config()).
func newNodeConfigPlan(def NodeDefinition, raw map[string]any) *nodeConfigPlan {
	p := &nodeConfigPlan{def: def, raw: raw}
	rs, err := CompileConfigSchema(def.ConfigSchema)
	if err != nil {
		p.schemaErr = valErrWrap(ValErrInvalidConfigSchema, def.Kind, "node "+def.Kind+" has an invalid config schema", err)
		return p
	}
	p.rs = rs
	p.dynamic = containsInterpolation(raw)
	if !p.dynamic {
		// Coerce once. The raw map is deep-copied first: the plan outlives
		// the caller's config reference and CoerceConfig must not share
		// structure with it (defs may be reused across workflows).
		p.static = CoerceConfig(deepCopyAny(raw).(map[string]any), rs)
	}
	return p
}

// config produces the coerced config for one attempt, byte-identical to
// the pre-P4.3 prepareNodeConfig output for the same inputs.
func (p *nodeConfigPlan) config(state, resources, results map[string]any) (map[string]any, error) {
	if p.schemaErr != nil {
		return nil, p.schemaErr
	}
	if !p.dynamic {
		// Defensive copy per attempt: nodes receive the map as their
		// config and must not be able to contaminate the next attempt,
		// run, or workflow through it.
		return store.DeepCopyMap(p.static), nil
	}
	return prepareNodeConfig(p.def, p.raw, state, resources, results)
}

// containsInterpolation reports whether any string anywhere in obj
// carries an interpolation token — the exact predicate Interpolate
// itself uses to decide per-string.
func containsInterpolation(obj any) bool {
	switch v := obj.(type) {
	case string:
		return anyInterpRe.MatchString(v)
	case []any:
		for _, item := range v {
			if containsInterpolation(item) {
				return true
			}
		}
	case map[string]any:
		for _, item := range v {
			if containsInterpolation(item) {
				return true
			}
		}
	}
	return false
}

// deepCopyAny deep-copies JSON-like structures (maps, slices, scalars)
// so build-time precomputation never aliases caller-owned config.
func deepCopyAny(obj any) any {
	switch v := obj.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, item := range v {
			out[k] = deepCopyAny(item)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = deepCopyAny(item)
		}
		return out
	default:
		return obj
	}
}
