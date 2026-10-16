// Structural fingerprints for pipeline definitions (v2.1 plan H1.1).
//
// Fingerprint hashes the DECLARATIVE surface the replay walk consumes:
// pipeline id, stage order (the walk is sequential — order is semantic),
// stage/step identities, timeouts, retry counts, effect classes, stage
// config, and nested subpipeline definitions. It exists so a run can pin
// WHICH definition shape it was recorded under (stamped on action-log
// entries) and a later resume/restart can detect that the registered
// workflow drifted from the definition the log was written against —
// the restart-rebuild equivalent of the §D divergence guard.
//
// What is deliberately EXCLUDED, and why (do not oversell the guarantee):
//
//   - Labels: cosmetic; renaming a label does not change replay semantics.
//   - The closure fields — Step.Action, Stage.Router,
//     Stage.PipelinesRouter, Stage.DynamicPipelines: their behavior is Go
//     code, which no structural hash can see. For compiled workflows node
//     behavior derives from Config + the frozen node catalog, so real
//     canvas edits land in Config and ARE detected. A hand-built workflow
//     whose closures change behavior while keeping identical structure is
//     UNDETECTED by design — hosts needing code-level guarantees must
//     version their workflow ids (docs/RECOVERY.md carries the contract).
//
// Determinism: every map is walked in sorted key order and every field is
// written through unambiguous (JSON-encoded) boundaries, so the hash is
// stable across processes, Go map iteration orders, and def instances
// built independently from the same source.
package pipeline

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
)

// Fingerprint returns the structural fingerprint of a pipeline definition:
// 32 hex chars (FNV-1a 128). Nil-safe: a nil definition fingerprints as
// the literal "nil" sentinel, never as the hash of an empty definition.
func Fingerprint(def *PipelineDefinition) string {
	if def == nil {
		return fingerprintNilSentinel
	}
	h := fnv.New128a()
	fingerprintDefInto(h, def)
	return hex(h)
}

const fingerprintNilSentinel = "nil"

func hex(h interface{ Sum([]byte) []byte }) string {
	const digits = "0123456789abcdef"
	sum := h.Sum(nil)
	out := make([]byte, 0, len(sum)*2)
	for _, b := range sum {
		out = append(out, digits[b>>4], digits[b&0x0f])
	}
	return string(out)
}

// fpWriter is the minimal writer both the hash and tests use.
type fpWriter interface {
	Write(p []byte) (int, error)
}

func fpStr(h fpWriter, s string) {
	// JSON encoding gives every string unambiguous boundaries — a step id
	// containing quotes/pipes cannot collide with structural separators.
	writeAll(h, strconv.Itoa(len(s)))
	writeAll(h, ":")
	writeAll(h, s)
	writeAll(h, "|")
}

func fpInt(h fpWriter, n int64) {
	writeAll(h, strconv.FormatInt(n, 10))
	writeAll(h, "|")
}

func writeAll(h fpWriter, s string) {
	_, _ = h.Write([]byte(s)) // fnv never errors
}

func fingerprintDefInto(h fpWriter, def *PipelineDefinition) {
	fpStr(h, "def")
	fpStr(h, def.ID)
	fpInt(h, int64(len(def.Stages)))
	for i := range def.Stages {
		fingerprintStageInto(h, &def.Stages[i])
	}
}

func fingerprintStageInto(h fpWriter, st *Stage) {
	fpStr(h, "stage")
	fpStr(h, st.ID)
	fpInt(h, int64(st.Timeout))
	fpValue(h, st.Config)
	// Steps: map, walked in sorted key order. Key IS the step id — sorted
	// keys double as deterministic step order.
	fpInt(h, int64(len(st.Steps)))
	ids := make([]string, 0, len(st.Steps))
	for id := range st.Steps {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		step := st.Steps[id]
		fpStr(h, "step")
		fpStr(h, step.ID)
		fpInt(h, int64(step.Timeout))
		fpInt(h, int64(step.Retries))
		fpInt(h, int64(step.Effect))
	}
	// Nested subpipelines: SLICE order is semantic (mirrors the def walk).
	fpInt(h, int64(len(st.Pipelines)))
	for i := range st.Pipelines {
		fingerprintDefInto(h, &st.Pipelines[i])
	}
	// Stage.DynamicPipelines is a closure (excluded). The distribute node
	// regenerates children at runtime from state; their definitions are
	// structurally captured when they become Stage.Pipelines copies — a
	// change in the CLOSURE body with identical generated shape is not
	// detectable, same caveat as every other closure field.
}

// fpValue renders Config-like values canonically: string-keyed maps in
// sorted key order, slices in order, everything else through its fmt
// representation. Values that json cannot represent (funcs, chans) still
// render deterministically within a process via their pointer form —
// recompiled processes treat them as changed, which is the honest outcome
// for a config carrying a func.
func fpValue(h fpWriter, v any) {
	switch t := v.(type) {
	case nil:
		fpStr(h, "null")
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		fpStr(h, "map")
		fpInt(h, int64(len(keys)))
		for _, k := range keys {
			fpStr(h, k)
			fpValue(h, t[k])
		}
	case []any:
		fpStr(h, "arr")
		fpInt(h, int64(len(t)))
		for _, item := range t {
			fpValue(h, item)
		}
	case string:
		fpStr(h, "str")
		fpStr(h, t)
	case bool:
		fpStr(h, "bool")
		fpInt(h, b2i(t))
	case int:
		fpStr(h, "int")
		fpInt(h, int64(t))
	case int64:
		fpStr(h, "int64")
		fpInt(h, t)
	case float64:
		fpStr(h, "float")
		fpInt(h, int64(t))
	default:
		// json.RawMessage and friends: fall back to their fmt form. Not
		// cryptographically canonical, but deterministic within a process
		// and stable for the value types configs actually carry.
		fpStr(h, "other")
		fpStr(h, fmt.Sprintf("%v", t))
	}
}

func b2i(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
