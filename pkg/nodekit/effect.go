package nodekit

// Effect classifies whether a node's execution has real-world consequences
// (or other non-deterministic behavior) that must be durably recorded rather
// than safely re-derived by re-running the node, versus being a pure,
// deterministic function of its inputs that's safe to re-execute freely.
//
// This classification exists to support event-sourced recovery: a run can be
// reconstructed by replaying its pipeline definition, re-executing every
// EffectPure step fresh, while EffectSideEffecting steps have their prior
// outcome injected from a durable record instead of being re-run. See
// EXECUTION_ENGINE_REDESIGN_V2.md §2 for the full design this supports.
//
// The zero value, EffectUnspecified, is deliberately invalid — Register
// panics if a node is registered without an explicit classification. There
// is no default in either direction: assuming Pure is unsafe (a
// side-effecting node could be silently re-executed on replay, duplicating
// a real-world action like an HTTP call), and silently assuming
// SideEffecting for everything defeats the purpose of classifying at all
// (nothing would ever be cheaply re-derivable). Node authors must decide,
// and the registry enforces that they did.
type Effect int

const (
	// EffectUnspecified is the zero value. Never a valid classification for
	// a registered node — Register rejects it.
	EffectUnspecified Effect = iota

	// EffectPure marks a node as a deterministic function of its config and
	// state, with no observable effect outside the pipeline run. Safe to
	// re-execute on replay/recovery; its outcome is never durably recorded.
	EffectPure

	// EffectSideEffecting marks a node as having consequences beyond this
	// pipeline run (a network call, a database write) or non-deterministic
	// behavior (wall-clock time, randomness) that must not be silently
	// duplicated or diverged on replay. Its outcome is durably recorded
	// before the step is considered complete, and a recorded outcome is
	// injected rather than the node being re-executed during recovery.
	EffectSideEffecting
)

// String returns a human-readable name, used in panic/error messages and in
// the registry's JSON representation (see NodeDefinition.MarshalJSON-adjacent
// handling — Effect is exposed to registry consumers, e.g. the visual
// editor, as this string via its json tag).
func (e Effect) String() string {
	switch e {
	case EffectPure:
		return "Pure"
	case EffectSideEffecting:
		return "SideEffecting"
	default:
		return "Unspecified"
	}
}

// Valid reports whether e is a real classification (not the zero value, and
// not an out-of-range value).
func (e Effect) Valid() bool {
	return e == EffectPure || e == EffectSideEffecting
}

// MarshalJSON renders Effect as its string name, so serialized node
// definitions expose a stable, readable value ("Pure" / "SideEffecting")
// rather than a bare integer that would be meaningless to consumers and
// would silently renumber if constants are ever reordered.
func (e Effect) MarshalJSON() ([]byte, error) {
	return []byte(`"` + e.String() + `"`), nil
}
