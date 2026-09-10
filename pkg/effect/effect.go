// Package effect defines the integer constants for step effect classification.
// These are shared between pkg/nodekit (which defines the full Effect type)
// and pkg/pipeline (which reads step.Effect to decide whether to record).
// This package exists solely to break the import cycle: pipeline cannot
// import nodekit (nodekit imports pipeline), so both import this package
// for the shared constants.
package effect

// Step effect classification constants. These mirror the values defined
// in pkg/nodekit/effect.go's Effect type. The integer values must stay
// in sync — see nodekit.EffectPure, nodekit.EffectSideEffecting.
const (
	Pure          = 1
	SideEffecting = 2
)
