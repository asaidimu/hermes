package nodekit

import "github.com/asaidimu/hermes/pkg/pipeline"

// Effect and its classification constants are canonically defined in
// pkg/pipeline (20261003 refactor, plan D.4 — the pkg/effect import-cycle
// workaround package was deleted). They are aliased here so node authors
// declare a node's classification entirely through the nodekit SDK without
// importing pkg/pipeline as well.
type Effect = pipeline.Effect

const (
	EffectUnspecified   = pipeline.EffectUnspecified
	EffectPure          = pipeline.EffectPure
	EffectSideEffecting = pipeline.EffectSideEffecting
)
