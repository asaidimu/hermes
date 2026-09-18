package pipelines

import (
	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/compiler"
	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/registry"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/asaidimu/hermes/pkg/timeline"
)

// @note #module-pins-an-unreleased-go-rel-d81f850c observation P1 #review,#production-readiness,#tooling : Module pins an unreleased Go release candidate (1.27rc1), not a stable toolchain
// @author hermes-review
//
// go.mod requires go >= 1.27rc1 and CI installs go-version '1.27' (an RC),
// not a GA release (README: 'Go >= 1.27 (the module targets go 1.27rc1;
// CI uses 1.27.0-rc.1)'). This blocks reproducible builds on any
// toolchain distribution that only ships stable Go (apt, most CI base
// images, most developers' machines) and means the project cannot be
// built at all until 1.27 GAs, without manually fetching a prerelease
// toolchain — confirmed directly in this review pass: apt's newest
// package here is golang-1.24-go, and building against it fails with
// 'requires go >= 1.27rc1'. For a project asking to be evaluated as
// production-ready, depending on a compiler that isn't released yet is
// a real adoption blocker, not a style nit. Track the GA date and pin
// to it, or clearly flag the RC dependency as a known constraint rather
// than a normal version requirement.

// Re-export Core types and constructors
type (
	PipelineDefinition = pipeline.PipelineDefinition
	Stage              = pipeline.Stage
	Step               = pipeline.Step
	PipelineContext    = pipeline.PipelineContext
	RoutingInstruction = pipeline.RoutingInstruction
	PipelineRunResult  = pipeline.PipelineRunResult
	RunContext         = pipeline.RunContext
	PipelineFactory    = pipeline.PipelineFactory
	FactoryOptions     = pipeline.FactoryOptions
	EntryAddress       = pipeline.EntryAddress
	SubPipelineAddress = pipeline.SubPipelineAddress
	PipelineCheckpoint = pipeline.PipelineCheckpoint

	Store       = store.Store
	MemoryStore = store.MemoryStore
	Mutator     = store.Mutator

	PipelineEvent  = events.PipelineEvent
	EventPath      = events.EventPath
	PathNode       = events.PathNode
	ScopedEventBus = events.ScopedEventBus

	TimelineEvent   = timeline.TimelineEvent
	RunTimelineMeta = timeline.RunTimelineMeta
	ActionLogStore  = actionlog.Store

	PipelineRegistry = registry.PipelineRegistry
	ActiveRun        = registry.ActiveRun

	SystemError = core.SystemError
	Logger      = core.Logger
)

// Instruction helpers
var (
	Advance   = pipeline.Advance
	Terminate = pipeline.Terminate
	Jump      = pipeline.Jump
	JumpTo    = pipeline.JumpTo
	Pause     = pipeline.Pause

	NewFactory          = pipeline.NewFactory
	NewMemoryStore      = store.NewMemoryStore
	NewScopedEventBus   = events.NewMemoryScopedBus
	NewPipelineRegistry = registry.NewPipelineRegistry
	NewMemoryActionLog  = actionlog.NewMemoryActionLog
	NewSystemError      = core.NewSystemError

	// Wire-graph entry points: canvas JSON documents reach the compiler
	// without an HTTP layer.
	DecodeWireGraph = compiler.DecodeWireGraph
	CompileWire     = compiler.CompileWire
)

// NewFactoryFromModel creates a factory reflecting a Go struct state model (Zero-Boilerplate).
func NewFactoryFromModel[T any](def PipelineDefinition, opts ...FactoryOptions) *PipelineFactory {
	return pipeline.NewFactoryFromModel[T](def, opts...)
}
