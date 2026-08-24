package pipelines

import (
	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/registry"
	"github.com/asaidimu/hermes/pkg/server"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/asaidimu/hermes/pkg/timeline"
)

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

	Store           = store.Store
	MemoryStore     = store.MemoryStore
	DocumentMutator = store.DocumentMutator

	PipelineEvent  = events.PipelineEvent
	EventPath      = events.EventPath
	PathNode       = events.PathNode
	ScopedEventBus = events.ScopedEventBus

	TimelineEvent   = timeline.TimelineEvent
	RunTimelineMeta = timeline.RunTimelineMeta
	TimelineStore   = timeline.TimelineStore

	PipelineRegistry = registry.PipelineRegistry
	ActiveRun        = registry.ActiveRun

	PipelineServer = server.PipelineServer
	ServerConfig   = server.ServerConfig

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
	NewTimelineStore    = timeline.NewMemoryTimelineStore
	NewPipelineServer   = server.NewPipelineServer
	NewSystemError      = core.NewSystemError
)

// NewFactoryFromModel creates a factory reflecting a Go struct state model (Zero-Boilerplate).
func NewFactoryFromModel[T any](def PipelineDefinition, opts ...FactoryOptions) *PipelineFactory {
	return pipeline.NewFactoryFromModel[T](def, opts...)
}
