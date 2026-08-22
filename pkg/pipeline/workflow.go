package pipeline

import (
	"context"

	"github.com/asaidimu/hermes/pkg/events"
)

// WorkflowTrigger declares an event source that starts a workflow run.
type WorkflowTrigger struct {
	ID        string
	Event     string
	Predicate func(event events.PipelineEvent) bool
	Cron      string // cron expression for recurring triggers (e.g. "@every 5m")
}

// Service declares a run-scoped (or workflow-scoped) resource dependency that
// the runtime initializes and tears down around pipeline execution.
type Service struct {
	ID      string
	Scope   string // "workflow" | "run" | "transient"
	Kind    string // resource node kind (emitted on resource:* events)
	Label   string // resource node label (emitted on resource:* events)
	Init    func(ctx context.Context) (any, error)
	Cleanup func(ctx context.Context, handle any) error
}

// Workflow is the compiled, runnable orchestration unit produced by the
// compiler: a set of trigger-bound pipelines plus optional resource services.
type Workflow struct {
	ID        string
	Label     string
	Triggers  map[string]WorkflowTrigger
	Pipelines map[string]PipelineDefinition
	Services  []Service
}

// PipelineRegistry resolves a referenced pipeline id (used by pipeline-ref
// nodes) into a compiled PipelineDefinition.
type PipelineRegistry interface {
	Resolve(id string) (*PipelineDefinition, bool)
}