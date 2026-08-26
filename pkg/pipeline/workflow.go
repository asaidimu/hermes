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
	// Requirements aggregates the env/secret keys declared by every node in
	// this workflow (deduplicated by kind+key). The runtime validates these
	// against what the host provides before a workflow may register or run.
	Requirements []Requirement
}

// RequirementKind classifies an external capability a node needs at execution
// time.
type RequirementKind string

const (
	// ReqEnv declares a non-secret configuration key read from the runtime's
	// environment layers (Options.Env).
	ReqEnv RequirementKind = "env"
	// ReqSecret declares a credential resolved at execution time through the
	// host-supplied SecretProvider. Secret values must never persist to state,
	// checkpoints, or events.
	ReqSecret RequirementKind = "secret"
)

// Requirement declares an external capability needed by a node. Nodes embed
// these in NodeDefinition.Requirements; the compiler aggregates them onto
// Workflow.Requirements and the runtime validates them pre-registration.
type Requirement struct {
	// Kind selects where the value comes from at execution time.
	Kind RequirementKind `json:"kind"`
	// Key identifies the env layer entry or secret.
	Key string `json:"key"`
	// Required marks whether registration/run must fail when the key cannot
	// be satisfied. Non-required entries are advisory (surfaced to hosts).
	Required bool `json:"required,omitempty"`
	// Description explains what the key is used for (editor/tooling UX).
	Description string `json:"description,omitempty"`
}

// PipelineRegistry resolves a referenced pipeline id (used by pipeline-ref
// nodes) into a compiled PipelineDefinition.
type PipelineRegistry interface {
	Resolve(id string) (*PipelineDefinition, bool)
}
