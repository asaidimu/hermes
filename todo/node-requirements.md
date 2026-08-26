# Node requirements: declared env/secrets, context-borne credentials

## Context
Code review finding #review-20260826-003 (gemini apiKey in plain config) exposed a design gap: secrets must never live in node configs — configs are persisted verbatim in workflow definitions and readable by anyone who can read definitions. Steps should read credentials/env directly from run context. Additionally the runtime needs to validate BEFORE registration/run that a workflow's nodes only require env/secret keys the host actually provides.

User decisions:
- `NodeDefinition.Requirements` is a **static slice** (`[]Requirement`), not config-dependent func
- Scope = **env + secret** kinds only (service/resource deps stay with existing dependency-edge mechanism)
- **SecretProvider interface defined hermes-side**; hosts (hestia) implement it
- **Gemini node dropped** entirely (was an experiment); requirement system supersedes note -003

## Design
```go
// pkg/pipeline/types.go (pipeline owns type; nodekit aliases)
type RequirementKind string  // "env" | "secret"
type Requirement struct{ Kind, Key string; Required bool; Description string }
// Workflow.Requirements []Requirement  — aggregated by compiler

// pkg/nodekit
NodeDefinition.Requirements []Requirement
NodeRunContext.Env map[string]any               // merged runtime env layers
NodeRunContext.Secret func(key string) (any, bool) // lazy lookup; never persisted

// pkg/runtime
type SecretProvider interface {
    Get(ctx context.Context, key string) (any, bool)
    Has(ctx context.Context, key string) bool   // pre-flight without reading values
}
Options.Secrets SecretProvider
Register(wf) / ValidateWorkflowRequirements(wf): required env ∈ rt.env; required secret ∈ provider.Has
```

Delivery path: FactoryOptions.RunEnv/SecretLookup → stamped on RunContextImpl at Prepare → PipelineContext.Env()/ResolveSecret() → BuildStep populates NodeRunContext.

## Tasks
- [*] pipeline: Requirement/Workflow.Requirements + PipelineContext Env/ResolveSecret accessors
- [*] nodekit: NodeDefinition.Requirements, NodeRunContext fields, BuildStep wiring, alias consts
- [*] factory/subpipeline: stamp RunEnv+SecretLookup onto every child context
- [*] runtime: SecretProvider, Options.Secrets, validateRequirements gate (Register + public Validate), executePipeline/Resume wiring
- [*] compiler: aggregate def.Requirements across top-level + bounded-body pipelines
- [*] drop gemini: pkg/nodes/gemini/, nodes.go, nodes_test, src/generated.ts regen
- [*] tests: aggregation, register gate (env+secret), step reads Env/Secret, secret never in snapshot
- [*] docs: FEATURES.md rewrite of gemini row + new requirements section; devnote -003 deprecated; AGENTS note

## Notes
- Options.Env was dead until now (assigned runtime.go:236, never read) — this feature gives it a consumer.
- Secret values must never enter state/checkpoints/timeline/events: function-based lookup, tests assert absence.
