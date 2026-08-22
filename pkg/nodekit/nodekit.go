package nodekit

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/asaidimu/go-anansi/v8/core/document"
	"github.com/asaidimu/go-anansi/v8/core/schema/definition"
	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
)

type HandleType string

const (
	HandleSource HandleType = "source"
	HandleTarget HandleType = "target"
)

type HandleKind string

const (
	HandleExecutable HandleKind = "executable"
	HandleResource   HandleKind = "resource"
)

// HandleSpec declares connection points on a visual node.
type HandleSpec struct {
	Type  HandleType `json:"type"`
	ID    string     `json:"id"`
	Label string     `json:"label,omitempty"`
	Kind  HandleKind `json:"kind,omitempty"`
}

// NodeRunContext provides configuration and state to a running node action.
type NodeRunContext struct {
	NodeID    string
	Config    map[string]any
	Document  *document.Document
	State     map[string]any
	Results   map[string]any
	Errors    map[string]any
	Resources map[string]any
	Store     store.Store
	Logger    core.Logger
}

// NodeRunner executes step logic for an executable node.
type NodeRunner func(ctx context.Context, nCtx NodeRunContext) (store.DocumentMutator, error)

// NodeRouter evaluates routing logic for branching nodes (if, switch, etc.).
type NodeRouter func(ctx context.Context, nCtx NodeRunContext) (string, error)

// NodeRouterFunc is an alternative routing interface that returns a
// RoutingInstruction directly, bypassing handle resolution. Use this for
// nodes that need non-jump routing (e.g. pause).
type NodeRouterFunc func(ctx context.Context, nCtx NodeRunContext) (pipeline.RoutingInstruction, error)

// NodeResourceInit initializes a resource handle at run scope.
type NodeResourceInit func(ctx context.Context, nCtx NodeRunContext) (any, error)

// NodeResourceCleanup tears down a resource handle at run completion.
type NodeResourceCleanup func(ctx context.Context, nCtx NodeRunContext, handle any) error

// NodeDefinition declares the visual and execution metadata for a node.
type NodeDefinition struct {
	Kind         string                                   `json:"kind"`
	Label        string                                   `json:"label"`
	Description  string                                   `json:"description,omitempty"`
	Icon         string                                   `json:"icon,omitempty"`
	ConfigSchema json.RawMessage                          `json:"configSchema,omitempty"`
	Scope        string                                   `json:"scope,omitempty"`
	Type         string                                   `json:"type,omitempty"`
	BodyHandle   string                                   `json:"bodyHandle,omitempty"`
	Handles      func(config map[string]any) []HandleSpec `json:"-"`
	HandlesJS    string                                   `json:"-"` // JS body of (config) => HandleSpec[] for /handles.js
	Run          NodeRunner                               `json:"-"`
	Router       NodeRouter                               `json:"-"`
	RouterFunc   NodeRouterFunc                           `json:"-"`
	// PipelinesRouterFunc is called after a bounded node's body completes.
	// It receives the body results and returns a RoutingInstruction.
	// This allows nodes like pause to check for buffered events and
	// either resume immediately or pause the pipeline.
	PipelinesRouterFunc func(ctx context.Context, nCtx NodeRunContext, results []pipeline.PipelineRunResult) (pipeline.RoutingInstruction, error) `json:"-"`
	ResourceInit        NodeResourceInit                                                                                                           `json:"-"`
	ResourceEnd         NodeResourceCleanup                                                                                                        `json:"-"`
}

// CompileConfigSchema compiles a node's ConfigSchema (an anansi schema) through
// the anansi schema compiler, producing the resolved field descriptors used for
// config coercion and defaults.
func CompileConfigSchema(raw json.RawMessage) (*definition.ResolvedSchema, error) {
	sc, err := definition.FromJSON(raw)
	if err != nil {
		return nil, err
	}
	return definition.Compile(sc)
}

var (
	registryMu sync.RWMutex
	nodeTypes  = make(map[string]NodeDefinition)
)

// Register registers a node type definition.
func Register(def NodeDefinition) {
	registryMu.Lock()
	defer registryMu.Unlock()
	nodeTypes[def.Kind] = def
}

// Get returns a registered node definition by kind.
func Get(kind string) (NodeDefinition, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	def, ok := nodeTypes[kind]
	return def, ok
}

// Registry returns all registered node definitions.
func Registry() map[string]NodeDefinition {
	registryMu.RLock()
	defer registryMu.RUnlock()
	res := make(map[string]NodeDefinition, len(nodeTypes))
	for k, v := range nodeTypes {
		res[k] = v
	}
	return res
}

// BuildStep creates a pipeline Step from a node definition and configuration.
// The raw config is deep-interpolated against the live document state on every
// execution (state changes across loop iterations), then coerced via the node's
// compiled anansi schema, mirroring the TS buildStep action. resources (optional)
// supplies run-scoped resource handles (the compiler wires dependency edges).
func BuildStep(nodeID string, def NodeDefinition, config map[string]any, resources func() map[string]any) pipeline.Step {
	return pipeline.Step{
		ID:    nodeID,
		Label: def.Label,
		Action: func(ctx context.Context, pcxt pipeline.PipelineContext, doc *document.Document) (store.DocumentMutator, error) {
			if def.Run == nil {
				return nil, nil
			}
			state := doc.Data()
			var results map[string]any
			if r, ok := state["results"].(map[string]any); ok {
				results = r
			}
			res := resolveStepResources(pcxt, resources)
			cfg, err := prepareNodeConfig(def, config, state, res, results)
			if err != nil {
				return nil, err
			}
			return def.Run(ctx, NodeRunContext{
				NodeID:    nodeID,
				Config:    cfg,
				State:     state,
				Results:   results,
				Resources: res,
				Document:  doc,
				Logger:    pcxt.Logger(),
			})
		},
	}
}

// resolveStepResources turns the compiler-supplied resource key map
// ({kind: "resource:<sourceNodeId>"}) into resolved handles via the pipeline
// context's resource resolver. Keys that cannot be resolved are passed through
// unchanged so interpolation/run still see the artifact key string.
func resolveStepResources(pcxt pipeline.PipelineContext, resources func() map[string]any) map[string]any {
	if resources == nil {
		return nil
	}
	res := map[string]any{}
	for kind, key := range resources() {
		if ks, ok := key.(string); ok {
			if handle, ok := pcxt.ResolveResource(ks); ok {
				res[kind] = handle
				continue
			}
		}
		res[kind] = key
	}
	return res
}

// PatchMutator wraps a flat (dotted-key) patch into a DocumentMutator that
// deep-merges it into the document (deleting keys marked with Delete).
func PatchMutator(flat map[string]any) store.DocumentMutator {
	return func(doc *document.Document) error {
		return ApplyPatch(doc, flat)
	}
}