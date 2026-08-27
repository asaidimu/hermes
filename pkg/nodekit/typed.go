package nodekit

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/asaidimu/go-anansi/v8/core/data"
	"github.com/asaidimu/go-anansi/v8/core/document"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
)

// @note #review-20260827-001 observation status=open priority=P1 tags=#review,#api,#typesafety : Dual RunContext types and field shadowing in TypedRunContext
// @author antigravity
//
// TypedRunContext[C] embeds NodeRunContext while also declaring Config *C. In Go struct
// embedding, TypedRunContext[C].Config shadows NodeRunContext.Config (map[string]any).
// Node authors receive TypedRunContext, but the underlying engine still operates on
// untyped NodeRunContext with map[string]any for Config, State, Results, and Errors.
// TypedRunContext[C] should be the default RunContext[C any] across the entire nodekit
// lifecycle rather than a wrapper over untyped legacy state bags.
//
// TypedRunContext[C] carries both typed config and general runtime context.
// Node authors receive this from typed Run/Router/BodyHandle callbacks.
type TypedRunContext[C any] struct {
	NodeRunContext
	Config *C
}

// TypedDefinition[C] is the generic node definition. C is the config struct.
// One struct per node kind — field tags drive schema derivation, coercion, and
// binding. The struct IS the schema: no hand-written ConfigSchema JSON needed.
//
// Convention:
//   - config:"fieldName"    — JSON field name (used for both schema and binding)
//   - anansi:"required=true" — metadata (required, default, nullable, type, values)
//   - anansi:"-"             — skip field entirely
//
// Example:
//
//	type HTTPConfig struct {
//	    Method       string            `config:"method" anansi:"default=GET"`
//	    URL          string            `config:"url" anansi:"required=true"`
//	    Headers      map[string]string `config:"headers"`
//	    ThrowOnError bool              `config:"throwOnError"`
//	}
type TypedDefinition[C any] struct {
	Kind                string
	Label               string
	Description         string
	Icon                string
	Requirements        []Requirement
	Scope               string
	Type                string
	BodyHandle          string
	Handles             func(*C) []HandleSpec
	HandlesJS           string
	Run                 func(context.Context, *TypedRunContext[C]) (store.Mutator, error)
	Router              func(context.Context, *TypedRunContext[C]) (string, error)
	RouterFunc          func(context.Context, *TypedRunContext[C]) (pipeline.RoutingInstruction, error)
	PipelinesRouterFunc func(ctx context.Context, nCtx *TypedRunContext[C], results []pipeline.PipelineRunResult) (pipeline.RoutingInstruction, error)
	ResourceInit        func(context.Context, *TypedRunContext[C]) (any, error)
	ResourceEnd         func(context.Context, *TypedRunContext[C], any) error
	ValidateConfig      func(*C) error
}

// @note #review-20260827-002 observation status=open priority=P2 tags=#review,#performance,#architecture : Type erasure and per-execution map-to-struct binding overhead in Define
// @author antigravity
//
// Define[C] derives the schema at registration time but immediately erases type C into
// an untyped NodeDefinition. On every single node execution, prepareNodeConfig coerces
// raw map state and the wrapped callback calls bindFromMap (anansi record view -> struct)
// afresh. If the engine executed typed steps natively, this per-execution reflection
// and map-copying overhead would be eliminated.
//
// Define creates an erased NodeDefinition from a TypedDefinition[C].
// Schema is derived once from C at registration time via ExtractDTOSchemaDirectWithTag.
// The returned NodeDefinition is ready for Register — the engine never sees C.
func Define[C any](def TypedDefinition[C]) NodeDefinition {
	var zero C
	schemaJSON, err := data.ExtractDTOSchemaDirectWithTag(&zero, "config")
	if err != nil {
		panic(fmt.Sprintf("nodekit.Define[%s]: failed to derive config schema: %v", def.Kind, err))
	}

	d := NodeDefinition{
		Kind:         def.Kind,
		Label:        def.Label,
		Description:  def.Description,
		Icon:         def.Icon,
		Requirements: def.Requirements,
		Scope:        def.Scope,
		Type:         def.Type,
		BodyHandle:   def.BodyHandle,
		HandlesJS:    def.HandlesJS,
		ConfigSchema: schemaJSON,
	}

	// Wrap Handles: bind raw map → *C, call typed callback.
	if def.Handles != nil {
		typedHandles := def.Handles
		d.Handles = func(raw map[string]any) []HandleSpec {
			var cfg C
			if err := bindFromMap(raw, schemaJSON, &cfg); err != nil {
				return nil
			}
			return typedHandles(&cfg)
		}
	}

	// Wrap Run: bind prepared config → *C, call typed callback.
	if def.Run != nil {
		typedRun := def.Run
		d.Run = func(ctx context.Context, nCtx NodeRunContext) (store.Mutator, error) {
			var cfg C
			if err := bindFromMap(nCtx.Config, schemaJSON, &cfg); err != nil {
				return nil, fmt.Errorf("config bind failed for node %s: %w", nCtx.NodeID, err)
			}
			return typedRun(ctx, &TypedRunContext[C]{NodeRunContext: nCtx, Config: &cfg})
		}
	}

	// Wrap Router: bind raw map → *C, call typed callback.
	if def.Router != nil {
		typedRouter := def.Router
		d.Router = func(ctx context.Context, nCtx NodeRunContext) (string, error) {
			var cfg C
			if err := bindFromMap(nCtx.Config, schemaJSON, &cfg); err != nil {
				return "", fmt.Errorf("config bind failed for node %s: %w", nCtx.NodeID, err)
			}
			return typedRouter(ctx, &TypedRunContext[C]{NodeRunContext: nCtx, Config: &cfg})
		}
	}

	// Wrap RouterFunc: bind raw map → *C, call typed callback.
	if def.RouterFunc != nil {
		typedRouterFunc := def.RouterFunc
		d.RouterFunc = func(ctx context.Context, nCtx NodeRunContext) (pipeline.RoutingInstruction, error) {
			var cfg C
			if err := bindFromMap(nCtx.Config, schemaJSON, &cfg); err != nil {
				return nil, fmt.Errorf("config bind failed for node %s: %w", nCtx.NodeID, err)
			}
			return typedRouterFunc(ctx, &TypedRunContext[C]{NodeRunContext: nCtx, Config: &cfg})
		}
	}

	// Wrap PipelinesRouterFunc: bind raw map → *C, call typed callback.
	if def.PipelinesRouterFunc != nil {
		typedPRF := def.PipelinesRouterFunc
		d.PipelinesRouterFunc = func(ctx context.Context, nCtx NodeRunContext, results []pipeline.PipelineRunResult) (pipeline.RoutingInstruction, error) {
			var cfg C
			if err := bindFromMap(nCtx.Config, schemaJSON, &cfg); err != nil {
				return nil, fmt.Errorf("config bind failed for node %s: %w", nCtx.NodeID, err)
			}
			return typedPRF(ctx, &TypedRunContext[C]{NodeRunContext: nCtx, Config: &cfg}, results)
		}
	}

	// Wrap ResourceInit: bind raw map → *C, call typed callback.
	if def.ResourceInit != nil {
		typedInit := def.ResourceInit
		d.ResourceInit = func(ctx context.Context, nCtx NodeRunContext) (any, error) {
			var cfg C
			if err := bindFromMap(nCtx.Config, schemaJSON, &cfg); err != nil {
				return nil, fmt.Errorf("config bind failed for node %s: %w", nCtx.NodeID, err)
			}
			return typedInit(ctx, &TypedRunContext[C]{NodeRunContext: nCtx, Config: &cfg})
		}
	}

	// Wrap ResourceEnd: bind raw map → *C, call typed callback.
	if def.ResourceEnd != nil {
		typedEnd := def.ResourceEnd
		d.ResourceEnd = func(ctx context.Context, nCtx NodeRunContext, handle any) error {
			var cfg C
			if err := bindFromMap(nCtx.Config, schemaJSON, &cfg); err != nil {
				return fmt.Errorf("config bind failed for node %s: %w", nCtx.NodeID, err)
			}
			return typedEnd(ctx, &TypedRunContext[C]{NodeRunContext: nCtx, Config: &cfg}, handle)
		}
	}

	// Custom validation: if the node author provided ValidateConfig, wrap it
	// into NodeDefinition.validateCustom so the compiler's Validate step calls it.
	if def.ValidateConfig != nil {
		typedVC := def.ValidateConfig
		d.validateCustom = func(raw map[string]any) error {
			var cfg C
			if err := bindFromMap(raw, schemaJSON, &cfg); err != nil {
				return err
			}
			return typedVC(&cfg)
		}
	}

	return d
}

// bindFromMap binds a raw config map into a typed struct.
// Flow: raw map → JSON marshal → schema-aware decode via anansi fast decoder →
// struct. The coerce-then-bind pattern from prepareNodeConfig handles defaults
// and type normalization before this step. Here we do the final bind.
//
// For the MVP we use the simpler path: NewRecordView(map) → BindToTag.
// This is correct because CoerceConfig (called by prepareNodeConfig) already
// normalizes types and applies defaults against the derived schema. By the time
// we bind, the map is schema-compliant and BindToTag just fills the struct fields.
func bindFromMap(raw map[string]any, schemaJSON json.RawMessage, target any) error {
	if raw == nil {
		return nil
	}
	doc := document.NewRecordView(raw)
	if err := doc.BindToTag(target, "config"); err != nil {
		return fmt.Errorf("bind failed: %w", err)
	}
	return nil
}

// TypeOf[C] returns the reflect.Type of C for callers that need the runtime type.
func TypeOf[C any]() reflect.Type {
	var zero C
	return reflect.TypeOf(zero)
}
