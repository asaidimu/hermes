package nodekit

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/asaidimu/go-anansi/v8/core/data"
	"github.com/asaidimu/go-anansi/v8/core/document"
	"go.uber.org/zap"

	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
)

// TypedRunContext[C] carries the typed config and the runtime context.
// Node authors receive it from typed Run/Router/BodyHandle callbacks.
//
// Resolved 20261003 refactor (plan R.3, closing #review-20260827-001):
// TypedRunContext used to EMBED NodeRunContext while declaring Config *C —
// the outer Config shadowed the embedded map[string]any one, so `nCtx.Config`
// and `nCtx.NodeRunContext.Config` were different values at the same
// selector depth. The fields are now flat: Config is ONLY the typed *C,
// and the untyped map stays reachable as RawConfig.
type TypedRunContext[C any] struct {
	NodeID    string
	RunID     string
	Config    *C
	RawConfig map[string]any
	State     map[string]any
	Errors    map[string]any
	Resources map[string]any
	Store     store.Store
	Logger    *zap.SugaredLogger
	Env       map[string]any
	// Secret resolves credentials by key at execution time. Values are for
	// immediate use only: never write them into state, checkpoints, or
	// event payloads.
	Secret func(key string) (any, bool)
}

// typedContext flattens an erased NodeRunContext into the typed form.
func typedContext[C any](n NodeRunContext, cfg *C) *TypedRunContext[C] {
	return &TypedRunContext[C]{
		NodeID:    n.NodeID,
		RunID:     n.RunID,
		Config:    cfg,
		RawConfig: n.Config,
		State:     n.State,
		Errors:    n.Errors,
		Resources: n.Resources,
		Store:     n.Store,
		Logger:    n.Logger,
		Env:       n.Env,
		Secret:    n.Secret,
	}
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
	// Effect classifies this node kind for event-sourced replay purposes
	// (see effect.go). Required: Define propagates it unchanged onto the
	// erased NodeDefinition, and Register panics if it's left unset. Every
	// node kind must decide EffectPure or EffectSideEffecting explicitly —
	// there is no safe default.
	Effect Effect
}

// @note #review-20260827-002 observation P2 status=open priority=P2 tags=#review,#performance,#architecture : Type erasure and per-execution map-to-struct binding overhead in Define
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
		Effect:       def.Effect,
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
				return nil, valErrWrap(ValErrInvalidConfig, nCtx.NodeID, "config bind failed for node "+nCtx.NodeID, err)
			}
			return typedRun(ctx, typedContext(nCtx, &cfg))
		}
	}

	// Wrap Router: bind raw map → *C, call typed callback.
	if def.Router != nil {
		typedRouter := def.Router
		d.Router = func(ctx context.Context, nCtx NodeRunContext) (string, error) {
			var cfg C
			if err := bindFromMap(nCtx.Config, schemaJSON, &cfg); err != nil {
				return "", valErrWrap(ValErrInvalidConfig, nCtx.NodeID, "config bind failed for node "+nCtx.NodeID, err)
			}
			return typedRouter(ctx, typedContext(nCtx, &cfg))
		}
	}

	// Wrap RouterFunc: bind raw map → *C, call typed callback.
	if def.RouterFunc != nil {
		typedRouterFunc := def.RouterFunc
		d.RouterFunc = func(ctx context.Context, nCtx NodeRunContext) (pipeline.RoutingInstruction, error) {
			var cfg C
			if err := bindFromMap(nCtx.Config, schemaJSON, &cfg); err != nil {
				return pipeline.RoutingInstruction{}, valErrWrap(ValErrInvalidConfig, nCtx.NodeID, "config bind failed for node "+nCtx.NodeID, err)
			}
			return typedRouterFunc(ctx, typedContext(nCtx, &cfg))
		}
	}

	// Wrap PipelinesRouterFunc: bind raw map → *C, call typed callback.
	if def.PipelinesRouterFunc != nil {
		typedPRF := def.PipelinesRouterFunc
		d.PipelinesRouterFunc = func(ctx context.Context, nCtx NodeRunContext, results []pipeline.PipelineRunResult) (pipeline.RoutingInstruction, error) {
			var cfg C
			if err := bindFromMap(nCtx.Config, schemaJSON, &cfg); err != nil {
				return pipeline.RoutingInstruction{}, valErrWrap(ValErrInvalidConfig, nCtx.NodeID, "config bind failed for node "+nCtx.NodeID, err)
			}
			return typedPRF(ctx, typedContext(nCtx, &cfg), results)
		}
	}

	// Wrap ResourceInit: bind raw map → *C, call typed callback.
	if def.ResourceInit != nil {
		typedInit := def.ResourceInit
		d.ResourceInit = func(ctx context.Context, nCtx NodeRunContext) (any, error) {
			var cfg C
			if err := bindFromMap(nCtx.Config, schemaJSON, &cfg); err != nil {
				return nil, valErrWrap(ValErrInvalidConfig, nCtx.NodeID, "config bind failed for node "+nCtx.NodeID, err)
			}
			return typedInit(ctx, typedContext(nCtx, &cfg))
		}
	}

	// Wrap ResourceEnd: bind raw map → *C, call typed callback.
	if def.ResourceEnd != nil {
		typedEnd := def.ResourceEnd
		d.ResourceEnd = func(ctx context.Context, nCtx NodeRunContext, handle any) error {
			var cfg C
			if err := bindFromMap(nCtx.Config, schemaJSON, &cfg); err != nil {
				return valErrWrap(ValErrInvalidConfig, nCtx.NodeID, "config bind failed for node "+nCtx.NodeID, err)
			}
			return typedEnd(ctx, typedContext(nCtx, &cfg), handle)
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
		return valErrWrap(ValErrInvalidConfig, "", "bind failed", err)
	}
	return nil
}
