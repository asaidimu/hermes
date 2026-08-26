package nodekit

import (
	"context"
	"fmt"

	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
)

// prepareNodeConfig deep-interpolates the raw config against the live state and
// coerces it through the node's compiled anansi schema. This mirrors the TS
// coerceConfig(deepInterpolate(rawConfig, { state, results, resources })) used
// by buildStep/buildRouter/buildBoundedStage on every execution.
func prepareNodeConfig(def NodeDefinition, raw map[string]any, state, resources, results map[string]any) (map[string]any, error) {
	rs, schemaErr := CompileConfigSchema(def.ConfigSchema)
	if schemaErr != nil {
		return nil, core.NewSystemError(core.ErrCodeExecutionFailed, "node "+def.Kind+" has an invalid config schema").WithCause(schemaErr)
	}
	interpolated, err := Interpolate(raw, InterpolationContext{
		State:     state,
		Resources: resources,
		Results:   results,
	})
	if err != nil {
		return nil, core.NewSystemError(core.ErrCodeExecutionFailed, "config interpolation failed").WithCause(err)
	}
	// @note #review-20260822-035 issue status=open priority=P1 tags=#review,#bug : Discarded comma-ok on type assertion
	//
	// cfg, _ := interpolated.(map[string]any) discards the comma-ok error. If Interpolate
	// returns a non-map (e.g., a string), cfg is nil and passed to def.Run, which will
	// nil-dereference on cfg["anyKey"]. The `if cfg == nil` guard only protects against
	// literal nil, not wrong-type returns.
	cfg, _ := interpolated.(map[string]any)
	if cfg == nil {
		cfg = map[string]any{}
	}
	return CoerceConfig(cfg, rs), nil
}

// BuildTrigger builds the workflow trigger for a trigger node. Only the
// "trigger" kind has a buildTrigger implementation (mirrors utils registry).
func BuildTrigger(nodeID string, def NodeDefinition, config map[string]any) (*pipeline.WorkflowTrigger, error) {
	if def.Kind != "trigger" {
		return nil, core.NewSystemError(core.ErrCodeExecutionFailed,
			fmt.Sprintf("Trigger node %s of kind %q has no buildTrigger implementation", nodeID, def.Kind))
	}
	event := "__manual__"
	if e, ok := config["event"].(string); ok && e != "" {
		event = e
	}
	var cron string
	if c, ok := config["cron"].(string); ok && c != "" {
		cron = c
	}
	return &pipeline.WorkflowTrigger{
		ID:        nodeID,
		Event:     event,
		Predicate: func(events.PipelineEvent) bool { return true },
		Cron:      cron,
	}, nil
}

// BuildRouter wraps a node's NodeRouter into a pipeline.StepStageRouter. The
// config is re-interpolated against the live document on every invocation (loop
// iterations mutate state between visits). The returned handle id is resolved
// through resolveHandle into a target stage id; unresolvable/empty handles
// produce no instruction (the engine advances).
func BuildRouter(nodeID string, def NodeDefinition, config map[string]any, resolveHandle func(string) string) pipeline.StepStageRouter {
	return func(ctx context.Context, state map[string]any, st store.Store) (pipeline.RoutingInstruction, error) {
		if def.Router == nil {
			return nil, nil
		}
		var results map[string]any
		if r, ok := state["results"].(map[string]any); ok {
			results = r
		}
		cfg, err := prepareNodeConfig(def, config, state, nil, results)
		if err != nil {
			return nil, err
		}
		handle, err := def.Router(ctx, NodeRunContext{
			NodeID:  nodeID,
			Config:  cfg,
			State:   state,
			Results: results,
			Store:   st,
		})
		if err != nil {
			return nil, err
		}
		if handle == "" {
			return nil, nil
		}
		target := resolveHandle(handle)
		if target == "" {
			return nil, nil
		}
		return pipeline.Jump(target), nil
	}
}

// buildRouterFunc wraps a NodeRouterFunc into a pipeline.StepStageRouter.
// Unlike BuildRouter, it returns the RoutingInstruction directly without
// handle resolution, enabling non-jump routing (e.g. pause). When the node's
// router returns nil (no special instruction), the default edge is followed
// like a plain node; a terminal leaf (no outgoing edge) terminates.
func buildRouterFunc(nodeID string, def NodeDefinition, config map[string]any, resolveHandle func(string) string) pipeline.StepStageRouter {
	return func(ctx context.Context, state map[string]any, st store.Store) (pipeline.RoutingInstruction, error) {
		if def.RouterFunc == nil {
			return nil, nil
		}
		var results map[string]any
		if r, ok := state["results"].(map[string]any); ok {
			results = r
		}
		cfg, err := prepareNodeConfig(def, config, state, nil, results)
		if err != nil {
			return nil, err
		}
		inst, err := def.RouterFunc(ctx, NodeRunContext{
			NodeID:  nodeID,
			Config:  cfg,
			State:   state,
			Results: results,
			Store:   st,
		})
		if err != nil {
			return nil, err
		}
		if inst != nil {
			return inst, nil
		}
		target := resolveHandle("")
		if target == "" {
			return pipeline.Terminate(), nil
		}
		return pipeline.Jump(target), nil
	}
}

// defaultStageRouter advances to the default next target. It mirrors the TS
// default stage router (resolveHandle("") ?? null); the terminate-on-error
// branch is handled by the engine, which fails the pipeline on step errors
// before routing is reached.
func defaultStageRouter(config map[string]any, resolveHandle func(string) string) pipeline.StepStageRouter {
	return func(ctx context.Context, state map[string]any, st store.Store) (pipeline.RoutingInstruction, error) {
		target := resolveHandle("")
		if target == "" {
			// No outgoing edge: this stage is a terminal leaf (e.g. the end of
			// one branch of an if/try-catch). Terminate so the engine doesn't
			// advance into a sibling branch that was compiled into the flat
			// stage list by the BFS.
			return pipeline.Terminate(), nil
		}
		return pipeline.Jump(target), nil
	}
}

// BuildStage compiles a standard executable node into a single-step stage with
// either its own router (routing nodes: if/switch/while/for-each) or the
// default stage router.
func BuildStage(nodeID string, def NodeDefinition, config map[string]any, resources func() map[string]any, resolveHandle func(string) string, order int) pipeline.Stage {
	stage := pipeline.Stage{ID: nodeID, Order: order, Label: def.Label}
	if def.Run != nil {
		stage.Steps = map[string]pipeline.Step{nodeID: BuildStep(nodeID, def, config, resources)}
	}
	if def.RouterFunc != nil {
		stage.Router = buildRouterFunc(nodeID, def, config, resolveHandle)
	} else if def.Router != nil {
		stage.Router = BuildRouter(nodeID, def, config, resolveHandle)
	} else {
		stage.Router = defaultStageRouter(config, resolveHandle)
	}
	return stage
}

// BuildBoundedStage compiles a bodyHandle node (try-catch) into a pipelines-mode
// stage: a `<nodeId>__body` subpipeline (optional `<nodeId>__setup` step + the
// compiled body stages) whose results are routed by the node's router. Routing
// back to the bounded node's own handle (e.g. "try") loops back to this stage,
// re-running the body.
func BuildBoundedStage(nodeID string, def NodeDefinition, config map[string]any, resources func() map[string]any, resolveHandle func(string) string, order int, bodyStages []pipeline.Stage) pipeline.Stage {
	subStages := make([]pipeline.Stage, 0, len(bodyStages)+1)
	if def.Run != nil {
		subStages = append(subStages, pipeline.Stage{
			ID:    nodeID + "__setup",
			Order: 0,
			Label: def.Label + " (setup)",
			Steps: map[string]pipeline.Step{nodeID: BuildStep(nodeID, def, config, resources)},
			Router: func(ctx context.Context, state map[string]any, st store.Store) (pipeline.RoutingInstruction, error) {
				if len(bodyStages) == 0 {
					return nil, nil
				}
				return pipeline.Jump(bodyStages[0].ID), nil
			},
		})
	}
	subStages = append(subStages, bodyStages...)

	subPipeline := pipeline.PipelineDefinition{
		ID:     nodeID + "__body",
		Label:  def.Label + " body (" + nodeID + ")",
		Stages: subStages,
	}

	return pipeline.Stage{
		ID:        nodeID,
		Order:     order,
		Label:     def.Label,
		Pipelines: []pipeline.PipelineDefinition{subPipeline},
		PipelinesRouter: func(ctx context.Context, state map[string]any, results []pipeline.PipelineRunResult, st store.Store) (pipeline.RoutingInstruction, error) {
			errors := map[string]any{}
			resultsByID := map[string]any{}
			for _, r := range results {
				resultsByID[r.PipelineID] = map[string]any{
					"status": r.Status,
					"ok":     r.Status == "succeeded",
				}
				if r.Status == "failed" && r.Error != nil {
					errors[r.PipelineID] = r.Error
				}
			}
			res := resources()
			cfg, err := prepareNodeConfig(def, config, state, res, resultsByID)
			if err != nil {
				return nil, err
			}

			// If PipelinesRouterFunc is defined, use it directly
			if def.PipelinesRouterFunc != nil {
				return def.PipelinesRouterFunc(ctx, NodeRunContext{
					NodeID:    nodeID,
					Config:    cfg,
					State:     state,
					Results:   resultsByID,
					Errors:    errors,
					Resources: res,
					Store:     st,
				}, results)
			}

			// Default PipelinesRouter logic
			var handle string
			if def.Router != nil {
				handle, err = def.Router(ctx, NodeRunContext{
					NodeID:    nodeID,
					Config:    cfg,
					State:     state,
					Results:   resultsByID,
					Errors:    errors,
					Resources: res,
					Store:     st,
				})
				if err != nil {
					return nil, err
				}
			} else {
				if len(errors) > 0 {
					return nil, nil
				}
				handle = "done"
			}

			if handle == "" {
				return nil, nil
			}
			if def.BodyHandle != "" && handle == def.BodyHandle {
				return pipeline.Jump(nodeID), nil
			}
			target := resolveHandle(handle)
			if target == "" {
				return nil, nil
			}
			return pipeline.Jump(target), nil
		},
	}
}

// BuildForkStage creates a pipelines-mode stage from a fork node.
// Each branch becomes a sub-pipeline. After all complete, PipelinesRouter
// jumps to the join node.
func BuildForkStage(
	nodeID string,
	def NodeDefinition,
	order int,
	branchStages map[string][]pipeline.Stage,
	joinID string,
) pipeline.Stage {
	subPipelines := make([]pipeline.PipelineDefinition, 0, len(branchStages))
	for handle, stages := range branchStages {
		subPipelines = append(subPipelines, pipeline.PipelineDefinition{
			ID:     nodeID + "__" + handle,
			Label:  def.Label + " branch " + handle,
			Stages: stages,
		})
	}

	return pipeline.Stage{
		ID:        nodeID,
		Order:     order,
		Label:     def.Label,
		Pipelines: subPipelines,
		PipelinesRouter: func(ctx context.Context, state map[string]any, results []pipeline.PipelineRunResult, st store.Store) (pipeline.RoutingInstruction, error) {
			if joinID == "" {
				return pipeline.Advance(), nil
			}
			return pipeline.Jump(joinID), nil
		},
	}
}

// BuildDistributeStage compiles a distribute (parallel for-each) node into a
// pipelines-mode stage. A single sub-pipeline contains:
//   - a setup stage that runs the node's logic (captures items in state)
//   - a pipelines stage with DynamicPipelines that spawns one child per array element
//
// Config keys:
//   - itemsKey: state path to the array (default "items")
//   - itemKey:  variable name injected into each child's state (default "item")
func BuildDistributeStage(
	nodeID string,
	def NodeDefinition,
	config map[string]any,
	resources func() map[string]any,
	resolveHandle func(string) string,
	order int,
	bodyStages []pipeline.Stage,
) pipeline.Stage {
	itemKey, _ := config["itemKey"].(string)
	if itemKey == "" {
		itemKey = "item"
	}

	// --- Setup stage: runs node logic, stores items for DynamicPipelines ---
	setupStage := pipeline.Stage{
		ID:    nodeID + "__setup",
		Order: 0,
		Label: def.Label + " (setup)",
		Steps: map[string]pipeline.Step{nodeID: BuildStep(nodeID, def, config, resources)},
		Router: func(ctx context.Context, state map[string]any, st store.Store) (pipeline.RoutingInstruction, error) {
			if len(bodyStages) == 0 {
				return nil, nil
			}
			return pipeline.Jump(bodyStages[0].ID), nil
		},
	}

	// --- Pipelines stage: DynamicPipelines fans out N children --------------
	pipelinesStage := pipeline.Stage{
		ID:    nodeID + "__pipelines",
		Order: 1,
		Label: def.Label + " (distribute)",
		DynamicPipelines: func(state map[string]any) []pipeline.PipelineDefinition {
			// Read items from state (set by distribute node's Run).
			key := "__$" + nodeID + "__items__"
			var items []any
			switch v := state[key].(type) {
			case []any:
				items = v
			default:
				return nil
			}
			if len(items) == 0 {
				return nil
			}

			subPipelines := make([]pipeline.PipelineDefinition, 0, len(items))
			for i, item := range items {
				idx := i
				val := item
				itemVal := map[string]any{
					"index": idx,
					"value": val,
				}

				// Clone body stages so each child owns its copy.
				cloned := make([]pipeline.Stage, len(bodyStages))
				copy(cloned, bodyStages)

				childStages := make([]pipeline.Stage, 0, len(cloned)+1)
				// Inject the item before the body runs.
				childStages = append(childStages, pipeline.Stage{
					ID:    nodeID + "__item",
					Order: 0,
					Label: def.Label + " item",
					Steps: map[string]pipeline.Step{
						nodeID + "__inject": {
							ID:    nodeID + "__inject",
							Label: "inject item",
							Action: func(ctx context.Context, pcxt pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
								return func(state map[string]any) error {
									state[itemKey] = itemVal
									return nil
								}, nil
							},
						},
					},
					Router: func(ctx context.Context, state map[string]any, st store.Store) (pipeline.RoutingInstruction, error) {
						if len(cloned) == 0 {
							return nil, nil
						}
						return pipeline.Jump(cloned[0].ID), nil
					},
				})
				childStages = append(childStages, cloned...)

				subPipelines = append(subPipelines, pipeline.PipelineDefinition{
					ID:     fmt.Sprintf("%s__%d", nodeID, idx),
					Label:  fmt.Sprintf("%s item %d", def.Label, idx),
					Stages: childStages,
				})
			}
			return subPipelines
		},
		PipelinesRouter: func(ctx context.Context, state map[string]any, results []pipeline.PipelineRunResult, st store.Store) (pipeline.RoutingInstruction, error) {
			target := resolveHandle("done")
			if target == "" {
				return pipeline.Advance(), nil
			}
			return pipeline.Jump(target), nil
		},
	}

	// Wrap setup + pipelines in a single sub-pipeline so the engine runs
	// setup first, then enters pipelines mode for the fan-out.
	subPipeline := pipeline.PipelineDefinition{
		ID:     nodeID + "__distribute",
		Label:  def.Label + " distribute (" + nodeID + ")",
		Stages: []pipeline.Stage{setupStage, pipelinesStage},
	}

	return pipeline.Stage{
		ID:        nodeID,
		Order:     order,
		Label:     def.Label,
		Pipelines: []pipeline.PipelineDefinition{subPipeline},
		PipelinesRouter: func(ctx context.Context, state map[string]any, results []pipeline.PipelineRunResult, st store.Store) (pipeline.RoutingInstruction, error) {
			target := resolveHandle("done")
			if target == "" {
				return pipeline.Advance(), nil
			}
			return pipeline.Jump(target), nil
		},
	}
}
