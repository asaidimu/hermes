package pause

import (
	"context"
	"time"

	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/asaidimu/hermes/pkg/watch"
)

// PauseConfig is the typed configuration for the pause node.
type PauseConfig struct {
	WaitForEvent  string   `config:"waitForEvent"`
	WaitForEvents []string `config:"waitForEvents"`
	Mode          string   `config:"mode" anansi:"default=any"`
	Timeout       float64  `config:"timeout" anansi:"default=0"`
}

// @note #review-20260827-003 todo status=resolved priority=P2 tags=#review,#refactoring,#typesafety : Migrate pause node from untyped NodeDefinition to TypedDefinition[PauseConfig]
// @author antigravity
//
// Resolved: migrated to nodekit.Define(nodekit.TypedDefinition[PauseConfig]{...})
// with a tagged PauseConfig struct (waitForEvent, waitForEvents, mode,
// timeout). The hand-written ConfigSchema JSON is gone — schema is now
// derived from PauseConfig's struct tags, same as every other migrated
// node. Run and PipelinesRouterFunc now receive *nodekit.TypedRunContext
// [PauseConfig] and read nCtx.Config.WaitForEvents / .WaitForEvent / .Mode
// / .Timeout directly instead of unchecked map type assertions.
var Node = nodekit.Define(nodekit.TypedDefinition[PauseConfig]{
	Kind:        "pause",
	Label:       "Pause",
	Description: "Pause pipeline execution until specific event(s) arrive.",
	Type:        "executable",
	BodyHandle:  "do",
	Handles: func(cfg *PauseConfig) []nodekit.HandleSpec {
		return []nodekit.HandleSpec{
			{Type: nodekit.HandleTarget, ID: ""},
			{Type: nodekit.HandleSource, ID: "do", Label: "do"},
			{Type: nodekit.HandleSource, ID: "onResume", Label: "onResume"},
			{Type: nodekit.HandleSource, ID: "onTimeout", Label: "onTimeout"},
		}
	},
	HandlesJS: `() => [{"type":"target","id":"","kind":"executable"},{"type":"source","id":"do","label":"do","kind":"executable"},{"type":"source","id":"onResume","label":"onResume","kind":"executable"},{"type":"source","id":"onTimeout","label":"onTimeout","kind":"executable"}]`,
	Run: func(ctx context.Context, nCtx *nodekit.TypedRunContext[PauseConfig]) (store.Mutator, error) {
		// Get the WatchService from resources
		wsRaw, ok := nCtx.Resources["resource:watch-service"]
		if !ok {
			// @note #review-20260822-003 observation status=resolved priority=P2 tags=#review,#robustness : Silent nil return when WatchService unavailable
			//
			// Resolved: log a warning instead of returning silently. Still
			// returns (nil, nil) rather than a hard error — a pause node
			// with no WatchService configured is a host-configuration gap,
			// not a per-run failure, so we don't want to fail every run
			// that happens to include a pause node when the host simply
			// didn't wire watch support. The warning makes the gap visible
			// in logs instead of vanishing silently.
			if nCtx.Logger != nil {
				nCtx.Logger.Warn("pause node: watch-service resource not available; pause will be a no-op", "nodeId", nCtx.NodeID)
			}
			return nil, nil
		}
		watchService, ok := wsRaw.(watch.WatchService)
		if !ok {
			if nCtx.Logger != nil {
				nCtx.Logger.Warn("pause node: resource:watch-service is not a watch.WatchService; pause will be a no-op", "nodeId", nCtx.NodeID)
			}
			return nil, nil
		}

		// Build watch descriptor from typed config
		eventTypes := nCtx.Config.WaitForEvents
		if len(eventTypes) == 0 && nCtx.Config.WaitForEvent != "" {
			eventTypes = []string{nCtx.Config.WaitForEvent}
		}
		if len(eventTypes) == 0 {
			eventTypes = []string{"__pause__"}
		}

		// Register with WatchService (pre-pause buffering)
		// This happens BEFORE the body executes, so callbacks are caught
		if err := watchService.Register(nCtx.NodeID, watch.WatchDescriptor{
			EventTypes: eventTypes,
			Mode:       nCtx.Config.Mode,
			Timeout:    int64(nCtx.Config.Timeout),
		}); err != nil {
			return nil, err
		}

		return nil, nil
	},
	// PipelinesRouterFunc is called after the body completes.
	// It checks for buffered events and either resumes immediately or pauses.
	PipelinesRouterFunc: func(ctx context.Context, nCtx *nodekit.TypedRunContext[PauseConfig], results []pipeline.PipelineRunResult) (pipeline.RoutingInstruction, error) {
		// Get the WatchService from resources
		wsRaw, ok := nCtx.Resources["resource:watch-service"]
		if !ok {
			// @note #review-20260822-002 observation status=resolved priority=P2 tags=#review,#robustness : Silent nil return when WatchService unavailable
			//
			// Resolved: log a warning (see the matching note in Run above
			// for why this stays a no-op rather than a hard error).
			if nCtx.Logger != nil {
				nCtx.Logger.Warn("pause node: watch-service resource not available; skipping pause routing", "nodeId", nCtx.NodeID)
			}
			return nil, nil
		}
		watchService, ok := wsRaw.(watch.WatchService)
		if !ok {
			if nCtx.Logger != nil {
				nCtx.Logger.Warn("pause node: resource:watch-service is not a watch.WatchService; skipping pause routing", "nodeId", nCtx.NodeID)
			}
			return nil, nil
		}

		// Check if there's a buffered event
		if bufferedEvent, found := watchService.PeekBufferedEvent(nCtx.NodeID); found {
			// Buffered event found - merge payload into state and route to onResume
			for k, v := range bufferedEvent.Patch {
				// @note #review-20260822-046 issue status=resolved priority=P1 tags=#review,#error-handling : Store update errors silently discarded
				//
				// Resolved: log the error instead of silently discarding
				// it. Still continues to onResume on failure rather than
				// failing the stage outright — a store-update failure here
				// means one patched field may be stale, which the pipeline
				// author can now at least see in logs, versus previously
				// having no signal at all. Failing the whole stage on a
				// single patch-key error felt like too large a behavior
				// change to make blind, without tests to confirm no
				// pipeline relies on best-effort patching.
				if err := nCtx.Store.Update(ctx, store.SetValue(k, v)); err != nil && nCtx.Logger != nil {
					nCtx.Logger.Error("pause node: failed to apply resume patch key to store", "nodeId", nCtx.NodeID, "key", k, "error", err)
				}
			}
			return pipeline.Jump("onResume"), nil
		}

		// No buffered event - check for timeout
		if reason, _ := nCtx.State["__resume_reason__"].(string); reason == "timeout" {
			return pipeline.Jump("onTimeout"), nil
		}

		// No buffered event, no timeout - pause the pipeline
		return pipeline.PauseForEvent("__pause__", time.Duration(nCtx.Config.Timeout)*time.Millisecond), nil
	},
})

// @note #review-20260822-045 issue status=resolved priority=P1 tags=#review,#bug : Double registration of pause node
//
// Resolved: removed the init() that duplicated the registration nodes.go
// already performs. Node ownership is now single: nodes.go's registry-wiring
// init is the only place pause.Node gets registered.
