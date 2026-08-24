package pause

import (
	"context"
	"encoding/json"
	"time"

	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
	"github.com/asaidimu/hermes/pkg/watch"
)

var Node = nodekit.NodeDefinition{
	Kind:        "pause",
	Label:       "Pause",
	Description: "Pause pipeline execution until specific event(s) arrive.",
	Type:        "executable",
	BodyHandle:  "do",
	ConfigSchema: json.RawMessage(`{
		"version": "1.0.0",
		"name": "pause",
		"fields": {
			"waitForEvent": { "name": "waitForEvent", "type": "string" },
			"waitForEvents": { "name": "waitForEvents", "type": "array", "items": { "type": "string" } },
			"mode": { "name": "mode", "type": "string", "default": "any", "enum": ["any", "all"] },
			"timeout": { "name": "timeout", "type": "number", "default": 0 }
		}
	}`),
	Handles: func(config map[string]any) []nodekit.HandleSpec {
		return []nodekit.HandleSpec{
			{Type: nodekit.HandleTarget, ID: ""},
			{Type: nodekit.HandleSource, ID: "do", Label: "do"},
			{Type: nodekit.HandleSource, ID: "onResume", Label: "onResume"},
			{Type: nodekit.HandleSource, ID: "onTimeout", Label: "onTimeout"},
		}
	},
	HandlesJS: `() => [{"type":"target","id":"","kind":"executable"},{"type":"source","id":"do","label":"do","kind":"executable"},{"type":"source","id":"onResume","label":"onResume","kind":"executable"},{"type":"source","id":"onTimeout","label":"onTimeout","kind":"executable"}]`,
	Run: func(ctx context.Context, nCtx nodekit.NodeRunContext) (store.DocumentMutator, error) {
		// Get the WatchService from resources
		wsRaw, ok := nCtx.Resources["resource:watch-service"]
		if !ok {
			// @note #review-20260822-003 observation status=open priority=P2 tags=#review,#robustness : Silent nil return when WatchService unavailable
			//
			// When the WatchService is not available, this function returns nil without an error.
			// This could make debugging difficult in production. Consider returning an error
			// or logging a warning when the WatchService is unavailable.
			return nil, nil
		}
		watchService, ok := wsRaw.(watch.WatchService)
		if !ok {
			return nil, nil
		}

		// Build watch descriptor from config
		var eventTypes []string
		if eventsRaw, ok := nCtx.Config["waitForEvents"].([]any); ok {
			for _, e := range eventsRaw {
				if s, ok := e.(string); ok && s != "" {
					eventTypes = append(eventTypes, s)
				}
			}
		}
		if len(eventTypes) == 0 {
			if waitForEvent, _ := nCtx.Config["waitForEvent"].(string); waitForEvent != "" {
				eventTypes = []string{waitForEvent}
			}
		}
		if len(eventTypes) == 0 {
			eventTypes = []string{"__pause__"}
		}

		mode, _ := nCtx.Config["mode"].(string)
		timeoutMs, _ := nCtx.Config["timeout"].(float64)

		// Register with WatchService (pre-pause buffering)
		// This happens BEFORE the body executes, so callbacks are caught
		watchService.Register(nCtx.NodeID, watch.WatchDescriptor{
			EventTypes: eventTypes,
			Mode:       mode,
			Timeout:    int64(timeoutMs),
		})

		return nil, nil
	},
	// PipelinesRouterFunc is called after the body completes.
	// It checks for buffered events and either resumes immediately or pauses.
	PipelinesRouterFunc: func(ctx context.Context, nCtx nodekit.NodeRunContext, results []pipeline.PipelineRunResult) (pipeline.RoutingInstruction, error) {
		// Get the WatchService from resources
		wsRaw, ok := nCtx.Resources["resource:watch-service"]
		if !ok {
			// @note #review-20260822-002 observation status=open priority=P2 tags=#review,#robustness : Silent nil return when WatchService unavailable
			//
			// When the WatchService is not available, this function returns nil without an error.
			// This could make debugging difficult in production. Consider returning an error
			// or logging a warning when the WatchService is unavailable.
			return nil, nil
		}
		watchService, ok := wsRaw.(watch.WatchService)
		if !ok {
			return nil, nil
		}

		// Check if there's a buffered event
		if bufferedEvent := watchService.PeekBufferedEvent(nCtx.NodeID); bufferedEvent != nil {
			// Buffered event found - merge payload into state and route to onResume
			for k, v := range bufferedEvent.Patch {
				// @note #review-20260822-046 issue status=open priority=P1 tags=#review,#error-handling : Store update errors silently discarded
				//
				// _ = nCtx.Store.Update(ctx, store.SetValue(k, v)) discards the store update
				// error. If the patch fails, the router still jumps to onResume with stale
				// state. At minimum, log the error; ideally, propagate it.
				_ = nCtx.Store.Update(ctx, store.SetValue(k, v))
			}
			return pipeline.Jump("onResume"), nil
		}

		// No buffered event - check for timeout
		if reason, _ := nCtx.State["__resume_reason__"].(string); reason == "timeout" {
			return pipeline.Jump("onTimeout"), nil
		}

		// No buffered event, no timeout - pause the pipeline
		timeoutMs, _ := nCtx.Config["timeout"].(float64)
		return pipeline.PauseForEvent("__pause__", time.Duration(timeoutMs)*time.Millisecond), nil
	},
}

func init() {
	// @note #review-20260822-045 issue status=open priority=P1 tags=#review,#bug : Double registration of pause node
	//
	// nodes.go registers pause.Node on line 31, and pause.go also registers itself in its
	// own init() on line 125. Since Register overwrites by kind, this is a harmless race
	// but signals confusion about ownership. Remove the init() in pause.go.
	nodekit.Register(Node)
}
