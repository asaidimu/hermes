package trycatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/store"
)

var Node = nodekit.NodeDefinition{
	Kind:        "try-catch",
	Label:       "Try / Catch",
	Description: "Execute a sub-flow and catch any errors it raises.",
	Type:        "executable",
	BodyHandle:  "try",
	ConfigSchema: json.RawMessage(`{
		"version": "1.0.0",
		"name": "try-catch",
		"fields": {
			"errorKey": { "name": "errorKey", "type": "string", "default": "error", "required": true }
		}
	}`),
	Handles: func(config map[string]any) []nodekit.HandleSpec {
		return []nodekit.HandleSpec{
			{Type: nodekit.HandleTarget, ID: ""},
			{Type: nodekit.HandleSource, ID: "done", Label: "done"},
			{Type: nodekit.HandleSource, ID: "catch", Label: "catch"},
			{Type: nodekit.HandleSource, ID: "try", Label: "try"},
		}
	},
	HandlesJS: `() => [{"type":"target","id":"","kind":"executable"},{"type":"source","id":"done","label":"done","kind":"executable"},{"type":"source","id":"catch","label":"catch","kind":"executable"},{"type":"source","id":"try","label":"try","kind":"executable"}]`,
	Run:       run,
	Router:    router,
}

// run mirrors the TS try-catch run: aggregates any errors seen by the step into
// a single SystemError JSON written at errorKey. The engine only surfaces errors
// to routers (buildStep does not pass errors), so this is effectively a no-op
// during normal execution — kept for parity.
func run(ctx context.Context, nCtx nodekit.NodeRunContext) (store.DocumentMutator, error) {
	errorKey, _ := nCtx.Config["errorKey"].(string)
	if errorKey == "" {
		errorKey = "error"
	}
	errorList := filterErrors(nCtx.Errors)
	if len(errorList) == 0 {
		return nil, nil
	}
	finalErr := buildFinalError(nCtx.NodeID, errorList)
	return nodekit.PatchMutator(map[string]any{errorKey: nodekit.SystemErrorJSON(finalErr)}), nil
}

// router mirrors the TS try-catch router: when the sub-pipeline produced errors
// it writes the aggregated SystemError JSON at errorKey (via the store) and
// routes to "catch"; otherwise routes to "done".
func router(ctx context.Context, nCtx nodekit.NodeRunContext) (string, error) {
	errorKey, _ := nCtx.Config["errorKey"].(string)
	if errorKey == "" {
		errorKey = "error"
	}
	errorList := filterErrors(nCtx.Errors)
	if len(errorList) == 0 {
		return "done", nil
	}
	finalErr := buildFinalError(nCtx.NodeID, errorList)
	if nCtx.Store != nil {
		if err := nCtx.Store.Update(ctx, store.SetValue(errorKey, nodekit.SystemErrorJSON(finalErr))); err != nil {
			return "", err
		}
	}
	return "catch", nil
}

func filterErrors(errors map[string]any) []any {
	out := make([]any, 0, len(errors))
	for _, e := range errors {
		if e != nil {
			out = append(out, e)
		}
	}
	return out
}

func buildFinalError(nodeID string, errorList []any) *core.SystemError {
	if len(errorList) == 1 {
		if se, ok := errorList[0].(*core.SystemError); ok {
			return se
		}
		var cause error
		if e, ok := errorList[0].(error); ok {
			cause = e
		} else {
			cause = core.NewSystemError("INTERNAL_ERROR", fmt.Sprintf("%v", errorList[0]))
		}
		return core.NewSystemError("INTERNAL_ERROR",
			"Try-Catch node \""+nodeID+"\": execution failed.").
			WithCause(cause)
	}
	errs := make([]error, 0, len(errorList))
	for _, e := range errorList {
		if err, ok := e.(error); ok {
			errs = append(errs, err)
		}
	}
	return core.NewSystemError("INTERNAL_ERROR",
		"Try-Catch node \""+nodeID+"\": parallel tracks in sub-pipeline failed.").
		WithCause(errors.Join(errs...))
}
