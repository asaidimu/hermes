package foreach

import (
	"context"
	"encoding/json"

	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/store"
)

var Node = nodekit.NodeDefinition{
	Kind:        "for-each",
	Label:       "For Each / Iterator",
	Description: "Iterate over an array or object collection step-by-step.",
	Type:        "executable",
	ConfigSchema: json.RawMessage(`{
		"version": "1.0.0",
		"name": "for-each",
		"fields": {
			"itemsKey": { "name": "itemsKey", "type": "string", "default": "items", "required": true },
			"itemKey":  { "name": "itemKey", "type": "string", "default": "item", "required": true }
		}
	}`),
	Handles: func(config map[string]any) []nodekit.HandleSpec {
		return []nodekit.HandleSpec{
			{Type: nodekit.HandleTarget, ID: ""},
			{Type: nodekit.HandleSource, ID: "done", Label: "done"},
			{Type: nodekit.HandleSource, ID: "do", Label: "do"},
		}
	},
	HandlesJS: `() => [{"type":"target","id":"","kind":"executable"},{"type":"source","id":"done","label":"done","kind":"executable"},{"type":"source","id":"do","label":"do","kind":"executable"}]`,
	Run:       run,
	Router:    router,
}

// run mirrors the TS for-each node: maintains an iterator state machine at
// `__$<nodeId>__items__`, shifting one entry per execution into the itemKey and
// clearing the internal state when exhausted.
func run(ctx context.Context, nCtx nodekit.NodeRunContext) (store.DocumentMutator, error) {
	cfg := nCtx.Config
	itemsKey, _ := cfg["itemsKey"].(string)
	itemKey, _ := cfg["itemKey"].(string)
	internalKey := "__$" + nCtx.NodeID + "__items__"

	var internalState map[string]any
	if is, ok := nCtx.State[internalKey].(map[string]any); ok {
		internalState = is
	}
	patch := make(map[string]any)
	if internalState == nil {
		items, _ := nodekit.GetNested(nCtx.State, itemsKey)
		switch t := items.(type) {
		case []any:
			internalState = map[string]any{"type": "array", "entries": t}
		case map[string]any:
			entries := make([]any, 0, len(t))
			for _, v := range t {
				entries = append(entries, v)
			}
			internalState = map[string]any{"type": "object", "entries": entries}
		default:
			internalState = map[string]any{"type": "array", "entries": []any{}}
		}
	}

	entries := entriesCopy(internalState["entries"])
	var value any
	hasValue := false
	if len(entries) > 0 {
		value = entries[0]
		entries = entries[1:]
		hasValue = true
	}

	patch[internalKey] = map[string]any{
		"type":     internalState["type"],
		"entries":  entries,
		"hasValue": hasValue,
	}
	if hasValue {
		patch[itemKey] = value
	} else {
		if itemKey != "" {
			patch[itemKey] = nodekit.Delete
		}
		patch[internalKey] = nodekit.Delete
	}

	return nodekit.PatchMutator(patch), nil
}

// router returns "do" while the iterator has a pending value, "done" otherwise.
func router(ctx context.Context, nCtx nodekit.NodeRunContext) (string, error) {
	internalKey := "__$" + nCtx.NodeID + "__items__"
	if is, ok := nCtx.State[internalKey].(map[string]any); ok {
		if hv, ok := is["hasValue"].(bool); ok && hv {
			return "do", nil
		}
	}
	return "done", nil
}

func entriesCopy(v any) []any {
	if arr, ok := v.([]any); ok {
		out := make([]any, len(arr))
		copy(out, arr)
		return out
	}
	return []any{}
}
