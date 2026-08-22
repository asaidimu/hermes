package transformer

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/store"
)

var Node = nodekit.NodeDefinition{
	Kind:        "transformer",
	Label:       "Transformer",
	Description: "Manipulate workflow state by applying transformation rules.",
	Type:        "executable",
	ConfigSchema: json.RawMessage(`{
		"version": "1.0.0",
		"name": "transformer",
		"fields": {
			"rules": { "name": "rules", "type": "array", "schema": { "type": "record" }, "required": true, "default": [] }
		}
	}`),
	Handles: func(config map[string]any) []nodekit.HandleSpec {
		return []nodekit.HandleSpec{
			{Type: nodekit.HandleTarget, ID: ""},
			{Type: nodekit.HandleSource, ID: ""},
		}
	},
	HandlesJS: `() => [{"type":"target","id":"","kind":"executable"},{"type":"source","id":"","kind":"executable"}]`,
	Run: run,
}

// run mirrors the TS transformer node: applies each rule to a working copy of
// state, collecting the resulting nested patch and DELETE_FIELD deletions
// (represented with the nodekit.Delete sentinel). The final patch is deep-merged
// into the document by the engine.
func run(ctx context.Context, nCtx nodekit.NodeRunContext) (store.DocumentMutator, error) {
	rules, _ := nCtx.Config["rules"].([]any)

	workingState := nodekit.MergeMaps(nCtx.State, nil)
	masterPatch := map[string]any{}
	var pendingDeletes []string

	for _, rule := range rules {
		rm, ok := rule.(map[string]any)
		if !ok {
			continue
		}
		targetKey, _ := rm["targetKey"].(string)
		if targetKey == "" {
			continue
		}
		action, _ := rm["action"].(string)
		sourceKey, _ := rm["sourceKey"].(string)
		actionParam, _ := rm["actionParam"].(string)

		if action == "DELETE_FIELD" {
			pendingDeletes = append(pendingDeletes, targetKey)
			deletePath(workingState, targetKey)
			continue
		}

		sourceValue, sourcePresent := nodekit.GetNested(workingState, sourceKey)
		calculated, present := executeTransform(action, sourceValue, sourcePresent, actionParam, workingState, targetKey)
		if !present {
			// Matches TS `if (calculatedValue === undefined) continue;`.
			continue
		}

		localRulePatch := nestedPatch(targetKey, calculated)
		workingState = nodekit.MergeMaps(workingState, localRulePatch)
		masterPatch = nodekit.MergeMaps(masterPatch, localRulePatch)
	}

	for _, targetKey := range pendingDeletes {
		setDeleteSentinel(masterPatch, targetKey)
	}

	return nodekit.PatchMutator(masterPatch), nil
}

// nestedPatch builds the nested object path for a dotted target key with the
// given leaf value, mirroring the TS localRulePatch construction.
func nestedPatch(targetKey string, value any) map[string]any {
	parts := splitPath(targetKey)
	root := map[string]any{}
	cur := root
	for i := 0; i < len(parts)-1; i++ {
		next := map[string]any{}
		cur[parts[i]] = next
		cur = next
	}
	cur[parts[len(parts)-1]] = value
	return root
}

// setDeleteSentinel marks a dotted path in the master patch with the Delete
// sentinel, creating intermediate objects as needed.
func setDeleteSentinel(patch map[string]any, targetKey string) {
	parts := splitPath(targetKey)
	cur := patch
	for i := 0; i < len(parts)-1; i++ {
		next, ok := cur[parts[i]].(map[string]any)
		if !ok {
			next = map[string]any{}
			cur[parts[i]] = next
		}
		cur = next
	}
	cur[parts[len(parts)-1]] = nodekit.Delete
}

// deletePath removes a dotted path from a map (no-op when a segment is missing),
// mirroring the TS delete in the DELETE_FIELD branch.
func deletePath(m map[string]any, path string) {
	parts := splitPath(path)
	if len(parts) == 1 {
		delete(m, parts[0])
		return
	}
	cur := m
	for i := 0; i < len(parts)-1; i++ {
		next, ok := cur[parts[i]].(map[string]any)
		if !ok {
			return
		}
		cur = next
	}
	delete(cur, parts[len(parts)-1])
}

func splitPath(path string) []string {
	return strings.Split(path, ".")
}