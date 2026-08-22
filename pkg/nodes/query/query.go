package query

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/store"
)

var Node = nodekit.NodeDefinition{
	Kind:        "query",
	Label:       "Database Query",
	Description: "Execute a query against a database service collection.",
	Type:        "executable",
	ConfigSchema: json.RawMessage(`{
		"version": "1.0.0",
		"name": "query",
		"fields": {
			"collection": { "name": "collection", "type": "string", "default": "", "required": true },
			"operation":  { "name": "operation", "type": "string", "default": "find", "required": true },
			"query":      { "name": "query", "type": "record", "default": {}, "required": false },
			"data":       { "name": "data", "type": "record", "default": {}, "required": false },
			"key":        { "name": "key", "type": "string", "default": "", "required": false }
		}
	}`),
	Handles: func(config map[string]any) []nodekit.HandleSpec {
		return []nodekit.HandleSpec{
			{Type: nodekit.HandleTarget, ID: "", Kind: nodekit.HandleExecutable},
			{Type: nodekit.HandleSource, ID: "", Kind: nodekit.HandleExecutable},
			{Type: nodekit.HandleTarget, ID: "service", Kind: nodekit.HandleResource, Label: "Database Service"},
		}
	},
	HandlesJS: `() => [{"type":"target","id":"","kind":"executable"},{"type":"source","id":"","kind":"executable"},{"type":"target","id":"service","kind":"resource","label":"Database Service"}]`,
	Run: run,
}

// run guards on the database resource handle, mirroring the TS query node. The
// database resource contract and its collection operations are on hold (see
// WIP/todo) and will be wired here when they land.
func run(ctx context.Context, nCtx nodekit.NodeRunContext) (store.DocumentMutator, error) {
	db := nCtx.Resources["database"]
	if db == nil {
		return nil, fmt.Errorf(
			"Query node %q requires a database service. Connect a Database Service node via a dependency edge.",
			nCtx.NodeID,
		)
	}
	_ = db // collection ops land with the database resource (Phase 5)
	return nil, fmt.Errorf("Query node %q: database resource not yet implemented", nCtx.NodeID)
}