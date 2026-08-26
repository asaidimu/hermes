package query

import (
	"context"
	"fmt"

	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/store"
)

type QueryConfig struct {
	Collection string         `config:"collection"`
	Operation  string         `config:"operation" anansi:"default=find"`
	Query      map[string]any `config:"query"`
	Data       map[string]any `config:"data"`
	Key        string         `config:"key"`
}

var Node = nodekit.Define(nodekit.TypedDefinition[QueryConfig]{
	Kind:        "query",
	Label:       "Database Query",
	Description: "Execute a query against a database service collection.",
	Type:        "executable",
	Handles: func(cfg *QueryConfig) []nodekit.HandleSpec {
		return []nodekit.HandleSpec{
			{Type: nodekit.HandleTarget, ID: "", Kind: nodekit.HandleExecutable},
			{Type: nodekit.HandleSource, ID: "", Kind: nodekit.HandleExecutable},
			{Type: nodekit.HandleTarget, ID: "service", Kind: nodekit.HandleResource, Label: "Database Service"},
		}
	},
	HandlesJS: `() => [{"type":"target","id":"","kind":"executable"},{"type":"source","id":"","kind":"executable"},{"type":"target","id":"service","kind":"resource","label":"Database Service"}]`,
	Run:       run,
})

func run(ctx context.Context, nCtx *nodekit.TypedRunContext[QueryConfig]) (store.Mutator, error) {
	db := nCtx.Resources["database"]
	if db == nil {
		return nil, fmt.Errorf(
			"Query node %q requires a database service. Connect a Database Service node via a dependency edge.",
			nCtx.NodeID,
		)
	}
	_ = db
	return nil, fmt.Errorf("Query node %q: database resource not yet implemented", nCtx.NodeID)
}
