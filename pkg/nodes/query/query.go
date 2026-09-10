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
	Effect:      nodekit.EffectSideEffecting,
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
	// @note #review-20260910-016 issue status=open priority=P2 tags=#review,#docs,#stub : Node is an always-failing stub but the README lists it as production-ready
	// @author hermes-review
	// @see #review-20260910-017
	//
	// Every execution returns "database resource not yet implemented" —
	// yet README.md's Built-in Node Catalog documents `query` as a working
	// production node ("Executes queries against a connected service
	// resource"). Users drawn to the catalog will wire dependency edges and
	// fail at runtime with no config-time signal (the node validates fine;
	// the failure only surfaces during execution). Also note the lookup key:
	// buildResourcesFor maps resources by source KIND, so the resolved map
	// is keyed "database" — this works today by coincidence with the kind
	// name, not via the "resource:<id>" artifact keys the compiler docs
	// describe. Either implement the node or hide it from the catalog and
	// README until it works.
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
