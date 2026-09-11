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
	Kind:   "query",
	Effect: nodekit.EffectSideEffecting,
	Label:  "Database Query",
	// Experimental: the node has no resource-backed implementation yet —
	// the compiler REJECTS workflows that use it (see the resolved
	// #review-20260910-016 note) so dead canvases fail at authoring time,
	// not at run time.
	Description: "[EXPERIMENTAL — not yet implemented] Execute a query against a database service collection.",
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
	// Resolution of #review-20260910-016 (the canonical note lives at the
	// compile gate in pkg/compiler compileStages): workflows using the query
	// node are rejected at COMPILE time, the README catalog marks it
	// experimental, and the definition description carries the same marker.
	// This runtime path remains as the defense-in-depth backstop (direct
	// factory use bypasses the compiler): the failure is still loud, never a
	// silent no-op. The resource lookup quirk noted at review time stands
	// documented: buildResourcesFor keys resources by source KIND
	// ("database"), not by the "resource:<id>" artifact keys.
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
