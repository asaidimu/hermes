package database

import (
	"github.com/asaidimu/hermes/pkg/nodekit"
)

type DatabaseConfig struct {
	DatabaseName string `config:"databaseName" anansi:"default=workflow"`
}

var Node = nodekit.Define(nodekit.TypedDefinition[DatabaseConfig]{
	Kind:        "database",
	Effect:      nodekit.EffectSideEffecting,
	Label:       "Database Service",
	Description: "Provides a database instance to workflow nodes via the artifact container.",
	Type:        "resource",
	Handles: func(cfg *DatabaseConfig) []nodekit.HandleSpec {
		return []nodekit.HandleSpec{
			{Type: nodekit.HandleSource, ID: "db", Kind: nodekit.HandleResource},
		}
	},
	HandlesJS: `() => [{"type":"source","id":"db","kind":"resource"}]`,
})

// @note #review-20260910-017 issue status=open priority=P2 tags=#review,#docs,#resources : Resource node never initializes a handle — the database/query pair is inert
// @author hermes-review
// @see #review-20260910-016
//
// This node has no ResourceInit/ResourceEnd: the compiler wraps resource
// nodes in a pipeline.Service whose Init returns (nil, nil) when
// def.ResourceInit == nil, so the runtime resolver publishes a nil handle
// under "resource:<id>", and the query node (which reads nCtx.Resources)
// always takes its error path. The README still advertises "database +
// query" workflows (fetch, enrich, and persist data on recurring
// intervals). Either wire ResourceInit to a real connection (anansi
// persistence per DatabaseName), or mark both kinds as experimental in the
// catalog so canvases cannot compile dead workflows.
