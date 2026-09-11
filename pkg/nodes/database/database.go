package database

import (
	"github.com/asaidimu/hermes/pkg/nodekit"
)

type DatabaseConfig struct {
	DatabaseName string `config:"databaseName" anansi:"default=workflow"`
}

var Node = nodekit.Define(nodekit.TypedDefinition[DatabaseConfig]{
	Kind:   "database",
	Effect: nodekit.EffectSideEffecting,
	Label:  "Database Service",
	// Experimental: this resource node publishes no usable handle yet (no
	// ResourceInit — see the resolved #review-20260910-017 note), and the
	// query node that would consume it is compile-time gated
	// (#review-20260910-016). Kept registered for editor compatibility.
	Description: "[EXPERIMENTAL — handle not initialized] Provides a database instance to workflow nodes via the artifact container.",
	Type:        "resource",
	Handles: func(cfg *DatabaseConfig) []nodekit.HandleSpec {
		return []nodekit.HandleSpec{
			{Type: nodekit.HandleSource, ID: "db", Kind: nodekit.HandleResource},
		}
	},
	HandlesJS: `() => [{"type":"source","id":"db","kind":"resource"}]`,
})

// @note #review-20260910-017 issue status=resolved priority=P2 tags=#review,#docs,#resources : Resource node never initialized a handle — the database/query pair was inert
// @author hermes-review
// @see #review-20260910-016
//
// Resolved via the "mark both kinds as experimental in the catalog" branch
// of this note. The node still has no ResourceInit/ResourceEnd: the compiler
// wraps resource nodes in a pipeline.Service whose Init returns (nil, nil)
// when def.ResourceInit == nil, so the runtime resolver published a nil
// handle under "resource:<id>" and any consumer took its error path — while
// the README advertised "database + query" workflows (fetch, enrich, and
// persist data on recurring intervals). Now: (1) the compiler rejects the
// `query` consumer at compile time (#review-20260910-016), so a dead
// db+query canvas cannot compile; (2) both kinds are marked
// "[EXPERIMENTAL — ...]" in their definitions and the README catalog, so
// authoring surfaces no longer present them as production-ready; (3) the
// README's scheduled-pipelines use case no longer implies working database
// persistence. Wiring a real ResourceInit (anansi persistence per
// DatabaseName) remains the future path to un-experimentalize the pair.
