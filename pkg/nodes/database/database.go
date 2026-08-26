package database

import (
	"github.com/asaidimu/hermes/pkg/nodekit"
)

type DatabaseConfig struct {
	DatabaseName string `config:"databaseName" anansi:"default=workflow"`
}

var Node = nodekit.Define(nodekit.TypedDefinition[DatabaseConfig]{
	Kind:        "database",
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
