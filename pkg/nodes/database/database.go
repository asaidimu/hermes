package database

import (
	"encoding/json"

	"github.com/asaidimu/hermes/pkg/nodekit"
)

var Node = nodekit.NodeDefinition{
	Kind:        "database",
	Label:       "Database Service",
	Description: "Provides a database instance to workflow nodes via the artifact container.",
	Type:        "resource",
	ConfigSchema: json.RawMessage(`{
		"version": "1.0.0",
		"name": "database",
		"fields": {
			"databaseName": { "name": "databaseName", "type": "string", "default": "workflow", "required": true }
		}
	}`),
	Handles: func(config map[string]any) []nodekit.HandleSpec {
		return []nodekit.HandleSpec{
			{Type: nodekit.HandleSource, ID: "db", Kind: nodekit.HandleResource},
		}
	},
}