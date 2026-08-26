package pipeleref

import (
	"context"
	"encoding/json"

	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
)

var Node = nodekit.NodeDefinition{
	Kind:        "pipeline-ref",
	Label:       "Pipeline Reference",
	Description: "Invoke a registered sub-pipeline with fresh state and optional result merging.",
	Type:        "executable",
	ConfigSchema: json.RawMessage(`{
		"version": "1.0.0",
		"name": "pipeline-ref",
		"fields": {
			"pipelineId": { "name": "pipelineId", "type": "string", "required": true },
			"initialState": { "name": "initialState", "type": "record" },
			"resultKey": { "name": "resultKey", "type": "string" }
		}
	}`),
	Handles: func(config map[string]any) []nodekit.HandleSpec {
		return []nodekit.HandleSpec{
			{Type: nodekit.HandleTarget, ID: ""},
			{Type: nodekit.HandleSource, ID: "success", Label: "Success"},
			{Type: nodekit.HandleSource, ID: "failure", Label: "Failure"},
		}
	},
	HandlesJS:  `() => [{"type":"target","id":"","kind":"executable"},{"type":"source","id":"success","label":"Success","kind":"executable"},{"type":"source","id":"failure","label":"Failure","kind":"executable"}]`,
	Run:        run,
	RouterFunc: routerFunc,
}

// run is a no-op. The sub-pipeline is executed at stage level by ExecuteSubPipelines.
func run(ctx context.Context, nCtx nodekit.NodeRunContext) (store.Mutator, error) {
	return nil, nil
}

// routerFunc routes to the "success" or "failure" handle based on the
// sub-pipeline result. The compiler wires this via PipelinesRouter on the
// stage, so this function is not called directly at runtime. It exists for
// UI handle display and registry metadata.
func routerFunc(ctx context.Context, nCtx nodekit.NodeRunContext) (pipeline.RoutingInstruction, error) {
	return pipeline.Terminate(), nil
}
