package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/asaidimu/hermes/pkg/compiler"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/runtime"
	"github.com/asaidimu/hermes/pkg/timeline"
	"github.com/stretchr/testify/require"
)

const rawWorkflowJSON = `{
  "nodes": [
    {"id":"trigger-1","type":"executable","position":{"x":960,"y":-170},"data":{"kind":"trigger","config":{"initialState":{}}}},
    {"id":"3bebb024-9018-4c34-be69-71e154d3c1f2","type":"executable","position":{"x":1480,"y":-170},"data":{"kind":"while","config":{"mode":"simple","condition":{"key":"total","predicate":"greater_equals","value":"10"}}}},
    {"id":"8ac61a7e-ef84-4bc5-86fe-816106f97a33","type":"executable","position":{"x":1780,"y":-20},"data":{"kind":"arithmetic","config":{"left":"total","right":"1","operation":"subtract","key":"total"}}},
    {"id":"c68d280e-78e5-427a-bd56-d548161c44d3","type":"executable","position":{"x":1780,"y":-280},"data":{"kind":"delay","config":{}}},
    {"id":"3e46f1a5-97b1-41d5-ad42-65d49a1812b7","type":"executable","position":{"x":1230,"y":-170},"data":{"kind":"transformer","config":{"rules":[{"targetKey":"total","sourceKey":"","action":"SET_VALUE","actionParam":"11"}]}}},
    {"id":"a40c3676-9f34-4294-be39-a5efa47caaec","type":"executable","position":{"x":2020,"y":-280},"data":{"kind":"fork","config":{}}},
    {"id":"2c54f25c-1cff-4359-bcf5-190fc84f91e8","type":"executable","position":{"x":2740,"y":-280},"data":{"kind":"join","config":{}}},
    {"id":"e9034a36-34bc-41d1-96cf-db9b70c71433","type":"executable","position":{"x":2380,"y":-420},"data":{"kind":"arithmetic","config":{"left":"total","operation":"multiply","right":"2","key":"twice"}}},
    {"id":"2a79c145-57c6-4f86-805e-3b51f9fb9512","type":"executable","position":{"x":2380,"y":-140},"data":{"kind":"arithmetic","config":{"left":"total","operation":"multiply","right":"3","key":"thrice"}}}
  ],
  "edges": [
    {"id":"c32b3a77-c60a-4463-91e3-eefe4fbddbf4","source":"3bebb024-9018-4c34-be69-71e154d3c1f2","target":"8ac61a7e-ef84-4bc5-86fe-816106f97a33","type":"adaptive","sourceHandle":"do","data":{"role":"flow"}},
    {"id":"cc0ee56f-182c-4f1c-aad6-74b4a1acb66a","source":"3bebb024-9018-4c34-be69-71e154d3c1f2","target":"c68d280e-78e5-427a-bd56-d548161c44d3","type":"adaptive","sourceHandle":"done","data":{"role":"flow"}},
    {"id":"2e28b963-2f12-4f72-bd65-99ffa9275f7d","source":"8ac61a7e-ef84-4bc5-86fe-816106f97a33","target":"3bebb024-9018-4c34-be69-71e154d3c1f2","type":"adaptive","data":{"role":"flow"}},
    {"id":"d13b024d-25af-452e-a4b9-1693c92369a4","source":"trigger-1","target":"3e46f1a5-97b1-41d5-ad42-65d49a1812b7","type":"adaptive","sourceHandle":"","targetHandle":"","data":{"role":"flow"}},
    {"id":"902752ea-7b27-4fb4-bee0-99cad2b6a3e0","source":"3e46f1a5-97b1-41d5-ad42-65d49a1812b7","target":"3bebb024-9018-4c34-be69-71e154d3c1f2","type":"adaptive","sourceHandle":"","data":{"role":"flow"}},
    {"id":"7321b4a6-b2a4-4289-88a9-da7c5a510eab","source":"c68d280e-78e5-427a-bd56-d548161c44d3","target":"a40c3676-9f34-4294-be39-a5efa47caaec","type":"adaptive","sourceHandle":"","data":{"role":"flow"}},
    {"id":"f1587949-d188-45f3-a3e1-9c86bab55d79","source":"e9034a36-34bc-41d1-96cf-db9b70c71433","target":"2c54f25c-1cff-4359-bcf5-190fc84f91e8","type":"adaptive","data":{"role":"flow"}},
    {"id":"1663cb6d-742f-4e96-9cca-122308a778a1","source":"a40c3676-9f34-4294-be39-a5efa47caaec","target":"e9034a36-34bc-41d1-96cf-db9b70c71433","type":"adaptive","sourceHandle":"do","data":{"role":"flow"}},
    {"id":"294226bb-62c1-402b-bd09-a95b8f030c57","source":"a40c3676-9f34-4294-be39-a5efa47caaec","target":"2a79c145-57c6-4f86-805e-3b51f9fb9512","type":"adaptive","sourceHandle":"do","data":{"role":"flow"}},
    {"id":"7fd6bfdf-5eab-4473-aecb-fe623bc77531","source":"2a79c145-57c6-4f86-805e-3b51f9fb9512","target":"2c54f25c-1cff-4359-bcf5-190fc84f91e8","type":"adaptive","data":{"role":"flow"}}
  ]
}`

type wirePayload struct {
	Nodes []struct {
		ID       string `json:"id"`
		Type     string `json:"type"`
		ParentID string `json:"parentId,omitempty"`
		Position struct {
			X float64 `json:"x"`
			Y float64 `json:"y"`
		} `json:"position"`
		Data struct {
			Kind   string         `json:"kind"`
			Config map[string]any `json:"config"`
		} `json:"data"`
	} `json:"nodes"`
	Edges []struct {
		ID           string `json:"id"`
		Source       string `json:"source"`
		Target       string `json:"target"`
		SourceHandle string `json:"sourceHandle"`
		Data         struct {
			Role string `json:"role"`
		} `json:"data"`
	} `json:"edges"`
}

func getPathStr(path events.EventPath) string {
	var parts []string
	for _, n := range path {
		parts = append(parts, fmt.Sprintf("%s(%s:%s)", n.Kind, n.ID, n.Label))
	}
	return fmt.Sprintf("%v", parts)
}

func TestRunForkWhileWorkflow(t *testing.T) {
	var payload wirePayload
	err := json.Unmarshal([]byte(rawWorkflowJSON), &payload)
	require.NoError(t, err)

	nodes := make([]compiler.Node, 0, len(payload.Nodes))
	for _, n := range payload.Nodes {
		var pos struct {
			X float64
			Y float64
		}
		pos.X = n.Position.X
		pos.Y = n.Position.Y
		nodes = append(nodes, compiler.Node{
			ID:       n.ID,
			Type:     compiler.NodeType(n.Type),
			Kind:     n.Data.Kind,
			Config:   n.Data.Config,
			ParentID: n.ParentID,
			Position: pos,
		})
	}

	edges := make([]compiler.Edge, 0, len(payload.Edges))
	for _, e := range payload.Edges {
		edges = append(edges, compiler.Edge{
			ID:           e.ID,
			Source:       e.Source,
			Target:       e.Target,
			SourceHandle: e.SourceHandle,
			Role:         compiler.EdgeRole(e.Data.Role),
		})
	}

	rt := runtime.NewWorkflowRuntime(runtime.Options{
		Timeline: timeline.NewMemoryTimelineStore(),
	})

	// Subscribe to all bus events to log every stage
	unsubscribe := rt.Bus().Subscribe("*", func(ctx context.Context, ev events.PipelineEvent) error {
		pathStr := getPathStr(ev.Path)
		switch ev.Type {
		case "stage:start":
			fmt.Printf("[STAGE START] Path: %s, Payload: %+v\n", pathStr, ev.Payload)
		case "stage:success":
			fmt.Printf("[STAGE SUCCESS] Path: %s, NextInstruction: %+v, Duration: %dms\n", pathStr, ev.Payload["nextInstruction"], ev.Duration)
		case "stage:failure":
			fmt.Printf("[STAGE FAILURE] Path: %s, Error: %+v\n", pathStr, ev.Payload["error"])
		case "subpipeline:fork":
			fmt.Printf("[SUBPIPELINE FORK] Path: %s, StageID: %v, SubPipelines: %v\n", pathStr, ev.Payload["stageId"], ev.Payload["subPipelineIds"])
		case "subpipeline:join":
			fmt.Printf("[SUBPIPELINE JOIN] Path: %s, StageID: %v, Results: %+v\n", pathStr, ev.Payload["stageId"], ev.Payload["results"])
		case "step:start":
			fmt.Printf("  [STEP START] Path: %s, Payload: %+v\n", pathStr, ev.Payload)
		case "step:success":
			fmt.Printf("  [STEP SUCCESS] Path: %s, Mutators: %+v\n", pathStr, ev.Payload["mutators"])
		}
		return nil
	})
	defer unsubscribe()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := rt.Run(ctx, nodes, edges)
	require.NoError(t, err)

	fmt.Printf("\n=== FINAL RUN RESULT ===\nStatus: %s\nFinal State: %+v\n", result.Status, result.FinalState)
}
