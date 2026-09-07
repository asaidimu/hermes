package compiler

import (
	"encoding/json"
	"fmt"

	"github.com/asaidimu/hermes/pkg/pipeline"
)

// Wire types mirror the TS WorkflowNode/WorkflowEdge shapes posted by
// visual workflow canvases. kind/config live inside node data; role lives
// in edge data. Previously decoded inside pkg/server (now removed); they
// live here so canvas JSON can reach the compiler without an HTTP layer.

// WireNode is a single canvas node.
type WireNode struct {
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Data     WireNodeData `json:"data"`
	ParentID string       `json:"parentId,omitempty"`
	Position WirePosition `json:"position"`
}

// WireNodeData carries the node kind and its configuration.
type WireNodeData struct {
	Kind   string         `json:"kind"`
	Config map[string]any `json:"config"`
}

// WirePosition is the canvas position. Carried through compilation but
// unused by the execution engine.
type WirePosition struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

// WireEdge is a single canvas edge.
type WireEdge struct {
	ID           string        `json:"id"`
	Source       string        `json:"source"`
	SourceHandle string        `json:"sourceHandle,omitempty"`
	Target       string        `json:"target"`
	TargetHandle string        `json:"targetHandle,omitempty"`
	Data         *WireEdgeData `json:"data,omitempty"`
}

// WireEdgeData carries the edge role.
type WireEdgeData struct {
	Role string `json:"role"`
}

// wireGraph is the top-level canvas document: flat node and edge lists.
type wireGraph struct {
	Nodes []WireNode `json:"nodes"`
	Edges []WireEdge `json:"edges"`
}

// ToNode maps a wire node onto the compiler's Node view.
func ToNode(n WireNode) Node {
	node := Node{
		ID:       n.ID,
		Kind:     n.Data.Kind,
		Config:   n.Data.Config,
		ParentID: n.ParentID,
	}
	switch n.Type {
	case "resource":
		node.Type = NodeResource
	case "container":
		node.Type = NodeContainer
	case "pause":
		node.Type = NodePause
	default:
		node.Type = NodeExecutable
	}
	node.Position.X = n.Position.X
	node.Position.Y = n.Position.Y
	return node
}

// ToEdge maps a wire edge onto the compiler's Edge view.
func ToEdge(e WireEdge) Edge {
	edge := Edge{
		ID:           e.ID,
		Source:       e.Source,
		Target:       e.Target,
		SourceHandle: e.SourceHandle,
		Role:         EdgeFlow,
	}
	if e.Data != nil {
		switch e.Data.Role {
		case "dependency":
			edge.Role = EdgeDependency
		case "placeholder":
			edge.Role = EdgePlaceholder
		}
	}
	return edge
}

// DecodeWireGraph parses a canvas JSON document ({nodes, edges}) into the
// compiler's Node/Edge view. It performs translation only — no validation
// beyond JSON decoding; Compile reports structural problems.
func DecodeWireGraph(data []byte) ([]Node, []Edge, error) {
	var body wireGraph
	if err := json.Unmarshal(data, &body); err != nil {
		return nil, nil, fmt.Errorf("decode wire graph: %w", err)
	}
	nodes := make([]Node, 0, len(body.Nodes))
	for _, n := range body.Nodes {
		nodes = append(nodes, ToNode(n))
	}
	edges := make([]Edge, 0, len(body.Edges))
	for _, e := range body.Edges {
		edges = append(edges, ToEdge(e))
	}
	return nodes, edges, nil
}

// CompileWire decodes a canvas JSON document and compiles it into a
// runnable Workflow in one call.
func CompileWire(data []byte, registry pipeline.PipelineRegistry) (*pipeline.Workflow, error) {
	nodes, edges, err := DecodeWireGraph(data)
	if err != nil {
		return nil, err
	}
	return Compile(nodes, edges, registry)
}

// WorkflowView renders a compiled Workflow as plain JSON-compatible maps
// (metadata only — runtime funcs are not serializable). Preserves the
// shape the old POST /compile endpoint served.
func WorkflowView(wf *pipeline.Workflow) map[string]any {
	triggers := map[string]any{}
	for id, tr := range wf.Triggers {
		triggers[id] = map[string]any{"id": tr.ID, "event": tr.Event}
	}
	pipelines := map[string]any{}
	for id, pd := range wf.Pipelines {
		stages := make([]any, 0, len(pd.Stages))
		for _, st := range pd.Stages {
			stages = append(stages, map[string]any{
				"id":            st.ID,
				"label":         st.Label,
				"order":         st.Order,
				"stepCount":     len(st.Steps),
				"pipelineCount": len(st.Pipelines),
			})
		}
		pipelines[id] = map[string]any{"id": pd.ID, "label": pd.Label, "stages": stages}
	}
	services := make([]any, 0, len(wf.Services))
	for _, svc := range wf.Services {
		services = append(services, map[string]any{
			"id":    svc.ID,
			"scope": svc.Scope,
			"kind":  svc.Kind,
			"label": svc.Label,
		})
	}
	return map[string]any{
		"id":        wf.ID,
		"label":     wf.Label,
		"triggers":  triggers,
		"pipelines": pipelines,
		"services":  services,
	}
}
