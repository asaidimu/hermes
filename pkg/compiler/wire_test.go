package compiler

import (
	"encoding/json"
	"testing"
)

const wireDoc = `{
  "nodes": [
    {"id":"trigger-1","type":"executable","position":{"x":1,"y":2},"data":{"kind":"trigger","config":{}}},
    {"id":"res-1","type":"resource","position":{"x":0,"y":0},"data":{"kind":"database","config":{"dsn":"x"}}},
    {"id":"cont-1","type":"container","position":{"x":0,"y":0},"data":{"kind":"fork","config":{}}},
    {"id":"pause-1","type":"pause","position":{"x":0,"y":0},"data":{"kind":"pause","config":{}}}
  ],
  "edges": [
    {"id":"e1","source":"trigger-1","target":"res-1","data":{"role":"flow"}},
    {"id":"e2","source":"res-1","target":"cont-1","data":{"role":"dependency"}},
    {"id":"e3","source":"cont-1","target":"pause-1","data":{"role":"placeholder"}},
    {"id":"e4","source":"pause-1","target":"trigger-1"}
  ]
}`

func TestDecodeWireGraph(t *testing.T) {
	nodes, edges, err := DecodeWireGraph([]byte(wireDoc))
	if err != nil {
		t.Fatalf("DecodeWireGraph: %v", err)
	}
	if len(nodes) != 4 {
		t.Fatalf("nodes = %d, want 4", len(nodes))
	}
	if len(edges) != 4 {
		t.Fatalf("edges = %d, want 4", len(edges))
	}

	byID := make(map[string]Node)
	for _, n := range nodes {
		byID[n.ID] = n
	}
	if byID["trigger-1"].Kind != "trigger" {
		t.Errorf("trigger kind = %q", byID["trigger-1"].Kind)
	}
	if byID["trigger-1"].Type != NodeExecutable {
		t.Errorf("trigger type = %q", byID["trigger-1"].Type)
	}
	if byID["res-1"].Type != NodeResource {
		t.Errorf("resource type = %q", byID["res-1"].Type)
	}
	if byID["cont-1"].Type != NodeContainer {
		t.Errorf("container type = %q", byID["cont-1"].Type)
	}
	if byID["pause-1"].Type != NodePause {
		t.Errorf("pause type = %q", byID["pause-1"].Type)
	}
	if byID["trigger-1"].Position.X != 1 || byID["trigger-1"].Position.Y != 2 {
		t.Errorf("position = %+v", byID["trigger-1"].Position)
	}

	byEdge := make(map[string]Edge)
	for _, e := range edges {
		byEdge[e.ID] = e
	}
	if byEdge["e1"].Role != EdgeFlow {
		t.Errorf("e1 role = %q", byEdge["e1"].Role)
	}
	if byEdge["e2"].Role != EdgeDependency {
		t.Errorf("e2 role = %q", byEdge["e2"].Role)
	}
	if byEdge["e3"].Role != EdgePlaceholder {
		t.Errorf("e3 role = %q", byEdge["e3"].Role)
	}
	// Missing role defaults to flow.
	if byEdge["e4"].Role != EdgeFlow {
		t.Errorf("e4 role = %q, want flow", byEdge["e4"].Role)
	}
}

func TestDecodeWireGraphInvalidJSON(t *testing.T) {
	if _, _, err := DecodeWireGraph([]byte(`{bad`)); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestCompileWire(t *testing.T) {
	doc := `{
  "nodes": [
    {"id":"trigger-1","type":"executable","position":{"x":0,"y":0},"data":{"kind":"trigger","config":{}}},
    {"id":"code-1","type":"executable","position":{"x":0,"y":0},"data":{"kind":"code","config":{"code":"return {done:true};"}}}
  ],
  "edges": [
    {"id":"e1","source":"trigger-1","target":"code-1","data":{"role":"flow"}}
  ]
}`
	wf, err := CompileWire([]byte(doc), nil)
	if err != nil {
		t.Fatalf("CompileWire: %v", err)
	}
	if wf == nil || len(wf.Pipelines) == 0 {
		t.Fatal("expected compiled pipelines")
	}

	view := WorkflowView(wf)
	raw, err := json.Marshal(view)
	if err != nil {
		t.Fatalf("WorkflowView not JSON-serializable: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("WorkflowView round-trip: %v", err)
	}
}
