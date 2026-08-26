package compiler_test

import (
	"testing"

	"github.com/asaidimu/hermes/pkg/compiler"
	"github.com/asaidimu/hermes/pkg/nodekit"
	_ "github.com/asaidimu/hermes/pkg/nodes"
)

func TestCalcSampleCompiles(t *testing.T) {
	nodes := []compiler.Node{
		{ID: "trigger-1", Type: compiler.NodeExecutable, Kind: "trigger", Config: map[string]any{"initialState": map[string]any{}}},
		{ID: "3bebb024-9018-4c34-be69-71e154d3c1f2", Type: compiler.NodeExecutable, Kind: "while", Config: map[string]any{"mode": "simple", "condition": map[string]any{"key": "total", "predicate": "greater_equals", "value": "10"}}},
		{ID: "8ac61a7e-ef84-4bc5-86fe-816106f97a33", Type: compiler.NodeExecutable, Kind: "arithmetic", Config: map[string]any{"left": "total", "right": "1", "operation": "subtract", "key": "total"}},
		{ID: "c68d280e-78e5-427a-bd56-d548161c44d3", Type: compiler.NodeExecutable, Kind: "delay", Config: map[string]any{}},
		{ID: "3e46f1a5-97b1-41d5-ad42-65d49a1812b7", Type: compiler.NodeExecutable, Kind: "transformer", Config: map[string]any{"rules": []any{map[string]any{"targetKey": "total", "sourceKey": "", "action": "SET_VALUE", "actionParam": "11"}}}},
		{ID: "63771c46-c2da-4f77-a177-c286a945327d", Type: compiler.NodeExecutable, Kind: "if", Config: map[string]any{"conditions": []any{map[string]any{"field": "total", "operator": "equals", "value": "10"}}}},
		{ID: "136c5f83-d327-4c34-8e85-a719bed844fd", Type: compiler.NodeExecutable, Kind: "arithmetic", Config: map[string]any{"left": "total", "right": "2", "operation": "multiply", "key": "total"}},
		{ID: "c4963b9c-9dc4-41cb-a83c-39393da73034", Type: compiler.NodeExecutable, Kind: "delay", Config: map[string]any{}},
	}
	edges := []compiler.Edge{
		{ID: "c32b3a77", Source: "3bebb024-9018-4c34-be69-71e154d3c1f2", Target: "8ac61a7e-ef84-4bc5-86fe-816106f97a33", SourceHandle: "do", Role: compiler.EdgeFlow},
		{ID: "cc0ee56f", Source: "3bebb024-9018-4c34-be69-71e154d3c1f2", Target: "c68d280e-78e5-427a-bd56-d548161c44d3", SourceHandle: "done", Role: compiler.EdgeFlow},
		{ID: "2e28b963", Source: "8ac61a7e-ef84-4bc5-86fe-816106f97a33", Target: "3bebb024-9018-4c34-be69-71e154d3c1f2", Role: compiler.EdgeFlow},
		{ID: "d13b024d", Source: "trigger-1", Target: "3e46f1a5-97b1-41d5-ad42-65d49a1812b7", Role: compiler.EdgeFlow},
		{ID: "902752ea", Source: "3e46f1a5-97b1-41d5-ad42-65d49a1812b7", Target: "3bebb024-9018-4c34-be69-71e154d3c1f2", Role: compiler.EdgeFlow},
		{ID: "0670e55b", Source: "c68d280e-78e5-427a-bd56-d548161c44d3", Target: "63771c46-c2da-4f77-a177-c286a945327d", Role: compiler.EdgeFlow},
		{ID: "0d4a3781", Source: "63771c46-c2da-4f77-a177-c286a945327d", Target: "136c5f83-d327-4c34-8e85-a719bed844fd", SourceHandle: "if", Role: compiler.EdgeFlow},
		{ID: "ad3d3ace", Source: "63771c46-c2da-4f77-a177-c286a945327d", Target: "c4963b9c-9dc4-41cb-a83c-39393da73034", SourceHandle: "else", Role: compiler.EdgeFlow},
	}

	_ = nodekit.NodeDefinition{} // ensure import
	wf, err := compiler.Compile(nodes, edges, nil)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}
	stages := wf.Pipelines["trigger-1"].Stages
	t.Logf("Compiled %d stages:", len(stages))
	for _, s := range stages {
		t.Logf("  %s (kind=%s) order=%d", s.ID, s.Label, s.Order)
	}
}
