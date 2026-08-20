package tests

import (
	"encoding/json"
	"testing"
)

func TestIfDbg2(t *testing.T) {
	graph := userIfWorkflow()
	conds := graph["nodes"].([]map[string]any)[2]["data"].(map[string]any)["config"].(map[string]any)["conditions"].([]any)
	conds[0].(map[string]any)["operator"] = "greater_than"
	b, _ := json.Marshal(graph)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	nodes := m["nodes"].([]any)
	ifCfg := nodes[2].(map[string]any)["data"].(map[string]any)["config"].(map[string]any)
	t.Logf("if config: %v", ifCfg)
}
