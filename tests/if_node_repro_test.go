package tests

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/asaidimu/hermes/pkg/server"
	"github.com/stretchr/testify/require"

	_ "github.com/asaidimu/hermes/pkg/nodes"
)

// userIfWorkflow is the exact graph the user reported: arithmetic 42+34 -> if
// total < 70 -> two delay branches (if/else).
func userIfWorkflow() map[string]any {
	return map[string]any{
		"nodes": []map[string]any{
			{"id": "trigger-1", "type": "executable", "position": map[string]any{"x": 500, "y": 10},
				"data": map[string]any{"kind": "trigger", "config": map[string]any{"initialState": map[string]any{}}}},
			{"id": "e5bf4655-c338-464a-b4d5-076348f4b2d1", "type": "executable", "position": map[string]any{"x": 730, "y": 10},
				"data": map[string]any{"kind": "arithmetic", "config": map[string]any{"left": "42", "right": "34", "key": "total"}}},
			{"id": "c695c6f9-56db-4d52-bf90-9eb843f3eba1", "type": "executable", "position": map[string]any{"x": 970, "y": 10},
				"data": map[string]any{"kind": "if", "config": map[string]any{
					"conditions": []any{map[string]any{"field": "total", "operator": "less_than", "value": "70"}}}}},
			{"id": "eb6e3969-360c-4384-b0ee-f24aab95a69f", "type": "executable", "position": map[string]any{"x": 1210, "y": -110},
				"data": map[string]any{"kind": "delay", "config": map[string]any{}}},
			{"id": "ba17639b-18b8-451b-83c0-86e680d056b6", "type": "executable", "position": map[string]any{"x": 1210, "y": 130},
				"data": map[string]any{"kind": "delay", "config": map[string]any{}}},
		},
		"edges": []map[string]any{
			{"id": "0f3f40d5-3391-4ba8-91f0-de6db18e00e2", "source": "trigger-1", "target": "e5bf4655-c338-464a-b4d5-076348f4b2d1",
				"type": "adaptive", "data": map[string]any{"role": "flow"}},
			{"id": "3209b5bd-fef2-4e12-ab6a-b5220cd45a01", "source": "e5bf4655-c338-464a-b4d5-076348f4b2d1", "target": "c695c6f9-56db-4d52-bf90-9eb843f3eba1",
				"type": "adaptive", "sourceHandle": "", "data": map[string]any{"role": "flow"}},
			{"id": "a2e6dca0-d75a-448e-a77d-aa2b46abddaf", "source": "c695c6f9-56db-4d52-bf90-9eb843f3eba1", "target": "eb6e3969-360c-4384-b0ee-f24aab95a69f",
				"type": "adaptive", "sourceHandle": "if", "data": map[string]any{"role": "flow"}},
			{"id": "88909105-638b-422e-8ff7-07e62d5907c7", "source": "c695c6f9-56db-4d52-bf90-9eb843f3eba1", "target": "ba17639b-18b8-451b-83c0-86e680d056b6",
				"type": "adaptive", "sourceHandle": "else", "data": map[string]any{"role": "flow"}},
		},
	}
}

// logEvents dumps the run event sequence so failures are self-diagnosing.
func logEvents(t *testing.T, evs []map[string]any) {
	t.Helper()
	for _, ev := range evs {
		step := ""
		if p, ok := ev["payload"].(map[string]any); ok {
			if s, ok := p["stepId"].(string); ok {
				step = s
			}
		}
		t.Logf("EVENT type=%s step=%s payload=%v", ev["type"], step, ev["payload"])
	}
}

// runWorkflow posts a graph and waits for the outcome, returning the events.
func runWorkflow(t *testing.T, handler http.Handler, graph map[string]any) []map[string]any {
	t.Helper()
	body, _ := json.Marshal(graph)
	req := httptest.NewRequest("POST", "/run", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var runRes map[string]string
	_ = json.Unmarshal(w.Body.Bytes(), &runRes)
	runID := runRes["runId"]

	deadline := time.Now().Add(5 * time.Second)
	for {
		req = httptest.NewRequest("GET", "/runs/"+runID+"/outcome", nil)
		w = httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		var outcome map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &outcome)
		if ok, _ := outcome["ok"].(bool); ok {
			t.Logf("OUTCOME status=%v finalState=%v", outcome["status"], outcome["finalState"])
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for outcome")
		}
		time.Sleep(10 * time.Millisecond)
	}

	req = httptest.NewRequest("GET", "/runs/"+runID+"/events", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	var evs []map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &evs)
	return evs
}

func TestIfNodeUserWorkflowReport(t *testing.T) {
	srv := server.NewPipelineServer(server.ServerConfig{})
	handler := srv.Handler()

	evs := runWorkflow(t, handler, userIfWorkflow())
	logEvents(t, evs)

	// Assert: 42+34=76, and 76 < 70 is false -> the "else" delay (ba17639b...)
	// must run and the "if" delay (eb6e3969...) must not.
	ifRan, elseRan := branchRuns(evs)
	require.True(t, elseRan, "else (false) branch delay should have run: 76 < 70 is false")
	require.False(t, ifRan, "if (true) branch delay should NOT have run: 76 < 70 is false")
}

// TestIfNodeFlippedSign proves the if node can route to "if": flip less_than to
// greater_than, so 76 > 70 is true and the "if" branch must run instead.
func TestIfNodeFlippedSign(t *testing.T) {
	srv := server.NewPipelineServer(server.ServerConfig{})
	handler := srv.Handler()

	graph := userIfWorkflow()
	conds := graph["nodes"].([]map[string]any)[2]["data"].(map[string]any)["config"].(map[string]any)["conditions"].([]any)
	conds[0].(map[string]any)["operator"] = "greater_than"

	evs := runWorkflow(t, handler, graph)
	logEvents(t, evs)

	ifRan, elseRan := branchRuns(evs)
	require.True(t, ifRan, "if (true) branch delay should have run: 76 > 70 is true")
	require.False(t, elseRan, "else (false) branch delay should NOT have run: 76 > 70 is true")
}

// branchRuns reports which delay branches executed based on step:success events.
func branchRuns(evs []map[string]any) (ifRan, elseRan bool) {
	for _, ev := range evs {
		payload, _ := ev["payload"].(map[string]any)
		step, _ := payload["stepId"].(string)
		if ev["type"] == "step:success" {
			if step == "eb6e3969-360c-4384-b0ee-f24aab95a69f" {
				ifRan = true
			}
			if step == "ba17639b-18b8-451b-83c0-86e680d056b6" {
				elseRan = true
			}
		}
	}
	return
}