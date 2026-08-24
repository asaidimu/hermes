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

// wireNodeEdge is a helper to build the TS wire graph the client posts.
func wireGraph() map[string]any {
	return map[string]any{
		"nodes": []map[string]any{
			{
				"id":   "trig-1",
				"type": "executable",
				"data": map[string]any{
					"kind":   "trigger",
					"config": map[string]any{"initialState": map[string]any{"hello": "world", "count": 42}},
				},
				"position": map[string]any{"x": 0, "y": 0},
			},
		},
		"edges": []map[string]any{},
	}
}

func TestFrontendRESTServerRuntimeAPI(t *testing.T) {
	srv := server.NewPipelineServer(server.ServerConfig{})
	handler := srv.Handler()

	// CORS preflight is accepted.
	req := httptest.NewRequest("OPTIONS", "/run", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusNoContent, w.Code)
	require.Equal(t, "*", w.Header().Get("Access-Control-Allow-Origin"))

	// POST /run accepts {nodes, edges} in the TS wire format.
	body, _ := json.Marshal(wireGraph())
	req = httptest.NewRequest("POST", "/run", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var runRes map[string]string
	_ = json.Unmarshal(w.Body.Bytes(), &runRes)
	runID := runRes["runId"]
	require.NotEmpty(t, runID)

	// Poll until the background run records its outcome.
	var outcome map[string]any
	deadline := time.Now().Add(5 * time.Second)
	for {
		req = httptest.NewRequest("GET", "/runs/"+runID+"/outcome", nil)
		w = httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code)
		_ = json.Unmarshal(w.Body.Bytes(), &outcome)
		if ok, _ := outcome["ok"].(bool); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for run outcome")
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.Equal(t, "succeeded", outcome["status"])
	fs, ok := outcome["finalState"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "world", fs["hello"])
	require.Equal(t, float64(42), fs["count"])

	// GET /runs/:runId
	req = httptest.NewRequest("GET", "/runs/"+runID, nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var meta map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &meta)
	require.Equal(t, runID, meta["runId"])

	// GET /runs/:runId/events
	req = httptest.NewRequest("GET", "/runs/"+runID+"/events", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var eventsList []map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &eventsList)
	require.NotEmpty(t, eventsList)

	// GET /runs/:runId/store
	req = httptest.NewRequest("GET", "/runs/"+runID+"/store", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var storeData map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &storeData)
	require.Equal(t, "world", storeData["hello"])

	// GET /runs lists the run.
	req = httptest.NewRequest("GET", "/runs", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var runs []map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &runs)
	require.NotEmpty(t, runs)

	// POST /runs/:runId/abort is accepted (run already done).
	req = httptest.NewRequest("POST", "/runs/"+runID+"/abort", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// GET /registry returns the node catalog (kind -> definition).
	req = httptest.NewRequest("GET", "/registry", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var reg map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &reg)
	require.Contains(t, reg, "trigger")
	require.Contains(t, reg, "arithmetic")

	// GET /handles.js is a JS object literal of handle functions.
	req = httptest.NewRequest("GET", "/handles.js", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "application/javascript; charset=utf-8", w.Header().Get("Content-Type"))
	js := w.Body.String()
	require.Contains(t, js, `"trigger":`)
	require.Contains(t, js, `"switch":`)
	require.Contains(t, js, `"if":`)

	// POST /compile returns a metadata view of the compiled workflow.
	body, _ = json.Marshal(wireGraph())
	req = httptest.NewRequest("POST", "/compile", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var compiled map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &compiled)
	require.NotEmpty(t, compiled["id"])
	pipes := compiled["pipelines"].(map[string]any)
	require.Contains(t, pipes, "trig-1")

	// POST /register + POST /deregister + POST /events round-trip.
	body, _ = json.Marshal(map[string]any{
		"workflow": map[string]any{
			"id":    "registered-1",
			"nodes": wireGraph()["nodes"],
			"edges": wireGraph()["edges"],
		},
	})
	req = httptest.NewRequest("POST", "/register", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	req = httptest.NewRequest("POST", "/deregister", bytes.NewReader([]byte(`{"workflowId":"registered-1"}`)))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	req = httptest.NewRequest("POST", "/events", bytes.NewReader([]byte(`{"type":"ping","payload":{"n":1}}`)))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Removed legacy endpoints 404.
	req = httptest.NewRequest("GET", "/handles", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusNotFound, w.Code, "legacy endpoint should be removed")

	// Unknown run 404s.
	req = httptest.NewRequest("GET", "/runs/nope/outcome", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestFrontendRESTServerBadGraph(t *testing.T) {
	srv := server.NewPipelineServer(server.ServerConfig{})
	handler := srv.Handler()

	// No trigger node -> compile error surfaced on POST /run.
	body, _ := json.Marshal(map[string]any{
		"nodes": []map[string]any{
			{"id": "n1", "type": "executable", "data": map[string]any{"kind": "if", "config": map[string]any{}}, "position": map[string]any{"x": 0, "y": 0}},
		},
		"edges": []map[string]any{},
	})
	req := httptest.NewRequest("POST", "/run", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
	var errResp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &errResp)
	require.NotEmpty(t, errResp["error"])
}
