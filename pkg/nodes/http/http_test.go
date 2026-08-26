package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/asaidimu/hermes/pkg/nodekit"
)

func httpRun(ctx context.Context, nodeID string, config map[string]any) (func(map[string]any) error, error) {
	var cfg HTTPConfig
	if v, ok := config["method"].(string); ok {
		cfg.Method = v
	} else {
		cfg.Method = "GET"
	}
	if v, ok := config["url"].(string); ok {
		cfg.URL = v
	}
	if v, ok := config["key"].(string); ok {
		cfg.Key = v
	}
	if v, ok := config["body"].(string); ok {
		cfg.Body = v
	}
	if v, ok := config["responseType"].(string); ok {
		cfg.ResponseType = v
	} else {
		cfg.ResponseType = "json"
	}
	if v, ok := config["throwOnError"].(bool); ok {
		cfg.ThrowOnError = v
	} else {
		cfg.ThrowOnError = true // matches anansi default
	}
	if v, ok := config["timeoutMs"].(float64); ok {
		cfg.TimeoutMs = v
	} else {
		cfg.TimeoutMs = 30000
	}
	if params, ok := config["params"].([]any); ok {
		for _, p := range params {
			if m, ok := p.(map[string]any); ok {
				cfg.Params = append(cfg.Params, HTTPParam{
					Key:   m["key"].(string),
					Value: m["value"].(string),
				})
			}
		}
	}
	if headers, ok := config["headers"].([]any); ok {
		for _, h := range headers {
			if m, ok := h.(map[string]any); ok {
				cfg.Headers = append(cfg.Headers, HTTPParam{
					Key:   m["key"].(string),
					Value: m["value"].(string),
				})
			}
		}
	}
	nCtx := &nodekit.TypedRunContext[HTTPConfig]{
		NodeRunContext: nodekit.NodeRunContext{
			NodeID: nodeID,
			Config: config,
		},
		Config: &cfg,
	}
	return run(ctx, nCtx)
}

func TestSSRFBlock(t *testing.T) {
	cases := []string{
		"http://127.0.0.1:8000/x",
		"http://10.0.0.5/x",
		"http://192.168.1.10/x",
		"http://172.16.4.4/x",
		"http://[::1]:3000/x",
		"http://[fc00:1234::1]/x",
	}
	for _, u := range cases {
		_, err := httpRun(context.Background(), "http1", map[string]any{"url": u})
		if err == nil || !strings.Contains(err.Error(), "security block") {
			t.Errorf("URL %s: want SSRF block, got %v", u, err)
		}
	}
}

func TestRunAgainstTestServer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Custom", "yes")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}))
	defer srv.Close()

	localURL := strings.Replace(srv.URL, "127.0.0.1", "localhost", 1)

	state := map[string]any{}
	mut, err := httpRun(context.Background(), "http1", map[string]any{
		"method": "GET",
		"url":    localURL + "/api",
		"params": []any{map[string]any{"key": "a", "value": "1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := mut(state); err != nil {
		t.Fatal(err)
	}

	result, ok := state["http_http1"].(map[string]any)
	if !ok {
		t.Fatalf("default key http_http1 missing: %v", state)
	}
	if result["status"] != float64(200) {
		t.Errorf("status = %v", result["status"])
	}
	data, ok := result["data"].(map[string]any)
	if !ok || data["ok"] != true {
		t.Errorf("data = %v", result["data"])
	}
	headers, ok := result["headers"].(map[string]any)
	if !ok || headers["x-custom"] != "yes" {
		t.Errorf("headers = %v", result["headers"])
	}
}

func TestCustomKeyAndThrowOnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("bad"))
	}))
	defer srv.Close()

	localURL := strings.Replace(srv.URL, "127.0.0.1", "localhost", 1)

	_, err := httpRun(context.Background(), "http1", map[string]any{"url": localURL, "key": "resp"})
	if err == nil || !strings.Contains(err.Error(), "HTTP 400") {
		t.Errorf("throwOnError: want HTTP 400 error, got %v", err)
	}

	state := map[string]any{}
	mut, err := httpRun(context.Background(), "http1", map[string]any{
		"url":          localURL,
		"key":          "resp",
		"throwOnError": false,
		"responseType": "text",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := mut(state); err != nil {
		t.Fatal(err)
	}
	if _, ok := state["resp"]; !ok {
		t.Errorf("custom key resp missing: %v", state)
	}
}

func TestConnectionPoolingAndReuse(t *testing.T) {
	requestCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"count": requestCount})
	}))
	defer srv.Close()

	localURL := strings.Replace(srv.URL, "127.0.0.1", "localhost", 1)

	for i := 1; i <= 5; i++ {
		state := map[string]any{}
		mut, err := httpRun(context.Background(), "http1", map[string]any{
			"url": localURL,
			"key": "resp",
		})
		if err != nil {
			t.Fatalf("request %d failed: %v", i, err)
		}
		if err := mut(state); err != nil {
			t.Fatalf("mutator %d failed: %v", i, err)
		}
		resp, ok := state["resp"].(map[string]any)
		if !ok || resp["status"] != float64(200) {
			t.Fatalf("unexpected response: %v", resp)
		}
	}
	if requestCount != 5 {
		t.Fatalf("expected 5 requests, got %d", requestCount)
	}
}
