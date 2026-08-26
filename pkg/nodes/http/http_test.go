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
		_, err := run(context.Background(), nodekit.NodeRunContext{
			NodeID: "http1",
			Config: map[string]any{"url": u},
		})
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

	// httptest binds to 127.0.0.1; the SSRF guard blocks private IP literals, so
	// route through "localhost" (hostname passes the guard, resolves to loopback).
	localURL := strings.Replace(srv.URL, "127.0.0.1", "localhost", 1)

	state := map[string]any{}
	mut, err := run(context.Background(), nodekit.NodeRunContext{
		NodeID: "http1",
		Config: map[string]any{
			"method": "GET",
			"url":    localURL + "/api",
			"params": []any{map[string]any{"key": "a", "value": "1"}},
		},
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

	_, err := run(context.Background(), nodekit.NodeRunContext{
		NodeID: "http1",
		Config: map[string]any{"url": localURL, "key": "resp"},
	})
	if err == nil || !strings.Contains(err.Error(), "HTTP 400") {
		t.Errorf("throwOnError: want HTTP 400 error, got %v", err)
	}

	state := map[string]any{}
	mut, err := run(context.Background(), nodekit.NodeRunContext{
		NodeID: "http1",
		Config: map[string]any{"url": localURL, "key": "resp", "throwOnError": false, "responseType": "text"},
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
