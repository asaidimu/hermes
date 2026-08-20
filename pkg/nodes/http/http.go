package http

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/store"
)

var Node = nodekit.NodeDefinition{
	Kind:        "http",
	Label:       "HTTP Request",
	Description: "Execute a standard HTTP request to an external service or API.",
	Type:        "executable",
	ConfigSchema: json.RawMessage(`{
		"version": "1.0.0",
		"name": "http",
		"fields": {
			"method":       { "name": "method", "type": "string", "default": "GET", "required": true },
			"url":          { "name": "url", "type": "string", "default": "", "required": true },
			"key":          { "name": "key", "type": "string", "default": "", "required": false },
			"headers":      { "name": "headers", "type": "array", "default": [], "required": false },
			"params":       { "name": "params", "type": "array", "default": [], "required": false },
			"body":         { "name": "body", "type": "string", "default": "", "required": false },
			"responseType": { "name": "responseType", "type": "string", "default": "json", "required": false },
			"throwOnError": { "name": "throwOnError", "type": "boolean", "default": true, "required": false },
			"timeoutMs":    { "name": "timeoutMs", "type": "number", "default": 30000, "required": false }
		}
	}`),
	Handles: func(config map[string]any) []nodekit.HandleSpec {
		return []nodekit.HandleSpec{
			{Type: nodekit.HandleTarget, ID: ""},
			{Type: nodekit.HandleSource, ID: ""},
		}
	},
	Run: run,
}

var privateIPRegex = regexp.MustCompile(
	`^(127\.|10\.|192\.168\.|169\.254\.|172\.(1[6-9]|2[0-9]|3[0-1])\.|::1|fe80|fc00|fd00)`,
)

func run(ctx context.Context, nCtx nodekit.NodeRunContext) (store.DocumentMutator, error) {
	cfg := nCtx.Config
	method, _ := cfg["method"].(string)
	if method == "" {
		method = "GET"
	}
	rawURL, _ := cfg["url"].(string)
	customKey, _ := cfg["key"].(string)
	responseType, _ := cfg["responseType"].(string)
	if responseType == "" {
		responseType = "json"
	}
	throwOnError := true
	if b, ok := cfg["throwOnError"].(bool); ok {
		throwOnError = b
	}
	timeoutMs := 30000.0
	if f, ok := nodekit.Number(cfg["timeoutMs"]); ok {
		timeoutMs = f
	}

	key := customKey
	if key == "" {
		key = "http_" + nCtx.NodeID
	}
	if rawURL == "" {
		return nil, fmt.Errorf("HTTP node: URL is required")
	}

	processedHeaders := map[string]string{}
	if headers, ok := cfg["headers"].([]any); ok {
		for _, h := range headers {
			m, ok := h.(map[string]any)
			if !ok {
				continue
			}
			k, _ := m["key"].(string)
			if strings.TrimSpace(k) == "" {
				continue
			}
			val, _ := m["value"].(string)
			processedHeaders[strings.TrimSpace(k)] = val
		}
	}

	urlObj, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("HTTP node: request failed - %v", err)
	}
	if params, ok := cfg["params"].([]any); ok {
		q := urlObj.Query()
		for _, p := range params {
			m, ok := p.(map[string]any)
			if !ok {
				continue
			}
			k, _ := m["key"].(string)
			if strings.TrimSpace(k) == "" {
				continue
			}
			v, _ := m["value"].(string)
			q.Add(strings.TrimSpace(k), v)
		}
		urlObj.RawQuery = q.Encode()
	}

	if privateIPRegex.MatchString(urlObj.Hostname()) {
		return nil, fmt.Errorf(
			`HTTP node security block: Restricted access to private network namespace %q`,
			urlObj.Hostname(),
		)
	}

	lowercasedHeaders := make(map[string]string, len(processedHeaders))
	for k, v := range processedHeaders {
		lowercasedHeaders[strings.ToLower(k)] = v
	}

	body, _ := cfg["body"].(string)
	if (method == "POST" || method == "PUT" || method == "PATCH") && body != "" {
		if _, ok := lowercasedHeaders["content-type"]; !ok {
			trimmed := strings.TrimSpace(body)
			if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
				processedHeaders["Content-Type"] = "application/json; charset=utf-8"
			}
		}
	}

	reqCtx := ctx
	var cancel context.CancelFunc
	if timeoutMs > 0 {
		reqCtx, cancel = context.WithTimeout(ctx, time.Duration(timeoutMs)*time.Millisecond)
		defer cancel()
	}

	req, err := http.NewRequestWithContext(reqCtx, method, urlObj.String(), strings.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("HTTP node: request failed - %v", err)
	}
	for k, v := range processedHeaders {
		req.Header.Set(k, v)
	}
	if method != "POST" && method != "PUT" && method != "PATCH" {
		req.Body = nil
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		if reqCtx.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf("HTTP node: request timed out after %vms", int(timeoutMs))
		}
		return nil, fmt.Errorf("HTTP node: request failed - %v", err)
	}
	defer resp.Body.Close()

	if throwOnError && resp.StatusCode >= 400 {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, http.StatusText(resp.StatusCode))
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("HTTP node: request failed - %v", err)
	}

	var data any
	switch responseType {
	case "text":
		data = string(respBody)
	case "blob":
		data = map[string]any{
			"type":  resp.Header.Get("Content-Type"),
			"size":  len(respBody),
			"_blob": respBody,
		}
	case "arrayBuffer":
		data = map[string]any{"_arrayBuffer": respBody}
	default:
		if err := json.Unmarshal(respBody, &data); err != nil {
			return nil, fmt.Errorf("HTTP node: request failed - %v", err)
		}
	}

	respHeaders := make(map[string]any)
	for k := range resp.Header {
		respHeaders[strings.ToLower(k)] = resp.Header.Get(k)
	}

	result := map[string]any{
		"data":       data,
		"status":     float64(resp.StatusCode),
		"statusText": http.StatusText(resp.StatusCode),
		"headers":    respHeaders,
	}

	return nodekit.PatchMutator(map[string]any{key: result}), nil
}