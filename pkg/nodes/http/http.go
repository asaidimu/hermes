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

type HTTPParam struct {
	Key   string `config:"key"`
	Value string `config:"value"`
}

type HTTPConfig struct {
	Method       string      `config:"method" anansi:"default=GET"`
	URL          string      `config:"url"`
	Key          string      `config:"key"`
	Headers      []HTTPParam `config:"headers"`
	Params       []HTTPParam `config:"params"`
	Body         string      `config:"body"`
	ResponseType string      `config:"responseType" anansi:"default=json"`
	ThrowOnError bool        `config:"throwOnError" anansi:"default=true"`
	TimeoutMs    float64     `config:"timeoutMs" anansi:"default=30000"`
}

var Node = nodekit.Define(nodekit.TypedDefinition[HTTPConfig]{
	Kind:        "http",
	Label:       "HTTP Request",
	Description: "Execute a standard HTTP request to an external service or API.",
	Type:        "executable",
	Handles: func(cfg *HTTPConfig) []nodekit.HandleSpec {
		return []nodekit.HandleSpec{
			{Type: nodekit.HandleTarget, ID: ""},
			{Type: nodekit.HandleSource, ID: ""},
		}
	},
	HandlesJS: `() => [{"type":"target","id":"","kind":"executable"},{"type":"source","id":"","kind":"executable"}]`,
	Run:       run,
})

// @note #review-20260826-004 observation status=open priority=P2 tags=#review,#security : SSRF guard is a host-literal regex — DNS rebinding and redirects bypass it
// @author ox-alpha
//
// privateIPRegex inspects the URL host string only. It does not resolve the
// hostname, so a public DNS name resolving to a private IP passes the guard,
// and 30x redirects to internal targets are followed unchecked. IPv6 zone or
// hex-encoded forms are also only partially covered. For production, resolve
// then dial with an IP-checking DialContext (or use a allowlist).
var privateIPRegex = regexp.MustCompile(
	`^(127\.|10\.|192\.168\.|169\.254\.|172\.(1[6-9]|2[0-9]|3[0-1])\.|::1|fe80|fc00|fd00)`,
)

func run(ctx context.Context, nCtx *nodekit.TypedRunContext[HTTPConfig]) (store.Mutator, error) {
	cfg := nCtx.Config
	method := cfg.Method
	if method == "" {
		method = "GET"
	}
	rawURL := cfg.URL
	responseType := cfg.ResponseType
	if responseType == "" {
		responseType = "json"
	}
	throwOnError := cfg.ThrowOnError
	timeoutMs := cfg.TimeoutMs
	if timeoutMs <= 0 {
		timeoutMs = 30000
	}

	key := cfg.Key
	if key == "" {
		key = "http_" + nCtx.NodeID
	}
	if rawURL == "" {
		return nil, fmt.Errorf("HTTP node: URL is required")
	}

	processedHeaders := map[string]string{}
	for _, h := range cfg.Headers {
		if strings.TrimSpace(h.Key) == "" {
			continue
		}
		processedHeaders[strings.TrimSpace(h.Key)] = h.Value
	}

	urlObj, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("HTTP node: request failed - %v", err)
	}
	q := urlObj.Query()
	for _, p := range cfg.Params {
		if strings.TrimSpace(p.Key) == "" {
			continue
		}
		q.Add(strings.TrimSpace(p.Key), p.Value)
	}
	urlObj.RawQuery = q.Encode()

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

	body := cfg.Body
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

	// @note #review-20260822-036 issue status=open priority=P1 tags=#review,#bug : New HTTP client per request
	//
	// client := &http.Client{} creates a new HTTP client per request with no connection
	// pooling and a nil Transport (no TLS tuning, no timeouts on dial/TLS/etc.). Should
	// use a package-level or injected client.
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

	// @note #review-20260822-037 issue status=open priority=P1 tags=#review,#bug : Unbounded response body read
	//
	// io.ReadAll(resp.Body) reads unbounded response into memory. A malicious endpoint
	// returning a multi-GB body will OOM the process. Use io.LimitReader(resp.Body,
	// maxBytes).
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
