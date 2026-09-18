package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/store"
)

// maxResponseBodyBytes limits the amount of data read from an HTTP response to prevent OOM.
const maxResponseBodyBytes = 32 * 1024 * 1024 // 32 MB

// errBlockedAddress is returned by safeDialer's Control hook when a dial
// target resolves to a restricted network. See the note on isBlockedIP.
var errBlockedAddress = errors.New("HTTP node security block: address resolves to a restricted network")

// isBlockedIP reports whether ip belongs to a loopback, private, link-local,
// unspecified, or multicast range — the ranges a public-facing HTTP node
// must never be allowed to reach, whether the target was a literal IP, a DNS
// name that rebound to one of these ranges, or a redirect Location header
// pointing at one.
func isBlockedIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	return ip.IsLoopback() ||
		ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsUnspecified() ||
		ip.IsMulticast()
}

// ssrfGuardDisabledForTests lets this package's own tests dial a local
// httptest server (which necessarily binds to a loopback address) without
// weakening the guard for real workflow runs. It is unexported, so nothing
// outside this package can touch it, and defaults to false (guard active).
// See TestRunAgainstTestServer and friends in http_test.go.
var ssrfGuardDisabledForTests = false

// safeDialer's Control hook runs after DNS resolution but before the TCP
// connection is established, receiving the actual numeric address about to
// be dialed. Rejecting restricted addresses here — rather than only
// inspecting the pre-resolution hostname — closes the two gaps privateIPRegex
// alone had: a public DNS name that resolves (or rebinds) to a private IP,
// and a 30x redirect whose Location points at an internal target. Go's
// http.Client dials fresh connections for every redirect hop through the
// same Transport, so this hook fires for those hops too, without needing a
// separate CheckRedirect.
var safeDialer = &net.Dialer{
	Timeout:   30 * time.Second,
	KeepAlive: 30 * time.Second,
	Control: func(network, address string, c syscall.RawConn) error {
		if ssrfGuardDisabledForTests {
			return nil
		}
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return err
		}
		ip := net.ParseIP(host)
		if isBlockedIP(ip) {
			return errBlockedAddress
		}
		return nil
	},
}

// defaultPooledTransport provides connection pooling and timeout configurations.
var defaultPooledTransport = &http.Transport{
	Proxy:                 http.ProxyFromEnvironment,
	DialContext:           safeDialer.DialContext,
	ForceAttemptHTTP2:     true,
	MaxIdleConns:          100,
	MaxIdleConnsPerHost:   10,
	IdleConnTimeout:       90 * time.Second,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 1 * time.Second,
}

var defaultHTTPClient = &http.Client{
	Transport: defaultPooledTransport,
}

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
	Effect:      nodekit.EffectSideEffecting,
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

// @note #review-20260826-004 observation P2 resolved status=resolved priority=P2 tags=#review,#security : SSRF guard is a host-literal regex — DNS rebinding and redirects bypass it
// @author ox-alpha
//
// Resolved: privateIPRegex stays as a cheap, clear-error fast path for the
// common case (a literal private/loopback IP in the URL), but it is no
// longer the security boundary. safeDialer's Control hook (above) now
// checks the actual resolved address on every dial the Transport makes —
// including the fresh dials Go's http.Client performs for each redirect
// hop — so a public DNS name that resolves (or rebinds) to a private IP,
// or a 30x Location pointing at an internal target, is rejected at the
// point of connection instead of only at the pre-resolution hostname
// string.
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

	// @note #review-20260822-036 issue P1 resolved status=resolved priority=P1 tags=#review,#bug : New HTTP client per request
	//
	// Resolved: Use package-level pooled defaultHTTPClient with configured connection pooling and timeouts.
	resp, err := defaultHTTPClient.Do(req)
	if err != nil {
		if reqCtx.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf("HTTP node: request timed out after %vms", int(timeoutMs))
		}
		if errors.Is(err, errBlockedAddress) {
			return nil, fmt.Errorf("HTTP node security block: request target resolves to a restricted network")
		}
		return nil, fmt.Errorf("HTTP node: request failed - %v", err)
	}
	defer func() {
		// Drain up to 512 bytes and close to ensure keep-alive connection reuse
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 512))
		_ = resp.Body.Close()
	}()

	if throwOnError && resp.StatusCode >= 400 {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, http.StatusText(resp.StatusCode))
	}

	// @note #review-20260822-037 issue P1 resolved status=resolved priority=P1 tags=#review,#bug : Unbounded response body read
	//
	// Resolved: Limit response body reading to maxResponseBodyBytes (32MB) using io.LimitReader.
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyBytes))
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
