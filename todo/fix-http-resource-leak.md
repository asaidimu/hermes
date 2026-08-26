# Fix Resource Leaks in HTTP Node

- [*] Replace per-request HTTP client with a shared, pooled HTTP client and bound response body reads
  - **Context:** `pkg/nodes/http/http.go` currently creates `&http.Client{}` on every request (`#review-20260822-036`), preventing connection pooling and leaking TCP sockets. It also reads unbounded response bodies (`#review-20260822-037`), which risks OOMing the process.
  - **Details:**
    1. Define a shared package-level `http.Client` with a tuned `http.Transport` (connection pooling, idle timeouts, dial timeouts).
    2. Limit body reading with `io.LimitReader` (32MB max limit) and drain remaining bytes before close.
    3. Update devnotes in `pkg/nodes/http/http.go`.
  - **Files:** `pkg/nodes/http/http.go`, `pkg/nodes/http/http_test.go`

- [*] Verify fix with tests
  - **Context:** Ensure HTTP requests still work properly for GET, POST, custom headers, timeouts, error status codes, and bounded payloads.
  - **Details:** Run `go test -v ./pkg/nodes/http/...`.
