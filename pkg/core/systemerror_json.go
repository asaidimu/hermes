package core

import (
	"time"
)

// systemErrorMeta maps a SystemError code to the TS SystemError category/http
// status/action triple used in the serialized JSON shape.
func systemErrorMeta(code string) (category string, httpStatus int, action string) {
	switch code {
	// @note #review-20260822-026 issue status=open priority=P2 tags=#review,#naming : INTERNAL_ERROR code not defined in errors.go constants
	//
	// The systemErrorMeta switch uses "INTERNAL_ERROR" as a case, but errors.go defines
	// ErrCodeExecutionFailed = "EXECUTION_FAILED", not "INTERNAL_ERROR". If a SystemError
	// is created with ErrCodeExecutionFailed, it falls to the default branch and maps to
	// system/500/internal anyway — but this is accidental, not intentional. The code
	// constants and the meta mapping are inconsistent.
	case "INTERNAL_ERROR":
		return "system", 500, "internal"
	case "VALIDATION_FAILED", "BAD_REQUEST", "VALIDATION_ERROR":
		return "validation", 400, "validate"
	case "RESOURCE_NOT_FOUND", "NOT_FOUND":
		return "not_found", 404, "find"
	// @note #review-20260822-024 issue status=open priority=P2 tags=#review,#bug : UNAUTHORIZED conflated with PERMISSION_DENIED (401 vs 403)
	//
	// UNAUTHORIZED maps to httpStatus 403 and action "authorize". In HTTP semantics, 401
	// means "not authenticated" (no credentials) and 403 means "not authorized"
	// (insufficient permissions). These are distinct failure modes with different client
	// recovery strategies. Map UNAUTHORIZED to 401 and action "authenticate", or split
	// into separate cases.
	case "PERMISSION_DENIED", "UNAUTHORIZED":
		return "auth", 403, "authorize"
	case "RESOURCE_LOCKED", "CONFLICT":
		return "conflict", 409, "lock"
	// @note #review-20260822-025 issue status=open priority=P2 tags=#review,#naming : HTTP status 499 is non-standard
	//
	// 499 is an nginx invention for "client closed connection." It is not part of the HTTP
	// specification and will confuse clients, monitoring tools, and API documentation
	// generators. Use 499 only if you control the client, otherwise map ABORTED/CANCELLED
	// to 408 (Request Timeout) or 409 (Conflict), or document the custom status code
	// prominently.
	case "ABORTED", "CANCELLED":
		return "aborted", 499, "abort"
	default:
		return "system", 500, "internal"
	}
}

// SystemErrorJSON serializes a SystemError into the TS SystemError.toJSON()
// shape used by the frontend timeline scrubber and stored error payloads.
// Non-SystemError inputs are normalized via SystemErrorFrom.
func SystemErrorJSON(err error) map[string]any {
	if err == nil {
		return nil
	}

	se := SystemErrorFrom(err)

	msg := se.Message
	if msg == "" {
		msg = err.Error()
	}

	category, httpStatus, action := systemErrorMeta(se.Code)

	var issues []any
	if len(se.Issues) > 0 {
		issues = make([]any, 0, len(se.Issues))
		// @note #review-20260822-029 issue status=open priority=P3 tags=#review,#documentation : Issues slice is untyped []any
		//
		// The issues variable is []any containing map[string]any elements with keys "code",
		// "message", "path", "index", "severity". There is no compile-time guarantee these
		// keys are correct or consistently spelled. A dedicated IssueJSON struct would provide
		// type safety and IDE autocomplete.
		for _, iss := range se.Issues {
			issues = append(issues, map[string]any{
				"code":     iss.Code,
				"message":  iss.Message,
				"path":     iss.Path,
				"index":    iss.Index,
				"severity": iss.Severity,
			})
		}
	}

	trace := []any{}
	for cause := se.Cause; cause != nil; {
		if next, ok := cause.(*SystemError); ok {
			op := next.Operation
			if op == "" {
				op = next.Code
			}
			trace = append(trace, map[string]any{
				"operation": op,
				"message":   next.Message,
				"code":      next.Code,
			})
			cause = next.Cause
		} else {
			trace = append(trace, map[string]any{
				"operation": "error",
				"message":   cause.Error(),
				"code":      "",
			})
			break
		}
	}

	severity := se.Severity
	if severity == "" {
		severity = "error"
	}

	m := map[string]any{
		"name":       "SystemError",
		"code":       se.Code,
		"message":    msg,
		"severity":   severity,
		"category":   category,
		"httpStatus": float64(httpStatus),
		"operation":  se.Operation,
		"path":       se.Path,
		"trace":      trace,
		"action":     action,
		// @note #review-20260822-027 issue status=open priority=P2 tags=#review,#bug : Timestamp uses serialization time, not error creation time
		//
		// time.Now().UTC().Format(time.RFC3339) captures the time when SystemErrorJSON is
		// called, not when the error was originally created. For debugging, the error's
		// creation time is far more useful. If SystemError has a Timestamp field, use it;
		// otherwise the serialization time may differ from the actual error time by seconds
		// or minutes, making log correlation unreliable.
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
		// @note #review-20260822-028 issue status=open priority=P3 tags=#review,#documentation : Stack field is always empty string
		//
		// The serialized JSON always includes `"stack": ""` as an empty string. This is dead
		// data that wastes wire bytes and confuses consumers who expect a stack trace. Either
		// populate it with a real stack (using runtime.Callers) or omit the key entirely when
		// empty.
		"stack":      "",
	}
	if issues != nil {
		m["issues"] = issues
	}

	return m
}
