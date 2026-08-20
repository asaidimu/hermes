package core

import (
	"time"
)

// systemErrorMeta maps a SystemError code to the TS SystemError category/http
// status/action triple used in the serialized JSON shape.
func systemErrorMeta(code string) (category string, httpStatus int, action string) {
	switch code {
	case "INTERNAL_ERROR":
		return "system", 500, "internal"
	case "VALIDATION_FAILED", "BAD_REQUEST", "VALIDATION_ERROR":
		return "validation", 400, "validate"
	case "RESOURCE_NOT_FOUND", "NOT_FOUND":
		return "not_found", 404, "find"
	case "PERMISSION_DENIED", "UNAUTHORIZED":
		return "auth", 403, "authorize"
	case "RESOURCE_LOCKED", "CONFLICT":
		return "conflict", 409, "lock"
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
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
		"stack":      "",
	}
	if issues != nil {
		m["issues"] = issues
	}

	return m
}