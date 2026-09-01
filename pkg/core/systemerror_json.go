package core

import (
	"time"
)

// systemErrorMeta maps a SystemError code to the TS SystemError category/http
// status/action triple used in the serialized JSON shape.
func systemErrorMeta(code string) (category string, httpStatus int, action string) {
	switch code {
	// @note #review-20260822-026 issue status=resolved priority=P2 tags=#review,#naming : INTERNAL_ERROR code not defined in errors.go constants
	//
	// Resolved: added ErrCodeInternal = "INTERNAL_ERROR" to errors.go and
	// switched the two direct string literals in trycatch.go to use it, so
	// the meta mapping and the code constants agree by name, not by accident.
	case ErrCodeInternal:
		return "system", 500, "internal"
	case "VALIDATION_FAILED", "BAD_REQUEST", "VALIDATION_ERROR":
		return "validation", 400, "validate"
	case "RESOURCE_NOT_FOUND", "NOT_FOUND":
		return "not_found", 404, "find"
	// @note #review-20260822-024 issue status=resolved priority=P2 tags=#review,#bug : UNAUTHORIZED conflated with PERMISSION_DENIED (401 vs 403)
	//
	// Resolved: split UNAUTHORIZED into its own case mapped to 401/authenticate
	// (no credentials presented), keeping PERMISSION_DENIED at 403/authorize
	// (credentials present but insufficient permissions), matching standard
	// HTTP semantics so clients can tell the two failure modes apart.
	case "UNAUTHORIZED":
		return "auth", 401, "authenticate"
	case "PERMISSION_DENIED":
		return "auth", 403, "authorize"
	case "RESOURCE_LOCKED", "CONFLICT":
		return "conflict", 409, "lock"
	// @note #review-20260822-025 issue status=wontfix priority=P2 tags=#review,#naming : HTTP status 499 is non-standard
	//
	// Considered remapping to 408/409, but pkg/server/server.go already
	// writes a raw 499 directly for the client-disconnect case, so 499 is
	// this codebase's established (if nginx-borrowed) convention for "the
	// client went away / operation was aborted," not an isolated accident.
	// Remapping only this call site would create an inconsistency with the
	// server's own direct usage rather than remove one. Documenting it here
	// instead: 499 is intentional and non-standard, keep client/monitoring
	// tooling aware of it.
	case "ABORTED", "CANCELLED":
		return "aborted", 499, "abort"
	default:
		return "system", 500, "internal"
	}
}

// IssueJSON is the typed shape of one entry in SystemErrorJSON's "issues"
// array. Building the slice through this struct (instead of ad-hoc
// map[string]any literals) gives compile-time field-name safety; it's
// converted to map[string]any only at the point of assembly into the
// untyped JSON payload.
//
// @note #review-20260822-029 issue status=resolved priority=P3 tags=#review,#documentation : Issues slice is untyped []any
//
// Resolved: introduced this struct so the per-issue fields are populated
// through named, typed struct fields instead of hand-typed map keys. The
// outer payload is still map[string]any (matching the TS toJSON() shape and
// every other field in SystemErrorJSON), so this only removes the typo risk
// on the issues entries specifically, not the whole function's output type.
type IssueJSON struct {
	Code     string `json:"code"`
	Message  string `json:"message"`
	Path     string `json:"path"`
	Index    *int   `json:"index"`
	Severity string `json:"severity"`
}

func (i IssueJSON) toMap() map[string]any {
	return map[string]any{
		"code":     i.Code,
		"message":  i.Message,
		"path":     i.Path,
		"index":    i.Index,
		"severity": i.Severity,
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
			issues = append(issues, IssueJSON{
				Code:     iss.Code,
				Message:  iss.Message,
				Path:     iss.Path,
				Index:    iss.Index,
				Severity: iss.Severity,
			}.toMap())
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
		// @note #review-20260822-027 issue status=wontfix priority=P2 tags=#review,#bug : Timestamp uses serialization time, not error creation time
		//
		// go-anansi's common.SystemError (checked directly against the
		// upstream source) has no Timestamp/CreatedAt field to fall back to
		// — Path, Operation, Message, Code, Severity, Issues, Cause only.
		// Capturing a true creation-time timestamp would require adding a
		// field to that external type or wrapping every construction site
		// across this codebase, both bigger changes than this function.
		// Left as serialization-time and documented here so callers know
		// not to treat it as authoritative for log correlation.
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		// @note #review-20260822-028 issue status=resolved priority=P3 tags=#review,#documentation : Stack field is always empty string
		//
		// Resolved: the "stack" key is now omitted entirely instead of
		// always being sent as an empty string. go-anansi's SystemError has
		// no captured stack to populate it with, so the empty placeholder
		// was pure dead weight on the wire; consumers should treat a
		// missing key the same as "no stack available."
	}
	if issues != nil {
		m["issues"] = issues
	}

	return m
}
