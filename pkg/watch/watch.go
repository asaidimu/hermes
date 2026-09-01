// Package watch defines the types and interface for event-based workflow resumption.
// It provides WatchEvent, WatchCondition, WatchDescriptor for describing what events
// a paused workflow should watch for, and the WatchService interface that
// implementations must satisfy.
// @note #review-20260822-009 issue status=resolved priority=P3 tags=#review,#documentation : Package doc misdescribes contents
//
// Fixed by rewriting the package doc to accurately describe what the package
// provides: type definitions and the WatchService interface for event-based
// workflow resumption.
package watch

// WatchEvent represents a matched event that should resume a paused run.
type WatchEvent struct {
	EventType string
	// Payload contains the event data that was matched against the watch condition.
	// Keys and value types depend on the event source (e.g., pipeline events may
	// include "runId", "stageId", "status" keys).
	Payload map[string]any
	// Patch contains field updates to apply to the workflow state when resuming.
	// Keys are dot-separated state paths (e.g., "entry.status") and values are
	// the new values to set.
	Patch map[string]any
	// @note #review-20260822-010 issue status=resolved priority=P3 tags=#review,#documentation : Payload and Patch lack doc comments
	//
	// Fixed by adding doc comments explaining that Payload contains matched event
	// data and Patch contains state updates to apply on resume.
}

// ConditionOp is a watch condition's comparison operator.
//
// @note #review-20260822-011 issue status=resolved priority=P2 tags=#review,#naming : Untyped string for operator, field, and mode
//
// Resolved (partially): defined ConditionOp and WatchMode as named string
// types with constants, so IDE autocomplete and doc comments have
// somewhere to live. Left WatchCondition.Field as a plain string — it's a
// dotted payload path, an open-ended value space, not an enum — and did
// not add compile-time validation of these constants' actual *values* at
// the point WatchDescriptor is built (that would need a validating
// constructor or a change to WatchService.Register's signature, which is
// its own note — review-20260822-013 — kept separate). A caller who writes
// ConditionOp("eqq") instead of OpEqual still compiles; this change gives
// the correct constant a name to reach for, without pretending to close
// the whole validation gap in one pass.
type ConditionOp string

const (
	OpEqual    ConditionOp = "=="
	OpNotEqual ConditionOp = "!="
	OpExists   ConditionOp = "exists"
)

// WatchMode selects how a WatchDescriptor's multiple EventTypes/Conditions
// combine to resolve a pause ("any" vs "all").
type WatchMode string

const (
	WatchModeAny WatchMode = "any"
	WatchModeAll WatchMode = "all"
)

// WatchCondition defines a filter on event payload fields.
type WatchCondition struct {
	Field string
	Op    ConditionOp
	Value any
}

// WatchDescriptor describes what events a pause node should watch for.
type WatchDescriptor struct {
	EventTypes []string
	Mode       string
	// Timeout is the duration to wait before giving up, in milliseconds.
	//
	// @note #review-20260822-012 issue status=resolved priority=P2 tags=#review,#naming : Timeout is int64 without documented units
	//
	// Resolved: documented the unit (milliseconds) rather than changing the
	// field to time.Duration. Every producer of this value already
	// converts through time.Duration.Milliseconds() first (see
	// pkg/pipeline/context.go's PauseInstruction handling and
	// pause.go's PauseConfig.Timeout), so int64 milliseconds is the value
	// that actually flows through checkpoints/state (which are
	// JSON-serialized — time.Duration would round-trip through JSON as a
	// bare int64 of nanoseconds anyway, which is easy to misread as
	// milliseconds and arguably more error-prone than a documented,
	// already-consistently-used int64-ms convention).
	Timeout    int64
	Conditions []WatchCondition
	Patch      map[string]any
}

// WatchService is the interface for the watch service.
type WatchService interface {
	// @note #review-20260822-013 issue status=resolved priority=P2 tags=#review,#interface : WatchService has no error returns
	//
	// Resolved: Register now returns error (validates runID and
	// EventTypes — the two conditions the single real implementation,
	// WatchService in pkg/runtime, can actually fail on today) and
	// PeekBufferedEvent now returns (*WatchEvent, bool) instead of a bare
	// pointer where nil was ambiguous between "no event" and "no such run".
	// This was safe to change in place (not just document) because this
	// interface has exactly one implementation and exactly one caller
	// (pause.go), both in this repo, both updated together.
	Register(runID string, desc WatchDescriptor) error
	PeekBufferedEvent(runID string) (*WatchEvent, bool)
}
