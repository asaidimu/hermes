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

// WatchCondition defines a filter on event payload fields.
type WatchCondition struct {
	// @note #review-20260822-011 issue status=open priority=P2 tags=#review,#naming : Untyped string for operator, field, and mode
	//
	// WatchCondition.Op, WatchCondition.Field, and WatchDescriptor.Mode are bare string
	// types with no compile-time validation. Invalid operator names like "eqq" or modes
	// like "ALL" will silently pass through. Define `type ConditionOp string` with
	// constants (OpEqual, OpNotEqual, etc.) and `type WatchMode string` to catch invalid
	// values at compile time.
	Field string
	Op    string
	Value any
}

// WatchDescriptor describes what events a pause node should watch for.
type WatchDescriptor struct {
	EventTypes []string
	Mode       string
	// @note #review-20260822-012 issue status=open priority=P2 tags=#review,#naming : Timeout is int64 without documented units
	//
	// WatchDescriptor.Timeout is int64 with no documentation of whether the value is in
	// milliseconds, seconds, or nanoseconds. Different callers will interpret this
	// differently. Change to time.Duration for type-safe, unambiguous units, or add a
	// clear doc comment specifying the unit.
	Timeout    int64
	Conditions []WatchCondition
	Patch      map[string]any
}

// WatchService is the interface for the watch service.
type WatchService interface {
	// @note #review-20260822-013 issue status=open priority=P2 tags=#review,#interface : WatchService has no error returns
	//
	// WatchService.Register returns nothing, so a registration failure (duplicate run ID,
	// invalid descriptor, resource exhaustion) is silently dropped. PeekBufferedEvent
	// returns *WatchEvent where nil means "no event," but this is ambiguous with a system
	// error. Register should return error, and PeekBufferedEvent should return
	// (WatchEvent, bool) or (WatchEvent, error).
	Register(runID string, desc WatchDescriptor)
	PeekBufferedEvent(runID string) *WatchEvent
}
