// Package watch defines shared types for the watch service interface.
package watch

// WatchEvent represents a matched event that should resume a paused run.
type WatchEvent struct {
	EventType string
	Payload   map[string]any
	Patch     map[string]any
}

// WatchCondition defines a filter on event payload fields.
type WatchCondition struct {
	Field string
	Op    string
	Value any
}

// WatchDescriptor describes what events a pause node should watch for.
type WatchDescriptor struct {
	EventTypes []string
	Mode       string
	Timeout    int64
	Conditions []WatchCondition
	Patch      map[string]any
}

// WatchService is the interface for the watch service.
type WatchService interface {
	Register(runID string, desc WatchDescriptor)
	PeekBufferedEvent(runID string) *WatchEvent
}
