package runtime

import (
	"context"
	"sync"

	"github.com/asaidimu/hermes/pkg/events"
)

// @note #no-shipped-webhook-http-layer-de-c47f5835 observation P2 #review,#production-readiness,#ux,#ops : No shipped webhook HTTP layer, deployment artifacts, or observability integration
// @author hermes-review
//
// hermes ships a Go runtime plus a TypeScript type/catalog package
// (CATALOG, HANDLES, wire types) that a separate frontend — the README
// credits a 'Hedwig' UI canvas as the wire-parity contract driving this
// package — is expected to consume. What hermes itself does not ship: a
// Dockerfile/docker-compose/Helm chart, a metrics endpoint (no
// Prometheus exporter or equivalent found under pkg/), or an HTTP
// webhook layer — EventSource (this file) is an inversion-of-control
// interface the host must implement to turn 'HTTP webhooks, message
// queues, cron, database watches, etc.' into trigger events; hermes
// does not listen on a port itself. Running hermes in production today
// means the deployment manifests, webhook HTTP layer, and
// metrics/alerting are the embedding host's responsibility, and the
// authoring/monitoring UI is a separate (unaudited here) project. That
// is a reasonable scope for an embeddable runtime library, but it is a
// large distance from a self-hostable, batteries-included product like
// n8n.

// EventSource is the inversion-of-control interface for wiring external events
// to workflow triggers. Implement this to connect your infrastructure (HTTP
// webhooks, message queues, cron, database watches, etc.) to the runtime.
//
// When a workflow is registered, the runtime calls OnRegister with the
// trigger definitions and an emit callback. The implementation subscribes to
// its event source and calls emit() whenever an event arrives.
type EventSource interface {
	// OnRegister is called when a workflow is registered. The params contain
	// the trigger definitions and an emit callback. Call emit(eventType, payload)
	// to fire an event on the runtime bus.
	//
	// Return a cleanup function that unsubscribes from the event source, or nil.
	OnRegister(ctx context.Context, params RegisterParams) (cleanup func(), err error)

	// OnShutdown is called when the runtime shuts down. Release all resources.
	OnShutdown(ctx context.Context) error
}

// RegisterParams is passed to EventSource.OnRegister.
type RegisterParams struct {
	// WorkflowID is the compiled workflow's unique ID.
	WorkflowID string

	// Triggers maps triggerID → registered trigger definition.
	Triggers map[string]RegisteredTrigger

	// Emit fires an event on the runtime bus. Call this when your event source
	// receives an external event. The runtime dispatches to matching workflows.
	Emit func(eventType string, payload map[string]any)
}

// RegisteredTrigger describes a trigger the workflow listens for.
type RegisteredTrigger struct {
	ID        string
	Event     string
	Predicate func(events.PipelineEvent) bool
}

// ---------------------------------------------------------------------------
// ManualEventSource — default in-memory implementation for testing and
// backward compatibility. Exposes Emit() for manual triggering.
// ---------------------------------------------------------------------------

// ManualEventSource is a trivial EventSource that stores the emit callback
// and exposes it for manual triggering (e.g. from HTTP handlers or tests).
type ManualEventSource struct {
	mu             sync.Mutex
	emit           func(eventType string, payload map[string]any)
	ShutdownCalled bool
}

// NewManualEventSource creates a ManualEventSource.
func NewManualEventSource() *ManualEventSource {
	return &ManualEventSource{}
}

// OnRegister stores the emit callback. No external subscriptions are wired.
func (m *ManualEventSource) OnRegister(_ context.Context, params RegisterParams) (func(), error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.emit = params.Emit
	return nil, nil
}

// OnShutdown sets the shutdown flag.
func (m *ManualEventSource) OnShutdown(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ShutdownCalled = true
	return nil
}

// Emit fires an event on the runtime bus. Safe for concurrent use.
// Returns false if no emit callback is registered (workflow not yet registered).
func (m *ManualEventSource) Emit(eventType string, payload map[string]any) bool {
	m.mu.Lock()
	emit := m.emit
	m.mu.Unlock()
	if emit == nil {
		return false
	}
	emit(eventType, payload)
	return true
}

// Emit fires an event on the runtime bus using the stored callback.
// This is a convenience for adapters that receive events from external sources.
func (m *ManualEventSource) EmitEvent(ctx context.Context, eventType string, payload map[string]any) {
	m.mu.Lock()
	emit := m.emit
	m.mu.Unlock()
	if emit != nil {
		emit(eventType, payload)
	}
}
