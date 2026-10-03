// Package runtime is the thin public entry over internal/runtime.
//
// 20261003 refactor (plan R.5): the workflow runtime implementation —
// registration, dispatch, pause/resume, outcomes, recovery, the watch
// service, and the event source machinery — moved to internal/runtime so
// internal decisions stop being public API by default. Everything this
// package declares is a type alias or a one-line constructor; the public
// surface is deliberate, not accidental:
//
//   - WorkflowRuntime + Options: construct and configure the orchestrator
//   - EventSource / ManualEventSource: the host-side event wiring seam
//   - RunResult / RunHandle: terminal outcomes and prepare hooks
//   - ManualEvent / AbortEvent: the well-known event names
//
// Layer map: internal/runtime is the top orchestration layer (see
// internal/runtime/runtime.go and tests/layering_test.go).
package runtime

import (
	internal "github.com/asaidimu/hermes/internal/runtime"
)

// Core types (aliases over internal/runtime — see R.5).
type (
	// WorkflowRuntime orchestrates workflow runs. Safe for concurrent use.
	WorkflowRuntime = internal.WorkflowRuntime
	// Options configures a WorkflowRuntime.
	Options = internal.Options
	// Mode controls per-workflow run concurrency.
	Mode = internal.Mode
	// RegisterOptions configures a workflow registration.
	RegisterOptions = internal.RegisterOptions
	// RunHandle exposes a live run to onPrepare hooks.
	RunHandle = internal.RunHandle
	// RunResult is the terminal outcome of a run.
	RunResult = internal.RunResult
	// RunOptions configures a Run(nodes, edges) invocation.
	RunOptions = internal.RunOptions
	// EventSource is the inversion-of-control interface for wiring external
	// events to workflow triggers.
	EventSource = internal.EventSource
	// RegisterParams is passed to EventSource.OnRegister.
	RegisterParams = internal.RegisterParams
	// RegisteredTrigger describes a trigger a workflow listens for.
	RegisteredTrigger = internal.RegisteredTrigger
	// ManualEventSource stores the emit callback and exposes Emit for
	// manual triggering (tests, HTTP handlers).
	ManualEventSource = internal.ManualEventSource
	// WatchService buffers pre-pause events and resolves waits (the
	// implementation behind watch.WatchService).
	WatchService = internal.WatchService
)

// Well-known event names (mirror the TS constants).
const (
	// ManualEvent triggers a run of the workflow's manual trigger.
	ManualEvent = internal.ManualEvent
	// AbortEvent aborts the run named in the event payload.
	AbortEvent = internal.AbortEvent
)

// NewWorkflowRuntime creates a runtime. See internal.Options for defaults.
func NewWorkflowRuntime(opts Options) *WorkflowRuntime {
	return internal.NewWorkflowRuntime(opts)
}

// NewManualEventSource creates a ManualEventSource.
func NewManualEventSource() *ManualEventSource {
	return internal.NewManualEventSource()
}
