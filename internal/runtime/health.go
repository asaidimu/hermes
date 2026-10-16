// Runtime health introspection (v2.1 plan H4): ONE concrete struct and ONE
// method — deliberately no HealthChecker interface (the alternative-design
// review borrowed the CONCERN, not the form; R.3 deleted exactly these
// single-implementation interfaces). Embedding hosts get a cheap snapshot
// for dashboards: queue depths, pause bookkeeping, retention posture, and
// the action log's durable footprint when the backend reports it.
//
// Lock discipline (P4.6 pattern): counters are snapshotted under rt.mu,
// then the backend query (I/O-shaped) runs OUTSIDE the lock — Health never
// stalls dispatch/Resume/Register behind a DB round trip.
package runtime

import (
	"context"
	"time"

	"github.com/asaidimu/hermes/pkg/actionlog"
)

// RuntimeHealth is a point-in-time snapshot of the runtime's bookkeeping.
// All counter fields are read under rt.mu; ActionLog is filled from the
// backend's UsageReporter capability after the lock is released (nil when
// no log is configured or the backend lacks the capability).
type RuntimeHealth struct {
	// Live bookkeeping.
	ActiveRuns     int `json:"activeRuns"`    // segments currently executing
	PausedRuns     int `json:"pausedRuns"`    // runs waiting for events
	PendingPauses  int `json:"pendingPauses"` // Recover-staged pauses not yet bound (S.3)
	CachedRunMetas int `json:"cachedRunMetas"`
	CachedOutcomes int `json:"cachedOutcomes"`
	LiveStores     int `json:"liveStores"`

	// Posture.
	Uptime               time.Duration `json:"uptime"`
	JanitorRunning       bool          `json:"janitorRunning"`
	StoreGrace           time.Duration `json:"storeGrace"`
	RunHistoryTTL        time.Duration `json:"runHistoryTtl"`
	MaxLogAge            time.Duration `json:"maxLogAge"`
	DefinitionPolicy     string        `json:"definitionPolicy"`
	FoldSnapshotsEnabled bool          `json:"foldSnapshotsEnabled"`

	// ActionLog is non-nil iff a log is configured AND it reports usage.
	ActionLog  *actionlog.LogUsage `json:"actionLog,omitempty"`
	LogBackend string              `json:"logBackend,omitempty"`
}

// Health returns the runtime's introspection snapshot. The backend query
// runs outside rt.mu (P4.6 discipline); a failing or absent UsageReporter
// simply leaves ActionLog nil — Health itself never fails.
func (rt *WorkflowRuntime) Health(ctx context.Context) RuntimeHealth {
	rt.mu.Lock()
	h := RuntimeHealth{
		ActiveRuns:           len(rt.active),
		PausedRuns:           len(rt.paused),
		PendingPauses:        len(rt.pendingPauses),
		CachedRunMetas:       len(rt.runMetas),
		CachedOutcomes:       len(rt.outcomes),
		LiveStores:           len(rt.stores),
		Uptime:               time.Since(rt.startedAt),
		JanitorRunning:       rt.janitorStop != nil,
		StoreGrace:           rt.storeGrace,
		RunHistoryTTL:        rt.runHistoryTTL,
		MaxLogAge:            rt.maxLogAge,
		DefinitionPolicy:     rt.definitionPolicy,
		FoldSnapshotsEnabled: !rt.disableFoldSnapshots,
	}
	if rt.actionLog != nil {
		h.LogBackend = fmtBackend(rt.actionLog)
	}
	rt.mu.Unlock()

	if usage, err := rt.StorageUsage(ctx); err == nil {
		h.ActionLog = &usage
	}
	return h
}

// fmtBackend names the log backend for the snapshot (type-derived; no
// registry — the runtime has exactly the two shipped backends plus hosts').
func fmtBackend(log actionlog.Store) string {
	switch log.(type) {
	case *actionlog.AnansiStore:
		return "anansi"
	case *actionlog.MemoryActionLog:
		return "memory"
	default:
		return "custom"
	}
}
