// Package registry tracks active runs in memory. It is a passive,
// best-effort view: registration is manual (a host or test registers a run
// it wants to track), nothing in pkg/runtime consults it, and it holds no
// recovery responsibility.
//
// 20261003 refactor (plan D.1): the "two-tier resumption" machinery —
// MarkPaused with time.AfterFunc pause-expiration timers and
// FastPathResume — was deleted. Nothing called it (the runtime has its own
// paused-run tracking; the single caller was a test), the expiration timer
// flipped a status no one read, and the README claim it backed was false.
package registry

import (
	"sync"
	"time"

	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
)

// ActiveRun represents a live, in-flight or paused pipeline execution.
type ActiveRun struct {
	RunID      string
	PipelineID string
	RunContext *pipeline.RunContextImpl
	Store      store.Store
	Status     string // "running" | "paused" | "completed" | "failed" | "aborted"
	CreatedAt  time.Time
}

// PipelineRegistry tracks active runs in memory.
type PipelineRegistry struct {
	mu   sync.RWMutex
	runs map[string]*ActiveRun
}

// NewPipelineRegistry creates a new pipeline registry.
func NewPipelineRegistry() *PipelineRegistry {
	return &PipelineRegistry{
		runs: make(map[string]*ActiveRun),
	}
}

// Register adds an active run into the registry.
func (r *PipelineRegistry) Register(run *ActiveRun) error {
	if run == nil || run.RunID == "" {
		return core.NewSystemError(core.ErrCodeValidation, "invalid run for registration")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if run.CreatedAt.IsZero() {
		run.CreatedAt = time.Now()
	}
	if run.Status == "" {
		run.Status = "running"
	}
	r.runs[run.RunID] = run
	return nil
}

// Get returns a copy of the ActiveRun for a given runID.
//
// @note #review-20260822-043 issue status=resolved priority=P1 tags=#review,#concurrency : Get returns mutable internal state pointer
//
// Resolved: return a shallow copy of the registry's internal ActiveRun
// instead of the live pointer. RunContext and Store are still shared (they
// are meant to be — the actual live execution context and its store), but
// the ActiveRun struct's own bookkeeping fields (Status, CreatedAt) are now
// decoupled: a caller mutating the returned value's Status can no longer
// race with the registry's own r.runs[runID] entry under r.mu.
func (r *PipelineRegistry) Get(runID string) (*ActiveRun, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	run, ok := r.runs[runID]
	return copyActiveRun(run), ok
}

// copyActiveRun returns a shallow copy of run, or nil if run is nil.
func copyActiveRun(run *ActiveRun) *ActiveRun {
	if run == nil {
		return nil
	}
	cp := *run
	return &cp
}

// Deregister removes a run from the registry.
func (r *PipelineRegistry) Deregister(runID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.runs, runID)
}

// List returns copies of all active runs (see the note on Get for why these
// are copies rather than live pointers into the registry's internal map).
func (r *PipelineRegistry) List() []*ActiveRun {
	r.mu.RLock()
	defer r.mu.RUnlock()

	list := make([]*ActiveRun, 0, len(r.runs))
	for _, run := range r.runs {
		list = append(list, copyActiveRun(run))
	}
	return list
}
