package registry

import (
	"context"
	"sync"
	"time"

	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/store"
)

// ActiveRun represents a live, in-flight or paused pipeline execution.
type ActiveRun struct {
	RunID        string
	PipelineID   string
	RunContext   *pipeline.RunContextImpl
	Store        store.Store
	Status       string // "running" | "paused" | "completed" | "failed" | "aborted"
	CreatedAt    time.Time
	PausedAt     *time.Time
	ExpiresAt    *time.Time
	timer        *time.Timer
	onExpiration func(runID string)
}

// RegistryOptions configures the pipeline registry.
type RegistryOptions struct {
	Logger       core.Logger
	OnExpiration func(runID string)
}

// PipelineRegistry tracks active runs in memory.
type PipelineRegistry struct {
	mu      sync.RWMutex
	runs    map[string]*ActiveRun
	options RegistryOptions
}

// NewPipelineRegistry creates a new pipeline registry.
func NewPipelineRegistry(opts ...RegistryOptions) *PipelineRegistry {
	var opt RegistryOptions
	if len(opts) > 0 {
		opt = opts[0]
	}
	if opt.Logger == nil {
		opt.Logger = core.NopLogger{}
	}
	return &PipelineRegistry{
		runs:    make(map[string]*ActiveRun),
		options: opt,
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

// Get returns the ActiveRun for a given runID.
func (r *PipelineRegistry) Get(runID string) (*ActiveRun, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	run, ok := r.runs[runID]
	// @note #review-20260822-043 issue status=open priority=P1 tags=#review,#concurrency : Get returns mutable internal state pointer
	//
	// Get returns a pointer to the internal ActiveRun under RLock. After RUnlock, the
	// caller holds a reference to mutable internal state. If Deregister or MarkPaused is
	// called concurrently, the caller's ActiveRun may be modified or its timer stopped.
	//
	// Fix by returning a copy of ActiveRun, or by documenting that the caller must not
	// mutate the returned value.
	return run, ok
}

// Deregister removes a run from the registry and stops any expiration timers.
func (r *PipelineRegistry) Deregister(runID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if run, ok := r.runs[runID]; ok {
		if run.timer != nil {
			run.timer.Stop()
		}
		delete(r.runs, runID)
	}
}

// MarkPaused sets a run to paused state and initiates an expiration timer if timeout > 0.
func (r *PipelineRegistry) MarkPaused(runID string, timeout time.Duration, onExpired ...func(runID string)) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	run, ok := r.runs[runID]
	if !ok {
		return core.NewSystemError(core.ErrCodeNotFound, "run not found: "+runID)
	}

	now := time.Now()
	run.Status = "paused"
	run.PausedAt = &now

	if timeout > 0 {
		exp := now.Add(timeout)
		run.ExpiresAt = &exp

		var expCb func(id string)
		if len(onExpired) > 0 && onExpired[0] != nil {
			expCb = onExpired[0]
		} else if r.options.OnExpiration != nil {
			expCb = r.options.OnExpiration
		}

		if run.timer != nil {
			run.timer.Stop()
		}

		run.timer = time.AfterFunc(timeout, func() {
			r.mu.Lock()
			if active, exists := r.runs[runID]; exists && active.Status == "paused" {
				active.Status = "expired"
			}
			r.mu.Unlock()

			if expCb != nil {
				expCb(runID)
			}
		})
	}

	return nil
}

// FastPathResume retrieves an in-memory run for fast resumption.
func (r *PipelineRegistry) FastPathResume(ctx context.Context, runID string) (*ActiveRun, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	run, ok := r.runs[runID]
	if !ok {
		return nil, core.NewSystemError(core.ErrCodeNotFound, "run not found: "+runID)
	}

	if run.timer != nil {
		run.timer.Stop()
		run.timer = nil
	}

	run.Status = "running"
	run.ExpiresAt = nil
	return run, nil
}

// List returns all active runs.
func (r *PipelineRegistry) List() []*ActiveRun {
	r.mu.RLock()
	defer r.mu.RUnlock()

	list := make([]*ActiveRun, 0, len(r.runs))
	for _, run := range r.runs {
		list = append(list, run)
	}
	return list
}
