// Package actionlog implements the unified, append-only Action Log.
//
// The log records every significant event during pipeline execution:
// pipeline/stage/step lifecycle events, routing decisions, state deltas,
// errors, and pause/resume metadata. It serves two purposes:
//
//   - Crash recovery: the Replayer reads the log to reconstruct state
//     by re-executing pure steps and injecting recorded deltas for
//     effectful steps.
//   - Observability: the full log is the audit trail for debugging,
//     with state derivable at any point by replaying entries forward.
//
// Each run's log is keyed by (runID, rerunIndex). Retries of the same
// run get separate log documents for audit purposes.
package actionlog

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

// EntryKind classifies an action log entry.
type EntryKind string

const (
	// Pipeline lifecycle
	KindPipelineStarted   EntryKind = "pipeline_started"
	KindPipelineCompleted EntryKind = "pipeline_completed"
	KindPipelineFailed    EntryKind = "pipeline_failed"

	// Stage lifecycle
	KindStageStarted   EntryKind = "stage_started"
	KindStageCompleted EntryKind = "stage_completed"
	KindStageFailed    EntryKind = "stage_failed"

	// Step lifecycle (all steps, pure and effectful)
	KindStepStarted   EntryKind = "step_started"
	KindStepCompleted EntryKind = "step_completed"
	KindStepFailed    EntryKind = "step_failed"
	KindStepRetry     EntryKind = "step_retry"

	// Effectful step outcomes (for crash recovery — replayer filters to these)
	KindEffectCompleted EntryKind = "effect_completed"
	KindEffectFailed    EntryKind = "effect_failed"

	// Routing decisions
	KindRouted EntryKind = "routed"

	// Pause/resume
	KindCheckpoint EntryKind = "checkpoint"
	KindResumed    EntryKind = "resumed"

	// Sub-pipelines
	KindSubpipelineForked EntryKind = "subpipeline_forked"
	KindSubpipelineJoined EntryKind = "subpipeline_joined"
)

// Entry is a durable fact in the action log. Records every significant
// event during pipeline execution for debugging, audit, and crash recovery.
type Entry struct {
	RunID      string    `json:"runId"`
	RerunIndex int       `json:"rerunIndex"`
	Seq        uint64    `json:"seq"`
	Kind       EntryKind `json:"kind"`
	// PipelineID identifies the pipeline (root or subpipeline instance)
	// whose execution produced this entry. Root entries carry the run's
	// primary pipeline id; subpipeline entries carry the child definition
	// id (e.g. "<nodeId>__body" for try-catch bodies, "<nodeId>__0" for
	// distribute item 0). Entries written before this field existed (and
	// entries outside any pipeline context) leave it empty; consumers
	// treat empty as "the run's primary pipeline" for backward
	// compatibility. See #review-20260910-015.
	PipelineID string          `json:"pipelineId,omitempty"`
	StageID    string          `json:"stageId,omitempty"`
	StepID     string          `json:"stepId,omitempty"`
	Handle     string          `json:"handle,omitempty"`  // routing handle
	Type       string          `json:"type,omitempty"`    // frontend event type (e.g. "step:success")
	Payload    json.RawMessage `json:"payload,omitempty"` // event-specific data
	Output     json.RawMessage `json:"output,omitempty"`  // state delta
	Error      string          `json:"error,omitempty"`
	Duration   int64           `json:"duration,omitempty"` // ms
	Attempt    int             `json:"attempt,omitempty"`  // retry attempt number
	Timestamp  time.Time       `json:"timestamp"`
}

// Store is the persistence interface for action log entries. Implementations
// must be safe for concurrent use and persist immediately on Append.
type Store interface {
	// Append adds an entry to the log. Implementations must assign a
	// monotonically increasing Seq and set Timestamp if zero. The entry
	// must be persisted immediately (no batching) for durability.
	Append(ctx context.Context, entry Entry) (uint64, error)

	// All returns all entries for a run at the given rerunIndex, in Seq
	// order. Returns nil (not an error) when no entries exist.
	All(ctx context.Context, runID string, rerunIndex int) ([]Entry, error)

	// ByStepID returns entries for a specific step within a run, in Seq
	// order.
	ByStepID(ctx context.Context, runID string, rerunIndex int, stepID string) ([]Entry, error)

	// LastByStageID returns the most recent routing entry for a stage,
	// if any.
	LastByStageID(ctx context.Context, runID string, rerunIndex int, stageID string) (*Entry, error)

	// Count returns the total number of entries for a run at the given
	// rerunIndex.
	Count(ctx context.Context, runID string, rerunIndex int) (uint64, error)

	// LatestRerunIndex returns the highest rerunIndex for a runID, or -1
	// if no log exists for the run.
	LatestRerunIndex(ctx context.Context, runID string) (int, error)
}

// NopLog is a no-op Store implementation used when action logging is
// disabled or not yet configured. All methods return zero values and nil error.
type NopLog struct{}

func (NopLog) Append(_ context.Context, _ Entry) (uint64, error)       { return 0, nil }
func (NopLog) All(_ context.Context, _ string, _ int) ([]Entry, error) { return nil, nil }
func (NopLog) ByStepID(_ context.Context, _ string, _ int, _ string) ([]Entry, error) {
	return nil, nil
}
func (NopLog) LastByStageID(_ context.Context, _ string, _ int, _ string) (*Entry, error) {
	return nil, nil
}
func (NopLog) Count(_ context.Context, _ string, _ int) (uint64, error)  { return 0, nil }
func (NopLog) LatestRerunIndex(_ context.Context, _ string) (int, error) { return -1, nil }

// MemoryActionLog is an in-memory Store implementation for testing and
// development. Not durable — data is lost on process exit. Callers that
// need persistence should provide a Store backed by a database.
type MemoryActionLog struct {
	mu      sync.Mutex
	entries map[string]map[int][]Entry // runID -> rerunIndex -> entries
	seqs    map[string]map[int]uint64  // runID -> rerunIndex -> next seq
}

// NewMemoryActionLog creates a new in-memory action log.
func NewMemoryActionLog() *MemoryActionLog {
	return &MemoryActionLog{
		entries: make(map[string]map[int][]Entry),
		seqs:    make(map[string]map[int]uint64),
	}
}

func (m *MemoryActionLog) Append(_ context.Context, entry Entry) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now().UTC()
	}

	runID := entry.RunID
	rerunIdx := entry.RerunIndex

	if m.entries[runID] == nil {
		m.entries[runID] = make(map[int][]Entry)
		m.seqs[runID] = make(map[int]uint64)
	}

	nextSeq := m.seqs[runID][rerunIdx] + 1
	entry.Seq = nextSeq
	m.seqs[runID][rerunIdx] = nextSeq
	m.entries[runID][rerunIdx] = append(m.entries[runID][rerunIdx], entry)
	return nextSeq, nil
}

func (m *MemoryActionLog) All(_ context.Context, runID string, rerunIndex int) ([]Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entries := m.entries[runID][rerunIndex]
	if len(entries) == 0 {
		return nil, nil
	}
	out := make([]Entry, len(entries))
	copy(out, entries)
	return out, nil
}

func (m *MemoryActionLog) ByStepID(_ context.Context, runID string, rerunIndex int, stepID string) ([]Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []Entry
	for _, e := range m.entries[runID][rerunIndex] {
		if e.StepID == stepID {
			result = append(result, e)
		}
	}
	return result, nil
}

func (m *MemoryActionLog) LastByStageID(_ context.Context, runID string, rerunIndex int, stageID string) (*Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entries := m.entries[runID][rerunIndex]
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].StageID == stageID && entries[i].Kind == KindRouted {
			e := entries[i]
			return &e, nil
		}
	}
	return nil, nil
}

func (m *MemoryActionLog) Count(_ context.Context, runID string, rerunIndex int) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return uint64(len(m.entries[runID][rerunIndex])), nil
}

func (m *MemoryActionLog) LatestRerunIndex(_ context.Context, runID string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	indices := m.entries[runID]
	if len(indices) == 0 {
		return -1, nil
	}
	max := -1
	for idx := range indices {
		if idx > max {
			max = idx
		}
	}
	return max, nil
}
