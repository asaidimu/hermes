package timeline

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"

	"github.com/asaidimu/go-anansi/v8/core/document"
	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/store"
)


type TimelineEventSource string

const (
	SourcePipeline  TimelineEventSource = "pipeline"
	SourceStore     TimelineEventSource = "store"
	SourceContainer TimelineEventSource = "container"
	SourceLogger    TimelineEventSource = "logger"
)

// TimelineEvent matches the exact JSON schema expected by the frontend timeline scrubber.
type TimelineEvent struct {
	RunID     string               `json:"runId"`
	Seq       int64                `json:"seq"`
	Timestamp int64                `json:"timestamp"` // Epoch Milliseconds
	Source    TimelineEventSource  `json:"source"`
	Type      string               `json:"type"`
	Path      events.EventPath     `json:"path"`
	Payload   map[string]any       `json:"payload"`
	Delta     map[string]any       `json:"delta,omitempty"`
	Snapshot  map[string]any       `json:"snapshot,omitempty"`
}

type RunTimelineStatus string

const (
	StatusRecording RunTimelineStatus = "recording"
	StatusComplete  RunTimelineStatus = "complete"
	StatusFailed    RunTimelineStatus = "failed"
	StatusPaused    RunTimelineStatus = "paused"
)

// RunTimelineMeta matches the response of GET /runs/:runId
type RunTimelineMeta struct {
	RunID            string            `json:"runId"`
	PipelineID       string            `json:"pipelineId"`
	StartTime        int64             `json:"startTime"` // Epoch Milliseconds
	EndTime          *int64            `json:"endTime,omitempty"`
	EventCount       int64             `json:"eventCount"`
	Status           RunTimelineStatus `json:"status"`
	SnapshotInterval int               `json:"snapshotInterval"`
	SnapshotSeqs     []int64           `json:"snapshotSeqs"`
	Metadata         map[string]any    `json:"metadata,omitempty"`
}

// TimelineStore manages append-only storage and indexed retrieval of timeline events.
type TimelineStore interface {
	Append(ctx context.Context, event TimelineEvent) error
	GetRunMeta(ctx context.Context, runID string) (*RunTimelineMeta, error)
	ListRuns(ctx context.Context) ([]RunTimelineMeta, error)
	GetEvents(ctx context.Context, runID string, fromSeq, toSeq int64) ([]TimelineEvent, error)
	GetNearestSnapshot(ctx context.Context, runID string, seq int64) (*TimelineEvent, error)
}

// MemoryTimelineStore is a concurrent-safe in-memory store for timeline events and runs.
type MemoryTimelineStore struct {
	mu     sync.RWMutex
	runs   map[string]*RunTimelineMeta
	events map[string][]TimelineEvent
}

func NewMemoryTimelineStore() *MemoryTimelineStore {
	return &MemoryTimelineStore{
		runs:   make(map[string]*RunTimelineMeta),
		events: make(map[string][]TimelineEvent),
	}
}

func (s *MemoryTimelineStore) Append(ctx context.Context, event TimelineEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	meta, ok := s.runs[event.RunID]
	if !ok {
		meta = &RunTimelineMeta{
			RunID:            event.RunID,
			StartTime:        event.Timestamp,
			Status:           StatusRecording,
			SnapshotInterval: 10,
			SnapshotSeqs:     make([]int64, 0),
		}
		s.runs[event.RunID] = meta
	}

	meta.EventCount++
	if event.Type == "pipeline:success" {
		meta.Status = StatusComplete
		endTime := event.Timestamp
		meta.EndTime = &endTime
	} else if event.Type == "pipeline:error" {
		meta.Status = StatusFailed
		endTime := event.Timestamp
		meta.EndTime = &endTime
	} else if event.Type == "pipeline:pause" {
		meta.Status = StatusPaused
	}

	if event.Snapshot != nil {
		meta.SnapshotSeqs = append(meta.SnapshotSeqs, event.Seq)
	}

	s.events[event.RunID] = append(s.events[event.RunID], event)
	return nil
}

func (s *MemoryTimelineStore) GetRunMeta(ctx context.Context, runID string) (*RunTimelineMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	meta, ok := s.runs[runID]
	if !ok {
		return nil, core.NewSystemError(core.ErrCodeNotFound, "run not found: "+runID)
	}
	cp := *meta
	return &cp, nil
}

func (s *MemoryTimelineStore) ListRuns(ctx context.Context) ([]RunTimelineMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	list := make([]RunTimelineMeta, 0, len(s.runs))
	for _, m := range s.runs {
		list = append(list, *m)
	}
	return list, nil
}

func (s *MemoryTimelineStore) GetEvents(ctx context.Context, runID string, fromSeq, toSeq int64) ([]TimelineEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	evts, ok := s.events[runID]
	if !ok {
		return []TimelineEvent{}, nil
	}

	res := make([]TimelineEvent, 0)
	for _, e := range evts {
		if (fromSeq <= 0 || e.Seq >= fromSeq) && (toSeq <= 0 || e.Seq <= toSeq) {
			res = append(res, e)
		}
	}
	return res, nil
}

func (s *MemoryTimelineStore) GetNearestSnapshot(ctx context.Context, runID string, seq int64) (*TimelineEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	evts, ok := s.events[runID]
	if !ok {
		return nil, nil
	}

	var best *TimelineEvent
	for i := range evts {
		e := &evts[i]
		if e.Snapshot != nil && e.Seq <= seq {
			best = e
		}
	}
	return best, nil
}

// TimelineRecorder subscribes to pipeline events, increments sequence numbers, and writes snapshots.
type TimelineRecorder struct {
	runID            string
	pipelineID       string
	store            TimelineStore
	seqCounter       atomic.Int64
	snapshotInterval int
}

func NewTimelineRecorder(runID, pipelineID string, tStore TimelineStore, snapshotInterval ...int) *TimelineRecorder {
	interval := 10
	if len(snapshotInterval) > 0 && snapshotInterval[0] > 0 {
		interval = snapshotInterval[0]
	}
	return &TimelineRecorder{
		runID:            runID,
		pipelineID:       pipelineID,
		store:            tStore,
		snapshotInterval: interval,
	}
}

// Attach registers the recorder to an event bus and records document snapshots on stage boundaries.
func (r *TimelineRecorder) Attach(bus events.ScopedEventBus, st store.Store) (unsubscribe func()) {
	return bus.Subscribe("*", func(ctx context.Context, evt events.PipelineEvent) error {
		seq := r.seqCounter.Add(1)

		var snapshot map[string]any
		if seq%int64(r.snapshotInterval) == 0 || evt.Type == "pipeline:start" || evt.Type == "pipeline:success" || evt.Type == "pipeline:pause" {
			if st != nil {
				snapshot, _ = st.ExportJSON()
			}
		}

		source := SourcePipeline
		if evt.Type == "store:update" {
			source = SourceStore
		}

		tEvt := TimelineEvent{
			RunID:     r.runID,
			Seq:       seq,
			Timestamp: evt.Timestamp,
			Source:    source,
			Type:      evt.Type,
			Path:      evt.Path,
			Payload:   evt.Payload,
			Snapshot:  snapshot,
		}

		return r.store.Append(ctx, tEvt)
	})
}

// TimelinePlayer provides time-travel playback over a recorded timeline.
type TimelinePlayer struct {
	store      TimelineStore
	runID      string
	currentSeq int64
}

func NewTimelinePlayer(tStore TimelineStore, runID string) *TimelinePlayer {
	return &TimelinePlayer{
		store: tStore,
		runID: runID,
	}
}

// Seek moves playback to a specific sequence number and reconstructs document state.
func (p *TimelinePlayer) Seek(ctx context.Context, targetSeq int64) (*document.Document, error) {
	snap, err := p.store.GetNearestSnapshot(ctx, p.runID, targetSeq)
	if err != nil {
		return nil, err
	}

	var state map[string]any
	startSeq := int64(0)

	if snap != nil && snap.Snapshot != nil {
		stateBytes, _ := json.Marshal(snap.Snapshot)
		_ = json.Unmarshal(stateBytes, &state)
		startSeq = snap.Seq
	}
	if state == nil {
		state = make(map[string]any)
	}

	doc := document.NewRecordView(state)

	// Replay delta events from snapshot up to targetSeq
	events, err := p.store.GetEvents(ctx, p.runID, startSeq+1, targetSeq)
	if err != nil {
		return nil, err
	}

	for _, e := range events {
		if e.Delta != nil {
			for k, v := range e.Delta {
				_ = doc.Set(k, v)
			}
		}
	}

	p.currentSeq = targetSeq
	return doc, nil
}
