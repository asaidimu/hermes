package timeline

import (
	"encoding/json"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/events"
)

// TimelineEvent matches the exact JSON schema expected by the frontend
// timeline scrubber. Events are derived from the unified action log
// (pkg/actionlog) via EntryToTimelineEvent — the log is the source of
// truth; these shapes are the wire projection.
type TimelineEventSource string

const (
	SourcePipeline  TimelineEventSource = "pipeline"
	SourceStore     TimelineEventSource = "store"
	SourceContainer TimelineEventSource = "container"
	SourceLogger    TimelineEventSource = "logger"
)

type TimelineEvent struct {
	RunID     string              `json:"runId"`
	Seq       int64               `json:"seq"`
	Timestamp int64               `json:"timestamp"` // Epoch Milliseconds
	Source    TimelineEventSource `json:"source"`
	Type      string              `json:"type"`
	Path      events.EventPath    `json:"path"`
	Payload   map[string]any      `json:"payload"`
	Delta     map[string]any      `json:"delta,omitempty"`
	Snapshot  map[string]any      `json:"snapshot,omitempty"`
}

type RunTimelineStatus string

const (
	StatusRecording RunTimelineStatus = "recording"
	StatusComplete  RunTimelineStatus = "complete"
	StatusFailed    RunTimelineStatus = "failed"
	StatusPaused    RunTimelineStatus = "paused"
)

// RunTimelineMeta describes a run for history queries.
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

// EntryToTimelineEvent projects an action log entry onto the frontend wire
// shape. Labels are recovered from the entry payload when present; the
// caller supplies the pipeline identity (not stored per entry).
func EntryToTimelineEvent(e actionlog.Entry, pipelineID, pipelineLabel string) TimelineEvent {
	typ := e.Type
	if typ == "" {
		typ = string(e.Kind)
	}

	var payload map[string]any
	if len(e.Payload) > 0 {
		_ = json.Unmarshal(e.Payload, &payload)
	}
	if payload == nil {
		payload = make(map[string]any)
	}

	var delta map[string]any
	if len(e.Output) > 0 {
		_ = json.Unmarshal(e.Output, &delta)
	}

	path := events.EventPath{{Kind: "pipeline", ID: pipelineID, Label: pipelineLabel}}
	if e.StageID != "" {
		path = path.Append("stage", e.StageID, strField(payload, "stageLabel"))
	}
	if e.StepID != "" {
		path = path.Append("step", e.StepID, strField(payload, "stepLabel"))
	}

	return TimelineEvent{
		RunID:     e.RunID,
		Seq:       int64(e.Seq),
		Timestamp: e.Timestamp.UnixMilli(),
		Source:    SourcePipeline,
		Type:      typ,
		Path:      path,
		Payload:   payload,
		Delta:     delta,
	}
}

func strField(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	s, _ := m[key].(string)
	return s
}
