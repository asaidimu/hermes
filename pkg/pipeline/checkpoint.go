package pipeline

import (
	"encoding/json"
	"time"

	"github.com/asaidimu/hermes/pkg/core"
)

// PipelineDataKey is the state key storing execution checkpoints. Checkpoints
// are ordinary state: they live in the run's flat state map and persist as
// part of it (under the "state" column for anansi-backed stores).
const PipelineDataKey = "__pipeline_data__"

// EntryAddress points to a target resume location inside a pipeline or nested subpipeline.
type EntryAddress struct {
	Stage    string              `json:"stage"`
	Step     string              `json:"step,omitempty"`
	Pipeline *SubPipelineAddress `json:"pipeline,omitempty"`
}

// SubPipelineAddress points to an address within an indexed subpipeline.
type SubPipelineAddress struct {
	Index int    `json:"index"`
	Stage string `json:"stage"`
	Step  string `json:"step,omitempty"`
}

// PipelineCheckpoint stores the serialized resume state.
type PipelineCheckpoint struct {
	RunID              string         `json:"runId"`
	PipelineID         string         `json:"pipelineId"`
	PausedAtStageID    string         `json:"pausedAtStageId"`
	PausedAtStageLabel string         `json:"pausedAtStageLabel"`
	PausedOn           string         `json:"pausedOn"` // ISO-8606
	ResumeAt           EntryAddress   `json:"resumeAt"`
	WaitForEvent       string         `json:"waitForEvent,omitempty"`
	WaitForEvents      []string       `json:"waitForEvents,omitempty"`
	WaitMode           string         `json:"waitMode,omitempty"` // "any" or "all"
	Timeout            int64          `json:"timeout,omitempty"`  // milliseconds, 0 = no timeout
	ReceivedEvents     []string       `json:"receivedEvents,omitempty"`
	Cron               string         `json:"cron,omitempty"`         // cron expression for auto-resume (e.g. "@every 5m")
	ResumeReason       string         `json:"resumeReason,omitempty"` // "event" or "timeout"
	Snapshot           map[string]any `json:"snapshot,omitempty"`     // state snapshot at pause time
}

// WriteCheckpoint saves a checkpoint into the state map under PipelineDataKey.
// Mutates state in place; callers invoke it inside a store.Update closure so
// the change goes through locking and write-through persistence.
func WriteCheckpoint(state map[string]any, ckpt PipelineCheckpoint) error {
	if ckpt.PausedOn == "" {
		ckpt.PausedOn = time.Now().UTC().Format(time.RFC3339)
	}

	var pipelineData map[string]any
	if m, ok := state[PipelineDataKey].(map[string]any); ok {
		pipelineData = m
	}
	if pipelineData == nil {
		pipelineData = make(map[string]any)
	}

	var checkpoints map[string]any
	if cVal, ok := pipelineData["checkpoints"]; ok {
		if cm, ok := cVal.(map[string]any); ok {
			checkpoints = cm
		}
	}
	if checkpoints == nil {
		checkpoints = make(map[string]any)
	}

	ckptBytes, err := json.Marshal(ckpt)
	if err != nil {
		return core.SystemErrorFrom(err, core.ErrCodeExecutionFailed)
	}

	var ckptMap map[string]any
	if err := json.Unmarshal(ckptBytes, &ckptMap); err != nil {
		return core.SystemErrorFrom(err, core.ErrCodeExecutionFailed)
	}

	checkpoints[ckpt.PipelineID] = ckptMap
	pipelineData["checkpoints"] = checkpoints
	state[PipelineDataKey] = pipelineData
	return nil
}

// ReadCheckpoint retrieves a checkpoint from the state map for a given
// pipelineID. Returns nil when absent.
func ReadCheckpoint(state map[string]any, pipelineID string) (*PipelineCheckpoint, error) {
	v, ok := state[PipelineDataKey]
	if !ok {
		return nil, nil
	}

	pipelineData, ok := v.(map[string]any)
	if !ok {
		return nil, nil
	}

	cVal, ok := pipelineData["checkpoints"]
	if !ok {
		return nil, nil
	}

	checkpoints, ok := cVal.(map[string]any)
	if !ok {
		return nil, nil
	}

	ckptRaw, ok := checkpoints[pipelineID]
	if !ok {
		return nil, nil
	}

	ckptBytes, err := json.Marshal(ckptRaw)
	if err != nil {
		return nil, core.SystemErrorFrom(err, core.ErrCodeExecutionFailed)
	}

	var ckpt PipelineCheckpoint
	if err := json.Unmarshal(ckptBytes, &ckpt); err != nil {
		return nil, core.SystemErrorFrom(err, core.ErrCodeExecutionFailed)
	}

	return &ckpt, nil
}
