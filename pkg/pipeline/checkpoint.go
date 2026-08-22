package pipeline

import (
	"encoding/json"
	"time"

	"github.com/asaidimu/go-anansi/v8/core/document"
	"github.com/asaidimu/hermes/pkg/core"
)

// PipelineDataKey is the document metadata key storing execution checkpoints and state.
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
	RunID              string       `json:"runId"`
	PipelineID         string       `json:"pipelineId"`
	PausedAtStageID    string       `json:"pausedAtStageId"`
	PausedAtStageLabel string       `json:"pausedAtStageLabel"`
	PausedOn           string       `json:"pausedOn"` // ISO-8606
	ResumeAt           EntryAddress `json:"resumeAt"`
	WaitForEvent       string       `json:"waitForEvent,omitempty"`
	WaitForEvents      []string     `json:"waitForEvents,omitempty"`
	WaitMode           string       `json:"waitMode,omitempty"` // "any" or "all"
	Timeout            int64        `json:"timeout,omitempty"`  // milliseconds, 0 = no timeout
	ReceivedEvents     []string     `json:"receivedEvents,omitempty"`
	Cron               string       `json:"cron,omitempty"`        // cron expression for auto-resume (e.g. "@every 5m")
	ResumeReason       string       `json:"resumeReason,omitempty"` // "event" or "timeout"
}

// WriteCheckpoint saves a checkpoint into the Anansi document metadata.
func WriteCheckpoint(doc *document.Document, ckpt PipelineCheckpoint) error {
	if doc == nil {
		return core.NewSystemError(core.ErrCodeValidation, "cannot write checkpoint to nil document")
	}

	if ckpt.PausedOn == "" {
		ckpt.PausedOn = time.Now().UTC().Format(time.RFC3339)
	}

	// Retrieve existing __pipeline_data__ or create new
	var pipelineData map[string]any
	rawMeta := doc.Metadata()
	if rawMeta != nil {
		if val, ok := rawMeta[PipelineDataKey]; ok {
			if m, ok := val.(map[string]any); ok {
				pipelineData = m
			}
		}
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

	return doc.SetMetadataValue(PipelineDataKey, pipelineData)
}

// ReadCheckpoint retrieves a checkpoint from document metadata for a given pipelineID.
func ReadCheckpoint(doc *document.Document, pipelineID string) (*PipelineCheckpoint, error) {
	if doc == nil {
		return nil, core.NewSystemError(core.ErrCodeNotFound, "document is nil")
	}

	rawMeta := doc.Metadata()
	if rawMeta == nil {
		return nil, nil
	}

	val, ok := rawMeta[PipelineDataKey]
	if !ok {
		return nil, nil
	}

	pipelineData, ok := val.(map[string]any)
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
