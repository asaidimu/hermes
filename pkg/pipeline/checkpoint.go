package pipeline

import (
        "encoding/json"
        "time"

        "github.com/asaidimu/hermes/pkg/core"
)

// PipelineDataKey is the state key storing execution checkpoints. Checkpoints
// are ordinary state: they live in the run's flat state map and persist as
// part of it (under the "state" column for anansi-backed stores).
//
// AUTHORITY (20261003 refactor, plan R.2): WriteCheckpoint and ReadCheckpoint
// are the ONLY sanctioned checkpoint serializer/parser. Every pause writes
// through WriteCheckpoint (root pipeline: handlePause in routing_interp.go;
// nested subpipeline bubbling: context.go's resume loop) and every legacy
// checkpoint resume reads through ReadCheckpoint (factory.Resume). New code
// must not hand-roll the __pipeline_data__ shape.
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
        Cron               string       `json:"cron,omitempty"`         // cron expression for auto-resume (e.g. "@every 5m")
        ResumeReason       string       `json:"resumeReason,omitempty"` // "event" or "timeout"
}

// WriteCheckpoint saves a checkpoint into the state map under PipelineDataKey.
// Mutates state in place; callers invoke it inside a store.Update closure so
// the change goes through locking.
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
//
// C4-fix (M5, 20261003): callers that already hold a store lock must not
// run this function's JSON round trip inside the lock. Use
// ExtractCheckpointRaw under the lock and ParseCheckpoint outside it —
// ReadCheckpoint itself is now just the composition of the two and is
// meant for lock-free contexts.
func ReadCheckpoint(state map[string]any, pipelineID string) (*PipelineCheckpoint, error) {
        return ParseCheckpoint(ExtractCheckpointRaw(state, pipelineID))
}

// ExtractCheckpointRaw pulls the raw checkpoint value for pipelineID out of
// the state map WITHOUT any JSON round trip. Cheap and side-effect free:
// safe to call under a store read lock. Returns nil when absent.
func ExtractCheckpointRaw(state map[string]any, pipelineID string) any {
        v, ok := state[PipelineDataKey]
        if !ok {
                return nil
        }

        pipelineData, ok := v.(map[string]any)
        if !ok {
                return nil
        }

        cVal, ok := pipelineData["checkpoints"]
        if !ok {
                return nil
        }

        checkpoints, ok := cVal.(map[string]any)
        if !ok {
                return nil
        }

        return checkpoints[pipelineID]
}

// ParseCheckpoint decodes a raw checkpoint value (as produced by
// ExtractCheckpointRaw or WriteCheckpoint) into a PipelineCheckpoint.
// Returns (nil, nil) for a nil input. The JSON round trip here is the
// I/O-shaped work C4-fix moved OUT of store read locks.
func ParseCheckpoint(raw any) (*PipelineCheckpoint, error) {
        if raw == nil {
                return nil, nil
        }

        ckptBytes, err := json.Marshal(raw)
        if err != nil {
                return nil, core.SystemErrorFrom(err, core.ErrCodeExecutionFailed)
        }

        var ckpt PipelineCheckpoint
        if err := json.Unmarshal(ckptBytes, &ckpt); err != nil {
                return nil, core.SystemErrorFrom(err, core.ErrCodeExecutionFailed)
        }

        return &ckpt, nil
}

