package store

import (
	"github.com/asaidimu/go-anansi/v8/core/document"
)

// Column names produced by the anansi tags below. Exported for tests and
// observability tooling that inspects persisted rows.
const (
	StateColumn   = "state"
	RunMetaColumn = "metadata"
	RunMetaKey    = "__run_meta__" // body key mirroring RunInfo in the flat in-memory view
)

// RunMetadata carries run linkage — which workflow/trigger/pipeline owns this
// run. It is APPLICATION metadata persisted under the "metadata" column; it is
// NOT the same as the reserved system "_metadata_" container.
type RunMetadata struct {
	WorkflowID string `anansi:"workflowId,omitempty"`
	TriggerID  string `anansi:"triggerId,omitempty"`
	PipelineID string `anansi:"pipelineId,omitempty"`
}

// RunData is the pipeline-visible state: arbitrary keys addressed by node
// configs (e.g. `total`, `status`). Stored under the "state" column.
type RunData map[string]any

// PipelineState is the persisted representation of a run's state document,
// modeled as a struct so anansi can derive the runs collection schema from it
// (see AGENTS.md "Anansi data modelling via structs").
//
// A run IS its state document: the embedded DocumentModel provides the
// system-minted UUIDv7 identity (_id_) and system metadata (_metadata_).
// The runtime works on a flat in-memory view (the RunData map IS the
// document body); PersistentStore translates between that flat view and
// this typed shape on load/persist.
type PipelineState struct {
	document.DocumentModel             // system fields (_id_, _metadata_)
	Data                   RunData     `anansi:"state"`
	RunInfo                RunMetadata `anansi:"metadata"` // application run linkage, distinct from reserved _metadata_
}

// RunInfoFromMap decodes a RunMetaKey body entry into RunMetadata.
func RunInfoFromMap(m map[string]any) RunMetadata {
	var info RunMetadata
	if m == nil {
		return info
	}
	info.WorkflowID, _ = m["workflowId"].(string)
	info.TriggerID, _ = m["triggerId"].(string)
	info.PipelineID, _ = m["pipelineId"].(string)
	return info
}

// Map renders RunMetadata back into the flat body representation.
func (r RunMetadata) Map() map[string]any {
	m := make(map[string]any, 3)
	if r.WorkflowID != "" {
		m["workflowId"] = r.WorkflowID
	}
	if r.TriggerID != "" {
		m["triggerId"] = r.TriggerID
	}
	if r.PipelineID != "" {
		m["pipelineId"] = r.PipelineID
	}
	return m
}
