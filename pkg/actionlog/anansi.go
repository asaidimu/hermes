package actionlog

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/asaidimu/go-anansi/v8/core/data"
	"github.com/asaidimu/go-anansi/v8/core/document"
	"github.com/asaidimu/go-anansi/v8/core/persistence/base"
	anansicollection "github.com/asaidimu/go-anansi/v8/core/persistence/collection"
	"github.com/asaidimu/go-anansi/v8/core/query"
	"github.com/asaidimu/go-anansi/v8/core/schema/definition"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// LogEntry is the persisted representation of one run attempt's log.
// One document per (runID, rerunIndex); entries are an append-only JSON
// array stored as text (RawMessage has no anansi column mapping).
//
// The document ID is a deterministic function of the run identity it
// holds (logDocID) — the runID is data inside the document, never baked
// into the Store instance's scope.
type LogEntry struct {
	document.DocumentModel
	RunID      string `anansi:"runId"`
	RerunIndex int    `anansi:"rerunIndex"`
	Entries    string `anansi:"entries"`
}

// AnansiStoreFactory creates durable action log stores backed by an anansi
// ModelCollection. One document per (runID, rerunIndex).
type AnansiStoreFactory struct {
	models *anansicollection.ModelCollection[*LogEntry]
}

// ensureDocumentFactory configures anansi's singleton document factory.
var ensureDocumentFactory sync.Once

// NewAnansiStoreFactory creates a factory bound to the named action log
// collection, deriving its schema from LogEntry and creating the collection
// when it does not exist yet.
func NewAnansiStoreFactory(ctx context.Context, persist base.Persistence, collectionName string) (*AnansiStoreFactory, error) {
	ensureDocumentFactory.Do(func() {
		_ = data.ConfigureDocumentFactory(data.DocumentFactoryConfig{}, nil)
	})

	schemaBytes, err := data.ExtractDTOSchemaDirect(&LogEntry{})
	if err != nil {
		return nil, err
	}
	var sc definition.Schema
	if err := json.Unmarshal(schemaBytes, &sc); err != nil {
		return nil, err
	}
	sc.Name = collectionName

	ok, err := persist.HasCollection(ctx, collectionName)
	if err != nil {
		return nil, err
	}
	if !ok {
		if _, err := persist.CreateCollection(ctx, &sc); err != nil {
			return nil, err
		}
	}

	coll, err := persist.Collection(ctx, collectionName)
	if err != nil {
		return nil, err
	}

	models, err := anansicollection.NewModelCollection[*LogEntry](coll, zap.NewNop())
	if err != nil {
		return nil, err
	}

	return &AnansiStoreFactory{models: models}, nil
}

// NewStore returns a multi-run durable Store. A single instance serves
// every (runID, rerunIndex) log document in the collection — entries are
// routed by the identity they themselves carry at Append time, mirroring
// MemoryActionLog's semantics. This is the store to wire into
// WorkflowRuntime's Options.ActionLog, which is shared across all runs.
func (f *AnansiStoreFactory) NewStore() *AnansiStore {
	return &AnansiStore{models: f.models}
}

// actionLogNamespace namespaces the deterministic log document IDs so they
// are stable across processes and cannot collide with other hash uses.
var actionLogNamespace = uuid.NewSHA1(uuid.NameSpaceURL, []byte("github.com/asaidimu/hermes/pkg/actionlog"))

// logDocID derives the deterministic document ID for a (runID, rerunIndex)
// log. Document identity is a pure function of the identity carried by the
// entries — never of any store-level scope: the same (runID, rerunIndex)
// maps to the same document in every process and after every restart.
//
// anansi only accepts UUIDv7-shaped 32-hex document IDs (document.isValidID
// enforces version 7), so the readable "log:<runID>:<rerunIndex>" name is
// hashed (SHA-1 over the action-log namespace) and the digest is laid out
// in UUIDv7 format with version/variant bits forced. The 48-bit timestamp
// field carries hash bits — not a creation time — precisely because the ID
// must not depend on when it was minted. The runID itself remains a plain
// queryable field inside the document for enumeration (readByRunID).
func logDocID(runID string, rerunIndex int) string {
	name := actionLogNamespace.String() + ":log:" + runID + ":" + fmt.Sprint(rerunIndex)
	sum := sha1.Sum([]byte(name))
	var id uuid.UUID
	copy(id[:], sum[:16])
	// Force UUIDv7 layout: version nibble 0b0111, RFC 4122 variant.
	id[6] = (id[6] & 0x0f) | 0x70
	id[8] = (id[8] & 0x3f) | 0x80
	return strings.ReplaceAll(id.String(), "-", "")
}

// AnansiStore is a durable multi-run Store implementation backed by an anansi
// ModelCollection. It holds no run scope of its own: every operation takes the
// (runID, rerunIndex) it operates on, and Append routes each entry to the
// document derived from the entry's own RunID/RerunIndex. Every Append
// persists immediately — no batching — so at most one entry is lost on crash.
type AnansiStore struct {
	models *anansicollection.ModelCollection[*LogEntry]
	mu     sync.Mutex
}

var _ Store = (*AnansiStore)(nil)

func (s *AnansiStore) Append(ctx context.Context, entry Entry) (uint64, error) {
	// @note #review-20260910-014 issue status=resolved priority=P2 tags=#review,#durability,#integration : No durable Store can serve the multi-run runtime; Append force-rewrites RunID
	// @author hermes-review
	//
	// WorkflowRuntime takes ONE Options.ActionLog for ALL runs and appends
	// entries carrying their own RunID. The only durable implementation,
	// AnansiStore, used to be scoped to a single (runID, rerunIndex)
	// document and OVERWROTE entry.RunID/RerunIndex with its own scope —
	// so pointing Options.ActionLog at one AnansiStore silently merged
	// every run's log into one foreign runID, and the document itself had
	// no stable identity (it was located by querying the runId FIELD, a
	// full collection scan, with the runID pinned by the constructor).
	// Nothing outside tests wired the factory into the runtime either, so
	// the README's durable, event-sourced recovery story had no shipped
	// integration path.
	//
	// Resolved: runID is detached from document identity. Log documents
	// now carry a deterministic ID derived from the entry's own (RunID,
	// RerunIndex) — logDocID, a UUIDv7-shaped hash because anansi only
	// accepts 32-hex v7 document IDs — while runId remains a plain
	// queryable FIELD inside the document for rerun enumeration
	// (readByRunID). One AnansiStore (factory.NewStore(), no scope) now
	// serves the multi-run runtime exactly like MemoryActionLog: Append
	// routes by entry identity and rejects entries without a RunID, and
	// Seq derives from max(existing)+1 so numbering is stable even if an
	// entry ever fails to marshal. Reads go straight to the deterministic
	// document ID instead of scanning the collection on runId. The
	// durable path is wired end-to-end in
	// pkg/runtime/durable_log_test.go (two runs, one shared store,
	// Replayer rebuild off it). Breaking API change: NewStore dropped its
	// (runID, rerunIndex) scope parameters — the scoped handle was the
	// bug. The per-Append O(n) array rewrite remains (inherent to the
	// one-document-per-run schema); a throughput fix belongs with the
	// path-qualified addressing schema change tracked in
	// #review-20260910-015.
	//
	// Identity must come from the entry, not the store: an entry without
	// a RunID has nowhere to land. Fail loudly instead of silently
	// merging it into a foreign run's log.
	if entry.RunID == "" {
		return 0, fmt.Errorf("action log entry rejected: RunID is empty — set RunID (and RerunIndex) on the entry before Append")
	}
	if entry.RerunIndex < 0 {
		return 0, fmt.Errorf("action log entry rejected: RerunIndex %d is negative for run %s", entry.RerunIndex, entry.RunID)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now().UTC()
	}

	existing, docID, err := s.loadDocument(ctx, entry.RunID, entry.RerunIndex)
	if err != nil {
		return 0, err
	}

	// Seq derives from the highest recorded sequence number, not from the
	// entry count, so numbering stays stable even if an earlier entry ever
	// failed to marshal into the document.
	var nextSeq uint64 = 1
	for _, e := range existing {
		if e.Seq >= nextSeq {
			nextSeq = e.Seq + 1
		}
	}
	entry.Seq = nextSeq
	existing = append(existing, entry)

	dataBytes, err := json.Marshal(existing)
	if err != nil {
		return 0, err
	}

	if err := s.saveDocument(ctx, entry.RunID, entry.RerunIndex, docID, string(dataBytes)); err != nil {
		return 0, err
	}

	return nextSeq, nil
}

func (s *AnansiStore) All(ctx context.Context, runID string, rerunIndex int) ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, _, err := s.loadDocument(ctx, runID, rerunIndex)
	return entries, err
}

func (s *AnansiStore) ByStepID(ctx context.Context, runID string, rerunIndex int, stepID string) ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries, _, err := s.loadDocument(ctx, runID, rerunIndex)
	if err != nil {
		return nil, err
	}

	var result []Entry
	for _, e := range entries {
		if e.StepID == stepID {
			result = append(result, e)
		}
	}
	return result, nil
}

func (s *AnansiStore) LastByStageID(ctx context.Context, runID string, rerunIndex int, stageID string) (*Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries, _, err := s.loadDocument(ctx, runID, rerunIndex)
	if err != nil {
		return nil, err
	}

	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].StageID == stageID && entries[i].Kind == KindRouted {
			e := entries[i]
			return &e, nil
		}
	}
	return nil, nil
}

func (s *AnansiStore) Count(ctx context.Context, runID string, rerunIndex int) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries, _, err := s.loadDocument(ctx, runID, rerunIndex)
	if err != nil {
		return 0, err
	}
	return uint64(len(entries)), nil
}

func (s *AnansiStore) LatestRerunIndex(ctx context.Context, runID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	docs, err := s.readByRunID(ctx, runID)
	if err != nil {
		return -1, err
	}
	max := -1
	for _, d := range docs {
		if d.RerunIndex > max {
			max = d.RerunIndex
		}
	}
	return max, nil
}

// loadDocument reads the log document for a (runID, rerunIndex) pair by its
// deterministic document ID and returns the decoded entries plus the found
// document's ID (empty when the document does not exist yet).
func (s *AnansiStore) loadDocument(ctx context.Context, runID string, rerunIndex int) ([]Entry, string, error) {
	q := query.NewQueryBuilder().
		Where(data.DocumentIDField).Eq(logDocID(runID, rerunIndex)).
		Limit(1).
		Build()
	docs, err := s.models.Read(ctx, &q)
	if err != nil {
		return nil, "", err
	}
	if len(docs) == 0 {
		return nil, "", nil
	}
	d := docs[0]
	if d.Entries == "" {
		return nil, d.GetID(), nil
	}
	var entries []Entry
	if err := json.Unmarshal([]byte(d.Entries), &entries); err != nil {
		return nil, "", err
	}
	return entries, d.GetID(), nil
}

// readByRunID returns all log documents that carry a given runID as DATA.
// This field-level query only backs rerun-index enumeration (the one lookup
// that cannot be keyed by document ID); all other reads go through
// loadDocument's direct document-ID lookup.
func (s *AnansiStore) readByRunID(ctx context.Context, runID string) ([]*LogEntry, error) {
	q := query.NewQueryBuilder().Where("runId").Eq(runID).Build()
	return s.models.Read(ctx, &q)
}

// saveDocument writes the entries array immediately, targeting the
// deterministic document ID for the given run identity. Updates the existing
// document when present; creates it with the pre-derived ID on first write.
func (s *AnansiStore) saveDocument(ctx context.Context, runID string, rerunIndex int, docID string, entries string) error {
	if docID != "" {
		_, err := s.models.Update(ctx, docID, &LogEntry{
			RunID:      runID,
			RerunIndex: rerunIndex,
			Entries:    entries,
		})
		return err
	}

	model := document.New(&LogEntry{
		RunID:      runID,
		RerunIndex: rerunIndex,
		Entries:    entries,
	})
	model.ID = logDocID(runID, rerunIndex)
	_, err := s.models.Create(ctx, model)
	return err
}
