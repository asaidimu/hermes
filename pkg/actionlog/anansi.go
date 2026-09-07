package actionlog

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/asaidimu/go-anansi/v8/core/data"
	"github.com/asaidimu/go-anansi/v8/core/document"
	"github.com/asaidimu/go-anansi/v8/core/persistence/base"
	anansicollection "github.com/asaidimu/go-anansi/v8/core/persistence/collection"
	"github.com/asaidimu/go-anansi/v8/core/query"
	"github.com/asaidimu/go-anansi/v8/core/schema/definition"
	"go.uber.org/zap"
)

// LogEntry is the persisted representation of one run attempt's log.
// One document per (runID, rerunIndex); entries are an append-only JSON
// array stored as text (RawMessage has no anansi column mapping).
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

// NewStore returns a Store scoped to a single (runID, rerunIndex) log document.
func (f *AnansiStoreFactory) NewStore(runID string, rerunIndex int) *AnansiStore {
	return &AnansiStore{
		models:     f.models,
		runID:      runID,
		rerunIndex: rerunIndex,
	}
}

// AnansiStore is a durable Store implementation backed by an anansi
// ModelCollection. Scoped to one (runID, rerunIndex) document. Every Append
// persists immediately — no batching — so at most one entry is lost on crash.
type AnansiStore struct {
	models     *anansicollection.ModelCollection[*LogEntry]
	runID      string
	rerunIndex int
	mu         sync.Mutex
}

var _ Store = (*AnansiStore)(nil)

func (s *AnansiStore) Append(ctx context.Context, entry Entry) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now().UTC()
	}
	// The scoped store is authoritative for identity: entries always
	// land in this store's (runID, rerunIndex) document.
	entry.RunID = s.runID
	entry.RerunIndex = s.rerunIndex

	existing, docID, err := s.loadEntries(ctx)
	if err != nil {
		return 0, err
	}

	nextSeq := uint64(len(existing) + 1)
	entry.Seq = nextSeq
	existing = append(existing, entry)

	dataBytes, err := json.Marshal(existing)
	if err != nil {
		return 0, err
	}

	if err := s.saveEntries(ctx, docID, string(dataBytes)); err != nil {
		return 0, err
	}

	return nextSeq, nil
}

func (s *AnansiStore) All(ctx context.Context, runID string, rerunIndex int) ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, _, err := s.loadEntriesFor(ctx, runID, rerunIndex)
	return entries, err
}

func (s *AnansiStore) ByStepID(ctx context.Context, runID string, rerunIndex int, stepID string) ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries, _, err := s.loadEntriesFor(ctx, runID, rerunIndex)
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

	entries, _, err := s.loadEntriesFor(ctx, runID, rerunIndex)
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

	entries, _, err := s.loadEntriesFor(ctx, runID, rerunIndex)
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

// loadEntries reads the scoped document's entries and its document ID
// (empty when the document does not exist yet).
func (s *AnansiStore) loadEntries(ctx context.Context) ([]Entry, string, error) {
	return s.loadEntriesFor(ctx, s.runID, s.rerunIndex)
}

func (s *AnansiStore) loadEntriesFor(ctx context.Context, runID string, rerunIndex int) ([]Entry, string, error) {
	docs, err := s.readByRunID(ctx, runID)
	if err != nil {
		return nil, "", err
	}
	for _, d := range docs {
		if d.RerunIndex != rerunIndex {
			continue
		}
		if d.Entries == "" {
			return nil, d.GetID(), nil
		}
		var entries []Entry
		if err := json.Unmarshal([]byte(d.Entries), &entries); err != nil {
			return nil, "", err
		}
		return entries, d.GetID(), nil
	}
	return nil, "", nil
}

// readByRunID returns all log documents for a run across rerun indices.
func (s *AnansiStore) readByRunID(ctx context.Context, runID string) ([]*LogEntry, error) {
	q := query.NewQueryBuilder().Where("runId").Eq(runID).Build()
	return s.models.Read(ctx, &q)
}

// saveEntries writes the entries array immediately. Updates the existing
// document when present; creates it on first write.
func (s *AnansiStore) saveEntries(ctx context.Context, docID string, data string) error {
	if docID != "" {
		_, err := s.models.Update(ctx, docID, &LogEntry{
			RunID:      s.runID,
			RerunIndex: s.rerunIndex,
			Entries:    data,
		})
		return err
	}

	doc := document.New(&LogEntry{
		RunID:      s.runID,
		RerunIndex: s.rerunIndex,
		Entries:    data,
	})
	_, err := s.models.Create(ctx, doc)
	return err
}
