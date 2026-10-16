package actionlog

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"hash/fnv"
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

// FoldSnapshotDoc is the persisted representation of ONE fold snapshot
// (v2.1 H2.1): one document per run attempt, keyed deterministically by
// (runId, rerunIndex) — the latest pause overwrites via Replace.
type FoldSnapshotDoc struct {
	document.DocumentModel
	RunID      string `anansi:"runId"`
	RerunIndex int    `anansi:"rerunIndex"`
	Data       string `anansi:"data"` // full FoldSnapshot JSON
}

// LogEntry is the persisted representation of ONE action log entry.
// One document per entry, keyed by (runId, rerunIndex, seq) — the
// append is a single INSERT (O(1) tail write), and All is an ordered
// range scan over the run's rows.
//
// 20261003 refactor (plan P4.1): the previous layout stored a whole
// run attempt's log as ONE JSON array inside a single document, so
// every append re-read, re-marshaled, and re-wrote the full array —
// two DB round trips per append, O(entries²) bytes per run, behind one
// store-wide mutex. The per-entry layout is the only layout (no
// production data existed to migrate; plan Trap #3).
//
// The document ID is a deterministic function of the entry's own
// identity (entryDocID) — the runID is data inside the document, never
// baked into the Store instance's scope.
type LogEntry struct {
	document.DocumentModel
	RunID      string `anansi:"runId"`
	RerunIndex int    `anansi:"rerunIndex"`
	Seq        uint64 `anansi:"seq"`
	Data       string `anansi:"data"` // full Entry JSON
}

// ensureDocumentFactory configures anansi's singleton document factory.
var ensureDocumentFactory sync.Once

// NewAnansiStore creates a durable action log Store backed by the named
// anansi collection, deriving its schema from LogEntry and creating the
// collection when it does not exist yet. One document per entry
// (runId, rerunIndex, seq).
//
// 20261003 refactor (plan D.3): the former AnansiStoreFactory — a struct
// holding one ModelCollection field with a single method — is collapsed
// into this constructor.
func NewAnansiStore(ctx context.Context, persist base.Persistence, collectionName string) (*AnansiStore, error) {
	ensureDocumentFactory.Do(func() {
		_ = data.ConfigureDocumentFactory(data.DocumentFactoryConfig{}, nil)
	})

	models, err := ensureCollection[*LogEntry](ctx, persist, collectionName, &LogEntry{})
	if err != nil {
		return nil, err
	}

	// v2.1 H2.1: the fold-snapshot side collection, created alongside the
	// entries. Separate collection — snapshots must never appear in
	// All/Count/Runs (they are a derived cache, invisible to the fold).
	snapModels, err := ensureCollection[*FoldSnapshotDoc](ctx, persist, collectionName+"_fold_snapshots", &FoldSnapshotDoc{})
	if err != nil {
		return nil, err
	}

	return &AnansiStore{models: models, snapshots: snapModels}, nil
}

// ensureCollection extracts the DTO schema, creates the collection when
// missing, and returns its typed model collection. Shared by the entries
// collection and the fold-snapshot side collection (H2.1).
func ensureCollection[P anansicollection.ModelIdentity](ctx context.Context, persist base.Persistence, name string, dto P) (*anansicollection.ModelCollection[P], error) {
	schemaBytes, err := data.ExtractDTOSchemaDirect(dto)
	if err != nil {
		return nil, err
	}
	var sc definition.Schema
	if err := json.Unmarshal(schemaBytes, &sc); err != nil {
		return nil, err
	}
	sc.Name = name

	ok, err := persist.HasCollection(ctx, name)
	if err != nil {
		return nil, err
	}
	if !ok {
		if _, err := persist.CreateCollection(ctx, &sc); err != nil {
			return nil, err
		}
	}

	coll, err := persist.Collection(ctx, name)
	if err != nil {
		return nil, err
	}
	return anansicollection.NewModelCollection[P](coll, zap.NewNop())
}

// actionLogNamespace namespaces the deterministic log document IDs so they
// are stable across processes and cannot collide with other hash uses.
var actionLogNamespace = uuid.NewSHA1(uuid.NameSpaceURL, []byte("github.com/asaidimu/hermes/pkg/actionlog"))

// uuid7Layout forces a 16-byte digest into anansi's required UUIDv7
// shape: version nibble 0b0111, RFC 4122 variant, 32-hex text. The
// 48-bit timestamp field carries hash bits — not a creation time —
// precisely because the ID must not depend on when it was minted.
func uuid7Layout(sum [sha1.Size]byte) string {
	var id uuid.UUID
	copy(id[:], sum[:16])
	id[6] = (id[6] & 0x0f) | 0x70
	id[8] = (id[8] & 0x3f) | 0x80
	return strings.ReplaceAll(id.String(), "-", "")
}

// snapshotDocID derives the deterministic document ID for one fold
// snapshot. One per (runID, rerunIndex): the latest pause replaces it.
func snapshotDocID(runID string, rerunIndex int) string {
	name := actionLogNamespace.String() + ":snapshot:" + runID + ":" + fmt.Sprint(rerunIndex)
	return uuid7Layout(sha1.Sum([]byte(name)))
}

// SaveFoldSnapshot upserts the attempt's snapshot (H2.1). Best-effort by
// contract — the runtime logs save failures and the walk covers the miss —
// but errors are still returned honestly for tests and loud hosts.
func (s *AnansiStore) SaveFoldSnapshot(ctx context.Context, snap FoldSnapshot) error {
	if snap.RunID == "" {
		return fmt.Errorf("fold snapshot rejected: RunID is empty")
	}
	dataBytes, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	id := snapshotDocID(snap.RunID, snap.RerunIndex)
	doc := &FoldSnapshotDoc{
		RunID:      snap.RunID,
		RerunIndex: snap.RerunIndex,
		Data:       string(dataBytes),
	}

	// Upsert via delete-then-create on the deterministic ID. (FindByID
	// panics on missing records in the current anansi version — the read
	// path below avoids it too.) writeMu serializes the pair so two
	// concurrent saves cannot interleave delete/create for one attempt.
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_ = s.snapshots.DeleteByID(ctx, id) // not-found is fine
	model := document.New(doc)
	model.ID = id
	_, err = s.snapshots.Create(ctx, model)
	return err
}

// LoadFoldSnapshot returns the attempt's snapshot, or (nil, nil) when none
// is persisted — absent is a normal state (fresh attempt, snapshots
// disabled, pruned), never an error.
func (s *AnansiStore) LoadFoldSnapshot(ctx context.Context, runID string, rerunIndex int) (*FoldSnapshot, error) {
	id := snapshotDocID(runID, rerunIndex)
	q := query.NewQueryBuilder().
		Where(data.DocumentIDField).Eq(id).
		Limit(1).
		Build()
	docs, err := s.snapshots.Read(ctx, &q)
	if err != nil {
		return nil, err
	}
	if len(docs) == 0 {
		return nil, nil // absent: normal
	}
	var snap FoldSnapshot
	if err := json.Unmarshal([]byte(docs[0].Data), &snap); err != nil {
		return nil, err // corrupt: reported, the caller falls back to the walk
	}
	return &snap, nil
}

// entryDocID derives the deterministic document ID for one log entry.
// Document identity is a pure function of the identity carried by the
// entry — never of any store-level scope: the same
// (runID, rerunIndex, seq) maps to the same document in every process
// and after every restart.
func entryDocID(runID string, rerunIndex int, seq uint64) string {
	name := actionLogNamespace.String() + ":entry:" + runID + ":" + fmt.Sprint(rerunIndex) + ":" + fmt.Sprint(seq)
	return uuid7Layout(sha1.Sum([]byte(name)))
}

// runKey identifies one run attempt inside the store's sharded state.
type runKey struct {
	runID      string
	rerunIndex int
}

// seqShard holds the lazily-initialized next-Seq counters for a stripe
// of run keys.
type seqShard struct {
	mu    sync.Mutex
	nexts map[runKey]uint64
}

// seqShardCount is the lock-striping width for seq assignment. Stripes
// are addressed by FNV-1a of the runID; 256 stripes keep unrelated
// runs off the same mutex with high probability while bounding memory.
const seqShardCount = 256

// AnansiStore is a durable multi-run Store implementation backed by an anansi
// ModelCollection. It holds no run scope of its own: every operation takes the
// (runID, rerunIndex) it operates on, and Append routes each entry to a
// document derived from the entry's own RunID/RerunIndex/Seq. Every Append
// persists immediately — a single INSERT, no batching, no array rewrite —
// so at most one entry is lost on crash.
//
// Seq assignment is guarded by per-shard mutexes (plan P4.1 lock
// sharding): appends for unrelated runs no longer serialize behind one
// store-wide mutex. Each (runID, rerunIndex) counter is initialized
// lazily from the stored maximum on first touch, so numbering survives
// process restarts; the persisted Seq field is authoritative on read.
type AnansiStore struct {
	models *anansicollection.ModelCollection[*LogEntry]
	// snapshots (v2.1 H2.1): the FoldSnapshotStore side collection.
	snapshots *anansicollection.ModelCollection[*FoldSnapshotDoc]
	shards    [seqShardCount]seqShard

	// writeMu serializes the DB write itself. The INSERT is the only
	// critical section — the expensive work (seq assignment, marshal)
	// stays shard-local — and single-writer backends (sqlite) reject
	// concurrent writes with transient lock errors, which would otherwise
	// punch seq gaps into a run's log. One mutex around a cheap INSERT is
	// the honest constraint; the O(n) array rewrite this replaced
	// serialized far more.
	writeMu sync.Mutex
}

var _ Store = (*AnansiStore)(nil)
var _ RunEnumerator = (*AnansiStore)(nil)
var _ FoldSnapshotStore = (*AnansiStore)(nil)

// shardFor returns the seq-assignment shard for a run.
func (s *AnansiStore) shardFor(runID string) *seqShard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(runID))
	return &s.shards[h.Sum32()%seqShardCount]
}

// nextSeqLocked assigns the next Seq for a run attempt, initializing the
// in-memory counter from the stored maximum on first touch. Callers hold
// the shard mutex.
func (s *AnansiStore) nextSeqLocked(ctx context.Context, key runKey) (uint64, error) {
	shard := s.shardFor(key.runID)
	if shard.nexts == nil {
		shard.nexts = make(map[runKey]uint64)
	}
	if next, ok := shard.nexts[key]; ok {
		return next, nil
	}

	// First touch this process: derive from the stored maximum so
	// numbering is stable across restarts. Gaps are fine (they already
	// were — see the historical note on failed marshals).
	max, err := s.maxSeq(ctx, key.runID, key.rerunIndex)
	if err != nil {
		return 0, err
	}
	next := max + 1
	shard.nexts[key] = next
	return next, nil
}

// maxSeq queries the highest persisted Seq for a run attempt.
func (s *AnansiStore) maxSeq(ctx context.Context, runID string, rerunIndex int) (uint64, error) {
	q := query.NewQueryBuilder().
		Where("runId").Eq(runID).
		Where("rerunIndex").Eq(rerunIndex).
		OrderByDesc("seq").
		Limit(1).
		Build()
	docs, err := s.models.Read(ctx, &q)
	if err != nil {
		return 0, err
	}
	if len(docs) == 0 {
		return 0, nil
	}
	return docs[0].Seq, nil
}

func (s *AnansiStore) Append(ctx context.Context, entry Entry) (uint64, error) {
	// Identity must come from the entry, not the store: an entry without
	// a RunID has nowhere to land. Fail loudly instead of silently
	// merging it into a foreign run's log (see the historical note below
	// on the review that fixed this contract).
	if entry.RunID == "" {
		return 0, fmt.Errorf("action log entry rejected: RunID is empty — set RunID (and RerunIndex) on the entry before Append")
	}
	if entry.RerunIndex < 0 {
		return 0, fmt.Errorf("action log entry rejected: RerunIndex %d is negative for run %s", entry.RerunIndex, entry.RunID)
	}

	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now().UTC()
	}

	// Seq assignment under the run's shard lock. The DB write happens
	// OUTSIDE the shard mutex (no I/O inside critical sections) — a
	// failed write leaves a seq gap, which is harmless because readers
	// order by the persisted Seq, not by row order.
	key := runKey{runID: entry.RunID, rerunIndex: entry.RerunIndex}
	shard := s.shardFor(entry.RunID)
	shard.mu.Lock()
	seq, err := s.nextSeqLocked(ctx, key)
	if err != nil {
		shard.mu.Unlock()
		return 0, err
	}
	shard.nexts[key] = seq + 1
	shard.mu.Unlock()

	entry.Seq = seq
	dataBytes, err := json.Marshal(entry)
	if err != nil {
		return 0, err
	}

	model := document.New(&LogEntry{
		RunID:      entry.RunID,
		RerunIndex: entry.RerunIndex,
		Seq:        entry.Seq,
		Data:       string(dataBytes),
	})
	model.ID = entryDocID(entry.RunID, entry.RerunIndex, entry.Seq)
	s.writeMu.Lock()
	_, err = s.models.Create(ctx, model)
	s.writeMu.Unlock()
	if err != nil {
		return 0, err
	}

	return entry.Seq, nil
}

// Historical note (kept for the record, #review-20260910-014): the store
// used to be scoped to a single (runID, rerunIndex) document and
// overwrote entry.RunID/RerunIndex with its own scope, silently merging
// every run's log into one foreign runID. runID is now detached from
// store identity entirely — Append routes by entry identity and rejects
// entries without a RunID. The per-Append O(n) array rewrite that note
// also flagged is gone with the per-entry row layout (plan P4.1).

func (s *AnansiStore) All(ctx context.Context, runID string, rerunIndex int) ([]Entry, error) {
	q := query.NewQueryBuilder().
		Where("runId").Eq(runID).
		Where("rerunIndex").Eq(rerunIndex).
		OrderByAsc("seq").
		Build()
	docs, err := s.models.Read(ctx, &q)
	if err != nil {
		return nil, err
	}
	entries := make([]Entry, 0, len(docs))
	for _, d := range docs {
		var e Entry
		if err := json.Unmarshal([]byte(d.Data), &e); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

func (s *AnansiStore) ByStepID(ctx context.Context, runID string, rerunIndex int, stepID string) ([]Entry, error) {
	entries, err := s.All(ctx, runID, rerunIndex)
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
	entries, err := s.All(ctx, runID, rerunIndex)
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
	entries, err := s.All(ctx, runID, rerunIndex)
	if err != nil {
		return 0, err
	}
	return uint64(len(entries)), nil
}

func (s *AnansiStore) LatestRerunIndex(ctx context.Context, runID string) (int, error) {
	q := query.NewQueryBuilder().Where("runId").Eq(runID).Build()
	docs, err := s.models.Read(ctx, &q)
	if err != nil {
		return -1, err
	}
	max := -1
	seen := make(map[int]bool)
	for _, d := range docs {
		if !seen[d.RerunIndex] {
			seen[d.RerunIndex] = true
			if d.RerunIndex > max {
				max = d.RerunIndex
			}
		}
	}
	return max, nil
}

// Runs enumerates every run attempt persisted in the collection
// (RunEnumerator capability). The startup rebuild (plan S.3) uses this
// to find runs whose recovery must be completed after a restart; it is
// a cold-path scan, not a hot-path read.
func (s *AnansiStore) Runs(ctx context.Context) ([]RunRef, error) {
	docs, err := s.models.Read(ctx, &query.Query{})
	if err != nil {
		return nil, err
	}
	seen := make(map[runKey]bool)
	var refs []RunRef
	for _, d := range docs {
		key := runKey{runID: d.RunID, rerunIndex: d.RerunIndex}
		if !seen[key] {
			seen[key] = true
			refs = append(refs, RunRef{RunID: d.RunID, RerunIndex: d.RerunIndex})
		}
	}
	return refs, nil
}
