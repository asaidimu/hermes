package store

import (
	"context"

	"github.com/asaidimu/go-anansi/v8/core/persistence/base"
)

// AnansiStoreFactory creates PersistentStore instances backed by an anansi
// collection. The collection is opened once and reused across stores.
type AnansiStoreFactory struct {
	persist base.Persistence
	coll    base.Collection
}

// NewAnansiStoreFactory creates a factory that persists stores to the named collection.
func NewAnansiStoreFactory(ctx context.Context, persist base.Persistence, collectionName string) (*AnansiStoreFactory, error) {
	coll, err := persist.Collection(ctx, collectionName)
	if err != nil {
		return nil, err
	}
	return &AnansiStoreFactory{
		persist: persist,
		coll:    coll,
	}, nil
}

// Create returns a PersistentStore for the given runID, loading existing
// state from the collection if present.
func (f *AnansiStoreFactory) Create(ctx context.Context, runID string) (Store, error) {
	return NewPersistentStore(ctx, f.coll, runID)
}

// AsStoreFactory returns a factory function compatible with WorkflowRuntime.
func (f *AnansiStoreFactory) AsStoreFactory() func(runID string) Store {
	return func(runID string) Store {
		s, err := f.Create(context.Background(), runID)
		if err != nil {
			return NewMemoryStore(nil)
		}
		return s
	}
}
