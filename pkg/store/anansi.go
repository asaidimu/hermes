package store

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/asaidimu/go-anansi/v8/core/data"
	"github.com/asaidimu/go-anansi/v8/core/persistence/base"
	anansicollection "github.com/asaidimu/go-anansi/v8/core/persistence/collection"
	"github.com/asaidimu/go-anansi/v8/core/schema/definition"
	"go.uber.org/zap"
)

// AnansiStoreFactory provisions and exposes run state documents backed by an
// anansi ModelCollection[*PipelineState]. The runs collection schema is derived
// from the PipelineState struct (see AGENTS.md); it is created on first use
// when absent.
//
// Create mints a new run document in memory (identity only, no DB write);
// Load recovers an existing run document by its identifier.
type AnansiStoreFactory struct {
	models *anansicollection.ModelCollection[*PipelineState]
}

// ensureDocumentFactory configures anansi's singleton document factory. Any
// persistence operation that mints documents panics without it
// (ERR_DATA_FACTORY_NOT_CONFIGURED). Idempotent.
var ensureDocumentFactory sync.Once

// NewAnansiStoreFactory creates a factory bound to the named runs collection,
// deriving its schema from PipelineState and creating the collection when it
// does not exist yet.
func NewAnansiStoreFactory(ctx context.Context, persist base.Persistence, collectionName string) (*AnansiStoreFactory, error) {
	ensureDocumentFactory.Do(func() {
		_ = data.ConfigureDocumentFactory(data.DocumentFactoryConfig{}, nil)
	})

	schemaBytes, err := data.ExtractDTOSchemaDirect(&PipelineState{})
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

	models, err := anansicollection.NewModelCollection[*PipelineState](coll, zap.NewNop())
	if err != nil {
		return nil, err
	}

	return &AnansiStoreFactory{models: models}, nil
}

// Models exposes the typed model collection for advanced queries (run
// listings, ad-hoc analytics).
func (f *AnansiStoreFactory) Models() *anansicollection.ModelCollection[*PipelineState] {
	return f.models
}

// Create returns a brand-new PersistentStore with a freshly minted run
// identity. No document exists in the collection until the store's first
// write-through.
func (f *AnansiStoreFactory) Create(_ context.Context) (Store, error) {
	return NewPersistentStore(f.models, nil), nil
}

// Load returns the PersistentStore for an existing run, recovering its state
// document from the collection. Returns a NotFound error when runID has no
// state document.
func (f *AnansiStoreFactory) Load(ctx context.Context, runID string) (Store, error) {
	return NewPersistentStoreForID(ctx, f.models, runID)
}

// Mint adapts the factory to WorkflowRuntime's Options.StoreFactory.
func (f *AnansiStoreFactory) Mint() func() (Store, error) {
	return func() (Store, error) { return f.Create(context.Background()) }
}

// Loader adapts the factory to WorkflowRuntime's Options.StoreLoader.
func (f *AnansiStoreFactory) Loader() func(runID string) (Store, error) {
	return func(runID string) (Store, error) { return f.Load(context.Background(), runID) }
}
