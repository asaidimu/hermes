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

// ensureDocumentFactory configures anansi's singleton document factory. Any
// persistence operation that mints documents panics without it
// (ERR_DATA_FACTORY_NOT_CONFIGURED). Idempotent.
var ensureDocumentFactory sync.Once

// NewAnansiStateCollection provisions the typed anansi model collection for
// run state documents, deriving its schema from PipelineState and creating
// the collection when it does not exist yet. Callers build stores directly
// on the returned collection: store.NewPersistentStore(models, nil) mints a
// fresh run; store.NewPersistentStoreForID(ctx, models, runID) recovers an
// existing one.
//
// 20261003 refactor (plan D.3): the former AnansiStoreFactory — a struct
// holding one ModelCollection field whose methods wrapped the two store
// constructors — is collapsed into this provisioning function.
func NewAnansiStateCollection(ctx context.Context, persist base.Persistence, collectionName string) (*anansicollection.ModelCollection[*PipelineState], error) {
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

        return models, nil
}
