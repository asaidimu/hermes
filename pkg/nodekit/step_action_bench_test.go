package nodekit_test

import (
        "context"
        "encoding/json"
        "testing"

        "github.com/asaidimu/hermes/pkg/events"
        "github.com/asaidimu/hermes/pkg/nodekit"
        "github.com/asaidimu/hermes/pkg/pipeline"
        "github.com/asaidimu/hermes/pkg/store"
        "go.uber.org/zap"
)

// benchConfig is the DTO the bench schema derives from (same mechanism
// the real nodes use — ExtractDTOSchemaDirectWithTag).
type benchConfig struct {
        URL     string `config:"url"`
        Retries int    `config:"retries"`
}

// BenchmarkBuildStepAction measures the per-attempt cost of a nodekit
// step action: config schema compile + interpolation + coercion (plan
// P4.3 — this work moves to build time for static configs).
func benchStepAction(b *testing.B, config map[string]any) {
        b.Helper()

        schema, err := json.Marshal(benchConfig{})
        if err != nil {
                b.Fatal(err)
        }
        def := nodekit.NodeDefinition{
                Kind:         "bench-node",
                Label:        "Bench Node",
                ConfigSchema: schema,
                Effect:       nodekit.EffectPure,
                Run: func(ctx context.Context, nCtx nodekit.NodeRunContext) (store.Mutator, error) {
                        return store.SetValue("out", nCtx.Config["url"]), nil
                },
        }
        step := nodekit.BuildStep("bench-node-1", def, config, nil)
        pcxt := pipeline.NewPipelineContext("bench-run", "bench-pipeline", "stage-1", "bench-node-1",
                events.EventPath{}, zap.NewNop().Sugar())
        state := map[string]any{"endpoint": "https://live.example/api"}

        for b.Loop() {
                if _, err := step.Action(context.Background(), pcxt, state); err != nil {
                        b.Fatal(err)
                }
        }
}

// BenchmarkStepActionStaticConfig: the config carries no {{ }} tokens —
// the whole prepare pipeline (schema compile + interpolate + coerce) is
// build-time work post-P4.3.
func BenchmarkStepActionStaticConfig(b *testing.B) {
        benchStepAction(b, map[string]any{
                "url":     "https://example.internal/api",
                "retries": float64(3),
        })
}

// BenchmarkStepActionDynamicConfig: the config interpolates against live
// state — per-attempt interpolation remains by design.
func BenchmarkStepActionDynamicConfig(b *testing.B) {
        benchStepAction(b, map[string]any{
                "url":     "{{ $.endpoint }}",
                "retries": float64(3),
        })
}
