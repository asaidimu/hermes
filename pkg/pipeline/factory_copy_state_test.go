package pipeline

import (
        "context"
        "sync"
        "testing"

        "github.com/asaidimu/hermes/pkg/store"
)

// C4-fix (M5) pin: copyStoreState must never hold two stores' locks at
// once. The old implementation nested dst.Update inside src.Read; two
// concurrent copies in opposite directions could then invert the lock
// order (src.RLock+dst.Lock vs dst.RLock+src.Lock) and deadlock. These
// tests hammer both directions simultaneously — under -race with the
// deadlock the test suite hangs; with the fix it terminates cleanly.
func TestCopyStoreStateConcurrentOppositeDirections(t *testing.T) {
        const rounds = 200
        a, b := store.NewMemoryStore(nil), store.NewMemoryStore(nil)
        if err := a.Update(context.Background(), store.SetValue("side", "a")); err != nil {
                t.Fatal(err)
        }
        if err := b.Update(context.Background(), store.SetValue("side", "b")); err != nil {
                t.Fatal(err)
        }

        var wg sync.WaitGroup
        for i := 0; i < rounds; i++ {
                wg.Add(2)
                go func() { defer wg.Done(); _ = copyStoreState(a, b) }()
                go func() { defer wg.Done(); _ = copyStoreState(b, a) }()
        }

        done := make(chan struct{})
        go func() { wg.Wait(); close(done) }()
        select {
        case <-done:
        case <-t.Context().Done():
                t.Fatal("copyStoreState deadlocked under concurrent opposite-direction copies (lock inversion)")
        }

        var sideA, sideB any
        _ = a.Read(func(s map[string]any) error { sideA = s["side"]; return nil })
        _ = b.Read(func(s map[string]any) error { sideB = s["side"]; return nil })
        if sideA != sideB {
                t.Fatalf("stores did not converge after bidirectional copies: %v vs %v", sideA, sideB)
        }
}

// Copy must be value-preserving: every top-level key of src lands in dst.
func TestCopyStoreStatePreservesKeys(t *testing.T) {
        src, dst := store.NewMemoryStore(nil), store.NewMemoryStore(nil)
        if err := src.Update(context.Background(), func(s map[string]any) error {
                s["k1"] = "v1"
                s["k2"] = map[string]any{"nested": true}
                return nil
        }); err != nil {
                t.Fatal(err)
        }
        if err := copyStoreState(src, dst); err != nil {
                t.Fatal(err)
        }
        _ = dst.Read(func(s map[string]any) error {
                if s["k1"] != "v1" {
                        t.Fatalf("k1 = %v, want v1", s["k1"])
                }
                if m, ok := s["k2"].(map[string]any); !ok || m["nested"] != true {
                        t.Fatalf("k2 = %#v, want nested map", s["k2"])
                }
                return nil
        })
}

// ExtractCheckpointRaw under a store read lock must see the same raw
// value ParseCheckpoint decodes — the C4-fix contract that keeps the
// JSON round trip outside the lock while preserving ReadCheckpoint
// semantics.
func TestExtractThenParseMatchesReadCheckpoint(t *testing.T) {
        ckpt := PipelineCheckpoint{
                RunID:      "run-1",
                PipelineID: "wf-1",
                PausedOn:   "event",
                ResumeAt:   EntryAddress{Stage: "s1", Step: "step-2"},
        }
        state := map[string]any{}
        if err := WriteCheckpoint(state, ckpt); err != nil {
                t.Fatal(err)
        }
        st := store.NewMemoryStore(state)

        var want *PipelineCheckpoint
        if err := st.Read(func(s map[string]any) error {
                var err error
                want, err = ReadCheckpoint(s, "wf-1")
                return err
        }); err != nil {
                t.Fatal(err)
        }

        var got *PipelineCheckpoint
        if err := st.Read(func(s map[string]any) error {
                raw := ExtractCheckpointRaw(s, "wf-1")
                var err error
                got, err = ParseCheckpoint(raw) // parse outside the store, per C4-fix
                return err
        }); err != nil {
                t.Fatal(err)
        }

        if want == nil || got == nil {
                t.Fatalf("both paths must find the checkpoint, got want=%v got=%v", want, got)
        }
        if got.ResumeAt != want.ResumeAt {
                t.Fatalf("ResumeAt mismatch: extract/parse = %+v, ReadCheckpoint = %+v", got.ResumeAt, want.ResumeAt)
        }
        if got.RunID != "run-1" {
                t.Fatalf("RunID = %q, want run-1", got.RunID)
        }
}
