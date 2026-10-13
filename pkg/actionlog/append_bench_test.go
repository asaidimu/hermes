package actionlog

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkAnansiAppend measures the hot-path cost of appending to a
// growing run log (plan P4.1). The pre-change store rewrote the WHOLE
// entries array per append — O(entries²) bytes per run; the per-entry
// row layout must keep ns/op flat as the log grows.
//
// Run as: go test ./pkg/actionlog/ -bench BenchmarkAnansiAppend -count N
func benchAnansiAppend(b *testing.B, entries int) {
	b.Helper()
	ctx := context.Background()
	logStore, cleanup := newLogFactoryB(b)
	defer cleanup()

	runID := fmt.Sprintf("bench-run-%d", entries)

	// Grow the log to `entries` entries first so every measured append
	// pays the full storage cost of a log that size.
	for i := 0; i < entries; i++ {
		_, err := logStore.Append(ctx, Entry{
			RunID:      runID,
			RerunIndex: 0,
			Kind:       KindStepCompleted,
			StepID:     fmt.Sprintf("step-%d", i),
			Payload:    []byte(`{"k":"v"}`),
		})
		require.NoError(b, err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := logStore.Append(ctx, Entry{
			RunID:      runID,
			RerunIndex: 0,
			Kind:       KindStepCompleted,
			StepID:     fmt.Sprintf("bench-%d", i),
			Payload:    []byte(`{"k":"v"}`),
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAnansiAppend_Log10(b *testing.B)  { benchAnansiAppend(b, 10) }
func BenchmarkAnansiAppend_Log100(b *testing.B) { benchAnansiAppend(b, 100) }
func BenchmarkAnansiAppend_Log1000(b *testing.B) {
	benchAnansiAppend(b, 1000)
}

// newLogFactoryB is the bench variant of newLogFactory (no *testing.T).
func newLogFactoryB(b *testing.B) (*AnansiStore, func()) {
	b.Helper()
	logStore, cleanup, err := newLogFactoryErr(b.Name())
	if err != nil {
		b.Fatal(err)
	}
	return logStore, cleanup
}
