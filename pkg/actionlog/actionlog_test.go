package actionlog

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func TestEntryKinds(t *testing.T) {
	cases := []struct {
		kind EntryKind
		want string
	}{
		{KindPipelineStarted, "pipeline_started"},
		{KindPipelineCompleted, "pipeline_completed"},
		{KindPipelineFailed, "pipeline_failed"},
		{KindStageStarted, "stage_started"},
		{KindStageCompleted, "stage_completed"},
		{KindStageFailed, "stage_failed"},
		{KindStepStarted, "step_started"},
		{KindStepCompleted, "step_completed"},
		{KindStepFailed, "step_failed"},
		{KindStepRetry, "step_retry"},
		{KindEffectCompleted, "effect_completed"},
		{KindEffectFailed, "effect_failed"},
		{KindRouted, "routed"},
		{KindCheckpoint, "checkpoint"},
		{KindResumed, "resumed"},
		{KindSubpipelineForked, "subpipeline_forked"},
		{KindSubpipelineJoined, "subpipeline_joined"},
	}
	for _, c := range cases {
		if got := string(c.kind); got != c.want {
			t.Errorf("EntryKind(%q) = %q, want %q", c.kind, got, c.want)
		}
	}
}

func TestMemoryActionLogAppendAndAll(t *testing.T) {
	log := NewMemoryActionLog()
	ctx := context.Background()

	seq1, err := log.Append(ctx, Entry{
		RunID:   "run-1",
		Kind:    KindEffectCompleted,
		StageID: "stage-a",
		StepID:  "step-1",
		Output:  json.RawMessage(`{"status":200}`),
	})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if seq1 != 1 {
		t.Fatalf("first Seq = %d, want 1", seq1)
	}

	seq2, err := log.Append(ctx, Entry{
		RunID:   "run-1",
		Kind:    KindRouted,
		StageID: "stage-a",
		Handle:  "stage-b",
	})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if seq2 != 2 {
		t.Fatalf("second Seq = %d, want 2", seq2)
	}

	entries, err := log.All(ctx, "run-1", 0)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("All returned %d entries, want 2", len(entries))
	}
	if entries[0].Kind != KindEffectCompleted {
		t.Errorf("entry[0].Kind = %q, want %q", entries[0].Kind, KindEffectCompleted)
	}
	if entries[1].Kind != KindRouted {
		t.Errorf("entry[1].Kind = %q, want %q", entries[1].Kind, KindRouted)
	}
}

func TestMemoryActionLogAllEmpty(t *testing.T) {
	log := NewMemoryActionLog()
	entries, err := log.All(context.Background(), "nonexistent", 0)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	if entries != nil {
		t.Fatalf("All returned %v, want nil", entries)
	}
}

func TestMemoryActionLogByStepID(t *testing.T) {
	log := NewMemoryActionLog()
	ctx := context.Background()

	log.Append(ctx, Entry{RunID: "r1", Kind: KindEffectCompleted, StepID: "s1", StageID: "a"})
	log.Append(ctx, Entry{RunID: "r1", Kind: KindRouted, StepID: "", StageID: "a"})
	log.Append(ctx, Entry{RunID: "r1", Kind: KindEffectFailed, StepID: "s2", StageID: "b"})
	log.Append(ctx, Entry{RunID: "r1", Kind: KindEffectCompleted, StepID: "s1", StageID: "a"})

	entries, err := log.ByStepID(ctx, "r1", 0, "s1")
	if err != nil {
		t.Fatalf("ByStepID: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("ByStepID(s1) returned %d entries, want 2", len(entries))
	}
	for _, e := range entries {
		if e.StepID != "s1" {
			t.Errorf("unexpected StepID %q", e.StepID)
		}
	}
}

func TestMemoryActionLogLastByStageID(t *testing.T) {
	log := NewMemoryActionLog()
	ctx := context.Background()

	log.Append(ctx, Entry{RunID: "r1", Kind: KindRouted, StageID: "a", Handle: "b"})
	log.Append(ctx, Entry{RunID: "r1", Kind: KindEffectCompleted, StageID: "a", StepID: "s1"})
	log.Append(ctx, Entry{RunID: "r1", Kind: KindRouted, StageID: "a", Handle: "c"})

	last, err := log.LastByStageID(ctx, "r1", 0, "a")
	if err != nil {
		t.Fatalf("LastByStageID: %v", err)
	}
	if last == nil {
		t.Fatal("LastByStageID returned nil")
	}
	if last.Handle != "c" {
		t.Errorf("Handle = %q, want %q", last.Handle, "c")
	}
}

func TestMemoryActionLogLastByStageIDNotFound(t *testing.T) {
	log := NewMemoryActionLog()
	last, err := log.LastByStageID(context.Background(), "nonexistent", 0, "stage")
	if err != nil {
		t.Fatalf("LastByStageID: %v", err)
	}
	if last != nil {
		t.Errorf("expected nil, got %+v", last)
	}
}

func TestMemoryActionLogCount(t *testing.T) {
	log := NewMemoryActionLog()
	ctx := context.Background()

	count, _ := log.Count(ctx, "r1", 0)
	if count != 0 {
		t.Fatalf("Count = %d, want 0", count)
	}

	log.Append(ctx, Entry{RunID: "r1", Kind: KindEffectCompleted, StageID: "a"})
	log.Append(ctx, Entry{RunID: "r1", Kind: KindRouted, StageID: "a"})
	log.Append(ctx, Entry{RunID: "r2", Kind: KindEffectCompleted, StageID: "b"})

	count, _ = log.Count(ctx, "r1", 0)
	if count != 2 {
		t.Errorf("Count(r1) = %d, want 2", count)
	}
	count, _ = log.Count(ctx, "r2", 0)
	if count != 1 {
		t.Errorf("Count(r2) = %d, want 1", count)
	}
}

func TestMemoryActionLogTimestampAutoSet(t *testing.T) {
	log := NewMemoryActionLog()
	before := time.Now().UTC().Add(-time.Millisecond)
	seq, err := log.Append(context.Background(), Entry{RunID: "r1", Kind: KindRouted})
	after := time.Now().UTC().Add(time.Millisecond)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if seq != 1 {
		t.Fatalf("Seq = %d, want 1", seq)
	}
	entries, _ := log.All(context.Background(), "r1", 0)
	if entries[0].Timestamp.Before(before) || entries[0].Timestamp.After(after) {
		t.Errorf("Timestamp %v not auto-set within expected range", entries[0].Timestamp)
	}
}

func TestMemoryActionLogReturnsCopies(t *testing.T) {
	log := NewMemoryActionLog()
	ctx := context.Background()

	log.Append(ctx, Entry{RunID: "r1", Kind: KindEffectCompleted, StageID: "a", Output: json.RawMessage(`{"x":1}`)})
	log.Append(ctx, Entry{RunID: "r1", Kind: KindRouted, StageID: "a", Handle: "b"})

	entries, _ := log.All(ctx, "r1", 0)
	// Mutate the returned slice
	entries[0].StageID = "mutated"
	entries = append(entries, Entry{RunID: "r1", Kind: KindCheckpoint, StageID: "c"})

	// Original should be unchanged
	original, _ := log.All(ctx, "r1", 0)
	if original[0].StageID != "a" {
		t.Errorf("original entry was mutated: StageID = %q", original[0].StageID)
	}
	if len(original) != 2 {
		t.Errorf("original slice grew to %d, want 2", len(original))
	}
}

func TestMemoryActionLogConcurrentAppend(t *testing.T) {
	log := NewMemoryActionLog()
	ctx := context.Background()
	const goroutines = 100
	const perGoroutine = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				_, err := log.Append(ctx, Entry{
					RunID:   "r-concurrent",
					Kind:    KindEffectCompleted,
					StageID: "stage",
					StepID:  "step",
				})
				if err != nil {
					t.Errorf("goroutine %d, iteration %d: Append: %v", gid, i, err)
				}
			}
		}(g)
	}
	wg.Wait()

	count, _ := log.Count(ctx, "r-concurrent", 0)
	expected := uint64(goroutines * perGoroutine)
	if count != expected {
		t.Errorf("Count = %d, want %d", count, expected)
	}

	// Verify Seq ordering
	entries, _ := log.All(ctx, "r-concurrent", 0)
	for i, e := range entries {
		if e.Seq != uint64(i+1) {
			t.Errorf("entry %d has Seq %d, want %d", i, e.Seq, i+1)
		}
	}
}

func TestMemoryActionLogMultipleRuns(t *testing.T) {
	log := NewMemoryActionLog()
	ctx := context.Background()

	log.Append(ctx, Entry{RunID: "r1", Kind: KindEffectCompleted, StageID: "s1"})
	log.Append(ctx, Entry{RunID: "r2", Kind: KindEffectCompleted, StageID: "s2"})
	log.Append(ctx, Entry{RunID: "r1", Kind: KindRouted, StageID: "s1"})

	e1, _ := log.All(ctx, "r1", 0)
	e2, _ := log.All(ctx, "r2", 0)
	if len(e1) != 2 {
		t.Errorf("r1 has %d entries, want 2", len(e1))
	}
	if len(e2) != 1 {
		t.Errorf("r2 has %d entries, want 1", len(e2))
	}
}

func TestNopLog(t *testing.T) {
	nop := NopLog{}
	ctx := context.Background()

	seq, err := nop.Append(ctx, Entry{RunID: "r1", Kind: KindEffectCompleted})
	if err != nil {
		t.Fatalf("NopLog.Append: %v", err)
	}
	if seq != 0 {
		t.Errorf("NopLog.Append seq = %d, want 0", seq)
	}

	entries, err := nop.All(ctx, "r1", 0)
	if err != nil || entries != nil {
		t.Errorf("NopLog.All: entries=%v, err=%v", entries, err)
	}

	byStep, err := nop.ByStepID(ctx, "r1", 0, "s1")
	if err != nil || byStep != nil {
		t.Errorf("NopLog.ByStepID: %v, %v", byStep, err)
	}

	last, err := nop.LastByStageID(ctx, "r1", 0, "a")
	if err != nil || last != nil {
		t.Errorf("NopLog.LastByStageID: %v, %v", last, err)
	}

	count, err := nop.Count(ctx, "r1", 0)
	if err != nil || count != 0 {
		t.Errorf("NopLog.Count: %d, %v", count, err)
	}

	idx, err := nop.LatestRerunIndex(ctx, "r1")
	if err != nil || idx != -1 {
		t.Errorf("NopLog.LatestRerunIndex: %d, %v", idx, err)
	}
}

func TestMemoryActionLogLatestRerunIndex(t *testing.T) {
	log := NewMemoryActionLog()
	ctx := context.Background()

	idx, err := log.LatestRerunIndex(ctx, "nonexistent")
	if err != nil || idx != -1 {
		t.Fatalf("LatestRerunIndex(nonexistent) = %d, %v; want -1, nil", idx, err)
	}

	log.Append(ctx, Entry{RunID: "r1", Kind: KindPipelineStarted})
	idx, _ = log.LatestRerunIndex(ctx, "r1")
	if idx != 0 {
		t.Errorf("LatestRerunIndex(r1) = %d, want 0", idx)
	}
}
