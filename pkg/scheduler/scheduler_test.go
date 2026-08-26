package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestInMemorySchedulerSchedule(t *testing.T) {
	s := New()
	defer s.Shutdown(context.Background())

	var mu sync.Mutex
	fired := false

	err := s.Schedule("test1", "@every 100ms", func(ctx context.Context) {
		mu.Lock()
		fired = true
		mu.Unlock()
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if !fired {
		t.Error("expected callback to fire")
	}
}

func TestInMemorySchedulerCancel(t *testing.T) {
	s := New()
	defer s.Shutdown(context.Background())

	var mu sync.Mutex
	fired := false

	err := s.Schedule("test2", "@every 100ms", func(ctx context.Context) {
		mu.Lock()
		fired = true
		mu.Unlock()
	})
	if err != nil {
		t.Fatal(err)
	}

	// Cancel before it fires
	err = s.Cancel("test2")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if fired {
		t.Error("expected callback not to fire after cancel")
	}
}

func TestInMemorySchedulerShutdown(t *testing.T) {
	s := New()

	var mu sync.Mutex
	fired := false

	err := s.Schedule("test3", "@every 100ms", func(ctx context.Context) {
		mu.Lock()
		fired = true
		mu.Unlock()
	})
	if err != nil {
		t.Fatal(err)
	}

	// Shutdown before it fires
	err = s.Shutdown(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if fired {
		t.Error("expected callback not to fire after shutdown")
	}
}

// @note #review-20260826-009 issue status=open priority=P2 tags=#review,#testing,#flaky : TestInMemorySchedulerReplace asserts exact tick counts against wall-clock sleeps
// @author ox-alpha
//
// The test sleeps 200ms and requires callCount to be EXACTLY 10 (one firing
// of a 100ms cron). Under CI load the timer can straddle the window and fire
// twice (observed: got 20), or the replaced job's already-armed timer can
// sneak one firing in. This makes `-race`/CI runs red non-deterministically.
// Fix directions: inject a fake clock into InMemoryScheduler, or assert
// callCount is a positive multiple of 10, or widen the window and use
// tolerance. Until then this blocks clean -race gate runs.
func TestInMemorySchedulerReplace(t *testing.T) {
	s := New()
	defer s.Shutdown(context.Background())

	var mu sync.Mutex
	callCount := 0

	// Schedule first job
	err := s.Schedule("test4", "@every 100ms", func(ctx context.Context) {
		mu.Lock()
		callCount++
		mu.Unlock()
	})
	if err != nil {
		t.Fatal(err)
	}

	// Replace with second job (same ID)
	err = s.Schedule("test4", "@every 100ms", func(ctx context.Context) {
		mu.Lock()
		callCount += 10
		mu.Unlock()
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	// Should only have fired the second callback
	if callCount != 10 {
		t.Errorf("expected callCount=10, got %d", callCount)
	}
}

func TestCronDelay(t *testing.T) {
	// Test @every
	d := CronDelay("@every 5m")
	if d != 5*time.Minute {
		t.Errorf("expected 5m, got %v", d)
	}

	// Test @daily
	d = CronDelay("@daily")
	if d < 0 || d > 24*time.Hour {
		t.Errorf("expected 0-24h, got %v", d)
	}

	// Test @hourly
	d = CronDelay("@hourly")
	if d < 0 || d > time.Hour {
		t.Errorf("expected 0-1h, got %v", d)
	}
}

func TestCronDelayInvalid(t *testing.T) {
	d := CronDelay("invalid")
	if d <= 0 {
		t.Error("expected positive delay for invalid cron")
	}
}

func TestParseCron(t *testing.T) {
	c, err := parseCron("* * * * *")
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Minute) != 60 {
		t.Errorf("expected 60 minutes, got %d", len(c.Minute))
	}
	if len(c.Hour) != 24 {
		t.Errorf("expected 24 hours, got %d", len(c.Hour))
	}
}

func TestParseCronRange(t *testing.T) {
	c, err := parseCron("0 9-17 * * *")
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Hour) != 9 {
		t.Errorf("expected 9 hours (9-17), got %d", len(c.Hour))
	}
}

func TestParseCronStep(t *testing.T) {
	c, err := parseCron("*/15 * * * *")
	if err != nil {
		t.Fatal(err)
	}
	// Should have 0, 15, 30, 45
	if len(c.Minute) != 4 {
		t.Errorf("expected 4 minutes, got %d", len(c.Minute))
	}
}

func TestParseCronInvalid(t *testing.T) {
	_, err := parseCron("invalid")
	if err == nil {
		t.Error("expected error for invalid cron")
	}
}

func TestParseCronTooManyFields(t *testing.T) {
	_, err := parseCron("* * * * * *")
	if err == nil {
		t.Error("expected error for too many fields")
	}
}
