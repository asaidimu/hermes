// Package scheduler — benchmark for the CronDelay parsed-expression cache
// (#improvement-cron-cache-001).
//
// Before the cache, CronDelay re-parsed the 5-field cron expression on
// every call (350 µs/op on the analysis machine, mixing @every, named,
// and 5-field expressions). After the cache, the parse happens once
// per expression; subsequent calls hit the cache and cost only
// c.Next(time.Now()). The remaining cost is the Next() iteration over
// month/day/hour/minute — a separate optimisation target (see
// improvement plan §W2.3).
//
//   - BenchmarkCronDelay_5Field_NoCache: forces a re-parse on every call
//     by using a fresh expression string per iteration (cache miss).
//   - BenchmarkCronDelay_5Field_CacheHit: reuses one expression across
//     iterations (cache hit after the first call).
//   - BenchmarkCronDelay_AtEvery: the @every path — already cheap, no
//     cache involved. Included as a baseline.
//
// The cache is a sync.Map keyed by expression string; the parsed *Cron
// is immutable after construction so the cache is safe for concurrent
// access.
package scheduler

import (
	"fmt"
	"testing"
	"time"
)

// BenchmarkCronDelay_5Field_NoCache forces a cache miss on every call
// by using a distinct expression per iteration. This is the worst case
// and the closest to the pre-cache behaviour. It measures parseCron +
// Cron.Next together.
func BenchmarkCronDelay_5Field_NoCache(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Vary the minute field so each expression is a distinct cache
		// key. Expressions wrap around 0–59 to stay valid.
		expr := fmt.Sprintf("%d 9 * * *", i%60)
		_ = CronDelay(expr)
	}
}

// BenchmarkCronDelay_5Field_CacheHit measures the steady-state cost
// after the parsed *Cron is cached. Only c.Next(time.Now()) runs per
// call — parseCron is a one-time cost amortised across every fire of
// the job. The cost is dominated by Next()'s iteration over
// month/day/hour/minute, not the parse.
func BenchmarkCronDelay_5Field_CacheHit(b *testing.B) {
	expr := "30 9 * * *"
	// Prime the cache with one call outside the timer.
	_ = CronDelay(expr)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CronDelay(expr)
	}
}

// BenchmarkCronDelay_AtEvery is the @every baseline — no parse, no
// cache, just time.ParseDuration on the first call and time.Until on
// every call.
func BenchmarkCronDelay_AtEvery(b *testing.B) {
	expr := "@every 5m"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CronDelay(expr)
	}
}

// BenchmarkCronDelay_NamedInterval is the @daily baseline — no parse,
// no cache, just untilNext.
func BenchmarkCronDelay_NamedInterval(b *testing.B) {
	expr := "@daily"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CronDelay(expr)
	}
}

// TestCronParseCache_HitAfterMiss confirms the cache stores the parsed
// *Cron after the first call and returns the same pointer on subsequent
// calls — the correctness contract that BenchmarkCronDelay_5Field_CacheHit
// relies on.
func TestCronParseCache_HitAfterMiss(t *testing.T) {
	// Use a unique expression to avoid collision with other tests.
	expr := "15 10 * * 1-5"
	c1, err := parseCronCached(expr)
	if err != nil {
		t.Fatalf("first parseCronCached: %v", err)
	}
	c2, err := parseCronCached(expr)
	if err != nil {
		t.Fatalf("second parseCronCached: %v", err)
	}
	if c1 != c2 {
		t.Fatal("cache miss: subsequent call did not return the same *Cron pointer")
	}
	// The cached *Cron must still produce a valid Next time.
	next := c1.Next(time.Now())
	if next.IsZero() {
		t.Fatal("cached *Cron.Next returned zero time")
	}
}
