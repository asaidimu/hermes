// Package scheduler provides a pluggable scheduling interface for the workflow
// runtime. It handles cron-based recurring events, one-shot delays, and
// cancellation. The in-memory implementation is suitable for single-process
// deployments; persistent implementations can back state in a database.
package scheduler

import "context"

// Scheduler is the abstraction for time-based event scheduling.
type Scheduler interface {
	// Schedule registers a job identified by id that fires according to the
	// cron expression. The callback is invoked with a context that is cancelled
	// when the job is cancelled or the scheduler shuts down.
	// Cron expressions: "* * * * *" (min hour day month weekday)
	//   - "@every 5m"   → every 5 minutes
	//   - "@daily"      → once a day at midnight
	//   - "30 * * * *"  → at minute 30 of every hour
	Schedule(id string, cron string, callback func(ctx context.Context)) error

	// Cancel removes a pending job (no-op if not found).
	Cancel(id string) error

	// Shutdown stops all jobs and releases resources.
	Shutdown(ctx context.Context) error
}
