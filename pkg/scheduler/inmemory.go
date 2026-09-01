package scheduler

import (
	"context"
	"sync"
	"time"
)

// InMemoryScheduler is a non-persistent Scheduler backed by time.Timer.
// Suitable for single-process, development, or test environments.
type InMemoryScheduler struct {
	mu   sync.Mutex
	jobs map[string]*jobState
}

type jobState struct {
	ctx      context.Context
	cancel   context.CancelFunc
	callback func(ctx context.Context)
	timer    *time.Timer
}

// New creates a new InMemoryScheduler.
func New() *InMemoryScheduler {
	return &InMemoryScheduler{
		jobs: make(map[string]*jobState),
	}
}

func (s *InMemoryScheduler) Schedule(id string, cron string, callback func(ctx context.Context)) error {
	s.Cancel(id) // remove any existing job with same ID

	s.mu.Lock()
	defer s.mu.Unlock()

	// @note #review-20260822-052 issue status=resolved priority=P2 tags=#review,#bug : Context never propagated to callback
	//
	// Resolved: store the cancellable context on jobState and pass it to
	// the callback (js.ctx) instead of a fresh context.Background() at fire
	// time. Cancel()/Shutdown() calling js.cancel() now actually reaches
	// the callback via ctx.Done() / ctx.Err(), instead of being pure dead
	// code that only cancelled a context nothing observed.
	ctx, cancel := context.WithCancel(context.Background())

	js := &jobState{ctx: ctx, cancel: cancel, callback: callback}
	s.jobs[id] = js

	s.scheduleNextLocked(id, js, cron)
	return nil
}

func (s *InMemoryScheduler) scheduleNextLocked(id string, js *jobState, cron string) {
	delay := CronDelay(cron)
	if delay <= 0 {
		delay = time.Second
	}
	js.timer = time.AfterFunc(delay, func() {
		s.mu.Lock()
		current, ok := s.jobs[id]
		s.mu.Unlock()
		if !ok || current != js {
			return
		}
		js.callback(js.ctx)
		// @note #review-20260826-001 issue status=resolved priority=P1 tags=#review,#concurrency : Timer re-arm writes js.timer without holding s.mu
		// @author ox-alpha
		//
		// The callback unlocked at the bottom, then called
		// s.scheduleNextLocked WITHOUT re-acquiring s.mu, so the js.timer
		// assignment ran unsynchronized on the recursive path. `go test
		// -race` confirmed: concurrent Shutdown()/Cancel() reads of js.timer
		// raced this write (TestDelayCronPauseResume). Fixed by moving the
		// still-registered check and reschedule call back inside the
		// critical section.
		// Reschedule if still registered. See the review note above: this
		// critical section must also cover the re-arm write.
		s.mu.Lock()
		_, still := s.jobs[id]
		if still {
			s.scheduleNextLocked(id, js, cron)
		}
		s.mu.Unlock()
	})
}

func (s *InMemoryScheduler) Cancel(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	js, ok := s.jobs[id]
	if !ok {
		return nil
	}
	if js.timer != nil {
		js.timer.Stop()
	}
	if js.cancel != nil {
		js.cancel()
	}
	delete(s.jobs, id)
	return nil
}

func (s *InMemoryScheduler) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id, js := range s.jobs {
		if js.timer != nil {
			js.timer.Stop()
		}
		if js.cancel != nil {
			js.cancel()
		}
		delete(s.jobs, id)
	}
	return nil
}
