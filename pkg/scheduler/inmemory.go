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

	// @note #review-20260822-052 issue status=open priority=P2 tags=#review,#bug : Context never propagated to callback
	//
	// context.WithCancel(context.Background()) creates a context that is never propagated
	// to the callback. The cancel is called in Cancel() and Shutdown(), but the callback
	// at line 56 receives context.Background(), not the cancellable context. The context
	// is unused — either pass it to the callback or remove it.
	_, cancel := context.WithCancel(context.Background())

	js := &jobState{cancel: cancel, callback: callback}
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
		js.callback(context.Background())
		// Reschedule if still registered.
		s.mu.Lock()
		_, still := s.jobs[id]
		s.mu.Unlock()
		if still {
			s.scheduleNextLocked(id, js, cron)
		}
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
