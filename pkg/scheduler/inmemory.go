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
