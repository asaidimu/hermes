package scheduler

import "context"

// HestiaSchedulerFunc adapts a hestia-style scheduler to the Scheduler interface.
// Instead of wrapping the full hestia scheduler, this accepts the three core
// functions (Register, Remove, Stop) as function values, decoupling hermes from
// the hestia scheduler implementation.
type HestiaSchedulerFunc struct {
	register func(name, cron string, fn func(ctx context.Context)) error
	remove   func(name string) bool
	stop     func(ctx context.Context) error
}

// NewHestiaSchedulerFunc creates a Scheduler from hestia-style functions.
func NewHestiaSchedulerFunc(
	register func(name, cron string, fn func(ctx context.Context)) error,
	remove func(name string) bool,
	stop func(ctx context.Context) error,
) *HestiaSchedulerFunc {
	return &HestiaSchedulerFunc{
		register: register,
		remove:   remove,
		stop:     stop,
	}
}

func (s *HestiaSchedulerFunc) Schedule(id string, cron string, callback func(ctx context.Context)) error {
	return s.register(id, cron, callback)
}

func (s *HestiaSchedulerFunc) Cancel(id string) error {
	s.remove(id)
	return nil
}

func (s *HestiaSchedulerFunc) Shutdown(ctx context.Context) error {
	if s.stop != nil {
		return s.stop(ctx)
	}
	return nil
}

var _ Scheduler = (*HestiaSchedulerFunc)(nil)
