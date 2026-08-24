package runtime

import (
	"context"
)

// HestiaEventSourceFunc adapts hestia-style event source functions to the
// EventSource interface. It accepts Register and Shutdown functions directly,
// decoupling hermes from the hestia event source implementation.
type HestiaEventSourceFunc struct {
	register func(ctx context.Context, params RegisterParams) (cleanup func(), err error)
	shutdown func(ctx context.Context) error
}

// NewHestiaEventSourceFunc creates an EventSource from hestia-style functions.
func NewHestiaEventSourceFunc(
	register func(ctx context.Context, params RegisterParams) (cleanup func(), err error),
	shutdown func(ctx context.Context) error,
) *HestiaEventSourceFunc {
	return &HestiaEventSourceFunc{
		register: register,
		shutdown: shutdown,
	}
}

func (e *HestiaEventSourceFunc) OnRegister(ctx context.Context, params RegisterParams) (func(), error) {
	if e.register != nil {
		return e.register(ctx, params)
	}
	return nil, nil
}

func (e *HestiaEventSourceFunc) OnShutdown(ctx context.Context) error {
	if e.shutdown != nil {
		return e.shutdown(ctx)
	}
	return nil
}

var _ EventSource = (*HestiaEventSourceFunc)(nil)
