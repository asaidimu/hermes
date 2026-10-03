// Package core holds the L0 shared layer: the concrete logger (zap), the
// error vocabulary, and context helpers.
//
// 20261003 refactor (plan R.3): the former core.Logger interface, NopLogger
// null-object, and ZapLogger adapter are deleted — the interface had exactly
// one real implementation (zap, via a hand-rolled adapter reimplementing
// what zap.SugaredLogger already provides). Callers take *zap.SugaredLogger
// directly; zap.NewNop().Sugar() is the no-op logger.
package core

import (
	"context"

	"go.uber.org/zap"
)

// contextKey is a private type for context keys to avoid collisions with
// keys from other packages. Callers should use WithLogger/LoggerFromContext
// rather than creating their own context keys for logger storage.
type contextKey struct{}

// WithLogger attaches a logger to the context.
func WithLogger(ctx context.Context, logger *zap.SugaredLogger) context.Context {
	return context.WithValue(ctx, contextKey{}, logger)
}

// LoggerFromContext retrieves the logger from the context, or a no-op
// sugared logger if none was set via WithLogger.
func LoggerFromContext(ctx context.Context) *zap.SugaredLogger {
	if ctx != nil {
		if l, ok := ctx.Value(contextKey{}).(*zap.SugaredLogger); ok && l != nil {
			return l
		}
	}
	return zap.NewNop().Sugar()
}
