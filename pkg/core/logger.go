package core

import "context"

// Logger is a structured logger interface.
type Logger interface {
	Debug(msg string, keysAndValues ...any)
	Info(msg string, keysAndValues ...any)
	Warn(msg string, keysAndValues ...any)
	Error(msg string, keysAndValues ...any)
	With(keysAndValues ...any) Logger
}

type NopLogger struct{}

func (NopLogger) Debug(msg string, keysAndValues ...any) {}
func (NopLogger) Info(msg string, keysAndValues ...any)  {}
func (NopLogger) Warn(msg string, keysAndValues ...any)  {}
func (NopLogger) Error(msg string, keysAndValues ...any) {}
func (n NopLogger) With(keysAndValues ...any) Logger    { return n }

type contextKey struct{}

func WithLogger(ctx context.Context, logger Logger) context.Context {
	return context.WithValue(ctx, contextKey{}, logger)
}

func GetLogger(ctx context.Context) Logger {
	if ctx != nil {
		if l, ok := ctx.Value(contextKey{}).(Logger); ok && l != nil {
			return l
		}
	}
	return NopLogger{}
}
