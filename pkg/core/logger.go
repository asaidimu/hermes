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

// NopLogger is a no-op logger that discards all log output. It serves as the
// default logger when none is configured, allowing code to call logging methods
// without nil checks.
// @note #review-20260822-030 issue status=resolved priority=P3 tags=#review,#documentation : NopLogger lacks doc comment
//
// Fixed by adding doc comment explaining NopLogger's purpose as the default
// silent logger.
type NopLogger struct{}

func (NopLogger) Debug(msg string, keysAndValues ...any) {}
func (NopLogger) Info(msg string, keysAndValues ...any)  {}
func (NopLogger) Warn(msg string, keysAndValues ...any)  {}
func (NopLogger) Error(msg string, keysAndValues ...any) {}
func (n NopLogger) With(keysAndValues ...any) Logger     { return n }

// contextKey is a private type for context keys to avoid collisions with
// keys from other packages. Callers should use WithLogger/GetLogger rather
// than creating their own context keys for logger storage.
// @note #review-20260822-031 issue status=resolved priority=P3 tags=#review,#documentation : Context key is undocumented
//
// Fixed by adding doc comment explaining the context key's purpose and
// warning callers to use WithLogger/GetLogger instead of their own keys.
type contextKey struct{}

func WithLogger(ctx context.Context, logger Logger) context.Context {
	return context.WithValue(ctx, contextKey{}, logger)
}

// LoggerFromContext retrieves the logger from the context, or NopLogger if
// none was set via WithLogger.
//
// @note #review-20260822-032 issue status=resolved priority=P2 tags=#review,#naming : GetLogger uses anti-pattern Get prefix
//
// Resolved: added LoggerFromContext as the idiomatic name (matching the Go
// stdlib's ctx.Value convention, e.g. httptest.ContextFrom-style helpers).
// GetLogger is kept as a deprecated thin alias rather than removed/renamed
// in place — it's an exported function on a package other code may already
// depend on, and this repo has no way to verify all callers (internal or
// external) without a working build in this environment.
func LoggerFromContext(ctx context.Context) Logger {
	if ctx != nil {
		if l, ok := ctx.Value(contextKey{}).(Logger); ok && l != nil {
			return l
		}
	}
	return NopLogger{}
}

// GetLogger retrieves the logger from the context.
//
// Deprecated: use LoggerFromContext instead. Kept for backward compatibility.
func GetLogger(ctx context.Context) Logger {
	return LoggerFromContext(ctx)
}
