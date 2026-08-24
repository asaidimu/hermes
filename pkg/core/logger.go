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

// @note #review-20260822-030 issue status=open priority=P3 tags=#review,#documentation : NopLogger lacks doc comment
//
// NopLogger is a public type used as the default logger when none is configured. Its
// purpose (silent discard of all log output) should be documented for callers who
// encounter it in debug traces.
func (NopLogger) Debug(msg string, keysAndValues ...any) {}
func (NopLogger) Info(msg string, keysAndValues ...any)  {}
func (NopLogger) Warn(msg string, keysAndValues ...any)  {}
func (NopLogger) Error(msg string, keysAndValues ...any) {}
func (n NopLogger) With(keysAndValues ...any) Logger     { return n }

type contextKey struct{}

// @note #review-20260822-031 issue status=open priority=P3 tags=#review,#documentation : Context key is undocumented
//
// WithLogger/GetLogger use a package-private contextKey{} struct to store the logger.
// There is no documentation warning callers not to create their own context keys, which
// would bypass the package's storage and retrieval mechanism.
func WithLogger(ctx context.Context, logger Logger) context.Context {
	return context.WithValue(ctx, contextKey{}, logger)
}

// GetLogger retrieves the logger from the context.
// @note #review-20260822-032 issue status=open priority=P2 tags=#review,#naming : GetLogger uses anti-pattern Get prefix
//
// GetLogger violates Go naming conventions where Get prefixes are considered Java-style.
// Idiomatic Go prefers LoggerFromContext(ctx) or LoggerFrom(ctx). This is a minor style
// issue but affects consistency with the Go standard library (ctx.Value pattern).
func GetLogger(ctx context.Context) Logger {
	if ctx != nil {
		if l, ok := ctx.Value(contextKey{}).(Logger); ok && l != nil {
			return l
		}
	}
	return NopLogger{}
}
