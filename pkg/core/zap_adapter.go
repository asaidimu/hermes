package core

import "go.uber.org/zap"

// ZapLogger adapts a *zap.Logger to the core.Logger interface.
type ZapLogger struct {
	l *zap.Logger
}

// NewZapLogger wraps a zap.Logger as a core.Logger.
func NewZapLogger(l *zap.Logger) *ZapLogger {
	if l == nil {
		return nil
	}
	return &ZapLogger{l: l}
}

func (z *ZapLogger) Debug(msg string, keysAndValues ...any) {
	z.l.Debug(msg, toZapFields(keysAndValues...)...)
}

func (z *ZapLogger) Info(msg string, keysAndValues ...any) {
	z.l.Info(msg, toZapFields(keysAndValues...)...)
}

func (z *ZapLogger) Warn(msg string, keysAndValues ...any) {
	z.l.Warn(msg, toZapFields(keysAndValues...)...)
}

func (z *ZapLogger) Error(msg string, keysAndValues ...any) {
	z.l.Error(msg, toZapFields(keysAndValues...)...)
}

func (z *ZapLogger) With(keysAndValues ...any) Logger {
	return &ZapLogger{l: z.l.With(toZapFields(keysAndValues)...)}
}

func toZapFields(keysAndValues ...any) []zap.Field {
	fields := make([]zap.Field, 0, len(keysAndValues)/2)
	for i := 0; i+1 < len(keysAndValues); i += 2 {
		key, ok := keysAndValues[i].(string)
		if !ok {
			continue
		}
		fields = append(fields, zap.Any(key, keysAndValues[i+1]))
	}
	return fields
}
