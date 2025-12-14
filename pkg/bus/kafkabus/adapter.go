package kafkabus

import (
	"github.com/forever-dev/go-bus/pkg/bus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaLoggerAdapter converts a bus.Logger into a kgo.Logger interface
type KafkaLoggerAdapter struct {
	Logger bus.Logger
	lvl    kgo.LogLevel
}

// Level implements kgo.Logger.
func (l *KafkaLoggerAdapter) Level() kgo.LogLevel {
	return l.lvl
}

// Log implements the kgo.Logger interface
func (l *KafkaLoggerAdapter) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	if l.lvl > level {
		return
	}

	// Simple mapping of keyvals to bus.Logger fields
	fields := make([]any, 0, len(keyvals))
	for i := 0; i < len(keyvals); i += 2 {
		if i+1 < len(keyvals) {
			fields = append(fields, keyvals[i], keyvals[i+1])
		}
	}

	switch level {
	case kgo.LogLevelDebug:
		l.Logger.Debug(msg, fields...)
	case kgo.LogLevelInfo:
		l.Logger.Info(msg, fields...)
	case kgo.LogLevelWarn:
		l.Logger.Warn(msg, fields...)
	case kgo.LogLevelError:
		l.Logger.Error(msg, fields...)
	default:
		l.Logger.Info(msg, fields...)
	}
}

// LogModes returns all log levels
func (*KafkaLoggerAdapter) LogModes() kgo.LogLevel {
	return kgo.LogLevelDebug | kgo.LogLevelInfo | kgo.LogLevelWarn | kgo.LogLevelError
}

// NewKafkaLoggerAdapter is a helper function to create the adapter.
func NewKafkaLoggerAdapter(l bus.Logger, lvl kgo.LogLevel) *KafkaLoggerAdapter {
	return &KafkaLoggerAdapter{
		Logger: l,
		lvl:    lvl,
	}
}
