package bus

type Logger interface {
	Debug(msg string, fields ...any)
	Info(msg string, fields ...any)
	Warn(msg string, fields ...any)
	Error(msg string, fields ...any)
}

type noOpLogger struct{}

func (l *noOpLogger) Debug(msg string, fields ...any) {}
func (l *noOpLogger) Info(msg string, fields ...any)  {}
func (l *noOpLogger) Warn(msg string, fields ...any)  {}
func (l *noOpLogger) Error(msg string, fields ...any) {}

func NewNoOpLogger() Logger {
	return &noOpLogger{}
}
