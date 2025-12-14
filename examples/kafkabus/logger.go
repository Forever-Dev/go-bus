package main

import (
	"fmt"
	"time"

	"github.com/forever-dev/go-bus/pkg/bus"
)

// SimpleConsoleLogger implements the bus.Logger interface for clear example output.
type SimpleConsoleLogger struct{}

func NewSimpleConsoleLogger() bus.Logger {
	return &SimpleConsoleLogger{}
}

// formatFields converts key-value pairs into a clean log string.
func formatFields(fields []any) string {
	var parts []string
	for i := 0; i < len(fields); i += 2 {
		key, ok := fields[i].(string)
		if !ok {
			continue
		}
		value := ""
		if i+1 < len(fields) {
			value = fmt.Sprintf("%v", fields[i+1])
		}
		parts = append(parts, fmt.Sprintf("%s=%s", key, value))
	}
	if len(parts) > 0 {
		return " | " + fmt.Sprintf("%s", parts)
	}
	return ""
}

func (l *SimpleConsoleLogger) log(level string, msg string, fields ...any) {
	formattedFields := formatFields(fields)
	timestamp := time.Now().Format("15:04:05.000")
	fmt.Printf("[%s] %-5s: %s%s\n", timestamp, level, msg, formattedFields)
}

func (l *SimpleConsoleLogger) Debug(msg string, fields ...any) {
	l.log("DEBUG", msg, fields...)
}

func (l *SimpleConsoleLogger) Info(msg string, fields ...any) {
	l.log("INFO", msg, fields...)
}

func (l *SimpleConsoleLogger) Warn(msg string, fields ...any) {
	l.log("WARN", msg, fields...)
}

func (l *SimpleConsoleLogger) Error(msg string, fields ...any) {
	l.log("ERROR", msg, fields...)
}
