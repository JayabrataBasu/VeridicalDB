// Package log provides structured logging for VeridicalDB.
package log

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Level represents the severity of a log message.
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

// String returns the string representation of a log level.
func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarn:
		return "WARN"
	case LevelError:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// ParseLevel converts a string to a Level.
func ParseLevel(s string) Level {
	switch s {
	case "debug":
		return LevelDebug
	case "info":
		return LevelInfo
	case "warn":
		return LevelWarn
	case "error":
		return LevelError
	default:
		return LevelInfo
	}
}

// Format represents the output format for logs.
type Format int

const (
	FormatText Format = iota
	FormatJSON
)

// ParseFormat converts a string to a Format.
func ParseFormat(s string) Format {
	switch s {
	case "json":
		return FormatJSON
	default:
		return FormatText
	}
}

// Logger provides structured logging functionality.
type Logger struct {
	mu     sync.Mutex
	out    io.Writer
	level  Level
	format Format
	fields map[string]interface{}
}

// New creates a new Logger with the specified configuration.
func New(out io.Writer, level Level, format Format) *Logger {
	return &Logger{
		out:    out,
		level:  level,
		format: format,
		fields: make(map[string]interface{}),
	}
}

// NewFromConfig creates a Logger from configuration strings.
func NewFromConfig(levelStr, formatStr, output string) (*Logger, error) {
	level := ParseLevel(levelStr)
	format := ParseFormat(formatStr)

	var out io.Writer
	switch output {
	case "stdout", "":
		out = os.Stdout
	case "stderr":
		out = os.Stderr
	default:
		f, err := os.OpenFile(output, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open log file %s: %w", output, err)
		}
		out = f
	}

	return New(out, level, format), nil
}

// With returns a new Logger with additional fields.
func (l *Logger) With(key string, value interface{}) *Logger {
	newLogger := &Logger{
		out:    l.out,
		level:  l.level,
		format: l.format,
		fields: make(map[string]interface{}),
	}
	for k, v := range l.fields {
		newLogger.fields[k] = v
	}
	newLogger.fields[key] = value
	return newLogger
}

// log writes a log entry.
func (l *Logger) log(level Level, msg string, args ...interface{}) {
	if level < l.level {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now().UTC()

	if l.format == FormatJSON {
		entry := map[string]interface{}{
			"time":  now.Format(time.RFC3339),
			"level": level.String(),
			"msg":   msg,
		}
		for k, v := range l.fields {
			entry[k] = v
		}
		// Add any additional key-value pairs from args
		for i := 0; i+1 < len(args); i += 2 {
			if key, ok := args[i].(string); ok {
				entry[key] = args[i+1]
			}
		}
		data, _ := json.Marshal(entry)
		fmt.Fprintln(l.out, string(data))
	} else {
		// Text format
		timestamp := now.Format("2006-01-02 15:04:05")
		fieldsStr := ""
		for k, v := range l.fields {
			fieldsStr += fmt.Sprintf(" %s=%v", k, v)
		}
		for i := 0; i+1 < len(args); i += 2 {
			fieldsStr += fmt.Sprintf(" %v=%v", args[i], args[i+1])
		}
		fmt.Fprintf(l.out, "%s [%s] %s%s\n", timestamp, level.String(), msg, fieldsStr)
	}
}

// Debug logs a message at DEBUG level.
func (l *Logger) Debug(msg string, args ...interface{}) {
	l.log(LevelDebug, msg, args...)
}

// Info logs a message at INFO level.
func (l *Logger) Info(msg string, args ...interface{}) {
	l.log(LevelInfo, msg, args...)
}

// Warn logs a message at WARN level.
func (l *Logger) Warn(msg string, args ...interface{}) {
	l.log(LevelWarn, msg, args...)
}

// Error logs a message at ERROR level.
func (l *Logger) Error(msg string, args ...interface{}) {
	l.log(LevelError, msg, args...)
}

// Default logger instance
var defaultLogger = New(os.Stdout, LevelInfo, FormatText)

// SetDefault sets the default logger.
func SetDefault(l *Logger) {
	defaultLogger = l
}

// Default returns the default logger.
func Default() *Logger {
	return defaultLogger
}

// Package-level convenience functions

func Debug(msg string, args ...interface{}) { defaultLogger.Debug(msg, args...) }
func Info(msg string, args ...interface{})  { defaultLogger.Info(msg, args...) }
func Warn(msg string, args ...interface{})  { defaultLogger.Warn(msg, args...) }
func Error(msg string, args ...interface{}) { defaultLogger.Error(msg, args...) }
