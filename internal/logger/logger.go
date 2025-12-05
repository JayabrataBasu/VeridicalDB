// Package logger provides structured logging for VeridicalDB
package logger

import (
	"fmt"
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger wraps zap.SugaredLogger with VeridicalDB-specific functionality
type Logger struct {
	*zap.SugaredLogger
	base *zap.Logger
}

// New creates a new Logger with the specified configuration
func New(level, format, output string) (*Logger, error) {
	// Parse log level
	var zapLevel zapcore.Level
	switch strings.ToLower(level) {
	case "debug":
		zapLevel = zapcore.DebugLevel
	case "info":
		zapLevel = zapcore.InfoLevel
	case "warn", "warning":
		zapLevel = zapcore.WarnLevel
	case "error":
		zapLevel = zapcore.ErrorLevel
	default:
		return nil, fmt.Errorf("unknown log level: %s", level)
	}

	// Configure encoder
	var encoderConfig zapcore.EncoderConfig
	var encoder zapcore.Encoder

	if strings.ToLower(format) == "json" {
		encoderConfig = zap.NewProductionEncoderConfig()
		encoderConfig.TimeKey = "timestamp"
		encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	} else {
		encoderConfig = zap.NewDevelopmentEncoderConfig()
		encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		encoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout("15:04:05.000")
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	}

	// Configure output
	var writeSyncer zapcore.WriteSyncer
	switch strings.ToLower(output) {
	case "stderr", "":
		writeSyncer = zapcore.AddSync(os.Stderr)
	case "stdout":
		writeSyncer = zapcore.AddSync(os.Stdout)
	default:
		// Treat as file path
		file, err := os.OpenFile(output, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open log file %s: %w", output, err)
		}
		writeSyncer = zapcore.AddSync(file)
	}

	// Build the logger
	core := zapcore.NewCore(encoder, writeSyncer, zapLevel)
	base := zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1))

	return &Logger{
		SugaredLogger: base.Sugar(),
		base:          base,
	}, nil
}

// Sync flushes any buffered log entries
func (l *Logger) Sync() error {
	return l.base.Sync()
}

// With returns a new Logger with additional context fields
func (l *Logger) With(args ...interface{}) *Logger {
	return &Logger{
		SugaredLogger: l.SugaredLogger.With(args...),
		base:          l.base,
	}
}

// Named returns a new Logger with the given name added to the logger's name
func (l *Logger) Named(name string) *Logger {
	return &Logger{
		SugaredLogger: l.base.Named(name).Sugar(),
		base:          l.base.Named(name),
	}
}

// Info logs a message with key-value pairs at Info level
func (l *Logger) Info(msg string, keysAndValues ...interface{}) {
	l.SugaredLogger.Infow(msg, keysAndValues...)
}

// Debug logs a message with key-value pairs at Debug level
func (l *Logger) Debug(msg string, keysAndValues ...interface{}) {
	l.SugaredLogger.Debugw(msg, keysAndValues...)
}

// Warn logs a message with key-value pairs at Warn level
func (l *Logger) Warn(msg string, keysAndValues ...interface{}) {
	l.SugaredLogger.Warnw(msg, keysAndValues...)
}

// Error logs a message with key-value pairs at Error level
func (l *Logger) Error(msg string, keysAndValues ...interface{}) {
	l.SugaredLogger.Errorw(msg, keysAndValues...)
}

// Fatal logs a message with key-value pairs at Fatal level then calls os.Exit(1)
func (l *Logger) Fatal(msg string, keysAndValues ...interface{}) {
	l.SugaredLogger.Fatalw(msg, keysAndValues...)
}

// NewNop returns a no-op Logger for testing
func NewNop() *Logger {
	return &Logger{
		SugaredLogger: zap.NewNop().Sugar(),
		base:          zap.NewNop(),
	}
}
