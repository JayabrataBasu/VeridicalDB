package log

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestLogLevels(t *testing.T) {
	tests := []struct {
		level    Level
		expected string
	}{
		{LevelDebug, "DEBUG"},
		{LevelInfo, "INFO"},
		{LevelWarn, "WARN"},
		{LevelError, "ERROR"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.level.String(); got != tt.expected {
				t.Errorf("Level.String() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestParseLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected Level
	}{
		{"debug", LevelDebug},
		{"info", LevelInfo},
		{"warn", LevelWarn},
		{"error", LevelError},
		{"unknown", LevelInfo}, // default
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := ParseLevel(tt.input); got != tt.expected {
				t.Errorf("ParseLevel(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestLoggerTextFormat(t *testing.T) {
	var buf bytes.Buffer
	logger := New(&buf, LevelDebug, FormatText)

	logger.Info("test message", "key", "value")

	output := buf.String()
	if !strings.Contains(output, "[INFO]") {
		t.Errorf("expected output to contain [INFO], got %s", output)
	}
	if !strings.Contains(output, "test message") {
		t.Errorf("expected output to contain 'test message', got %s", output)
	}
	if !strings.Contains(output, "key=value") {
		t.Errorf("expected output to contain 'key=value', got %s", output)
	}
}

func TestLoggerJSONFormat(t *testing.T) {
	var buf bytes.Buffer
	logger := New(&buf, LevelDebug, FormatJSON)

	logger.Info("test message", "key", "value")

	var entry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON output: %v", err)
	}

	if entry["level"] != "INFO" {
		t.Errorf("expected level 'INFO', got %v", entry["level"])
	}
	if entry["msg"] != "test message" {
		t.Errorf("expected msg 'test message', got %v", entry["msg"])
	}
	if entry["key"] != "value" {
		t.Errorf("expected key 'value', got %v", entry["key"])
	}
}

func TestLoggerLevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	logger := New(&buf, LevelWarn, FormatText)

	logger.Debug("debug message")
	logger.Info("info message")
	logger.Warn("warn message")
	logger.Error("error message")

	output := buf.String()

	if strings.Contains(output, "debug message") {
		t.Error("debug message should be filtered out")
	}
	if strings.Contains(output, "info message") {
		t.Error("info message should be filtered out")
	}
	if !strings.Contains(output, "warn message") {
		t.Error("warn message should be present")
	}
	if !strings.Contains(output, "error message") {
		t.Error("error message should be present")
	}
}

func TestLoggerWith(t *testing.T) {
	var buf bytes.Buffer
	logger := New(&buf, LevelDebug, FormatText)
	childLogger := logger.With("component", "test")

	childLogger.Info("test message")

	output := buf.String()
	if !strings.Contains(output, "component=test") {
		t.Errorf("expected output to contain 'component=test', got %s", output)
	}
}

func TestNewFromConfig(t *testing.T) {
	logger, err := NewFromConfig("debug", "text", "stdout")
	if err != nil {
		t.Fatalf("NewFromConfig() error = %v", err)
	}
	if logger == nil {
		t.Error("NewFromConfig() returned nil logger")
	}
}
