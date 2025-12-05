package logger

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewLogger(t *testing.T) {
	tests := []struct {
		name        string
		level       string
		format      string
		output      string
		shouldError bool
	}{
		{"debug text stderr", "debug", "text", "stderr", false},
		{"info json stdout", "info", "json", "stdout", false},
		{"warn text stderr", "warn", "text", "stderr", false},
		{"error json stderr", "error", "json", "stderr", false},
		{"invalid level", "invalid", "text", "stderr", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log, err := New(tt.level, tt.format, tt.output)

			if tt.shouldError {
				if err == nil {
					t.Error("Expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if log == nil {
				t.Fatal("Logger is nil")
			}

			log.Sync()
		})
	}
}

func TestLoggerToFile(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "test.log")

	log, err := New("info", "text", logFile)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}

	log.Info("test message", "key", "value")
	log.Sync()

	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	if !strings.Contains(string(content), "test message") {
		t.Error("Log file doesn't contain expected message")
	}
}

func TestLoggerNop(t *testing.T) {
	log := NewNop()
	if log == nil {
		t.Fatal("NewNop returned nil")
	}

	// Should not panic
	log.Info("test")
	log.Debug("test")
	log.Warn("test")
	log.Error("test")
	log.Sync()
}

func TestLoggerWith(t *testing.T) {
	log := NewNop()
	child := log.With("component", "test")

	if child == nil {
		t.Fatal("With returned nil")
	}

	// Should not panic
	child.Info("test with context")
}

func TestLoggerNamed(t *testing.T) {
	log := NewNop()
	named := log.Named("subsystem")

	if named == nil {
		t.Fatal("Named returned nil")
	}

	// Should not panic
	named.Info("test with name")
}

func TestLoggerJSON(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "test.json.log")

	log, err := New("info", "json", logFile)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}

	log.Info("json test", "number", 42)
	log.Sync()

	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	// JSON format should contain these
	if !bytes.Contains(content, []byte(`"msg"`)) {
		t.Error("JSON log doesn't contain msg field")
	}
}
