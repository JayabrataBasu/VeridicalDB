package cli

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/log"
)

func TestREPLCommands(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string // substrings that should appear in output
	}{
		{
			name:     "help command",
			input:    "HELP;\n",
			expected: []string{"Available Commands", "EXIT", "VERSION"},
		},
		{
			name:     "version command",
			input:    "VERSION;\n",
			expected: []string{"VeridicalDB version", Version},
		},
		{
			name:     "status command",
			input:    "STATUS;\n",
			expected: []string{"Server Status", "Running"},
		},
		{
			name:     "list tables command",
			input:    "\\list;\n",
			expected: []string{"not initialized"},
		},
		{
			name:     "describe command",
			input:    "\\describe users;\n",
			expected: []string{"not initialized"},
		},
		{
			name:     "unknown command",
			input:    "FOOBAR;\n",
			expected: []string{"Unknown command", "FOOBAR"},
		},
		{
			name:     "create table without executor",
			input:    "CREATE TABLE users(id INT);\n",
			expected: []string{"SQL executor not initialized"},
		},
		{
			name:     "select without executor",
			input:    "SELECT * FROM users;\n",
			expected: []string{"SQL executor not initialized"},
		},
		{
			name:     "insert without executor",
			input:    "INSERT INTO users VALUES (1);\n",
			expected: []string{"SQL executor not initialized"},
		},
		{
			name:     "begin stub",
			input:    "BEGIN;\n",
			expected: []string{"Not yet implemented", "Stage 4"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := strings.NewReader(tt.input + "EXIT;\n")
			var output bytes.Buffer
			logger := log.New(&output, log.LevelError, log.FormatText) // Suppress debug logs

			repl := NewREPL(input, &output, logger, nil)
			if err := repl.Run(); err != nil {
				t.Fatalf("REPL.Run() error = %v", err)
			}

			result := output.String()
			for _, exp := range tt.expected {
				if !strings.Contains(result, exp) {
					t.Errorf("expected output to contain %q, got:\n%s", exp, result)
				}
			}
		})
	}
}

func TestREPLMultilineInput(t *testing.T) {
	// Test that statements without semicolons continue to next line
	input := strings.NewReader("SELECT\n*\nFROM\nusers;\nEXIT;\n")
	var output bytes.Buffer
	logger := log.New(&output, log.LevelError, log.FormatText)

	repl := NewREPL(input, &output, logger, nil)
	if err := repl.Run(); err != nil {
		t.Fatalf("REPL.Run() error = %v", err)
	}

	result := output.String()
	// Without TableManager, SQL executor is not initialized
	if !strings.Contains(result, "SQL executor not initialized") {
		t.Errorf("multiline SELECT should be processed, got:\n%s", result)
	}
}

func TestREPLWelcomeBanner(t *testing.T) {
	input := strings.NewReader("EXIT;\n")
	var output bytes.Buffer
	logger := log.New(&output, log.LevelError, log.FormatText)

	repl := NewREPL(input, &output, logger, nil)
	if err := repl.Run(); err != nil {
		t.Fatalf("REPL.Run() error = %v", err)
	}

	result := output.String()
	if !strings.Contains(result, "VeridicalDB") {
		t.Error("expected welcome banner to contain 'VeridicalDB'")
	}
	if !strings.Contains(result, "Goodbye") {
		t.Error("expected goodbye message on exit")
	}
}

func TestVersion(t *testing.T) {
	if Version == "" {
		t.Error("Version should not be empty")
	}
}

func TestREPLWithTableManager(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "data")

	tm, err := catalog.NewTableManager(dataDir, 4096)
	if err != nil {
		t.Fatalf("NewTableManager error: %v", err)
	}

	// Create a table
	cols := []catalog.Column{
		{Name: "id", Type: catalog.TypeInt32, NotNull: true},
		{Name: "name", Type: catalog.TypeText, NotNull: false},
	}
	if err := tm.CreateTable("users", cols); err != nil {
		t.Fatalf("CreateTable error: %v", err)
	}

	// Test \list command
	input := strings.NewReader("\\list;\nEXIT;\n")
	var output bytes.Buffer
	logger := log.New(&output, log.LevelError, log.FormatText)

	repl := NewREPL(input, &output, logger, tm)
	if err := repl.Run(); err != nil {
		t.Fatalf("REPL.Run() error = %v", err)
	}

	result := output.String()
	if !strings.Contains(result, "users") {
		t.Errorf("expected output to list 'users' table, got:\n%s", result)
	}
	if !strings.Contains(result, "1 table") {
		t.Errorf("expected '1 table' in output, got:\n%s", result)
	}

	// Test \describe command
	input2 := strings.NewReader("\\describe users;\nEXIT;\n")
	var output2 bytes.Buffer

	repl2 := NewREPL(input2, &output2, logger, tm)
	if err := repl2.Run(); err != nil {
		t.Fatalf("REPL.Run() error = %v", err)
	}

	result2 := output2.String()
	if !strings.Contains(result2, "id") {
		t.Errorf("expected 'id' column in describe output, got:\n%s", result2)
	}
	if !strings.Contains(result2, "INT") {
		t.Errorf("expected 'INT' type in describe output, got:\n%s", result2)
	}
	if !strings.Contains(result2, "name") {
		t.Errorf("expected 'name' column in describe output, got:\n%s", result2)
	}
	if !strings.Contains(result2, "TEXT") {
		t.Errorf("expected 'TEXT' type in describe output, got:\n%s", result2)
	}
}
