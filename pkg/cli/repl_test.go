package cli

import (
	"bytes"
	"strings"
	"testing"

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
			expected: []string{"No tables yet"},
		},
		{
			name:     "describe command",
			input:    "\\describe users;\n",
			expected: []string{"not found"},
		},
		{
			name:     "unknown command",
			input:    "FOOBAR;\n",
			expected: []string{"Unknown command", "FOOBAR"},
		},
		{
			name:     "create table stub",
			input:    "CREATE TABLE users(id INT);\n",
			expected: []string{"Not yet implemented", "Stage 2"},
		},
		{
			name:     "select stub",
			input:    "SELECT * FROM users;\n",
			expected: []string{"Not yet implemented", "Stage 3"},
		},
		{
			name:     "insert stub",
			input:    "INSERT INTO users VALUES (1);\n",
			expected: []string{"Not yet implemented", "Stage 3"},
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

			repl := NewREPL(input, &output, logger)
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

	repl := NewREPL(input, &output, logger)
	if err := repl.Run(); err != nil {
		t.Fatalf("REPL.Run() error = %v", err)
	}

	result := output.String()
	if !strings.Contains(result, "Not yet implemented") {
		t.Errorf("multiline SELECT should be processed, got:\n%s", result)
	}
}

func TestREPLWelcomeBanner(t *testing.T) {
	input := strings.NewReader("EXIT;\n")
	var output bytes.Buffer
	logger := log.New(&output, log.LevelError, log.FormatText)

	repl := NewREPL(input, &output, logger)
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
