package display

import (
	"strings"
	"testing"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql"
)

func TestValueFormatterFormat(t *testing.T) {
	vf := NewValueFormatter()

	tests := []struct {
		name     string
		value    catalog.Value
		expected string
	}{
		{
			name:     "null value",
			value:    catalog.Value{IsNull: true},
			expected: "NULL",
		},
		{
			name: "int32",
			value: catalog.Value{
				Type:  catalog.TypeInt32,
				Int32: 42,
			},
			expected: "42",
		},
		{
			name: "int64",
			value: catalog.Value{
				Type:  catalog.TypeInt64,
				Int64: 9223372036854775807,
			},
			expected: "9223372036854775807",
		},
		{
			name: "text",
			value: catalog.Value{
				Type: catalog.TypeText,
				Text: "hello",
			},
			expected: "hello",
		},
		{
			name: "bool true",
			value: catalog.Value{
				Type: catalog.TypeBool,
				Bool: true,
			},
			expected: "true",
		},
		{
			name: "bool false",
			value: catalog.Value{
				Type: catalog.TypeBool,
				Bool: false,
			},
			expected: "false",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := vf.Format(tc.value)
			if result != tc.expected {
				t.Errorf("expected %q, got %q", tc.expected, result)
			}
		})
	}
}

func TestTableFormatterCalculateWidths(t *testing.T) {
	tf := NewTableFormatter()

	columns := []string{"ID", "Name", "Email"}
	rows := [][]catalog.Value{
		{
			{Type: catalog.TypeInt32, Int32: 1},
			{Type: catalog.TypeText, Text: "Alice"},
			{Type: catalog.TypeText, Text: "alice@example.com"},
		},
		{
			{Type: catalog.TypeInt32, Int32: 2},
			{Type: catalog.TypeText, Text: "Bob"},
			{Type: catalog.TypeText, Text: "bob@example.com"},
		},
	}

	widths := tf.calculateWidths(columns, rows)

	if len(widths) != 3 {
		t.Errorf("expected 3 widths, got %d", len(widths))
	}

	// ID column should be at least 2 (header length)
	if widths[0] < 2 {
		t.Errorf("ID column width too small: %d", widths[0])
	}

	// Name column should accommodate "Alice" (5 chars)
	if widths[1] < 5 {
		t.Errorf("Name column width too small: %d", widths[1])
	}

	// Email column should be capped at 40
	if widths[2] > 40 {
		t.Errorf("Email column width exceeds max: %d > 40", widths[2])
	}
}

func TestTableFormatterFormatResult(t *testing.T) {
	tf := NewTableFormatter()

	result := &sql.Result{
		Columns: []string{"ID", "Name"},
		Rows: [][]catalog.Value{
			{
				{Type: catalog.TypeInt32, Int32: 1},
				{Type: catalog.TypeText, Text: "Alice"},
			},
		},
	}

	output := tf.FormatResult(result)

	// Check that output contains expected elements
	if !strings.Contains(output, "ID") {
		t.Errorf("output missing column header ID")
	}
	if !strings.Contains(output, "Name") {
		t.Errorf("output missing column header Name")
	}
	if !strings.Contains(output, "Alice") {
		t.Errorf("output missing data Alice")
	}
	if !strings.Contains(output, "(1 row(s))") {
		t.Errorf("output missing row count")
	}
}

func TestTableFormatterMessageResult(t *testing.T) {
	tf := NewTableFormatter()

	result := &sql.Result{
		Message: "Table 'users' created.",
	}

	output := tf.FormatResult(result)

	if output != "Table 'users' created." {
		t.Errorf("expected message result, got %q", output)
	}
}

func TestTruncate(t *testing.T) {
	tests := []struct {
		input    string
		maxLen   int
		expected string
	}{
		{
			input:    "hello",
			maxLen:   10,
			expected: "hello",
		},
		{
			input:    "hello world",
			maxLen:   8,
			expected: "hello...",
		},
		{
			input:    "hello world",
			maxLen:   3,
			expected: "...",
		},
		{
			input:    "",
			maxLen:   5,
			expected: "",
		},
	}

	for _, tc := range tests {
		result := truncate(tc.input, tc.maxLen)
		if result != tc.expected {
			t.Errorf("truncate(%q, %d): expected %q, got %q", tc.input, tc.maxLen, tc.expected, result)
		}
	}
}

func TestPromptBuilderBuild(t *testing.T) {
	tests := []struct {
		name             string
		database         string
		inTransaction    bool
		expectedContains string
	}{
		{
			name:             "default prompt",
			database:         "",
			inTransaction:    false,
			expectedContains: "veridical>",
		},
		{
			name:             "with database",
			database:         "mydb",
			inTransaction:    false,
			expectedContains: "veridical[mydb]>",
		},
		{
			name:             "transaction prompt",
			database:         "",
			inTransaction:    true,
			expectedContains: "[tx]>",
		},
		{
			name:             "transaction with database",
			database:         "mydb",
			inTransaction:    true,
			expectedContains: "veridical[mydb] [tx]>",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pb := NewPromptBuilder()
			pb.SetDatabase(tc.database)
			pb.SetTransaction(tc.inTransaction)

			prompt := pb.Build()
			if !strings.Contains(prompt, tc.expectedContains) {
				t.Errorf("expected prompt to contain %q, got %q", tc.expectedContains, prompt)
			}
		})
	}
}

func TestValueFormatterFormatForCSV(t *testing.T) {
	vf := NewValueFormatter()

	tests := []struct {
		name     string
		value    catalog.Value
		expected string
	}{
		{
			name:     "simple text",
			value:    catalog.Value{Type: catalog.TypeText, Text: "hello"},
			expected: "hello",
		},
		{
			name:     "text with comma",
			value:    catalog.Value{Type: catalog.TypeText, Text: "hello,world"},
			expected: `"hello,world"`,
		},
		{
			name:     "text with quote",
			value:    catalog.Value{Type: catalog.TypeText, Text: `hello"world`},
			expected: `"hello""world"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := vf.FormatForCSV(tc.value)
			if result != tc.expected {
				t.Errorf("expected %q, got %q", tc.expected, result)
			}
		})
	}
}

func TestValueFormatterFormatForJSON(t *testing.T) {
	vf := NewValueFormatter()

	// Test null
	result := vf.FormatForJSON(catalog.Value{IsNull: true})
	if result != nil {
		t.Errorf("expected nil for null value, got %v", result)
	}

	// Test int32
	result = vf.FormatForJSON(catalog.Value{Type: catalog.TypeInt32, Int32: 42})
	if result != int32(42) {
		t.Errorf("expected 42, got %v", result)
	}

	// Test bool
	result = vf.FormatForJSON(catalog.Value{Type: catalog.TypeBool, Bool: true})
	if result != true {
		t.Errorf("expected true, got %v", result)
	}

	// Test text
	result = vf.FormatForJSON(catalog.Value{Type: catalog.TypeText, Text: "hello"})
	if result != "hello" {
		t.Errorf("expected 'hello', got %v", result)
	}
}

func TestValueFormatterEstimatedWidth(t *testing.T) {
	vf := NewValueFormatter()

	value := catalog.Value{Type: catalog.TypeText, Text: "hello"}
	width := vf.EstimatedWidth(value)

	if width != 5 {
		t.Errorf("expected width 5, got %d", width)
	}
}

func TestErrorFormatterFormat(t *testing.T) {
	ef := NewErrorFormatter()

	// Test nil error
	result := ef.Format(nil)
	if result != "" {
		t.Errorf("expected empty string for nil error, got %q", result)
	}

	// Test error formatting
	result = ef.Format(catalog.ErrTableNotFound)
	if !strings.Contains(result, "Error:") {
		t.Errorf("expected 'Error:' in output, got %q", result)
	}
}

func TestValueFormatterTimestamp(t *testing.T) {
	vf := NewValueFormatter()

	now := time.Now()
	value := catalog.Value{
		Type:      catalog.TypeTimestamp,
		Timestamp: now,
	}

	result := vf.Format(value)

	// Check that the result contains date components
	if !strings.Contains(result, "-") {
		t.Errorf("expected date separator in timestamp, got %q", result)
	}

	// Verify it matches our layout
	expected := now.Format("2006-01-02 15:04:05")
	if result != expected {
		t.Errorf("expected %q, got %q", expected, result)
	}
}
