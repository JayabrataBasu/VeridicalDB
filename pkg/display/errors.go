package display

import (
	"fmt"
	"strings"
)

// ErrorFormatter formats error messages for display.
type ErrorFormatter struct {
	IncludeSuggestion bool // Show recovery suggestions
}

// NewErrorFormatter creates a new ErrorFormatter with default settings.
func NewErrorFormatter() *ErrorFormatter {
	return &ErrorFormatter{
		IncludeSuggestion: true,
	}
}

// Format formats an error for display to the user.
func (ef *ErrorFormatter) Format(err error) string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf("Error: %v", err)
}

// FormatWithContext formats an error with operation context.
func (ef *ErrorFormatter) FormatWithContext(operation string, err error) string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf("Error executing %s: %v", operation, err)
}

// FormatWithSuggestion formats an error with recovery suggestions.
// Note: This is a placeholder for future enhanced error messages.
func (ef *ErrorFormatter) FormatWithSuggestion(err error, suggestion string) string {
	if err == nil {
		return ""
	}
	msg := fmt.Sprintf("Error: %v", err)
	if ef.IncludeSuggestion && suggestion != "" {
		msg += fmt.Sprintf("\nTip: %s", suggestion)
	}
	return msg
}

// GetSuggestion returns a suggested action based on the error type.
// This is a placeholder for smarter error handling in the future.
func (ef *ErrorFormatter) GetSuggestion(err error) string {
	if err == nil {
		return ""
	}

	errMsg := err.Error()

	// Syntax errors
	if contains(errMsg, "syntax", "parse", "unexpected") {
		return "Check your SQL syntax and try again."
	}

	// Not found errors
	if contains(errMsg, "not found", "no such") {
		return "Verify the table or column name exists."
	}

	// Permission errors
	if contains(errMsg, "permission", "access denied", "privilege") {
		return "Check your user privileges."
	}

	// Transaction errors
	if contains(errMsg, "transaction", "commit", "rollback") {
		return "Review your transaction state with \\txns; command."
	}

	return ""
}

// contains checks if a string contains any of the given substrings (case-insensitive).
func contains(s string, substrs ...string) bool {
	for _, substr := range substrs {
		if strings.Contains(strings.ToLower(s), strings.ToLower(substr)) {
			return true
		}
	}
	return false
}
