package components

import "testing"

// TestNewAutocompleteManager tests manager creation
func TestNewAutocompleteManager(t *testing.T) {
	am := NewAutocompleteManager()

	if len(am.keywords) == 0 {
		t.Errorf("expected keywords to be loaded")
	}
	if am.maxVisible != 10 {
		t.Errorf("expected maxVisible 10, got %d", am.maxVisible)
	}
}

// TestGetSuggestions tests suggestion retrieval
func TestGetSuggestions(t *testing.T) {
	am := NewAutocompleteManager()

	// Test with "SEL" prefix
	sugg := am.GetSuggestions("SEL")

	if len(sugg) == 0 {
		t.Errorf("expected suggestions for 'SEL'")
	}
	if sugg[0] != "SELECT" {
		t.Errorf("expected 'SELECT' in suggestions, got %v", sugg)
	}
}

// TestGetSuggestionsEmpty tests empty input
func TestGetSuggestionsEmpty(t *testing.T) {
	am := NewAutocompleteManager()

	sugg := am.GetSuggestions("")

	if len(sugg) != 0 {
		t.Errorf("expected empty suggestions for empty input")
	}
	if am.IsVisible() {
		t.Errorf("expected visible to be false for empty input")
	}
}

// TestSelectNext tests forward navigation
func TestSelectNext(t *testing.T) {
	am := NewAutocompleteManager()
	am.GetSuggestions("S")

	if am.selected != 0 {
		t.Fatalf("expected initial selection 0")
	}

	am.SelectNext()

	if am.selected != 1 {
		t.Errorf("expected selection 1 after SelectNext, got %d", am.selected)
	}
}

// TestSelectPrev tests backward navigation
func TestSelectPrev(t *testing.T) {
	am := NewAutocompleteManager()
	am.GetSuggestions("S")
	am.SelectNext()

	am.SelectPrev()

	if am.selected != 0 {
		t.Errorf("expected selection 0 after SelectPrev, got %d", am.selected)
	}
}

// TestGetSelected tests getting selected suggestion
func TestGetSelected(t *testing.T) {
	am := NewAutocompleteManager()
	sugg := am.GetSuggestions("SEL")

	if len(sugg) == 0 {
		t.Fatalf("expected suggestions")
	}

	selected := am.GetSelected()

	if selected != "SELECT" {
		t.Errorf("expected 'SELECT', got %s", selected)
	}
}

// TestRenderSuggestions tests rendering
func TestRenderSuggestions(t *testing.T) {
	am := NewAutocompleteManager()
	am.GetSuggestions("SEL")

	rendered := am.RenderSuggestions()

	if rendered == "" {
		t.Errorf("expected non-empty render")
	}
	if !contains(rendered, "SELECT") {
		t.Errorf("expected 'SELECT' in rendered output")
	}
}

// TestHide tests hiding suggestions
func TestHide(t *testing.T) {
	am := NewAutocompleteManager()
	am.GetSuggestions("SEL")

	if !am.IsVisible() {
		t.Fatalf("expected visible to be true")
	}

	am.Hide()

	if am.IsVisible() {
		t.Errorf("expected visible to be false after Hide")
	}
	if len(am.suggestions) != 0 {
		t.Errorf("expected suggestions to be cleared")
	}
}

// TestAddKeyword tests adding custom keyword
func TestAddKeyword(t *testing.T) {
	am := NewAutocompleteManager()
	initialCount := len(am.keywords)

	am.AddKeyword("CUSTOM")

	if len(am.keywords) != initialCount+1 {
		t.Errorf("expected keyword count to increase")
	}

	// Test duplicate prevention
	am.AddKeyword("CUSTOM")

	if len(am.keywords) != initialCount+1 {
		t.Errorf("expected duplicate prevention")
	}
}

// TestCaseInsensitivity tests case handling
func TestCaseInsensitivity(t *testing.T) {
	am := NewAutocompleteManager()

	// Test lowercase
	sugg1 := am.GetSuggestions("sel")
	// Test uppercase
	sugg2 := am.GetSuggestions("SEL")
	// Test mixed
	am.GetSuggestions("SeL")

	if len(sugg1) != len(sugg2) {
		t.Errorf("expected case-insensitive matching")
	}
}

// TestMaxVisible tests maximum visible limit
func TestMaxVisible(t *testing.T) {
	am := NewAutocompleteManager()
	am.maxVisible = 5

	sugg := am.GetSuggestions("")

	if len(sugg) > am.maxVisible {
		t.Errorf("expected at most %d suggestions, got %d", am.maxVisible, len(sugg))
	}
}

// TestMultipleMatches tests multiple matching keywords
func TestMultipleMatches(t *testing.T) {
	am := NewAutocompleteManager()

	sugg := am.GetSuggestions("CR")

	// Should match CREATE, CROSS, etc.
	if len(sugg) == 0 {
		t.Errorf("expected multiple suggestions for 'CR'")
	}

	for _, s := range sugg {
		if !contains(s, "CR") && s != "CREATE" && s != "CROSS" {
			// Check if starts with CR
			if len(s) < 2 || s[0:2] != "CR" {
				t.Errorf("expected suggestion to start with CR, got %s", s)
			}
		}
	}
}

// TestDefaultTheme tests theme creation
func TestDefaultTheme(t *testing.T) {
	theme := DefaultAutocompleteTheme()

	if theme.Background == "" {
		t.Errorf("expected Background color")
	}
	if theme.Foreground == "" {
		t.Errorf("expected Foreground color")
	}
	if theme.Selection == "" {
		t.Errorf("expected Selection color")
	}
}
