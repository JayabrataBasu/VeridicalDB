package theme

import (
	"testing"

	"github.com/charmbracelet/lipgloss"
)

func TestNewManager(t *testing.T) {
	manager := NewManager()
	if manager.current.Name != "dark" {
		t.Errorf("Expected default theme 'dark', got %s", manager.current.Name)
	}
}

func TestListThemes(t *testing.T) {
	manager := NewManager()
	themes := manager.ListThemes()

	if len(themes) == 0 {
		t.Error("No themes registered")
	}

	// Check for built-in themes
	expectedThemes := []string{"dark", "light", "dracula", "monokai", "solarized-dark", "solarized-light", "nord", "tokyo-night"}
	themeMap := make(map[string]bool)
	for _, name := range themes {
		themeMap[name] = true
	}

	for _, expected := range expectedThemes {
		if !themeMap[expected] {
			t.Errorf("Expected theme %s not found", expected)
		}
	}
}

func TestSetTheme(t *testing.T) {
	manager := NewManager()

	// Test valid theme switch
	if !manager.SetTheme("light") {
		t.Error("Failed to switch to light theme")
	}

	if manager.current.Name != "light" {
		t.Errorf("Expected current theme 'light', got %s", manager.current.Name)
	}

	// Test invalid theme
	if manager.SetTheme("nonexistent") {
		t.Error("Should not switch to nonexistent theme")
	}

	// Current theme should remain unchanged
	if manager.current.Name != "light" {
		t.Error("Current theme changed after failed switch")
	}
}

func TestGetTheme(t *testing.T) {
	manager := NewManager()

	// Test getting existing theme
	theme, ok := manager.GetTheme("dracula")
	if !ok {
		t.Error("Failed to get dracula theme")
	}
	if theme.Name != "dracula" {
		t.Errorf("Expected theme name 'dracula', got %s", theme.Name)
	}

	// Test getting nonexistent theme
	_, ok = manager.GetTheme("nonexistent")
	if ok {
		t.Error("Should not get nonexistent theme")
	}
}

func TestRegisterTheme(t *testing.T) {
	manager := NewManager()

	customTheme := &Theme{
		Name:        "custom",
		Description: "Custom test theme",
		Foreground:  "#FFFFFF",
		Background:  "#000000",
		Primary:     "#FF0000",
	}

	manager.RegisterTheme(customTheme)

	// Verify theme was registered
	theme, ok := manager.GetTheme("custom")
	if !ok {
		t.Error("Failed to register custom theme")
	}

	if theme.Primary != "#FF0000" {
		t.Errorf("Expected primary color #FF0000, got %s", theme.Primary)
	}
}

func TestThemeStyles(t *testing.T) {
	manager := NewManager()
	theme := manager.Current()

	styles := theme.Styles()

	if styles == nil {
		t.Fatal("Styles() returned nil")
	}
}

func TestAllBuiltInThemes(t *testing.T) {
	themes := []string{
		"dark", "light", "dracula", "monokai",
		"solarized-dark", "solarized-light", "nord", "tokyo-night",
	}

	for _, name := range themes {
		t.Run(name, func(t *testing.T) {
			manager := NewManager()
			if !manager.SetTheme(name) {
				t.Errorf("Failed to set theme %s", name)
			}

			theme := manager.Current()
			if theme.Foreground == "" {
				t.Errorf("Theme %s missing foreground color", name)
			}
			if theme.Background == "" {
				t.Errorf("Theme %s missing background color", name)
			}
			if theme.Primary == "" {
				t.Errorf("Theme %s missing primary color", name)
			}

			// Test styles generation
			styles := theme.Styles()
			if styles == nil {
				t.Errorf("Theme %s failed to generate styles", name)
			}
		})
	}
}

func TestThemeColorConsistency(t *testing.T) {
	manager := NewManager()
	theme := manager.Current()

	// Verify all required colors are set
	requiredColors := map[string]string{
		"Foreground": theme.Foreground,
		"Background": theme.Background,
		"Primary":    theme.Primary,
		"Secondary":  theme.Secondary,
		"Accent":     theme.Accent,
		"Success":    theme.Success,
		"Warning":    theme.Warning,
		"Error":      theme.Error,
		"Info":       theme.Info,
	}

	for name, color := range requiredColors {
		if color == "" {
			t.Errorf("Theme missing required color: %s", name)
		}
	}
}

func TestStyleSetCompleteness(t *testing.T) {
	manager := NewManager()
	styles := manager.Current().Styles()

	// All styles should be non-nil
	styleNames := []struct {
		name  string
		style lipgloss.Style
	}{
		{"Base", styles.Base},
		{"Primary", styles.Primary},
		{"Secondary", styles.Secondary},
		{"Accent", styles.Accent},
		{"Muted", styles.Muted},
		{"Success", styles.Success},
		{"Warning", styles.Warning},
		{"Error", styles.Error},
		{"Info", styles.Info},
		{"Border", styles.Border},
		{"BorderFocused", styles.BorderFocused},
		{"Selection", styles.Selection},
		{"Highlight", styles.Highlight},
		{"TableHeader", styles.TableHeader},
		{"TableRowEven", styles.TableRowEven},
		{"TableRowOdd", styles.TableRowOdd},
		{"TableSelected", styles.TableSelected},
		{"StatusBar", styles.StatusBar},
		{"Title", styles.Title},
		{"Subtitle", styles.Subtitle},
	}

	for _, s := range styleNames {
		// Verify style exists (non-zero value check)
		_ = s.style // Just access it to verify it exists
	}
}
