package tui

import (
	"strings"
	"testing"

	"github.com/charmbracelet/lipgloss"
)

// TestPaneStyles verifies that pane styles are created correctly for each theme.
func TestPaneStyles(t *testing.T) {
	themes := []string{"dark", "light", "high-contrast"}

	for _, themeName := range themes {
		t.Run(themeName, func(t *testing.T) {
			styles := NewPaneStylesFromPalette(themeName)

			if styles == nil {
				t.Fatalf("NewPaneStylesFromPalette(%q) returned nil", themeName)
				return
			}

			// Verify all style fields are set (non-zero) using GetBorder() multi-value return
			activeBorder, _, _, _, _ := styles.ActiveBorder.GetBorder()
			if activeBorder == (lipgloss.Border{}) {
				t.Error("ActiveBorder has no border set")
			}
			inactiveBorder, _, _, _, _ := styles.InactiveBorder.GetBorder()
			if inactiveBorder == (lipgloss.Border{}) {
				t.Error("InactiveBorder has no border set")
			}
		})
	}
}

// TestPaneStateTransitions tests active/inactive state changes.
func TestPaneStateTransitions(t *testing.T) {
	styles := NewPaneStylesFromPalette("dark")
	pane := NewPane("Test", 40, 10, styles)

	// Initially inactive
	if pane.State != PaneInactive {
		t.Errorf("Expected initial state to be PaneInactive, got %v", pane.State)
	}

	// Set active
	pane.SetActive()
	if pane.State != PaneActive {
		t.Errorf("Expected state to be PaneActive after SetActive(), got %v", pane.State)
	}

	// Set inactive
	pane.SetInactive()
	if pane.State != PaneInactive {
		t.Errorf("Expected state to be PaneInactive after SetInactive(), got %v", pane.State)
	}
}

// TestPaneContentUpdate tests content and dimension updates.
func TestPaneContentUpdate(t *testing.T) {
	styles := NewPaneStylesFromPalette("dark")
	pane := NewPane("Test", 40, 10, styles)

	// Set content
	pane.SetContent("Hello, World!")
	if pane.Content != "Hello, World!" {
		t.Errorf("Expected content 'Hello, World!', got '%s'", pane.Content)
	}

	// Update dimensions
	pane.SetDimensions(80, 20)
	if pane.Width != 80 || pane.Height != 20 {
		t.Errorf("Expected dimensions 80x20, got %dx%d", pane.Width, pane.Height)
	}
}

// TestPaneViewRendersTitle tests that the pane view includes the title.
func TestPaneViewRendersTitle(t *testing.T) {
	styles := NewPaneStylesFromPalette("dark")
	pane := NewPane("My Title", 40, 10, styles)
	pane.SetContent("Content here")

	view := pane.View()

	if !strings.Contains(view, "My Title") {
		t.Error("Pane view should contain the title")
	}
}

// TestPaneViewRendersContent tests that the pane view includes the content.
func TestPaneViewRendersContent(t *testing.T) {
	styles := NewPaneStylesFromPalette("dark")
	pane := NewPane("Title", 40, 10, styles)
	pane.SetContent("Test content 123")

	view := pane.View()

	if !strings.Contains(view, "Test content 123") {
		t.Error("Pane view should contain the content")
	}
}

// TestPaneRoundedBorders verifies rounded borders are used.
func TestPaneRoundedBorders(t *testing.T) {
	styles := NewPaneStylesFromPalette("dark")

	// Check that rounded border is used - GetBorder returns (Border, top, right, bottom, left)
	activeBorder, _, _, _, _ := styles.ActiveBorder.GetBorder()
	roundedBorder := lipgloss.RoundedBorder()

	// Compare corner characters (rounded uses ╭╮╰╯)
	if activeBorder.TopLeft != roundedBorder.TopLeft {
		t.Errorf("Expected rounded border TopLeft %q, got %q",
			roundedBorder.TopLeft, activeBorder.TopLeft)
	}
	if activeBorder.TopRight != roundedBorder.TopRight {
		t.Errorf("Expected rounded border TopRight %q, got %q",
			roundedBorder.TopRight, activeBorder.TopRight)
	}
}

// TestNewPaneStylesFromTheme tests creating styles from a Theme object.
func TestNewPaneStylesFromTheme(t *testing.T) {
	theme := NewTheme("dark")
	styles := NewPaneStyles(theme)

	if styles == nil {
		t.Fatal("NewPaneStyles returned nil")
		return
	}

	// Verify border is set - GetBorder returns (Border, top, right, bottom, left)
	activeBorder, _, _, _, _ := styles.ActiveBorder.GetBorder()
	if activeBorder == (lipgloss.Border{}) {
		t.Error("ActiveBorder should have a border")
	}
}
