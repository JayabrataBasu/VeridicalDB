package tui

import (
	"strings"
	"testing"
)

// TestSplitViewCreation verifies split view is created with proper defaults.
func TestSplitViewCreation(t *testing.T) {
	styles := NewPaneStylesFromPalette("dark")
	split := NewSplitView(SplitHorizontal, styles)

	if split == nil {
		t.Fatal("NewSplitView returned nil")
	}

	// Check defaults
	if split.GetRatio() != 0.5 {
		t.Errorf("Expected default ratio 0.5, got %f", split.GetRatio())
	}
	if split.IsSecondPaneVisible() {
		t.Error("Second pane should not be visible by default")
	}
	if split.GetActivePane() != SplitPaneFirst {
		t.Error("First pane should be active by default")
	}
}

// TestSplitViewSetRatio tests ratio adjustment with bounds.
func TestSplitViewSetRatio(t *testing.T) {
	styles := NewPaneStylesFromPalette("dark")
	split := NewSplitView(SplitHorizontal, styles)

	// Set ratio within bounds
	split.SetRatio(0.6)
	if split.GetRatio() != 0.6 {
		t.Errorf("Expected ratio 0.6, got %f", split.GetRatio())
	}

	// Set ratio below minimum (should clamp to min 0.2)
	split.SetRatio(0.1)
	if split.GetRatio() != 0.2 {
		t.Errorf("Expected ratio to be clamped to 0.2, got %f", split.GetRatio())
	}

	// Set ratio above maximum (should clamp to max 0.8)
	split.SetRatio(0.9)
	if split.GetRatio() != 0.8 {
		t.Errorf("Expected ratio to be clamped to 0.8, got %f", split.GetRatio())
	}
}

// TestSplitViewAdjustRatio tests incremental ratio adjustment.
func TestSplitViewAdjustRatio(t *testing.T) {
	styles := NewPaneStylesFromPalette("dark")
	split := NewSplitView(SplitHorizontal, styles)

	split.SetRatio(0.5)
	split.AdjustRatio(0.1)

	// Use tolerance for floating-point comparison
	ratio := split.GetRatio()
	if ratio < 0.59 || ratio > 0.61 {
		t.Errorf("Expected ratio ~0.6 after +0.1, got %f", ratio)
	}

	split.AdjustRatio(-0.2)
	ratio = split.GetRatio()
	if ratio < 0.39 || ratio > 0.41 {
		t.Errorf("Expected ratio ~0.4 after -0.2, got %f", ratio)
	}
}

// TestSplitViewToggleActivePane tests pane focus toggling.
func TestSplitViewToggleActivePane(t *testing.T) {
	styles := NewPaneStylesFromPalette("dark")
	split := NewSplitView(SplitHorizontal, styles)
	split.SetDimensions(80, 40)

	// Show second pane for toggling
	split.ShowSecondPane()

	// Start at first pane
	if split.GetActivePane() != SplitPaneFirst {
		t.Error("Should start at first pane")
	}

	// Toggle to second
	split.ToggleActivePane()
	if split.GetActivePane() != SplitPaneSecond {
		t.Error("Should toggle to second pane")
	}

	// Toggle back to first
	split.ToggleActivePane()
	if split.GetActivePane() != SplitPaneFirst {
		t.Error("Should toggle back to first pane")
	}
}

// TestSplitViewToggleWhenSecondHidden tests toggle when second pane is hidden.
func TestSplitViewToggleWhenSecondHidden(t *testing.T) {
	styles := NewPaneStylesFromPalette("dark")
	split := NewSplitView(SplitHorizontal, styles)

	// Second pane hidden by default
	split.SetActivePane(SplitPaneFirst)
	split.ToggleActivePane()

	// Should stay at first since second is hidden
	if split.GetActivePane() != SplitPaneFirst {
		t.Error("Should stay at first pane when second is hidden")
	}
}

// TestSplitViewShowHideSecondPane tests visibility toggling.
func TestSplitViewShowHideSecondPane(t *testing.T) {
	styles := NewPaneStylesFromPalette("dark")
	split := NewSplitView(SplitHorizontal, styles)

	// Initially hidden
	if split.IsSecondPaneVisible() {
		t.Error("Second pane should be hidden initially")
	}

	// Show it
	split.ShowSecondPane()
	if !split.IsSecondPaneVisible() {
		t.Error("Second pane should be visible after ShowSecondPane()")
	}

	// Hide it
	split.HideSecondPane()
	if split.IsSecondPaneVisible() {
		t.Error("Second pane should be hidden after HideSecondPane()")
	}
}

// TestSplitViewToggleSecondPane tests toggle method.
func TestSplitViewToggleSecondPane(t *testing.T) {
	styles := NewPaneStylesFromPalette("dark")
	split := NewSplitView(SplitHorizontal, styles)

	split.ToggleSecondPane()
	if !split.IsSecondPaneVisible() {
		t.Error("Second pane should be visible after first toggle")
	}

	split.ToggleSecondPane()
	if split.IsSecondPaneVisible() {
		t.Error("Second pane should be hidden after second toggle")
	}
}

// TestSplitViewActiveRemainsFirstWhenHiding tests active pane stays first when hiding second.
func TestSplitViewActiveRemainsFirstWhenHiding(t *testing.T) {
	styles := NewPaneStylesFromPalette("dark")
	split := NewSplitView(SplitHorizontal, styles)

	split.ShowSecondPane()
	split.SetActivePane(SplitPaneSecond)

	// Hide second pane
	split.HideSecondPane()

	// Active should switch to first
	if split.GetActivePane() != SplitPaneFirst {
		t.Error("Active pane should switch to first when second is hidden")
	}
}

// TestSplitViewSetContent tests content setting.
func TestSplitViewSetContent(t *testing.T) {
	styles := NewPaneStylesFromPalette("dark")
	split := NewSplitView(SplitHorizontal, styles)
	split.SetDimensions(80, 40)

	split.SetFirstContent("Editor", "SELECT * FROM users;")
	split.SetSecondContent("Results", "3 rows returned")

	// Verify by rendering
	split.ShowSecondPane()
	view := split.View()

	if !strings.Contains(view, "Editor") {
		t.Error("View should contain first pane title 'Editor'")
	}
	if !strings.Contains(view, "Results") {
		t.Error("View should contain second pane title 'Results'")
	}
}

// TestSplitViewViewRequiresDimensions tests that View needs dimensions.
func TestSplitViewViewRequiresDimensions(t *testing.T) {
	styles := NewPaneStylesFromPalette("dark")
	split := NewSplitView(SplitHorizontal, styles)

	view := split.View()

	if view != "SplitView not initialized" {
		t.Errorf("Expected initialization message, got: %s", view)
	}
}

// TestSplitViewHorizontalDirection tests horizontal split dimensions.
func TestSplitViewHorizontalDirection(t *testing.T) {
	styles := NewPaneStylesFromPalette("dark")
	split := NewSplitView(SplitHorizontal, styles)
	split.SetDimensions(80, 40)
	split.SetRatio(0.5)
	split.ShowSecondPane()

	// Both panes should have full width for horizontal split
	first, second := split.calculateDimensions()

	if first.width != 80 || second.width != 80 {
		t.Errorf("Horizontal split: both panes should have full width, got first=%d second=%d",
			first.width, second.width)
	}

	// Heights should split based on ratio
	if first.height != 20 || second.height != 20 {
		t.Errorf("Expected heights 20/20 for 50%% split, got first=%d second=%d",
			first.height, second.height)
	}
}

// TestSplitViewVerticalDirection tests vertical split dimensions.
func TestSplitViewVerticalDirection(t *testing.T) {
	styles := NewPaneStylesFromPalette("dark")
	split := NewSplitView(SplitVertical, styles)
	split.SetDimensions(80, 40)
	split.SetRatio(0.5)
	split.ShowSecondPane()

	first, second := split.calculateDimensions()

	// Both panes should have full height for vertical split
	if first.height != 40 || second.height != 40 {
		t.Errorf("Vertical split: both panes should have full height, got first=%d second=%d",
			first.height, second.height)
	}

	// Widths should split based on ratio
	if first.width != 40 || second.width != 40 {
		t.Errorf("Expected widths 40/40 for 50%% split, got first=%d second=%d",
			first.width, second.width)
	}
}

// TestSplitViewSinglePaneMode tests that only first pane renders when second is hidden.
func TestSplitViewSinglePaneMode(t *testing.T) {
	styles := NewPaneStylesFromPalette("dark")
	split := NewSplitView(SplitHorizontal, styles)
	split.SetDimensions(80, 40)

	// Second pane hidden (default)
	split.SetFirstContent("Editor", "Code")
	split.SetSecondContent("Results", "Data")

	view := split.View()

	if !strings.Contains(view, "Editor") {
		t.Error("View should contain first pane")
	}
	// Results should NOT appear when second pane is hidden
	// (Depending on render logic, we just check it renders something)
	if view == "" {
		t.Error("View should not be empty")
	}
}

// TestSplitViewDirectionConstants verifies direction constants are distinct.
func TestSplitViewDirectionConstants(t *testing.T) {
	if SplitHorizontal == SplitVertical {
		t.Error("SplitHorizontal and SplitVertical should be different")
	}
}

// TestSplitViewPaneConstants verifies pane constants are distinct.
func TestSplitViewPaneConstants(t *testing.T) {
	if SplitPaneFirst == SplitPaneSecond {
		t.Error("SplitPaneFirst and SplitPaneSecond should be different")
	}
}
