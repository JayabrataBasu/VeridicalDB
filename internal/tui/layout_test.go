package tui

import (
	"testing"
)

// TestDefaultLayoutConfig verifies default configuration values.
func TestDefaultLayoutConfig(t *testing.T) {
	config := DefaultLayoutConfig()

	if config.SidebarWidth != 24 {
		t.Errorf("Expected SidebarWidth 24, got %d", config.SidebarWidth)
	}
	if config.AuxiliaryWidth != 40 {
		t.Errorf("Expected AuxiliaryWidth 40, got %d", config.AuxiliaryWidth)
	}
	if config.SidebarCollapsed {
		t.Error("Sidebar should not be collapsed by default")
	}
	if !config.AuxiliaryCollapsed {
		t.Error("Auxiliary should be collapsed by default")
	}
}

// TestLayoutCreation verifies layout is created with proper defaults.
func TestLayoutCreation(t *testing.T) {
	styles := NewPaneStylesFromPalette("dark")
	layout := NewLayout(nil, styles) // nil config uses defaults

	if layout == nil {
		t.Fatal("NewLayout returned nil")
	}

	config := layout.GetConfig()
	if config.SidebarWidth != 24 {
		t.Errorf("Expected default SidebarWidth 24, got %d", config.SidebarWidth)
	}
}

// TestLayoutPaneCycling tests cycling through active panes.
func TestLayoutPaneCycling(t *testing.T) {
	styles := NewPaneStylesFromPalette("dark")
	config := DefaultLayoutConfig()
	config.SidebarCollapsed = false
	config.AuxiliaryCollapsed = false

	layout := NewLayout(config, styles)
	layout.SetDimensions(120, 40)

	// Start at main
	layout.SetActivePane(PaneMain)
	if layout.GetActivePane() != PaneMain {
		t.Error("Expected active pane to be PaneMain")
	}

	// Cycle to auxiliary
	layout.CycleActivePane()
	if layout.GetActivePane() != PaneAuxiliary {
		t.Errorf("Expected active pane to be PaneAuxiliary after cycle, got %v", layout.GetActivePane())
	}

	// Cycle to sidebar
	layout.CycleActivePane()
	if layout.GetActivePane() != PaneSidebar {
		t.Errorf("Expected active pane to be PaneSidebar after cycle, got %v", layout.GetActivePane())
	}

	// Cycle back to main
	layout.CycleActivePane()
	if layout.GetActivePane() != PaneMain {
		t.Errorf("Expected active pane to be PaneMain after full cycle, got %v", layout.GetActivePane())
	}
}

// TestLayoutToggleSidebar tests sidebar toggle functionality.
func TestLayoutToggleSidebar(t *testing.T) {
	styles := NewPaneStylesFromPalette("dark")
	layout := NewLayout(nil, styles)
	layout.SetDimensions(120, 40)

	// Initially not collapsed
	if layout.IsSidebarCollapsed() {
		t.Error("Sidebar should not be collapsed initially")
	}

	// Toggle to collapse
	layout.ToggleSidebar()
	if !layout.IsSidebarCollapsed() {
		t.Error("Sidebar should be collapsed after toggle")
	}

	// Toggle to show
	layout.ToggleSidebar()
	if layout.IsSidebarCollapsed() {
		t.Error("Sidebar should be visible after second toggle")
	}
}

// TestLayoutToggleAuxiliary tests auxiliary pane toggle.
func TestLayoutToggleAuxiliary(t *testing.T) {
	styles := NewPaneStylesFromPalette("dark")
	layout := NewLayout(nil, styles)
	layout.SetDimensions(120, 40)

	// Initially collapsed (per default config)
	if !layout.IsAuxiliaryCollapsed() {
		t.Error("Auxiliary should be collapsed initially")
	}

	// Toggle to show
	layout.ToggleAuxiliary()
	if layout.IsAuxiliaryCollapsed() {
		t.Error("Auxiliary should be visible after toggle")
	}

	// Toggle to hide
	layout.ToggleAuxiliary()
	if !layout.IsAuxiliaryCollapsed() {
		t.Error("Auxiliary should be collapsed after second toggle")
	}
}

// TestLayoutShowHideAuxiliary tests explicit show/hide methods.
func TestLayoutShowHideAuxiliary(t *testing.T) {
	styles := NewPaneStylesFromPalette("dark")
	layout := NewLayout(nil, styles)
	layout.SetDimensions(120, 40)

	layout.ShowAuxiliary()
	if layout.IsAuxiliaryCollapsed() {
		t.Error("Auxiliary should be visible after ShowAuxiliary()")
	}

	layout.HideAuxiliary()
	if !layout.IsAuxiliaryCollapsed() {
		t.Error("Auxiliary should be hidden after HideAuxiliary()")
	}
}

// TestLayoutResizeSidebar tests sidebar resizing.
func TestLayoutResizeSidebar(t *testing.T) {
	styles := NewPaneStylesFromPalette("dark")
	layout := NewLayout(nil, styles)
	layout.SetDimensions(120, 40)

	originalWidth := layout.GetConfig().SidebarWidth

	// Increase width
	layout.ResizeSidebar(4)
	if layout.GetConfig().SidebarWidth != originalWidth+4 {
		t.Errorf("Expected sidebar width %d, got %d", originalWidth+4, layout.GetConfig().SidebarWidth)
	}

	// Decrease width
	layout.ResizeSidebar(-2)
	if layout.GetConfig().SidebarWidth != originalWidth+2 {
		t.Errorf("Expected sidebar width %d, got %d", originalWidth+2, layout.GetConfig().SidebarWidth)
	}
}

// TestLayoutResizeAuxiliary tests auxiliary pane resizing.
func TestLayoutResizeAuxiliary(t *testing.T) {
	styles := NewPaneStylesFromPalette("dark")
	layout := NewLayout(nil, styles)
	layout.SetDimensions(120, 40)

	originalWidth := layout.GetConfig().AuxiliaryWidth

	// Increase width
	layout.ResizeAuxiliary(5)
	if layout.GetConfig().AuxiliaryWidth != originalWidth+5 {
		t.Errorf("Expected auxiliary width %d, got %d", originalWidth+5, layout.GetConfig().AuxiliaryWidth)
	}
}

// TestLayoutResizeMinimumBounds tests that resize respects minimum bounds.
func TestLayoutResizeMinimumBounds(t *testing.T) {
	styles := NewPaneStylesFromPalette("dark")
	config := DefaultLayoutConfig()
	config.SidebarWidth = config.MinSidebarWidth + 2

	layout := NewLayout(config, styles)
	layout.SetDimensions(120, 40)

	// Try to shrink below minimum
	layout.ResizeSidebar(-10)
	if layout.GetConfig().SidebarWidth < config.MinSidebarWidth {
		t.Errorf("Sidebar width %d should not go below minimum %d",
			layout.GetConfig().SidebarWidth, config.MinSidebarWidth)
	}
}

// TestLayoutSetContent tests setting content for each pane.
func TestLayoutSetContent(t *testing.T) {
	styles := NewPaneStylesFromPalette("dark")
	layout := NewLayout(nil, styles)
	layout.SetDimensions(120, 40)

	layout.SetSidebarContent("Nav", "Menu items here")
	layout.SetMainContent("Editor", "SQL code here")
	layout.SetAuxiliaryContent("Results", "Query results here")

	// Content is set internally; we verify by rendering
	layout.ShowAuxiliary()
	view := layout.View()

	if view == "" {
		t.Error("Layout.View() should return non-empty string")
	}
}

// TestLayoutViewRequiresDimensions tests that View requires SetDimensions.
func TestLayoutViewRequiresDimensions(t *testing.T) {
	styles := NewPaneStylesFromPalette("dark")
	layout := NewLayout(nil, styles)

	// Don't set dimensions
	view := layout.View()

	if view != "Layout not initialized (call SetDimensions first)" {
		t.Errorf("Expected initialization error message, got: %s", view)
	}
}

// TestLayoutActivePaneSwitchOnCollapse tests that active pane switches when collapsed.
func TestLayoutActivePaneSwitchOnCollapse(t *testing.T) {
	styles := NewPaneStylesFromPalette("dark")
	config := DefaultLayoutConfig()
	config.AuxiliaryCollapsed = false

	layout := NewLayout(config, styles)
	layout.SetDimensions(120, 40)

	// Set auxiliary as active
	layout.SetActivePane(PaneAuxiliary)
	if layout.GetActivePane() != PaneAuxiliary {
		t.Error("Active pane should be PaneAuxiliary")
	}

	// Collapse auxiliary - should switch to main
	layout.ToggleAuxiliary()
	if layout.GetActivePane() != PaneMain {
		t.Errorf("Active pane should switch to PaneMain when auxiliary is collapsed, got %v", layout.GetActivePane())
	}
}
