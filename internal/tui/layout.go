// Package tui provides the Bubble Tea Terminal User Interface for VeridicalDB.
package tui

import (
	"github.com/charmbracelet/lipgloss"
)

// LayoutPane identifies which pane in the layout.
type LayoutPane int

const (
	// PaneSidebar is the left navigation pane.
	PaneSidebar LayoutPane = iota
	// PaneMain is the central main content pane.
	PaneMain
	// PaneAuxiliary is the right auxiliary pane (results/logs).
	PaneAuxiliary
)

// LayoutConfig holds the configuration for the 3-pane layout.
type LayoutConfig struct {
	// SidebarWidth is the width of the sidebar (default 20).
	SidebarWidth int
	// AuxiliaryWidth is the width of the auxiliary pane (default 40).
	AuxiliaryWidth int
	// SidebarCollapsed hides the sidebar when true.
	SidebarCollapsed bool
	// AuxiliaryCollapsed hides the auxiliary pane when true.
	AuxiliaryCollapsed bool
	// MinSidebarWidth is the minimum width for the sidebar.
	MinSidebarWidth int
	// MinAuxiliaryWidth is the minimum width for the auxiliary pane.
	MinAuxiliaryWidth int
	// MinMainWidth is the minimum width for the main pane.
	MinMainWidth int
}

// DefaultLayoutConfig returns the default layout configuration.
func DefaultLayoutConfig() *LayoutConfig {
	return &LayoutConfig{
		SidebarWidth:       24,
		AuxiliaryWidth:     40,
		SidebarCollapsed:   false,
		AuxiliaryCollapsed: true, // Start with auxiliary collapsed
		MinSidebarWidth:    16,
		MinAuxiliaryWidth:  30,
		MinMainWidth:       40,
	}
}

// Layout manages a 3-pane layout: Sidebar | Main | Auxiliary.
type Layout struct {
	config      *LayoutConfig
	activePane  LayoutPane
	paneStyles  *PaneStyles
	totalWidth  int
	totalHeight int

	// Content for each pane
	sidebarContent   string
	sidebarTitle     string
	mainContent      string
	mainTitle        string
	auxiliaryContent string
	auxiliaryTitle   string
}

// NewLayout creates a new 3-pane layout.
func NewLayout(config *LayoutConfig, styles *PaneStyles) *Layout {
	if config == nil {
		config = DefaultLayoutConfig()
	}
	return &Layout{
		config:         config,
		activePane:     PaneMain,
		paneStyles:     styles,
		sidebarTitle:   "Navigation",
		mainTitle:      "Main",
		auxiliaryTitle: "Output",
	}
}

// SetDimensions updates the total available dimensions.
func (l *Layout) SetDimensions(width, height int) {
	l.totalWidth = width
	l.totalHeight = height
}

// SetActivePane sets which pane is currently active/focused.
func (l *Layout) SetActivePane(pane LayoutPane) {
	l.activePane = pane
}

// GetActivePane returns the currently active pane.
func (l *Layout) GetActivePane() LayoutPane {
	return l.activePane
}

// CycleActivePane cycles through the visible panes.
func (l *Layout) CycleActivePane() {
	switch l.activePane {
	case PaneSidebar:
		l.activePane = PaneMain
	case PaneMain:
		if !l.config.AuxiliaryCollapsed {
			l.activePane = PaneAuxiliary
		} else if !l.config.SidebarCollapsed {
			l.activePane = PaneSidebar
		}
	case PaneAuxiliary:
		if !l.config.SidebarCollapsed {
			l.activePane = PaneSidebar
		} else {
			l.activePane = PaneMain
		}
	}
}

// ToggleSidebar toggles the sidebar visibility.
func (l *Layout) ToggleSidebar() {
	l.config.SidebarCollapsed = !l.config.SidebarCollapsed
	// If we collapsed the active pane, switch to main
	if l.config.SidebarCollapsed && l.activePane == PaneSidebar {
		l.activePane = PaneMain
	}
}

// ToggleAuxiliary toggles the auxiliary pane visibility.
func (l *Layout) ToggleAuxiliary() {
	l.config.AuxiliaryCollapsed = !l.config.AuxiliaryCollapsed
	// If we collapsed the active pane, switch to main
	if l.config.AuxiliaryCollapsed && l.activePane == PaneAuxiliary {
		l.activePane = PaneMain
	}
}

// ShowAuxiliary ensures the auxiliary pane is visible.
func (l *Layout) ShowAuxiliary() {
	l.config.AuxiliaryCollapsed = false
}

// HideAuxiliary hides the auxiliary pane.
func (l *Layout) HideAuxiliary() {
	l.config.AuxiliaryCollapsed = true
	if l.activePane == PaneAuxiliary {
		l.activePane = PaneMain
	}
}

// IsSidebarCollapsed returns whether the sidebar is collapsed.
func (l *Layout) IsSidebarCollapsed() bool {
	return l.config.SidebarCollapsed
}

// IsAuxiliaryCollapsed returns whether the auxiliary pane is collapsed.
func (l *Layout) IsAuxiliaryCollapsed() bool {
	return l.config.AuxiliaryCollapsed
}

// ResizeSidebar adjusts the sidebar width by delta.
func (l *Layout) ResizeSidebar(delta int) {
	newWidth := l.config.SidebarWidth + delta
	if newWidth >= l.config.MinSidebarWidth && newWidth <= l.totalWidth/3 {
		l.config.SidebarWidth = newWidth
	}
}

// ResizeAuxiliary adjusts the auxiliary pane width by delta.
func (l *Layout) ResizeAuxiliary(delta int) {
	newWidth := l.config.AuxiliaryWidth + delta
	if newWidth >= l.config.MinAuxiliaryWidth && newWidth <= l.totalWidth/2 {
		l.config.AuxiliaryWidth = newWidth
	}
}

// SetSidebarContent sets the sidebar content and title.
func (l *Layout) SetSidebarContent(title, content string) {
	l.sidebarTitle = title
	l.sidebarContent = content
}

// SetMainContent sets the main pane content and title.
func (l *Layout) SetMainContent(title, content string) {
	l.mainTitle = title
	l.mainContent = content
}

// SetAuxiliaryContent sets the auxiliary pane content and title.
func (l *Layout) SetAuxiliaryContent(title, content string) {
	l.auxiliaryTitle = title
	l.auxiliaryContent = content
}

// calculatePaneWidths calculates the width for each visible pane.
func (l *Layout) calculatePaneWidths() (sidebarW, mainW, auxW int) {
	available := l.totalWidth

	// Sidebar width
	if !l.config.SidebarCollapsed {
		sidebarW = l.config.SidebarWidth
		available -= sidebarW
	}

	// Auxiliary width
	if !l.config.AuxiliaryCollapsed {
		auxW = l.config.AuxiliaryWidth
		available -= auxW
	}

	// Main gets the rest
	mainW = available
	if mainW < l.config.MinMainWidth {
		mainW = l.config.MinMainWidth
	}

	return sidebarW, mainW, auxW
}

// View renders the complete 3-pane layout.
func (l *Layout) View() string {
	if l.totalWidth == 0 || l.totalHeight == 0 {
		return "Layout not initialized (call SetDimensions first)"
	}

	sidebarW, mainW, auxW := l.calculatePaneWidths()
	contentHeight := l.totalHeight - 2 // Reserve space for status bar

	var panes []string

	// Render sidebar if visible
	if !l.config.SidebarCollapsed && sidebarW > 0 {
		panes = append(panes, l.renderPane(
			l.sidebarTitle,
			l.sidebarContent,
			sidebarW,
			contentHeight,
			l.activePane == PaneSidebar,
		))
	}

	// Render main pane (always visible)
	panes = append(panes, l.renderPane(
		l.mainTitle,
		l.mainContent,
		mainW,
		contentHeight,
		l.activePane == PaneMain,
	))

	// Render auxiliary pane if visible
	if !l.config.AuxiliaryCollapsed && auxW > 0 {
		panes = append(panes, l.renderPane(
			l.auxiliaryTitle,
			l.auxiliaryContent,
			auxW,
			contentHeight,
			l.activePane == PaneAuxiliary,
		))
	}

	return lipgloss.JoinHorizontal(lipgloss.Top, panes...)
}

// renderPane renders a single pane with the appropriate styling.
func (l *Layout) renderPane(title, content string, width, height int, active bool) string {
	var borderStyle, titleStyle lipgloss.Style

	if active {
		borderStyle = l.paneStyles.ActiveBorder
		titleStyle = l.paneStyles.ActiveTitle
	} else {
		borderStyle = l.paneStyles.InactiveBorder
		titleStyle = l.paneStyles.InactiveTitle
	}

	// Build the bordered container
	container := borderStyle.
		Width(width-2).
		Height(height-3). // Account for border and title
		Padding(0, 1)

	// Render title
	titleBar := titleStyle.
		Width(width - 2).
		Align(lipgloss.Center).
		Render(title)

	// Combine title and content
	return lipgloss.JoinVertical(lipgloss.Left,
		titleBar,
		container.Render(content),
	)
}

// GetConfig returns the current layout configuration.
func (l *Layout) GetConfig() *LayoutConfig {
	return l.config
}

// SetPaneStyles updates the pane styles.
func (l *Layout) SetPaneStyles(styles *PaneStyles) {
	l.paneStyles = styles
}
