// Package tui provides the Bubble Tea Terminal User Interface for VeridicalDB.
package tui

import (
	"github.com/charmbracelet/lipgloss"
)

// SplitDirection indicates the direction of the split.
type SplitDirection int

const (
	// SplitHorizontal splits top/bottom.
	SplitHorizontal SplitDirection = iota
	// SplitVertical splits left/right.
	SplitVertical
)

// SplitPane is the identifier for a pane in the split view.
type SplitPane int

const (
	// SplitPaneFirst is the first (top or left) pane.
	SplitPaneFirst SplitPane = iota
	// SplitPaneSecond is the second (bottom or right) pane.
	SplitPaneSecond
)

// SplitView manages a split view with two panes.
type SplitView struct {
	direction   SplitDirection
	ratio       float64 // Ratio of first pane (0.0 to 1.0)
	minRatio    float64
	maxRatio    float64
	activePane  SplitPane
	paneStyles  *PaneStyles
	totalWidth  int
	totalHeight int

	// Content and titles for each pane
	firstTitle    string
	firstContent  string
	secondTitle   string
	secondContent string

	// Whether the second pane is visible
	secondVisible bool
}

// NewSplitView creates a new split view.
func NewSplitView(direction SplitDirection, styles *PaneStyles) *SplitView {
	return &SplitView{
		direction:     direction,
		ratio:         0.5, // Default 50/50 split
		minRatio:      0.2,
		maxRatio:      0.8,
		activePane:    SplitPaneFirst,
		paneStyles:    styles,
		secondVisible: false,
		firstTitle:    "Editor",
		secondTitle:   "Results",
	}
}

// SetDimensions updates the total available dimensions.
func (s *SplitView) SetDimensions(width, height int) {
	s.totalWidth = width
	s.totalHeight = height
}

// SetRatio sets the split ratio (0.0 to 1.0).
func (s *SplitView) SetRatio(ratio float64) {
	if ratio < s.minRatio {
		ratio = s.minRatio
	}
	if ratio > s.maxRatio {
		ratio = s.maxRatio
	}
	s.ratio = ratio
}

// AdjustRatio adjusts the ratio by a delta.
func (s *SplitView) AdjustRatio(delta float64) {
	s.SetRatio(s.ratio + delta)
}

// SetActivePane sets which pane is currently active.
func (s *SplitView) SetActivePane(pane SplitPane) {
	s.activePane = pane
}

// GetActivePane returns the currently active pane.
func (s *SplitView) GetActivePane() SplitPane {
	return s.activePane
}

// ToggleActivePane switches between the two panes.
func (s *SplitView) ToggleActivePane() {
	if s.secondVisible {
		if s.activePane == SplitPaneFirst {
			s.activePane = SplitPaneSecond
		} else {
			s.activePane = SplitPaneFirst
		}
	}
}

// ShowSecondPane makes the second pane visible.
func (s *SplitView) ShowSecondPane() {
	s.secondVisible = true
}

// HideSecondPane hides the second pane.
func (s *SplitView) HideSecondPane() {
	s.secondVisible = false
	s.activePane = SplitPaneFirst
}

// IsSecondPaneVisible returns whether the second pane is visible.
func (s *SplitView) IsSecondPaneVisible() bool {
	return s.secondVisible
}

// ToggleSecondPane toggles visibility of the second pane.
func (s *SplitView) ToggleSecondPane() {
	s.secondVisible = !s.secondVisible
	if !s.secondVisible {
		s.activePane = SplitPaneFirst
	}
}

// SetFirstContent sets the content and title of the first pane.
func (s *SplitView) SetFirstContent(title, content string) {
	s.firstTitle = title
	s.firstContent = content
}

// SetSecondContent sets the content and title of the second pane.
func (s *SplitView) SetSecondContent(title, content string) {
	s.secondTitle = title
	s.secondContent = content
}

// calculateDimensions calculates dimensions for each pane.
func (s *SplitView) calculateDimensions() (first, second struct{ width, height int }) {
	if s.direction == SplitHorizontal {
		// Top/bottom split
		first.width = s.totalWidth
		second.width = s.totalWidth

		if s.secondVisible {
			first.height = int(float64(s.totalHeight) * s.ratio)
			second.height = s.totalHeight - first.height
		} else {
			first.height = s.totalHeight
			second.height = 0
		}
	} else {
		// Left/right split
		first.height = s.totalHeight
		second.height = s.totalHeight

		if s.secondVisible {
			first.width = int(float64(s.totalWidth) * s.ratio)
			second.width = s.totalWidth - first.width
		} else {
			first.width = s.totalWidth
			second.width = 0
		}
	}

	return first, second
}

// View renders the split view.
func (s *SplitView) View() string {
	if s.totalWidth == 0 || s.totalHeight == 0 {
		return "SplitView not initialized"
	}

	first, second := s.calculateDimensions()

	// Render first pane
	firstPane := s.renderPane(
		s.firstTitle,
		s.firstContent,
		first.width,
		first.height,
		s.activePane == SplitPaneFirst,
	)

	// If second pane not visible, return just the first
	if !s.secondVisible {
		return firstPane
	}

	// Render second pane
	secondPane := s.renderPane(
		s.secondTitle,
		s.secondContent,
		second.width,
		second.height,
		s.activePane == SplitPaneSecond,
	)

	// Join based on direction
	if s.direction == SplitHorizontal {
		return lipgloss.JoinVertical(lipgloss.Left, firstPane, secondPane)
	}
	return lipgloss.JoinHorizontal(lipgloss.Top, firstPane, secondPane)
}

// renderPane renders a single pane with styling.
func (s *SplitView) renderPane(title, content string, width, height int, active bool) string {
	if width <= 0 || height <= 0 {
		return ""
	}

	var borderStyle, titleStyle lipgloss.Style

	if active {
		borderStyle = s.paneStyles.ActiveBorder
		titleStyle = s.paneStyles.ActiveTitle
	} else {
		borderStyle = s.paneStyles.InactiveBorder
		titleStyle = s.paneStyles.InactiveTitle
	}

	// Calculate inner dimensions (accounting for border)
	innerWidth := width - 2
	innerHeight := height - 3 // Border + title

	if innerWidth < 1 {
		innerWidth = 1
	}
	if innerHeight < 1 {
		innerHeight = 1
	}

	// Build container with rounded border
	container := borderStyle.
		Width(innerWidth).
		Height(innerHeight).
		Padding(0, 1)

	// Build title bar
	titleBar := titleStyle.
		Width(innerWidth).
		Align(lipgloss.Center).
		Render(title)

	return lipgloss.JoinVertical(lipgloss.Left,
		titleBar,
		container.Render(content),
	)
}

// SetPaneStyles updates the pane styles.
func (s *SplitView) SetPaneStyles(styles *PaneStyles) {
	s.paneStyles = styles
}

// GetRatio returns the current split ratio.
func (s *SplitView) GetRatio() float64 {
	return s.ratio
}
