// Package screens provides TUI screen implementations.
package screens

import "github.com/charmbracelet/lipgloss"

// Theme represents the visual theme for screens.
// This is a simplified local theme to avoid circular dependencies.
type Theme struct {
	Title     lipgloss.Style
	Subtle    lipgloss.Style
	Highlight lipgloss.Style
	Error     lipgloss.Style
	Help      lipgloss.Style
}

// NewDarkTheme creates a dark theme for screens.
func NewDarkTheme() *Theme {
	return &Theme{
		Title:     lipgloss.NewStyle().Foreground(lipgloss.Color("#00FFFF")).Bold(true),
		Subtle:    lipgloss.NewStyle().Foreground(lipgloss.Color("#666666")),
		Highlight: lipgloss.NewStyle().Foreground(lipgloss.Color("#FFFF00")),
		Error:     lipgloss.NewStyle().Foreground(lipgloss.Color("#FF0000")),
		Help:      lipgloss.NewStyle().Foreground(lipgloss.Color("#888888")),
	}
}
