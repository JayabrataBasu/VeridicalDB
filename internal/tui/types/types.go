// Package types defines shared types for the TUI package.
package types

import (
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// Screen represents a single screen in the TUI.
type Screen interface {
	// Init returns the initial command for the screen.
	Init() tea.Cmd

	// Update handles messages for the screen.
	Update(msg tea.Msg) (Screen, tea.Cmd)

	// View renders the screen.
	View() string
}

// ScreenChangeMsg requests a change to a different screen.
type ScreenChangeMsg struct {
	ScreenID string
}

// ErrorMsg represents an error message.
type ErrorMsg struct {
	Error string
}

// StatusMsg represents a status message.
type StatusMsg struct {
	Message string
}

// ExecuteQueryMsg requests query execution.
type ExecuteQueryMsg struct {
	SQL string
}

// QueryCompletedMsg signals query completion.
type QueryCompletedMsg struct {
	Result *QueryResult
	Error  error
}

// QueryResult holds query execution results.
type QueryResult struct {
	Columns []string
	Rows    [][]interface{}
	Message string
}

// StylePalette captures the shared styles used across screens.
type StylePalette struct {
	Title         lipgloss.Style
	Subtle        lipgloss.Style
	Highlight     lipgloss.Style
	Error         lipgloss.Style
	Help          lipgloss.Style
	SidebarHeader lipgloss.Style
	SidebarSub    lipgloss.Style
	Footer        lipgloss.Style
}

// StyleProvider exposes a palette for screens.
type StyleProvider interface {
	GetStyles() *StylePalette
}
