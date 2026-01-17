package tui

import tea "github.com/charmbracelet/bubbletea"

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
	Error  string
}

// QueryResult holds query execution results.
type QueryResult struct {
	Columns []string
	Rows    [][]interface{}
	Message string
	Error   string
}
