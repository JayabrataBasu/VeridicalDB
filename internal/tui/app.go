// Package tui provides the Bubble Tea Terminal User Interface for VeridicalDB.
package tui

import (
	"github.com/JayabrataBasu/VeridicalDB/internal/tui/screens"
	"github.com/JayabrataBasu/VeridicalDB/internal/tui/types"
	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// Model represents the main TUI application state.
type Model struct {
	// Current screen
	screen Screen

	// Shared session for database operations
	session *sql.Session

	// Theme configuration
	theme *Theme

	// Screen registry
	screens map[string]Screen

	// Message channel for async operations
	msgChan chan tea.Msg

	// Application quit flag
	quitting bool

	// Last error message
	lastError string

	// Status message
	statusMessage string

	// Layout manager for 3-pane layout
	layout *Layout

	// Window dimensions
	width  int
	height int

	// Sidebar collapsed state
	sidebarCollapsed bool
}

// New creates a new TUI Model.
func New(session *sql.Session) *Model {
	theme := NewTheme("dark")

	// Configure compact sidebar layout
	layoutConfig := &LayoutConfig{
		SidebarWidth:       20, // Compact sidebar
		AuxiliaryWidth:     40,
		SidebarCollapsed:   false,
		AuxiliaryCollapsed: true,
		MinSidebarWidth:    14,
		MinAuxiliaryWidth:  30,
		MinMainWidth:       50,
	}

	// Create pane styles for premium look
	paneStyles := &PaneStyles{
		ActiveBorder: lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("#00D9FF")),
		InactiveBorder: lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("#3a3a5c")),
		ActiveTitle: lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#00D9FF")).
			Background(lipgloss.Color("#1a1a2e")).
			Padding(0, 1),
		InactiveTitle: lipgloss.NewStyle().
			Foreground(lipgloss.Color("#888888")).
			Padding(0, 1),
		ActiveBackground: lipgloss.NewStyle(),
		InactiveBackground: lipgloss.NewStyle(),
	}

	m := &Model{
		session:          session,
		theme:            theme,
		screens:          make(map[string]Screen),
		msgChan:          make(chan tea.Msg, 100),
		quitting:         false,
		layout:           NewLayout(layoutConfig, paneStyles),
		width:            120,
		height:           40,
		sidebarCollapsed: false,
	}

	// Initialize screens
	homeScreen := NewHomeScreen(m)
	editorScreen := screens.NewEditorScreen(m)
	resultsScreen := screens.NewResultsScreen(m)

	m.screens["home"] = homeScreen
	m.screens["editor"] = editorScreen
	m.screens["results"] = resultsScreen

	// Initialize real feature screens with adapters
	m.screens["browser"] = NewDatabaseBrowserAdapter(m, 120, 40)
	m.screens["monitoring"] = NewDashboardAdapter(m)

	// Placeholder screens for not-yet-implemented features
	m.screens["users"] = NewPlaceholderScreen(m, "User Management")
	m.screens["backup"] = NewPlaceholderScreen(m, "Backup & Restore")
	m.screens["settings"] = NewPlaceholderScreen(m, "Settings")
	m.screens["about"] = NewPlaceholderScreen(m, "About")

	m.screen = homeScreen

	return m
}

// Init initializes the model and returns an initial command.
func (m *Model) Init() tea.Cmd {
	return nil
}

// Update handles messages and updates the model.
func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		// Update dimensions for proper layout
		m.width = msg.Width
		m.height = msg.Height
		m.layout.SetDimensions(msg.Width, msg.Height)
		return m, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "ctrl+q":
			m.quitting = true
			return m, tea.Quit

		case "ctrl+b":
			// Toggle sidebar visibility
			m.sidebarCollapsed = !m.sidebarCollapsed
			m.layout.ToggleSidebar()
			return m, nil

		case "ctrl+l":
			// Clear screen
			return m, tea.ClearScreen

		case "f1", "ctrl+h":
			// Show help (TODO)
			m.statusMessage = "Help not yet implemented"
			return m, nil
		}

	case ScreenChangeMsg:
		// Switch to new screen
		if screen, ok := m.screens[msg.ScreenID]; ok {
			m.screen = screen
			return m, screen.Init()
		}
		m.lastError = "Screen not found: " + msg.ScreenID
		return m, nil

	case screens.ExecuteQueryMsg:
		// Execute query asynchronously
		return m, m.executeQuery(msg.SQL)

	case screens.QueryCompletedMsg:
		// Update results screen with query result
		if resultsScreen, ok := m.screens["results"].(*screens.ResultsScreen); ok {
			if msg.Error != nil {
				m.lastError = msg.Error.Error()
				m.statusMessage = "Query failed"
			} else {
				resultsScreen.SetResult(msg.Result)
				m.statusMessage = "Query executed successfully"
			}
		}
		// Forward to current screen
		if m.screen != nil {
			newScreen, cmd := m.screen.Update(msg)
			m.screen = newScreen
			return m, cmd
		}
		return m, nil

	case ErrorMsg:
		m.lastError = msg.Error
		return m, nil

	case StatusMsg:
		m.statusMessage = msg.Message
		return m, nil
	}

	// Delegate to current screen
	if m.screen != nil {
		newScreen, cmd := m.screen.Update(msg)
		m.screen = newScreen
		return m, cmd
	}

	return m, nil
}

// View renders the TUI.
func (m *Model) View() string {
	if m.quitting {
		return ""
	}

	if m.screen == nil {
		return "No screen active"
	}

	// Calculate available space
	sidebarWidth := 20
	if m.sidebarCollapsed {
		sidebarWidth = 0
	}

	// Main content with proper width constraint
	mainWidth := m.width - sidebarWidth - 2 // 2 for border/spacing
	if mainWidth < 50 {
		mainWidth = 50
	}

	// Style main content area with breathing room
	mainContentStyle := lipgloss.NewStyle().
		Width(mainWidth).
		Padding(1, 2).
		MarginRight(1)

	mainContent := mainContentStyle.Render(m.screen.View())

	// Render sidebar if not collapsed
	var layout string
	if !m.sidebarCollapsed {
		side := NewSidebar(m)
		sidebarContent := side.View(sidebarWidth)
		layout = lipgloss.JoinHorizontal(lipgloss.Top, mainContent, sidebarContent)
	} else {
		layout = mainContent
	}

	// Status bar with better styling
	status := ""
	if m.lastError != "" {
		status = m.theme.ErrorStyle.Render("✗ " + m.lastError)
	} else if m.statusMessage != "" {
		status = m.theme.PrimaryStyle.Render("✓ " + m.statusMessage)
	} else {
		status = m.theme.SecondaryStyle.Render("Ready")
	}

	// Footer with toggle hint
	toggleHint := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#666666")).
		Render(" │ Ctrl+B Toggle Sidebar")

	footerStyle := lipgloss.NewStyle().
		Background(lipgloss.Color("#101010")).
		Foreground(lipgloss.Color("#00D9FF")).
		Padding(0, 1).
		Width(m.width)

	footer := footerStyle.Render(" " + status + toggleHint)

	return lipgloss.JoinVertical(lipgloss.Left, layout, footer)
}

// RegisterScreen registers a screen in the TUI.
func (m *Model) RegisterScreen(id string, screen Screen) {
	m.screens[id] = screen
}

// GetSession returns the shared database session.
func (m *Model) GetSession() *sql.Session {
	return m.session
}

// GetTheme returns the current theme.
func (m *Model) GetTheme() *Theme {
	return m.theme
}

// GetStyles returns the shared style palette for screens.
func (m *Model) GetStyles() *types.StylePalette {
	if m.theme == nil {
		return nil
	}
	return m.theme.Palette()
}

// SetTheme updates the theme.
func (m *Model) SetTheme(theme *Theme) {
	m.theme = theme
}

// ShowError displays an error message.
func (m *Model) ShowError(err string) {
	m.lastError = err
}

// GetLastError returns the last error message.
func (m *Model) GetLastError() string {
	return m.lastError
}

// ClearError clears the last error message.
func (m *Model) ClearError() {
	m.lastError = ""
}

// SetStatus updates the status message.
func (m *Model) SetStatus(msg string) {
	m.statusMessage = msg
}

// GetStatus returns the current status message.
func (m *Model) GetStatus() string {
	return m.statusMessage
}

// executeQuery executes a SQL query asynchronously.
func (m *Model) executeQuery(sqlQuery string) tea.Cmd {
	return func() tea.Msg {
		// Execute query using session
		result, err := m.session.ExecuteSQL(sqlQuery)

		if err != nil {
			return screens.QueryCompletedMsg{
				Result: nil,
				Error:  err,
			}
		}

		// Convert result to QueryResult format
		queryResult := &screens.QueryResult{
			Columns: result.Columns,
			Rows:    convertRows(result.Rows),
			Message: result.Message,
		}

		return screens.QueryCompletedMsg{
			Result: queryResult,
			Error:  nil,
		}
	}
}

// convertRows converts catalog.Value rows to interface{} rows for display.
func convertRows(catalogRows [][]catalog.Value) [][]interface{} {
	if catalogRows == nil {
		return nil
	}

	rows := make([][]interface{}, len(catalogRows))
	for i, catalogRow := range catalogRows {
		rows[i] = make([]interface{}, len(catalogRow))
		for j, val := range catalogRow {
			rows[i][j] = convertValue(val)
		}
	}
	return rows
}

// convertValue converts a catalog.Value to interface{} for display.
func convertValue(v catalog.Value) interface{} {
	if v.IsNull {
		return nil
	}

	switch v.Type {
	case catalog.TypeInt32:
		return v.Int32
	case catalog.TypeInt64:
		return v.Int64
	case catalog.TypeFloat64:
		return v.Float64
	case catalog.TypeText:
		return v.Text
	case catalog.TypeBool:
		return v.Bool
	case catalog.TypeTimestamp:
		return v.Timestamp
	case catalog.TypeJSON:
		return v.JSON
	default:
		return nil
	}
}
