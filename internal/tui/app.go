// Package tui provides the Bubble Tea Terminal User Interface for VeridicalDB.
package tui

import (
	"fmt"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/internal/tui/screens"
	"github.com/JayabrataBasu/VeridicalDB/internal/tui/theme"
	"github.com/JayabrataBasu/VeridicalDB/internal/tui/types"
	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// Model represents the main TUI application state.
type Model struct {
	// Current screen and its registry id
	screen   Screen
	screenID string

	// Shared session for database operations
	session Session

	// Theme configuration (legacy, for compatibility)
	theme *Theme

	// Theme manager for advanced theming
	themeManager *theme.Manager

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

	// Status bar
	statusBar *StatusBar

	// Global help overlay
	helpOverlay *screens.HelpOverlay

	// Window dimensions
	width  int
	height int

	// Stats from the most recent query, surfaced in the status bar.
	lastQueryRows     int
	lastQueryDuration time.Duration
	lastQueryOK       bool

	// Available theme names for cycling
	themeNames []string
	themeIndex int
}

// New creates a new TUI Model.
func New(session Session) *Model {
	// Initialize icons based on environment
	InitIcons()

	// Initialize theme manager
	themeManager := theme.NewManager()
	themeNames := themeManager.ListThemes()

	// Legacy theme for backward compatibility
	legacyTheme := NewTheme("dark")

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

	// Create pane styles using theme colors - NO backgrounds
	t := themeManager.Current()
	paneStyles := &PaneStyles{
		ActiveBorder: lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color(t.BorderFocused)),
		InactiveBorder: lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color(t.Border)),
		ActiveTitle: lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color(t.Primary)).
			Padding(0, 1),
		InactiveTitle: lipgloss.NewStyle().
			Foreground(lipgloss.Color(t.Muted)).
			Padding(0, 1),
		ActiveBackground:   lipgloss.NewStyle(),
		InactiveBackground: lipgloss.NewStyle(),
	}

	// Create status bar
	statusBar := NewStatusBar(themeManager)

	m := &Model{
		session:      session,
		screenID:     "home",
		theme:        legacyTheme,
		themeManager: themeManager,
		screens:      make(map[string]Screen),
		msgChan:      make(chan tea.Msg, 100),
		quitting:     false,
		layout:       NewLayout(layoutConfig, paneStyles),
		statusBar:    statusBar,
		width:        120,
		height:       40,
		themeNames:   themeNames,
		themeIndex:   0,
		helpOverlay:  screens.NewHelpOverlay(),
	}

	// Prime overlay dimensions with initial viewport.
	if m.helpOverlay != nil {
		_ = m.helpOverlay.Update(tea.WindowSizeMsg{Width: m.width, Height: m.height})
	}

	// Initialize screens
	homeScreen := NewHomeScreen(m)
	editorScreen := screens.NewEditorScreen(m)
	resultsScreen := screens.NewResultsScreen(m)
	historyScreen := screens.NewQueryHistoryScreen(m)

	m.screens["home"] = homeScreen
	m.screens["editor"] = editorScreen
	m.screens["results"] = resultsScreen
	m.screens["history"] = historyScreen

	// Initialize real feature screens with adapters
	m.screens["browser"] = NewDatabaseBrowserAdapter(m, 120, 40)
	m.screens["monitoring"] = NewDashboardAdapter(m)

	// Feature screens
	m.screens["users"] = screens.NewUsersScreen(m)
	m.screens["backup"] = screens.NewBackupScreen(m)
	m.screens["settings"] = NewSettingsScreen(m)
	m.screens["about"] = NewAboutScreen(m)

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
		m.statusBar.SetWidth(msg.Width)
		if m.helpOverlay != nil {
			_ = m.helpOverlay.Update(msg)
		}
		// Screens get the full width; the shell reserves 2 lines for the top bar
		// (header + hairline) and 1 for the status bar.
		screenMsg := tea.WindowSizeMsg{
			Width:  msg.Width,
			Height: msg.Height - 3,
		}
		if m.screen != nil {
			newScreen, cmd := m.screen.Update(screenMsg)
			m.screen = newScreen
			return m, cmd
		}
		return m, nil

	case tea.KeyMsg:
		if m.helpOverlay != nil && m.helpOverlay.IsVisible() {
			cmd := m.helpOverlay.Update(msg)
			return m, cmd
		}

		switch msg.String() {
		case "ctrl+c", "ctrl+q":
			m.quitting = true
			return m, tea.Quit

		case "ctrl+t":
			// Cycle through themes
			themeName := m.CycleTheme()
			m.statusMessage = "Theme: " + themeName
			return m, nil

		case "ctrl+l":
			// Clear screen
			return m, tea.ClearScreen

		case "f1", "ctrl+h", "?":
			if m.helpOverlay != nil {
				m.helpOverlay.Show()
				m.statusMessage = ""
			}
			return m, nil
		}

	case ScreenChangeMsg:
		// Switch to new screen
		if screen, ok := m.screens[msg.ScreenID]; ok {
			m.screen = screen
			m.screenID = msg.ScreenID
			return m, screen.Init()
		}
		m.lastError = "Screen not found: " + msg.ScreenID
		return m, nil

	case screens.ExecuteQueryMsg:
		// Execute query asynchronously
		m.statusMessage = "Running query…"
		return m, m.executeQuery(msg.SQL)

	case screens.QueryCompletedMsg:
		// Record stats and update the results screen.
		m.lastQueryDuration = msg.Duration
		if msg.Error != nil {
			m.lastError = msg.Error.Error()
			m.lastQueryOK = false
			m.lastQueryRows = 0
			m.statusMessage = "Query failed"
		} else {
			m.lastError = ""
			m.lastQueryOK = true
			if msg.Result != nil {
				m.lastQueryRows = len(msg.Result.Rows)
			}
			if resultsScreen, ok := m.screens["results"].(*screens.ResultsScreen); ok {
				resultsScreen.SetResult(msg.Result)
				resultsScreen.SetElapsed(msg.Duration)
			}
			m.statusMessage = msg.Summary()
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

	if m.helpOverlay != nil && m.helpOverlay.IsVisible() {
		return m.helpOverlay.View()
	}

	if m.screen == nil {
		return "No screen active"
	}

	width := m.width
	if width < 40 {
		width = 40
	}

	topBar := m.renderTopBar(width)

	// The screen renders itself at the width/height it was handed via
	// WindowSizeMsg. Do NOT re-wrap it (that corrupts embedded ANSI codes).
	mainContent := m.screen.View()

	// Status bar state.
	m.statusBar.SetDatabase(m.currentDBName(), m.session != nil)
	if m.lastQueryOK || m.lastError != "" {
		m.statusBar.SetQueryStats(m.lastQueryRows, m.lastQueryDuration, m.lastError == "")
	} else {
		m.statusBar.ClearQueryStats()
	}
	if m.lastError != "" {
		m.statusBar.SetInfo(m.lastError)
	} else {
		m.statusBar.SetInfo(m.statusMessage)
	}

	return lipgloss.JoinVertical(lipgloss.Left, topBar, mainContent, m.statusBar.View())
}

// RegisterScreen registers a screen in the TUI.
func (m *Model) RegisterScreen(id string, screen Screen) {
	m.screens[id] = screen
}

// GetSession returns the shared database session.
func (m *Model) GetSession() Session {
	return m.session
}

// GetTheme returns the current theme (legacy compatibility).
func (m *Model) GetTheme() *Theme {
	return m.theme
}

// GetThemeManager returns the theme manager.
func (m *Model) GetThemeManager() *theme.Manager {
	return m.themeManager
}

// CycleTheme advances to the next available theme and returns its name.
func (m *Model) CycleTheme() string {
	if len(m.themeNames) == 0 {
		return ""
	}
	m.themeIndex = (m.themeIndex + 1) % len(m.themeNames)
	themeName := m.themeNames[m.themeIndex]
	m.themeManager.SetTheme(themeName)
	return themeName
}

// GetStyles returns the shared style palette for screens.
func (m *Model) GetStyles() *types.StylePalette {
	if m.theme == nil {
		return nil
	}
	return m.theme.Palette()
}

// SetTheme updates the theme (legacy compatibility).
func (m *Model) SetTheme(t *Theme) {
	m.theme = t
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

// executeQuery executes a SQL query asynchronously, timing the round trip.
func (m *Model) executeQuery(sqlQuery string) tea.Cmd {
	session := m.session
	return func() tea.Msg {
		if session == nil {
			return screens.QueryCompletedMsg{Error: fmt.Errorf("no database session")}
		}
		start := time.Now()
		result, err := session.ExecuteSQL(sqlQuery)
		elapsed := time.Since(start)

		if err != nil {
			return screens.QueryCompletedMsg{Error: err, Duration: elapsed}
		}
		return screens.QueryCompletedMsg{
			Result: &screens.QueryResult{
				Columns: result.Columns,
				Rows:    convertRows(result.Rows),
				Message: result.Message,
			},
			Duration: elapsed,
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

// GetWidth returns the current terminal width
func (m *Model) GetWidth() int {
	return m.width
}
