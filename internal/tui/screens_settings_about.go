package tui

import (
	"fmt"
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/internal/tui/types"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// SettingsScreen provides an interactive settings view.
type SettingsScreen struct {
	app *Model
}

// NewSettingsScreen creates a settings screen.
func NewSettingsScreen(app *Model) *SettingsScreen {
	return &SettingsScreen{app: app}
}

// Init initializes the settings screen.
func (s *SettingsScreen) Init() tea.Cmd {
	return nil
}

// Update handles settings interactions.
func (s *SettingsScreen) Update(msg tea.Msg) (Screen, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "esc", "q":
			return s, func() tea.Msg {
				return ScreenChangeMsg{ScreenID: "home"}
			}
		case "t", "enter":
			themeName := s.app.CycleTheme()
			s.app.SetStatus("Theme: " + themeName)
			return s, nil
		}
	}
	return s, nil
}

// View renders the settings screen.
func (s *SettingsScreen) View() string {
	var buf strings.Builder

	accent := "#00D9FF"
	muted := "#666666"
	highlight := "#BD00FF"
	if tm := s.app.GetThemeManager(); tm != nil {
		t := tm.Current()
		accent = t.BrandAccent
		muted = t.Muted
		highlight = t.BrandHighlight
	}

	headerStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(accent)).
		Padding(0, 2).
		MarginBottom(1)

	sectionTitle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(highlight))

	contentStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(muted)).
		Padding(0, 2)

	helpStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(muted)).
		MarginTop(2)

	currentTheme := "unknown"
	if tm := s.app.GetThemeManager(); tm != nil && tm.Current() != nil {
		currentTheme = tm.Current().Name
	}

	buf.WriteString(headerStyle.Render(types.Icons.Settings + "  Settings"))
	buf.WriteString("\n")
	buf.WriteString(sectionTitle.Render("Appearance"))
	buf.WriteString("\n")
	buf.WriteString(contentStyle.Render(fmt.Sprintf("Current theme: %s", currentTheme)))
	buf.WriteString("\n")
	buf.WriteString(contentStyle.Render("Press Enter or t to cycle theme"))
	buf.WriteString("\n\n")
	buf.WriteString(sectionTitle.Render("Session"))
	buf.WriteString("\n")
	buf.WriteString(contentStyle.Render("Use Ctrl+T anywhere to cycle theme quickly"))
	buf.WriteString("\n")
	buf.WriteString(contentStyle.Render("Use Ctrl+B to toggle sidebar"))
	buf.WriteString("\n")
	buf.WriteString(helpStyle.Render("Esc/q Return to menu"))

	return buf.String()
}

// AboutScreen provides project/about information.
type AboutScreen struct {
	app *Model
}

// NewAboutScreen creates an about screen.
func NewAboutScreen(app *Model) *AboutScreen {
	return &AboutScreen{app: app}
}

// Init initializes the about screen.
func (a *AboutScreen) Init() tea.Cmd {
	return nil
}

// Update handles about screen input.
func (a *AboutScreen) Update(msg tea.Msg) (Screen, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "esc", "q":
			return a, func() tea.Msg {
				return ScreenChangeMsg{ScreenID: "home"}
			}
		}
	}
	return a, nil
}

// View renders the about screen.
func (a *AboutScreen) View() string {
	var buf strings.Builder

	accent := "#00D9FF"
	muted := "#666666"
	highlight := "#BD00FF"
	if tm := a.app.GetThemeManager(); tm != nil {
		t := tm.Current()
		accent = t.BrandAccent
		muted = t.Muted
		highlight = t.BrandHighlight
	}

	headerStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(accent)).
		Padding(0, 2).
		MarginBottom(1)

	contentStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(muted)).
		Padding(0, 2)

	highlightStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(highlight)).
		Bold(true)

	helpStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(muted)).
		MarginTop(2)

	buf.WriteString(headerStyle.Render(types.Icons.About + "  About VeridicalDB"))
	buf.WriteString("\n")
	buf.WriteString(contentStyle.Render(highlightStyle.Render("VeridicalDB") + " is a Go-native database engine with SQL, MVCC, WAL, pgwire, and TUI support."))
	buf.WriteString("\n")
	buf.WriteString(contentStyle.Render("This terminal interface provides query execution, history, browser, monitoring, backup, and admin workflows."))
	buf.WriteString("\n\n")
	buf.WriteString(contentStyle.Render("Project: github.com/JayabrataBasu/VeridicalDB"))
	buf.WriteString("\n")
	buf.WriteString(contentStyle.Render("License: Apache-2.0"))
	buf.WriteString("\n")
	buf.WriteString(helpStyle.Render("Esc/q Return to menu"))

	return buf.String()
}
