// Package tui provides the Bubble Tea Terminal User Interface for VeridicalDB.
package tui

import (
	"fmt"
	"strings"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/internal/tui/theme"
	"github.com/charmbracelet/lipgloss"
)

// StatusBarMode represents the current interaction mode.
type StatusBarMode string

const (
	ModeNormal  StatusBarMode = "NORMAL"
	ModeInsert  StatusBarMode = "INSERT"
	ModeCommand StatusBarMode = "COMMAND"
	ModeSearch  StatusBarMode = "SEARCH"
)

// StatusBar renders a vim-style status bar at the bottom of the screen.
type StatusBar struct {
	// Current mode
	Mode StatusBarMode

	// Database connection info
	DatabaseName string
	Connected    bool

	// Performance metrics
	Latency time.Duration

	// Current screen/context
	Context string

	// Additional info slots
	Info string

	// Theme manager for styling
	themeManager *theme.Manager

	// Width of the status bar
	width int
}

// NewStatusBar creates a new status bar.
func NewStatusBar(tm *theme.Manager) *StatusBar {
	return &StatusBar{
		Mode:         ModeNormal,
		DatabaseName: "default",
		Connected:    true,
		Latency:      0,
		Context:      "",
		Info:         "",
		themeManager: tm,
		width:        80,
	}
}

// SetWidth sets the status bar width.
func (s *StatusBar) SetWidth(width int) {
	s.width = width
}

// SetMode sets the current mode.
func (s *StatusBar) SetMode(mode StatusBarMode) {
	s.Mode = mode
}

// SetDatabase sets the database connection info.
func (s *StatusBar) SetDatabase(name string, connected bool) {
	s.DatabaseName = name
	s.Connected = connected
}

// SetLatency sets the query latency.
func (s *StatusBar) SetLatency(latency time.Duration) {
	s.Latency = latency
}

// SetContext sets the current context/screen name.
func (s *StatusBar) SetContext(ctx string) {
	s.Context = ctx
}

// SetInfo sets additional info text.
func (s *StatusBar) SetInfo(info string) {
	s.Info = info
}

// View renders the status bar.
func (s *StatusBar) View() string {
	t := s.themeManager.Current()
	styles := t.Styles()

	// Mode indicator
	modeStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(t.Background)).
		Background(lipgloss.Color(t.Primary)).
		Padding(0, 1)

	switch s.Mode {
	case ModeInsert:
		modeStyle = modeStyle.Background(lipgloss.Color(t.Success))
	case ModeCommand:
		modeStyle = modeStyle.Background(lipgloss.Color(t.Warning))
	case ModeSearch:
		modeStyle = modeStyle.Background(lipgloss.Color(t.Info))
	}

	modeSection := modeStyle.Render(string(s.Mode))

	// Database section
	dbIcon := Icons.Connected
	dbStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.Success)).
		Padding(0, 1)
	if !s.Connected {
		dbIcon = Icons.Disconnected
		dbStyle = dbStyle.Foreground(lipgloss.Color(t.Error))
	}
	dbSection := dbStyle.Render(fmt.Sprintf("%s %s", dbIcon, s.DatabaseName))

	// Latency section
	latencyStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.Muted)).
		Padding(0, 1)
	latencyText := "--"
	if s.Latency > 0 {
		if s.Latency < 100*time.Millisecond {
			latencyStyle = latencyStyle.Foreground(lipgloss.Color(t.Success))
		} else if s.Latency < 500*time.Millisecond {
			latencyStyle = latencyStyle.Foreground(lipgloss.Color(t.Warning))
		} else {
			latencyStyle = latencyStyle.Foreground(lipgloss.Color(t.Error))
		}
		latencyText = fmt.Sprintf("%dms", s.Latency.Milliseconds())
	}
	latencySection := latencyStyle.Render(latencyText)

	// Help section
	helpStyle := styles.Muted
	helpStyle = helpStyle.Padding(0, 1)
	helpSection := helpStyle.Render("? Help")

	// Separator
	sep := lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.Border)).
		Render(" │ ")

	// Left side
	leftParts := []string{modeSection, sep, dbSection, sep, latencySection}
	if s.Context != "" {
		ctxStyle := lipgloss.NewStyle().
			Foreground(lipgloss.Color(t.Secondary)).
			Padding(0, 1)
		leftParts = append(leftParts, sep, ctxStyle.Render(s.Context))
	}
	left := strings.Join(leftParts, "")

	// Right side
	rightParts := []string{}
	if s.Info != "" {
		infoStyle := lipgloss.NewStyle().
			Foreground(lipgloss.Color(t.Muted)).
			Padding(0, 1)
		rightParts = append(rightParts, infoStyle.Render(s.Info))
		rightParts = append(rightParts, sep)
	}
	rightParts = append(rightParts, helpSection)
	right := strings.Join(rightParts, "")

	// Calculate spacing
	leftWidth := lipgloss.Width(left)
	rightWidth := lipgloss.Width(right)
	spacerWidth := s.width - leftWidth - rightWidth
	if spacerWidth < 0 {
		spacerWidth = 0
	}
	spacer := strings.Repeat(" ", spacerWidth)

	// Background style for entire bar
	barStyle := lipgloss.NewStyle().
		Background(lipgloss.Color(t.Border)).
		Width(s.width)

	return barStyle.Render(left + spacer + right)
}

// CompactView renders a minimal status bar for narrow terminals.
func (s *StatusBar) CompactView() string {
	t := s.themeManager.Current()

	// Mode indicator (short)
	modeStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(t.Background)).
		Background(lipgloss.Color(t.Primary)).
		Padding(0, 1)

	modeChar := "N"
	switch s.Mode {
	case ModeInsert:
		modeChar = "I"
		modeStyle = modeStyle.Background(lipgloss.Color(t.Success))
	case ModeCommand:
		modeChar = "C"
		modeStyle = modeStyle.Background(lipgloss.Color(t.Warning))
	case ModeSearch:
		modeChar = "/"
		modeStyle = modeStyle.Background(lipgloss.Color(t.Info))
	}

	// DB status
	dbIcon := Icons.Connected
	if !s.Connected {
		dbIcon = Icons.Disconnected
	}

	left := modeStyle.Render(modeChar)

	sep := lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.Border)).
		Render(" │ ")

	dbStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(t.Foreground))

	right := dbStyle.Render(fmt.Sprintf("%s %s", dbIcon, s.DatabaseName))

	barStyle := lipgloss.NewStyle().
		Background(lipgloss.Color(t.Border)).
		Width(s.width)

	spacerWidth := s.width - lipgloss.Width(left) - lipgloss.Width(sep) - lipgloss.Width(right)
	if spacerWidth < 0 {
		spacerWidth = 0
	}

	return barStyle.Render(left + sep + right + strings.Repeat(" ", spacerWidth))
}
