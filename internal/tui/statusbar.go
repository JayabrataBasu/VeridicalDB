// Package tui provides the Bubble Tea Terminal User Interface for VeridicalDB.
package tui

import (
	"fmt"
	"os/exec"
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

// View renders the status bar with Powerline styling.
func (s *StatusBar) View() string {
	// Powerline arrow separators
	powerlineRight := ""
	powerlineRightThin := ""

	// Section 1: Mode (Green background for NORMAL, varies by mode)
	var modeBg, modeNextBg string
	switch s.Mode {
	case ModeInsert:
		modeBg = "#7dce13" // Radioactive green
	case ModeCommand:
		modeBg = "#f0b429" // Yellow
	case ModeSearch:
		modeBg = "#00f2ff" // Electric blue
	default: // NORMAL
		modeBg = "#7dce13" // Radioactive green
	}
	modeNextBg = "#2a2139" // Deep purple for next section

	modeStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#0d1117")). // Dark bg as fg
		Background(lipgloss.Color(modeBg)).
		Padding(0, 1)

	modeSeparator := lipgloss.NewStyle().
		Foreground(lipgloss.Color(modeBg)).
		Background(lipgloss.Color(modeNextBg)).
		Render(powerlineRight)

	modeSection := modeStyle.Render(string(s.Mode)) + modeSeparator

	// Section 2: Context (Database + Context) - Gray background
	contextBg := "#2a2139"     // Deep purple
	contextNextBg := "#161b22" // Darker gray

	dbIcon := Icons.Connected
	if !s.Connected {
		dbIcon = Icons.Disconnected
	}

	contextText := fmt.Sprintf("%s %s", dbIcon, s.DatabaseName)
	if s.Context != "" {
		contextText += " • " + s.Context
	}

	contextStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#e6edf3")).
		Background(lipgloss.Color(contextBg)).
		Padding(0, 1)

	contextSeparator := lipgloss.NewStyle().
		Foreground(lipgloss.Color(contextBg)).
		Background(lipgloss.Color(contextNextBg)).
		Render(powerlineRight)

	contextSection := contextStyle.Render(contextText) + contextSeparator

	// Section 3: Git Branch (if available)
	gitBranch := s.getGitBranch()
	var gitSection string
	if gitBranch != "" {
		gitBg := "#161b22"
		gitStyle := lipgloss.NewStyle().
			Foreground(lipgloss.Color("#7dce13")).
			Background(lipgloss.Color(gitBg)).
			Padding(0, 1)

		gitSeparator := lipgloss.NewStyle().
			Foreground(lipgloss.Color(gitBg)).
			Background(lipgloss.Color("#0d1117")). // Back to main bg
			Render(powerlineRight)

		gitSection = gitStyle.Render(" "+gitBranch) + gitSeparator
	}

	// Left side: mode + context + git
	left := modeSection + contextSection + gitSection

	// Right side: Latency + Info
	var right string
	rightParts := []string{}

	// Latency indicator with gradient colors
	if s.Latency > 0 {
		latencyColor := "#7dce13" // Green
		if s.Latency > 500*time.Millisecond {
			latencyColor = "#ff5370" // Red
		} else if s.Latency > 100*time.Millisecond {
			latencyColor = "#f0b429" // Yellow
		}

		latencyStyle := lipgloss.NewStyle().
			Foreground(lipgloss.Color(latencyColor)).
			Background(lipgloss.Color("#0d1117"))

		rightParts = append(rightParts, latencyStyle.Render(fmt.Sprintf("⚡%dms", s.Latency.Milliseconds())))
	}

	// Info section
	if s.Info != "" {
		infoStyle := lipgloss.NewStyle().
			Foreground(lipgloss.Color("#6e7681")).
			Background(lipgloss.Color("#0d1117"))
		rightParts = append(rightParts, infoStyle.Render(s.Info))
	}

	// Help indicator with gradient
	helpStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#00f2ff")).
		Background(lipgloss.Color("#0d1117"))
	rightParts = append(rightParts, helpStyle.Render("? Help"))

	if len(rightParts) > 0 {
		right = strings.Join(rightParts, powerlineRightThin)
	}

	// Calculate spacing
	leftWidth := lipgloss.Width(left)
	rightWidth := lipgloss.Width(right)
	spacerWidth := s.width - leftWidth - rightWidth
	if spacerWidth < 0 {
		spacerWidth = 0
	}

	spacerStyle := lipgloss.NewStyle().
		Background(lipgloss.Color("#0d1117"))
	spacer := spacerStyle.Render(strings.Repeat(" ", spacerWidth))

	// Background style for entire bar
	barStyle := lipgloss.NewStyle().
		Background(lipgloss.Color("#0d1117")).
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

// getGitBranch attempts to get the current git branch name.
func (s *StatusBar) getGitBranch() string {
	cmd := exec.Command("git", "rev-parse", "--abbrev-ref", "HEAD")
	output, err := cmd.Output()
	if err != nil {
		return ""
	}
	branch := strings.TrimSpace(string(output))
	if branch == "" || branch == "HEAD" {
		return ""
	}
	return branch
}
