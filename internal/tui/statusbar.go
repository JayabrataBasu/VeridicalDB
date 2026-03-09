// Package tui provides the Bubble Tea Terminal User Interface for VeridicalDB.
package tui

import (
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/internal/tui/styles"
	"github.com/JayabrataBasu/VeridicalDB/internal/tui/theme"
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

// View renders the status bar with simple ANSI styling - no lipgloss backgrounds.
func (s *StatusBar) View() string {
	t := s.themeManager.Current()

	// Build status bar with pure ANSI - no block backgrounds
	var parts []string

	// Mode indicator
	modeColor := t.BrandSuccess
	switch s.Mode {
	case ModeInsert:
		modeColor = t.BrandSuccess
	case ModeCommand:
		modeColor = t.BrandWarning
	case ModeSearch:
		modeColor = t.BrandAccent
	}
	parts = append(parts, styles.FromHexBold(string(s.Mode), modeColor))

	// Separator
	parts = append(parts, styles.FromHex(" │ ", t.Border))

	// Database connection
	dbIcon := Icons.Connected
	if !s.Connected {
		dbIcon = Icons.Disconnected
	}
	connColor := t.BrandSuccess
	if !s.Connected {
		connColor = t.BrandDanger
	}
	parts = append(parts, styles.FromHex(dbIcon+" "+s.DatabaseName, connColor))

	// Context if set
	if s.Context != "" {
		parts = append(parts, styles.FromHex(" • ", t.Muted))
		parts = append(parts, styles.FromHex(s.Context, t.Foreground))
	}

	// Git branch
	gitBranch := s.getGitBranch()
	if gitBranch != "" {
		parts = append(parts, styles.FromHex(" │ ", t.Border))
		parts = append(parts, styles.FromHex(" "+gitBranch, t.BrandSuccess))
	}

	left := strings.Join(parts, "")

	// Right side
	var rightParts []string

	// Latency
	if s.Latency > 0 {
		latencyColor := t.BrandSuccess
		if s.Latency > 500*time.Millisecond {
			latencyColor = t.BrandDanger
		} else if s.Latency > 100*time.Millisecond {
			latencyColor = t.BrandWarning
		}
		rightParts = append(rightParts, styles.FromHex(fmt.Sprintf("⚡%dms", s.Latency.Milliseconds()), latencyColor))
	}

	// Info
	if s.Info != "" {
		rightParts = append(rightParts, styles.FromHex(s.Info, t.Muted))
	}

	// Help
	rightParts = append(rightParts, styles.FromHex(Icons.Help+" Help", t.BrandAccent))

	right := strings.Join(rightParts, styles.FromHex(" │ ", t.Border))

	// Simple join with spacing
	return left + "  " + right
}

// CompactView renders a minimal status bar for narrow terminals.
func (s *StatusBar) CompactView() string {
	t := s.themeManager.Current()

	// Mode indicator
	modeColor := t.BrandSuccess
	modeChar := "N"
	switch s.Mode {
	case ModeInsert:
		modeChar = "I"
		modeColor = t.BrandSuccess
	case ModeCommand:
		modeChar = "C"
		modeColor = t.BrandWarning
	case ModeSearch:
		modeChar = "/"
		modeColor = t.BrandAccent
	}

	// DB status
	dbIcon := Icons.Connected
	if !s.Connected {
		dbIcon = Icons.Disconnected
	}

	return styles.FromHexBold(modeChar, modeColor) +
		styles.FromHex(" │ ", t.Border) +
		styles.FromHex(dbIcon+" "+s.DatabaseName, t.Foreground)
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
