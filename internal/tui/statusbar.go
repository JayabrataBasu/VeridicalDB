// Package tui provides the Bubble Tea Terminal User Interface for VeridicalDB.
package tui

import (
	"fmt"
	"strings"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/internal/tui/styles"
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

	// Stats from the last query, shown on the right when set.
	hasQueryStats bool
	queryRows     int
	queryDuration time.Duration
	queryOK       bool

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

// SetQueryStats records the outcome of the most recent query.
func (s *StatusBar) SetQueryStats(rows int, dur time.Duration, ok bool) {
	s.hasQueryStats = true
	s.queryRows = rows
	s.queryDuration = dur
	s.queryOK = ok
}

// ClearQueryStats hides the query stats segment.
func (s *StatusBar) ClearQueryStats() {
	s.hasQueryStats = false
}

// View renders a single full-width status line: mode and database on the left,
// query stats / info / a help hint on the right, padded to the bar width.
func (s *StatusBar) View() string {
	t := s.themeManager.Current()
	sep := styles.FromHex(" "+Icons.Separator+" ", t.Border)

	// ---- left ----
	modeColor := t.BrandSuccess
	switch s.Mode {
	case ModeCommand:
		modeColor = t.BrandWarning
	case ModeSearch:
		modeColor = t.BrandAccent
	}
	dbIcon, connColor := Icons.Connected, t.BrandSuccess
	if !s.Connected {
		dbIcon, connColor = Icons.Disconnected, t.Muted
	}
	left := " " + styles.FromHexBold(string(s.Mode), modeColor) + sep +
		styles.FromHex(s.DatabaseName+" ", t.Muted) + styles.FromHex(dbIcon, connColor)

	// ---- right ----
	var right []string
	if s.hasQueryStats {
		c := t.BrandSuccess
		if !s.queryOK {
			c = t.BrandDanger
		} else if s.queryDuration > 500*time.Millisecond {
			c = t.BrandWarning
		}
		label := "OK"
		if s.queryOK && s.queryRows >= 0 {
			label = fmt.Sprintf("%d %s", s.queryRows, plural(s.queryRows, "row", "rows"))
		}
		right = append(right, styles.FromHex(fmt.Sprintf("%s %s", label, fmtDur(s.queryDuration)), c))
	}
	if s.Info != "" {
		right = append(right, styles.FromHex(s.Info, t.Foreground))
	}
	right = append(right, styles.FromHex(Icons.Help+" help ", t.BrandAccent))
	rightStr := strings.Join(right, sep)

	// ---- pad ----
	pad := s.width - lipgloss.Width(left) - lipgloss.Width(rightStr)
	if pad < 1 {
		pad = 1
	}
	return left + strings.Repeat(" ", pad) + rightStr
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

func fmtDur(d time.Duration) string {
	if d <= 0 {
		return ""
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%dµs", d.Microseconds())
	}
	if d < time.Second {
		return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000)
	}
	return fmt.Sprintf("%.2fs", d.Seconds())
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
