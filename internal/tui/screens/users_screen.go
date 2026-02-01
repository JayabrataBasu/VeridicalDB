package screens

import (
	"fmt"
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/internal/tui/theme"
	"github.com/JayabrataBasu/VeridicalDB/internal/tui/types"
	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type userActionsLoadedMsg struct {
	actions []catalog.UserAction
	err     error
}

// UsersScreen shows user management activity from persistence.
type UsersScreen struct {
	app      types.StyleProvider
	actions  []catalog.UserAction
	selected int
	scroll   int
	width    int
	height   int
	err      string
	loading  bool
}

// NewUsersScreen creates a new users screen.
func NewUsersScreen(app types.StyleProvider) *UsersScreen {
	return &UsersScreen{
		app:      app,
		actions:  make([]catalog.UserAction, 0),
		selected: 0,
		scroll:   0,
		width:    80,
		height:   24,
		loading:  true,
	}
}

func (u *UsersScreen) Init() tea.Cmd {
	return u.loadCmd()
}

func (u *UsersScreen) Update(msg tea.Msg) (Screen, tea.Cmd) {
	switch msg := msg.(type) {
	case userActionsLoadedMsg:
		u.loading = false
		if msg.err != nil {
			u.err = msg.err.Error()
			u.actions = nil
			return u, nil
		}
		u.actions = msg.actions
		u.selected = 0
		u.scroll = 0
		u.err = ""
		return u, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "esc", "q":
			return u, func() tea.Msg { return ScreenChangeMsg{ScreenID: "home"} }
		case "r":
			u.loading = true
			return u, u.loadCmd()
		case "up", "k":
			if u.selected > 0 {
				u.selected--
				if u.selected < u.scroll {
					u.scroll = u.selected
				}
			}
		case "down", "j":
			if u.selected < len(u.actions)-1 {
				u.selected++
				if u.selected >= u.scroll+u.maxVisibleRows() {
					u.scroll = u.selected - u.maxVisibleRows() + 1
				}
			}
		case "pgup":
			u.selected -= u.maxVisibleRows()
			if u.selected < 0 {
				u.selected = 0
			}
			if u.selected < u.scroll {
				u.scroll = u.selected
			}
		case "pgdown":
			u.selected += u.maxVisibleRows()
			if u.selected > len(u.actions)-1 {
				u.selected = len(u.actions) - 1
			}
			if u.selected >= u.scroll+u.maxVisibleRows() {
				u.scroll = u.selected - u.maxVisibleRows() + 1
			}
		case "home":
			u.selected = 0
			u.scroll = 0
		case "end":
			if len(u.actions) > 0 {
				u.selected = len(u.actions) - 1
				if u.selected >= u.maxVisibleRows() {
					u.scroll = u.selected - u.maxVisibleRows() + 1
				}
			}
		}

	case tea.WindowSizeMsg:
		u.width = msg.Width
		u.height = msg.Height
	}

	return u, nil
}

func (u *UsersScreen) palette() (accent, highlight, muted, text string) {
	accent = "#00D9FF"
	highlight = "#FF006E"
	muted = "#44475A"
	text = "#FFFFFF"
	if tp, ok := u.app.(interface{ GetThemeManager() *theme.Manager }); ok {
		if tm := tp.GetThemeManager(); tm != nil {
			t := tm.Current()
			accent = t.BrandAccent
			highlight = t.BrandHighlight
			muted = t.BrandMuted
			text = t.Foreground
		}
	}
	return
}

func (u *UsersScreen) maxVisibleRows() int {
	rows := u.height - 10
	if rows < 5 {
		rows = 5
	}
	return rows
}

func (u *UsersScreen) loadCmd() tea.Cmd {
	return func() tea.Msg {
		p, err := catalog.NewTUIPersistence("data")
		if err != nil {
			return userActionsLoadedMsg{err: err}
		}
		return userActionsLoadedMsg{actions: p.GetUserActions()}
	}
}

func (u *UsersScreen) View() string {
	accent, highlight, muted, text := u.palette()

	headerStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(accent)).
		Padding(0, 2).
		MarginBottom(1)

	containerStyle := lipgloss.NewStyle().
		Padding(1, 2)

	mutedStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(muted))

	rowStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(text))

	selectedStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(accent)).
		Bold(true)

	var out strings.Builder
	out.WriteString(headerStyle.Render(types.Icons.Users + "  User Management"))
	out.WriteString("\n")
	out.WriteString(mutedStyle.Render(types.Icons.Pointer + " Home › Users"))
	out.WriteString("\n\n")

	if u.loading {
		out.WriteString(containerStyle.Render(mutedStyle.Render("Loading user activity...")))
		return out.String()
	}

	if u.err != "" {
		errBox := containerStyle.Render(lipgloss.NewStyle().Foreground(lipgloss.Color(highlight)).Bold(true).Render("Error: ") + mutedStyle.Render(u.err))
		out.WriteString(errBox)
		return out.String()
	}

	if len(u.actions) == 0 {
		out.WriteString(containerStyle.Render(mutedStyle.Render("No user actions recorded.")))
		return out.String()
	}

	header := fmt.Sprintf("%-8s  %-10s  %-12s  %-8s  %s", "Time", "Action", "Target", "Status", "Details")
	out.WriteString(containerStyle.Render(lipgloss.NewStyle().Foreground(lipgloss.Color(accent)).Bold(true).Render(header)))
	out.WriteString("\n")

	maxRows := u.maxVisibleRows()
	end := u.scroll + maxRows
	if end > len(u.actions) {
		end = len(u.actions)
	}

	for i := u.scroll; i < end; i++ {
		act := u.actions[i]
		line := u.formatActionLine(act)
		if i == u.selected {
			out.WriteString(selectedStyle.Render(line))
		} else {
			out.WriteString(rowStyle.Render(line))
		}
		out.WriteString("\n")
	}

	out.WriteString("\n")
	out.WriteString(mutedStyle.Render("↑/↓ Navigate  PgUp/PgDn Scroll  r Refresh  Esc Back"))

	return out.String()
}

func (u *UsersScreen) formatActionLine(act catalog.UserAction) string {
	timeStr := act.Timestamp.Local().Format("15:04:05")
	action := act.Action
	if action == "" {
		action = "unknown"
	}
	target := act.TargetUser
	if target == "" {
		target = "-"
	}
	status := act.Status
	if status == "" {
		status = "-"
	}
	details := strings.ReplaceAll(act.Details, "\n", " ")
	details = u.truncate(details, u.width-50)
	return fmt.Sprintf("%-8s  %-10s  %-12s  %-8s  %s", timeStr, action, target, status, details)
}

func (u *UsersScreen) truncate(s string, max int) string {
	if max <= 0 {
		return ""
	}
	if len(s) <= max {
		return s
	}
	if max <= 3 {
		return s[:max]
	}
	return s[:max-3] + "..."
}
