package screens

//this is gonna be awesome or a nightmare.
import (
	"fmt"
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/internal/tui/theme"
	"github.com/JayabrataBasu/VeridicalDB/internal/tui/types"
	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type queryHistoryLoadedMsg struct {
	records []catalog.QueryRecord
	err     error
}

// QueryHistoryScreen shows recent query history from persistence.
type QueryHistoryScreen struct {
	app      types.StyleProvider
	records  []catalog.QueryRecord
	selected int
	scroll   int
	width    int
	height   int
	err      string
	loading  bool
}

// NewQueryHistoryScreen creates a new query history screen.
func NewQueryHistoryScreen(app types.StyleProvider) *QueryHistoryScreen {
	return &QueryHistoryScreen{
		app:      app,
		records:  make([]catalog.QueryRecord, 0),
		selected: 0,
		scroll:   0,
		width:    80,
		height:   24,
		loading:  true,
	}
}

// Init initializes the query history screen.
func (q *QueryHistoryScreen) Init() tea.Cmd {
	return q.loadCmd()
}

// Update handles messages for the query history screen.
func (q *QueryHistoryScreen) Update(msg tea.Msg) (Screen, tea.Cmd) {
	switch msg := msg.(type) {
	case queryHistoryLoadedMsg:
		q.loading = false
		if msg.err != nil {
			q.err = msg.err.Error()
			q.records = nil
			return q, nil
		}
		q.records = msg.records
		q.selected = 0
		q.scroll = 0
		q.err = ""
		return q, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "esc", "q":
			return q, func() tea.Msg { return ScreenChangeMsg{ScreenID: "home"} }
		case "r":
			q.loading = true
			return q, q.loadCmd()
		case "up", "k":
			if q.selected > 0 {
				q.selected--
				if q.selected < q.scroll {
					q.scroll = q.selected
				}
			}
		case "down", "j":
			if q.selected < len(q.records)-1 {
				q.selected++
				if q.selected >= q.scroll+q.maxVisibleRows() {
					q.scroll = q.selected - q.maxVisibleRows() + 1
				}
			}
		case "pgup":
			q.selected -= q.maxVisibleRows()
			if q.selected < 0 {
				q.selected = 0
			}
			if q.selected < q.scroll {
				q.scroll = q.selected
			}
		case "pgdown":
			q.selected += q.maxVisibleRows()
			if q.selected > len(q.records)-1 {
				q.selected = len(q.records) - 1
			}
			if q.selected >= q.scroll+q.maxVisibleRows() {
				q.scroll = q.selected - q.maxVisibleRows() + 1
			}
		case "home":
			q.selected = 0
			q.scroll = 0
		case "end":
			if len(q.records) > 0 {
				q.selected = len(q.records) - 1
				if q.selected >= q.maxVisibleRows() {
					q.scroll = q.selected - q.maxVisibleRows() + 1
				}
			}
		}

	case tea.WindowSizeMsg:
		q.width = msg.Width
		q.height = msg.Height
	}

	return q, nil
}

// an awesoem nightmare
func (q *QueryHistoryScreen) palette() (accent, highlight, muted, border, selectionBg, selectionFg, text string) {
	accent = "#00D9FF"
	highlight = "#FF006E"
	muted = "#44475A"
	border = "#3a3a5c"
	selectionBg = "#1c2938"
	selectionFg = "#FFFFFF"
	text = "#FFFFFF"
	if tp, ok := q.app.(interface{ GetThemeManager() *theme.Manager }); ok {
		if tm := tp.GetThemeManager(); tm != nil {
			t := tm.Current()
			accent = t.BrandAccent
			highlight = t.BrandHighlight
			muted = t.BrandMuted
			border = t.Border
			selectionBg = t.BrandSelection
			selectionFg = t.Foreground
			text = t.Foreground
		}
	}
	return
}

func (q *QueryHistoryScreen) maxVisibleRows() int {
	rows := q.height - 10
	if rows < 5 {
		rows = 5
	}
	return rows
}

func (q *QueryHistoryScreen) loadCmd() tea.Cmd {
	return func() tea.Msg {
		p, err := catalog.NewTUIPersistence("data")
		if err != nil {
			return queryHistoryLoadedMsg{err: err}
		}
		return queryHistoryLoadedMsg{records: p.GetQueryHistory()}
	}
}

// View renders the query history screen.
func (q *QueryHistoryScreen) View() string {
	accent, highlight, muted, border, selectionBg, selectionFg, text := q.palette()

	headerStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color(accent)).
		Padding(0, 2).
		MarginBottom(1)

	containerStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color(border)).
		Padding(1, 2)

	mutedStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(muted))

	rowStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(text))

	selectedStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(selectionFg)).
		Background(lipgloss.Color(selectionBg)).
		Bold(true)

	// Header
	var b strings.Builder
	b.WriteString(headerStyle.Render(types.Icons.Clock + "  Query History"))
	b.WriteString("\n")
	b.WriteString(mutedStyle.Render(types.Icons.Pointer + " Home › " + types.Icons.Query + " Editor › History"))
	b.WriteString("\n\n")

	if q.loading {
		loading := containerStyle.Render(mutedStyle.Render("Loading query history..."))
		b.WriteString(loading)
		return b.String()
	}

	if q.err != "" {
		errBox := containerStyle.Render(lipgloss.NewStyle().Foreground(lipgloss.Color(highlight)).Bold(true).Render("Error: ") + mutedStyle.Render(q.err))
		b.WriteString(errBox)
		return b.String()
	}

	if len(q.records) == 0 {
		empty := containerStyle.Render(mutedStyle.Render("No query history available."))
		b.WriteString(empty)
		return b.String()
	}

	// Table header
	header := fmt.Sprintf("%-8s  %-8s  %-6s  %-10s  %s", "Time", "Status", "Rows", "DB", "Query")
	b.WriteString(containerStyle.Render(lipgloss.NewStyle().Foreground(lipgloss.Color(accent)).Bold(true).Render(header)))
	b.WriteString("\n")

	// Records list
	maxRows := q.maxVisibleRows()
	end := q.scroll + maxRows
	if end > len(q.records) {
		end = len(q.records)
	}

	for i := q.scroll; i < end; i++ {
		rec := q.records[i]
		line := q.formatRecordLine(rec)
		if i == q.selected {
			b.WriteString(selectedStyle.Render(line))
		} else {
			b.WriteString(rowStyle.Render(line))
		}
		b.WriteString("\n")
	}

	// Footer
	b.WriteString("\n")
	help := mutedStyle.Render("↑/↓ Navigate  PgUp/PgDn Scroll  r Refresh  Esc Back")
	b.WriteString(help)

	return b.String()
}

func (q *QueryHistoryScreen) formatRecordLine(rec catalog.QueryRecord) string {
	timeStr := rec.ExecutedAt.Local().Format("15:04:05")
	status := rec.Status
	rows := fmt.Sprintf("%d", rec.RowsAffected)
	if rec.RowsAffected == 0 && rec.Status == "error" {
		rows = "-"
	}
	db := rec.Database
	if db == "" {
		db = "default"
	}
	query := q.truncate(strings.ReplaceAll(rec.QueryText, "\n", " "), q.width-40)
	return fmt.Sprintf("%-8s  %-8s  %-6s  %-10s  %s", timeStr, status, rows, db, query)
}

func (q *QueryHistoryScreen) truncate(s string, max int) string {
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
