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

type backupLoadedMsg struct {
	records []catalog.BackupRecord
	err     error
}

// BackupScreen shows backup metadata from persistence.
type BackupScreen struct {
	app      types.StyleProvider
	records  []catalog.BackupRecord
	selected int
	scroll   int
	width    int
	height   int
	err      string
	loading  bool
}

// NewBackupScreen creates a new backup screen.
func NewBackupScreen(app types.StyleProvider) *BackupScreen {
	return &BackupScreen{
		app:      app,
		records:  make([]catalog.BackupRecord, 0),
		selected: 0,
		scroll:   0,
		width:    80,
		height:   24,
		loading:  true,
	}
}

func (b *BackupScreen) Init() tea.Cmd {
	return b.loadCmd()
}

func (b *BackupScreen) Update(msg tea.Msg) (Screen, tea.Cmd) {
	switch msg := msg.(type) {
	case backupLoadedMsg:
		b.loading = false
		if msg.err != nil {
			b.err = msg.err.Error()
			b.records = nil
			return b, nil
		}
		b.records = msg.records
		b.selected = 0
		b.scroll = 0
		b.err = ""
		return b, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "esc", "q":
			return b, func() tea.Msg { return ScreenChangeMsg{ScreenID: "home"} }
		case "r":
			b.loading = true
			return b, b.loadCmd()
		case "up", "k":
			if b.selected > 0 {
				b.selected--
				if b.selected < b.scroll {
					b.scroll = b.selected
				}
			}
		case "down", "j":
			if b.selected < len(b.records)-1 {
				b.selected++
				if b.selected >= b.scroll+b.maxVisibleRows() {
					b.scroll = b.selected - b.maxVisibleRows() + 1
				}
			}
		case "pgup":
			b.selected -= b.maxVisibleRows()
			if b.selected < 0 {
				b.selected = 0
			}
			if b.selected < b.scroll {
				b.scroll = b.selected
			}
		case "pgdown":
			b.selected += b.maxVisibleRows()
			if b.selected > len(b.records)-1 {
				b.selected = len(b.records) - 1
			}
			if b.selected >= b.scroll+b.maxVisibleRows() {
				b.scroll = b.selected - b.maxVisibleRows() + 1
			}
		case "home":
			b.selected = 0
			b.scroll = 0
		case "end":
			if len(b.records) > 0 {
				b.selected = len(b.records) - 1
				if b.selected >= b.maxVisibleRows() {
					b.scroll = b.selected - b.maxVisibleRows() + 1
				}
			}
		}

	case tea.WindowSizeMsg:
		b.width = msg.Width
		b.height = msg.Height
	}

	return b, nil
}

func (b *BackupScreen) palette() (accent, highlight, muted, border, selectionBg, selectionFg, text string) {
	accent = "#00D9FF"
	highlight = "#FFB86C"
	muted = "#44475A"
	border = "#3a3a5c"
	selectionBg = "#1c2938"
	selectionFg = "#FFFFFF"
	text = "#FFFFFF"
	if tp, ok := b.app.(interface{ GetThemeManager() *theme.Manager }); ok {
		if tm := tp.GetThemeManager(); tm != nil {
			t := tm.Current()
			accent = t.BrandAccent
			highlight = t.BrandWarning
			muted = t.BrandMuted
			border = t.Border
			selectionBg = t.BrandSelection
			selectionFg = t.Foreground
			text = t.Foreground
		}
	}
	return
}

func (b *BackupScreen) maxVisibleRows() int {
	rows := b.height - 10
	if rows < 5 {
		rows = 5
	}
	return rows
}

func (b *BackupScreen) loadCmd() tea.Cmd {
	return func() tea.Msg {
		p, err := catalog.NewTUIPersistence("data")
		if err != nil {
			return backupLoadedMsg{err: err}
		}
		return backupLoadedMsg{records: p.GetBackupHistory()}
	}
}

func (b *BackupScreen) View() string {
	accent, highlight, muted, border, selectionBg, selectionFg, text := b.palette()

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

	var out strings.Builder
	out.WriteString(headerStyle.Render(types.Icons.Backup + "  Backup & Restore"))
	out.WriteString("\n")
	out.WriteString(mutedStyle.Render(types.Icons.Pointer + " Home › Backups"))
	out.WriteString("\n\n")

	if b.loading {
		out.WriteString(containerStyle.Render(mutedStyle.Render("Loading backups...")))
		return out.String()
	}

	if b.err != "" {
		errBox := containerStyle.Render(lipgloss.NewStyle().Foreground(lipgloss.Color(highlight)).Bold(true).Render("Error: ") + mutedStyle.Render(b.err))
		out.WriteString(errBox)
		return out.String()
	}

	if len(b.records) == 0 {
		out.WriteString(containerStyle.Render(mutedStyle.Render("No backups found.")))
		return out.String()
	}

	header := fmt.Sprintf("%-8s  %-10s  %-8s  %-10s  %s", "Time", "Status", "Size", "DB", "Type")
	out.WriteString(containerStyle.Render(lipgloss.NewStyle().Foreground(lipgloss.Color(accent)).Bold(true).Render(header)))
	out.WriteString("\n")

	maxRows := b.maxVisibleRows()
	end := b.scroll + maxRows
	if end > len(b.records) {
		end = len(b.records)
	}

	for i := b.scroll; i < end; i++ {
		rec := b.records[i]
		line := b.formatRecordLine(rec)
		if i == b.selected {
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

func (b *BackupScreen) formatRecordLine(rec catalog.BackupRecord) string {
	timeStr := rec.CreatedAt.Local().Format("15:04:05")
	status := rec.Status
	size := formatBytesSimple(rec.SizeBytes)
	db := rec.Database
	if db == "" {
		db = "default"
	}
	btype := rec.Type
	if btype == "" {
		btype = "full"
	}
	return fmt.Sprintf("%-8s  %-10s  %-8s  %-10s  %s", timeStr, status, size, db, btype)
}

func formatBytesSimple(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
