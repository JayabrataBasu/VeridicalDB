package screens

import (
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// HelpOverlay displays keyboard shortcuts and help
type HelpOverlay struct {
	visible    bool
	width      int
	height     int
	scrollPos  int
	content    string
	pageIndex  int
	totalPages int
}

// NewHelpOverlay creates a new help overlay
func NewHelpOverlay() *HelpOverlay {
	return &HelpOverlay{
		visible:    false,
		scrollPos:  0,
		pageIndex:  0,
		totalPages: 3,
	}
}

// Show displays the help overlay
func (h *HelpOverlay) Show() {
	h.visible = true
	h.scrollPos = 0
	h.pageIndex = 0
}

// Hide hides the help overlay
func (h *HelpOverlay) Hide() {
	h.visible = false
}

// IsVisible returns whether help is visible
func (h *HelpOverlay) IsVisible() bool {
	return h.visible
}

// Update handles help overlay input
func (h *HelpOverlay) Update(msg tea.Msg) tea.Cmd {
	if !h.visible {
		return nil
	}

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "esc":
			h.Hide()
			return nil
		case "up":
			if h.scrollPos > 0 {
				h.scrollPos--
			}
		case "down":
			if h.scrollPos < h.contentHeight()-h.height {
				h.scrollPos++
			}
		case "home":
			h.scrollPos = 0
		case "end":
			if contentHeight := h.contentHeight(); contentHeight > h.height {
				h.scrollPos = contentHeight - h.height
			}
		case "page-up":
			h.scrollPos -= (h.height / 2)
			if h.scrollPos < 0 {
				h.scrollPos = 0
			}
		case "page-down":
			h.scrollPos += (h.height / 2)
			if contentHeight := h.contentHeight(); h.scrollPos+h.height > contentHeight {
				h.scrollPos = contentHeight - h.height
			}
		case "right", "n":
			if h.pageIndex < h.totalPages-1 {
				h.pageIndex++
				h.scrollPos = 0
			}
		case "left", "p":
			if h.pageIndex > 0 {
				h.pageIndex--
				h.scrollPos = 0
			}
		}
	case tea.WindowSizeMsg:
		h.width = msg.Width
		h.height = msg.Height
	}

	return nil
}

// View renders the help overlay
func (h *HelpOverlay) View() string {
	if !h.visible {
		return ""
	}

	content := h.getPageContent()
	headerStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("33")).
		MarginBottom(1)

	footerStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("240")).
		MarginTop(1)

	header := headerStyle.Render("📚 Keyboard Reference")
	footer := footerStyle.Render(
		"[↑↓/PgUp/PgDn] Scroll • [←/→] Pages • [q] Quit • Page " +
			lipgloss.NewStyle().Bold(true).Render(string(rune('1'+h.pageIndex))) +
			" of " + lipgloss.NewStyle().Bold(true).Render(string(rune('0'+h.totalPages))))

	// Create scrollable viewport
	contentLines := strings.Split(content, "\n")
	visibleLines := contentLines[h.scrollPos : h.scrollPos+h.height-4]
	if len(visibleLines) > h.height-4 {
		visibleLines = visibleLines[:h.height-4]
	}

	body := strings.Join(visibleLines, "\n")

	// Modal background
	bgStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("33")).
		Background(lipgloss.Color("235")).
		Foreground(lipgloss.Color("252")).
		Padding(1, 2).
		Width(h.width - 4).
		Height(h.height - 2)

	return bgStyle.Render(
		lipgloss.JoinVertical(
			lipgloss.Top,
			header,
			body,
			footer,
		),
	)
}

// getPageContent returns content for current page
func (h *HelpOverlay) getPageContent() string {
	switch h.pageIndex {
	case 0:
		return helpPageNavigation()
	case 1:
		return helpPageEditor()
	case 2:
		return helpPageGlobal()
	default:
		return ""
	}
}

// contentHeight calculates total content height
func (h *HelpOverlay) contentHeight() int {
	return strings.Count(h.getPageContent(), "\n") + 1
}

func helpPageNavigation() string {
	return `NAVIGATION & SCREENS
═══════════════════════════════════════════════════════════════

Screen Switching:
  Tab / Shift+Tab .......... Cycle through main screens
  Alt+1 / Alt+2 / Alt+3 ... Jump to Editor / Results / Dashboard
  Alt+4 .................... Jump to Database Browser

Arrow Navigation:
  ↑ / ↓ / ← / → ........... Move cursor/selection
  Home / End ............... Jump to start/end of list
  Page Up / Page Down ...... Scroll by page
  Ctrl+Home / Ctrl+End .... Jump to first/last item

Database Browser (Tab/Shift+Tab between panels):
  ↑ / ↓ .................... Navigate within panel
  ← / → .................... Switch between panels
  Enter .................... Expand/select item

Results Panel:
  ↑ / ↓ .................... Navigate rows
  ← / → .................... Navigate columns
  Enter .................... View full row details
  Esc ...................... Back to table view`
}

func helpPageEditor() string {
	return `EDITOR OPERATIONS
═══════════════════════════════════════════════════════════════

Query Execution:
  Ctrl+Enter ............... Execute query
  Ctrl+Shift+I ............. Execute with plan
  Ctrl+; ................... Execute and time

Editing:
  Ctrl+Z / Ctrl+Y .......... Undo / Redo
  Ctrl+X / Ctrl+C / Ctrl+V  Cut / Copy / Paste
  Ctrl+A ................... Select all
  Ctrl+L ................... Clear editor
  Ctrl+/ ................... Toggle comment

History & Search:
  Ctrl+Up / Ctrl+Down ...... Previous / Next query
  Ctrl+F ................... Find in query
  Ctrl+H ................... Find & Replace

Autocomplete:
  Ctrl+Space ............... Show suggestions
  ↑ / ↓ .................... Navigate suggestions
  Enter / Tab .............. Accept suggestion
  Esc ...................... Close autocomplete

Advanced:
  Tab / Shift+Tab .......... Indent / Dedent
  Ctrl+Shift+/ ............. Toggle block comment`
}

func helpPageGlobal() string {
	return `GLOBAL COMMANDS & HELP
═══════════════════════════════════════════════════════════════

Application Control:
  q / Ctrl+C ............... Quit application
  ? / F1 ................... Show this help
  Ctrl+? ................... Keyboard shortcuts
  Ctrl+T ................... Tutorial mode

File Operations:
  Ctrl+S ................... Save query to file
  Ctrl+O ................... Open query from file
  Ctrl+Shift+S ............. Save results

Command Mode (prefix with :):
  :help .................... Show help documentation
  :status .................. Show connection status
  :config .................. Show configuration
  :set ..................... Change settings
  :exit / :quit ............ Quit application

Export (Results Panel):
  Ctrl+E ................... Export results
  (supports CSV, JSON, Excel)

Tips:
  • All shortcuts are remappable in ~/.veridicaldb/keybindings.yaml
  • Use :help <command> for detailed documentation
  • Press ? in any screen for context-specific help
  • Vim-mode available: :set keybindings=vim`
}
