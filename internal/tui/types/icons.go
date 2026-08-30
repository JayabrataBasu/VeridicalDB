// Package types provides shared types and icons for the TUI package.
package types

import "os"

// IconMode represents the icon rendering mode
type IconMode int

const (
	// IconModeASCII uses basic ASCII characters (broadest compatibility)
	IconModeASCII IconMode = iota
	// IconModeUnicode uses Unicode symbols (most terminals)
	IconModeUnicode
	// IconModeNerdFont uses Nerd Font glyphs (requires Nerd Font installed)
	IconModeNerdFont
)

// IconSet contains all icons used in the TUI
type IconSet struct {
	// Navigation
	ArrowRight string
	ArrowLeft  string
	ArrowUp    string
	ArrowDown  string
	Pointer    string

	// Status
	Success      string
	Error        string
	Warning      string
	Info         string
	Pending      string
	Running      string
	Connected    string
	Disconnected string

	// Database
	Database   string
	Table      string
	Column     string
	Index      string
	Key        string
	ForeignKey string

	// Actions
	Query    string
	Execute  string
	Edit     string
	Delete   string
	Add      string
	Refresh  string
	Export   string
	Import   string
	Settings string
	Help     string
	Exit     string
	About    string

	// UI Elements
	Separator  string
	Bullet     string
	Checkbox   string
	CheckboxOn string
	Folder     string
	File       string
	Clock      string
	Memory     string
	CPU        string
	Disk       string
	Network    string

	// Sections
	Dashboard    string
	Browser      string
	Backup       string
	Users        string
	Logs         string
	Transactions string
	Connections  string
	Performance  string
}

// NerdFontIcons returns an IconSet using Nerd Font glyphs
func NerdFontIcons() IconSet {
	return IconSet{
		// Navigation (Nerd Font)
		ArrowRight: "→",
		ArrowLeft:  "←",
		ArrowUp:    "↑",
		ArrowDown:  "↓",
		Pointer:    "󰜴",

		// Status
		Success:      "󰄬",
		Error:        "󰅖",
		Warning:      "󰀪",
		Info:         "󰋽",
		Pending:      "󰔛",
		Running:      "󰓅",
		Connected:    "󰌘",
		Disconnected: "󰌙",

		// Database
		Database:   "󰆼",
		Table:      "󰓫",
		Column:     "󰅪",
		Index:      "󰈞",
		Key:        "󰌋",
		ForeignKey: "󰌹",

		// Actions
		Query:    "󰆍",
		Execute:  "󰐕",
		Edit:     "󰏫",
		Delete:   "󰆴",
		Add:      "󰐕",
		Refresh:  "󰑐",
		Export:   "󰈈",
		Import:   "󰈇",
		Settings: "󰒓",
		Help:     "󰋖",
		Exit:     "󰍃",
		About:    "󰋼",

		// UI Elements
		Separator:  "│",
		Bullet:     "•",
		Checkbox:   "󰄱",
		CheckboxOn: "󰱒",
		Folder:     "󰉋",
		File:       "󰈔",
		Clock:      "󰥔",
		Memory:     "󰍛",
		CPU:        "󰻠",
		Disk:       "󰋊",
		Network:    "󰛳",

		// Sections
		Dashboard:    "󰕮",
		Browser:      "󰈹",
		Backup:       "󰁯",
		Users:        "󰀉",
		Logs:         "󰌱",
		Transactions: "󰓦",
		Connections:  "󰌘",
		Performance:  "󰓅",
	}
}

// UnicodeIcons returns an IconSet using a restrained, consistent set of glyphs.
// One visual family, all single-width, no emoji: section markers are a thin bar,
// status is a small dot/tick, actions borrow a handful of arrows.
func UnicodeIcons() IconSet {
	const bar = "▍" // uniform section / nav marker

	return IconSet{
		// Navigation
		ArrowRight: "→",
		ArrowLeft:  "←",
		ArrowUp:    "↑",
		ArrowDown:  "↓",
		Pointer:    "▸",

		// Status
		Success:      "✓",
		Error:        "✗",
		Warning:      "!",
		Info:         "·",
		Pending:      "◦",
		Running:      "◐",
		Connected:    "●",
		Disconnected: "○",

		// Database
		Database:   bar,
		Table:      "▤",
		Column:     "·",
		Index:      "≡",
		Key:        "◇",
		ForeignKey: "→",

		// Actions
		Query:    "»",
		Execute:  "▶",
		Edit:     "▎",
		Delete:   "✗",
		Add:      "+",
		Refresh:  "↻",
		Export:   "↑",
		Import:   "↓",
		Settings: bar,
		Help:     "?",
		Exit:     "✕",
		About:    bar,

		// UI Elements
		Separator:  "│",
		Bullet:     "·",
		Checkbox:   "○",
		CheckboxOn: "●",
		Folder:     "▸",
		File:       "·",
		Clock:      "○",
		Memory:     "▪",
		CPU:        "▪",
		Disk:       "▪",
		Network:    "▪",

		// Sections — all the same marker; the label carries the meaning
		Dashboard:    bar,
		Browser:      bar,
		Backup:       bar,
		Users:        bar,
		Logs:         bar,
		Transactions: bar,
		Connections:  "●",
		Performance:  bar,
	}
}

// ASCIIIcons returns an IconSet using only basic ASCII characters
func ASCIIIcons() IconSet {
	return IconSet{
		// Navigation (ASCII)
		ArrowRight: ">",
		ArrowLeft:  "<",
		ArrowUp:    "^",
		ArrowDown:  "v",
		Pointer:    ">",

		// Status
		Success:      "+",
		Error:        "x",
		Warning:      "!",
		Info:         "i",
		Pending:      "o",
		Running:      "*",
		Connected:    "*",
		Disconnected: "o",

		// Database
		Database:   "[D]",
		Table:      "[T]",
		Column:     "|",
		Index:      "[I]",
		Key:        "[K]",
		ForeignKey: "->",

		// Actions
		Query:    ">",
		Execute:  "*",
		Edit:     "~",
		Delete:   "x",
		Add:      "+",
		Refresh:  "@",
		Export:   "^",
		Import:   "v",
		Settings: "#",
		Help:     "?",
		Exit:     "X",
		About:    "i",

		// UI Elements
		Separator:  "|",
		Bullet:     "*",
		Checkbox:   "[ ]",
		CheckboxOn: "[x]",
		Folder:     "+",
		File:       "-",
		Clock:      "@",
		Memory:     "M",
		CPU:        "C",
		Disk:       "D",
		Network:    "N",

		// Sections
		Dashboard:    "[#]",
		Browser:      "[B]",
		Backup:       "[S]",
		Users:        "[U]",
		Logs:         "[L]",
		Transactions: "[X]",
		Connections:  "[C]",
		Performance:  "[P]",
	}
}

// Icons is the global icon set used throughout the TUI
var Icons = UnicodeIcons()

// SetIconMode changes the global icon set
func SetIconMode(mode IconMode) {
	switch mode {
	case IconModeASCII:
		Icons = ASCIIIcons()
	case IconModeNerdFont:
		Icons = NerdFontIcons()
	default:
		Icons = UnicodeIcons()
	}
}

// DetectIconMode determines the best icon mode based on environment
func DetectIconMode() IconMode {
	// Check for Nerd Font environment variable
	if os.Getenv("NERD_FONTS") == "1" || os.Getenv("VERIDICALDB_ICONS") == "nerd" {
		return IconModeNerdFont
	}

	// Check for ASCII-only mode
	if os.Getenv("VERIDICALDB_ICONS") == "ascii" {
		return IconModeASCII
	}

	// Default to Unicode (works in most modern terminals)
	return IconModeUnicode
}

// InitIcons initializes the icon system with automatic detection
func InitIcons() {
	SetIconMode(DetectIconMode())
}
