// Package tui provides the Bubble Tea Terminal User Interface for VeridicalDB.
package tui

import "os"

// IconSet defines a collection of icons for the TUI.
type IconSet struct {
	// Navigation
	Database   string
	Table      string
	Column     string
	Index      string
	Query      string
	Terminal   string
	Settings   string
	Help       string
	Exit       string
	Folder     string
	File       string

	// Status
	Success    string
	Error      string
	Warning    string
	Info       string
	Loading    string
	Connected  string
	Disconnected string

	// UI Elements
	Selected   string
	Unselected string
	Expanded   string
	Collapsed  string
	ArrowRight string
	ArrowLeft  string
	ArrowUp    string
	ArrowDown  string
	Separator  string
	Bullet     string

	// Actions
	New        string
	Edit       string
	Delete     string
	Save       string
	Refresh    string
	Search     string
	Filter     string
	Sort       string
	Export     string
	Import     string

	// Monitoring
	Dashboard  string
	Metrics    string
	Users      string
	Backup     string
	About      string
}

// NerdFontIcons returns icons using Nerd Font glyphs.
func NerdFontIcons() *IconSet {
	return &IconSet{
		// Navigation (Nerd Font glyphs)
		Database:   "\uf1c0", // 
		Table:      "\uf0ce", // 
		Column:     "\uf0db", // 
		Index:      "\uf15c", // 
		Query:      "\uf121", // 
		Terminal:   "\uf120", // 
		Settings:   "\uf013", // 
		Help:       "\uf059", // 
		Exit:       "\uf2f5", // 
		Folder:     "\uf07b", // 
		File:       "\uf15b", // 

		// Status
		Success:    "\uf00c", // 
		Error:      "\uf00d", // 
		Warning:    "\uf071", // 
		Info:       "\uf05a", // 
		Loading:    "\uf110", // 
		Connected:  "\uf1e6", // 
		Disconnected: "\uf127", // 

		// UI Elements
		Selected:   "\uf00c", // 
		Unselected: "\uf10c", // 
		Expanded:   "\uf0d7", // 
		Collapsed:  "\uf0da", // 
		ArrowRight: "\uf054", // 
		ArrowLeft:  "\uf053", // 
		ArrowUp:    "\uf077", // 
		ArrowDown:  "\uf078", // 
		Separator:  "\uf142", // 
		Bullet:     "\uf111", // 

		// Actions
		New:        "\uf067", // 
		Edit:       "\uf044", // 
		Delete:     "\uf1f8", // 
		Save:       "\uf0c7", // 
		Refresh:    "\uf021", // 
		Search:     "\uf002", // 
		Filter:     "\uf0b0", // 
		Sort:       "\uf0dc", // 
		Export:     "\uf093", // 
		Import:     "\uf019", // 

		// Monitoring
		Dashboard:  "\uf0e4", // 
		Metrics:    "\uf080", // 
		Users:      "\uf0c0", // 
		Backup:     "\uf1c0", // 
		About:      "\uf05a", // 
	}
}

// ASCIIIcons returns icons using simple ASCII characters.
// This is the fallback for terminals without Nerd Font support.
func ASCIIIcons() *IconSet {
	return &IconSet{
		// Navigation
		Database:   "[DB]",
		Table:      "[T]",
		Column:     "[C]",
		Index:      "[I]",
		Query:      ">",
		Terminal:   "$",
		Settings:   "[*]",
		Help:       "?",
		Exit:       "X",
		Folder:     "/",
		File:       "-",

		// Status
		Success:    "[+]",
		Error:      "[!]",
		Warning:    "[~]",
		Info:       "[i]",
		Loading:    "...",
		Connected:  "[*]",
		Disconnected: "[-]",

		// UI Elements
		Selected:   "*",
		Unselected: "o",
		Expanded:   "v",
		Collapsed:  ">",
		ArrowRight: ">",
		ArrowLeft:  "<",
		ArrowUp:    "^",
		ArrowDown:  "v",
		Separator:  "|",
		Bullet:     "*",

		// Actions
		New:        "+",
		Edit:       "~",
		Delete:     "x",
		Save:       "S",
		Refresh:    "R",
		Search:     "/",
		Filter:     "F",
		Sort:       "^v",
		Export:     "E",
		Import:     "I",

		// Monitoring
		Dashboard:  "[=]",
		Metrics:    "[#]",
		Users:      "[@]",
		Backup:     "[B]",
		About:      "[?]",
	}
}

// UnicodeIcons returns icons using Unicode symbols (middle ground).
// Works in most modern terminals without requiring Nerd Fonts.
func UnicodeIcons() *IconSet {
	return &IconSet{
		// Navigation
		Database:   "◆",
		Table:      "▤",
		Column:     "│",
		Index:      "⊕",
		Query:      "▶",
		Terminal:   "⌘",
		Settings:   "⚙",
		Help:       "?",
		Exit:       "⏻",
		Folder:     "▸",
		File:       "─",

		// Status
		Success:    "✓",
		Error:      "✗",
		Warning:    "⚠",
		Info:       "ℹ",
		Loading:    "◐",
		Connected:  "●",
		Disconnected: "○",

		// UI Elements
		Selected:   "●",
		Unselected: "○",
		Expanded:   "▼",
		Collapsed:  "▶",
		ArrowRight: "→",
		ArrowLeft:  "←",
		ArrowUp:    "↑",
		ArrowDown:  "↓",
		Separator:  "│",
		Bullet:     "•",

		// Actions
		New:        "+",
		Edit:       "✎",
		Delete:     "×",
		Save:       "▪",
		Refresh:    "↻",
		Search:     "⌕",
		Filter:     "⊙",
		Sort:       "⇅",
		Export:     "↗",
		Import:     "↙",

		// Monitoring
		Dashboard:  "▣",
		Metrics:    "▥",
		Users:      "◉",
		Backup:     "⬒",
		About:      "ℹ",
	}
}

// IconMode represents the icon rendering mode.
type IconMode int

const (
	// IconModeASCII uses plain ASCII characters.
	IconModeASCII IconMode = iota
	// IconModeUnicode uses Unicode symbols (default).
	IconModeUnicode
	// IconModeNerdFont uses Nerd Font glyphs.
	IconModeNerdFont
)

// Icons holds the global icon set.
var Icons = UnicodeIcons()

// IconModeName returns the current icon mode.
var currentIconMode = IconModeUnicode

// SetIconMode changes the icon set based on mode.
func SetIconMode(mode IconMode) {
	currentIconMode = mode
	switch mode {
	case IconModeASCII:
		Icons = ASCIIIcons()
	case IconModeUnicode:
		Icons = UnicodeIcons()
	case IconModeNerdFont:
		Icons = NerdFontIcons()
	}
}

// GetIconMode returns the current icon mode.
func GetIconMode() IconMode {
	return currentIconMode
}

// DetectIconMode attempts to detect the best icon mode based on environment.
func DetectIconMode() IconMode {
	// Check for explicit Nerd Font environment variable
	if os.Getenv("NERD_FONTS") == "1" || os.Getenv("VERIDICALDB_ICONS") == "nerd" {
		return IconModeNerdFont
	}
	
	// Check for explicit ASCII mode
	if os.Getenv("VERIDICALDB_ICONS") == "ascii" {
		return IconModeASCII
	}

	// Check for explicit Unicode mode
	if os.Getenv("VERIDICALDB_ICONS") == "unicode" {
		return IconModeUnicode
	}

	// Default to Unicode (works in most modern terminals)
	return IconModeUnicode
}

// InitIcons initializes the icon set based on environment detection.
func InitIcons() {
	SetIconMode(DetectIconMode())
}
