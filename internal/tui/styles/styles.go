// Package styles provides shared text styling utilities using raw ANSI escape codes.
// This package exists to avoid import cycles between tui and screens packages I committed a blunder of enormity.
// It uses ANSI codes directly instead of lipgloss to avoid block-level rendering artifacts.
// This exists as a uniform styling utility across the TUI.
package styles

import "fmt"

// ANSI escape code constants
const (
	Reset     = "\033[0m"
	Bold      = "\033[1m"
	Dim       = "\033[2m"
	Italic    = "\033[3m"
	Underline = "\033[4m"
)

// Color returns text with the given foreground color (no background).
// Uses 256-color mode for compatibility.
func Color(text string, r, g, b int) string {
	return fmt.Sprintf("\033[38;2;%d;%d;%dm%s%s", r, g, b, text, Reset)
}

// ColorBold returns bold text with the given foreground color.
func ColorBold(text string, r, g, b int) string {
	return fmt.Sprintf("%s\033[38;2;%d;%d;%dm%s%s", Bold, r, g, b, text, Reset)
}

// ColorUnderline returns underlined text with the given foreground color.
func ColorUnderline(text string, r, g, b int) string {
	return fmt.Sprintf("%s\033[38;2;%d;%d;%dm%s%s", Underline, r, g, b, text, Reset)
}

// ColorBoldUnderline returns bold underlined text with the given foreground color.
func ColorBoldUnderline(text string, r, g, b int) string {
	return fmt.Sprintf("%s%s\033[38;2;%d;%d;%dm%s%s", Bold, Underline, r, g, b, text, Reset)
}

// Hex converts a hex color string like "#268bd2" to RGB values.
func Hex(hex string) (r, g, b int) {
	if len(hex) == 7 && hex[0] == '#' {
		_, _ = fmt.Sscanf(hex[1:], "%02x%02x%02x", &r, &g, &b)
	}
	return
}

// FromHex returns colored text from a hex color string.
func FromHex(text, hex string) string {
	r, g, b := Hex(hex)
	return Color(text, r, g, b)
}

// FromHexBold returns bold colored text from a hex color string.
func FromHexBold(text, hex string) string {
	r, g, b := Hex(hex)
	return ColorBold(text, r, g, b)
}

// FromHexUnderline returns underlined colored text from a hex color string.
func FromHexUnderline(text, hex string) string {
	r, g, b := Hex(hex)
	return ColorUnderline(text, r, g, b)
}

// Plain returns text with no styling (just ensures reset).
func Plain(text string) string {
	return text + Reset
}

// BoldText returns bold text without color.
func BoldText(text string) string {
	return Bold + text + Reset
}

// DimText returns dimmed text.
func DimText(text string) string {
	return Dim + text + Reset
}
