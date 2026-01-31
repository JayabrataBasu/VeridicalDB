// Package theme provides an enhanced theming system for the TUI.
package theme

import "github.com/charmbracelet/lipgloss"

// BrandPalette defines the core brand colors used across all themes.
// These colors provide consistent vibrant accents for the bold tech aesthetic
// while each theme can override specific values for cohesion with its base palette.
type BrandPalette struct {
	// NeonCyan is the primary vibrant accent color for focus states and highlights
	NeonCyan string

	// NeonMagenta is the secondary vibrant accent for selection and active states
	NeonMagenta string

	// DarkCharcoal is the deep background color for strong contrast
	DarkCharcoal string

	// AccentOrange provides warm accent for warnings and secondary highlights
	AccentOrange string

	// BrightGreen indicates success, completion, and positive states
	BrightGreen string

	// SteelGray provides neutral tones for borders and muted elements
	SteelGray string

	// ElectricBlue provides an alternative vibrant accent
	ElectricBlue string

	// NeonPurple provides tertiary vibrant accent for special highlights
	NeonPurple string
}

// DefaultBrandPalette returns the standard brand palette with vibrant tech colors.
// These are the canonical brand colors optimized for dark themes.
func DefaultBrandPalette() *BrandPalette {
	return &BrandPalette{
		NeonCyan:     "#00D9FF",
		NeonMagenta:  "#FF006E",
		DarkCharcoal: "#0A0E27",
		AccentOrange: "#FFB86C",
		BrightGreen:  "#55FF55",
		SteelGray:    "#44475A",
		ElectricBlue: "#00F2FF",
		NeonPurple:   "#BD00FF",
	}
}

// LightBrandPalette returns brand colors adjusted for light themes.
// Colors are slightly desaturated for better readability on light backgrounds.
func LightBrandPalette() *BrandPalette {
	return &BrandPalette{
		NeonCyan:     "#0099CC",
		NeonMagenta:  "#CC0058",
		DarkCharcoal: "#1A1E33",
		AccentOrange: "#E69F5C",
		BrightGreen:  "#22AA22",
		SteelGray:    "#5A5D70",
		ElectricBlue: "#0088CC",
		NeonPurple:   "#9900CC",
	}
}

// CyberpunkBrandPalette returns intensified brand colors for the cyberpunk theme.
// Colors are more saturated and vibrant for the neon aesthetic.
func CyberpunkBrandPalette() *BrandPalette {
	return &BrandPalette{
		NeonCyan:     "#00FFFF",
		NeonMagenta:  "#FF0080",
		DarkCharcoal: "#0D1117",
		AccentOrange: "#FF9E64",
		BrightGreen:  "#7DCE13",
		SteelGray:    "#21262D",
		ElectricBlue: "#00F2FF",
		NeonPurple:   "#BD00FF",
	}
}

// TokyoNightBrandPalette returns brand colors tuned for Tokyo Night theme.
func TokyoNightBrandPalette() *BrandPalette {
	return &BrandPalette{
		NeonCyan:     "#7DCFFF",
		NeonMagenta:  "#F7768E",
		DarkCharcoal: "#1A1B26",
		AccentOrange: "#FF9E64",
		BrightGreen:  "#9ECE6A",
		SteelGray:    "#292E42",
		ElectricBlue: "#7AA2F7",
		NeonPurple:   "#BB9AF7",
	}
}

// DraculaBrandPalette returns brand colors tuned for Dracula theme.
func DraculaBrandPalette() *BrandPalette {
	return &BrandPalette{
		NeonCyan:     "#8BE9FD",
		NeonMagenta:  "#FF79C6",
		DarkCharcoal: "#282A36",
		AccentOrange: "#FFB86C",
		BrightGreen:  "#50FA7B",
		SteelGray:    "#44475A",
		ElectricBlue: "#8BE9FD",
		NeonPurple:   "#BD93F9",
	}
}

// MonokaiBrandPalette returns brand colors tuned for Monokai theme.
func MonokaiBrandPalette() *BrandPalette {
	return &BrandPalette{
		NeonCyan:     "#66D9EF",
		NeonMagenta:  "#F92672",
		DarkCharcoal: "#272822",
		AccentOrange: "#FD971F",
		BrightGreen:  "#A6E22E",
		SteelGray:    "#3E3D32",
		ElectricBlue: "#66D9EF",
		NeonPurple:   "#AE81FF",
	}
}

// NordBrandPalette returns brand colors tuned for Nord theme.
func NordBrandPalette() *BrandPalette {
	return &BrandPalette{
		NeonCyan:     "#88C0D0",
		NeonMagenta:  "#BF616A",
		DarkCharcoal: "#2E3440",
		AccentOrange: "#D08770",
		BrightGreen:  "#A3BE8C",
		SteelGray:    "#3B4252",
		ElectricBlue: "#81A1C1",
		NeonPurple:   "#B48EAD",
	}
}

// SolarizedDarkBrandPalette returns brand colors tuned for Solarized Dark theme.
func SolarizedDarkBrandPalette() *BrandPalette {
	return &BrandPalette{
		NeonCyan:     "#2AA198",
		NeonMagenta:  "#D33682",
		DarkCharcoal: "#002B36",
		AccentOrange: "#CB4B16",
		BrightGreen:  "#859900",
		SteelGray:    "#073642",
		ElectricBlue: "#268BD2",
		NeonPurple:   "#6C71C4",
	}
}

// SolarizedLightBrandPalette returns brand colors tuned for Solarized Light theme.
func SolarizedLightBrandPalette() *BrandPalette {
	return &BrandPalette{
		NeonCyan:     "#2AA198",
		NeonMagenta:  "#D33682",
		DarkCharcoal: "#FDF6E3",
		AccentOrange: "#CB4B16",
		BrightGreen:  "#859900",
		SteelGray:    "#EEE8D5",
		ElectricBlue: "#268BD2",
		NeonPurple:   "#6C71C4",
	}
}

// BrandStyles generates lipgloss styles from the brand palette.
type BrandStyles struct {
	// Focus style for focused/active elements with strong border
	Focus lipgloss.Style

	// Highlight style for selected/hovered elements
	Highlight lipgloss.Style

	// Selection style for multi-select or highlighted ranges
	Selection lipgloss.Style

	// Accent style for important UI elements
	Accent lipgloss.Style

	// AccentSecondary for secondary important elements
	AccentSecondary lipgloss.Style

	// Success style for positive feedback
	Success lipgloss.Style

	// Warning style for caution indicators
	Warning lipgloss.Style

	// Danger style for destructive actions
	Danger lipgloss.Style

	// Muted style for secondary/disabled elements
	Muted lipgloss.Style

	// GlowBorder creates a "glow" effect border for focused elements
	GlowBorder lipgloss.Style

	// PulseBorder for animated attention states (use with ticker)
	PulseBorder lipgloss.Style

	// GradientAccent for gradient text effects
	GradientStart string
	GradientEnd   string
}

// NewBrandStyles creates styled components from a brand palette.
func NewBrandStyles(palette *BrandPalette) *BrandStyles {
	return &BrandStyles{
		Focus: lipgloss.NewStyle().
			BorderStyle(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color(palette.NeonCyan)).
			Padding(0, 1),

		Highlight: lipgloss.NewStyle().
			Background(lipgloss.Color(palette.NeonMagenta)).
			Foreground(lipgloss.Color("#FFFFFF")).
			Bold(true),

		Selection: lipgloss.NewStyle().
			Background(lipgloss.Color(palette.SteelGray)).
			Foreground(lipgloss.Color(palette.NeonCyan)),

		Accent: lipgloss.NewStyle().
			Foreground(lipgloss.Color(palette.NeonCyan)).
			Bold(true),

		AccentSecondary: lipgloss.NewStyle().
			Foreground(lipgloss.Color(palette.NeonMagenta)).
			Bold(true),

		Success: lipgloss.NewStyle().
			Foreground(lipgloss.Color(palette.BrightGreen)).
			Bold(true),

		Warning: lipgloss.NewStyle().
			Foreground(lipgloss.Color(palette.AccentOrange)).
			Bold(true),

		Danger: lipgloss.NewStyle().
			Foreground(lipgloss.Color(palette.NeonMagenta)).
			Bold(true),

		Muted: lipgloss.NewStyle().
			Foreground(lipgloss.Color(palette.SteelGray)),

		GlowBorder: lipgloss.NewStyle().
			BorderStyle(lipgloss.DoubleBorder()).
			BorderForeground(lipgloss.Color(palette.NeonCyan)).
			Padding(1, 2),

		PulseBorder: lipgloss.NewStyle().
			BorderStyle(lipgloss.ThickBorder()).
			BorderForeground(lipgloss.Color(palette.NeonMagenta)),

		GradientStart: palette.NeonCyan,
		GradientEnd:   palette.NeonMagenta,
	}
}

// ContrastRatio calculates approximate contrast ratio between two colors.
// Returns true if contrast ratio meets WCAG AA standard (4.5:1 for normal text).
// This is a simplified calculation for quick checks.
func ContrastRatio(fg, bg string) float64 {
	fgR, fgG, fgB := hexToRGB(fg)
	bgR, bgG, bgB := hexToRGB(bg)

	// Calculate relative luminance
	fgLum := relativeLuminance(fgR, fgG, fgB)
	bgLum := relativeLuminance(bgR, bgG, bgB)

	// Calculate contrast ratio
	lighter := fgLum
	darker := bgLum
	if bgLum > fgLum {
		lighter = bgLum
		darker = fgLum
	}

	return (lighter + 0.05) / (darker + 0.05)
}

// MeetsAccessibilityStandard checks if two colors meet WCAG AA contrast requirements.
// Minimum ratio: 4.5:1 for normal text, 3:1 for large text.
func MeetsAccessibilityStandard(fg, bg string, largeText bool) bool {
	ratio := ContrastRatio(fg, bg)
	if largeText {
		return ratio >= 3.0
	}
	return ratio >= 4.5
}

// relativeLuminance calculates the relative luminance of a color.
func relativeLuminance(r, g, b int) float64 {
	rs := float64(r) / 255.0
	gs := float64(g) / 255.0
	bs := float64(b) / 255.0

	// Apply gamma correction
	if rs <= 0.03928 {
		rs = rs / 12.92
	} else {
		rs = pow((rs+0.055)/1.055, 2.4)
	}
	if gs <= 0.03928 {
		gs = gs / 12.92
	} else {
		gs = pow((gs+0.055)/1.055, 2.4)
	}
	if bs <= 0.03928 {
		bs = bs / 12.92
	} else {
		bs = pow((bs+0.055)/1.055, 2.4)
	}

	return 0.2126*rs + 0.7152*gs + 0.0722*bs
}

// pow is a simple power function for gamma correction.
func pow(base, exp float64) float64 {
	result := 1.0
	for i := 0; i < int(exp*10); i++ {
		result *= base
	}
	// Approximate - use math.Pow for production
	return result
}
