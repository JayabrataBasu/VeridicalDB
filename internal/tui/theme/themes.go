package theme

// darkTheme provides a dark color scheme.
func darkTheme() *Theme {
	palette := DefaultBrandPalette()
	t := &Theme{
		Name:        "dark",
		Description: "Default dark theme",

		Foreground: "#E0E0E0",
		Background: "#1A1A1A",

		Primary:   "#00D9FF",
		Secondary: "#00AA88",
		Accent:    "#FFB86C",
		Muted:     "#8A93A5",

		Success: "#55FF55",
		Warning: "#FFAA00",
		Error:   "#FF5555",
		Info:    "#8BE9FD",

		Border:        "#4A4E5A",
		BorderFocused: "#00D9FF",
		Selection:     "#264F78",
		Highlight:     "#00D9FF",
		LineNumber:    "#6B7280",
		Cursor:        "#00D9FF",
		CurrentLine:   "#2A2A2A",
		Comment:       "#6272A4",

		Keyword:  "#FF79C6",
		String:   "#F1FA8C",
		Number:   "#BD93F9",
		Function: "#50FA7B",
		Operator: "#FF79C6",
		Variable: "#8BE9FD",
		Type:     "#FFB86C",
		Constant: "#BD93F9",

		TableHeader:    "#00D9FF",
		TableBorder:    "#3A3A3A",
		TableRowEven:   "#1A1A1A",
		TableRowOdd:    "#222222",
		TableSelected:  "#264F78",
		TableHighlight: "#00D9FF",

		// Brand colors - Bold tech aesthetic
		BrandAccent:    palette.NeonCyan,
		BrandHighlight: palette.NeonMagenta,
		BrandSelection: palette.SteelGray,
		BrandFocus:     palette.NeonCyan,
		BrandSuccess:   palette.BrightGreen,
		BrandWarning:   palette.AccentOrange,
		BrandDanger:    palette.NeonMagenta,
		BrandMuted:     palette.SteelGray,
		BrandGlow:      palette.NeonCyan,
		BrandGradientA: palette.NeonCyan,
		BrandGradientB: palette.NeonMagenta,
	}
	t.SetBrandPalette(palette)
	return t
}

// lightTheme provides a light color scheme.
func lightTheme() *Theme {
	palette := LightBrandPalette()
	t := &Theme{
		Name:        "light",
		Description: "Light theme with high contrast",

		Foreground: "#24292E",
		Background: "#FFFFFF",

		Primary:   "#0366D6",
		Secondary: "#28A745",
		Accent:    "#F66A0A",
		Muted:     "#6A737D",

		Success: "#28A745",
		Warning: "#FFC107",
		Error:   "#D73A49",
		Info:    "#0366D6",

		Border:        "#E1E4E8",
		BorderFocused: "#0366D6",
		Selection:     "#C8E1FF",
		Highlight:     "#FFEA7F",
		LineNumber:    "#959DA5",
		Cursor:        "#0366D6",
		CurrentLine:   "#F6F8FA",
		Comment:       "#6A737D",

		Keyword:  "#D73A49",
		String:   "#032F62",
		Number:   "#005CC5",
		Function: "#6F42C1",
		Operator: "#D73A49",
		Variable: "#24292E",
		Type:     "#005CC5",
		Constant: "#005CC5",

		TableHeader:    "#0366D6",
		TableBorder:    "#E1E4E8",
		TableRowEven:   "#FFFFFF",
		TableRowOdd:    "#F6F8FA",
		TableSelected:  "#C8E1FF",
		TableHighlight: "#0366D6",

		// Brand colors - Bold tech aesthetic (adjusted for light)
		BrandAccent:    palette.NeonCyan,
		BrandHighlight: palette.NeonMagenta,
		BrandSelection: "#E1E4E8",
		BrandFocus:     palette.NeonCyan,
		BrandSuccess:   palette.BrightGreen,
		BrandWarning:   palette.AccentOrange,
		BrandDanger:    palette.NeonMagenta,
		BrandMuted:     palette.SteelGray,
		BrandGlow:      palette.NeonCyan,
		BrandGradientA: palette.NeonCyan,
		BrandGradientB: palette.NeonMagenta,
	}
	t.SetBrandPalette(palette)
	return t
}

// draculaTheme provides the popular Dracula color scheme.
func draculaTheme() *Theme {
	palette := DraculaBrandPalette()
	t := &Theme{
		Name:        "dracula",
		Description: "Dracula theme - a dark theme for many editors",

		Foreground: "#F8F8F2",
		Background: "#282A36",

		Primary:   "#BD93F9",
		Secondary: "#50FA7B",
		Accent:    "#FFB86C",
		Muted:     "#7A85C0",

		Success: "#50FA7B",
		Warning: "#F1FA8C",
		Error:   "#FF5555",
		Info:    "#8BE9FD",

		Border:        "#565A70",
		BorderFocused: "#BD93F9",
		Selection:     "#44475A",
		Highlight:     "#FFB86C",
		LineNumber:    "#6272A4",
		Cursor:        "#F8F8F0",
		CurrentLine:   "#44475A",
		Comment:       "#6272A4",

		Keyword:  "#FF79C6",
		String:   "#F1FA8C",
		Number:   "#BD93F9",
		Function: "#50FA7B",
		Operator: "#FF79C6",
		Variable: "#8BE9FD",
		Type:     "#FFB86C",
		Constant: "#BD93F9",

		TableHeader:    "#BD93F9",
		TableBorder:    "#44475A",
		TableRowEven:   "#282A36",
		TableRowOdd:    "#2D2F3D",
		TableSelected:  "#44475A",
		TableHighlight: "#FF79C6",

		// Brand colors - Dracula-tuned vibrant accents
		BrandAccent:    palette.NeonCyan,
		BrandHighlight: palette.NeonMagenta,
		BrandSelection: palette.SteelGray,
		BrandFocus:     palette.NeonPurple,
		BrandSuccess:   palette.BrightGreen,
		BrandWarning:   palette.AccentOrange,
		BrandDanger:    palette.NeonMagenta,
		BrandMuted:     palette.SteelGray,
		BrandGlow:      palette.NeonPurple,
		BrandGradientA: palette.NeonCyan,
		BrandGradientB: palette.NeonPurple,
	}
	t.SetBrandPalette(palette)
	return t
}

// monokaiTheme provides the popular Monokai color scheme.
func monokaiTheme() *Theme {
	palette := MonokaiBrandPalette()
	t := &Theme{
		Name:        "monokai",
		Description: "Monokai theme - sublime and vibrant",

		Foreground: "#F8F8F2",
		Background: "#272822",

		Primary:   "#F92672",
		Secondary: "#A6E22E",
		Accent:    "#FD971F",
		Muted:     "#8F8A76",

		Success: "#A6E22E",
		Warning: "#E6DB74",
		Error:   "#F92672",
		Info:    "#66D9EF",

		Border:        "#54524A",
		BorderFocused: "#F92672",
		Selection:     "#49483E",
		Highlight:     "#FD971F",
		LineNumber:    "#90908A",
		Cursor:        "#F8F8F0",
		CurrentLine:   "#3E3D32",
		Comment:       "#75715E",

		Keyword:  "#F92672",
		String:   "#E6DB74",
		Number:   "#AE81FF",
		Function: "#A6E22E",
		Operator: "#F92672",
		Variable: "#66D9EF",
		Type:     "#66D9EF",
		Constant: "#AE81FF",

		TableHeader:    "#F92672",
		TableBorder:    "#3E3D32",
		TableRowEven:   "#272822",
		TableRowOdd:    "#2D2E22",
		TableSelected:  "#49483E",
		TableHighlight: "#FD971F",

		// Brand colors - Monokai-tuned vibrant accents
		BrandAccent:    palette.NeonCyan,
		BrandHighlight: palette.NeonMagenta,
		BrandSelection: palette.SteelGray,
		BrandFocus:     palette.NeonMagenta,
		BrandSuccess:   palette.BrightGreen,
		BrandWarning:   palette.AccentOrange,
		BrandDanger:    palette.NeonMagenta,
		BrandMuted:     palette.SteelGray,
		BrandGlow:      palette.NeonMagenta,
		BrandGradientA: palette.NeonMagenta,
		BrandGradientB: palette.NeonPurple,
	}
	t.SetBrandPalette(palette)
	return t
}

// solarizedDarkTheme provides the Solarized Dark color scheme.
func solarizedDarkTheme() *Theme {
	palette := SolarizedDarkBrandPalette()
	t := &Theme{
		Name:        "solarized-dark",
		Description: "Solarized Dark - precision colors for machines and people",

		Foreground: "#839496",
		Background: "#002B36",

		Primary:   "#268BD2",
		Secondary: "#2AA198",
		Accent:    "#CB4B16",
		Muted:     "#7C929A",

		Success: "#859900",
		Warning: "#B58900",
		Error:   "#DC322F",
		Info:    "#268BD2",

		Border:        "#0E4B5C",
		BorderFocused: "#268BD2",
		Selection:     "#073642",
		Highlight:     "#B58900",
		LineNumber:    "#586E75",
		Cursor:        "#839496",
		CurrentLine:   "#073642",
		Comment:       "#586E75",

		Keyword:  "#859900",
		String:   "#2AA198",
		Number:   "#D33682",
		Function: "#268BD2",
		Operator: "#859900",
		Variable: "#268BD2",
		Type:     "#B58900",
		Constant: "#D33682",

		TableHeader:    "#268BD2",
		TableBorder:    "#073642",
		TableRowEven:   "#002B36",
		TableRowOdd:    "#013340",
		TableSelected:  "#073642",
		TableHighlight: "#2AA198",

		// Brand colors - Solarized Dark-tuned accents
		BrandAccent:    palette.NeonCyan,
		BrandHighlight: palette.NeonMagenta,
		BrandSelection: palette.SteelGray,
		BrandFocus:     palette.ElectricBlue,
		BrandSuccess:   palette.BrightGreen,
		BrandWarning:   palette.AccentOrange,
		BrandDanger:    palette.NeonMagenta,
		BrandMuted:     palette.SteelGray,
		BrandGlow:      palette.ElectricBlue,
		BrandGradientA: palette.NeonCyan,
		BrandGradientB: palette.NeonPurple,
	}
	t.SetBrandPalette(palette)
	return t
}

// solarizedLightTheme provides the Solarized Light color scheme.
func solarizedLightTheme() *Theme {
	palette := SolarizedLightBrandPalette()
	t := &Theme{
		Name:        "solarized-light",
		Description: "Solarized Light - precision colors for machines and people",

		Foreground: "#657B83",
		Background: "#FDF6E3",

		Primary:   "#268BD2",
		Secondary: "#2AA198",
		Accent:    "#CB4B16",
		Muted:     "#93A1A1",

		Success: "#859900",
		Warning: "#B58900",
		Error:   "#DC322F",
		Info:    "#268BD2",

		Border:        "#EEE8D5",
		BorderFocused: "#268BD2",
		Selection:     "#EEE8D5",
		Highlight:     "#B58900",
		LineNumber:    "#93A1A1",
		Cursor:        "#657B83",
		CurrentLine:   "#EEE8D5",
		Comment:       "#93A1A1",

		Keyword:  "#859900",
		String:   "#2AA198",
		Number:   "#D33682",
		Function: "#268BD2",
		Operator: "#859900",
		Variable: "#268BD2",
		Type:     "#B58900",
		Constant: "#D33682",

		TableHeader:    "#268BD2",
		TableBorder:    "#EEE8D5",
		TableRowEven:   "#FDF6E3",
		TableRowOdd:    "#F5EDDA",
		TableSelected:  "#EEE8D5",
		TableHighlight: "#2AA198",

		// Brand colors - Solarized Light-tuned accents
		BrandAccent:    palette.NeonCyan,
		BrandHighlight: palette.NeonMagenta,
		BrandSelection: palette.SteelGray,
		BrandFocus:     palette.ElectricBlue,
		BrandSuccess:   palette.BrightGreen,
		BrandWarning:   palette.AccentOrange,
		BrandDanger:    palette.NeonMagenta,
		BrandMuted:     palette.SteelGray,
		BrandGlow:      palette.ElectricBlue,
		BrandGradientA: palette.NeonCyan,
		BrandGradientB: palette.NeonPurple,
	}
	t.SetBrandPalette(palette)
	return t
}

// nordTheme provides the Nord color scheme.
func nordTheme() *Theme {
	palette := NordBrandPalette()
	t := &Theme{
		Name:        "nord",
		Description: "Nord - An arctic, north-bluish color palette",

		Foreground: "#D8DEE9",
		Background: "#2E3440",

		Primary:   "#88C0D0",
		Secondary: "#81A1C1",
		Accent:    "#D08770",
		Muted:     "#6E7994",

		Success: "#A3BE8C",
		Warning: "#EBCB8B",
		Error:   "#BF616A",
		Info:    "#88C0D0",

		Border:        "#4C566A",
		BorderFocused: "#88C0D0",
		Selection:     "#434C5E",
		Highlight:     "#EBCB8B",
		LineNumber:    "#4C566A",
		Cursor:        "#D8DEE9",
		CurrentLine:   "#3B4252",
		Comment:       "#616E88",

		Keyword:  "#81A1C1",
		String:   "#A3BE8C",
		Number:   "#B48EAD",
		Function: "#88C0D0",
		Operator: "#81A1C1",
		Variable: "#D8DEE9",
		Type:     "#8FBCBB",
		Constant: "#5E81AC",

		TableHeader:    "#88C0D0",
		TableBorder:    "#3B4252",
		TableRowEven:   "#2E3440",
		TableRowOdd:    "#313844",
		TableSelected:  "#434C5E",
		TableHighlight: "#81A1C1",

		// Brand colors - Nord-tuned accents (arctic palette)
		BrandAccent:    palette.NeonCyan,
		BrandHighlight: palette.NeonMagenta,
		BrandSelection: palette.SteelGray,
		BrandFocus:     palette.NeonCyan,
		BrandSuccess:   palette.BrightGreen,
		BrandWarning:   palette.AccentOrange,
		BrandDanger:    palette.NeonMagenta,
		BrandMuted:     palette.SteelGray,
		BrandGlow:      palette.NeonCyan,
		BrandGradientA: palette.NeonCyan,
		BrandGradientB: palette.NeonPurple,
	}
	t.SetBrandPalette(palette)
	return t
}

// tokyoNightTheme provides the Tokyo Night color scheme.
func tokyoNightTheme() *Theme {
	palette := TokyoNightBrandPalette()
	t := &Theme{
		Name:        "tokyo-night",
		Description: "Tokyo Night - A clean, dark theme",

		Foreground: "#C0CAF5",
		Background: "#1A1B26",

		Primary:   "#7AA2F7",
		Secondary: "#9ECE6A",
		Accent:    "#FF9E64",
		Muted:     "#7982B0",

		Success: "#9ECE6A",
		Warning: "#E0AF68",
		Error:   "#F7768E",
		Info:    "#7DCFFF",

		Border:        "#3B4261",
		BorderFocused: "#7AA2F7",
		Selection:     "#283457",
		Highlight:     "#FF9E64",
		LineNumber:    "#565F89",
		Cursor:        "#C0CAF5",
		CurrentLine:   "#24283B",
		Comment:       "#565F89",

		Keyword:  "#BB9AF7",
		String:   "#9ECE6A",
		Number:   "#FF9E64",
		Function: "#7AA2F7",
		Operator: "#89DDFF",
		Variable: "#7DCFFF",
		Type:     "#2AC3DE",
		Constant: "#FF9E64",

		TableHeader:    "#7AA2F7",
		TableBorder:    "#292E42",
		TableRowEven:   "#1A1B26",
		TableRowOdd:    "#1F202E",
		TableSelected:  "#283457",
		TableHighlight: "#BB9AF7",

		// Brand colors - Tokyo Night-tuned accents
		BrandAccent:    palette.NeonCyan,
		BrandHighlight: palette.NeonMagenta,
		BrandSelection: palette.SteelGray,
		BrandFocus:     palette.ElectricBlue,
		BrandSuccess:   palette.BrightGreen,
		BrandWarning:   palette.AccentOrange,
		BrandDanger:    palette.NeonMagenta,
		BrandMuted:     palette.SteelGray,
		BrandGlow:      palette.ElectricBlue,
		BrandGradientA: palette.ElectricBlue,
		BrandGradientB: palette.NeonPurple,
	}
	t.SetBrandPalette(palette)
	return t
}

// cyberpunkTheme provides the Deep Space Neon cyberpunk color scheme.
func cyberpunkTheme() *Theme {
	palette := CyberpunkBrandPalette()
	t := &Theme{
		Name:        "cyberpunk",
		Description: "Deep Space Neon - Radioactive gradients and living interfaces",

		Foreground: "#e6edf3",
		Background: "#0d1117",

		Primary:   "#7dce13", // Radioactive green
		Secondary: "#2a2139", // Deep purple
		Accent:    "#00f2ff", // Electric blue
		Muted:     "#8b95a3",

		Success: "#7dce13", // Radioactive green
		Warning: "#f0b429",
		Error:   "#ff5370",
		Info:    "#00f2ff", // Electric blue

		Border:        "#30363f",
		BorderFocused: "#7dce13", // Radioactive green focus
		Selection:     "#1c2938",
		Highlight:     "#bd00ff", // Neon purple
		LineNumber:    "#6e7681",
		Cursor:        "#00f2ff",
		CurrentLine:   "#161b22",
		Comment:       "#8b949e",

		Keyword:  "#ff79c6",
		String:   "#7dce13",
		Number:   "#bd00ff",
		Function: "#00f2ff",
		Operator: "#ff79c6",
		Variable: "#e6edf3",
		Type:     "#bd00ff",
		Constant: "#00f2ff",

		TableHeader:    "#7dce13",
		TableBorder:    "#21262d",
		TableRowEven:   "#0d1117",
		TableRowOdd:    "#161b22",
		TableSelected:  "#1c2938",
		TableHighlight: "#00f2ff",

		// Brand colors - Cyberpunk intensified neon accents
		BrandAccent:    palette.ElectricBlue,
		BrandHighlight: palette.NeonMagenta,
		BrandSelection: palette.SteelGray,
		BrandFocus:     palette.BrightGreen,
		BrandSuccess:   palette.BrightGreen,
		BrandWarning:   palette.AccentOrange,
		BrandDanger:    palette.NeonMagenta,
		BrandMuted:     palette.SteelGray,
		BrandGlow:      palette.NeonPurple,
		BrandGradientA: palette.ElectricBlue,
		BrandGradientB: palette.NeonPurple,
	}
	t.SetBrandPalette(palette)
	return t
}
