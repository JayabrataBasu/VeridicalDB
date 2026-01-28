// Package tui provides the Bubble Tea Terminal User Interface for VeridicalDB.
package tui

import (
	"github.com/JayabrataBasu/VeridicalDB/internal/tui/types"
)

// Re-export from types package for backward compatibility within tui package
var Icons = &types.Icons

// IconMode re-exports
type IconMode = types.IconMode

const (
	IconModeASCII    = types.IconModeASCII
	IconModeUnicode  = types.IconModeUnicode
	IconModeNerdFont = types.IconModeNerdFont
)

// SetIconMode changes the global icon set
func SetIconMode(mode IconMode) {
	types.SetIconMode(mode)
}

// InitIcons initializes the icon system
func InitIcons() {
	types.InitIcons()
}
