package tui

import "github.com/JayabrataBasu/VeridicalDB/internal/tui/types"

// Re-export types for convenience
type (
	Screen            = types.Screen
	ScreenChangeMsg   = types.ScreenChangeMsg
	ErrorMsg          = types.ErrorMsg
	StatusMsg         = types.StatusMsg
	ExecuteQueryMsg   = types.ExecuteQueryMsg
	QueryCompletedMsg = types.QueryCompletedMsg
	QueryResult       = types.QueryResult
)
