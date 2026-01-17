package screens

import "github.com/JayabrataBasu/VeridicalDB/internal/tui/types"

// Re-export message types from types package for convenience
type (
	Screen            = types.Screen
	ScreenChangeMsg   = types.ScreenChangeMsg
	ExecuteQueryMsg   = types.ExecuteQueryMsg
	QueryCompletedMsg = types.QueryCompletedMsg
	QueryResult       = types.QueryResult
	StatusMsg         = types.StatusMsg
	ErrorMsg          = types.ErrorMsg
)
