package display

import "fmt"

// PromptBuilder constructs REPL prompts with context information.
type PromptBuilder struct {
	Database       string // Current database context
	InTransaction  bool   // Whether in an explicit transaction
	User           string // Current authenticated user
	ReadyPrompt    string // Primary prompt
	ContinuePrompt string // Multi-line continuation prompt
}

// NewPromptBuilder creates a new PromptBuilder with default prompts.
func NewPromptBuilder() *PromptBuilder {
	return &PromptBuilder{
		ReadyPrompt:    "veridical> ",
		ContinuePrompt: "       ... ",
	}
}

// Build constructs the appropriate prompt string based on current context.
func (pb *PromptBuilder) Build() string {
	if pb.Database != "" {
		if pb.InTransaction {
			return fmt.Sprintf("veridical[%s] [tx]> ", pb.Database)
		}
		return fmt.Sprintf("veridical[%s]> ", pb.Database)
	}

	if pb.InTransaction {
		return "veridical [tx]> "
	}

	return pb.ReadyPrompt
}

// BuildContinuation returns the prompt for multi-line statement continuation.
func (pb *PromptBuilder) BuildContinuation() string {
	return pb.ContinuePrompt
}

// SetDatabase updates the current database context.
func (pb *PromptBuilder) SetDatabase(db string) {
	pb.Database = db
}

// SetTransaction sets whether we're in a transaction.
func (pb *PromptBuilder) SetTransaction(active bool) {
	pb.InTransaction = active
}

// SetUser updates the current user context.
func (pb *PromptBuilder) SetUser(user string) {
	pb.User = user
}

// SetReadyPrompt customizes the default ready prompt.
func (pb *PromptBuilder) SetReadyPrompt(prompt string) {
	pb.ReadyPrompt = prompt
}

// SetContinuePrompt customizes the continuation prompt.
func (pb *PromptBuilder) SetContinuePrompt(prompt string) {
	pb.ContinuePrompt = prompt
}
