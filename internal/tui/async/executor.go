// Package async provides context-aware asynchronous command execution for TUI operations.
package async

import (
	"context"
	"fmt"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/google/uuid"
)

// Executor provides a simplified interface for common async operations.
type Executor struct {
	queue *CommandQueue
}

// NewExecutor creates a new executor with the given command queue.
func NewExecutor(queue *CommandQueue) *Executor {
	return &Executor{queue: queue}
}

// ExecuteQuery queues a database query for async execution.
func (e *Executor) ExecuteQuery(
	queryText string,
	execute func(ctx context.Context) (interface{}, error),
	onComplete func(result interface{}, err error) tea.Msg,
) (string, error) {
	cmd := &Command{
		ID:          fmt.Sprintf("query-%s", uuid.New().String()[:8]),
		Type:        TypeQuery,
		Description: truncateString(queryText, 50),
		Execute:     execute,
		OnComplete:  onComplete,
		Timeout:     DefaultQueryTimeout,
	}

	return cmd.ID, e.queue.QueueCommand(cmd)
}

// ExecuteQueryWithTimeout queues a query with a custom timeout.
func (e *Executor) ExecuteQueryWithTimeout(
	queryText string,
	timeout time.Duration,
	execute func(ctx context.Context) (interface{}, error),
	onComplete func(result interface{}, err error) tea.Msg,
) (string, error) {
	cmd := &Command{
		ID:          fmt.Sprintf("query-%s", uuid.New().String()[:8]),
		Type:        TypeQuery,
		Description: truncateString(queryText, 50),
		Execute:     execute,
		OnComplete:  onComplete,
		Timeout:     timeout,
	}

	return cmd.ID, e.queue.QueueCommand(cmd)
}

// ExecuteBackup will queue a backup operation for async execution.
func (e *Executor) ExecuteBackup(
	database string,
	execute func(ctx context.Context) (interface{}, error),
	onComplete func(result interface{}, err error) tea.Msg,
) (string, error) {
	cmd := &Command{
		ID:          fmt.Sprintf("backup-%s", uuid.New().String()[:8]),
		Type:        TypeBackup,
		Description: fmt.Sprintf("Backup database: %s", database),
		Execute:     execute,
		OnComplete:  onComplete,
		Timeout:     DefaultBackupTimeout,
		Priority:    10, // High priority
	}

	return cmd.ID, e.queue.QueueCommand(cmd)
}

// ExecuteRestore queues a restore operation for async execution.
func (e *Executor) ExecuteRestore(
	backupID string,
	execute func(ctx context.Context) (interface{}, error),
	onComplete func(result interface{}, err error) tea.Msg,
) (string, error) {
	cmd := &Command{
		ID:          fmt.Sprintf("restore-%s", uuid.New().String()[:8]),
		Type:        TypeRestore,
		Description: fmt.Sprintf("Restore from backup: %s", backupID),
		Execute:     execute,
		OnComplete:  onComplete,
		Timeout:     DefaultBackupTimeout,
		Priority:    10, // High priority
	}

	return cmd.ID, e.queue.QueueCommand(cmd)
}

// ExecuteUserOp queues a user management operation for async execution.
func (e *Executor) ExecuteUserOp(
	operation string,
	targetUser string,
	execute func(ctx context.Context) (interface{}, error),
	onComplete func(result interface{}, err error) tea.Msg,
) (string, error) {
	cmd := &Command{
		ID:          fmt.Sprintf("user-%s", uuid.New().String()[:8]),
		Type:        TypeUserOp,
		Description: fmt.Sprintf("%s user: %s", operation, targetUser),
		Execute:     execute,
		OnComplete:  onComplete,
		Timeout:     DefaultUserOpTimeout,
	}

	return cmd.ID, e.queue.QueueCommand(cmd)
}

// ExecuteMetrics queues a metrics fetch operation for async execution.
func (e *Executor) ExecuteMetrics(
	execute func(ctx context.Context) (interface{}, error),
	onComplete func(result interface{}, err error) tea.Msg,
) (string, error) {
	cmd := &Command{
		ID:          fmt.Sprintf("metrics-%s", uuid.New().String()[:8]),
		Type:        TypeMetrics,
		Description: "Fetch system metrics",
		Execute:     execute,
		OnComplete:  onComplete,
		Timeout:     DefaultShortTimeout,
	}

	return cmd.ID, e.queue.QueueCommand(cmd)
}

// ExecuteGeneral queues a general operation for async execution.
func (e *Executor) ExecuteGeneral(
	id string,
	description string,
	timeout time.Duration,
	execute func(ctx context.Context) (interface{}, error),
	onComplete func(result interface{}, err error) tea.Msg,
) (string, error) {
	if id == "" {
		id = fmt.Sprintf("general-%s", uuid.New().String()[:8])
	}

	cmd := &Command{ //this is trivial
		ID:          id,
		Type:        TypeGeneral,
		Description: description,
		Execute:     execute,
		OnComplete:  onComplete,
		Timeout:     timeout,
	}

	return cmd.ID, e.queue.QueueCommand(cmd)
}

// Cancel cancels a running command by ID.
func (e *Executor) Cancel(commandID string) error {
	return e.queue.CancelCommand(commandID)
}

// GetStatus returns the status of a command.
func (e *Executor) GetStatus(commandID string) (CommandStatus, bool) {
	return e.queue.GetStatus(commandID)
}

// GetResult returns the result of a command.
func (e *Executor) GetResult(commandID string) (*CommandResult, bool) {
	return e.queue.GetResult(commandID)
}

// IsRunning checks if a command is still running.
func (e *Executor) IsRunning(commandID string) bool {
	status, ok := e.queue.GetStatus(commandID)
	if !ok {
		return false
	}
	return status == StatusRunning || status == StatusPending
}

// WaitForCompletion blocks until the command completes or timeout.
func (e *Executor) WaitForCompletion(commandID string, timeout time.Duration) (*CommandResult, error) {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		status, ok := e.queue.GetStatus(commandID)
		if !ok {
			return nil, fmt.Errorf("command %s not found", commandID)
		}

		if status != StatusPending && status != StatusRunning {
			result, _ := e.queue.GetResult(commandID)
			return result, nil
		}

		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timeout waiting for command %s", commandID)
		}
	}
	return nil, fmt.Errorf("ticker stopped unexpectedly for command %s", commandID)
}

// Helper functions

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}

// TickerCommand creates a command that ticks at regular intervals.
// Useful for periodic updates like metrics refresh.
// ticker ensures that you do not suffer perpetually
type TickerCommand struct {
	ID       string
	Interval time.Duration
	ticker   *time.Ticker
	ctx      context.Context
	cancel   context.CancelFunc
	msgChan  chan tea.Msg
}

// NewTickerCommand creates a new ticker command.
func NewTickerCommand(id string, interval time.Duration, msgChan chan tea.Msg) *TickerCommand {
	ctx, cancel := context.WithCancel(context.Background())
	return &TickerCommand{
		ID:       id,
		Interval: interval,
		ctx:      ctx,
		cancel:   cancel,
		msgChan:  msgChan,
	}
}

// TickMsg is sent on each tick.
type TickMsg struct {
	ID        string
	Timestamp time.Time
}

// Start begins the ticker.
func (tc *TickerCommand) Start() {
	tc.ticker = time.NewTicker(tc.Interval)

	go func() {
		for {
			select {
			case <-tc.ctx.Done():
				tc.ticker.Stop()
				return
			case t := <-tc.ticker.C:
				select {
				case tc.msgChan <- TickMsg{ID: tc.ID, Timestamp: t}:
				default:
					// Channel full, skip tick
				}
			}
		}
	}()
}

// Stop stops the ticker.
func (tc *TickerCommand) Stop() {
	tc.cancel()
}

// Reset resets the ticker interval.
func (tc *TickerCommand) Reset(interval time.Duration) {
	tc.Interval = interval
	if tc.ticker != nil {
		tc.ticker.Reset(interval)
	}
}
