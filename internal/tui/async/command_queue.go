// Package async provides context-aware asynchronous command execution for TUI operations.
package async

import (
	"context"
	"errors"
	"sync"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

// Default timeouts for different operation types.
const (
	DefaultQueryTimeout  = 30 * time.Second
	DefaultBackupTimeout = 5 * time.Minute
	DefaultUserOpTimeout = 10 * time.Second
	DefaultShortTimeout  = 5 * time.Second
)

// CommandStatus represents the status of a command execution.
type CommandStatus string

const (
	StatusPending   CommandStatus = "pending"
	StatusRunning   CommandStatus = "running"
	StatusCompleted CommandStatus = "completed"
	StatusFailed    CommandStatus = "failed"
	StatusCancelled CommandStatus = "cancelled"
	StatusTimeout   CommandStatus = "timeout"
)

// CommandType categorizes commands for priority and timeout handling.
type CommandType string

const (
	TypeQuery   CommandType = "query"
	TypeBackup  CommandType = "backup"
	TypeRestore CommandType = "restore"
	TypeUserOp  CommandType = "user_op"
	TypeMetrics CommandType = "metrics"
	TypeGeneral CommandType = "general"
)

// Command represents an async operation to be executed.
type Command struct {
	ID          string
	Type        CommandType
	Description string
	Execute     func(ctx context.Context) (interface{}, error)
	OnComplete  func(result interface{}, err error) tea.Msg
	Timeout     time.Duration
	Priority    int // Higher = more urgent
	CreatedAt   time.Time
}

// CommandResult holds the result of a command execution.
type CommandResult struct {
	CommandID   string
	Status      CommandStatus
	Result      interface{}
	Error       error
	StartedAt   time.Time
	CompletedAt time.Time
	Duration    time.Duration
}

// CommandResultMsg is a Bubble Tea message for command completion.
type CommandResultMsg struct {
	CommandID string
	Result    interface{}
	Error     error
	Status    CommandStatus
}

// CommandProgressMsg is a Bubble Tea message for progress updates.
type CommandProgressMsg struct {
	CommandID string
	Progress  float64 // 0.0 to 1.0
	Message   string
}

// CommandQueue manages async command execution with proper lifecycle management.
type CommandQueue struct {
	mu         sync.RWMutex
	commands   chan *queuedCommand
	results    map[string]*CommandResult
	active     map[string]context.CancelFunc
	maxWorkers int
	workerWg   sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	msgChan    chan tea.Msg
	started    bool
	stopped    bool
}

type queuedCommand struct {
	command *Command
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewCommandQueue creates a new command queue with the specified number of workers.
func NewCommandQueue(maxWorkers int, msgChan chan tea.Msg) *CommandQueue {
	if maxWorkers <= 0 {
		maxWorkers = 3
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &CommandQueue{
		commands:   make(chan *queuedCommand, 100),
		results:    make(map[string]*CommandResult),
		active:     make(map[string]context.CancelFunc),
		maxWorkers: maxWorkers,
		ctx:        ctx,
		cancel:     cancel,
		msgChan:    msgChan,
	}
}

// Start begins processing commands with the configured number of workers.
func (q *CommandQueue) Start() {
	q.mu.Lock()
	if q.started {
		q.mu.Unlock()
		return
	}
	q.started = true
	q.mu.Unlock()

	for i := 0; i < q.maxWorkers; i++ {
		q.workerWg.Add(1)
		go q.worker(i)
	}
}

// Stop gracefully shuts down the command queue.
func (q *CommandQueue) Stop() {
	q.mu.Lock()
	if q.stopped {
		q.mu.Unlock()
		return
	}
	q.stopped = true
	q.mu.Unlock()

	// Cancel all pending operations
	q.cancel()

	// Wait for workers to finish with timeout
	done := make(chan struct{})
	go func() {
		q.workerWg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		// Force shutdown after timeout
	}

	close(q.commands)
}

// QueueCommand adds a command to the queue for execution.
func (q *CommandQueue) QueueCommand(cmd *Command) error {
	q.mu.RLock()
	if q.stopped {
		q.mu.RUnlock()
		return errors.New("command queue is stopped")
	}
	q.mu.RUnlock()

	if cmd.ID == "" {
		return errors.New("command ID is required")
	}

	if cmd.Execute == nil {
		return errors.New("command Execute function is required")
	}

	if cmd.Timeout == 0 {
		cmd.Timeout = q.defaultTimeoutForType(cmd.Type)
	}

	if cmd.CreatedAt.IsZero() {
		cmd.CreatedAt = time.Now()
	}

	// Create command-specific context with timeout
	ctx, cancel := context.WithTimeout(q.ctx, cmd.Timeout)

	// Track active command
	q.mu.Lock()
	q.active[cmd.ID] = cancel
	q.results[cmd.ID] = &CommandResult{
		CommandID: cmd.ID,
		Status:    StatusPending,
	}
	q.mu.Unlock()

	// Queue the command
	select {
	case q.commands <- &queuedCommand{
		command: cmd,
		ctx:     ctx,
		cancel:  cancel,
	}:
		return nil
	case <-q.ctx.Done():
		cancel()
		return errors.New("command queue is shutting down")
	default:
		cancel()
		return errors.New("command queue is full")
	}
}

// CancelCommand cancels a running command by ID.
func (q *CommandQueue) CancelCommand(id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	cancel, ok := q.active[id]
	if !ok {
		return errors.New("command not found or already completed")
	}

	cancel()
	return nil
}

// GetResult returns the result of a command by ID.
func (q *CommandQueue) GetResult(id string) (*CommandResult, bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	result, ok := q.results[id]
	if !ok {
		return nil, false
	}

	// Return a copy
	copy := *result
	return &copy, true
}

// GetStatus returns the status of a command by ID.
func (q *CommandQueue) GetStatus(id string) (CommandStatus, bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	result, ok := q.results[id]
	if !ok {
		return "", false
	}

	return result.Status, true
}

// GetActiveCount returns the number of currently executing commands.
func (q *CommandQueue) GetActiveCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()

	count := 0
	for _, result := range q.results {
		if result.Status == StatusRunning {
			count++
		}
	}
	return count
}

// GetPendingCount returns the number of pending commands.
func (q *CommandQueue) GetPendingCount() int {
	return len(q.commands)
}

// worker processes commands from the queue.
func (q *CommandQueue) worker(id int) {
	defer q.workerWg.Done()

	for {
		select {
		case <-q.ctx.Done():
			return
		case qc, ok := <-q.commands:
			if !ok {
				return
			}
			q.executeCommand(qc)
		}
	}
}

// executeCommand executes a single command with proper lifecycle management.
func (q *CommandQueue) executeCommand(qc *queuedCommand) {
	cmd := qc.command
	startTime := time.Now()

	// Update status to running
	q.mu.Lock()
	if result, ok := q.results[cmd.ID]; ok {
		result.Status = StatusRunning
		result.StartedAt = startTime
	}
	q.mu.Unlock()

	// Execute the command
	var result interface{}
	var err error
	var status CommandStatus

	// Use a channel to capture the result
	done := make(chan struct{})
	go func() {
		defer close(done)
		result, err = cmd.Execute(qc.ctx)
	}()

	// Wait for completion or context cancellation
	select {
	case <-done:
		if err != nil {
			status = StatusFailed
		} else {
			status = StatusCompleted
		}
	case <-qc.ctx.Done():
		if errors.Is(qc.ctx.Err(), context.DeadlineExceeded) {
			status = StatusTimeout
			err = errors.New("command timed out")
		} else {
			status = StatusCancelled
			err = errors.New("command was cancelled")
		}
	}

	completedAt := time.Now()
	duration := completedAt.Sub(startTime)

	// Update result
	q.mu.Lock()
	if r, ok := q.results[cmd.ID]; ok {
		r.Status = status
		r.Result = result
		r.Error = err
		r.CompletedAt = completedAt
		r.Duration = duration
	}
	delete(q.active, cmd.ID)
	q.mu.Unlock()

	// Clean up context
	qc.cancel()

	// Send result message
	if q.msgChan != nil {
		msg := CommandResultMsg{
			CommandID: cmd.ID,
			Result:    result,
			Error:     err,
			Status:    status,
		}

		// Use non-blocking send
		select {
		case q.msgChan <- msg:
		default:
			// Channel full, drop message
		}
	}

	// Call completion callback if provided
	if cmd.OnComplete != nil {
		if teaMsg := cmd.OnComplete(result, err); teaMsg != nil && q.msgChan != nil {
			select {
			case q.msgChan <- teaMsg:
			default:
			}
		}
	}
}

// defaultTimeoutForType returns the default timeout for a command type.
func (q *CommandQueue) defaultTimeoutForType(cmdType CommandType) time.Duration {
	switch cmdType {
	case TypeQuery:
		return DefaultQueryTimeout
	case TypeBackup, TypeRestore:
		return DefaultBackupTimeout
	case TypeUserOp:
		return DefaultUserOpTimeout
	case TypeMetrics:
		return DefaultShortTimeout
	default:
		return DefaultQueryTimeout
	}
}

// CleanupOldResults removes results older than the specified duration.
func (q *CommandQueue) CleanupOldResults(maxAge time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()

	cutoff := time.Now().Add(-maxAge)
	for id, result := range q.results {
		if result.Status != StatusRunning && result.Status != StatusPending {
			if result.CompletedAt.Before(cutoff) {
				delete(q.results, id)
			}
		}
	}
}
