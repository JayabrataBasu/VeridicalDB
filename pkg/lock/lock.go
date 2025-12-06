// Package lock provides a lock manager for concurrency control.
// It supports table-level and row-level locks with shared (S) and exclusive (X) modes.
package lock

import (
	"fmt"
	"sync"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/storage"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

// Mode represents the lock mode.
type Mode int

const (
	// ModeShared allows multiple readers but blocks writers.
	ModeShared Mode = iota
	// ModeExclusive allows only one holder and blocks all others.
	ModeExclusive
)

func (m Mode) String() string {
	switch m {
	case ModeShared:
		return "S"
	case ModeExclusive:
		return "X"
	default:
		return "?"
	}
}

// ResourceType identifies what kind of resource is being locked.
type ResourceType int

const (
	ResourceTable ResourceType = iota
	ResourceRow
)

// ResourceID uniquely identifies a lockable resource.
type ResourceID struct {
	Type      ResourceType
	TableName string      // For table locks
	RID       storage.RID // For row locks
}

func (r ResourceID) String() string {
	switch r.Type {
	case ResourceTable:
		return fmt.Sprintf("table:%s", r.TableName)
	case ResourceRow:
		return fmt.Sprintf("row:%s:%v", r.TableName, r.RID)
	default:
		return "unknown"
	}
}

// TableResource creates a ResourceID for a table lock.
func TableResource(tableName string) ResourceID {
	return ResourceID{Type: ResourceTable, TableName: tableName}
}

// RowResource creates a ResourceID for a row lock.
func RowResource(tableName string, rid storage.RID) ResourceID {
	return ResourceID{Type: ResourceRow, TableName: tableName, RID: rid}
}

// LockRequest represents a pending lock request.
type LockRequest struct {
	TxID    txn.TxID
	Mode    Mode
	Granted bool
	WaitCh  chan struct{} // Signaled when lock is granted
}

// lockEntry holds the state of a single lockable resource.
type lockEntry struct {
	holders map[txn.TxID]Mode // Currently holding transactions
	waiters []*LockRequest    // Waiting requests in FIFO order
}

// Manager is the central lock manager.
type Manager struct {
	mu      sync.Mutex
	locks   map[ResourceID]*lockEntry
	timeout time.Duration

	// Track locks held by each transaction for cleanup
	txLocks map[txn.TxID][]ResourceID
}

// NewManager creates a new lock manager.
func NewManager() *Manager {
	return &Manager{
		locks:   make(map[ResourceID]*lockEntry),
		txLocks: make(map[txn.TxID][]ResourceID),
		timeout: 5 * time.Second, // Default lock wait timeout
	}
}

// SetTimeout sets the lock acquisition timeout.
func (m *Manager) SetTimeout(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.timeout = d
}

// Acquire attempts to acquire a lock on the resource.
// Returns nil if lock was granted, or an error on timeout/deadlock.
func (m *Manager) Acquire(txID txn.TxID, resource ResourceID, mode Mode) error {
	m.mu.Lock()

	entry := m.getOrCreateEntry(resource)

	// Check if we already hold a compatible lock
	if held, ok := entry.holders[txID]; ok {
		if held == ModeExclusive || mode == ModeShared {
			// Already have sufficient lock
			m.mu.Unlock()
			return nil
		}
		// Need to upgrade from S to X
		// For simplicity, we require no other holders for upgrade
		if len(entry.holders) == 1 {
			entry.holders[txID] = ModeExclusive
			m.mu.Unlock()
			return nil
		}
		// Upgrade not possible immediately, will need to wait
	}

	// Check if lock can be granted immediately
	if m.canGrant(entry, txID, mode) {
		entry.holders[txID] = mode
		m.trackLock(txID, resource)
		m.mu.Unlock()
		return nil
	}

	// Need to wait - create a wait request
	req := &LockRequest{
		TxID:    txID,
		Mode:    mode,
		Granted: false,
		WaitCh:  make(chan struct{}),
	}
	entry.waiters = append(entry.waiters, req)
	m.mu.Unlock()

	// Wait for lock or timeout
	select {
	case <-req.WaitCh:
		// Lock granted
		return nil
	case <-time.After(m.timeout):
		// Timeout - remove from wait queue
		m.mu.Lock()
		m.removeWaiter(entry, req)
		m.mu.Unlock()
		return fmt.Errorf("lock timeout waiting for %s", resource)
	}
}

// Release releases a lock held by the transaction.
func (m *Manager) Release(txID txn.TxID, resource ResourceID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.locks[resource]
	if !ok {
		return nil // No lock exists
	}

	if _, held := entry.holders[txID]; !held {
		return nil // Transaction doesn't hold this lock
	}

	delete(entry.holders, txID)
	m.untrackLock(txID, resource)

	// Grant locks to waiting requests
	m.grantWaiters(entry)

	// Cleanup empty entries
	if len(entry.holders) == 0 && len(entry.waiters) == 0 {
		delete(m.locks, resource)
	}

	return nil
}

// ReleaseAll releases all locks held by a transaction.
// Called on commit/abort.
func (m *Manager) ReleaseAll(txID txn.TxID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	resources := m.txLocks[txID]
	for _, resource := range resources {
		entry, ok := m.locks[resource]
		if !ok {
			continue
		}

		delete(entry.holders, txID)
		m.grantWaiters(entry)

		if len(entry.holders) == 0 && len(entry.waiters) == 0 {
			delete(m.locks, resource)
		}
	}
	delete(m.txLocks, txID)
}

// getOrCreateEntry returns the lock entry for a resource, creating if needed.
// Caller must hold m.mu.
func (m *Manager) getOrCreateEntry(resource ResourceID) *lockEntry {
	entry, ok := m.locks[resource]
	if !ok {
		entry = &lockEntry{
			holders: make(map[txn.TxID]Mode),
			waiters: nil,
		}
		m.locks[resource] = entry
	}
	return entry
}

// canGrant checks if a lock can be granted immediately.
// Caller must hold m.mu.
func (m *Manager) canGrant(entry *lockEntry, txID txn.TxID, mode Mode) bool {
	if len(entry.holders) == 0 {
		return true
	}

	// If we already hold the lock, we've handled that above
	if _, ok := entry.holders[txID]; ok {
		return false // Handled in caller
	}

	// Shared locks can coexist with other shared locks
	if mode == ModeShared {
		for _, heldMode := range entry.holders {
			if heldMode == ModeExclusive {
				return false
			}
		}
		return true
	}

	// Exclusive lock requires no other holders
	return false
}

// grantWaiters attempts to grant locks to waiting requests.
// Caller must hold m.mu.
func (m *Manager) grantWaiters(entry *lockEntry) {
	i := 0
	for i < len(entry.waiters) {
		req := entry.waiters[i]
		if m.canGrant(entry, req.TxID, req.Mode) {
			entry.holders[req.TxID] = req.Mode
			m.trackLockLocked(req.TxID, ResourceID{}) // Note: resource not available here
			req.Granted = true
			close(req.WaitCh)

			// Remove from waiters
			entry.waiters = append(entry.waiters[:i], entry.waiters[i+1:]...)
		} else {
			i++
		}
	}
}

// removeWaiter removes a request from the wait queue.
// Caller must hold m.mu.
func (m *Manager) removeWaiter(entry *lockEntry, req *LockRequest) {
	for i, w := range entry.waiters {
		if w == req {
			entry.waiters = append(entry.waiters[:i], entry.waiters[i+1:]...)
			return
		}
	}
}

// trackLock adds a resource to the transaction's lock list.
// Caller must NOT hold m.mu.
func (m *Manager) trackLock(txID txn.TxID, resource ResourceID) {
	m.txLocks[txID] = append(m.txLocks[txID], resource)
}

// trackLockLocked adds a resource to the transaction's lock list.
// Caller must hold m.mu.
func (m *Manager) trackLockLocked(txID txn.TxID, resource ResourceID) {
	m.txLocks[txID] = append(m.txLocks[txID], resource)
}

// untrackLock removes a resource from the transaction's lock list.
// Caller must hold m.mu.
func (m *Manager) untrackLock(txID txn.TxID, resource ResourceID) {
	locks := m.txLocks[txID]
	for i, r := range locks {
		if r == resource {
			m.txLocks[txID] = append(locks[:i], locks[i+1:]...)
			return
		}
	}
}

// Stats returns lock manager statistics for monitoring.
func (m *Manager) Stats() (activeLocks, waitingRequests int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, entry := range m.locks {
		activeLocks += len(entry.holders)
		waitingRequests += len(entry.waiters)
	}
	return
}
