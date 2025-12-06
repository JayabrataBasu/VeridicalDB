// Package txn provides transaction management and MVCC support for VeridicalDB.
package txn

import (
	"sync"
	"sync/atomic"
)

// TxID is a unique transaction identifier.
// TxID 0 is reserved as "invalid/none".
// TxID values increase monotonically.
type TxID uint64

const (
	// InvalidTxID represents no transaction or an invalid transaction.
	InvalidTxID TxID = 0

	// FrozenTxID is a special TxID for tuples that are always visible
	// (e.g., bootstrapped/frozen tuples).
	FrozenTxID TxID = 1
)

// TxState represents the state of a transaction.
type TxState uint8

const (
	// TxInProgress indicates the transaction is still running.
	TxInProgress TxState = iota

	// TxCommitted indicates the transaction has committed successfully.
	TxCommitted

	// TxAborted indicates the transaction has been rolled back.
	TxAborted
)

func (s TxState) String() string {
	switch s {
	case TxInProgress:
		return "IN_PROGRESS"
	case TxCommitted:
		return "COMMITTED"
	case TxAborted:
		return "ABORTED"
	default:
		return "UNKNOWN"
	}
}

// Transaction represents an active database transaction.
type Transaction struct {
	ID       TxID
	State    TxState
	Snapshot *Snapshot

	// mu protects State changes
	mu sync.RWMutex
}

// IsActive returns true if the transaction is still in progress.
func (tx *Transaction) IsActive() bool {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	return tx.State == TxInProgress
}

// IsCommitted returns true if the transaction has committed.
func (tx *Transaction) IsCommitted() bool {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	return tx.State == TxCommitted
}

// IsAborted returns true if the transaction has been aborted.
func (tx *Transaction) IsAborted() bool {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	return tx.State == TxAborted
}

// Snapshot captures the state of the database at a point in time for MVCC reads.
// A snapshot determines which tuples are visible to a transaction.
type Snapshot struct {
	// XMin is the lowest transaction ID that was active when this snapshot was taken.
	// Transactions with ID < XMin are guaranteed to be finished (committed or aborted).
	XMin TxID

	// XMax is the first transaction ID that was not yet assigned when this snapshot was taken.
	// Transactions with ID >= XMax are guaranteed to have started after this snapshot.
	XMax TxID

	// InProgress is the set of transaction IDs that were in progress when this snapshot was taken.
	// These transactions' changes should not be visible regardless of their current state.
	InProgress map[TxID]struct{}
}

// NewSnapshot creates a new snapshot with the given parameters.
func NewSnapshot(xmin, xmax TxID, inProgress []TxID) *Snapshot {
	s := &Snapshot{
		XMin:       xmin,
		XMax:       xmax,
		InProgress: make(map[TxID]struct{}, len(inProgress)),
	}
	for _, txid := range inProgress {
		s.InProgress[txid] = struct{}{}
	}
	return s
}

// IsInProgress checks if a transaction was in progress when this snapshot was taken.
func (s *Snapshot) IsInProgress(txid TxID) bool {
	_, ok := s.InProgress[txid]
	return ok
}

// MVCCHeader contains the MVCC metadata for a tuple.
// This is stored as a prefix to each tuple in the heap.
type MVCCHeader struct {
	// XMin is the TxID of the transaction that created this tuple version.
	XMin TxID

	// XMax is the TxID of the transaction that deleted/updated this tuple.
	// A value of InvalidTxID (0) means the tuple has not been deleted.
	XMax TxID
}

// HeaderSize is the size of the MVCC header in bytes.
const HeaderSize = 16 // 8 bytes for XMin + 8 bytes for XMax

// IsDeleted returns true if this tuple has been marked for deletion.
func (h *MVCCHeader) IsDeleted() bool {
	return h.XMax != InvalidTxID
}

// Manager handles transaction lifecycle and provides MVCC support.
type Manager struct {
	// nextTxID is the next transaction ID to assign (atomic).
	nextTxID atomic.Uint64

	// transactions tracks all active transactions.
	transactions map[TxID]*Transaction

	// mu protects the transactions map.
	mu sync.RWMutex

	// oldestActive is the oldest transaction ID that might still be active.
	// Used for garbage collection decisions.
	oldestActive TxID
}

// NewManager creates a new transaction manager.
func NewManager() *Manager {
	m := &Manager{
		transactions: make(map[TxID]*Transaction),
		oldestActive: InvalidTxID,
	}
	// Start TxID counter at 2 (0=invalid, 1=frozen)
	m.nextTxID.Store(2)
	return m
}

// Begin starts a new transaction and returns it.
func (m *Manager) Begin() *Transaction {
	txid := TxID(m.nextTxID.Add(1) - 1)

	m.mu.Lock()
	defer m.mu.Unlock()

	// Build snapshot: collect all in-progress transactions
	inProgress := make([]TxID, 0, len(m.transactions))
	xmin := txid // Will be updated to oldest active
	for id, tx := range m.transactions {
		if tx.State == TxInProgress {
			inProgress = append(inProgress, id)
			if id < xmin {
				xmin = id
			}
		}
	}

	// If no active transactions, xmin = txid
	if len(inProgress) == 0 {
		xmin = txid
	}

	snapshot := NewSnapshot(xmin, txid+1, inProgress)

	tx := &Transaction{
		ID:       txid,
		State:    TxInProgress,
		Snapshot: snapshot,
	}

	m.transactions[txid] = tx

	// Update oldest active
	if m.oldestActive == InvalidTxID || txid < m.oldestActive {
		m.oldestActive = txid
	}

	return tx
}

// Commit commits a transaction.
func (m *Manager) Commit(txid TxID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, ok := m.transactions[txid]
	if !ok {
		return ErrTxNotFound
	}

	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.State != TxInProgress {
		return ErrTxNotActive
	}

	tx.State = TxCommitted
	m.updateOldestActive()

	return nil
}

// Abort aborts a transaction.
func (m *Manager) Abort(txid TxID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, ok := m.transactions[txid]
	if !ok {
		return ErrTxNotFound
	}

	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.State != TxInProgress {
		return ErrTxNotActive
	}

	tx.State = TxAborted
	m.updateOldestActive()

	return nil
}

// GetTransaction returns a transaction by ID.
func (m *Manager) GetTransaction(txid TxID) (*Transaction, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	tx, ok := m.transactions[txid]
	return tx, ok
}

// GetState returns the state of a transaction.
func (m *Manager) GetState(txid TxID) TxState {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Special cases
	if txid == InvalidTxID {
		return TxAborted
	}
	if txid == FrozenTxID {
		return TxCommitted
	}

	tx, ok := m.transactions[txid]
	if !ok {
		// Unknown transaction - treat as aborted for visibility purposes
		// In a real DB, we'd check the commit log
		return TxAborted
	}
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	return tx.State
}

// updateOldestActive recalculates the oldest active transaction.
// Must be called with m.mu held.
func (m *Manager) updateOldestActive() {
	m.oldestActive = InvalidTxID
	for id, tx := range m.transactions {
		if tx.State == TxInProgress {
			if m.oldestActive == InvalidTxID || id < m.oldestActive {
				m.oldestActive = id
			}
		}
	}
}

// OldestActiveTxID returns the oldest active transaction ID.
// Returns InvalidTxID if no transactions are active.
func (m *Manager) OldestActiveTxID() TxID {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.oldestActive
}

// ActiveCount returns the number of active transactions.
func (m *Manager) ActiveCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	count := 0
	for _, tx := range m.transactions {
		if tx.State == TxInProgress {
			count++
		}
	}
	return count
}

// GetActiveTransactions returns all active transactions.
func (m *Manager) GetActiveTransactions() []*Transaction {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var active []*Transaction
	for _, tx := range m.transactions {
		if tx.State == TxInProgress {
			active = append(active, tx)
		}
	}
	return active
}
