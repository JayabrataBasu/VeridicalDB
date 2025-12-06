package sql

import (
	"fmt"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/lock"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

// Session represents a database session with transaction state.
// Each REPL connection has its own session.
type Session struct {
	mtm      *catalog.MVCCTableManager
	executor *MVCCExecutor
	txnMgr   *txn.Manager
	lockMgr  *lock.Manager // Optional, can be nil for single-threaded mode

	// currentTx is the current transaction, or nil if in autocommit mode.
	currentTx *txn.Transaction

	// autocommit mode: if true, each statement runs in its own transaction
	autocommit bool
}

// NewSession creates a new database session.
func NewSession(mtm *catalog.MVCCTableManager) *Session {
	return &Session{
		mtm:        mtm,
		executor:   NewMVCCExecutor(mtm),
		txnMgr:     mtm.TxnManager(),
		lockMgr:    nil, // No locking by default
		autocommit: true,
	}
}

// NewSessionWithLocks creates a new database session with lock support.
func NewSessionWithLocks(mtm *catalog.MVCCTableManager, lockMgr *lock.Manager) *Session {
	return &Session{
		mtm:        mtm,
		executor:   NewMVCCExecutor(mtm),
		txnMgr:     mtm.TxnManager(),
		lockMgr:    lockMgr,
		autocommit: true,
	}
}

// SetLockManager sets the lock manager for concurrent access control.
func (s *Session) SetLockManager(lockMgr *lock.Manager) {
	s.lockMgr = lockMgr
}

// ExecuteSQL parses and executes a SQL string.
func (s *Session) ExecuteSQL(input string) (*Result, error) {
	parser := NewParser(input)
	stmt, err := parser.Parse()
	if err != nil {
		return nil, fmt.Errorf("syntax error: %w", err)
	}
	return s.Execute(stmt)
}

// Execute executes a SQL statement and returns the result.
func (s *Session) Execute(stmt Statement) (*Result, error) {
	// Handle transaction control statements specially
	switch stmt.(type) {
	case *BeginStmt:
		return s.handleBegin()
	case *CommitStmt:
		return s.handleCommit()
	case *RollbackStmt:
		return s.handleRollback()
	}

	// For DDL statements (CREATE/DROP), we don't need a transaction
	switch stmt.(type) {
	case *CreateTableStmt, *DropTableStmt:
		return s.executor.Execute(stmt, nil)
	}

	// For DML statements, we need a transaction
	tx, shouldCommit, err := s.ensureTransaction()
	if err != nil {
		return nil, err
	}

	result, err := s.executor.Execute(stmt, tx)
	if err != nil {
		// If autocommit and error, abort the transaction
		if shouldCommit {
			// Release locks
			if s.lockMgr != nil {
				s.lockMgr.ReleaseAll(tx.ID)
			}
			_ = s.txnMgr.Abort(tx.ID)
			s.currentTx = nil
		}
		return nil, err
	}

	// If autocommit, commit the transaction and release locks
	if shouldCommit {
		// Release locks
		if s.lockMgr != nil {
			s.lockMgr.ReleaseAll(tx.ID)
		}
		if err := s.txnMgr.Commit(tx.ID); err != nil {
			return nil, fmt.Errorf("autocommit failed: %w", err)
		}
		s.currentTx = nil
	}

	return result, nil
}

// ensureTransaction ensures there's an active transaction.
// Returns the transaction and whether it should be auto-committed.
func (s *Session) ensureTransaction() (*txn.Transaction, bool, error) {
	if s.currentTx != nil {
		// Already in a transaction
		return s.currentTx, false, nil
	}

	// Start a new transaction (autocommit mode)
	tx := s.txnMgr.Begin()
	if s.autocommit {
		return tx, true, nil
	}

	s.currentTx = tx
	return tx, false, nil
}

// handleBegin starts a new explicit transaction.
func (s *Session) handleBegin() (*Result, error) {
	if s.currentTx != nil {
		return nil, fmt.Errorf("transaction already in progress")
	}

	s.currentTx = s.txnMgr.Begin()
	s.autocommit = false

	return &Result{
		Message: fmt.Sprintf("BEGIN (txid=%d)", s.currentTx.ID),
	}, nil
}

// handleCommit commits the current transaction.
func (s *Session) handleCommit() (*Result, error) {
	if s.currentTx == nil {
		return nil, fmt.Errorf("no transaction in progress")
	}

	txid := s.currentTx.ID

	// Release all locks held by this transaction
	if s.lockMgr != nil {
		s.lockMgr.ReleaseAll(txid)
	}

	if err := s.txnMgr.Commit(txid); err != nil {
		return nil, err
	}

	s.currentTx = nil
	s.autocommit = true

	return &Result{
		Message: fmt.Sprintf("COMMIT (txid=%d)", txid),
	}, nil
}

// handleRollback aborts the current transaction.
func (s *Session) handleRollback() (*Result, error) {
	if s.currentTx == nil {
		return nil, fmt.Errorf("no transaction in progress")
	}

	txid := s.currentTx.ID

	// Release all locks held by this transaction
	if s.lockMgr != nil {
		s.lockMgr.ReleaseAll(txid)
	}

	if err := s.txnMgr.Abort(txid); err != nil {
		return nil, err
	}

	s.currentTx = nil
	s.autocommit = true

	return &Result{
		Message: fmt.Sprintf("ROLLBACK (txid=%d)", txid),
	}, nil
}

// InTransaction returns true if there's an active explicit transaction.
func (s *Session) InTransaction() bool {
	return s.currentTx != nil
}

// CurrentTxID returns the current transaction ID, or 0 if none.
func (s *Session) CurrentTxID() txn.TxID {
	if s.currentTx == nil {
		return txn.InvalidTxID
	}
	return s.currentTx.ID
}

// Close cleans up the session, aborting any open transaction.
func (s *Session) Close() {
	if s.currentTx != nil {
		txid := s.currentTx.ID
		// Release all locks
		if s.lockMgr != nil {
			s.lockMgr.ReleaseAll(txid)
		}
		_ = s.txnMgr.Abort(txid)
		s.currentTx = nil
	}
}
