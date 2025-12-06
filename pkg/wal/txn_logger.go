package wal

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

// TxnLogger provides WAL integration for transaction operations.
// It wraps a txn.Manager and logs transaction events to the WAL.
type TxnLogger struct {
	mu     sync.Mutex
	wal    *WAL
	txnMgr *txn.Manager

	// prevLSN tracks the previous LSN for each transaction (for undo chain)
	prevLSN map[txn.TxID]LSN
}

// NewTxnLogger creates a new transaction logger.
func NewTxnLogger(wal *WAL, txnMgr *txn.Manager) *TxnLogger {
	return &TxnLogger{
		wal:     wal,
		txnMgr:  txnMgr,
		prevLSN: make(map[txn.TxID]LSN),
	}
}

// WAL returns the underlying WAL.
func (tl *TxnLogger) WAL() *WAL {
	return tl.wal
}

// TxnManager returns the underlying transaction manager.
func (tl *TxnLogger) TxnManager() *txn.Manager {
	return tl.txnMgr
}

// Begin starts a new transaction and logs a BEGIN record.
func (tl *TxnLogger) Begin() (*txn.Transaction, error) {
	// Start transaction first
	tx := tl.txnMgr.Begin()

	// Log BEGIN record
	rec := &Record{
		TxID: uint64(tx.ID),
		Type: RecordBegin,
	}

	lsn, err := tl.wal.Append(rec)
	if err != nil {
		// Try to abort the transaction
		_ = tl.txnMgr.Abort(tx.ID)
		return nil, fmt.Errorf("failed to log BEGIN: %w", err)
	}

	tl.mu.Lock()
	tl.prevLSN[tx.ID] = lsn
	tl.mu.Unlock()

	return tx, nil
}

// Commit commits a transaction with WAL durability.
// The COMMIT record is written and flushed to disk before returning.
func (tl *TxnLogger) Commit(txid txn.TxID) error {
	tl.mu.Lock()
	prevLSN := tl.prevLSN[txid]
	delete(tl.prevLSN, txid)
	tl.mu.Unlock()

	// Log COMMIT record
	rec := &Record{
		TxID:    uint64(txid),
		Type:    RecordCommit,
		PrevLSN: prevLSN,
	}

	// AppendAndFlush ensures the commit record is on disk before we return
	_, err := tl.wal.AppendAndFlush(rec)
	if err != nil {
		return fmt.Errorf("failed to log COMMIT: %w", err)
	}

	// Now commit in the transaction manager
	if err := tl.txnMgr.Commit(txid); err != nil {
		return err
	}

	return nil
}

// Abort aborts a transaction and logs an ABORT record.
func (tl *TxnLogger) Abort(txid txn.TxID) error {
	tl.mu.Lock()
	prevLSN := tl.prevLSN[txid]
	delete(tl.prevLSN, txid)
	tl.mu.Unlock()

	// Log ABORT record
	rec := &Record{
		TxID:    uint64(txid),
		Type:    RecordAbort,
		PrevLSN: prevLSN,
	}

	// Flush to ensure abort is recorded
	_, err := tl.wal.AppendAndFlush(rec)
	if err != nil {
		// Log the error but proceed with abort
		// The transaction will be rolled back on recovery anyway
	}

	// Abort in the transaction manager
	if err := tl.txnMgr.Abort(txid); err != nil {
		return err
	}

	return nil
}

// LogInsert logs an INSERT operation for a transaction.
// This should be called BEFORE the data is written to the heap.
func (tl *TxnLogger) LogInsert(txid txn.TxID, tableName string, pageID uint32, slotID uint16, data []byte) (LSN, error) {
	tl.mu.Lock()
	prevLSN := tl.prevLSN[txid]
	tl.mu.Unlock()

	rec := &Record{
		TxID:      uint64(txid),
		Type:      RecordInsert,
		PrevLSN:   prevLSN,
		TableName: tableName,
		PageID:    pageID,
		SlotID:    slotID,
		Data:      data,
	}

	lsn, err := tl.wal.Append(rec)
	if err != nil {
		return InvalidLSN, fmt.Errorf("failed to log INSERT: %w", err)
	}

	tl.mu.Lock()
	tl.prevLSN[txid] = lsn
	tl.mu.Unlock()

	return lsn, nil
}

// LogDelete logs a DELETE operation for a transaction.
// The oldData is the tuple being deleted (for undo).
func (tl *TxnLogger) LogDelete(txid txn.TxID, tableName string, pageID uint32, slotID uint16, oldData []byte) (LSN, error) {
	tl.mu.Lock()
	prevLSN := tl.prevLSN[txid]
	tl.mu.Unlock()

	rec := &Record{
		TxID:      uint64(txid),
		Type:      RecordDelete,
		PrevLSN:   prevLSN,
		TableName: tableName,
		PageID:    pageID,
		SlotID:    slotID,
		Data:      oldData,
	}

	lsn, err := tl.wal.Append(rec)
	if err != nil {
		return InvalidLSN, fmt.Errorf("failed to log DELETE: %w", err)
	}

	tl.mu.Lock()
	tl.prevLSN[txid] = lsn
	tl.mu.Unlock()

	return lsn, nil
}

// LogUpdate logs an UPDATE operation for a transaction.
// Both old and new data are stored for undo/redo.
func (tl *TxnLogger) LogUpdate(txid txn.TxID, tableName string, pageID uint32, slotID uint16, oldData, newData []byte) (LSN, error) {
	tl.mu.Lock()
	prevLSN := tl.prevLSN[txid]
	tl.mu.Unlock()

	// Encode: [oldLen:4][oldData][newData]
	data := make([]byte, 4+len(oldData)+len(newData))
	binary.LittleEndian.PutUint32(data[0:4], uint32(len(oldData)))
	copy(data[4:], oldData)
	copy(data[4+len(oldData):], newData)

	rec := &Record{
		TxID:      uint64(txid),
		Type:      RecordUpdate,
		PrevLSN:   prevLSN,
		TableName: tableName,
		PageID:    pageID,
		SlotID:    slotID,
		Data:      data,
	}

	lsn, err := tl.wal.Append(rec)
	if err != nil {
		return InvalidLSN, fmt.Errorf("failed to log UPDATE: %w", err)
	}

	tl.mu.Lock()
	tl.prevLSN[txid] = lsn
	tl.mu.Unlock()

	return lsn, nil
}

// GetPrevLSN returns the previous LSN for a transaction.
func (tl *TxnLogger) GetPrevLSN(txid txn.TxID) LSN {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	return tl.prevLSN[txid]
}

// Flush ensures all pending WAL records are on disk.
func (tl *TxnLogger) Flush() error {
	return tl.wal.Flush()
}

// GetActiveTransactions returns the list of transaction IDs that are currently in progress.
func (tl *TxnLogger) GetActiveTransactions() []txn.TxID {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	active := make([]txn.TxID, 0, len(tl.prevLSN))
	for txid := range tl.prevLSN {
		active = append(active, txid)
	}
	return active
}
