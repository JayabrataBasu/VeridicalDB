package wal

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

// Checkpointer manages checkpoint operations for crash recovery.
// A checkpoint records the state of the database at a point in time,
// allowing the WAL to be truncated and speeding up recovery.
type Checkpointer struct {
	mu sync.Mutex

	wal       *WAL
	txnLogger *TxnLogger

	// pageFlusher is called to flush dirty pages during checkpoint
	pageFlusher func() error

	// lastCheckpointLSN is the LSN of the last completed checkpoint
	lastCheckpointLSN LSN

	// checkpointInterval is the time between automatic checkpoints
	checkpointInterval time.Duration

	// stopChan signals the background checkpointer to stop
	stopChan chan struct{}

	// running indicates if the background checkpointer is running
	running bool
}

// NewCheckpointer creates a new checkpointer.
func NewCheckpointer(wal *WAL, txnLogger *TxnLogger) *Checkpointer {
	return &Checkpointer{
		wal:                wal,
		txnLogger:          txnLogger,
		checkpointInterval: 5 * time.Minute, // Default: 5 minutes
		stopChan:           make(chan struct{}),
	}
}

// SetPageFlusher sets the function to flush dirty pages.
// This should be called before starting checkpoints.
func (c *Checkpointer) SetPageFlusher(flusher func() error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pageFlusher = flusher
}

// SetCheckpointInterval sets the interval between automatic checkpoints.
func (c *Checkpointer) SetCheckpointInterval(interval time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.checkpointInterval = interval
}

// LastCheckpointLSN returns the LSN of the last completed checkpoint.
func (c *Checkpointer) LastCheckpointLSN() LSN {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastCheckpointLSN
}

// Checkpoint performs a checkpoint operation.
// This is a fuzzy checkpoint that doesn't block concurrent transactions.
//
// The checkpoint process:
// 1. Write CHECKPOINT_BEGIN record
// 2. Record all active transactions
// 3. Flush dirty pages to disk
// 4. Write CHECKPOINT_END record with active transaction list
// 5. Flush WAL to ensure checkpoint is durable
func (c *Checkpointer) Checkpoint() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Step 1: Write CHECKPOINT_BEGIN
	beginRec := &Record{
		TxID: 0, // Checkpoint is not part of any transaction
		Type: RecordCheckpointBegin,
	}
	beginLSN, err := c.wal.Append(beginRec)
	if err != nil {
		return fmt.Errorf("checkpoint begin: %w", err)
	}

	// Step 2: Get active transactions
	activeTxns := c.txnLogger.GetActiveTransactions()

	// Step 3: Flush dirty pages (if flusher is set)
	if c.pageFlusher != nil {
		if err := c.pageFlusher(); err != nil {
			return fmt.Errorf("flush pages: %w", err)
		}
	}

	// Step 4: Write CHECKPOINT_END with active transaction list
	// Format: [count:8][txid1:8][lsn1:8][txid2:8][lsn2:8]...
	data := make([]byte, 8+len(activeTxns)*16)
	binary.LittleEndian.PutUint64(data[0:8], uint64(len(activeTxns)))

	offset := 8
	for _, txid := range activeTxns {
		binary.LittleEndian.PutUint64(data[offset:], uint64(txid))
		offset += 8
		// Get the last LSN for this transaction
		prevLSN := c.txnLogger.GetPrevLSN(txid)
		binary.LittleEndian.PutUint64(data[offset:], uint64(prevLSN))
		offset += 8
	}

	endRec := &Record{
		TxID: 0,
		Type: RecordCheckpointEnd,
		Data: data,
	}

	_, err = c.wal.Append(endRec)
	if err != nil {
		return fmt.Errorf("checkpoint end: %w", err)
	}

	// Step 5: Flush WAL to ensure checkpoint is durable
	if err := c.wal.Flush(); err != nil {
		return fmt.Errorf("flush wal: %w", err)
	}

	// Update checkpoint LSN
	c.lastCheckpointLSN = beginLSN
	c.wal.SetCheckpointLSN(beginLSN)

	return nil
}

// StartBackground starts the background checkpointer.
func (c *Checkpointer) StartBackground() {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return
	}
	c.running = true
	c.stopChan = make(chan struct{})
	interval := c.checkpointInterval
	c.mu.Unlock()

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				_ = c.Checkpoint() // Ignore errors in background
			case <-c.stopChan:
				return
			}
		}
	}()
}

// StopBackground stops the background checkpointer.
func (c *Checkpointer) StopBackground() {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return
	}
	c.running = false
	close(c.stopChan)
	c.mu.Unlock()
}

// FindLastCheckpoint scans the WAL to find the last checkpoint.
// Returns the LSN of CHECKPOINT_BEGIN and the list of active transactions.
func FindLastCheckpoint(w *WAL) (checkpointLSN LSN, activeTxns []ActiveTxInfo, err error) {
	var lastBeginLSN LSN
	var lastEndData []byte
	var foundEnd bool

	err = w.Iterate(0, func(rec *Record) error {
		switch rec.Type {
		case RecordCheckpointBegin:
			lastBeginLSN = rec.LSN
			foundEnd = false
		case RecordCheckpointEnd:
			if lastBeginLSN > 0 {
				lastEndData = rec.Data
				foundEnd = true
			}
		}
		return nil
	})

	if err != nil {
		return InvalidLSN, nil, err
	}

	if !foundEnd {
		// No complete checkpoint found
		return InvalidLSN, nil, nil
	}

	// Parse checkpoint end data
	activeTxns, err = ParseCheckpointEndData(lastEndData)
	if err != nil {
		return InvalidLSN, nil, fmt.Errorf("parse checkpoint: %w", err)
	}

	return lastBeginLSN, activeTxns, nil
}

// ActiveTxInfo represents an active transaction at checkpoint time.
type ActiveTxInfo struct {
	TxID    txn.TxID
	LastLSN LSN
}

// ParseCheckpointEndData parses the raw data from a CHECKPOINT_END record.
// This version includes both TxID and LastLSN for each active transaction.
func ParseCheckpointEndData(data []byte) ([]ActiveTxInfo, error) {
	if len(data) < 8 {
		return nil, ErrInvalidRecord
	}

	count := binary.LittleEndian.Uint64(data[0:8])
	if len(data) < 8+int(count)*16 {
		return nil, ErrInvalidRecord
	}

	activeTxns := make([]ActiveTxInfo, count)
	offset := 8
	for i := uint64(0); i < count; i++ {
		txid := txn.TxID(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8
		lsn := LSN(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8
		activeTxns[i] = ActiveTxInfo{TxID: txid, LastLSN: lsn}
	}

	return activeTxns, nil
}
