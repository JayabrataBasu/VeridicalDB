package wal

import (
	"fmt"

	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

// Recovery performs crash recovery using the WAL.
// It implements a simplified ARIES-style recovery with:
// 1. Analysis phase: Find last checkpoint and active transactions
// 2. Redo phase: Replay all changes from checkpoint forward
// 3. Undo phase: Rollback uncommitted transactions
type Recovery struct {
	wal *WAL

	// redoHandler is called for each record that needs to be redone
	redoHandler func(rec *Record) error

	// undoHandler is called for each record that needs to be undone
	undoHandler func(rec *Record) error

	// stats tracks recovery statistics
	stats RecoveryStats
}

// RecoveryStats contains statistics from the recovery process.
type RecoveryStats struct {
	// CheckpointLSN is the LSN of the checkpoint we recovered from
	CheckpointLSN LSN

	// RecordsAnalyzed is the number of records examined in analysis
	RecordsAnalyzed int

	// RecordsRedone is the number of records redone
	RecordsRedone int

	// RecordsUndone is the number of records undone
	RecordsUndone int

	// WinnersCount is the number of committed transactions found
	WinnersCount int

	// LosersCount is the number of uncommitted transactions rolled back
	LosersCount int
}

// NewRecovery creates a new recovery instance.
func NewRecovery(wal *WAL) *Recovery {
	return &Recovery{
		wal: wal,
	}
}

// SetRedoHandler sets the handler for redo operations.
// The handler receives records for INSERT, UPDATE, DELETE and should apply them.
func (r *Recovery) SetRedoHandler(handler func(rec *Record) error) {
	r.redoHandler = handler
}

// SetUndoHandler sets the handler for undo operations.
// The handler receives records for INSERT, UPDATE, DELETE and should reverse them.
func (r *Recovery) SetUndoHandler(handler func(rec *Record) error) {
	r.undoHandler = handler
}

// Recover performs the full recovery process.
// Returns statistics about what was recovered.
func (r *Recovery) Recover() (*RecoveryStats, error) {
	r.stats = RecoveryStats{}

	// Phase 1: Analysis
	startLSN, activeTxns, winners, losers, err := r.analysis()
	if err != nil {
		return nil, fmt.Errorf("analysis phase: %w", err)
	}

	r.stats.CheckpointLSN = startLSN
	r.stats.WinnersCount = len(winners)
	r.stats.LosersCount = len(losers)

	// Phase 2: Redo
	if err := r.redo(startLSN, activeTxns); err != nil {
		return nil, fmt.Errorf("redo phase: %w", err)
	}

	// Phase 3: Undo
	if err := r.undo(losers); err != nil {
		return nil, fmt.Errorf("undo phase: %w", err)
	}

	return &r.stats, nil
}

// analysis is the first phase of recovery.
// It scans the WAL from the last checkpoint to determine:
// - Which transactions committed (winners)
// - Which transactions were in-progress and need rollback (losers)
func (r *Recovery) analysis() (startLSN LSN, activeTxns map[txn.TxID]LSN, winners, losers map[txn.TxID]bool, err error) {
	// Find last checkpoint
	checkpointLSN, checkpointActive, err := FindLastCheckpoint(r.wal)
	if err != nil {
		return 0, nil, nil, nil, err
	}

	// Start from checkpoint or beginning of WAL
	if checkpointLSN != InvalidLSN {
		startLSN = checkpointLSN
	} else {
		startLSN = 0
	}

	// Initialize active transactions from checkpoint
	activeTxns = make(map[txn.TxID]LSN)
	for _, info := range checkpointActive {
		activeTxns[info.TxID] = info.LastLSN
	}

	winners = make(map[txn.TxID]bool)
	losers = make(map[txn.TxID]bool)

	// Scan from checkpoint to end, tracking transaction status
	err = r.wal.Iterate(startLSN, func(rec *Record) error {
		r.stats.RecordsAnalyzed++
		txid := txn.TxID(rec.TxID)

		switch rec.Type {
		case RecordBegin:
			activeTxns[txid] = rec.LSN
		case RecordCommit:
			winners[txid] = true
			delete(activeTxns, txid)
		case RecordAbort:
			// Already aborted, no need to undo again
			delete(activeTxns, txid)
		case RecordInsert, RecordDelete, RecordUpdate:
			// Update last LSN for this transaction
			activeTxns[txid] = rec.LSN
		case RecordCheckpointBegin, RecordCheckpointEnd:
			// Skip checkpoint records
		}
		return nil
	})

	if err != nil {
		return 0, nil, nil, nil, err
	}

	// Any transaction still in activeTxns is a loser (needs undo)
	for txid := range activeTxns {
		losers[txid] = true
	}

	return startLSN, activeTxns, winners, losers, nil
}

// redo is the second phase of recovery.
// It replays all data modifications from startLSN forward.
// This is idempotent - applying the same change twice has no additional effect.
func (r *Recovery) redo(startLSN LSN, activeTxns map[txn.TxID]LSN) error {
	if r.redoHandler == nil {
		// No redo handler, skip phase
		return nil
	}

	return r.wal.Iterate(startLSN, func(rec *Record) error {
		switch rec.Type {
		case RecordInsert, RecordDelete, RecordUpdate:
			r.stats.RecordsRedone++
			if err := r.redoHandler(rec); err != nil {
				return fmt.Errorf("redo record at LSN %d: %w", rec.LSN, err)
			}
		}
		return nil
	})
}

// undo is the third phase of recovery.
// It rolls back all changes made by loser transactions.
// Uses the PrevLSN chain to walk backwards through each transaction's changes.
func (r *Recovery) undo(losers map[txn.TxID]bool) error {
	if len(losers) == 0 || r.undoHandler == nil {
		return nil
	}

	// Collect all records for loser transactions
	// We need to undo them in reverse order (newest first)
	loserRecords := make([]*Record, 0)

	err := r.wal.Iterate(0, func(rec *Record) error {
		txid := txn.TxID(rec.TxID)
		if losers[txid] {
			switch rec.Type {
			case RecordInsert, RecordDelete, RecordUpdate:
				loserRecords = append(loserRecords, rec)
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	// Undo in reverse order (LIFO)
	for i := len(loserRecords) - 1; i >= 0; i-- {
		rec := loserRecords[i]
		r.stats.RecordsUndone++
		if err := r.undoHandler(rec); err != nil {
			return fmt.Errorf("undo record at LSN %d: %w", rec.LSN, err)
		}
	}

	return nil
}

// RecoverWithHandlers is a convenience method that performs recovery
// with provided redo and undo handlers.
func RecoverWithHandlers(
	wal *WAL,
	redoHandler func(rec *Record) error,
	undoHandler func(rec *Record) error,
) (*RecoveryStats, error) {
	recovery := NewRecovery(wal)
	recovery.SetRedoHandler(redoHandler)
	recovery.SetUndoHandler(undoHandler)
	return recovery.Recover()
}

// AnalyzeOnly performs only the analysis phase without redo/undo.
// Useful for diagnostics and testing.
func (r *Recovery) AnalyzeOnly() (*AnalysisResult, error) {
	startLSN, activeTxns, winners, losers, err := r.analysis()
	if err != nil {
		return nil, err
	}

	return &AnalysisResult{
		StartLSN:   startLSN,
		ActiveTxns: activeTxns,
		Winners:    winners,
		Losers:     losers,
	}, nil
}

// AnalysisResult contains the output of the analysis phase.
type AnalysisResult struct {
	// StartLSN is where redo should begin
	StartLSN LSN

	// ActiveTxns maps TxID to their last LSN
	ActiveTxns map[txn.TxID]LSN

	// Winners are transactions that committed
	Winners map[txn.TxID]bool

	// Losers are transactions that need rollback
	Losers map[txn.TxID]bool
}
