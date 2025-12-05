package txn

// Visibility implements MVCC visibility rules.
// A tuple is visible to a snapshot if:
// 1. The creating transaction (xmin) is committed and visible to this snapshot
// 2. The tuple has not been deleted (xmax = 0), OR
//    the deleting transaction (xmax) is not visible to this snapshot

// IsVisible determines if a tuple with the given MVCC header is visible
// to the specified snapshot and transaction manager.
//
// Visibility rules (PostgreSQL-style):
// - If xmin is the current transaction: visible if not deleted by current tx
// - If xmin was in progress when snapshot taken: not visible
// - If xmin >= snapshot.XMax: not visible (started after snapshot)
// - If xmin committed: check xmax
//   - If xmax = 0: visible
//   - If xmax is current transaction: not visible (we deleted it)
//   - If xmax was in progress when snapshot taken: visible (delete not committed)
//   - If xmax >= snapshot.XMax: visible (deleter started after snapshot)
//   - If xmax committed: not visible (deleted)
//   - If xmax aborted: visible (delete rolled back)
func IsVisible(header *MVCCHeader, snapshot *Snapshot, mgr *Manager, currentTxID TxID) bool {
	xmin := header.XMin
	xmax := header.XMax

	// Special case: frozen tuples are always visible
	if xmin == FrozenTxID {
		return !isXmaxVisible(xmax, snapshot, mgr, currentTxID)
	}

	// Check if xmin is visible
	if !isXminVisible(xmin, snapshot, mgr, currentTxID) {
		return false
	}

	// Check if tuple has been deleted
	if xmax == InvalidTxID {
		// Not deleted, visible
		return true
	}

	// Check if the deletion is visible
	return !isXmaxVisible(xmax, snapshot, mgr, currentTxID)
}

// isXminVisible checks if the creating transaction is visible to the snapshot.
func isXminVisible(xmin TxID, snapshot *Snapshot, mgr *Manager, currentTxID TxID) bool {
	// If we created this tuple, it's visible to us
	if xmin == currentTxID {
		return true
	}

	// If xmin started after our snapshot, not visible
	if xmin >= snapshot.XMax {
		return false
	}

	// If xmin was in progress when we took our snapshot, not visible
	if snapshot.IsInProgress(xmin) {
		return false
	}

	// If xmin is before our snapshot window, check if it committed
	state := mgr.GetState(xmin)
	return state == TxCommitted
}

// isXmaxVisible checks if the deleting transaction's effects are visible.
// Returns true if the deletion should be considered "done" (tuple not visible).
func isXmaxVisible(xmax TxID, snapshot *Snapshot, mgr *Manager, currentTxID TxID) bool {
	if xmax == InvalidTxID {
		return false
	}

	// If we deleted this tuple, the deletion is visible to us
	if xmax == currentTxID {
		return true
	}

	// If xmax started after our snapshot, deletion not visible
	if xmax >= snapshot.XMax {
		return false
	}

	// If xmax was in progress when we took our snapshot, deletion not visible
	if snapshot.IsInProgress(xmax) {
		return false
	}

	// Check if the deleting transaction committed
	state := mgr.GetState(xmax)
	return state == TxCommitted
}

// CanModify checks if the current transaction can modify (update/delete) a tuple.
// This is used to detect write-write conflicts.
// Returns true if the tuple can be modified, false if there's a conflict.
func CanModify(header *MVCCHeader, currentTxID TxID, mgr *Manager) (bool, error) {
	xmax := header.XMax

	// If not deleted by anyone, we can modify
	if xmax == InvalidTxID {
		return true, nil
	}

	// If we already marked it for deletion, can't modify again
	if xmax == currentTxID {
		return false, nil // Already modified by us
	}

	// Check the state of the transaction that marked it
	state := mgr.GetState(xmax)

	switch state {
	case TxInProgress:
		// Another transaction has a pending modification
		// In a single-threaded system, this shouldn't happen
		// In a concurrent system, we'd wait or abort
		return false, ErrSerializationFailure

	case TxCommitted:
		// The tuple was already deleted/updated by a committed transaction
		// We're trying to modify something that no longer exists
		return false, nil

	case TxAborted:
		// The deleting transaction aborted, so the tuple is still there
		// We can modify it
		return true, nil

	default:
		return false, ErrSerializationFailure
	}
}

// HeapTupleStatus represents the visibility status of a tuple.
type HeapTupleStatus int

const (
	// HeapTupleInvisible means the tuple is not visible to the current snapshot.
	HeapTupleInvisible HeapTupleStatus = iota

	// HeapTupleLive means the tuple is visible and not deleted.
	HeapTupleLive

	// HeapTupleRecentlyDead means the tuple was recently deleted but might
	// still be visible to some transactions (for VACUUM decisions).
	HeapTupleRecentlyDead

	// HeapTupleDead means the tuple is deleted and no longer visible to any
	// active transaction (safe to remove).
	HeapTupleDead

	// HeapTupleInsertInProgress means the inserting transaction is still running.
	HeapTupleInsertInProgress

	// HeapTupleDeleteInProgress means the deleting transaction is still running.
	HeapTupleDeleteInProgress
)

// GetTupleStatus returns detailed status of a tuple for the given snapshot.
// This is useful for VACUUM and debugging.
func GetTupleStatus(header *MVCCHeader, snapshot *Snapshot, mgr *Manager, currentTxID TxID) HeapTupleStatus {
	xmin := header.XMin
	xmax := header.XMax

	// Check inserter status
	if xmin != FrozenTxID && xmin != currentTxID {
		state := mgr.GetState(xmin)
		if state == TxInProgress {
			return HeapTupleInsertInProgress
		}
		if state == TxAborted {
			return HeapTupleInvisible
		}
	}

	// Tuple was inserted by committed or current transaction
	// Now check if deleted
	if xmax == InvalidTxID {
		if IsVisible(header, snapshot, mgr, currentTxID) {
			return HeapTupleLive
		}
		return HeapTupleInvisible
	}

	// Tuple has xmax set
	if xmax == currentTxID {
		return HeapTupleDeleteInProgress
	}

	state := mgr.GetState(xmax)
	switch state {
	case TxInProgress:
		return HeapTupleDeleteInProgress
	case TxAborted:
		if IsVisible(header, snapshot, mgr, currentTxID) {
			return HeapTupleLive
		}
		return HeapTupleInvisible
	case TxCommitted:
		// Check if any active transaction could still see this tuple
		oldestActive := mgr.OldestActiveTxID()
		if oldestActive != InvalidTxID && xmax < oldestActive {
			return HeapTupleDead
		}
		return HeapTupleRecentlyDead
	}

	return HeapTupleInvisible
}
