package txn

import (
	"testing"
)

func TestTransactionLifecycle(t *testing.T) {
	mgr := NewManager()

	// Begin a transaction
	tx1 := mgr.Begin()
	if tx1.ID < 2 {
		t.Errorf("expected TxID >= 2, got %d", tx1.ID)
	}
	if tx1.State != TxInProgress {
		t.Errorf("expected TxInProgress, got %s", tx1.State)
	}
	if !tx1.IsActive() {
		t.Error("expected transaction to be active")
	}

	// Begin another transaction
	tx2 := mgr.Begin()
	if tx2.ID <= tx1.ID {
		t.Errorf("expected tx2.ID > tx1.ID, got %d <= %d", tx2.ID, tx1.ID)
	}

	// Commit tx1
	if err := mgr.Commit(tx1.ID); err != nil {
		t.Errorf("commit error: %v", err)
	}
	if tx1.State != TxCommitted {
		t.Errorf("expected TxCommitted, got %s", tx1.State)
	}
	if tx1.IsActive() {
		t.Error("expected transaction to not be active after commit")
	}

	// Abort tx2
	if err := mgr.Abort(tx2.ID); err != nil {
		t.Errorf("abort error: %v", err)
	}
	if tx2.State != TxAborted {
		t.Errorf("expected TxAborted, got %s", tx2.State)
	}

	// Try to commit already committed tx
	if err := mgr.Commit(tx1.ID); err != ErrTxNotActive {
		t.Errorf("expected ErrTxNotActive, got %v", err)
	}

	// Try to abort already aborted tx
	if err := mgr.Abort(tx2.ID); err != ErrTxNotActive {
		t.Errorf("expected ErrTxNotActive, got %v", err)
	}
}

func TestSnapshot(t *testing.T) {
	mgr := NewManager()

	// Start tx1
	tx1 := mgr.Begin()

	// Start tx2 - should see tx1 as in-progress
	tx2 := mgr.Begin()

	if tx2.Snapshot.XMax <= tx2.ID {
		t.Errorf("expected XMax > tx2.ID")
	}

	if !tx2.Snapshot.IsInProgress(tx1.ID) {
		t.Error("tx2 snapshot should see tx1 as in-progress")
	}

	// Commit tx1
	mgr.Commit(tx1.ID)

	// tx2's snapshot should still see tx1 as in-progress (snapshot is immutable)
	if !tx2.Snapshot.IsInProgress(tx1.ID) {
		t.Error("tx2 snapshot should still see tx1 as in-progress (snapshot is immutable)")
	}

	// Start tx3 - should NOT see tx1 as in-progress (it committed)
	tx3 := mgr.Begin()
	if tx3.Snapshot.IsInProgress(tx1.ID) {
		t.Error("tx3 snapshot should NOT see tx1 as in-progress")
	}
	if !tx3.Snapshot.IsInProgress(tx2.ID) {
		t.Error("tx3 snapshot should see tx2 as in-progress")
	}
}

func TestVisibilityOwnTransaction(t *testing.T) {
	mgr := NewManager()
	tx := mgr.Begin()

	// A tuple created by the current transaction should be visible
	header := &MVCCHeader{XMin: tx.ID, XMax: InvalidTxID}
	if !IsVisible(header, tx.Snapshot, mgr, tx.ID) {
		t.Error("tuple created by current tx should be visible")
	}

	// If we delete it, it should not be visible
	header.XMax = tx.ID
	if IsVisible(header, tx.Snapshot, mgr, tx.ID) {
		t.Error("tuple deleted by current tx should NOT be visible")
	}
}

func TestVisibilityCommittedTransaction(t *testing.T) {
	mgr := NewManager()

	// tx1 creates a tuple and commits
	tx1 := mgr.Begin()
	header := &MVCCHeader{XMin: tx1.ID, XMax: InvalidTxID}
	mgr.Commit(tx1.ID)

	// tx2 starts after tx1 commits - should see the tuple
	tx2 := mgr.Begin()
	if !IsVisible(header, tx2.Snapshot, mgr, tx2.ID) {
		t.Error("tx2 should see tuple from committed tx1")
	}
}

func TestVisibilityUncommittedTransaction(t *testing.T) {
	mgr := NewManager()

	// tx1 creates a tuple but doesn't commit
	tx1 := mgr.Begin()
	header := &MVCCHeader{XMin: tx1.ID, XMax: InvalidTxID}

	// tx2 starts - should NOT see the tuple (tx1 is in-progress)
	tx2 := mgr.Begin()
	if IsVisible(header, tx2.Snapshot, mgr, tx2.ID) {
		t.Error("tx2 should NOT see tuple from uncommitted tx1")
	}

	// Now tx1 commits
	mgr.Commit(tx1.ID)

	// tx2 still shouldn't see it (snapshot was taken before commit)
	if IsVisible(header, tx2.Snapshot, mgr, tx2.ID) {
		t.Error("tx2 should NOT see tuple (tx1 was in-progress when snapshot taken)")
	}

	// tx3 starts after commit - should see it
	tx3 := mgr.Begin()
	if !IsVisible(header, tx3.Snapshot, mgr, tx3.ID) {
		t.Error("tx3 should see tuple from committed tx1")
	}
}

func TestVisibilityAbortedTransaction(t *testing.T) {
	mgr := NewManager()

	// tx1 creates a tuple and aborts
	tx1 := mgr.Begin()
	header := &MVCCHeader{XMin: tx1.ID, XMax: InvalidTxID}
	mgr.Abort(tx1.ID)

	// tx2 should NOT see the tuple (tx1 aborted)
	tx2 := mgr.Begin()
	if IsVisible(header, tx2.Snapshot, mgr, tx2.ID) {
		t.Error("tx2 should NOT see tuple from aborted tx1")
	}
}

func TestVisibilityDeletedTuple(t *testing.T) {
	mgr := NewManager()

	// tx1 creates a tuple
	tx1 := mgr.Begin()
	header := &MVCCHeader{XMin: tx1.ID, XMax: InvalidTxID}
	mgr.Commit(tx1.ID)

	// tx2 sees the tuple
	tx2 := mgr.Begin()
	if !IsVisible(header, tx2.Snapshot, mgr, tx2.ID) {
		t.Error("tx2 should see the tuple before deletion")
	}

	// tx3 deletes the tuple
	tx3 := mgr.Begin()
	header.XMax = tx3.ID

	// tx2 should still see it (tx3 hasn't committed)
	if !IsVisible(header, tx2.Snapshot, mgr, tx2.ID) {
		t.Error("tx2 should still see tuple (deletion not committed)")
	}

	// tx3 commits the deletion
	mgr.Commit(tx3.ID)

	// tx2's snapshot still shows it (tx3 was in-progress when snapshot taken)
	if !IsVisible(header, tx2.Snapshot, mgr, tx2.ID) {
		t.Error("tx2 should still see tuple (tx3 was in-progress in tx2's snapshot)")
	}

	// tx4 starts after deletion commit - should NOT see the tuple
	tx4 := mgr.Begin()
	if IsVisible(header, tx4.Snapshot, mgr, tx4.ID) {
		t.Error("tx4 should NOT see deleted tuple")
	}
}

func TestVisibilityDeleteAborted(t *testing.T) {
	mgr := NewManager()

	// tx1 creates and commits a tuple
	tx1 := mgr.Begin()
	header := &MVCCHeader{XMin: tx1.ID, XMax: InvalidTxID}
	mgr.Commit(tx1.ID)

	// tx2 tries to delete but aborts
	tx2 := mgr.Begin()
	header.XMax = tx2.ID
	mgr.Abort(tx2.ID)

	// tx3 should still see the tuple (deletion was aborted)
	tx3 := mgr.Begin()
	if !IsVisible(header, tx3.Snapshot, mgr, tx3.ID) {
		t.Error("tx3 should see tuple (deletion was aborted)")
	}
}

func TestFrozenTuple(t *testing.T) {
	mgr := NewManager()

	// A frozen tuple should always be visible
	header := &MVCCHeader{XMin: FrozenTxID, XMax: InvalidTxID}

	tx := mgr.Begin()
	if !IsVisible(header, tx.Snapshot, mgr, tx.ID) {
		t.Error("frozen tuple should always be visible")
	}
}

func TestCanModify(t *testing.T) {
	mgr := NewManager()

	// Create a committed tuple
	tx1 := mgr.Begin()
	header := &MVCCHeader{XMin: tx1.ID, XMax: InvalidTxID}
	mgr.Commit(tx1.ID)

	// tx2 should be able to modify it
	tx2 := mgr.Begin()
	canMod, err := CanModify(header, tx2.ID, mgr)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !canMod {
		t.Error("tx2 should be able to modify the tuple")
	}

	// After tx2 marks it for deletion, tx2 can't modify again
	header.XMax = tx2.ID
	canMod, err = CanModify(header, tx2.ID, mgr)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if canMod {
		t.Error("tx2 should NOT be able to modify again")
	}

	// tx3 can't modify (tx2's delete is in progress)
	tx3 := mgr.Begin()
	_, err = CanModify(header, tx3.ID, mgr)
	if err != ErrSerializationFailure {
		t.Errorf("expected ErrSerializationFailure, got %v", err)
	}

	// If tx2 aborts, tx3 can modify
	mgr.Abort(tx2.ID)
	canMod, err = CanModify(header, tx3.ID, mgr)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !canMod {
		t.Error("tx3 should be able to modify after tx2 aborted")
	}
}

func TestActiveCount(t *testing.T) {
	mgr := NewManager()

	if mgr.ActiveCount() != 0 {
		t.Errorf("expected 0 active, got %d", mgr.ActiveCount())
	}

	tx1 := mgr.Begin()
	if mgr.ActiveCount() != 1 {
		t.Errorf("expected 1 active, got %d", mgr.ActiveCount())
	}

	tx2 := mgr.Begin()
	if mgr.ActiveCount() != 2 {
		t.Errorf("expected 2 active, got %d", mgr.ActiveCount())
	}

	mgr.Commit(tx1.ID)
	if mgr.ActiveCount() != 1 {
		t.Errorf("expected 1 active after commit, got %d", mgr.ActiveCount())
	}

	mgr.Abort(tx2.ID)
	if mgr.ActiveCount() != 0 {
		t.Errorf("expected 0 active after abort, got %d", mgr.ActiveCount())
	}
}

func TestGetState(t *testing.T) {
	mgr := NewManager()

	// Invalid TxID should be treated as aborted
	if mgr.GetState(InvalidTxID) != TxAborted {
		t.Error("InvalidTxID should be TxAborted")
	}

	// Frozen TxID should be treated as committed
	if mgr.GetState(FrozenTxID) != TxCommitted {
		t.Error("FrozenTxID should be TxCommitted")
	}

	// Unknown TxID should be treated as aborted
	if mgr.GetState(TxID(9999)) != TxAborted {
		t.Error("Unknown TxID should be TxAborted")
	}
}
