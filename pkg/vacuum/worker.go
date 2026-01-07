package vacuum

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

// Worker performs vacuum on a single table.
type Worker struct {
	table   string
	config  Config
	txnMgr  *txn.Manager
	storage StorageInterface
	full    bool
	metrics *Metrics

	// Cost tracking for throttling
	currentCost int
}

// Run executes the vacuum operation on the table.
func (w *Worker) Run(ctx context.Context, result *VacuumResult) error {
	pageCount, err := w.storage.GetPageCount(w.table)
	if err != nil {
		return err
	}

	// Get the oldest active transaction for visibility checks
	oldestActive := w.txnMgr.OldestActiveTxID()

	// Process each page
	for pageID := uint32(0); pageID < uint32(pageCount); pageID++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Throttle if needed
		if err := w.throttle(ctx); err != nil {
			return err
		}

		compacted, tuplesRemoved, bytesReclaimed, err := w.processPage(ctx, pageID, oldestActive)
		if err != nil {
			// Log error but continue with other pages
			continue
		}

		result.PagesScanned++
		if compacted {
			result.PagesCompacted++
		}
		result.TuplesRemoved += tuplesRemoved
		result.BytesReclaimed += bytesReclaimed
	}

	return nil
}

// processPage scans a single page and removes dead tuples.
func (w *Worker) processPage(ctx context.Context, pageID uint32, oldestActive txn.TxID) (compacted bool, tuplesRemoved int64, bytesReclaimed int64, err error) {
	// Read the page
	pageData, err := w.storage.FetchPage(w.table, pageID)
	if err != nil {
		return false, 0, 0, err
	}
	w.currentCost += w.config.PageCostRead

	// Check if page is valid
	if len(pageData) < 24 {
		return false, 0, 0, nil
	}

	// Check magic
	magic := binary.LittleEndian.Uint16(pageData[8:10])
	if magic != 0xDB01 {
		return false, 0, 0, nil // Not a valid heap page
	}

	slotCount := int(binary.LittleEndian.Uint16(pageData[10:12]))
	if slotCount == 0 {
		return false, 0, 0, nil
	}

	// Find dead tuples
	deadSlots := make([]int, 0)
	for slot := 0; slot < slotCount; slot++ {
		offset, length := w.getSlot(pageData, slot)
		if length == 0 || offset == 0 {
			continue // Already empty
		}

		// Read tuple data to check MVCC header
		tupleData := pageData[offset : offset+length]
		if len(tupleData) < txn.HeaderSize {
			continue
		}

		header, err := txn.DecodeMVCCHeader(tupleData)
		if err != nil {
			continue
		}

		// Check if tuple is dead (safe to remove)
		if w.isTupleDead(header, oldestActive) {
			deadSlots = append(deadSlots, slot)
			bytesReclaimed += int64(length)
		}
	}

	if len(deadSlots) == 0 {
		return false, 0, 0, nil
	}

	// Compact the page
	reclaimed, err := w.storage.CompactPage(w.table, pageID, deadSlots)
	if err != nil {
		return false, 0, 0, err
	}
	w.currentCost += w.config.PageCostWrite

	return true, int64(len(deadSlots)), reclaimed, nil
}

// isTupleDead checks if a tuple is safe to remove (no active transaction can see it).
func (w *Worker) isTupleDead(header *txn.MVCCHeader, oldestActive txn.TxID) bool {
	xmin := header.XMin
	xmax := header.XMax

	// If not deleted, not dead
	if xmax == txn.InvalidTxID {
		return false
	}

	// Check if the inserting transaction aborted
	xminState := w.txnMgr.GetState(xmin)
	if xminState == txn.TxAborted {
		// Tuple was inserted by aborted transaction - dead
		return true
	}

	// If xmin is still in progress, tuple might not be committed yet
	if xminState == txn.TxInProgress {
		return false
	}

	// Now check xmax (deletion)
	xmaxState := w.txnMgr.GetState(xmax)

	// If deleting transaction is still in progress, tuple might become visible again (rollback)
	if xmaxState == txn.TxInProgress {
		return false
	}

	// If deleting transaction aborted, tuple is NOT deleted (still visible)
	if xmaxState == txn.TxAborted {
		return false
	}

	// Deleting transaction committed - check if any active transaction could still see it
	// A tuple is safe to remove if:
	// 1. The deleting transaction committed
	// 2. No active transaction could still see the tuple (xmax < oldestActive)
	if oldestActive == txn.InvalidTxID {
		// No active transactions - safe to remove
		return true
	}

	// If the deleting transaction is older than all active transactions,
	// no one can see the tuple anymore
	return xmax < oldestActive
}

// getSlot reads slot entry from page buffer.
func (w *Worker) getSlot(buf []byte, slot int) (offset uint16, length uint16) {
	const headerSize = 24
	const slotEntrySize = 4
	off := headerSize + slot*slotEntrySize
	if off+4 > len(buf) {
		return 0, 0
	}
	return binary.LittleEndian.Uint16(buf[off : off+2]),
		binary.LittleEndian.Uint16(buf[off+2 : off+4])
}

// throttle pauses execution if cost limit is reached.
func (w *Worker) throttle(ctx context.Context) error {
	if w.currentCost >= w.config.CostLimit {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sleepContext(ctx, w.config.CostDelay):
		}
		w.currentCost = 0
	}
	return nil
}

// sleepContext returns a channel that receives after duration or context is cancelled.
func sleepContext(ctx context.Context, d time.Duration) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
		case <-time.After(d):
		}
		close(ch)
	}()
	return ch
}
