package catalog

import (
	"fmt"

	"github.com/JayabrataBasu/VeridicalDB/pkg/storage"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

// MVCCTableManager wraps TableManager with MVCC support.
// It handles transaction visibility and proper tuple versioning.
type MVCCTableManager struct {
	tm     *TableManager
	txnMgr *txn.Manager
}

// NewMVCCTableManager creates an MVCC-aware table manager.
func NewMVCCTableManager(tm *TableManager, txnMgr *txn.Manager) *MVCCTableManager {
	return &MVCCTableManager{
		tm:     tm,
		txnMgr: txnMgr,
	}
}

// TableManager returns the underlying table manager for non-MVCC operations.
func (m *MVCCTableManager) TableManager() *TableManager {
	return m.tm
}

// TxnManager returns the transaction manager.
func (m *MVCCTableManager) TxnManager() *txn.Manager {
	return m.txnMgr
}

// MVCCRow represents a row with its MVCC header and decoded values.
type MVCCRow struct {
	RID    storage.RID
	Header *txn.MVCCHeader
	Values []Value
}

// Insert inserts a row with MVCC metadata.
func (m *MVCCTableManager) Insert(tableName string, values []Value, tx *txn.Transaction) (storage.RID, error) {
	meta, err := m.tm.catalog.GetTable(tableName)
	if err != nil {
		return storage.RID{}, err
	}

	// Encode the row data
	rowData, err := EncodeRow(meta.Schema, values)
	if err != nil {
		return storage.RID{}, fmt.Errorf("encode row: %w", err)
	}

	// Create MVCC header with current transaction as creator
	header := &txn.MVCCHeader{
		XMin: tx.ID,
		XMax: txn.InvalidTxID, // Not deleted
	}

	// Wrap with MVCC header
	mvccData := txn.WrapWithMVCC(header, rowData)

	// Insert into storage
	rid, err := m.tm.storage.Insert(tableName, mvccData)
	if err != nil {
		return storage.RID{}, err
	}

	return rid, nil
}

// FetchWithMVCC retrieves a row with its MVCC header.
func (m *MVCCTableManager) FetchWithMVCC(tableName string, rid storage.RID) (*MVCCRow, error) {
	meta, err := m.tm.catalog.GetTable(tableName)
	if err != nil {
		return nil, err
	}

	// Fetch raw data from storage
	data, err := m.tm.storage.Fetch(rid)
	if err != nil {
		return nil, err
	}

	// Extract MVCC header and row data
	header, rowData, err := txn.UnwrapMVCC(data)
	if err != nil {
		return nil, fmt.Errorf("unwrap MVCC: %w", err)
	}

	// Decode row values
	values, err := DecodeRow(meta.Schema, rowData)
	if err != nil {
		return nil, fmt.Errorf("decode row: %w", err)
	}

	return &MVCCRow{
		RID:    rid,
		Header: header,
		Values: values,
	}, nil
}

// MarkDeleted sets the XMax on a tuple to mark it as deleted.
// This is called during DELETE and UPDATE operations.
func (m *MVCCTableManager) MarkDeleted(rid storage.RID, tx *txn.Transaction) error {
	// Fetch the current data
	data, err := m.tm.storage.Fetch(rid)
	if err != nil {
		return err
	}

	// Check if we can modify this tuple
	header, _, err := txn.UnwrapMVCC(data)
	if err != nil {
		return err
	}

	canMod, err := txn.CanModify(header, tx.ID, m.txnMgr)
	if err != nil {
		return err
	}
	if !canMod {
		return fmt.Errorf("cannot modify tuple: already modified")
	}

	// Update XMax in the data
	if err := txn.SetXMax(data, tx.ID); err != nil {
		return err
	}

	// Write back to storage
	// We need to use a low-level update that writes the data back
	return m.tm.storage.Update(rid, data)
}

// Scan iterates over all tuples in a table, calling fn for each visible tuple.
// Only tuples visible to the given transaction are passed to fn.
func (m *MVCCTableManager) Scan(tableName string, tx *txn.Transaction, fn func(*MVCCRow) (bool, error)) error {
	meta, err := m.tm.catalog.GetTable(tableName)
	if err != nil {
		return err
	}

	// Iterate over all slots in the table
	// This is the same scanning logic as before, but now we check visibility
	return m.scanAllTuples(tableName, meta.Schema, tx, fn)
}

// scanAllTuples iterates through all tuples and filters by visibility.
func (m *MVCCTableManager) scanAllTuples(tableName string, schema *Schema, tx *txn.Transaction, fn func(*MVCCRow) (bool, error)) error {
	for pageID := uint32(0); pageID < 10000; pageID++ {
		foundAny := false
		for slotID := uint16(0); slotID < 1000; slotID++ {
			rid := storage.RID{Table: tableName, Page: pageID, Slot: slotID}

			data, err := m.tm.storage.Fetch(rid)
			if err != nil {
				errStr := err.Error()
				if contains(errStr, "invalid slot") {
					break // No more slots on this page
				}
				if contains(errStr, "slot empty") {
					foundAny = true
					continue // Deleted slot, skip
				}
				continue
			}

			foundAny = true

			// Extract MVCC header
			header, rowData, err := txn.UnwrapMVCC(data)
			if err != nil {
				continue // Skip malformed tuples
			}

			// Check visibility
			if !txn.IsVisible(header, tx.Snapshot, m.txnMgr, tx.ID) {
				continue // Not visible to this transaction
			}

			// Decode row
			values, err := DecodeRow(schema, rowData)
			if err != nil {
				continue // Skip malformed rows
			}

			row := &MVCCRow{
				RID:    rid,
				Header: header,
				Values: values,
			}

			cont, err := fn(row)
			if err != nil {
				return err
			}
			if !cont {
				return nil
			}
		}

		if !foundAny {
			return nil // No more pages
		}
	}

	return nil
}

// contains is a helper to check if a string contains a substring.
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsImpl(s, substr))
}

func containsImpl(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// CreateTable delegates to the underlying TableManager.
func (m *MVCCTableManager) CreateTable(name string, cols []Column) error {
	return m.tm.CreateTable(name, cols)
}

// CreateTableWithStorage delegates to the underlying TableManager.
func (m *MVCCTableManager) CreateTableWithStorage(name string, cols []Column, storageType string) error {
	return m.tm.CreateTableWithStorage(name, cols, storageType)
}

// DropTable delegates to the underlying TableManager.
func (m *MVCCTableManager) DropTable(name string) error {
	return m.tm.catalog.DropTable(name)
}

// ListTables delegates to the underlying TableManager.
func (m *MVCCTableManager) ListTables() []string {
	return m.tm.ListTables()
}

// DescribeTable delegates to the underlying TableManager.
func (m *MVCCTableManager) DescribeTable(name string) ([]Column, error) {
	return m.tm.DescribeTable(name)
}

// Catalog returns the underlying catalog.
func (m *MVCCTableManager) Catalog() *Catalog {
	return m.tm.Catalog()
}

// GetTableMeta returns the full table metadata.
func (m *MVCCTableManager) GetTableMeta(name string) (*TableMeta, error) {
	return m.tm.GetTableMeta(name)
}

// UpdateTableMeta updates table metadata (for ALTER TABLE operations).
func (m *MVCCTableManager) UpdateTableMeta(meta *TableMeta) error {
	return m.tm.UpdateTableMeta(meta)
}

// RenameTable renames a table.
func (m *MVCCTableManager) RenameTable(oldName, newName string) error {
	return m.tm.RenameTable(oldName, newName)
}

// TruncateTable removes all rows from a table.
func (m *MVCCTableManager) TruncateTable(tableName string) (int, error) {
	return m.tm.TruncateTable(tableName)
}
