package catalog

import (
	"fmt"

	"github.com/JayabrataBasu/VeridicalDB/pkg/storage"
)

// TableManager provides high-level table operations with typed rows.
type TableManager struct {
	catalog  *Catalog
	storage  *storage.Storage
	dataDir  string
	pageSize int
}

// NewTableManager creates a TableManager.
func NewTableManager(dataDir string, pageSize int) (*TableManager, error) {
	cat, err := NewCatalog(dataDir)
	if err != nil {
		return nil, err
	}
	store := storage.NewStorage(dataDir, pageSize)
	return &TableManager{
		catalog:  cat,
		storage:  store,
		dataDir:  dataDir,
		pageSize: pageSize,
	}, nil
}

// Catalog returns the underlying catalog.
func (tm *TableManager) Catalog() *Catalog {
	return tm.catalog
}

// CreateTable creates a new table with the given schema.
func (tm *TableManager) CreateTable(name string, cols []Column) error {
	// Register in catalog
	_, err := tm.catalog.CreateTable(name, cols, "row")
	if err != nil {
		return err
	}
	// Create heap file
	if err := tm.storage.CreateTable(name); err != nil {
		// Rollback catalog entry
		_ = tm.catalog.DropTable(name)
		return err
	}
	return nil
}

// Insert inserts a row into a table, returning the RID.
func (tm *TableManager) Insert(tableName string, values []Value) (storage.RID, error) {
	meta, err := tm.catalog.GetTable(tableName)
	if err != nil {
		return storage.RID{}, err
	}

	// Encode row
	data, err := EncodeRow(meta.Schema, values)
	if err != nil {
		return storage.RID{}, fmt.Errorf("encode row: %w", err)
	}

	// Insert into storage
	rid, err := tm.storage.Insert(tableName, data)
	if err != nil {
		return storage.RID{}, err
	}
	return rid, nil
}

// Fetch retrieves a row by RID and decodes it.
func (tm *TableManager) Fetch(tableName string, rid storage.RID) ([]Value, error) {
	meta, err := tm.catalog.GetTable(tableName)
	if err != nil {
		return nil, err
	}

	data, err := tm.storage.Fetch(rid)
	if err != nil {
		return nil, err
	}

	return DecodeRow(meta.Schema, data)
}

// ListTables returns all table names.
func (tm *TableManager) ListTables() []string {
	return tm.catalog.ListTables()
}

// DescribeTable returns column information for a table.
func (tm *TableManager) DescribeTable(name string) ([]Column, error) {
	meta, err := tm.catalog.GetTable(name)
	if err != nil {
		return nil, err
	}
	return meta.Columns, nil
}
