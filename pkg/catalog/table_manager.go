package catalog

import (
	"fmt"
	"sync"

	"github.com/JayabrataBasu/VeridicalDB/pkg/storage"
)

// TableManager provides high-level table operations with typed rows.
type TableManager struct {
	catalog        *Catalog
	storage        *storage.Storage
	columnarTables map[string]*storage.ColumnarEngine // columnar table engines
	dataDir        string
	pageSize       int
	mu             sync.RWMutex // Protects all operations
}

// NewTableManager creates a TableManager.
func NewTableManager(dataDir string, pageSize int) (*TableManager, error) {
	cat, err := NewCatalog(dataDir)
	if err != nil {
		return nil, err
	}
	store := storage.NewStorage(dataDir, pageSize)
	return &TableManager{
		catalog:        cat,
		storage:        store,
		columnarTables: make(map[string]*storage.ColumnarEngine),
		dataDir:        dataDir,
		pageSize:       pageSize,
	}, nil
}

// Catalog returns the underlying catalog.
func (tm *TableManager) Catalog() *Catalog {
	return tm.catalog
}

// CreateTable creates a new table with the given schema.
func (tm *TableManager) CreateTable(name string, cols []Column) error {
	return tm.CreateTableWithStorage(name, cols, "row")
}

// CreateTableWithStorage creates a new table with the given schema and storage type.
func (tm *TableManager) CreateTableWithStorage(name string, cols []Column, storageType string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Register in catalog
	_, err := tm.catalog.CreateTable(name, cols, storageType)
	if err != nil {
		return err
	}

	// For columnar tables, create a columnar engine
	if storageType == "column" || storageType == "COLUMN" {
		schema := tm.convertToStorageSchema(cols)
		engine, err := storage.NewColumnarEngine(tm.dataDir, name, schema)
		if err != nil {
			_ = tm.catalog.DropTable(name)
			return fmt.Errorf("create columnar engine: %w", err)
		}
		tm.columnarTables[name] = engine
		return nil
	}

	// Create heap file for row storage
	if err := tm.storage.CreateTable(name); err != nil {
		// Rollback catalog entry
		_ = tm.catalog.DropTable(name)
		return err
	}
	return nil
}

// convertToStorageSchema converts catalog columns to storage schema.
func (tm *TableManager) convertToStorageSchema(cols []Column) *storage.Schema {
	schema := &storage.Schema{
		Columns: make([]storage.ColumnInfo, len(cols)),
	}
	for i, col := range cols {
		schema.Columns[i] = storage.ColumnInfo{
			Name: col.Name,
			Type: convertToStorageType(col.Type),
		}
	}
	return schema
}

// convertToStorageType maps catalog data types to storage column types.
func convertToStorageType(dt DataType) storage.ColumnType {
	switch dt {
	case TypeInt32:
		return storage.TypeInt32
	case TypeInt64:
		return storage.TypeInt64
	case TypeText:
		return storage.TypeString
	case TypeBool:
		return storage.TypeBool
	case TypeTimestamp:
		return storage.TypeInt64 // Store timestamp as int64
	default:
		return storage.TypeString // Default to string
	}
}

// convertToStorageRow converts catalog values to storage row.
func (tm *TableManager) convertToStorageRow(values []Value) *storage.Row {
	row := &storage.Row{
		Values: make([]storage.Value, len(values)),
	}
	for i, v := range values {
		if v.IsNull {
			row.Values[i] = storage.Value{IsNull: true}
			continue
		}
		switch v.Type {
		case TypeInt32:
			row.Values[i] = storage.Value{Data: v.Int32}
		case TypeInt64:
			row.Values[i] = storage.Value{Data: v.Int64}
		case TypeText:
			row.Values[i] = storage.Value{Data: v.Text}
		case TypeBool:
			row.Values[i] = storage.Value{Data: v.Bool}
		case TypeTimestamp:
			row.Values[i] = storage.Value{Data: v.Timestamp.UnixNano()}
		default:
			row.Values[i] = storage.Value{Data: v.Text}
		}
	}
	return row
}

// Insert inserts a row into a table, returning the RID.
func (tm *TableManager) Insert(tableName string, values []Value) (storage.RID, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	meta, err := tm.catalog.GetTable(tableName)
	if err != nil {
		return storage.RID{}, err
	}

	// Check if this is a columnar table
	if engine, ok := tm.columnarTables[tableName]; ok {
		row := tm.convertToStorageRow(values)
		return engine.Insert(row, nil)
	}

	// Row storage path
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
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	meta, err := tm.catalog.GetTable(tableName)
	if err != nil {
		return nil, err
	}

	// Check if this is a columnar table
	if engine, ok := tm.columnarTables[tableName]; ok {
		row, err := engine.Fetch(rid, nil)
		if err != nil {
			return nil, err
		}
		return tm.convertFromStorageRow(row, meta.Columns), nil
	}

	data, err := tm.storage.Fetch(rid)
	if err != nil {
		return nil, err
	}

	return DecodeRow(meta.Schema, data)
}

// convertFromStorageRow converts storage row to catalog values.
func (tm *TableManager) convertFromStorageRow(row *storage.Row, cols []Column) []Value {
	values := make([]Value, len(row.Values))
	for i, v := range row.Values {
		if v.IsNull {
			values[i] = Value{Type: cols[i].Type, IsNull: true}
			continue
		}
		val := Value{Type: cols[i].Type}
		switch cols[i].Type {
		case TypeInt32:
			val.Int32 = v.Data.(int32)
		case TypeInt64:
			val.Int64 = v.Data.(int64)
		case TypeText:
			val.Text = v.Data.(string)
		case TypeBool:
			val.Bool = v.Data.(bool)
		}
		values[i] = val
	}
	return values
}

// ListTables returns all table names.
func (tm *TableManager) ListTables() []string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.catalog.ListTables()
}

// DescribeTable returns column information for a table.
func (tm *TableManager) DescribeTable(name string) ([]Column, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	meta, err := tm.catalog.GetTable(name)
	if err != nil {
		return nil, err
	}
	return meta.Columns, nil
}

// Delete removes a row by RID.
func (tm *TableManager) Delete(rid storage.RID) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Check if this is a columnar table
	if engine, ok := tm.columnarTables[rid.Table]; ok {
		return engine.Delete(rid, nil)
	}

	return tm.storage.Delete(rid)
}

// IsColumnarTable returns true if the table uses columnar storage.
func (tm *TableManager) IsColumnarTable(tableName string) bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	_, ok := tm.columnarTables[tableName]
	return ok
}

// GetColumnarEngine returns the columnar engine for a table, if any.
func (tm *TableManager) GetColumnarEngine(tableName string) (*storage.ColumnarEngine, bool) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	engine, ok := tm.columnarTables[tableName]
	return engine, ok
}
