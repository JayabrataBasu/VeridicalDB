// Package storage provides storage engine implementations for VeridicalDB.
// It supports both row-based (heap) and columnar storage formats.

package storage

import (
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

// StorageType identifies the storage format for a table.
type StorageType uint8

const (
	// StorageTypeRow is traditional row-based heap storage.
	// Best for OLTP workloads with point queries and updates.
	StorageTypeRow StorageType = iota

	// StorageTypeColumn is columnar storage.
	// Best for OLAP workloads with analytical queries on few columns.
	StorageTypeColumn
)

func (st StorageType) String() string {
	switch st {
	case StorageTypeRow:
		return "ROW"
	case StorageTypeColumn:
		return "COLUMN"
	default:
		return "UNKNOWN"
	}
}

// ParseStorageType parses a storage type string.
func ParseStorageType(s string) (StorageType, bool) {
	switch s {
	case "ROW", "row", "HEAP", "heap":
		return StorageTypeRow, true
	case "COLUMN", "column", "COLUMNAR", "columnar":
		return StorageTypeColumn, true
	default:
		return StorageTypeRow, false
	}
}

// ColumnInfo describes a column in a table schema.
type ColumnInfo struct {
	Name    string
	Type    ColumnType
	NotNull bool
	ColID   int
}

// ColumnType represents the data type of a column.
type ColumnType uint8

const (
	TypeInt32 ColumnType = iota
	TypeInt64
	TypeFloat64
	TypeBool
	TypeString
	TypeBytes
	TypeTimestamp
)

func (ct ColumnType) String() string {
	switch ct {
	case TypeInt32:
		return "INT32"
	case TypeInt64:
		return "INT64"
	case TypeFloat64:
		return "FLOAT64"
	case TypeBool:
		return "BOOL"
	case TypeString:
		return "STRING"
	case TypeBytes:
		return "BYTES"
	case TypeTimestamp:
		return "TIMESTAMP"
	default:
		return "UNKNOWN"
	}
}

// IsFixedWidth returns true if the type has a fixed byte width.
func (ct ColumnType) IsFixedWidth() bool {
	switch ct {
	case TypeInt32, TypeInt64, TypeFloat64, TypeBool, TypeTimestamp:
		return true
	default:
		return false
	}
}

// Width returns the byte width for fixed-width types, or 0 for variable-width.
func (ct ColumnType) Width() int {
	switch ct {
	case TypeInt32:
		return 4
	case TypeInt64, TypeFloat64, TypeTimestamp:
		return 8
	case TypeBool:
		return 1
	default:
		return 0 // Variable width
	}
}

// Schema represents a table's column definitions.
type Schema struct {
	Columns []ColumnInfo
}

// ColumnCount returns the number of columns.
func (s *Schema) ColumnCount() int {
	return len(s.Columns)
}

// ColumnByName returns a column by name, or nil if not found.
func (s *Schema) ColumnByName(name string) *ColumnInfo {
	for i := range s.Columns {
		if s.Columns[i].Name == name {
			return &s.Columns[i]
		}
	}
	return nil
}

// ColumnIndex returns the index of a column by name, or -1 if not found.
func (s *Schema) ColumnIndex(name string) int {
	for i := range s.Columns {
		if s.Columns[i].Name == name {
			return i
		}
	}
	return -1
}

// Row represents a single row of data with values for each column.
type Row struct {
	Values []Value
}

// Value represents a single column value (can be null).
type Value struct {
	IsNull bool
	Data   interface{} // int32, int64, float64, bool, string, []byte, time.Time
}

// StorageEngine is the interface for table storage backends.
// Both row and columnar storage implement this interface.
type StorageEngine interface {
	// Insert adds a new row to the table.
	// Returns the RID (row identifier) for the inserted row.
	Insert(row *Row, tx *txn.Transaction) (RID, error)

	// Delete marks a row as deleted.
	Delete(rid RID, tx *txn.Transaction) error

	// Update modifies an existing row.
	Update(rid RID, row *Row, tx *txn.Transaction) error

	// Fetch retrieves a single row by RID.
	Fetch(rid RID, tx *txn.Transaction) (*Row, error)

	// Scan returns an iterator over all visible rows.
	// projectedCols specifies which columns to read (nil = all columns).
	Scan(projectedCols []int, tx *txn.Transaction) (RowIterator, error)

	// ScanWithPredicate returns an iterator with a filter predicate.
	// The predicate is applied during scan to skip non-matching rows.
	ScanWithPredicate(projectedCols []int, pred Predicate, tx *txn.Transaction) (RowIterator, error)

	// Schema returns the table schema.
	Schema() *Schema

	// StorageType returns the storage format (ROW or COLUMN).
	StorageType() StorageType

	// Close releases resources.
	Close() error

	// Flush ensures all data is persisted to disk.
	Flush() error
}

// RowIterator iterates over rows from a scan.
type RowIterator interface {
	// Next advances to the next row.
	// Returns false when there are no more rows.
	Next() bool

	// Row returns the current row.
	// Only valid after Next() returns true.
	Row() *Row

	// RID returns the RID of the current row.
	RID() RID

	// Err returns any error encountered during iteration.
	Err() error

	// Close releases iterator resources.
	Close() error
}

// Predicate is a filter condition for scans.
type Predicate interface {
	// Evaluate returns true if the row matches the predicate.
	Evaluate(row *Row) bool

	// Columns returns the column indices needed to evaluate this predicate.
	Columns() []int
}

// SimplePredicate is a basic comparison predicate.
type SimplePredicate struct {
	ColIndex int
	Op       CompareOp
	Value    Value
}

// CompareOp is a comparison operator.
type CompareOp uint8

const (
	OpEq CompareOp = iota // =
	OpNe                  // !=
	OpLt                  // <
	OpLe                  // <=
	OpGt                  // >
	OpGe                  // >=
)

func (op CompareOp) String() string {
	switch op {
	case OpEq:
		return "="
	case OpNe:
		return "!="
	case OpLt:
		return "<"
	case OpLe:
		return "<="
	case OpGt:
		return ">"
	case OpGe:
		return ">="
	default:
		return "?"
	}
}

// Evaluate implements Predicate for SimplePredicate.
func (p *SimplePredicate) Evaluate(row *Row) bool {
	if p.ColIndex >= len(row.Values) {
		return false
	}

	val := row.Values[p.ColIndex]

	// Handle NULL comparisons
	if val.IsNull || p.Value.IsNull {
		return false // NULL comparisons always return false (except IS NULL)
	}

	cmp := compareValues(val, p.Value)
	switch p.Op {
	case OpEq:
		return cmp == 0
	case OpNe:
		return cmp != 0
	case OpLt:
		return cmp < 0
	case OpLe:
		return cmp <= 0
	case OpGt:
		return cmp > 0
	case OpGe:
		return cmp >= 0
	default:
		return false
	}
}

// Columns implements Predicate for SimplePredicate.
func (p *SimplePredicate) Columns() []int {
	return []int{p.ColIndex}
}

// compareValues compares two values.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
func compareValues(a, b Value) int {
	switch av := a.Data.(type) {
	case int32:
		bv := b.Data.(int32)
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	case int64:
		bv := b.Data.(int64)
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	case float64:
		bv := b.Data.(float64)
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	case string:
		bv := b.Data.(string)
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	case bool:
		bv := b.Data.(bool)
		if !av && bv {
			return -1
		} else if av && !bv {
			return 1
		}
		return 0
	default:
		return 0
	}
}

// AndPredicate combines multiple predicates with AND logic.
type AndPredicate struct {
	Predicates []Predicate
}

// Evaluate implements Predicate for AndPredicate.
func (p *AndPredicate) Evaluate(row *Row) bool {
	for _, pred := range p.Predicates {
		if !pred.Evaluate(row) {
			return false
		}
	}
	return true
}

// Columns implements Predicate for AndPredicate.
func (p *AndPredicate) Columns() []int {
	colSet := make(map[int]bool)
	for _, pred := range p.Predicates {
		for _, col := range pred.Columns() {
			colSet[col] = true
		}
	}
	cols := make([]int, 0, len(colSet))
	for col := range colSet {
		cols = append(cols, col)
	}
	return cols
}
