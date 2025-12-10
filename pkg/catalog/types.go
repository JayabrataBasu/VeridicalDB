// Package catalog provides the type system, schema definitions, and catalog management.
package catalog

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// DataType represents a column data type.
type DataType int

const (
	TypeUnknown DataType = iota
	TypeInt32
	TypeInt64
	TypeText
	TypeBool
	TypeTimestamp
)

// String returns the SQL name of the type.
func (t DataType) String() string {
	switch t {
	case TypeInt32:
		return "INT"
	case TypeInt64:
		return "BIGINT"
	case TypeText:
		return "TEXT"
	case TypeBool:
		return "BOOL"
	case TypeTimestamp:
		return "TIMESTAMP"
	default:
		return "UNKNOWN"
	}
}

// ParseDataType converts a string to DataType.
func ParseDataType(s string) DataType {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "INT", "INT32", "INTEGER":
		return TypeInt32
	case "BIGINT", "INT64":
		return TypeInt64
	case "TEXT", "STRING", "VARCHAR":
		return TypeText
	case "BOOL", "BOOLEAN":
		return TypeBool
	case "TIMESTAMP", "DATETIME":
		return TypeTimestamp
	default:
		return TypeUnknown
	}
}

// IsFixedWidth returns true if the type has a fixed byte width.
func (t DataType) IsFixedWidth() bool {
	switch t {
	case TypeInt32, TypeInt64, TypeBool, TypeTimestamp:
		return true
	default:
		return false
	}
}

// FixedWidth returns the byte width for fixed-width types, 0 otherwise.
func (t DataType) FixedWidth() int {
	switch t {
	case TypeInt32:
		return 4
	case TypeInt64, TypeTimestamp:
		return 8
	case TypeBool:
		return 1
	default:
		return 0
	}
}

// Value represents a typed value that can be stored in a column.
type Value struct {
	Type      DataType
	IsNull    bool
	Int32     int32
	Int64     int64
	Text      string
	Bool      bool
	Timestamp time.Time
}

// NewInt32 creates an INT32 value.
func NewInt32(v int32) Value {
	return Value{Type: TypeInt32, Int32: v}
}

// NewInt64 creates an INT64 value.
func NewInt64(v int64) Value {
	return Value{Type: TypeInt64, Int64: v}
}

// NewText creates a TEXT value.
func NewText(v string) Value {
	return Value{Type: TypeText, Text: v}
}

// NewBool creates a BOOL value.
func NewBool(v bool) Value {
	return Value{Type: TypeBool, Bool: v}
}

// NewTimestamp creates a TIMESTAMP value.
func NewTimestamp(v time.Time) Value {
	return Value{Type: TypeTimestamp, Timestamp: v}
}

// Null creates a NULL value of the given type.
func Null(t DataType) Value {
	return Value{Type: t, IsNull: true}
}

// String returns a human-readable representation.
func (v Value) String() string {
	if v.IsNull {
		return "NULL"
	}
	switch v.Type {
	case TypeInt32:
		return strconv.FormatInt(int64(v.Int32), 10)
	case TypeInt64:
		return strconv.FormatInt(v.Int64, 10)
	case TypeText:
		return v.Text
	case TypeBool:
		if v.Bool {
			return "true"
		}
		return "false"
	case TypeTimestamp:
		return v.Timestamp.Format(time.RFC3339)
	default:
		return "?"
	}
}

// Column defines a column in a table schema.
type Column struct {
	ID            int
	Name          string
	Type          DataType
	NotNull       bool
	PrimaryKey    bool
	HasDefault    bool
	DefaultValue  *Value // pointer to allow nil for no default
	AutoIncrement bool
	CheckExpr     string // CHECK constraint expression (stored as SQL string)
}

// Schema represents the structure of a table.
type Schema struct {
	Columns []Column
}

// NewSchema creates a schema from a slice of columns.
func NewSchema(cols []Column) *Schema {
	// assign IDs if not set
	for i := range cols {
		if cols[i].ID == 0 {
			cols[i].ID = i + 1
		}
	}
	return &Schema{Columns: cols}
}

// ColumnByName finds a column by name (case-insensitive).
func (s *Schema) ColumnByName(name string) (*Column, int) {
	nameUpper := strings.ToUpper(name)
	for i, c := range s.Columns {
		if strings.ToUpper(c.Name) == nameUpper {
			return &s.Columns[i], i
		}
	}
	return nil, -1
}

// Validate checks that values match the schema.
func (s *Schema) Validate(values []Value) error {
	if len(values) != len(s.Columns) {
		return fmt.Errorf("expected %d values, got %d", len(s.Columns), len(values))
	}
	for i, col := range s.Columns {
		v := values[i]
		if v.IsNull && col.NotNull {
			return fmt.Errorf("column %q does not allow NULL", col.Name)
		}
		if !v.IsNull && v.Type != col.Type {
			return fmt.Errorf("column %q expects %s, got %s", col.Name, col.Type, v.Type)
		}
	}
	return nil
}
