package catalog

import (
	"path/filepath"
	"testing"
	"time"
)

func TestTypesAndEncoding(t *testing.T) {
	schema := NewSchema([]Column{
		{Name: "id", Type: TypeInt32, NotNull: true},
		{Name: "name", Type: TypeText, NotNull: false},
		{Name: "active", Type: TypeBool, NotNull: true},
		{Name: "created", Type: TypeTimestamp, NotNull: false},
	})

	now := time.Now().Truncate(time.Nanosecond)
	values := []Value{
		NewInt32(42),
		NewText("Alice"),
		NewBool(true),
		NewTimestamp(now),
	}

	// Encode
	data, err := EncodeRow(schema, values)
	if err != nil {
		t.Fatalf("EncodeRow error: %v", err)
	}

	// Decode
	decoded, err := DecodeRow(schema, data)
	if err != nil {
		t.Fatalf("DecodeRow error: %v", err)
	}

	if len(decoded) != len(values) {
		t.Fatalf("expected %d values, got %d", len(values), len(decoded))
	}

	if decoded[0].Int32 != 42 {
		t.Errorf("id: expected 42, got %d", decoded[0].Int32)
	}
	if decoded[1].Text != "Alice" {
		t.Errorf("name: expected Alice, got %s", decoded[1].Text)
	}
	if decoded[2].Bool != true {
		t.Errorf("active: expected true, got %v", decoded[2].Bool)
	}
	if !decoded[3].Timestamp.Equal(now) {
		t.Errorf("created: expected %v, got %v", now, decoded[3].Timestamp)
	}
}

func TestEncodingWithNulls(t *testing.T) {
	schema := NewSchema([]Column{
		{Name: "id", Type: TypeInt32, NotNull: true},
		{Name: "name", Type: TypeText, NotNull: false},
	})

	values := []Value{
		NewInt32(1),
		Null(TypeText),
	}

	data, err := EncodeRow(schema, values)
	if err != nil {
		t.Fatalf("EncodeRow error: %v", err)
	}

	decoded, err := DecodeRow(schema, data)
	if err != nil {
		t.Fatalf("DecodeRow error: %v", err)
	}

	if decoded[1].IsNull != true {
		t.Errorf("expected name to be NULL")
	}
}

func TestSchemaValidation(t *testing.T) {
	schema := NewSchema([]Column{
		{Name: "id", Type: TypeInt32, NotNull: true},
	})

	// NULL on NOT NULL column
	_, err := EncodeRow(schema, []Value{Null(TypeInt32)})
	if err == nil {
		t.Error("expected error for NULL on NOT NULL column")
	}

	// Wrong type
	_, err = EncodeRow(schema, []Value{NewText("oops")})
	if err == nil {
		t.Error("expected error for wrong type")
	}

	// Wrong count
	_, err = EncodeRow(schema, []Value{NewInt32(1), NewInt32(2)})
	if err == nil {
		t.Error("expected error for wrong value count")
	}
}

func TestCatalogPersistence(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "data")

	// Create catalog and add table
	cat, err := NewCatalog(dataDir)
	if err != nil {
		t.Fatalf("NewCatalog error: %v", err)
	}

	cols := []Column{
		{Name: "id", Type: TypeInt32, NotNull: true},
		{Name: "name", Type: TypeText, NotNull: false},
	}
	_, err = cat.CreateTable("users", cols, "row")
	if err != nil {
		t.Fatalf("CreateTable error: %v", err)
	}

	// Reopen catalog
	cat2, err := NewCatalog(dataDir)
	if err != nil {
		t.Fatalf("NewCatalog reopen error: %v", err)
	}

	tables := cat2.ListTables()
	if len(tables) != 1 || tables[0] != "users" {
		t.Errorf("expected [users], got %v", tables)
	}

	meta, err := cat2.GetTable("users")
	if err != nil {
		t.Fatalf("GetTable error: %v", err)
	}
	if len(meta.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(meta.Columns))
	}
}

func TestTableManager(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "data")

	tm, err := NewTableManager(dataDir, 4096)
	if err != nil {
		t.Fatalf("NewTableManager error: %v", err)
	}

	// Create table
	cols := []Column{
		{Name: "id", Type: TypeInt32, NotNull: true},
		{Name: "name", Type: TypeText, NotNull: false},
	}
	if err := tm.CreateTable("users", cols); err != nil {
		t.Fatalf("CreateTable error: %v", err)
	}

	// Insert row
	rid, err := tm.Insert("users", []Value{NewInt32(1), NewText("Alice")})
	if err != nil {
		t.Fatalf("Insert error: %v", err)
	}

	// Fetch row
	values, err := tm.Fetch("users", rid)
	if err != nil {
		t.Fatalf("Fetch error: %v", err)
	}

	if values[0].Int32 != 1 {
		t.Errorf("id: expected 1, got %d", values[0].Int32)
	}
	if values[1].Text != "Alice" {
		t.Errorf("name: expected Alice, got %s", values[1].Text)
	}

	// List tables
	tables := tm.ListTables()
	if len(tables) != 1 {
		t.Errorf("expected 1 table, got %d", len(tables))
	}

	// Describe table
	described, err := tm.DescribeTable("users")
	if err != nil {
		t.Fatalf("DescribeTable error: %v", err)
	}
	if len(described) != 2 {
		t.Errorf("expected 2 columns, got %d", len(described))
	}
}

func TestTableManagerPersistence(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "data")

	// Create and insert
	tm, err := NewTableManager(dataDir, 4096)
	if err != nil {
		t.Fatalf("NewTableManager error: %v", err)
	}

	cols := []Column{
		{Name: "id", Type: TypeInt32, NotNull: true},
		{Name: "value", Type: TypeInt64, NotNull: true},
	}
	if err := tm.CreateTable("counters", cols); err != nil {
		t.Fatalf("CreateTable error: %v", err)
	}

	rid, err := tm.Insert("counters", []Value{NewInt32(100), NewInt64(999999)})
	if err != nil {
		t.Fatalf("Insert error: %v", err)
	}

	// Reopen
	tm2, err := NewTableManager(dataDir, 4096)
	if err != nil {
		t.Fatalf("NewTableManager reopen error: %v", err)
	}

	values, err := tm2.Fetch("counters", rid)
	if err != nil {
		t.Fatalf("Fetch after reopen error: %v", err)
	}

	if values[0].Int32 != 100 || values[1].Int64 != 999999 {
		t.Errorf("values mismatch: got %v", values)
	}
}
