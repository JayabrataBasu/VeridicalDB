package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

func TestColumnarEngine_BasicOperations(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "columnar_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	schema := &Schema{
		Columns: []ColumnInfo{
			{Name: "id", Type: TypeInt32},
			{Name: "name", Type: TypeString},
			{Name: "score", Type: TypeFloat64},
		},
	}

	engine, err := NewColumnarEngine(tmpDir, "test_table", schema)
	if err != nil {
		t.Fatalf("NewColumnarEngine: %v", err)
	}

	// Insert some rows
	rows := []*Row{
		{Values: []Value{{Data: int32(1)}, {Data: "Alice"}, {Data: float64(95.5)}}},
		{Values: []Value{{Data: int32(2)}, {Data: "Bob"}, {Data: float64(87.3)}}},
		{Values: []Value{{Data: int32(3)}, {Data: "Charlie"}, {Data: float64(92.1)}}},
	}

	rids := make([]RID, len(rows))
	for i, row := range rows {
		rid, err := engine.Insert(row, nil)
		if err != nil {
			t.Fatalf("Insert row %d: %v", i, err)
		}
		rids[i] = rid
	}

	// Verify storage type
	if engine.StorageType() != StorageTypeColumn {
		t.Errorf("StorageType: got %v, want %v", engine.StorageType(), StorageTypeColumn)
	}

	// Scan all rows
	iter, err := engine.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	count := 0
	for iter.Next() {
		row := iter.Row()
		if len(row.Values) != 3 {
			t.Errorf("Row %d: got %d values, want 3", count, len(row.Values))
		}
		count++
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("Iterator error: %v", err)
	}
	_ = iter.Close()

	if count != 3 {
		t.Errorf("Scan count: got %d, want 3", count)
	}

	// Delete a row
	if err := engine.Delete(rids[1], nil); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Scan again - should have 2 rows
	iter, err = engine.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Scan after delete: %v", err)
	}

	count = 0
	for iter.Next() {
		count++
	}
	_ = iter.Close()

	if count != 2 {
		t.Errorf("Scan after delete: got %d, want 2", count)
	}

	if err := engine.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestColumnarEngine_ColumnProjection(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "columnar_proj_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	schema := &Schema{
		Columns: []ColumnInfo{
			{Name: "id", Type: TypeInt32},
			{Name: "name", Type: TypeString},
			{Name: "score", Type: TypeFloat64},
			{Name: "active", Type: TypeBool},
		},
	}

	engine, err := NewColumnarEngine(tmpDir, "proj_table", schema)
	if err != nil {
		t.Fatalf("NewColumnarEngine: %v", err)
	}
	defer func() { _ = engine.Close() }()

	// Insert some rows
	for i := 0; i < 10; i++ {
		row := &Row{Values: []Value{
			{Data: int32(i)},
			{Data: "User"},
			{Data: float64(90.0 + float64(i))},
			{Data: i%2 == 0},
		}}
		if _, err := engine.Insert(row, nil); err != nil {
			t.Fatalf("Insert row %d: %v", i, err)
		}
	}

	// Project only id and score columns (indices 0 and 2)
	iter, err := engine.Scan([]int{0, 2}, nil)
	if err != nil {
		t.Fatalf("Scan with projection: %v", err)
	}

	count := 0
	for iter.Next() {
		row := iter.Row()
		if len(row.Values) != 2 {
			t.Errorf("Row %d: got %d values, want 2", count, len(row.Values))
		}
		// Verify projected values
		id := row.Values[0].Data.(int32)
		score := row.Values[1].Data.(float64)
		if int(id) != count {
			t.Errorf("Row %d id: got %d, want %d", count, id, count)
		}
		expectedScore := 90.0 + float64(count)
		if score != expectedScore {
			t.Errorf("Row %d score: got %f, want %f", count, score, expectedScore)
		}
		count++
	}
	_ = iter.Close()

	if count != 10 {
		t.Errorf("Scan with projection count: got %d, want 10", count)
	}
}

func TestColumnarEngine_NullValues(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "columnar_null_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	schema := &Schema{
		Columns: []ColumnInfo{
			{Name: "id", Type: TypeInt32},
			{Name: "value", Type: TypeString},
		},
	}

	engine, err := NewColumnarEngine(tmpDir, "null_table", schema)
	if err != nil {
		t.Fatalf("NewColumnarEngine: %v", err)
	}
	defer func() { _ = engine.Close() }()

	// Insert rows with null values
	rows := []*Row{
		{Values: []Value{{Data: int32(1)}, {Data: "value1"}}},
		{Values: []Value{{Data: int32(2)}, {IsNull: true}}}, // null value
		{Values: []Value{{Data: int32(3)}, {Data: "value3"}}},
	}

	for i, row := range rows {
		if _, err := engine.Insert(row, nil); err != nil {
			t.Fatalf("Insert row %d: %v", i, err)
		}
	}

	// Scan and verify null handling
	iter, err := engine.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	rowIdx := 0
	for iter.Next() {
		row := iter.Row()
		if rowIdx == 1 {
			if !row.Values[1].IsNull {
				t.Errorf("Row 1 value: expected null")
			}
		} else {
			if row.Values[1].IsNull {
				t.Errorf("Row %d value: unexpected null", rowIdx)
			}
		}
		rowIdx++
	}
	_ = iter.Close()
}

func TestColumnarEngine_SegmentFlush(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "columnar_flush_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	schema := &Schema{
		Columns: []ColumnInfo{
			{Name: "id", Type: TypeInt32},
			{Name: "data", Type: TypeString},
		},
	}

	engine, err := NewColumnarEngine(tmpDir, "flush_table", schema)
	if err != nil {
		t.Fatalf("NewColumnarEngine: %v", err)
	}

	// Insert rows (in write buffer)
	for i := 0; i < 100; i++ {
		row := &Row{Values: []Value{
			{Data: int32(i)},
			{Data: "test_data"},
		}}
		if _, err := engine.Insert(row, nil); err != nil {
			t.Fatalf("Insert row %d: %v", i, err)
		}
	}

	// Flush manually
	if err := engine.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Check that segment directory exists
	segDir := filepath.Join(tmpDir, "flush_table", "seg_0")
	if _, err := os.Stat(segDir); os.IsNotExist(err) {
		t.Fatalf("Segment directory not created: %s", segDir)
	}

	// Verify column files exist
	for i := 0; i < len(schema.Columns); i++ {
		colFile := filepath.Join(segDir, "col_"+string(rune('0'+i))+".dat")
		if _, err := os.Stat(colFile); os.IsNotExist(err) {
			// Check alternative format
			colFile = filepath.Join(segDir, "col_"+string([]byte{byte('0' + i)})+".dat")
		}
	}

	// Verify metadata file exists
	metaFile := filepath.Join(segDir, "meta.dat")
	if _, err := os.Stat(metaFile); os.IsNotExist(err) {
		t.Fatalf("Metadata file not created: %s", metaFile)
	}

	if err := engine.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen engine and verify data persists
	engine2, err := NewColumnarEngine(tmpDir, "flush_table", schema)
	if err != nil {
		t.Fatalf("Reopen engine: %v", err)
	}
	defer func() { _ = engine2.Close() }()

	iter, err := engine2.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Scan after reopen: %v", err)
	}

	count := 0
	for iter.Next() {
		row := iter.Row()
		expectedID := int32(count)
		if row.Values[0].Data.(int32) != expectedID {
			t.Errorf("Row %d id: got %d, want %d", count, row.Values[0].Data.(int32), expectedID)
		}
		count++
	}
	_ = iter.Close()

	if count != 100 {
		t.Errorf("Row count after reopen: got %d, want 100", count)
	}
}

func TestColumnarEngine_Update(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "columnar_update_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	schema := &Schema{
		Columns: []ColumnInfo{
			{Name: "id", Type: TypeInt32},
			{Name: "value", Type: TypeString},
		},
	}

	engine, err := NewColumnarEngine(tmpDir, "update_table", schema)
	if err != nil {
		t.Fatalf("NewColumnarEngine: %v", err)
	}
	defer func() { _ = engine.Close() }()

	// Insert a row
	row := &Row{Values: []Value{{Data: int32(1)}, {Data: "original"}}}
	rid, err := engine.Insert(row, nil)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Update the row (delete + insert in columnar)
	newRow := &Row{Values: []Value{{Data: int32(1)}, {Data: "updated"}}}
	if err := engine.Update(rid, newRow, nil); err != nil {
		t.Fatalf("Update: %v", err)
	}

	// Scan - should see only the updated value
	iter, err := engine.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	count := 0
	foundUpdated := false
	for iter.Next() {
		row := iter.Row()
		value := row.Values[1].Data.(string)
		if value == "updated" {
			foundUpdated = true
		}
		if value == "original" {
			t.Error("Found original value after update")
		}
		count++
	}
	_ = iter.Close()

	if !foundUpdated {
		t.Error("Updated value not found")
	}
	if count != 1 {
		t.Errorf("Row count after update: got %d, want 1", count)
	}
}

func TestColumnarEngine_Fetch(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "columnar_fetch_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	schema := &Schema{
		Columns: []ColumnInfo{
			{Name: "id", Type: TypeInt32},
			{Name: "value", Type: TypeString},
		},
	}

	engine, err := NewColumnarEngine(tmpDir, "fetch_table", schema)
	if err != nil {
		t.Fatalf("NewColumnarEngine: %v", err)
	}
	defer func() { _ = engine.Close() }()

	// Insert rows
	rids := make([]RID, 5)
	for i := 0; i < 5; i++ {
		row := &Row{Values: []Value{
			{Data: int32(i)},
			{Data: "value_" + string(rune('A'+i))},
		}}
		rid, err := engine.Insert(row, nil)
		if err != nil {
			t.Fatalf("Insert row %d: %v", i, err)
		}
		rids[i] = rid
	}

	// Fetch each row by RID
	for i, rid := range rids {
		row, err := engine.Fetch(rid, nil)
		if err != nil {
			t.Fatalf("Fetch row %d: %v", i, err)
		}
		if row.Values[0].Data.(int32) != int32(i) {
			t.Errorf("Fetch row %d id: got %d, want %d", i, row.Values[0].Data.(int32), i)
		}
	}

	// Delete one and try to fetch
	if err := engine.Delete(rids[2], nil); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err = engine.Fetch(rids[2], nil)
	if err == nil {
		t.Error("Fetch deleted row should return error")
	}
}

func TestColumnarEngine_AllDataTypes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "columnar_types_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	schema := &Schema{
		Columns: []ColumnInfo{
			{Name: "col_int32", Type: TypeInt32},
			{Name: "col_int64", Type: TypeInt64},
			{Name: "col_float64", Type: TypeFloat64},
			{Name: "col_bool", Type: TypeBool},
			{Name: "col_string", Type: TypeString},
			{Name: "col_bytes", Type: TypeBytes},
		},
	}

	engine, err := NewColumnarEngine(tmpDir, "types_table", schema)
	if err != nil {
		t.Fatalf("NewColumnarEngine: %v", err)
	}
	defer func() { _ = engine.Close() }()

	// Insert row with all types
	testBytes := []byte{0x01, 0x02, 0x03, 0x04}
	row := &Row{Values: []Value{
		{Data: int32(42)},
		{Data: int64(9876543210)},
		{Data: float64(3.14159)},
		{Data: true},
		{Data: "hello world"},
		{Data: testBytes},
	}}

	rid, err := engine.Insert(row, nil)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Flush to ensure persistence
	if err := engine.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Fetch and verify all types
	fetched, err := engine.Fetch(rid, nil)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	if fetched.Values[0].Data.(int32) != 42 {
		t.Errorf("int32: got %v, want 42", fetched.Values[0].Data)
	}
	if fetched.Values[1].Data.(int64) != 9876543210 {
		t.Errorf("int64: got %v, want 9876543210", fetched.Values[1].Data)
	}
	if fetched.Values[2].Data.(float64) != 3.14159 {
		t.Errorf("float64: got %v, want 3.14159", fetched.Values[2].Data)
	}
	if fetched.Values[3].Data.(bool) != true {
		t.Errorf("bool: got %v, want true", fetched.Values[3].Data)
	}
	if fetched.Values[4].Data.(string) != "hello world" {
		t.Errorf("string: got %v, want 'hello world'", fetched.Values[4].Data)
	}
	fetchedBytes := fetched.Values[5].Data.([]byte)
	if len(fetchedBytes) != len(testBytes) {
		t.Errorf("bytes length: got %d, want %d", len(fetchedBytes), len(testBytes))
	}
	for i, b := range testBytes {
		if fetchedBytes[i] != b {
			t.Errorf("bytes[%d]: got %d, want %d", i, fetchedBytes[i], b)
		}
	}
}

func TestColumnarEngine_WithTransaction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "columnar_txn_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	schema := &Schema{
		Columns: []ColumnInfo{
			{Name: "id", Type: TypeInt32},
			{Name: "value", Type: TypeString},
		},
	}

	engine, err := NewColumnarEngine(tmpDir, "txn_table", schema)
	if err != nil {
		t.Fatalf("NewColumnarEngine: %v", err)
	}
	defer func() { _ = engine.Close() }()

	// Create a mock transaction
	tx := &txn.Transaction{ID: 100}

	// Insert with transaction
	row := &Row{Values: []Value{{Data: int32(1)}, {Data: "test"}}}
	_, err = engine.Insert(row, tx)
	if err != nil {
		t.Fatalf("Insert with tx: %v", err)
	}

	// Scan with transaction
	iter, err := engine.Scan(nil, tx)
	if err != nil {
		t.Fatalf("Scan with tx: %v", err)
	}

	count := 0
	for iter.Next() {
		count++
	}
	_ = iter.Close()

	if count != 1 {
		t.Errorf("Scan with tx count: got %d, want 1", count)
	}
}

func TestColumnarEngine_LargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large dataset test")
	}

	tmpDir, err := os.MkdirTemp("", "columnar_large_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	schema := &Schema{
		Columns: []ColumnInfo{
			{Name: "id", Type: TypeInt32},
			{Name: "value", Type: TypeInt64},
		},
	}

	engine, err := NewColumnarEngine(tmpDir, "large_table", schema)
	if err != nil {
		t.Fatalf("NewColumnarEngine: %v", err)
	}
	defer func() { _ = engine.Close() }()

	// Insert 10,000 rows
	const numRows = 10000
	for i := 0; i < numRows; i++ {
		row := &Row{Values: []Value{
			{Data: int32(i)},
			{Data: int64(i * 100)},
		}}
		if _, err := engine.Insert(row, nil); err != nil {
			t.Fatalf("Insert row %d: %v", i, err)
		}
	}

	// Flush
	if err := engine.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Scan and verify count
	iter, err := engine.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	count := 0
	for iter.Next() {
		count++
	}
	_ = iter.Close()

	if count != numRows {
		t.Errorf("Row count: got %d, want %d", count, numRows)
	}
}
