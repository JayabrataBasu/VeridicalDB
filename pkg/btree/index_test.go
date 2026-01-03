package btree

import (
	"os"
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/storage"
)

func TestIndexManagerCreate(t *testing.T) {
	dir := tempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	im, err := NewIndexManager(dir, 4096)
	if err != nil {
		t.Fatalf("Failed to create index manager: %v", err)
	}
	defer func() { _ = im.Close() }()

	// Create an index
	err = im.CreateIndex(IndexMeta{
		Name:      "idx_users_email",
		TableName: "users",
		Columns:   []string{"email"},
		Unique:    true,
	})
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	// Verify it exists
	idx, err := im.GetIndex("idx_users_email")
	if err != nil {
		t.Fatalf("GetIndex failed: %v", err)
	}
	if idx.Name != "idx_users_email" {
		t.Errorf("Expected name idx_users_email, got %s", idx.Name)
	}
	if idx.TableName != "users" {
		t.Errorf("Expected table users, got %s", idx.TableName)
	}
	if !idx.Unique {
		t.Error("Expected unique=true")
	}
}

func TestIndexManagerDuplicate(t *testing.T) {
	dir := tempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	im, err := NewIndexManager(dir, 4096)
	if err != nil {
		t.Fatalf("Failed to create index manager: %v", err)
	}
	defer func() { _ = im.Close() }()

	// Create an index
	err = im.CreateIndex(IndexMeta{
		Name:      "idx_test",
		TableName: "test",
		Columns:   []string{"id"},
	})
	if err != nil {
		t.Fatalf("First CreateIndex failed: %v", err)
	}

	// Try to create duplicate
	err = im.CreateIndex(IndexMeta{
		Name:      "idx_test",
		TableName: "test",
		Columns:   []string{"name"},
	})
	if err != ErrIndexExists {
		t.Errorf("Expected ErrIndexExists, got %v", err)
	}
}

func TestIndexManagerDrop(t *testing.T) {
	dir := tempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	im, err := NewIndexManager(dir, 4096)
	if err != nil {
		t.Fatalf("Failed to create index manager: %v", err)
	}
	defer func() { _ = im.Close() }()

	// Create an index
	_ = im.CreateIndex(IndexMeta{
		Name:      "idx_drop",
		TableName: "test",
		Columns:   []string{"id"},
	})

	// Drop it
	if err := im.DropIndex("idx_drop"); err != nil {
		t.Fatalf("DropIndex failed: %v", err)
	}

	// Should not exist
	_, err = im.GetIndex("idx_drop")
	if err != ErrIndexNotFound {
		t.Errorf("Expected ErrIndexNotFound, got %v", err)
	}
}

func TestIndexManagerInsertSearch(t *testing.T) {
	dir := tempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	im, err := NewIndexManager(dir, 4096)
	if err != nil {
		t.Fatalf("Failed to create index manager: %v", err)
	}
	defer func() { _ = im.Close() }()

	// Create an index
	_ = im.CreateIndex(IndexMeta{
		Name:      "idx_search",
		TableName: "test",
		Columns:   []string{"id"},
		Unique:    true,
	})

	// Insert some entries
	for i := 0; i < 100; i++ {
		key := EncodeIntKey(int64(i))
		rid := storage.RID{Table: "test", Page: uint32(i / 10), Slot: uint16(i % 10)}
		if err := im.Insert("idx_search", key, rid); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Search
	for i := 0; i < 100; i++ {
		key := EncodeIntKey(int64(i))
		rid, err := im.Search("idx_search", key)
		if err != nil {
			t.Fatalf("Search %d failed: %v", i, err)
		}
		if rid.Page != uint32(i/10) || rid.Slot != uint16(i%10) {
			t.Errorf("Search %d: expected %d.%d, got %d.%d",
				i, i/10, i%10, rid.Page, rid.Slot)
		}
	}
}

func TestIndexManagerRangeSearch(t *testing.T) {
	dir := tempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	im, err := NewIndexManager(dir, 4096)
	if err != nil {
		t.Fatalf("Failed to create index manager: %v", err)
	}
	defer func() { _ = im.Close() }()

	_ = im.CreateIndex(IndexMeta{
		Name:      "idx_range",
		TableName: "test",
		Columns:   []string{"id"},
	})

	// Insert 0-99
	for i := 0; i < 100; i++ {
		key := EncodeIntKey(int64(i))
		rid := storage.RID{Page: uint32(i), Slot: 0}
		_ = im.Insert("idx_range", key, rid)
	}

	// Range search [25, 50]
	results, err := im.SearchRange("idx_range", EncodeIntKey(25), EncodeIntKey(50))
	if err != nil {
		t.Fatalf("SearchRange failed: %v", err)
	}

	if len(results) != 26 {
		t.Errorf("Expected 26 results, got %d", len(results))
	}
}

func TestIndexManagerListIndexes(t *testing.T) {
	dir := tempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	im, err := NewIndexManager(dir, 4096)
	if err != nil {
		t.Fatalf("Failed to create index manager: %v", err)
	}
	defer func() { _ = im.Close() }()

	// Create indexes on different tables
	_ = im.CreateIndex(IndexMeta{Name: "idx_a", TableName: "users", Columns: []string{"id"}})
	_ = im.CreateIndex(IndexMeta{Name: "idx_b", TableName: "users", Columns: []string{"email"}})
	_ = im.CreateIndex(IndexMeta{Name: "idx_c", TableName: "orders", Columns: []string{"id"}})

	// List all
	all := im.ListIndexes("")
	if len(all) != 3 {
		t.Errorf("Expected 3 indexes, got %d", len(all))
	}

	// List by table
	userIndexes := im.ListIndexes("users")
	if len(userIndexes) != 2 {
		t.Errorf("Expected 2 user indexes, got %d", len(userIndexes))
	}

	orderIndexes := im.ListIndexes("orders")
	if len(orderIndexes) != 1 {
		t.Errorf("Expected 1 order index, got %d", len(orderIndexes))
	}
}

func TestIndexManagerPersistence(t *testing.T) {
	dir := tempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	// Create and populate
	im1, err := NewIndexManager(dir, 4096)
	if err != nil {
		t.Fatalf("Failed to create index manager: %v", err)
	}

	_ = im1.CreateIndex(IndexMeta{
		Name:      "idx_persist",
		TableName: "test",
		Columns:   []string{"id"},
		Unique:    true,
	})

	for i := 0; i < 50; i++ {
		key := EncodeIntKey(int64(i))
		rid := storage.RID{Page: uint32(i), Slot: 0}
		_ = im1.Insert("idx_persist", key, rid)
	}

	_ = im1.Close()

	// Reopen
	im2, err := NewIndexManager(dir, 4096)
	if err != nil {
		t.Fatalf("Failed to reopen index manager: %v", err)
	}
	defer func() { _ = im2.Close() }()

	// Verify index exists
	idx, err := im2.GetIndex("idx_persist")
	if err != nil {
		t.Fatalf("GetIndex after reopen failed: %v", err)
	}
	if idx.Name != "idx_persist" {
		t.Errorf("Expected idx_persist, got %s", idx.Name)
	}

	// Verify data persisted
	for i := 0; i < 50; i++ {
		key := EncodeIntKey(int64(i))
		rid, err := im2.Search("idx_persist", key)
		if err != nil {
			t.Fatalf("Search %d after reopen failed: %v", i, err)
		}
		if rid.Page != uint32(i) {
			t.Errorf("Key %d: expected page %d, got %d", i, i, rid.Page)
		}
	}
}

func TestIntKeyEncoding(t *testing.T) {
	tests := []int64{
		0,
		1,
		-1,
		100,
		-100,
		1000000,
		-1000000,
		1 << 62,
		-(1 << 62),
	}

	for _, v := range tests {
		key := EncodeIntKey(v)
		decoded := DecodeIntKey(key)
		if decoded != v {
			t.Errorf("Encoding %d: got %d", v, decoded)
		}
	}

	// Verify ordering
	for i := -100; i < 99; i++ {
		key1 := EncodeIntKey(int64(i))
		key2 := EncodeIntKey(int64(i + 1))
		if compareKeys(key1, key2) >= 0 {
			t.Errorf("%d should sort before %d", i, i+1)
		}
	}
}

func TestCompositeKeyEncoding(t *testing.T) {
	// Encode composite key
	key := EncodeCompositeKey(
		EncodeIntKey(42),
		EncodeStringKey("hello"),
	)

	if len(key) == 0 {
		t.Error("Composite key should not be empty")
	}

	// Different composite keys should be different
	key2 := EncodeCompositeKey(
		EncodeIntKey(42),
		EncodeStringKey("world"),
	)

	if compareKeys(key, key2) == 0 {
		t.Error("Different composite keys should not be equal")
	}
}
