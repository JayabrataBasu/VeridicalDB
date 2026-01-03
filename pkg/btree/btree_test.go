package btree

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/storage"
)

const testPageSize = 4096

// Test helper: create a temp directory for testing
func tempDir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "btree_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	return dir
}

// Test helper: create a BTree with a pager
func createTestBTree(t *testing.T, dir string, name string, unique bool) (*BTree, *storage.Pager) {
	pager, err := storage.OpenPager(dir, name, testPageSize)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}

	bt, err := New(Config{
		Pager:      pager,
		PageSize:   testPageSize,
		Unique:     unique,
		RootPageID: InvalidPageID, // Signal new tree
	})
	if err != nil {
		pager.Close()
		t.Fatalf("Failed to create B+ tree: %v", err)
	}

	return bt, pager
}

// Test helper: create an int key (big-endian for proper sorting)
func intKey(n int) []byte {
	key := make([]byte, 8)
	for i := 7; i >= 0; i-- {
		key[i] = byte(n & 0xff)
		n >>= 8
	}
	return key
}

// Test helper: create a RID for testing
// Note: B+ tree doesn't store Table name, only Page and Slot
func testRID(page uint32, slot uint16) storage.RID {
	return storage.RID{Table: "", Page: page, Slot: slot}
}

// Test helper: compare RIDs (ignoring Table name since B+ tree doesn't store it)
func sameRID(a, b storage.RID) bool {
	return a.Page == b.Page && a.Slot == b.Slot
}

// === Basic Tests ===

func TestBTreeCreateNew(t *testing.T) {
	dir := tempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	bt, pager := createTestBTree(t, dir, "test.idx", true)
	defer pager.Close()

	// Verify root page is set
	if bt.RootPage() == InvalidPageID {
		t.Error("Root page should not be invalid")
	}
}

func TestBTreeSingleInsertSearch(t *testing.T) {
	dir := tempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	bt, pager := createTestBTree(t, dir, "test.idx", true)
	defer pager.Close()

	// Insert one key
	key := []byte("hello")
	rid := testRID(1, 5)

	if err := bt.Insert(key, rid); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Search for the key
	foundRID, err := bt.Search(key)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if foundRID != rid {
		t.Errorf("Expected RID %v, got %v", rid, foundRID)
	}
}

func TestBTreeMultipleInserts(t *testing.T) {
	dir := tempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	bt, pager := createTestBTree(t, dir, "test.idx", true)
	defer pager.Close()

	// Insert 100 keys sequentially
	count := 100
	for i := 0; i < count; i++ {
		key := intKey(i)
		rid := testRID(uint32(i/10), uint16(i%10))
		if err := bt.Insert(key, rid); err != nil {
			t.Fatalf("Insert failed for key %d: %v", i, err)
		}
	}

	// Verify all keys can be found
	for i := 0; i < count; i++ {
		key := intKey(i)
		expectedRID := testRID(uint32(i/10), uint16(i%10))
		foundRID, err := bt.Search(key)
		if err != nil {
			t.Fatalf("Search failed for key %d: %v", i, err)
		}
		if foundRID != expectedRID {
			t.Errorf("Key %d: expected RID %v, got %v", i, expectedRID, foundRID)
		}
	}
}

func TestBTreeSearchNotFound(t *testing.T) {
	dir := tempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	bt, pager := createTestBTree(t, dir, "test.idx", true)
	defer pager.Close()

	// Insert some keys
	for i := 0; i < 10; i++ {
		key := intKey(i * 2) // Even numbers only
		rid := testRID(uint32(i), 0)
		bt.Insert(key, rid)
	}

	// Search for non-existent key (odd number)
	_, err := bt.Search(intKey(5))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}

	// Search for key beyond range
	_, err = bt.Search(intKey(100))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for key 100, got %v", err)
	}
}

func TestBTreeDuplicateKey(t *testing.T) {
	dir := tempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	bt, pager := createTestBTree(t, dir, "test.idx", true) // unique=true
	defer pager.Close()

	key := []byte("duplicate")
	rid1 := testRID(1, 1)
	rid2 := testRID(2, 2)

	// First insert should succeed
	if err := bt.Insert(key, rid1); err != nil {
		t.Fatalf("First insert failed: %v", err)
	}

	// Second insert with same key should fail
	err := bt.Insert(key, rid2)
	if err != ErrDuplicateKey {
		t.Errorf("Expected ErrDuplicateKey, got %v", err)
	}

	// Original value should remain
	foundRID, _ := bt.Search(key)
	if foundRID != rid1 {
		t.Errorf("Expected original RID %v, got %v", rid1, foundRID)
	}
}

func TestBTreeNonUniqueDuplicateKey(t *testing.T) {
	dir := tempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	bt, pager := createTestBTree(t, dir, "test.idx", false) // unique=false
	defer pager.Close()

	key := []byte("duplicate")
	rid1 := testRID(1, 1)
	rid2 := testRID(2, 2)

	// First insert should succeed
	if err := bt.Insert(key, rid1); err != nil {
		t.Fatalf("First insert failed: %v", err)
	}

	// Second insert with same key should also succeed (non-unique index)
	if err := bt.Insert(key, rid2); err != nil {
		t.Errorf("Second insert should succeed for non-unique index, got %v", err)
	}
}

func TestBTreeEmptyKey(t *testing.T) {
	dir := tempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	bt, pager := createTestBTree(t, dir, "test.idx", true)
	defer pager.Close()

	// Insert with empty key should fail
	err := bt.Insert([]byte{}, testRID(1, 1))
	if err != ErrEmptyKey {
		t.Errorf("Expected ErrEmptyKey for insert, got %v", err)
	}

	// Search with empty key should fail
	_, err = bt.Search([]byte{})
	if err != ErrEmptyKey {
		t.Errorf("Expected ErrEmptyKey for search, got %v", err)
	}
}

// === Range Search Tests ===

func TestBTreeRangeSearch(t *testing.T) {
	dir := tempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	bt, pager := createTestBTree(t, dir, "test.idx", true)
	defer pager.Close()

	// Insert keys 0-99
	for i := 0; i < 100; i++ {
		key := intKey(i)
		rid := testRID(uint32(i), 0)
		bt.Insert(key, rid)
	}

	// Range query [25, 50]
	results, err := bt.SearchRange(intKey(25), intKey(50))
	if err != nil {
		t.Fatalf("Range search failed: %v", err)
	}

	// Should have 26 results (inclusive)
	if len(results) != 26 {
		t.Errorf("Expected 26 results, got %d", len(results))
	}

	// Verify all results are in range
	for i, rid := range results {
		expected := uint32(25 + i)
		if rid.Page != expected {
			t.Errorf("Result %d: expected Page %d, got %d", i, expected, rid.Page)
		}
	}
}

func TestBTreeRangeSearchOpenEnded(t *testing.T) {
	dir := tempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	bt, pager := createTestBTree(t, dir, "test.idx", true)
	defer pager.Close()

	// Insert keys 0-49
	for i := 0; i < 50; i++ {
		key := intKey(i)
		rid := testRID(uint32(i), 0)
		bt.Insert(key, rid)
	}

	// All keys from beginning to 10
	results1, err := bt.SearchRange(nil, intKey(10))
	if err != nil {
		t.Fatalf("Range search (nil, 10) failed: %v", err)
	}
	if len(results1) != 11 { // 0-10 inclusive
		t.Errorf("Expected 11 results for (nil, 10), got %d", len(results1))
	}

	// All keys from 40 to end
	results2, err := bt.SearchRange(intKey(40), nil)
	if err != nil {
		t.Fatalf("Range search (40, nil) failed: %v", err)
	}
	if len(results2) != 10 { // 40-49 inclusive
		t.Errorf("Expected 10 results for (40, nil), got %d", len(results2))
	}

	// All keys (both nil)
	results3, err := bt.SearchRange(nil, nil)
	if err != nil {
		t.Fatalf("Range search (nil, nil) failed: %v", err)
	}
	if len(results3) != 50 {
		t.Errorf("Expected 50 results for (nil, nil), got %d", len(results3))
	}
}

// === Delete Tests ===

func TestBTreeDelete(t *testing.T) {
	dir := tempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	bt, pager := createTestBTree(t, dir, "test.idx", true)
	defer pager.Close()

	// Insert 20 keys
	for i := 0; i < 20; i++ {
		key := intKey(i)
		rid := testRID(uint32(i), 0)
		bt.Insert(key, rid)
	}

	// Delete key 10
	if err := bt.Delete(intKey(10)); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Key 10 should not be found
	_, err := bt.Search(intKey(10))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for deleted key, got %v", err)
	}

	// Other keys should still exist
	for i := 0; i < 20; i++ {
		if i == 10 {
			continue
		}
		_, err := bt.Search(intKey(i))
		if err != nil {
			t.Errorf("Key %d should still exist, got %v", i, err)
		}
	}
}

func TestBTreeDeleteNonExistent(t *testing.T) {
	dir := tempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	bt, pager := createTestBTree(t, dir, "test.idx", true)
	defer pager.Close()

	// Insert some keys
	for i := 0; i < 10; i++ {
		bt.Insert(intKey(i), testRID(uint32(i), 0))
	}

	// Delete non-existent key
	err := bt.Delete(intKey(100))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for non-existent key, got %v", err)
	}
}

func TestBTreeDeleteAll(t *testing.T) {
	dir := tempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	bt, pager := createTestBTree(t, dir, "test.idx", true)
	defer pager.Close()

	// Insert keys
	count := 50
	for i := 0; i < count; i++ {
		bt.Insert(intKey(i), testRID(uint32(i), 0))
	}

	// Delete all keys in random order
	perm := rand.Perm(count)
	for _, i := range perm {
		if err := bt.Delete(intKey(i)); err != nil {
			t.Fatalf("Delete of key %d failed: %v", i, err)
		}
	}

	// Verify all deleted
	for i := 0; i < count; i++ {
		_, err := bt.Search(intKey(i))
		if err != ErrKeyNotFound {
			t.Errorf("Key %d should be deleted, got %v", i, err)
		}
	}
}

// === Random Operations Tests ===

func TestBTreeRandomOperations(t *testing.T) {
	dir := tempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	bt, pager := createTestBTree(t, dir, "test.idx", true)
	defer pager.Close()

	// Keep track of what we've inserted
	inserted := make(map[int]storage.RID)
	rng := rand.New(rand.NewSource(42))

	for i := 0; i < 500; i++ {
		key := rng.Intn(1000)
		if _, exists := inserted[key]; !exists {
			// Insert new key
			rid := testRID(uint32(key), uint16(i))
			err := bt.Insert(intKey(key), rid)
			if err != nil {
				t.Fatalf("Insert of key %d failed: %v", key, err)
			}
			inserted[key] = rid
		}
	}

	// Verify all inserted keys can be found
	for key, expectedRID := range inserted {
		foundRID, err := bt.Search(intKey(key))
		if err != nil {
			t.Fatalf("Search for key %d failed: %v", key, err)
		}
		if foundRID != expectedRID {
			t.Errorf("Key %d: expected %v, got %v", key, expectedRID, foundRID)
		}
	}
}

// === Large Scale Tests ===

func TestBTreeLargeScale(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large scale test in short mode")
	}

	dir := tempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	bt, pager := createTestBTree(t, dir, "test.idx", true)
	defer pager.Close()

	count := 10000
	for i := 0; i < count; i++ {
		key := intKey(i)
		rid := testRID(uint32(i/100), uint16(i%100))
		if err := bt.Insert(key, rid); err != nil {
			t.Fatalf("Insert failed for key %d: %v", i, err)
		}
	}

	// Spot check some values
	for i := 0; i < count; i += 100 {
		key := intKey(i)
		expectedRID := testRID(uint32(i/100), uint16(i%100))
		foundRID, err := bt.Search(key)
		if err != nil {
			t.Fatalf("Search failed for key %d: %v", i, err)
		}
		if foundRID != expectedRID {
			t.Errorf("Key %d: expected %v, got %v", i, expectedRID, foundRID)
		}
	}
}

// === Persistence Tests ===

func TestBTreePersistence(t *testing.T) {
	dir := tempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	indexPath := filepath.Join(dir, "persist.idx")

	// Create and populate
	pager1, err := storage.OpenPager(dir, "persist.idx", testPageSize)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}

	bt1, err := New(Config{
		Pager:      pager1,
		PageSize:   testPageSize,
		Unique:     true,
		RootPageID: InvalidPageID, // New tree
	})
	if err != nil {
		pager1.Close()
		t.Fatalf("Failed to create B+ tree: %v", err)
	}

	count := 100
	for i := 0; i < count; i++ {
		key := intKey(i)
		rid := testRID(uint32(i), uint16(i))
		bt1.Insert(key, rid)
	}

	rootPage := bt1.RootPage()
	pager1.Close()

	// Reopen and verify
	pager2, err := storage.OpenPager(dir, "persist.idx", testPageSize)
	if err != nil {
		t.Fatalf("Failed to reopen pager: %v", err)
	}
	defer pager2.Close()

	bt2, err := New(Config{
		Pager:      pager2,
		PageSize:   testPageSize,
		Unique:     true,
		RootPageID: rootPage,
	})
	if err != nil {
		t.Fatalf("Failed to reopen B+ tree: %v", err)
	}

	// Verify all keys
	for i := 0; i < count; i++ {
		key := intKey(i)
		expectedRID := testRID(uint32(i), uint16(i))
		foundRID, err := bt2.Search(key)
		if err != nil {
			t.Fatalf("After reopen, search for key %d failed: %v", i, err)
		}
		if foundRID != expectedRID {
			t.Errorf("After reopen, key %d: expected %v, got %v", i, expectedRID, foundRID)
		}
	}

	_ = indexPath // suppress unused warning
}

// === Node Split Tests ===

func TestBTreeLeafSplit(t *testing.T) {
	dir := tempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	bt, pager := createTestBTree(t, dir, "test.idx", true)
	defer pager.Close()

	// Insert enough keys to trigger splits
	for i := 0; i < 200; i++ {
		key := intKey(i)
		rid := testRID(uint32(i), 0)
		if err := bt.Insert(key, rid); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Verify all keys still accessible
	for i := 0; i < 200; i++ {
		key := intKey(i)
		rid, err := bt.Search(key)
		if err != nil {
			t.Errorf("After splits, key %d not found: %v", i, err)
		}
		if rid.Page != uint32(i) {
			t.Errorf("Key %d: wrong RID Page, expected %d got %d", i, i, rid.Page)
		}
	}
}

func TestBTreeReverseInsert(t *testing.T) {
	dir := tempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	bt, pager := createTestBTree(t, dir, "test.idx", true)
	defer pager.Close()

	// Insert in reverse order
	count := 100
	for i := count - 1; i >= 0; i-- {
		key := intKey(i)
		rid := testRID(uint32(i), 0)
		if err := bt.Insert(key, rid); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Verify forward scan works
	results, err := bt.SearchRange(nil, nil)
	if err != nil {
		t.Fatalf("Range scan failed: %v", err)
	}

	if len(results) != count {
		t.Errorf("Expected %d results, got %d", count, len(results))
	}

	// Results should be in order
	for i, rid := range results {
		if rid.Page != uint32(i) {
			t.Errorf("Result %d out of order: expected Page %d, got %d", i, i, rid.Page)
		}
	}
}

// === Concurrent Read Tests ===

func TestBTreeConcurrentReads(t *testing.T) {
	dir := tempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	bt, pager := createTestBTree(t, dir, "test.idx", true)
	defer pager.Close()

	// Populate
	count := 1000
	for i := 0; i < count; i++ {
		bt.Insert(intKey(i), testRID(uint32(i), 0))
	}

	// Concurrent reads
	done := make(chan error, 10)
	for g := 0; g < 10; g++ {
		go func(goroutine int) {
			for i := 0; i < 100; i++ {
				key := (goroutine*100 + i) % count
				_, err := bt.Search(intKey(key))
				if err != nil {
					done <- fmt.Errorf("goroutine %d: search for key %d failed: %v", goroutine, key, err)
					return
				}
			}
			done <- nil
		}(g)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		if err := <-done; err != nil {
			t.Error(err)
		}
	}
}

// === Edge Case Tests ===

func TestBTreeLargeKey(t *testing.T) {
	dir := tempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	bt, pager := createTestBTree(t, dir, "test.idx", true)
	defer pager.Close()

	// Create a reasonably large key (but not too large)
	largeKey := make([]byte, 100)
	for i := range largeKey {
		largeKey[i] = byte(i)
	}

	rid := testRID(42, 1)
	if err := bt.Insert(largeKey, rid); err != nil {
		t.Fatalf("Insert large key failed: %v", err)
	}

	foundRID, err := bt.Search(largeKey)
	if err != nil {
		t.Fatalf("Search large key failed: %v", err)
	}
	if foundRID != rid {
		t.Errorf("Expected RID %v, got %v", rid, foundRID)
	}
}

func TestBTreeSingleByteKey(t *testing.T) {
	dir := tempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	bt, pager := createTestBTree(t, dir, "test.idx", true)
	defer pager.Close()

	// Single byte key
	key := []byte{0x42}
	rid := testRID(1, 1)

	if err := bt.Insert(key, rid); err != nil {
		t.Fatalf("Insert single byte key failed: %v", err)
	}

	foundRID, err := bt.Search(key)
	if err != nil {
		t.Fatalf("Search single byte key failed: %v", err)
	}
	if foundRID != rid {
		t.Errorf("Expected RID %v, got %v", rid, foundRID)
	}
}

func TestBTreeVariableLengthKeys(t *testing.T) {
	dir := tempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	bt, pager := createTestBTree(t, dir, "test.idx", true)
	defer pager.Close()

	// Insert keys of various lengths
	keys := [][]byte{
		{0x01},
		{0x02, 0x03},
		{0x04, 0x05, 0x06},
		{0x07, 0x08, 0x09, 0x0a},
		{0x0b, 0x0c, 0x0d, 0x0e, 0x0f},
	}

	for i, key := range keys {
		rid := testRID(uint32(i), 0)
		if err := bt.Insert(key, rid); err != nil {
			t.Fatalf("Insert key %d failed: %v", i, err)
		}
	}

	// Verify all can be found
	for i, key := range keys {
		expectedRID := testRID(uint32(i), 0)
		foundRID, err := bt.Search(key)
		if err != nil {
			t.Fatalf("Search key %d failed: %v", i, err)
		}
		if foundRID != expectedRID {
			t.Errorf("Key %d: expected %v, got %v", i, expectedRID, foundRID)
		}
	}
}
