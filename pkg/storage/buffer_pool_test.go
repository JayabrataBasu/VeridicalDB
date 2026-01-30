package storage

import (
	"testing"
	"time"
)

func TestBufferPoolBasics(t *testing.T) {
	bp := NewBufferPool(10, 4096)

	// Test initial state
	stats := bp.Stats()
	if stats.PagesInPool != 0 {
		t.Errorf("expected 0 pages, got %d", stats.PagesInPool)
	}

	// Test fetch page
	key := BufferKey{Table: "test", PageID: 0}
	loader := func() ([]byte, error) {
		data := make([]byte, 4096)
		copy(data, []byte("test data"))
		return data, nil
	}

	data, err := bp.FetchPage(key, loader)
	if err != nil {
		t.Fatalf("FetchPage failed: %v", err)
	}

	if string(data[:9]) != "test data" {
		t.Errorf("unexpected data: %s", data[:9])
	}

	// Verify stats
	stats = bp.Stats()
	if stats.PagesInPool != 1 {
		t.Errorf("expected 1 page, got %d", stats.PagesInPool)
	}
	if stats.Hits != 0 {
		t.Errorf("expected 0 hits, got %d", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Errorf("expected 1 miss, got %d", stats.Misses)
	}
}

func TestBufferPoolCacheHit(t *testing.T) {
	bp := NewBufferPool(10, 4096)
	key := BufferKey{Table: "test", PageID: 0}

	loadCount := 0
	loader := func() ([]byte, error) {
		loadCount++
		data := make([]byte, 4096)
		return data, nil
	}

	// First fetch - miss
	_, err := bp.FetchPage(key, loader)
	if err != nil {
		t.Fatalf("FetchPage failed: %v", err)
	}

	// Second fetch - should hit cache
	_, err = bp.FetchPage(key, loader)
	if err != nil {
		t.Fatalf("FetchPage failed: %v", err)
	}

	if loadCount != 1 {
		t.Errorf("expected loader called once, got %d", loadCount)
	}

	stats := bp.Stats()
	if stats.Hits != 1 {
		t.Errorf("expected 1 hit, got %d", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Errorf("expected 1 miss, got %d", stats.Misses)
	}
	if stats.HitRate < 49 || stats.HitRate > 51 {
		t.Errorf("expected 50%% hit rate, got %.2f%%", stats.HitRate)
	}
}

func TestBufferPoolUnpin(t *testing.T) {
	bp := NewBufferPool(10, 4096)
	key := BufferKey{Table: "test", PageID: 0}

	loader := func() ([]byte, error) {
		return make([]byte, 4096), nil
	}

	data, err := bp.FetchPage(key, loader)
	if err != nil {
		t.Fatalf("FetchPage failed: %v", err)
	}

	// Modify data and mark dirty
	copy(data, []byte("modified"))

	err = bp.UnpinPage(key, true)
	if err != nil {
		t.Fatalf("UnpinPage failed: %v", err)
	}

	stats := bp.Stats()
	if stats.DirtyPages != 1 {
		t.Errorf("expected 1 dirty page, got %d", stats.DirtyPages)
	}
}

func TestBufferPoolEviction(t *testing.T) {
	bp := NewBufferPool(3, 4096) // Small pool for eviction testing

	loader := func(_ uint32) func() ([]byte, error) {
		return func() ([]byte, error) {
			return make([]byte, 4096), nil
		}
	}

	// Fill pool
	keys := make([]BufferKey, 3)
	for i := 0; i < 3; i++ {
		keys[i] = BufferKey{Table: "test", PageID: uint32(i)}
		_, err := bp.FetchPage(keys[i], loader(uint32(i)))
		if err != nil {
			t.Fatalf("FetchPage %d failed: %v", i, err)
		}
		// Unpin immediately
		_ = bp.UnpinPage(keys[i], false)
	}

	stats := bp.Stats()
	if stats.PagesInPool != 3 {
		t.Errorf("expected 3 pages, got %d", stats.PagesInPool)
	}

	// Add one more - should evict LRU
	key4 := BufferKey{Table: "test", PageID: 3}
	_, err := bp.FetchPage(key4, loader(3))
	if err != nil {
		t.Fatalf("FetchPage 3 failed: %v", err)
	}

	stats = bp.Stats()
	if stats.PagesInPool != 3 {
		t.Errorf("expected 3 pages after eviction, got %d", stats.PagesInPool)
	}
	if stats.Evictions != 1 {
		t.Errorf("expected 1 eviction, got %d", stats.Evictions)
	}
}

func TestBufferPoolFlush(t *testing.T) {
	bp := NewBufferPool(10, 4096)
	key := BufferKey{Table: "test", PageID: 0}

	loader := func() ([]byte, error) {
		return make([]byte, 4096), nil
	}

	data, err := bp.FetchPage(key, loader)
	if err != nil {
		t.Fatalf("FetchPage failed: %v", err)
	}

	copy(data, []byte("dirty data"))
	_ = bp.UnpinPage(key, true)

	// Flush page
	flushed := false
	writer := func(pageData []byte) error {
		if string(pageData[:10]) != "dirty data" {
			t.Errorf("unexpected data in flush: %s", pageData[:10])
		}
		flushed = true
		return nil
	}

	err = bp.FlushPage(key, writer)
	if err != nil {
		t.Fatalf("FlushPage failed: %v", err)
	}

	if !flushed {
		t.Error("writer not called")
	}

	stats := bp.Stats()
	if stats.DirtyPages != 0 {
		t.Errorf("expected 0 dirty pages after flush, got %d", stats.DirtyPages)
	}
	if stats.Flushes != 1 {
		t.Errorf("expected 1 flush, got %d", stats.Flushes)
	}
}

func TestBufferPoolLRUOrdering(t *testing.T) {
	bp := NewBufferPool(3, 4096)

	loader := func(id uint32) func() ([]byte, error) {
		return func() ([]byte, error) {
			_ = id // unused, but kept for loader function signature
			return make([]byte, 4096), nil
		}
	}

	// Add 3 pages
	for i := 0; i < 3; i++ {
		key := BufferKey{Table: "test", PageID: uint32(i)}
		_, _ = bp.FetchPage(key, loader(uint32(i)))
		_ = bp.UnpinPage(key, false)
		time.Sleep(time.Millisecond) // Ensure different access times
	}

	// Access page 0 again (should move to front of LRU)
	key0 := BufferKey{Table: "test", PageID: 0}
	_, _ = bp.FetchPage(key0, loader(0))
	_ = bp.UnpinPage(key0, false)

	// Add new page - should evict page 1 (oldest unused)
	key3 := BufferKey{Table: "test", PageID: 3}
	_, _ = bp.FetchPage(key3, loader(3))

	stats := bp.Stats()
	if stats.Evictions != 1 {
		t.Errorf("expected 1 eviction, got %d", stats.Evictions)
	}
}

func TestBufferPoolPinning(t *testing.T) {
	bp := NewBufferPool(2, 4096)

	loader := func() ([]byte, error) {
		return make([]byte, 4096), nil
	}

	// Add and pin 2 pages
	key1 := BufferKey{Table: "test", PageID: 0}
	key2 := BufferKey{Table: "test", PageID: 1}

	_, _ = bp.FetchPage(key1, loader)
	_, _ = bp.FetchPage(key2, loader)
	// Don't unpin - both are pinned

	// Try to add third page - should fail (all pinned)
	key3 := BufferKey{Table: "test", PageID: 2}
	_, err := bp.FetchPage(key3, loader)

	if err == nil {
		t.Error("expected error when pool full of pinned pages")
	}

	// Unpin one page
	_ = bp.UnpinPage(key1, false)

	// Now should succeed
	_, err = bp.FetchPage(key3, loader)
	if err != nil {
		t.Errorf("FetchPage failed after unpin: %v", err)
	}
}
