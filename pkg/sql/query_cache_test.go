package sql

import (
	"testing"
	"time"
)

func TestQueryCacheBasics(t *testing.T) {
	cache := NewQueryCache(10)
	
	// Test initial state
	stats := cache.Stats()
	if stats.CurrentEntries != 0 {
		t.Errorf("expected 0 entries, got %d", stats.CurrentEntries)
	}
	
	// Test Put
	sql := "SELECT * FROM users WHERE id = 1"
	stmt := &SelectStmt{TableName: "users"}
	
	err := cache.Put(sql, stmt)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	
	stats = cache.Stats()
	if stats.CurrentEntries != 1 {
		t.Errorf("expected 1 entry, got %d", stats.CurrentEntries)
	}
}

func TestQueryCacheHit(t *testing.T) {
	cache := NewQueryCache(10)
	sql := "SELECT * FROM users"
	stmt := &SelectStmt{TableName: "users"}
	
	// Put entry
	cache.Put(sql, stmt)
	
	// First get - should hit
	entry, found := cache.Get(sql)
	if !found {
		t.Error("expected cache hit")
	}
	if entry == nil {
		t.Fatal("entry is nil")
	}
	if entry.AccessCount != 1 {
		t.Errorf("expected access count 1, got %d", entry.AccessCount)
	}
	
	// Second get - should also hit
	entry, found = cache.Get(sql)
	if !found {
		t.Error("expected cache hit")
	}
	if entry.AccessCount != 2 {
		t.Errorf("expected access count 2, got %d", entry.AccessCount)
	}
	
	stats := cache.Stats()
	if stats.Hits != 2 {
		t.Errorf("expected 2 hits, got %d", stats.Hits)
	}
	if stats.Misses != 0 {
		t.Errorf("expected 0 misses, got %d", stats.Misses)
	}
}

func TestQueryCacheMiss(t *testing.T) {
	cache := NewQueryCache(10)
	
	// Get non-existent entry
	_, found := cache.Get("SELECT * FROM nonexistent")
	if found {
		t.Error("expected cache miss")
	}
	
	stats := cache.Stats()
	if stats.Misses != 1 {
		t.Errorf("expected 1 miss, got %d", stats.Misses)
	}
}

func TestQueryCacheEviction(t *testing.T) {
	cache := NewQueryCache(3) // Small cache for testing
	
	// Fill cache
	for i := 0; i < 3; i++ {
		sql := string(rune('A' + i)) // "A", "B", "C"
		stmt := &SelectStmt{TableName: "table"}
		cache.Put(sql, stmt)
	}
	
	stats := cache.Stats()
	if stats.CurrentEntries != 3 {
		t.Errorf("expected 3 entries, got %d", stats.CurrentEntries)
	}
	
	// Add one more - should evict LRU
	cache.Put("D", &SelectStmt{TableName: "table"})
	
	stats = cache.Stats()
	if stats.CurrentEntries != 3 {
		t.Errorf("expected 3 entries after eviction, got %d", stats.CurrentEntries)
	}
	if stats.Evictions != 1 {
		t.Errorf("expected 1 eviction, got %d", stats.Evictions)
	}
	
	// First entry should be evicted
	_, found := cache.Get("A")
	if found {
		t.Error("expected first entry to be evicted")
	}
}

func TestQueryCacheLRUOrdering(t *testing.T) {
	cache := NewQueryCache(3)
	
	// Add 3 entries
	cache.Put("A", &SelectStmt{TableName: "table"})
	time.Sleep(time.Millisecond)
	cache.Put("B", &SelectStmt{TableName: "table"})
	time.Sleep(time.Millisecond)
	cache.Put("C", &SelectStmt{TableName: "table"})
	
	// Access A (should move to front)
	cache.Get("A")
	
	// Add D - should evict B (oldest unused)
	cache.Put("D", &SelectStmt{TableName: "table"})
	
	// A should still be present
	_, found := cache.Get("A")
	if !found {
		t.Error("A should still be in cache")
	}
	
	// B should be evicted
	_, found = cache.Get("B")
	if found {
		t.Error("B should be evicted")
	}
}

func TestQueryCacheInvalidate(t *testing.T) {
	cache := NewQueryCache(10)
	sql := "SELECT * FROM users"
	
	cache.Put(sql, &SelectStmt{TableName: "users"})
	
	// Verify cached
	_, found := cache.Get(sql)
	if !found {
		t.Error("expected entry in cache")
	}
	
	// Invalidate
	cache.Invalidate(sql)
	
	// Should be gone
	_, found = cache.Get(sql)
	if found {
		t.Error("expected entry to be invalidated")
	}
}

func TestQueryCacheInvalidateTable(t *testing.T) {
	cache := NewQueryCache(10)
	
	// Add queries for different tables
	cache.Put("SELECT * FROM users", &SelectStmt{TableName: "users"})
	cache.Put("SELECT * FROM orders", &SelectStmt{TableName: "orders"})
	cache.Put("INSERT INTO users VALUES (1)", &InsertStmt{TableName: "users"})
	
	stats := cache.Stats()
	if stats.CurrentEntries != 3 {
		t.Errorf("expected 3 entries, got %d", stats.CurrentEntries)
	}
	
	// Invalidate all users queries
	cache.InvalidateTable("users")
	
	stats = cache.Stats()
	if stats.CurrentEntries != 1 {
		t.Errorf("expected 1 entry after invalidation, got %d", stats.CurrentEntries)
	}
	
	// Orders query should still be there
	_, found := cache.Get("SELECT * FROM orders")
	if !found {
		t.Error("orders query should still be cached")
	}
}

func TestQueryCacheClear(t *testing.T) {
	cache := NewQueryCache(10)
	
	// Add some entries
	cache.Put("A", &SelectStmt{TableName: "table"})
	cache.Put("B", &SelectStmt{TableName: "table"})
	cache.Put("C", &SelectStmt{TableName: "table"})
	
	cache.Clear()
	
	stats := cache.Stats()
	if stats.CurrentEntries != 0 {
		t.Errorf("expected 0 entries after clear, got %d", stats.CurrentEntries)
	}
}

func TestQueryCacheHitRate(t *testing.T) {
	cache := NewQueryCache(10)
	sql := "SELECT * FROM test"
	
	cache.Put(sql, &SelectStmt{TableName: "test"})
	
	// 3 hits
	cache.Get(sql)
	cache.Get(sql)
	cache.Get(sql)
	
	// 1 miss
	cache.Get("SELECT * FROM other")
	
	stats := cache.Stats()
	// 3 hits out of 4 total = 75%
	if stats.HitRate < 74.9 || stats.HitRate > 75.1 {
		t.Errorf("expected 75%% hit rate, got %.2f%%", stats.HitRate)
	}
}

func TestQueryCacheWithPlan(t *testing.T) {
	cache := NewQueryCache(10)
	sql := "SELECT * FROM users"
	stmt := &SelectStmt{TableName: "users"}
	plan := "TableScan(users)" // Mock plan
	
	err := cache.PutWithPlan(sql, stmt, plan)
	if err != nil {
		t.Fatalf("PutWithPlan failed: %v", err)
	}
	
	entry, found := cache.Get(sql)
	if !found {
		t.Fatal("expected entry in cache")
	}
	
	if entry.PreparedPlan == nil {
		t.Error("expected prepared plan to be set")
	}
	
	if entry.PreparedPlan.(string) != plan {
		t.Errorf("expected plan %s, got %s", plan, entry.PreparedPlan)
	}
}
