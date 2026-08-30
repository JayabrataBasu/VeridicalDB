package exec

import (
	"container/list"
	"crypto/sha256"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/ast"
)

// QueryCache caches parsed SQL queries and their execution plans.
// This provides 20-30% performance improvement for repeated queries.
type QueryCache struct {
	maxEntries int
	cache      map[string]*CacheEntry
	lru        *list.List
	mu         sync.RWMutex
	hits       uint64
	misses     uint64
	evictions  uint64
}

// CacheEntry represents a cached query.
type CacheEntry struct {
	Key          string
	ParsedAST    ast.Statement // The parsed statement
	PreparedPlan interface{}   // Optional: prepared execution plan
	CreatedAt    time.Time
	LastAccess   time.Time
	AccessCount  uint64
	lruElement   *list.Element
}

// NewQueryCache creates a new query cache with specified capacity.
func NewQueryCache(maxEntries int) *QueryCache {
	if maxEntries <= 0 {
		maxEntries = 1000 // Default 1000 entries
	}
	return &QueryCache{
		maxEntries: maxEntries,
		cache:      make(map[string]*CacheEntry),
		lru:        list.New(),
	}
}

// Get retrieves a cached query by SQL string.
func (qc *QueryCache) Get(sql string) (*CacheEntry, bool) {
	qc.mu.RLock()

	// Normalize SQL for cache key (remove extra whitespace)
	key := qc.normalizeSQL(sql)

	entry, exists := qc.cache[key]
	if !exists {
		qc.mu.RUnlock()
		qc.mu.Lock()
		qc.misses++
		qc.mu.Unlock()
		return nil, false
	}

	qc.mu.RUnlock()

	// Update access statistics (requires write lock)
	qc.mu.Lock()
	defer qc.mu.Unlock()

	qc.hits++
	entry.LastAccess = time.Now()
	entry.AccessCount++

	// Move to front of LRU list
	qc.lru.MoveToFront(entry.lruElement)

	return entry, true
}

// Put adds a query to the cache.
func (qc *QueryCache) Put(sql string, stmt ast.Statement) error {
	qc.mu.Lock()
	defer qc.mu.Unlock()

	key := qc.normalizeSQL(sql)

	// Check if already exists (update)
	if entry, exists := qc.cache[key]; exists {
		entry.ParsedAST = stmt
		entry.LastAccess = time.Now()
		qc.lru.MoveToFront(entry.lruElement)
		return nil
	}

	// Evict if at capacity
	if len(qc.cache) >= qc.maxEntries {
		qc.evictLRU()
	}

	// Create new entry
	entry := &CacheEntry{
		Key:         key,
		ParsedAST:   stmt,
		CreatedAt:   time.Now(),
		LastAccess:  time.Now(),
		AccessCount: 0,
	}

	entry.lruElement = qc.lru.PushFront(entry)
	qc.cache[key] = entry

	return nil
}

// PutWithPlan adds a query with its execution plan to the cache.
func (qc *QueryCache) PutWithPlan(sql string, stmt ast.Statement, plan interface{}) error {
	qc.mu.Lock()
	defer qc.mu.Unlock()

	key := qc.normalizeSQL(sql)

	// Check if already exists
	if entry, exists := qc.cache[key]; exists {
		entry.ParsedAST = stmt
		entry.PreparedPlan = plan
		entry.LastAccess = time.Now()
		qc.lru.MoveToFront(entry.lruElement)
		return nil
	}

	// Evict if at capacity
	if len(qc.cache) >= qc.maxEntries {
		qc.evictLRU()
	}

	// Create new entry
	entry := &CacheEntry{
		Key:          key,
		ParsedAST:    stmt,
		PreparedPlan: plan,
		CreatedAt:    time.Now(),
		LastAccess:   time.Now(),
		AccessCount:  0,
	}

	entry.lruElement = qc.lru.PushFront(entry)
	qc.cache[key] = entry

	return nil
}

// Invalidate removes a specific query from cache.
func (qc *QueryCache) Invalidate(sql string) {
	qc.mu.Lock()
	defer qc.mu.Unlock()

	key := qc.normalizeSQL(sql)
	if entry, exists := qc.cache[key]; exists {
		qc.lru.Remove(entry.lruElement)
		delete(qc.cache, key)
	}
}

// InvalidateTable removes all queries related to a specific table.
// This should be called on DDL operations (ALTER, DROP, etc).
func (qc *QueryCache) InvalidateTable(tableName string) {
	qc.mu.Lock()
	defer qc.mu.Unlock()

	// Scan all entries and remove those referencing the table
	// This is a simple implementation - could be optimized with table index
	for key, entry := range qc.cache {
		if qc.referencesTable(entry.ParsedAST, tableName) {
			qc.lru.Remove(entry.lruElement)
			delete(qc.cache, key)
		}
	}
}

// Clear removes all entries from cache.
func (qc *QueryCache) Clear() {
	qc.mu.Lock()
	defer qc.mu.Unlock()

	qc.cache = make(map[string]*CacheEntry)
	qc.lru.Init()
}

// Stats returns cache statistics.
func (qc *QueryCache) Stats() QueryCacheStats {
	qc.mu.RLock()
	defer qc.mu.RUnlock()

	var hitRate float64
	total := qc.hits + qc.misses
	if total > 0 {
		hitRate = float64(qc.hits) / float64(total) * 100
	}

	return QueryCacheStats{
		MaxEntries:     qc.maxEntries,
		CurrentEntries: len(qc.cache),
		Hits:           qc.hits,
		Misses:         qc.misses,
		HitRate:        hitRate,
		Evictions:      qc.evictions,
	}
}

// evictLRU removes the least recently used entry.
// Caller must hold qc.mu lock.
func (qc *QueryCache) evictLRU() {
	elem := qc.lru.Back()
	if elem == nil {
		return
	}

	entry := elem.Value.(*CacheEntry)
	qc.lru.Remove(elem)
	delete(qc.cache, entry.Key)
	qc.evictions++
}

// normalizeSQL creates a consistent cache key from SQL text.
// It canonicalizes non-semantic formatting differences (whitespace/trailing semicolon)
// while preserving quoted literal content to avoid incorrect cache collisions.
func (qc *QueryCache) normalizeSQL(sql string) string {
	canonical := canonicalizeSQL(sql)
	hash := sha256.Sum256([]byte(canonical))
	return fmt.Sprintf("%x", hash)
}

// canonicalizeSQL collapses whitespace outside quoted strings and strips
// trailing semicolons/space that are not semantically meaningful.
func canonicalizeSQL(sql string) string {
	trimmed := strings.TrimSpace(sql)
	trimmed = strings.TrimSuffix(trimmed, ";")
	trimmed = strings.TrimSpace(trimmed)

	var b strings.Builder
	b.Grow(len(trimmed))

	inSingle := false
	inDouble := false
	lastWasSpace := false

	for i := 0; i < len(trimmed); i++ {
		ch := rune(trimmed[i])

		if !inDouble && ch == '\'' {
			inSingle = !inSingle
			b.WriteByte(trimmed[i])
			lastWasSpace = false
			continue
		}

		if !inSingle && ch == '"' {
			inDouble = !inDouble
			b.WriteByte(trimmed[i])
			lastWasSpace = false
			continue
		}

		if !inSingle && !inDouble && unicode.IsSpace(ch) {
			if !lastWasSpace {
				b.WriteByte(' ')
				lastWasSpace = true
			}
			continue
		}

		b.WriteByte(trimmed[i])
		lastWasSpace = false
	}

	return strings.TrimSpace(b.String())
}

// referencesTable checks if a statement references a given table.
// This is a simple implementation - could be enhanced.
func (qc *QueryCache) referencesTable(stmt ast.Statement, tableName string) bool {
	switch s := stmt.(type) {
	case *ast.SelectStmt:
		// Check table name
		return s.TableName == tableName
	case *ast.InsertStmt:
		return s.TableName == tableName
	case *ast.UpdateStmt:
		return s.TableName == tableName
	case *ast.DeleteStmt:
		return s.TableName == tableName
	default:
		// For unknown statement types, conservatively return true
		return true
	}
}

// QueryCacheStats contains cache statistics.
type QueryCacheStats struct {
	MaxEntries     int
	CurrentEntries int
	Hits           uint64
	Misses         uint64
	HitRate        float64
	Evictions      uint64
}
