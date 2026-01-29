package storage

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

// BufferPool manages in-memory page cache with LRU eviction policy.
// This is the single most impactful performance improvement for the database.
type BufferPool struct {
	maxPages   int                    // Maximum number of pages to cache
	pageSize   int                    // Size of each page in bytes
	pool       map[BufferKey]*Frame   // Map of page identifiers to frames
	lru        *list.List             // LRU list of frame references
	dirtyPages map[BufferKey]struct{} // Set of dirty page keys
	mu         sync.RWMutex           // Protects all buffer pool operations
	hits       uint64                 // Cache hit counter
	misses     uint64                 // Cache miss counter
	evictions  uint64                 // Number of evictions performed
	flushes    uint64                 // Number of dirty page flushes
}

// BufferKey uniquely identifies a page in the buffer pool.
type BufferKey struct {
	Table  string // Table name
	PageID uint32 // Page number within table
}

// Frame represents a single page in the buffer pool.
type Frame struct {
	Key        BufferKey     // Page identifier
	Data       []byte        // Page data
	PinCount   int           // Number of active references
	Dirty      bool          // Whether page has been modified
	LastAccess time.Time     // Last access time for statistics
	lruElement *list.Element // Pointer to position in LRU list
}

// NewBufferPool creates a new buffer pool with specified capacity.
func NewBufferPool(maxPages int, pageSize int) *BufferPool {
	if maxPages <= 0 {
		maxPages = 1000 // Default to 1000 pages (~4MB for 4KB pages)
	}
	return &BufferPool{
		maxPages:   maxPages,
		pageSize:   pageSize,
		pool:       make(map[BufferKey]*Frame),
		lru:        list.New(),
		dirtyPages: make(map[BufferKey]struct{}),
	}
}

// FetchPage retrieves a page from buffer pool or loads from disk.
// The page is pinned and must be released with UnpinPage when done.
func (bp *BufferPool) FetchPage(key BufferKey, loader func() ([]byte, error)) ([]byte, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	// Check if page is already in buffer pool
	if frame, exists := bp.pool[key]; exists {
		bp.hits++
		frame.PinCount++
		frame.LastAccess = time.Now()
		// Move to front of LRU list (most recently used)
		bp.lru.MoveToFront(frame.lruElement)
		return frame.Data, nil
	}

	bp.misses++

	// Page not in pool, need to load from disk
	// First, ensure we have space (evict if necessary)
	if len(bp.pool) >= bp.maxPages {
		if err := bp.evictLRUPage(); err != nil {
			return nil, fmt.Errorf("eviction failed: %w", err)
		}
	}

	// Load page from disk using provided loader function
	data, err := loader()
	if err != nil {
		return nil, fmt.Errorf("load page: %w", err)
	}

	// Ensure data is correct size
	if len(data) != bp.pageSize {
		// Resize if needed
		if len(data) < bp.pageSize {
			resized := make([]byte, bp.pageSize)
			copy(resized, data)
			data = resized
		} else {
			data = data[:bp.pageSize]
		}
	}

	// Create new frame
	frame := &Frame{
		Key:        key,
		Data:       data,
		PinCount:   1, // Pinned for caller
		Dirty:      false,
		LastAccess: time.Now(),
	}

	// Add to LRU list (front = most recently used)
	frame.lruElement = bp.lru.PushFront(frame)
	bp.pool[key] = frame

	return frame.Data, nil
}

// UnpinPage releases a pin on a page and optionally marks it dirty.
func (bp *BufferPool) UnpinPage(key BufferKey, dirty bool) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	frame, exists := bp.pool[key]
	if !exists {
		return fmt.Errorf("page not in buffer pool: %v", key)
	}

	if frame.PinCount <= 0 {
		return fmt.Errorf("page not pinned: %v", key)
	}

	frame.PinCount--

	if dirty {
		frame.Dirty = true
		bp.dirtyPages[key] = struct{}{}
	}

	return nil
}

// MarkDirty marks a page as dirty without unpinning.
func (bp *BufferPool) MarkDirty(key BufferKey) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	frame, exists := bp.pool[key]
	if !exists {
		return fmt.Errorf("page not in buffer pool: %v", key)
	}

	frame.Dirty = true
	bp.dirtyPages[key] = struct{}{}
	return nil
}

// FlushPage writes a dirty page back to disk using provided writer.
func (bp *BufferPool) FlushPage(key BufferKey, writer func([]byte) error) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	frame, exists := bp.pool[key]
	if !exists {
		return fmt.Errorf("page not in buffer pool: %v", key)
	}

	if !frame.Dirty {
		return nil // Nothing to flush
	}

	// Write page to disk
	if err := writer(frame.Data); err != nil {
		return fmt.Errorf("write page: %w", err)
	}

	// Mark as clean
	frame.Dirty = false
	delete(bp.dirtyPages, key)
	bp.flushes++

	return nil
}

// FlushAll writes all dirty pages to disk using provided writer.
func (bp *BufferPool) FlushAll(writer func(BufferKey, []byte) error) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	var errors []error
	for key := range bp.dirtyPages {
		frame := bp.pool[key]
		if err := writer(key, frame.Data); err != nil {
			errors = append(errors, fmt.Errorf("flush %v: %w", key, err))
		} else {
			frame.Dirty = false
			delete(bp.dirtyPages, key)
			bp.flushes++
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("flush errors: %v", errors)
	}

	return nil
}

// evictLRUPage evicts the least recently used unpinned page.
// Caller must hold bp.mu.
func (bp *BufferPool) evictLRUPage() error {
	// Scan from back of LRU list (least recently used)
	for elem := bp.lru.Back(); elem != nil; elem = elem.Prev() {
		frame := elem.Value.(*Frame)

		// Can only evict unpinned pages
		if frame.PinCount == 0 {
			// If dirty, must flush first (requires external writer)
			if frame.Dirty {
				// Cannot evict dirty page without flushing
				// This is a limitation - in production, we'd queue for background flush
				// For now, skip dirty pages during eviction
				continue
			}

			// Evict this page
			bp.lru.Remove(elem)
			delete(bp.pool, frame.Key)
			delete(bp.dirtyPages, frame.Key)
			bp.evictions++
			return nil
		}
	}

	return fmt.Errorf("no evictable pages (all pinned or dirty)")
}

// EvictPage removes a specific page from buffer pool.
// Returns error if page is pinned or dirty.
func (bp *BufferPool) EvictPage(key BufferKey) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	frame, exists := bp.pool[key]
	if !exists {
		return nil // Already evicted
	}

	if frame.PinCount > 0 {
		return fmt.Errorf("cannot evict pinned page: %v", key)
	}

	if frame.Dirty {
		return fmt.Errorf("cannot evict dirty page without flushing: %v", key)
	}

	bp.lru.Remove(frame.lruElement)
	delete(bp.pool, key)
	bp.evictions++

	return nil
}

// Clear removes all pages from buffer pool (flushes dirty pages first).
func (bp *BufferPool) Clear(writer func(BufferKey, []byte) error) error {
	if err := bp.FlushAll(writer); err != nil {
		return err
	}

	bp.mu.Lock()
	defer bp.mu.Unlock()

	bp.pool = make(map[BufferKey]*Frame)
	bp.lru.Init()
	bp.dirtyPages = make(map[BufferKey]struct{})

	return nil
}

// Stats returns buffer pool statistics.
func (bp *BufferPool) Stats() BufferPoolStats {
	bp.mu.RLock()
	defer bp.mu.RUnlock()

	var hitRate float64
	total := bp.hits + bp.misses
	if total > 0 {
		hitRate = float64(bp.hits) / float64(total) * 100
	}

	return BufferPoolStats{
		MaxPages:    bp.maxPages,
		PagesInPool: len(bp.pool),
		DirtyPages:  len(bp.dirtyPages),
		Hits:        bp.hits,
		Misses:      bp.misses,
		HitRate:     hitRate,
		Evictions:   bp.evictions,
		Flushes:     bp.flushes,
	}
}

// BufferPoolStats contains buffer pool statistics.
type BufferPoolStats struct {
	MaxPages    int     // Maximum capacity
	PagesInPool int     // Current pages in pool
	DirtyPages  int     // Number of dirty pages
	Hits        uint64  // Cache hits
	Misses      uint64  // Cache misses
	HitRate     float64 // Hit rate percentage
	Evictions   uint64  // Number of evictions
	Flushes     uint64  // Number of flushes
}
