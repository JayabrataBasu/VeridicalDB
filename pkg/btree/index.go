// Package btree/index.go provides the IndexManager for managing B+ tree indexes.
// It handles index creation, deletion, and maintenance (insert/update/delete).
package btree

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/JayabrataBasu/VeridicalDB/pkg/storage"
)

// Index metadata errors
var (
	ErrIndexNotFound    = errors.New("index: not found")
	ErrIndexExists      = errors.New("index: already exists")
	ErrColumnNotFound   = errors.New("index: column not found in table")
	ErrTableNotFound    = errors.New("index: table not found")
	ErrInvalidIndexName = errors.New("index: invalid index name")
)

// IndexType represents the type of index.
type IndexType int

const (
	IndexTypeBTree IndexType = iota
	// Future: IndexTypeHash, IndexTypeGist, etc.
)

// IndexMeta holds metadata about an index.
type IndexMeta struct {
	Name      string    `json:"name"`       // Index name (unique within database)
	TableName string    `json:"table_name"` // Table being indexed
	Columns   []string  `json:"columns"`    // Column names (for composite index)
	Unique    bool      `json:"unique"`     // Whether index enforces uniqueness
	Type      IndexType `json:"type"`       // Index type (btree, hash, etc.)
	RootPage  uint32    `json:"root_page"`  // Root page in the index file
}

// IndexManager manages all indexes for a database.
type IndexManager struct {
	dataDir  string
	indexes  map[string]*IndexMeta     // name -> metadata
	btrees   map[string]*BTree         // name -> btree instance
	pagers   map[string]*storage.Pager // name -> pager
	mu       sync.RWMutex
	pageSize int
}

// NewIndexManager creates a new index manager.
func NewIndexManager(dataDir string, pageSize int) (*IndexManager, error) {
	if pageSize < 512 {
		pageSize = 4096 // Default
	}

	im := &IndexManager{
		dataDir:  dataDir,
		indexes:  make(map[string]*IndexMeta),
		btrees:   make(map[string]*BTree),
		pagers:   make(map[string]*storage.Pager),
		pageSize: pageSize,
	}

	// Load existing index metadata
	if err := im.loadIndexes(); err != nil {
		return nil, fmt.Errorf("load indexes: %w", err)
	}

	return im, nil
}

// metaFilePath returns the path to the index metadata file.
func (im *IndexManager) metaFilePath() string {
	return filepath.Join(im.dataDir, "indexes.json")
}

// indexFilePath returns the path to a specific index file.
func (im *IndexManager) indexFilePath(name string) string {
	return filepath.Join(im.dataDir, fmt.Sprintf("idx_%s.db", name))
}

// loadIndexes loads index metadata from disk.
func (im *IndexManager) loadIndexes() error {
	metaPath := im.metaFilePath()

	data, err := os.ReadFile(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No indexes yet
		}
		return err
	}

	var indexes []IndexMeta
	if err := json.Unmarshal(data, &indexes); err != nil {
		return fmt.Errorf("parse index metadata: %w", err)
	}

	for i := range indexes {
		idx := &indexes[i]
		im.indexes[idx.Name] = idx
	}

	return nil
}

// saveIndexesLocked saves index metadata (caller must hold lock).
func (im *IndexManager) saveIndexesLocked() error {
	var indexes []IndexMeta
	for _, idx := range im.indexes {
		indexes = append(indexes, *idx)
	}

	data, err := json.MarshalIndent(indexes, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(im.metaFilePath(), data, 0o644)
}

// CreateIndex creates a new index.
func (im *IndexManager) CreateIndex(meta IndexMeta) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	if meta.Name == "" {
		return ErrInvalidIndexName
	}

	if _, exists := im.indexes[meta.Name]; exists {
		return ErrIndexExists
	}

	// Create the pager for this index
	pager, err := storage.OpenPager(im.dataDir, fmt.Sprintf("idx_%s.db", meta.Name), im.pageSize)
	if err != nil {
		return fmt.Errorf("create index pager: %w", err)
	}

	// Create the B+ tree (InvalidPageID signals new tree)
	bt, err := New(Config{
		Pager:      pager,
		PageSize:   im.pageSize,
		Unique:     meta.Unique,
		RootPageID: InvalidPageID,
	})
	if err != nil {
		_ = pager.Close()
		return fmt.Errorf("create btree: %w", err)
	}

	meta.RootPage = bt.RootPage()
	meta.Type = IndexTypeBTree

	im.indexes[meta.Name] = &meta
	im.btrees[meta.Name] = bt
	im.pagers[meta.Name] = pager

	// Persist metadata
	if err := im.saveIndexesLocked(); err != nil {
		return fmt.Errorf("save index metadata: %w", err)
	}

	return nil
}

// DropIndex removes an index.
func (im *IndexManager) DropIndex(name string) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	if _, exists := im.indexes[name]; !exists {
		return ErrIndexNotFound
	}

	// Close btree and pager if open
	if bt, ok := im.btrees[name]; ok {
		// BTree doesn't have Close, but pager does
		_ = bt
		delete(im.btrees, name)
	}
	if pager, ok := im.pagers[name]; ok {
		_ = pager.Close()
		delete(im.pagers, name)
	}

	// Remove from metadata
	delete(im.indexes, name)

	// Remove the index file
	_ = os.Remove(im.indexFilePath(name))

	// Persist metadata
	if err := im.saveIndexesLocked(); err != nil {
		return fmt.Errorf("save index metadata: %w", err)
	}

	return nil
}

// GetIndex returns the metadata for an index.
func (im *IndexManager) GetIndex(name string) (*IndexMeta, error) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	idx, ok := im.indexes[name]
	if !ok {
		return nil, ErrIndexNotFound
	}
	return idx, nil
}

// ListIndexes returns all indexes, optionally filtered by table.
func (im *IndexManager) ListIndexes(tableName string) []*IndexMeta {
	im.mu.RLock()
	defer im.mu.RUnlock()

	var result []*IndexMeta
	for _, idx := range im.indexes {
		if tableName == "" || idx.TableName == tableName {
			result = append(result, idx)
		}
	}
	return result
}

// GetBTree returns the B+ tree for an index, opening it if necessary.
func (im *IndexManager) GetBTree(name string) (*BTree, error) {
	im.mu.Lock()
	defer im.mu.Unlock()

	if bt, ok := im.btrees[name]; ok {
		return bt, nil
	}

	idx, ok := im.indexes[name]
	if !ok {
		return nil, ErrIndexNotFound
	}

	// Open the pager
	pager, err := storage.OpenPager(im.dataDir, fmt.Sprintf("idx_%s.db", name), im.pageSize)
	if err != nil {
		return nil, fmt.Errorf("open index pager: %w", err)
	}

	// Open the B+ tree with existing root
	bt, err := New(Config{
		Pager:      pager,
		PageSize:   im.pageSize,
		Unique:     idx.Unique,
		RootPageID: idx.RootPage,
	})
	if err != nil {
		_ = pager.Close()
		return nil, fmt.Errorf("open btree: %w", err)
	}

	im.btrees[name] = bt
	im.pagers[name] = pager

	return bt, nil
}

// Insert inserts a key-RID pair into an index.
func (im *IndexManager) Insert(indexName string, key []byte, rid storage.RID) error {
	bt, err := im.GetBTree(indexName)
	if err != nil {
		return err
	}
	return bt.Insert(key, rid)
}

// Delete removes a key from an index.
func (im *IndexManager) Delete(indexName string, key []byte) error {
	bt, err := im.GetBTree(indexName)
	if err != nil {
		return err
	}
	return bt.Delete(key)
}

// Search looks up a key in an index.
func (im *IndexManager) Search(indexName string, key []byte) (storage.RID, error) {
	im.mu.RLock()
	meta, ok := im.indexes[indexName]
	im.mu.RUnlock()
	if !ok {
		return storage.RID{}, ErrIndexNotFound
	}

	bt, err := im.GetBTree(indexName)
	if err != nil {
		return storage.RID{}, err
	}
	rid, err := bt.Search(key)
	if err != nil {
		return rid, err
	}
	// Fill in the table name from metadata (not stored in B+ tree)
	rid.Table = meta.TableName
	return rid, nil
}

// SearchAll finds all RIDs matching an exact key (for non-unique indexes).
func (im *IndexManager) SearchAll(indexName string, key []byte) ([]storage.RID, error) {
	im.mu.RLock()
	meta, ok := im.indexes[indexName]
	im.mu.RUnlock()
	if !ok {
		return nil, ErrIndexNotFound
	}

	bt, err := im.GetBTree(indexName)
	if err != nil {
		return nil, err
	}
	rids, err := bt.SearchAll(key)
	if err != nil {
		return nil, err
	}
	// Fill in the table name for all RIDs
	for i := range rids {
		rids[i].Table = meta.TableName
	}
	return rids, nil
}

// SearchRange performs a range scan on an index.
func (im *IndexManager) SearchRange(indexName string, startKey, endKey []byte) ([]storage.RID, error) {
	im.mu.RLock()
	meta, ok := im.indexes[indexName]
	im.mu.RUnlock()
	if !ok {
		return nil, ErrIndexNotFound
	}

	bt, err := im.GetBTree(indexName)
	if err != nil {
		return nil, err
	}
	rids, err := bt.SearchRange(startKey, endKey)
	if err != nil {
		return nil, err
	}
	// Fill in the table name for all RIDs
	for i := range rids {
		rids[i].Table = meta.TableName
	}
	return rids, nil
}

// Close closes all open indexes.
func (im *IndexManager) Close() error {
	im.mu.Lock()
	defer im.mu.Unlock()

	// Update root pages in metadata (may have changed due to splits)
	for name, bt := range im.btrees {
		if idx, ok := im.indexes[name]; ok {
			idx.RootPage = bt.RootPage()
		}
	}

	// Save updated metadata
	_ = im.saveIndexesLocked()

	var lastErr error
	for name, pager := range im.pagers {
		if err := pager.Close(); err != nil {
			lastErr = fmt.Errorf("close pager %s: %w", name, err)
		}
	}

	im.btrees = make(map[string]*BTree)
	im.pagers = make(map[string]*storage.Pager)

	return lastErr
}

// === Key Encoding Helpers ===

// EncodeIntKey encodes an integer as a big-endian key for proper ordering.
func EncodeIntKey(v int64) []byte {
	// XOR with sign bit to make negative numbers sort correctly
	key := make([]byte, 8)
	u := uint64(v) ^ (1 << 63)
	binary.BigEndian.PutUint64(key, u)
	return key
}

// DecodeIntKey decodes a big-endian key back to an integer.
func DecodeIntKey(key []byte) int64 {
	if len(key) < 8 {
		return 0
	}
	u := binary.BigEndian.Uint64(key)
	return int64(u ^ (1 << 63))
}

// EncodeStringKey encodes a string as a key.
func EncodeStringKey(s string) []byte {
	return []byte(s)
}

// EncodeCompositeKey encodes multiple values as a composite key.
// Each value is length-prefixed to allow variable-length components.
func EncodeCompositeKey(values ...[]byte) []byte {
	var result []byte
	for _, v := range values {
		// 2-byte length prefix
		lenBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(lenBytes, uint16(len(v)))
		result = append(result, lenBytes...)
		result = append(result, v...)
	}
	return result
}
