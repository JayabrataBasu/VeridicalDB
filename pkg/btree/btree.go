// Package btree implements a B+ tree index structure for VeridicalDB.
// It provides efficient key-based lookups, range scans, and ordered traversal.
//
// Design:
// - Keys are []byte (any comparable type can be serialized)
// - Values are RIDs (Record IDs pointing to heap tuples)
// - Interior nodes contain keys and child page pointers
// - Leaf nodes contain keys and RIDs, linked for range scans
// - All nodes are stored on fixed-size pages via the pager
package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/JayabrataBasu/VeridicalDB/pkg/storage"
)

// Common errors for B+ tree operations
var (
	ErrKeyNotFound   = errors.New("btree: key not found")
	ErrDuplicateKey  = errors.New("btree: duplicate key not allowed")
	ErrInvalidNode   = errors.New("btree: invalid node format")
	ErrPageFull      = errors.New("btree: page is full")
	ErrTreeCorrupted = errors.New("btree: tree structure corrupted")
	ErrEmptyKey      = errors.New("btree: empty key not allowed")
	ErrKeyTooLarge   = errors.New("btree: key exceeds maximum size")
	ErrNilPager      = errors.New("btree: pager is nil")
	ErrInvalidPageID = errors.New("btree: invalid page ID")
	ErrNodeUnderflow = errors.New("btree: node has too few keys")
)

// NodeType identifies whether a node is a leaf or internal node.
type NodeType uint8

const (
	NodeTypeLeaf     NodeType = 1
	NodeTypeInternal NodeType = 2
)

// Page layout constants
const (
	// Header layout (first 16 bytes of each node page):
	// [0:1]   NodeType (1 byte)
	// [1:2]   Reserved/flags (1 byte)
	// [2:4]   Number of keys (uint16)
	// [4:8]   Right sibling page ID (uint32, for leaf nodes only)
	// [8:12]  Parent page ID (uint32)
	// [12:16] Reserved

	HeaderSize     = 16
	NodeTypeOffset = 0
	FlagsOffset    = 1
	NumKeysOffset  = 2
	RightSibOffset = 4
	ParentOffset   = 8

	// For leaf nodes: entries are (keyLen, key, RID)
	// For internal nodes: entries are (keyLen, key, childPageID)

	// Minimum fill factor: nodes should be at least half full (except root)
	MinFillFactor = 0.5

	// Magic number to identify B+ tree pages
	BTreePageMagic uint16 = 0xB7EE
)

// InvalidPageID represents an invalid or null page reference
const InvalidPageID uint32 = 0xFFFFFFFF

// BTree represents a B+ tree index.
type BTree struct {
	pager    *storage.Pager
	rootPage uint32
	pageSize int
	unique   bool // If true, duplicate keys are not allowed
	mu       sync.RWMutex

	// Computed values based on page size
	maxKeysPerLeaf     int
	maxKeysPerInternal int
	minKeysPerLeaf     int
	minKeysPerInternal int
}

// Config holds configuration for creating a new B+ tree.
type Config struct {
	Pager      *storage.Pager
	PageSize   int
	Unique     bool   // If true, reject duplicate keys
	RootPageID uint32 // InvalidPageID for new tree, or existing root page
}

// New creates a new B+ tree or opens an existing one.
func New(cfg Config) (*BTree, error) {
	if cfg.Pager == nil {
		return nil, ErrNilPager
	}
	if cfg.PageSize < 512 {
		return nil, fmt.Errorf("btree: page size %d too small (min 512)", cfg.PageSize)
	}

	bt := &BTree{
		pager:    cfg.Pager,
		pageSize: cfg.PageSize,
		unique:   cfg.Unique,
		rootPage: cfg.RootPageID,
	}

	// Calculate max keys per node based on page size
	// Conservative estimate: assume average key size of 32 bytes
	// Leaf entry: 2 (keyLen) + key + 12 (RID: table+page+slot)
	// Internal entry: 2 (keyLen) + key + 4 (childPageID)
	avgKeySize := 32
	leafEntrySize := 2 + avgKeySize + 12
	internalEntrySize := 2 + avgKeySize + 4

	usableSpace := cfg.PageSize - HeaderSize
	bt.maxKeysPerLeaf = usableSpace / leafEntrySize
	bt.maxKeysPerInternal = usableSpace / internalEntrySize

	// Ensure minimum reasonable capacity
	if bt.maxKeysPerLeaf < 4 {
		bt.maxKeysPerLeaf = 4
	}
	if bt.maxKeysPerInternal < 4 {
		bt.maxKeysPerInternal = 4
	}

	bt.minKeysPerLeaf = bt.maxKeysPerLeaf / 2
	bt.minKeysPerInternal = bt.maxKeysPerInternal / 2

	// If RootPageID is InvalidPageID, create new empty tree
	if cfg.RootPageID == InvalidPageID {
		if err := bt.initEmptyTree(); err != nil {
			return nil, fmt.Errorf("btree: failed to init empty tree: %w", err)
		}
	}

	return bt, nil
}

// initEmptyTree creates an initial empty leaf node as the root.
func (bt *BTree) initEmptyTree() error {
	// Allocate a new page for root
	rootPage := make([]byte, bt.pageSize)

	// Initialize as empty leaf node
	rootPage[NodeTypeOffset] = byte(NodeTypeLeaf)
	binary.LittleEndian.PutUint16(rootPage[NumKeysOffset:], 0)
	binary.LittleEndian.PutUint32(rootPage[RightSibOffset:], InvalidPageID)
	binary.LittleEndian.PutUint32(rootPage[ParentOffset:], InvalidPageID)

	// Write to page 0
	if err := bt.pager.WritePage(0, rootPage); err != nil {
		return err
	}

	bt.rootPage = 0
	return nil
}

// RootPage returns the current root page ID.
func (bt *BTree) RootPage() uint32 {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return bt.rootPage
}

// node represents a B+ tree node in memory.
type node struct {
	pageID   uint32
	nodeType NodeType
	numKeys  uint16
	rightSib uint32 // For leaf nodes: next leaf
	parent   uint32

	// For leaf nodes: keys[i] -> rids[i]
	// For internal nodes: keys[i] separates children[i] and children[i+1]
	keys     [][]byte
	rids     []storage.RID // Leaf nodes only
	children []uint32      // Internal nodes only
}

// readNode reads a node from a page.
func (bt *BTree) readNode(pageID uint32) (*node, error) {
	buf := make([]byte, bt.pageSize)
	if err := bt.pager.ReadPage(pageID, buf); err != nil {
		return nil, fmt.Errorf("btree: read page %d: %w", pageID, err)
	}

	n := &node{
		pageID:   pageID,
		nodeType: NodeType(buf[NodeTypeOffset]),
		numKeys:  binary.LittleEndian.Uint16(buf[NumKeysOffset:]),
		rightSib: binary.LittleEndian.Uint32(buf[RightSibOffset:]),
		parent:   binary.LittleEndian.Uint32(buf[ParentOffset:]),
	}

	if n.nodeType != NodeTypeLeaf && n.nodeType != NodeTypeInternal {
		return nil, fmt.Errorf("%w: invalid node type %d at page %d", ErrInvalidNode, n.nodeType, pageID)
	}

	// Parse entries
	offset := HeaderSize
	for i := uint16(0); i < n.numKeys; i++ {
		if offset+2 > bt.pageSize {
			return nil, fmt.Errorf("%w: truncated key at page %d", ErrInvalidNode, pageID)
		}

		keyLen := binary.LittleEndian.Uint16(buf[offset:])
		offset += 2

		if offset+int(keyLen) > bt.pageSize {
			return nil, fmt.Errorf("%w: key overflow at page %d", ErrInvalidNode, pageID)
		}

		key := make([]byte, keyLen)
		copy(key, buf[offset:offset+int(keyLen)])
		offset += int(keyLen)
		n.keys = append(n.keys, key)

		if n.nodeType == NodeTypeLeaf {
			// Read RID (12 bytes: 4 for table name len + name + 4 page + 2 slot)
			// Simplified: store as fixed 12 bytes
			if offset+12 > bt.pageSize {
				return nil, fmt.Errorf("%w: RID overflow at page %d", ErrInvalidNode, pageID)
			}
			rid := decodeRID(buf[offset:])
			offset += 12
			n.rids = append(n.rids, rid)
		} else {
			// Read child page ID
			if offset+4 > bt.pageSize {
				return nil, fmt.Errorf("%w: child overflow at page %d", ErrInvalidNode, pageID)
			}
			childID := binary.LittleEndian.Uint32(buf[offset:])
			offset += 4
			n.children = append(n.children, childID)
		}
	}

	// Internal nodes have one more child than keys
	if n.nodeType == NodeTypeInternal && n.numKeys > 0 {
		if offset+4 > bt.pageSize {
			return nil, fmt.Errorf("%w: last child overflow at page %d", ErrInvalidNode, pageID)
		}
		childID := binary.LittleEndian.Uint32(buf[offset:])
		n.children = append(n.children, childID)
	}

	return n, nil
}

// writeNode writes a node to its page.
func (bt *BTree) writeNode(n *node) error {
	buf := make([]byte, bt.pageSize)

	buf[NodeTypeOffset] = byte(n.nodeType)
	binary.LittleEndian.PutUint16(buf[NumKeysOffset:], n.numKeys)
	binary.LittleEndian.PutUint32(buf[RightSibOffset:], n.rightSib)
	binary.LittleEndian.PutUint32(buf[ParentOffset:], n.parent)

	offset := HeaderSize
	for i := 0; i < int(n.numKeys); i++ {
		key := n.keys[i]

		// Write key length and key
		binary.LittleEndian.PutUint16(buf[offset:], uint16(len(key)))
		offset += 2
		copy(buf[offset:], key)
		offset += len(key)

		if n.nodeType == NodeTypeLeaf {
			// Write RID
			encodeRID(buf[offset:], n.rids[i])
			offset += 12
		} else {
			// Write child page ID
			binary.LittleEndian.PutUint32(buf[offset:], n.children[i])
			offset += 4
		}
	}

	// Internal nodes: write last child
	if n.nodeType == NodeTypeInternal && len(n.children) > int(n.numKeys) {
		binary.LittleEndian.PutUint32(buf[offset:], n.children[n.numKeys])
	}

	return bt.pager.WritePage(n.pageID, buf)
}

// encodeRID encodes a RID into 12 bytes (simplified fixed format).
func encodeRID(buf []byte, rid storage.RID) {
	// Format: 4 bytes table name hash, 4 bytes page, 2 bytes slot, 2 reserved
	h := hashString(rid.Table)
	binary.LittleEndian.PutUint32(buf[0:], h)
	binary.LittleEndian.PutUint32(buf[4:], rid.Page)
	binary.LittleEndian.PutUint16(buf[8:], rid.Slot)
	binary.LittleEndian.PutUint16(buf[10:], 0) // reserved
}

// decodeRID decodes a RID from 12 bytes.
func decodeRID(buf []byte) storage.RID {
	return storage.RID{
		Table: "", // Table name not stored, must be known from context
		Page:  binary.LittleEndian.Uint32(buf[4:]),
		Slot:  binary.LittleEndian.Uint16(buf[8:]),
	}
}

// hashString creates a simple hash of a string for storage.
func hashString(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

// compareKeys compares two keys.
// Returns: -1 if a < b, 0 if a == b, 1 if a > b
func compareKeys(a, b []byte) int {
	return bytes.Compare(a, b)
}
