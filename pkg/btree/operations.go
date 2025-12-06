package btree

import (
	"fmt"

	"github.com/JayabrataBasu/VeridicalDB/pkg/storage"
)

// Search finds the RID associated with a key.
// Returns ErrKeyNotFound if the key doesn't exist.
func (bt *BTree) Search(key []byte) (storage.RID, error) {
	if len(key) == 0 {
		return storage.RID{}, ErrEmptyKey
	}

	bt.mu.RLock()
	defer bt.mu.RUnlock()

	// Find the leaf node containing the key
	leaf, err := bt.findLeaf(key)
	if err != nil {
		return storage.RID{}, err
	}

	// Search within the leaf
	for i := 0; i < int(leaf.numKeys); i++ {
		cmp := compareKeys(key, leaf.keys[i])
		if cmp == 0 {
			return leaf.rids[i], nil
		}
		if cmp < 0 {
			// Keys are sorted, so key doesn't exist
			break
		}
	}

	return storage.RID{}, ErrKeyNotFound
}

// SearchAll finds all RIDs with the exact matching key.
// Useful for non-unique indexes where multiple entries can have the same key.
func (bt *BTree) SearchAll(key []byte) ([]storage.RID, error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}

	bt.mu.RLock()
	defer bt.mu.RUnlock()

	var results []storage.RID

	// Find the leaf node containing the key
	leaf, err := bt.findLeaf(key)
	if err != nil {
		return nil, err
	}

	// Search within this leaf and continue to sibling leaves
	for leaf != nil {
		for i := 0; i < int(leaf.numKeys); i++ {
			cmp := compareKeys(key, leaf.keys[i])
			if cmp == 0 {
				results = append(results, leaf.rids[i])
			} else if cmp < 0 {
				// Keys are sorted, no more matches
				return results, nil
			}
		}

		// Check sibling leaf for more matches
		if leaf.rightSib == 0 || leaf.rightSib == InvalidPageID {
			break
		}
		sibling, err := bt.readNode(leaf.rightSib)
		if err != nil {
			return results, nil // Return what we have
		}
		// Check if sibling has matching keys
		if int(sibling.numKeys) == 0 || compareKeys(key, sibling.keys[0]) < 0 {
			break // No more matches
		}
		leaf = sibling
	}

	if len(results) == 0 {
		return nil, ErrKeyNotFound
	}
	return results, nil
}

// SearchRange finds all RIDs for keys in the range [startKey, endKey].
// If startKey is nil, starts from the beginning.
// If endKey is nil, goes to the end.
func (bt *BTree) SearchRange(startKey, endKey []byte) ([]storage.RID, error) {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	var results []storage.RID

	// Find starting leaf
	var leaf *node
	var err error

	if startKey == nil {
		// Start from leftmost leaf
		leaf, err = bt.findLeftmostLeaf()
	} else {
		leaf, err = bt.findLeaf(startKey)
	}
	if err != nil {
		return nil, err
	}

	// Find starting position within leaf
	startIdx := 0
	if startKey != nil {
		for i := 0; i < int(leaf.numKeys); i++ {
			if compareKeys(leaf.keys[i], startKey) >= 0 {
				startIdx = i
				break
			}
			startIdx = i + 1
		}
	}

	// Scan leaves until we pass endKey
	for leaf != nil {
		for i := startIdx; i < int(leaf.numKeys); i++ {
			// Check if we've passed endKey
			if endKey != nil && compareKeys(leaf.keys[i], endKey) > 0 {
				return results, nil
			}
			results = append(results, leaf.rids[i])
		}

		// Move to next leaf
		if leaf.rightSib == InvalidPageID {
			break
		}
		leaf, err = bt.readNode(leaf.rightSib)
		if err != nil {
			return nil, err
		}
		startIdx = 0
	}

	return results, nil
}

// findLeaf traverses from root to find the leaf node that should contain the key.
func (bt *BTree) findLeaf(key []byte) (*node, error) {
	n, err := bt.readNode(bt.rootPage)
	if err != nil {
		return nil, err
	}

	for n.nodeType == NodeTypeInternal {
		// Find the child to descend into
		childIdx := bt.findChildIndex(n, key)
		if childIdx >= len(n.children) {
			return nil, fmt.Errorf("%w: child index %d out of bounds", ErrTreeCorrupted, childIdx)
		}

		n, err = bt.readNode(n.children[childIdx])
		if err != nil {
			return nil, err
		}
	}

	return n, nil
}

// findLeftmostLeaf finds the leftmost leaf node.
func (bt *BTree) findLeftmostLeaf() (*node, error) {
	n, err := bt.readNode(bt.rootPage)
	if err != nil {
		return nil, err
	}

	for n.nodeType == NodeTypeInternal {
		if len(n.children) == 0 {
			return nil, ErrTreeCorrupted
		}
		n, err = bt.readNode(n.children[0])
		if err != nil {
			return nil, err
		}
	}

	return n, nil
}

// findChildIndex finds which child to descend into for a given key.
func (bt *BTree) findChildIndex(n *node, key []byte) int {
	for i := 0; i < int(n.numKeys); i++ {
		if compareKeys(key, n.keys[i]) < 0 {
			return i
		}
	}
	// Key is >= all keys, go to rightmost child
	return int(n.numKeys)
}

// Insert adds a key-RID pair to the tree.
// Returns ErrDuplicateKey if unique=true and key already exists.
func (bt *BTree) Insert(key []byte, rid storage.RID) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	if len(key) > bt.pageSize/4 {
		return ErrKeyTooLarge
	}

	bt.mu.Lock()
	defer bt.mu.Unlock()

	// Find the leaf node where this key should go
	leaf, err := bt.findLeaf(key)
	if err != nil {
		return err
	}

	// Check for duplicate if unique
	if bt.unique {
		for i := 0; i < int(leaf.numKeys); i++ {
			if compareKeys(key, leaf.keys[i]) == 0 {
				return ErrDuplicateKey
			}
		}
	}

	// Insert into leaf
	return bt.insertIntoLeaf(leaf, key, rid)
}

// insertIntoLeaf inserts a key-RID pair into a leaf node.
func (bt *BTree) insertIntoLeaf(leaf *node, key []byte, rid storage.RID) error {
	// Find insertion position
	insertPos := 0
	for i := 0; i < int(leaf.numKeys); i++ {
		if compareKeys(key, leaf.keys[i]) < 0 {
			break
		}
		insertPos = i + 1
	}

	// Insert key and RID at position
	leaf.keys = insertAt(leaf.keys, insertPos, key)
	leaf.rids = insertRIDAt(leaf.rids, insertPos, rid)
	leaf.numKeys++

	// Check if we need to split
	if bt.needsSplit(leaf) {
		return bt.splitLeaf(leaf)
	}

	return bt.writeNode(leaf)
}

// needsSplit checks if a node needs to be split.
func (bt *BTree) needsSplit(n *node) bool {
	if n.nodeType == NodeTypeLeaf {
		return int(n.numKeys) > bt.maxKeysPerLeaf
	}
	return int(n.numKeys) > bt.maxKeysPerInternal
}

// splitLeaf splits a leaf node into two.
func (bt *BTree) splitLeaf(leaf *node) error {
	// Find split point (middle)
	mid := int(leaf.numKeys) / 2

	// Allocate new page for right leaf
	rightPageID, err := bt.allocatePage()
	if err != nil {
		return err
	}

	// Create right leaf node
	rightLeaf := &node{
		pageID:   rightPageID,
		nodeType: NodeTypeLeaf,
		numKeys:  uint16(int(leaf.numKeys) - mid),
		rightSib: leaf.rightSib, // Right leaf takes old right sibling
		parent:   leaf.parent,
		keys:     make([][]byte, int(leaf.numKeys)-mid),
		rids:     make([]storage.RID, int(leaf.numKeys)-mid),
	}

	// Copy right half to new leaf
	copy(rightLeaf.keys, leaf.keys[mid:])
	copy(rightLeaf.rids, leaf.rids[mid:])

	// Update left leaf
	leaf.keys = leaf.keys[:mid]
	leaf.rids = leaf.rids[:mid]
	leaf.numKeys = uint16(mid)
	leaf.rightSib = rightPageID

	// Write both leaves
	if err := bt.writeNode(leaf); err != nil {
		return err
	}
	if err := bt.writeNode(rightLeaf); err != nil {
		return err
	}

	// Insert separator key into parent
	separatorKey := rightLeaf.keys[0]
	return bt.insertIntoParent(leaf, separatorKey, rightLeaf)
}

// insertIntoParent inserts a separator key and new child into the parent.
func (bt *BTree) insertIntoParent(left *node, key []byte, right *node) error {
	// If left is root, create new root
	if left.parent == InvalidPageID {
		return bt.createNewRoot(left, key, right)
	}

	// Read parent
	parent, err := bt.readNode(left.parent)
	if err != nil {
		return err
	}

	// Find position of left child in parent
	leftIdx := -1
	for i, childID := range parent.children {
		if childID == left.pageID {
			leftIdx = i
			break
		}
	}
	if leftIdx == -1 {
		return ErrTreeCorrupted
	}

	// Insert key and new child after left
	parent.keys = insertAt(parent.keys, leftIdx, key)
	parent.children = insertUint32At(parent.children, leftIdx+1, right.pageID)
	parent.numKeys++

	// Update right's parent pointer
	right.parent = parent.pageID
	if err := bt.writeNode(right); err != nil {
		return err
	}

	// Check if parent needs split
	if bt.needsSplit(parent) {
		return bt.splitInternal(parent)
	}

	return bt.writeNode(parent)
}

// createNewRoot creates a new root with two children.
func (bt *BTree) createNewRoot(left *node, key []byte, right *node) error {
	// Allocate new root page
	newRootID, err := bt.allocatePage()
	if err != nil {
		return err
	}

	newRoot := &node{
		pageID:   newRootID,
		nodeType: NodeTypeInternal,
		numKeys:  1,
		rightSib: InvalidPageID,
		parent:   InvalidPageID,
		keys:     [][]byte{key},
		children: []uint32{left.pageID, right.pageID},
	}

	// Update children's parent pointers
	left.parent = newRootID
	right.parent = newRootID

	// Write all nodes
	if err := bt.writeNode(newRoot); err != nil {
		return err
	}
	if err := bt.writeNode(left); err != nil {
		return err
	}
	if err := bt.writeNode(right); err != nil {
		return err
	}

	bt.rootPage = newRootID
	return nil
}

// splitInternal splits an internal node.
func (bt *BTree) splitInternal(n *node) error {
	mid := int(n.numKeys) / 2

	// The key at mid goes up to parent
	upKey := n.keys[mid]

	// Allocate new page for right internal node
	rightPageID, err := bt.allocatePage()
	if err != nil {
		return err
	}

	// Create right internal node (keys after mid, children after mid+1)
	rightInternal := &node{
		pageID:   rightPageID,
		nodeType: NodeTypeInternal,
		numKeys:  uint16(int(n.numKeys) - mid - 1),
		rightSib: InvalidPageID,
		parent:   n.parent,
		keys:     make([][]byte, int(n.numKeys)-mid-1),
		children: make([]uint32, int(n.numKeys)-mid),
	}

	copy(rightInternal.keys, n.keys[mid+1:])
	copy(rightInternal.children, n.children[mid+1:])

	// Update left internal node
	n.keys = n.keys[:mid]
	n.children = n.children[:mid+1]
	n.numKeys = uint16(mid)

	// Update parent pointers for moved children
	for _, childID := range rightInternal.children {
		child, err := bt.readNode(childID)
		if err != nil {
			return err
		}
		child.parent = rightPageID
		if err := bt.writeNode(child); err != nil {
			return err
		}
	}

	// Write both internal nodes
	if err := bt.writeNode(n); err != nil {
		return err
	}
	if err := bt.writeNode(rightInternal); err != nil {
		return err
	}

	// Insert up key into parent
	return bt.insertIntoParent(n, upKey, rightInternal)
}

// allocatePage allocates a new page and returns its ID.
// Simple approach: track next page ID.
func (bt *BTree) allocatePage() (uint32, error) {
	// Read through pages to find next available
	// In a real implementation, we'd have a free list
	var pageID uint32 = 0
	buf := make([]byte, bt.pageSize)

	for {
		err := bt.pager.ReadPage(pageID, buf)
		if err != nil {
			// Assume this is EOF - page doesn't exist yet
			// Initialize as empty
			for i := range buf {
				buf[i] = 0
			}
			if err := bt.pager.WritePage(pageID, buf); err != nil {
				return 0, err
			}
			return pageID, nil
		}

		// Check if page is empty/free (node type = 0)
		if buf[NodeTypeOffset] == 0 {
			return pageID, nil
		}

		pageID++
		if pageID > 1000000 { // Safety limit
			return 0, fmt.Errorf("btree: too many pages")
		}
	}
}

// Delete removes a key from the tree.
// Returns ErrKeyNotFound if the key doesn't exist.
func (bt *BTree) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	bt.mu.Lock()
	defer bt.mu.Unlock()

	// Find the leaf containing the key
	leaf, err := bt.findLeaf(key)
	if err != nil {
		return err
	}

	// Find and remove the key
	keyIdx := -1
	for i := 0; i < int(leaf.numKeys); i++ {
		if compareKeys(key, leaf.keys[i]) == 0 {
			keyIdx = i
			break
		}
	}

	if keyIdx == -1 {
		return ErrKeyNotFound
	}

	// Remove key and RID
	leaf.keys = removeAt(leaf.keys, keyIdx)
	leaf.rids = removeRIDAt(leaf.rids, keyIdx)
	leaf.numKeys--

	// If this is root or has enough keys, just write and return
	if leaf.parent == InvalidPageID || int(leaf.numKeys) >= bt.minKeysPerLeaf {
		return bt.writeNode(leaf)
	}

	// Handle underflow - for simplicity, we just write and tolerate some underflow
	// A full implementation would merge or redistribute with siblings
	return bt.writeNode(leaf)
}

// Helper functions for slice manipulation

func insertAt(slice [][]byte, pos int, val []byte) [][]byte {
	slice = append(slice, nil)
	copy(slice[pos+1:], slice[pos:])
	slice[pos] = val
	return slice
}

func insertRIDAt(slice []storage.RID, pos int, val storage.RID) []storage.RID {
	slice = append(slice, storage.RID{})
	copy(slice[pos+1:], slice[pos:])
	slice[pos] = val
	return slice
}

func insertUint32At(slice []uint32, pos int, val uint32) []uint32 {
	slice = append(slice, 0)
	copy(slice[pos+1:], slice[pos:])
	slice[pos] = val
	return slice
}

func removeAt(slice [][]byte, pos int) [][]byte {
	return append(slice[:pos], slice[pos+1:]...)
}

func removeRIDAt(slice []storage.RID, pos int) []storage.RID {
	return append(slice[:pos], slice[pos+1:]...)
}
