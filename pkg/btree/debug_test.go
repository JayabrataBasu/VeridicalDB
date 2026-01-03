package btree

import (
	"os"
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/storage"
)

func TestIndexManagerPersistenceDebug(t *testing.T) {
	dir := tempDir(t)
	defer func() { _ = os.RemoveAll(dir) }()

	// Create and populate
	im1, err := NewIndexManager(dir, 4096)
	if err != nil {
		t.Fatalf("Failed to create index manager: %v", err)
	}

	im1.CreateIndex(IndexMeta{
		Name:      "idx_persist",
		TableName: "test",
		Columns:   []string{"id"},
		Unique:    true,
	})

	idx1, _ := im1.GetIndex("idx_persist")
	t.Logf("After create: RootPage=%d", idx1.RootPage)

	for i := 0; i < 50; i++ {
		key := EncodeIntKey(int64(i))
		rid := storage.RID{Page: uint32(i), Slot: 0}
		im1.Insert("idx_persist", key, rid)
	}

	bt1, _ := im1.GetBTree("idx_persist")
	t.Logf("After inserts: BTree.RootPage=%d", bt1.RootPage())

	idx2, _ := im1.GetIndex("idx_persist")
	t.Logf("Index metadata RootPage=%d", idx2.RootPage)

	im1.Close()

	// Read the metadata file
	data, _ := os.ReadFile(im1.metaFilePath())
	t.Logf("Saved metadata: %s", string(data))

	// Reopen
	im2, err := NewIndexManager(dir, 4096)
	if err != nil {
		t.Fatalf("Failed to reopen index manager: %v", err)
	}
	defer im2.Close()

	idx3, _ := im2.GetIndex("idx_persist")
	t.Logf("After reopen: RootPage=%d", idx3.RootPage)

	// Verify data persisted
	key := EncodeIntKey(int64(0))
	rid, err := im2.Search("idx_persist", key)
	t.Logf("Search result: RID=%v, err=%v", rid, err)
}
