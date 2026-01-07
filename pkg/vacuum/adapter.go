package vacuum

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/JayabrataBasu/VeridicalDB/pkg/storage"
)

// StorageAdapter wraps the existing storage.Storage to implement StorageInterface.
type StorageAdapter struct {
	storage  *storage.Storage
	dataDir  string
	pageSize int
}

// NewStorageAdapter creates a new storage adapter for vacuum operations.
func NewStorageAdapter(s *storage.Storage, dataDir string, pageSize int) *StorageAdapter {
	return &StorageAdapter{
		storage:  s,
		dataDir:  dataDir,
		pageSize: pageSize,
	}
}

// ScanTable returns all RIDs in a table.
func (a *StorageAdapter) ScanTable(table string) ([]RID, error) {
	pageCount, err := a.GetPageCount(table)
	if err != nil {
		return nil, err
	}

	var rids []RID
	for pageID := 0; pageID < pageCount; pageID++ {
		pageData, err := a.FetchPage(table, uint32(pageID))
		if err != nil {
			continue
		}

		// Check magic
		if len(pageData) < 12 {
			continue
		}
		magic := binary.LittleEndian.Uint16(pageData[8:10])
		if magic != 0xDB01 {
			continue
		}

		slotCount := int(binary.LittleEndian.Uint16(pageData[10:12]))
		for slot := 0; slot < slotCount; slot++ {
			offset, length := a.getSlot(pageData, slot)
			if length > 0 && offset > 0 {
				rids = append(rids, RID{Table: table, Page: uint32(pageID), Slot: uint16(slot)})
			}
		}
	}

	return rids, nil
}

// FetchPage reads a page into buffer.
func (a *StorageAdapter) FetchPage(table string, pageID uint32) ([]byte, error) {
	path := filepath.Join(a.dataDir, "tables", table+".tbl")
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	buf := make([]byte, a.pageSize)
	offset := int64(pageID) * int64(a.pageSize)
	n, err := f.ReadAt(buf, offset)
	if err != nil && n == 0 {
		return nil, err
	}

	return buf, nil
}

// WritePage writes a page buffer back.
func (a *StorageAdapter) WritePage(table string, pageID uint32, data []byte) error {
	path := filepath.Join(a.dataDir, "tables", table+".tbl")
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	offset := int64(pageID) * int64(a.pageSize)
	_, err = f.WriteAt(data, offset)
	return err
}

// GetPageCount returns the number of pages in a table.
func (a *StorageAdapter) GetPageCount(table string) (int, error) {
	path := filepath.Join(a.dataDir, "tables", table+".tbl")
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	return int(info.Size() / int64(a.pageSize)), nil
}

// FetchRaw returns raw tuple data for a RID.
func (a *StorageAdapter) FetchRaw(rid RID) ([]byte, error) {
	return a.storage.Fetch(storage.RID{Table: rid.Table, Page: rid.Page, Slot: rid.Slot})
}

// DeleteSlot physically removes a tuple slot.
func (a *StorageAdapter) DeleteSlot(rid RID) error {
	return a.storage.Delete(storage.RID{Table: rid.Table, Page: rid.Page, Slot: rid.Slot})
}

// CompactPage compacts a page, removing dead tuples and defragmenting.
func (a *StorageAdapter) CompactPage(table string, pageID uint32, deadSlots []int) (bytesReclaimed int64, err error) {
	pageData, err := a.FetchPage(table, pageID)
	if err != nil {
		return 0, err
	}

	// Clear the dead slots
	for _, slot := range deadSlots {
		offset, length := a.getSlot(pageData, slot)
		if length > 0 {
			bytesReclaimed += int64(length)
			// Clear slot entry (offset=0, length=0)
			a.setSlot(pageData, slot, 0, 0)
		}
		_ = offset // suppress unused
	}

	// Defragment the page if this is a full vacuum
	// For now, we just clear slots - full defragmentation is more complex
	// and would require rewriting all tuples contiguously

	return bytesReclaimed, a.WritePage(table, pageID, pageData)
}

// getSlot reads slot entry from page buffer.
func (a *StorageAdapter) getSlot(buf []byte, slot int) (offset uint16, length uint16) {
	const headerSize = 24
	const slotEntrySize = 4
	off := headerSize + slot*slotEntrySize
	if off+4 > len(buf) {
		return 0, 0
	}
	return binary.LittleEndian.Uint16(buf[off : off+2]),
		binary.LittleEndian.Uint16(buf[off+2 : off+4])
}

// setSlot writes slot entry to page buffer.
func (a *StorageAdapter) setSlot(buf []byte, slot int, offset, length uint16) {
	const headerSize = 24
	const slotEntrySize = 4
	off := headerSize + slot*slotEntrySize
	if off+4 > len(buf) {
		return
	}
	binary.LittleEndian.PutUint16(buf[off:off+2], offset)
	binary.LittleEndian.PutUint16(buf[off+2:off+4], length)
}

// CatalogAdapter wraps the existing catalog to implement CatalogInterface.
type CatalogAdapter struct {
	listTablesFn func() []string
	getStatsFn   func(table string) (*TableStats, error)
}

// NewCatalogAdapter creates a new catalog adapter.
func NewCatalogAdapter(listTablesFn func() []string, getStatsFn func(table string) (*TableStats, error)) *CatalogAdapter {
	return &CatalogAdapter{
		listTablesFn: listTablesFn,
		getStatsFn:   getStatsFn,
	}
}

// ListTables returns all table names.
func (a *CatalogAdapter) ListTables() []string {
	return a.listTablesFn()
}

// GetTableStats returns statistics for a table.
func (a *CatalogAdapter) GetTableStats(table string) (*TableStats, error) {
	return a.getStatsFn(table)
}

// SimpleCatalogAdapter is a simple implementation that scans storage for stats.
type SimpleCatalogAdapter struct {
	storage   *StorageAdapter
	tableList []string
}

// NewSimpleCatalogAdapter creates a catalog adapter that computes stats by scanning.
func NewSimpleCatalogAdapter(storage *StorageAdapter, tables []string) *SimpleCatalogAdapter {
	return &SimpleCatalogAdapter{
		storage:   storage,
		tableList: tables,
	}
}

// ListTables returns all table names.
func (a *SimpleCatalogAdapter) ListTables() []string {
	return a.tableList
}

// GetTableStats computes statistics by scanning the table.
// This is expensive and should be cached in production.
func (a *SimpleCatalogAdapter) GetTableStats(table string) (*TableStats, error) {
	stats := &TableStats{
		TableName: table,
	}

	pageCount, err := a.storage.GetPageCount(table)
	if err != nil {
		return nil, fmt.Errorf("get page count: %w", err)
	}
	stats.TotalPages = int64(pageCount)

	// Scan pages to count tuples
	for pageID := 0; pageID < pageCount; pageID++ {
		pageData, err := a.storage.FetchPage(table, uint32(pageID))
		if err != nil {
			continue
		}

		if len(pageData) < 12 {
			continue
		}
		magic := binary.LittleEndian.Uint16(pageData[8:10])
		if magic != 0xDB01 {
			continue
		}

		slotCount := int(binary.LittleEndian.Uint16(pageData[10:12]))
		for slot := 0; slot < slotCount; slot++ {
			offset, length := a.storage.getSlot(pageData, slot)
			if length == 0 || offset == 0 {
				continue
			}

			stats.TotalTuples++

			// Check if tuple is dead (simplified check)
			tupleData := pageData[offset : offset+length]
			if len(tupleData) >= 16 {
				xmax := binary.LittleEndian.Uint64(tupleData[8:16])
				if xmax != 0 {
					// Has xmax set - potentially dead
					stats.DeadTuples++
				}
			}
		}
	}

	return stats, nil
}
