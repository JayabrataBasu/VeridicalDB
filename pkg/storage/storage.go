package storage

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/JayabrataBasu/VeridicalDB/pkg/wal"
)

// Storage is a simple table manager that uses one heap file per table.
type Storage struct {
	dataDir  string
	pageSize int
	wal      *wal.WAL
	mu       sync.Mutex // Protects all storage operations
}

// NewStorage creates a Storage rooted at dataDir using pageSize.
func NewStorage(dataDir string, pageSize int, walLog *wal.WAL) *Storage {
	return &Storage{dataDir: dataDir, pageSize: pageSize, wal: walLog}
}

// CreateTable creates an empty table file (if not exists) and writes an initial page.
func (s *Storage) CreateTable(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	pager, err := OpenPagerWithWAL(s.dataDir, tableFileName(name), s.pageSize, s.wal)
	if err != nil {
		return err
	}
	defer pager.Close()
	// ensure at least one page exists; write initialized page
	buf := make([]byte, s.pageSize)
	initPage(buf)
	if err := pager.WritePage(0, buf); err != nil {
		return fmt.Errorf("write initial page: %w", err)
	}
	return nil
}

// Insert inserts a record into the named table and returns RID.
func (s *Storage) Insert(table string, data []byte) (RID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pager, err := OpenPagerWithWAL(s.dataDir, tableFileName(table), s.pageSize, s.wal)
	if err != nil {
		return RID{}, err
	}
	defer pager.Close()

	// scan pages for free space; naive linear scan
	pageBuf := make([]byte, s.pageSize)
	var pageID uint32 = 0
	for {
		if err := pager.ReadPage(pageID, pageBuf); err != nil {
			return RID{}, err
		}
		// if page uninitialized, init it
		// Check magic at offset 8 (after LSN)
		if len(pageBuf) >= 10 {
			// assume initialized if magic matches
			if binary.LittleEndian.Uint16(pageBuf[8:10]) != pageMagic {
				initPage(pageBuf)
			}
		} else {
			initPage(pageBuf)
		}

		slot, err := insertTuple(pageBuf, data)
		if err == nil {
			if err := pager.WritePage(pageID, pageBuf); err != nil {
				return RID{}, err
			}
			return RID{Table: table, Page: pageID, Slot: uint16(slot)}, nil
		}
		// if no space, go to next page; check EOF by attempting to read next page
		pageID++
		// ensure file grows by writing new zeroed initialized page when we pass EOF
		// Try reading next page; if read returns zeroed buf, we'll initialize on next loop
		// The pager.ReadPage zeros remainder if short file, so that's fine
		if pageID > 1_000_000 { // safety
			return RID{}, fmt.Errorf("table appears full")
		}
	}
}

// Fetch returns record bytes for a given RID.
func (s *Storage) Fetch(rid RID) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pager, err := OpenPagerWithWAL(s.dataDir, tableFileName(rid.Table), s.pageSize, s.wal)
	if err != nil {
		return nil, err
	}
	defer pager.Close()
	pageBuf := make([]byte, s.pageSize)
	if err := pager.ReadPage(rid.Page, pageBuf); err != nil {
		return nil, err
	}
	return fetchTuple(pageBuf, int(rid.Slot))
}

// Delete marks a record as deleted by clearing its slot.
func (s *Storage) Delete(rid RID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	pager, err := OpenPager(s.dataDir, tableFileName(rid.Table), s.pageSize)
	if err != nil {
		return err
	}
	defer pager.Close()
	pageBuf := make([]byte, s.pageSize)
	if err := pager.ReadPage(rid.Page, pageBuf); err != nil {
		return err
	}
	if err := deleteTuple(pageBuf, int(rid.Slot)); err != nil {
		return err
	}
	return pager.WritePage(rid.Page, pageBuf)
}

// Update updates a record in place. The new data must be the same size as the old.
// This is primarily used for MVCC XMax updates.
func (s *Storage) Update(rid RID, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	pager, err := OpenPager(s.dataDir, tableFileName(rid.Table), s.pageSize)
	if err != nil {
		return err
	}
	defer pager.Close()
	pageBuf := make([]byte, s.pageSize)
	if err := pager.ReadPage(rid.Page, pageBuf); err != nil {
		return err
	}
	if err := updateTupleInPlace(pageBuf, int(rid.Slot), data); err != nil {
		return err
	}
	return pager.WritePage(rid.Page, pageBuf)
}

func tableFileName(table string) string {
	return filepath.Join("tables", table+".tbl")
}

// ReplayInsert inserts a record into a specific page and slot.
// Used for WAL recovery.
func (s *Storage) ReplayInsert(rid RID, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	pager, err := OpenPagerWithWAL(s.dataDir, tableFileName(rid.Table), s.pageSize, s.wal)
	if err != nil {
		return err
	}
	defer pager.Close()

	pageBuf := make([]byte, s.pageSize)
	if err := pager.ReadPage(rid.Page, pageBuf); err != nil {
		// If page doesn't exist (EOF), we might need to create it.
		// But ReadPage usually returns error or zeroed buf.
		// If it's a new page, we init it.
		initPage(pageBuf)
	}

	// Check magic
	if binary.LittleEndian.Uint16(pageBuf[8:10]) != pageMagic {
		initPage(pageBuf)
	}

	if err := insertTupleAt(pageBuf, int(rid.Slot), data); err != nil {
		return err
	}

	return pager.WritePage(rid.Page, pageBuf)
}

// ReplayDelete deletes a record at a specific page and slot.
// Used for WAL recovery.
func (s *Storage) ReplayDelete(rid RID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	pager, err := OpenPagerWithWAL(s.dataDir, tableFileName(rid.Table), s.pageSize, s.wal)
	if err != nil {
		return err
	}
	defer pager.Close()

	pageBuf := make([]byte, s.pageSize)
	if err := pager.ReadPage(rid.Page, pageBuf); err != nil {
		return err
	}

	// Check magic
	if binary.LittleEndian.Uint16(pageBuf[8:10]) != pageMagic {
		return fmt.Errorf("page not initialized")
	}

	if err := deleteTuple(pageBuf, int(rid.Slot)); err != nil {
		// If slot already empty, that's fine for replay (idempotent)
		return nil
	}

	return pager.WritePage(rid.Page, pageBuf)
}

// ReplayUpdate updates a record at a specific page and slot.
// Used for WAL recovery.
func (s *Storage) ReplayUpdate(rid RID, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	pager, err := OpenPagerWithWAL(s.dataDir, tableFileName(rid.Table), s.pageSize, s.wal)
	if err != nil {
		return err
	}
	defer pager.Close()

	pageBuf := make([]byte, s.pageSize)
	if err := pager.ReadPage(rid.Page, pageBuf); err != nil {
		return err
	}

	// Check magic
	if binary.LittleEndian.Uint16(pageBuf[8:10]) != pageMagic {
		return fmt.Errorf("page not initialized")
	}

	// For update, we use insertTupleAt which handles overwrite
	if err := insertTupleAt(pageBuf, int(rid.Slot), data); err != nil {
		return err
	}

	return pager.WritePage(rid.Page, pageBuf)
}
