package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/JayabrataBasu/VeridicalDB/pkg/wal"
)

// PageLSNOffset is the offset in the page header where the pageLSN is stored.
// The first 8 bytes of each page are reserved for the pageLSN.
const PageLSNOffset = 0
const PageLSNSize = 8

// Pager provides page-level read/write access to a file with a fixed page size.
// It integrates with the WAL for crash recovery support.
type Pager struct {
	file     *os.File
	pageSize int
	path     string
	mu       sync.Mutex // Protects file operations

	// wal is the Write-Ahead Log for durability
	wal *wal.WAL

	// pageLSNs tracks the LSN of the last modification to each page
	// This is kept in memory for quick access; also stored in page headers
	pageLSNs map[uint32]wal.LSN
}

// OpenPager opens or creates a file under dataDir with given name and page size.
func OpenPager(dataDir, name string, pageSize int) (*Pager, error) {
	return OpenPagerWithWAL(dataDir, name, pageSize, nil)
}

// OpenPagerWithWAL opens a pager with WAL support for crash recovery.
func OpenPagerWithWAL(dataDir, name string, pageSize int, walLog *wal.WAL) (*Pager, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir data dir: %w", err)
	}
	path := filepath.Join(dataDir, name)
	// ensure parent directories for the file exist
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("mkdir pager parent dir: %w", err)
		}
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open pager file: %w", err)
	}
	return &Pager{
		file:     f,
		pageSize: pageSize,
		path:     path,
		wal:      walLog,
		pageLSNs: make(map[uint32]wal.LSN),
	}, nil
}

// SetWAL sets the WAL for this pager (can be set after construction).
func (p *Pager) SetWAL(walLog *wal.WAL) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.wal = walLog
}

// GetPageLSN returns the LSN of the last modification to a page.
func (p *Pager) GetPageLSN(pageID uint32) wal.LSN {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pageLSNs[pageID]
}

// SetPageLSN updates the in-memory LSN for a page.
// The LSN is also embedded in the page header when written.
func (p *Pager) SetPageLSN(pageID uint32, lsn wal.LSN) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pageLSNs[pageID] = lsn
}

// ReadPageLSN reads the LSN from the page header on disk.
func (p *Pager) ReadPageLSN(pageID uint32, buf []byte) wal.LSN {
	if len(buf) < PageLSNSize {
		return wal.InvalidLSN
	}
	return wal.LSN(binary.LittleEndian.Uint64(buf[PageLSNOffset:]))
}

// WritePageLSN writes the LSN to the page buffer header.
func WritePageLSN(buf []byte, lsn wal.LSN) {
	if len(buf) >= PageLSNSize {
		binary.LittleEndian.PutUint64(buf[PageLSNOffset:], uint64(lsn))
	}
}

// ReadPage reads exactly one page into buf. buf must be of length pageSize.
func (p *Pager) ReadPage(pageID uint32, buf []byte) error {
	if len(buf) != p.pageSize {
		return fmt.Errorf("buffer size %d != page size %d", len(buf), p.pageSize)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	off := int64(pageID) * int64(p.pageSize)
	if _, err := p.file.Seek(off, 0); err != nil {
		return fmt.Errorf("seek: %w", err)
	}
	// Read full page; if file shorter, return zeroed buffer
	n, err := p.file.Read(buf)
	if err != nil && err != io.EOF {
		return fmt.Errorf("read page: %w", err)
	}
	if n < len(buf) {
		// zero remainder
		for i := n; i < len(buf); i++ {
			buf[i] = 0
		}
	}
	return nil
}

// WritePage writes exactly one page from buf to pageID.
func (p *Pager) WritePage(pageID uint32, buf []byte) error {
	if len(buf) != p.pageSize {
		return fmt.Errorf("buffer size %d != page size %d", len(buf), p.pageSize)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	off := int64(pageID) * int64(p.pageSize)
	if _, err := p.file.Seek(off, 0); err != nil {
		return fmt.Errorf("seek: %w", err)
	}
	n, err := p.file.Write(buf)
	if err != nil {
		return fmt.Errorf("write page: %w", err)
	}
	if n != len(buf) {
		return fmt.Errorf("short write")
	}

	// Update in-memory pageLSN from the page header
	if len(buf) >= PageLSNSize {
		lsn := wal.LSN(binary.LittleEndian.Uint64(buf[PageLSNOffset:]))
		if lsn > 0 {
			p.pageLSNs[pageID] = lsn
		}
	}

	return p.file.Sync()
}

// WritePageWithLSN writes a page and updates its LSN in both the buffer and memory.
// This should be called after the WAL record has been written.
func (p *Pager) WritePageWithLSN(pageID uint32, buf []byte, lsn wal.LSN) error {
	if len(buf) != p.pageSize {
		return fmt.Errorf("buffer size %d != page size %d", len(buf), p.pageSize)
	}

	// Embed LSN in page header
	if len(buf) >= PageLSNSize {
		binary.LittleEndian.PutUint64(buf[PageLSNOffset:], uint64(lsn))
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	off := int64(pageID) * int64(p.pageSize)
	if _, err := p.file.Seek(off, 0); err != nil {
		return fmt.Errorf("seek: %w", err)
	}
	n, err := p.file.Write(buf)
	if err != nil {
		return fmt.Errorf("write page: %w", err)
	}
	if n != len(buf) {
		return fmt.Errorf("short write")
	}

	// Update in-memory pageLSN
	p.pageLSNs[pageID] = lsn

	return p.file.Sync()
}

// Close closes the underlying file.
func (p *Pager) Close() error {
	return p.file.Close()
}

// GetDirtyPages returns all pages that have been modified (have a non-zero LSN).
// This is used for checkpointing.
func (p *Pager) GetDirtyPages() map[uint32]wal.LSN {
	p.mu.Lock()
	defer p.mu.Unlock()

	result := make(map[uint32]wal.LSN, len(p.pageLSNs))
	for pageID, lsn := range p.pageLSNs {
		result[pageID] = lsn
	}
	return result
}

// ClearPageLSN clears the LSN tracking for a page (used after checkpoint).
func (p *Pager) ClearPageLSN(pageID uint32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.pageLSNs, pageID)
}

// Sync flushes all pending writes to disk without closing.
func (p *Pager) Sync() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.file.Sync()
}

// NumPages returns the total number of pages in the file.
func (p *Pager) NumPages() (uint32, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	info, err := p.file.Stat()
	if err != nil {
		return 0, err
	}
	return uint32(info.Size() / int64(p.pageSize)), nil
}

// Path returns the file path of this pager.
func (p *Pager) Path() string {
	return p.path
}
