package storage

import (
    "fmt"
    "io"
    "os"
    "path/filepath"
)

// Pager provides page-level read/write access to a file with a fixed page size.
type Pager struct {
    file     *os.File
    pageSize int
    path     string
}

// OpenPager opens or creates a file under dataDir with given name and page size.
func OpenPager(dataDir, name string, pageSize int) (*Pager, error) {
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
    return &Pager{file: f, pageSize: pageSize, path: path}, nil
}

// ReadPage reads exactly one page into buf. buf must be of length pageSize.
func (p *Pager) ReadPage(pageID uint32, buf []byte) error {
    if len(buf) != p.pageSize {
        return fmt.Errorf("buffer size %d != page size %d", len(buf), p.pageSize)
    }
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
    return p.file.Sync()
}

// Close closes the underlying file.
func (p *Pager) Close() error {
    return p.file.Close()
}
