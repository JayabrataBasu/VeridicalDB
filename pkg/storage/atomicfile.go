package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

// WriteFileAtomic writes data to path atomically: it writes to a temp file in
// the same directory, fsyncs it, then renames it over path. A crash at any point
// leaves either the old file intact or the new file fully written — never a
// truncated or partially-written file. Used for catalog / index / procedure /
// trigger metadata, where a torn write corrupts the database's schema.
func WriteFileAtomic(path string, data []byte, perm os.FileMode) (err error) {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("atomic write %s: create temp: %w", path, err)
	}
	tmpName := tmp.Name()

	// On any failure past this point, remove the temp file.
	defer func() {
		if err != nil {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err = tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("atomic write %s: write temp: %w", path, err)
	}
	if err = tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("atomic write %s: fsync temp: %w", path, err)
	}
	if err = tmp.Close(); err != nil {
		return fmt.Errorf("atomic write %s: close temp: %w", path, err)
	}
	if err = os.Chmod(tmpName, perm); err != nil {
		return fmt.Errorf("atomic write %s: chmod temp: %w", path, err)
	}
	if err = os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("atomic write %s: rename: %w", path, err)
	}

	// Best-effort fsync of the directory so the rename itself is durable.
	if d, derr := os.Open(dir); derr == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}
