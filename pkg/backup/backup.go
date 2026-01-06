// Package backup provides backup and point-in-time recovery (PITR) capabilities
// for VeridicalDB. It supports:
// - Full base backups with consistent LSN markers
// - WAL archiving for continuous recovery points
// - Point-in-time recovery to any archived WAL position
package backup

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/wal"
)

// BackupMetadata contains information about a base backup.
type BackupMetadata struct {
	// ID is a unique identifier for the backup
	ID string `json:"id"`

	// StartTime is when the backup started
	StartTime time.Time `json:"start_time"`

	// EndTime is when the backup completed
	EndTime time.Time `json:"end_time"`

	// StartLSN is the WAL position at backup start
	StartLSN uint64 `json:"start_lsn"`

	// EndLSN is the WAL position at backup end
	EndLSN uint64 `json:"end_lsn"`

	// DataDir is the original data directory that was backed up
	DataDir string `json:"data_dir"`

	// Size is the total size of the backup in bytes
	Size int64 `json:"size"`

	// Checksum is a SHA256 hash of the backup contents
	Checksum string `json:"checksum"`

	// Compressed indicates if the backup is compressed
	Compressed bool `json:"compressed"`

	// Files contains checksums for individual files
	Files map[string]string `json:"files"`

	// Version is the backup format version
	Version int `json:"version"`
}

// Config holds backup configuration options.
type Config struct {
	// BackupDir is the directory where backups are stored
	BackupDir string `json:"backup_dir" yaml:"backup_dir"`

	// ArchiveDir is the directory for archived WAL segments
	ArchiveDir string `json:"archive_dir" yaml:"archive_dir"`

	// Compress enables gzip compression for backups
	Compress bool `json:"compress" yaml:"compress"`

	// RetentionDays is how long to keep backups (0 = keep forever)
	RetentionDays int `json:"retention_days" yaml:"retention_days"`

	// ArchiveCommand is an optional command to run for WAL archiving
	// Use %f for filename and %p for full path
	ArchiveCommand string `json:"archive_command" yaml:"archive_command"`

	// RestoreCommand is an optional command to restore WAL from archive
	// Use %f for filename and %p for destination path
	RestoreCommand string `json:"restore_command" yaml:"restore_command"`
}

// DefaultConfig returns default backup configuration.
func DefaultConfig() *Config {
	return &Config{
		BackupDir:     "./backups",
		ArchiveDir:    "./wal_archive",
		Compress:      true,
		RetentionDays: 30,
	}
}

// Manager handles backup and restore operations.
type Manager struct {
	mu sync.RWMutex

	config  *Config
	dataDir string
	walMgr  *wal.WAL

	// checkpointFn triggers a checkpoint before backup
	checkpointFn func() error
}

// NewManager creates a new backup manager.
func NewManager(config *Config, dataDir string, walMgr *wal.WAL) (*Manager, error) {
	if config == nil {
		config = DefaultConfig()
	}

	// Ensure directories exist
	if err := os.MkdirAll(config.BackupDir, 0755); err != nil {
		return nil, fmt.Errorf("create backup dir: %w", err)
	}
	if err := os.MkdirAll(config.ArchiveDir, 0755); err != nil {
		return nil, fmt.Errorf("create archive dir: %w", err)
	}

	return &Manager{
		config:  config,
		dataDir: dataDir,
		walMgr:  walMgr,
	}, nil
}

// SetCheckpointFunc sets the function to call for checkpoint before backup.
func (m *Manager) SetCheckpointFunc(fn func() error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checkpointFn = fn
}

// CreateBaseBackup creates a full backup of the data directory.
// Returns the backup metadata and output path.
func (m *Manager) CreateBaseBackup(outputPath string) (*BackupMetadata, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	startTime := time.Now()
	backupID := fmt.Sprintf("backup_%s", startTime.Format("20060102_150405"))

	if outputPath == "" {
		outputPath = filepath.Join(m.config.BackupDir, backupID)
		if m.config.Compress {
			outputPath += ".tar.gz"
		}
	}

	// Trigger checkpoint to get consistent state
	if m.checkpointFn != nil {
		if err := m.checkpointFn(); err != nil {
			return nil, fmt.Errorf("checkpoint: %w", err)
		}
	}

	// Record starting LSN
	startLSN := m.walMgr.CurrentLSN()

	// Flush WAL to ensure durability
	if err := m.walMgr.Flush(); err != nil {
		return nil, fmt.Errorf("flush wal: %w", err)
	}

	// Create backup
	var totalSize int64
	var checksum string
	files := make(map[string]string)

	if m.config.Compress {
		size, hash, fileHashes, err := m.createCompressedBackup(outputPath, files)
		if err != nil {
			return nil, fmt.Errorf("create compressed backup: %w", err)
		}
		totalSize = size
		checksum = hash
		files = fileHashes
	} else {
		size, hash, fileHashes, err := m.createDirectoryBackup(outputPath, files)
		if err != nil {
			return nil, fmt.Errorf("create directory backup: %w", err)
		}
		totalSize = size
		checksum = hash
		files = fileHashes
	}

	// Record ending LSN
	endLSN := m.walMgr.CurrentLSN()

	metadata := &BackupMetadata{
		ID:         backupID,
		StartTime:  startTime,
		EndTime:    time.Now(),
		StartLSN:   uint64(startLSN),
		EndLSN:     uint64(endLSN),
		DataDir:    m.dataDir,
		Size:       totalSize,
		Checksum:   checksum,
		Compressed: m.config.Compress,
		Files:      files,
		Version:    1,
	}

	// Write metadata file
	metadataPath := outputPath
	if m.config.Compress {
		metadataPath = strings.TrimSuffix(outputPath, ".tar.gz")
	}
	metadataPath += ".meta.json"

	metaData, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal metadata: %w", err)
	}
	if err := os.WriteFile(metadataPath, metaData, 0644); err != nil {
		return nil, fmt.Errorf("write metadata: %w", err)
	}

	return metadata, nil
}

// createCompressedBackup creates a tar.gz archive of the data directory.
func (m *Manager) createCompressedBackup(outputPath string, _ map[string]string) (int64, string, map[string]string, error) {
	file, err := os.Create(outputPath)
	if err != nil {
		return 0, "", nil, err
	}
	defer func() { _ = file.Close() }()

	hash := sha256.New()
	multiWriter := io.MultiWriter(file, hash)

	gzWriter := gzip.NewWriter(multiWriter)
	defer func() { _ = gzWriter.Close() }()

	tarWriter := tar.NewWriter(gzWriter)
	defer func() { _ = tarWriter.Close() }()

	var totalSize int64
	files := make(map[string]string)

	err = filepath.Walk(m.dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip the backup directory itself if it's inside data dir
		if strings.HasPrefix(path, m.config.BackupDir) {
			return nil
		}
		if strings.HasPrefix(path, m.config.ArchiveDir) {
			return nil
		}

		// Create tar header
		relPath, err := filepath.Rel(m.dataDir, path)
		if err != nil {
			return err
		}

		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = relPath

		if err := tarWriter.WriteHeader(header); err != nil {
			return err
		}

		// If it's a file, write contents
		if !info.IsDir() {
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			defer func() { _ = f.Close() }()

			fileHash := sha256.New()
			reader := io.TeeReader(f, fileHash)

			written, err := io.Copy(tarWriter, reader)
			if err != nil {
				return err
			}
			totalSize += written
			files[relPath] = hex.EncodeToString(fileHash.Sum(nil))
		}

		return nil
	})

	if err != nil {
		return 0, "", nil, err
	}

	// Close writers to flush data
	if err := tarWriter.Close(); err != nil {
		return 0, "", nil, err
	}
	if err := gzWriter.Close(); err != nil {
		return 0, "", nil, err
	}

	return totalSize, hex.EncodeToString(hash.Sum(nil)), files, nil
}

// createDirectoryBackup creates a copy of the data directory.
func (m *Manager) createDirectoryBackup(outputPath string, _ map[string]string) (int64, string, map[string]string, error) {
	if err := os.MkdirAll(outputPath, 0755); err != nil {
		return 0, "", nil, err
	}

	hash := sha256.New()
	var totalSize int64
	files := make(map[string]string)

	err := filepath.Walk(m.dataDir, func(srcPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip backup directories
		if strings.HasPrefix(srcPath, m.config.BackupDir) {
			return nil
		}
		if strings.HasPrefix(srcPath, m.config.ArchiveDir) {
			return nil
		}

		relPath, err := filepath.Rel(m.dataDir, srcPath)
		if err != nil {
			return err
		}
		dstPath := filepath.Join(outputPath, relPath)

		if info.IsDir() {
			return os.MkdirAll(dstPath, info.Mode())
		}

		// Copy file
		src, err := os.Open(srcPath)
		if err != nil {
			return err
		}
		defer func() { _ = src.Close() }()

		dst, err := os.Create(dstPath)
		if err != nil {
			return err
		}
		defer func() { _ = dst.Close() }()

		fileHash := sha256.New()
		multiWriter := io.MultiWriter(dst, fileHash, hash)

		written, err := io.Copy(multiWriter, src)
		if err != nil {
			return err
		}
		totalSize += written
		files[relPath] = hex.EncodeToString(fileHash.Sum(nil))

		return os.Chmod(dstPath, info.Mode())
	})

	if err != nil {
		return 0, "", nil, err
	}

	return totalSize, hex.EncodeToString(hash.Sum(nil)), files, nil
}

// ListBackups returns all available backups sorted by time (newest first).
func (m *Manager) ListBackups() ([]*BackupMetadata, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entries, err := os.ReadDir(m.config.BackupDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var backups []*BackupMetadata
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".meta.json") {
			continue
		}

		metaPath := filepath.Join(m.config.BackupDir, entry.Name())
		data, err := os.ReadFile(metaPath)
		if err != nil {
			continue
		}

		var meta BackupMetadata
		if err := json.Unmarshal(data, &meta); err != nil {
			continue
		}
		backups = append(backups, &meta)
	}

	// Sort by start time, newest first
	sort.Slice(backups, func(i, j int) bool {
		return backups[i].StartTime.After(backups[j].StartTime)
	})

	return backups, nil
}

// VerifyBackup checks the integrity of a backup.
func (m *Manager) VerifyBackup(backupPath string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Find metadata file
	metaPath := backupPath
	if strings.HasSuffix(backupPath, ".tar.gz") {
		metaPath = strings.TrimSuffix(backupPath, ".tar.gz")
	}
	metaPath += ".meta.json"

	data, err := os.ReadFile(metaPath)
	if err != nil {
		return fmt.Errorf("read metadata: %w", err)
	}

	var meta BackupMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return fmt.Errorf("parse metadata: %w", err)
	}

	// Check backup file exists
	if _, err := os.Stat(backupPath); err != nil {
		return fmt.Errorf("backup not found: %w", err)
	}

	// Verify checksum
	if meta.Compressed {
		return m.verifyCompressedBackup(backupPath, meta.Checksum)
	}
	return m.verifyDirectoryBackup(backupPath, &meta)
}

func (m *Manager) verifyCompressedBackup(path string, expectedChecksum string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		_ = file.Close()
		return err
	}

	actualChecksum := hex.EncodeToString(hash.Sum(nil))
	if actualChecksum != expectedChecksum {
		_ = file.Close()
		return fmt.Errorf("checksum mismatch: expected %s, got %s", expectedChecksum, actualChecksum)
	}

	_ = file.Close()
	return nil
}

func (m *Manager) verifyDirectoryBackup(path string, meta *BackupMetadata) error {
	for relPath, expectedHash := range meta.Files {
		filePath := filepath.Join(path, relPath)
		file, err := os.Open(filePath)
		if err != nil {
			return fmt.Errorf("open %s: %w", relPath, err)
		}

		hash := sha256.New()
		if _, err := io.Copy(hash, file); err != nil {
			_ = file.Close()
			return fmt.Errorf("read %s: %w", relPath, err)
		}
		_ = file.Close()

		actualHash := hex.EncodeToString(hash.Sum(nil))
		if actualHash != expectedHash {
			return fmt.Errorf("file %s: checksum mismatch", relPath)
		}
	}

	return nil
}

// DeleteBackup removes a backup and its metadata.
func (m *Manager) DeleteBackup(backupID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Find backup files
	entries, err := os.ReadDir(m.config.BackupDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if strings.Contains(entry.Name(), backupID) {
			path := filepath.Join(m.config.BackupDir, entry.Name())
			if err := os.RemoveAll(path); err != nil {
				return fmt.Errorf("remove %s: %w", entry.Name(), err)
			}
		}
	}

	return nil
}

// PruneOldBackups removes backups older than the retention period.
func (m *Manager) PruneOldBackups() (int, error) {
	if m.config.RetentionDays <= 0 {
		return 0, nil
	}

	cutoff := time.Now().AddDate(0, 0, -m.config.RetentionDays)
	backups, err := m.ListBackups()
	if err != nil {
		return 0, err
	}

	pruned := 0
	for _, backup := range backups {
		if backup.StartTime.Before(cutoff) {
			if err := m.DeleteBackup(backup.ID); err != nil {
				return pruned, err
			}
			pruned++
		}
	}

	return pruned, nil
}
