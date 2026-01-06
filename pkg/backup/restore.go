package backup

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/wal"
)

// RestoreOptions configures a restore operation.
type RestoreOptions struct {
	// BaseBackupPath is the path to the base backup
	BaseBackupPath string

	// TargetDir is where to restore the database
	TargetDir string

	// TargetTime for point-in-time recovery (optional)
	TargetTime *time.Time

	// TargetLSN for point-in-time recovery (optional)
	TargetLSN *uint64

	// ArchiveDir is where archived WAL segments are stored
	ArchiveDir string

	// RestoreCommand is used to retrieve archived WAL (optional)
	RestoreCommand string
}

// RestoreResult contains information about a completed restore.
type RestoreResult struct {
	// BaseBackupID is the ID of the base backup used
	BaseBackupID string

	// StartTime is when the restore started
	StartTime time.Time

	// EndTime is when the restore completed
	EndTime time.Time

	// RestoredLSN is the final WAL position after restore
	RestoredLSN uint64

	// RestoredTime is the timestamp of the restored state
	RestoredTime time.Time

	// FilesRestored is the number of files restored
	FilesRestored int

	// WALSegmentsApplied is the number of WAL segments replayed
	WALSegmentsApplied int
}

// Restore performs a point-in-time recovery.
func (m *Manager) Restore(opts RestoreOptions) (*RestoreResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	startTime := time.Now()
	result := &RestoreResult{
		StartTime: startTime,
	}

	// Step 1: Load and verify base backup
	meta, err := loadBackupMetadata(opts.BaseBackupPath)
	if err != nil {
		return nil, fmt.Errorf("load backup metadata: %w", err)
	}
	result.BaseBackupID = meta.ID

	// Step 2: Extract base backup to target directory
	filesRestored, err := m.extractBackup(opts.BaseBackupPath, opts.TargetDir, meta)
	if err != nil {
		return nil, fmt.Errorf("extract backup: %w", err)
	}
	result.FilesRestored = filesRestored

	// Step 3: If PITR is requested, replay WAL
	if opts.TargetTime != nil || opts.TargetLSN != nil {
		segmentsApplied, finalLSN, err := m.replayWALForPITR(opts, meta)
		if err != nil {
			return nil, fmt.Errorf("replay wal: %w", err)
		}
		result.WALSegmentsApplied = segmentsApplied
		result.RestoredLSN = finalLSN
	} else {
		result.RestoredLSN = meta.EndLSN
	}

	// Step 4: Write recovery marker
	if err := m.writeRecoveryMarker(opts.TargetDir, result); err != nil {
		return nil, fmt.Errorf("write recovery marker: %w", err)
	}

	result.EndTime = time.Now()
	result.RestoredTime = time.Now()

	return result, nil
}

func loadBackupMetadata(backupPath string) (*BackupMetadata, error) {
	metaPath := backupPath
	if strings.HasSuffix(backupPath, ".tar.gz") {
		metaPath = strings.TrimSuffix(backupPath, ".tar.gz")
	}
	metaPath += ".meta.json"

	data, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, err
	}

	var meta BackupMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}

	return &meta, nil
}

func (m *Manager) extractBackup(backupPath, targetDir string, meta *BackupMetadata) (int, error) {
	// Ensure target directory is empty or doesn't exist
	if _, err := os.Stat(targetDir); err == nil {
		// Directory exists, check if empty
		entries, err := os.ReadDir(targetDir)
		if err != nil {
			return 0, err
		}
		if len(entries) > 0 {
			return 0, fmt.Errorf("target directory is not empty: %s", targetDir)
		}
	}

	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return 0, err
	}

	if meta.Compressed {
		return m.extractCompressedBackup(backupPath, targetDir)
	}
	return m.extractDirectoryBackup(backupPath, targetDir)
}

func (m *Manager) extractCompressedBackup(backupPath, targetDir string) (int, error) {
	file, err := os.Open(backupPath)
	if err != nil {
		return 0, err
	}
	defer func() { _ = file.Close() }()

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return 0, err
	}
	defer func() { _ = gzReader.Close() }()

	tarReader := tar.NewReader(gzReader)
	filesRestored := 0

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return filesRestored, err
		}

		targetPath := filepath.Join(targetDir, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(targetPath, os.FileMode(header.Mode)); err != nil {
				return filesRestored, err
			}
		case tar.TypeReg:
			// Ensure parent directory exists
			parentDir := filepath.Dir(targetPath)
			if err := os.MkdirAll(parentDir, 0755); err != nil {
				return filesRestored, err
			}

			outFile, err := os.Create(targetPath)
			if err != nil {
				return filesRestored, err
			}

			if _, err := io.Copy(outFile, tarReader); err != nil {
				_ = outFile.Close()
				return filesRestored, err
			}
			_ = outFile.Close()

			if err := os.Chmod(targetPath, os.FileMode(header.Mode)); err != nil {
				return filesRestored, err
			}
			filesRestored++
		}
	}

	return filesRestored, nil
}

func (m *Manager) extractDirectoryBackup(backupPath, targetDir string) (int, error) {
	filesRestored := 0

	err := filepath.Walk(backupPath, func(srcPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(backupPath, srcPath)
		if err != nil {
			return err
		}
		dstPath := filepath.Join(targetDir, relPath)

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

		if _, err := io.Copy(dst, src); err != nil {
			return err
		}

		filesRestored++
		return os.Chmod(dstPath, info.Mode())
	})

	return filesRestored, err
}

func (m *Manager) replayWALForPITR(opts RestoreOptions, meta *BackupMetadata) (int, uint64, error) {
	archiveDir := opts.ArchiveDir
	if archiveDir == "" {
		archiveDir = m.config.ArchiveDir
	}

	// Find WAL segments needed for recovery
	archiver := &Archiver{config: &Config{ArchiveDir: archiveDir}}
	segments, err := archiver.FindSegmentsForPITR(meta.StartLSN, opts.TargetTime, opts.TargetLSN)
	if err != nil {
		return 0, 0, err
	}

	if len(segments) == 0 {
		return 0, meta.EndLSN, nil
	}

	// Copy WAL segments to target WAL directory
	walDir := filepath.Join(opts.TargetDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return 0, 0, err
	}

	var lastLSN uint64
	appliedCount := 0

	for _, seg := range segments {
		// Copy segment to recovery WAL directory
		srcPath := seg.Path
		if opts.RestoreCommand != "" {
			// Use restore command to retrieve archived segment
			srcPath, err = m.retrieveArchivedSegment(seg.Name, opts.RestoreCommand)
			if err != nil {
				return appliedCount, lastLSN, fmt.Errorf("retrieve %s: %w", seg.Name, err)
			}
		}

		dstPath := filepath.Join(walDir, "wal.log")
		if err := copyFile(srcPath, dstPath); err != nil {
			return appliedCount, lastLSN, fmt.Errorf("copy %s: %w", seg.Name, err)
		}

		// Open WAL for replay
		walMgr, err := wal.Open(walDir)
		if err != nil {
			return appliedCount, lastLSN, fmt.Errorf("open wal: %w", err)
		}

		// Check if we should stop based on target
		if opts.TargetLSN != nil && seg.LSN >= *opts.TargetLSN {
			_ = walMgr.Close()
			break
		}
		if opts.TargetTime != nil && seg.Timestamp.After(*opts.TargetTime) {
			_ = walMgr.Close()
			break
		}

		lastLSN = seg.LSN
		appliedCount++
		_ = walMgr.Close()
	}

	// Write recovery configuration
	recoveryConf := RecoveryConfig{
		TargetTime:      opts.TargetTime,
		TargetLSN:       opts.TargetLSN,
		RestoreFinished: true,
	}
	if err := writeRecoveryConfig(opts.TargetDir, &recoveryConf); err != nil {
		return appliedCount, lastLSN, err
	}

	return appliedCount, lastLSN, nil
}

func (m *Manager) retrieveArchivedSegment(segName, restoreCmd string) (string, error) {
	tmpDir := os.TempDir()
	dstPath := filepath.Join(tmpDir, segName)

	cmd := restoreCmd
	cmd = strings.ReplaceAll(cmd, "%f", segName)
	cmd = strings.ReplaceAll(cmd, "%p", dstPath)

	if err := runCommand(cmd); err != nil {
		return "", err
	}

	return dstPath, nil
}

func runCommand(cmd string) error {
	return fmt.Errorf("restore command execution not implemented: %s", cmd)
}

func (m *Manager) writeRecoveryMarker(targetDir string, result *RestoreResult) error {
	markerPath := filepath.Join(targetDir, "recovery.done")

	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(markerPath, data, 0644)
}

// RecoveryConfig stores recovery settings in the data directory.
type RecoveryConfig struct {
	TargetTime      *time.Time `json:"target_time,omitempty"`
	TargetLSN       *uint64    `json:"target_lsn,omitempty"`
	RestoreFinished bool       `json:"restore_finished"`
}

func writeRecoveryConfig(targetDir string, cfg *RecoveryConfig) error {
	path := filepath.Join(targetDir, "recovery.conf")
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

// ReadRecoveryConfig reads recovery configuration from a data directory.
func ReadRecoveryConfig(dataDir string) (*RecoveryConfig, error) {
	path := filepath.Join(dataDir, "recovery.conf")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var cfg RecoveryConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// ClearRecoveryConfig removes the recovery configuration after successful recovery.
func ClearRecoveryConfig(dataDir string) error {
	path := filepath.Join(dataDir, "recovery.conf")
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
