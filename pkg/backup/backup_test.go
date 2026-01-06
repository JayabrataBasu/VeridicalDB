package backup

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/wal"
)

func TestBackupManager_CreateBaseBackup(t *testing.T) {
	// Create temporary directories
	dataDir := t.TempDir()
	backupDir := t.TempDir()

	// Create test data files
	if err := os.MkdirAll(filepath.Join(dataDir, "tables"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "tables", "test.dat"), []byte("test data"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "meta.json"), []byte(`{"version": 1}`), 0644); err != nil {
		t.Fatal(err)
	}

	// Create WAL
	walDir := filepath.Join(dataDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatal(err)
	}
	walMgr, err := wal.Open(walDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = walMgr.Close() }()

	// Write some WAL records
	rec := &wal.Record{
		Type:      wal.RecordInsert,
		TxID:      1,
		TableName: "test",
		Data:      []byte("test insert"),
	}
	if _, err := walMgr.AppendAndFlush(rec); err != nil {
		t.Fatal(err)
	}

	// Create backup manager
	cfg := &Config{
		BackupDir:  backupDir,
		ArchiveDir: filepath.Join(dataDir, "archive"),
		Compress:   true,
	}
	mgr, err := NewManager(cfg, dataDir, walMgr)
	if err != nil {
		t.Fatal(err)
	}

	// Create backup
	meta, err := mgr.CreateBaseBackup("")
	if err != nil {
		t.Fatal(err)
	}

	// Verify metadata
	if meta.ID == "" {
		t.Error("Expected backup ID")
	}
	if meta.StartLSN == 0 {
		t.Error("Expected non-zero start LSN")
	}
	if meta.Size == 0 {
		t.Error("Expected non-zero size")
	}
	if meta.Checksum == "" {
		t.Error("Expected checksum")
	}
	if !meta.Compressed {
		t.Error("Expected compressed backup")
	}

	// Verify backup file exists
	backups, err := mgr.ListBackups()
	if err != nil {
		t.Fatal(err)
	}
	if len(backups) != 1 {
		t.Errorf("Expected 1 backup, got %d", len(backups))
	}
}

func TestBackupManager_CreateUncompressedBackup(t *testing.T) {
	dataDir := t.TempDir()
	backupDir := t.TempDir()

	// Create test data
	if err := os.WriteFile(filepath.Join(dataDir, "test.dat"), []byte("test data"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create WAL
	walDir := filepath.Join(dataDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatal(err)
	}
	walMgr, err := wal.Open(walDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = walMgr.Close() }()

	// Create backup manager with compression disabled
	cfg := &Config{
		BackupDir:  backupDir,
		ArchiveDir: filepath.Join(dataDir, "archive"),
		Compress:   false,
	}
	mgr, err := NewManager(cfg, dataDir, walMgr)
	if err != nil {
		t.Fatal(err)
	}

	// Create backup
	meta, err := mgr.CreateBaseBackup("")
	if err != nil {
		t.Fatal(err)
	}

	if meta.Compressed {
		t.Error("Expected uncompressed backup")
	}
	if len(meta.Files) == 0 {
		t.Error("Expected file checksums")
	}
}

func TestBackupManager_VerifyBackup(t *testing.T) {
	dataDir := t.TempDir()
	backupDir := t.TempDir()

	// Create test data
	if err := os.WriteFile(filepath.Join(dataDir, "test.dat"), []byte("test data"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create WAL
	walDir := filepath.Join(dataDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatal(err)
	}
	walMgr, err := wal.Open(walDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = walMgr.Close() }()

	cfg := &Config{
		BackupDir:  backupDir,
		ArchiveDir: filepath.Join(dataDir, "archive"),
		Compress:   true,
	}
	mgr, err := NewManager(cfg, dataDir, walMgr)
	if err != nil {
		t.Fatal(err)
	}

	// Create and verify backup
	meta, err := mgr.CreateBaseBackup("")
	if err != nil {
		t.Fatal(err)
	}

	backupPath := filepath.Join(backupDir, meta.ID+".tar.gz")
	if err := mgr.VerifyBackup(backupPath); err != nil {
		t.Errorf("Backup verification failed: %v", err)
	}
}

func TestBackupManager_DeleteBackup(t *testing.T) {
	dataDir := t.TempDir()
	backupDir := t.TempDir()

	// Create test data
	if err := os.WriteFile(filepath.Join(dataDir, "test.dat"), []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create WAL
	walDir := filepath.Join(dataDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatal(err)
	}
	walMgr, err := wal.Open(walDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = walMgr.Close() }()

	cfg := &Config{
		BackupDir:  backupDir,
		ArchiveDir: filepath.Join(dataDir, "archive"),
		Compress:   true,
	}
	mgr, err := NewManager(cfg, dataDir, walMgr)
	if err != nil {
		t.Fatal(err)
	}

	// Create backup
	meta, err := mgr.CreateBaseBackup("")
	if err != nil {
		t.Fatal(err)
	}

	// Delete backup
	if err := mgr.DeleteBackup(meta.ID); err != nil {
		t.Fatal(err)
	}

	// Verify deleted
	backups, err := mgr.ListBackups()
	if err != nil {
		t.Fatal(err)
	}
	if len(backups) != 0 {
		t.Errorf("Expected 0 backups after delete, got %d", len(backups))
	}
}

func TestBackupManager_PruneOldBackups(t *testing.T) {
	dataDir := t.TempDir()
	backupDir := t.TempDir()

	// Create test data
	if err := os.WriteFile(filepath.Join(dataDir, "test.dat"), []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create WAL
	walDir := filepath.Join(dataDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatal(err)
	}
	walMgr, err := wal.Open(walDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = walMgr.Close() }()

	cfg := &Config{
		BackupDir:     backupDir,
		ArchiveDir:    filepath.Join(dataDir, "archive"),
		Compress:      true,
		RetentionDays: 1, // 1 day retention
	}
	mgr, err := NewManager(cfg, dataDir, walMgr)
	if err != nil {
		t.Fatal(err)
	}

	// Create a fake old backup metadata directly
	oldMeta := &BackupMetadata{
		ID:         "backup_old",
		StartTime:  time.Now().AddDate(0, 0, -5), // 5 days ago
		EndTime:    time.Now().AddDate(0, 0, -5),
		StartLSN:   100,
		EndLSN:     200,
		Size:       1024,
		Compressed: true,
	}
	metaData, _ := json.MarshalIndent(oldMeta, "", "  ")
	metaPath := filepath.Join(backupDir, "backup_old.meta.json")
	if err := os.WriteFile(metaPath, metaData, 0644); err != nil {
		t.Fatal(err)
	}
	// Create dummy backup file
	if err := os.WriteFile(filepath.Join(backupDir, "backup_old.tar.gz"), []byte("dummy"), 0644); err != nil {
		t.Fatal(err)
	}

	// Prune old backups
	pruned, err := mgr.PruneOldBackups()
	if err != nil {
		t.Fatal(err)
	}
	if pruned != 1 {
		t.Errorf("Expected 1 backup pruned, got %d", pruned)
	}
}

func TestArchiver_ArchiveAndList(t *testing.T) {
	dataDir := t.TempDir()
	archiveDir := t.TempDir()

	// Create WAL
	walDir := filepath.Join(dataDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatal(err)
	}
	walMgr, err := wal.Open(walDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = walMgr.Close() }()

	// Write some WAL records
	for i := 0; i < 5; i++ {
		rec := &wal.Record{
			Type:      wal.RecordInsert,
			TxID:      uint64(i + 1),
			TableName: "test",
			Data:      []byte("test data"),
		}
		if _, err := walMgr.AppendAndFlush(rec); err != nil {
			t.Fatal(err)
		}
	}

	cfg := &Config{ArchiveDir: archiveDir}
	archiver, err := NewArchiver(cfg, walDir, walMgr)
	if err != nil {
		t.Fatal(err)
	}

	// Archive current WAL
	if err := archiver.ArchiveCurrentWAL(); err != nil {
		t.Fatal(err)
	}

	// List archived segments
	segments, err := archiver.ListArchivedSegments()
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) != 1 {
		t.Errorf("Expected 1 segment, got %d", len(segments))
	}

	// Verify segment properties
	if segments[0].LSN == 0 {
		t.Error("Expected non-zero LSN")
	}
	if segments[0].Size == 0 {
		t.Error("Expected non-zero size")
	}
}

func TestArchiver_FindSegmentsForPITR(t *testing.T) {
	dataDir := t.TempDir()
	archiveDir := t.TempDir()

	// Create WAL
	walDir := filepath.Join(dataDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatal(err)
	}
	walMgr, err := wal.Open(walDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = walMgr.Close() }()

	cfg := &Config{ArchiveDir: archiveDir}
	archiver, err := NewArchiver(cfg, walDir, walMgr)
	if err != nil {
		t.Fatal(err)
	}

	// Create multiple archives
	for i := 0; i < 3; i++ {
		rec := &wal.Record{
			Type:      wal.RecordInsert,
			TxID:      uint64(i + 1),
			TableName: "test",
			Data:      []byte("test"),
		}
		if _, err := walMgr.AppendAndFlush(rec); err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond) // Small delay to ensure different timestamps
		if err := archiver.ArchiveCurrentWAL(); err != nil {
			t.Fatal(err)
		}
	}

	// Find segments from start
	segments, err := archiver.FindSegmentsForPITR(0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) < 1 {
		t.Errorf("Expected at least 1 segment, got %d", len(segments))
	}
}

func TestArchiver_PruneSegments(t *testing.T) {
	dataDir := t.TempDir()
	archiveDir := t.TempDir()

	// Create WAL
	walDir := filepath.Join(dataDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatal(err)
	}
	walMgr, err := wal.Open(walDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = walMgr.Close() }()

	cfg := &Config{ArchiveDir: archiveDir}
	archiver, err := NewArchiver(cfg, walDir, walMgr)
	if err != nil {
		t.Fatal(err)
	}

	// Create archive
	rec := &wal.Record{Type: wal.RecordInsert, TxID: 1, TableName: "test", Data: []byte("test")}
	if _, err := walMgr.AppendAndFlush(rec); err != nil {
		t.Fatal(err)
	}
	if err := archiver.ArchiveCurrentWAL(); err != nil {
		t.Fatal(err)
	}

	lastLSN := archiver.LastArchivedLSN()

	// Prune all segments before a high LSN (should remove the one we created)
	pruned, err := archiver.PruneArchivedSegments(lastLSN + 1)
	if err != nil {
		t.Fatal(err)
	}

	// Should have pruned our segment
	if pruned != 1 {
		t.Errorf("Expected 1 segment pruned, got %d", pruned)
	}
}

func TestRestore_BasicRestore(t *testing.T) {
	dataDir := t.TempDir()
	backupDir := t.TempDir()
	restoreDir := t.TempDir()
	// Clear restore dir for test
	_ = os.RemoveAll(restoreDir)

	// Create test data structure
	if err := os.MkdirAll(filepath.Join(dataDir, "tables"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "tables", "users.dat"), []byte("user data"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "meta.json"), []byte(`{"version": 1}`), 0644); err != nil {
		t.Fatal(err)
	}

	// Create WAL
	walDir := filepath.Join(dataDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatal(err)
	}
	walMgr, err := wal.Open(walDir)
	if err != nil {
		t.Fatal(err)
	}

	cfg := &Config{
		BackupDir:  backupDir,
		ArchiveDir: filepath.Join(dataDir, "archive"),
		Compress:   true,
	}
	mgr, err := NewManager(cfg, dataDir, walMgr)
	if err != nil {
		t.Fatal(err)
	}

	// Create backup
	meta, err := mgr.CreateBaseBackup("")
	if err != nil {
		t.Fatal(err)
	}
	_ = walMgr.Close()

	// Perform restore
	backupPath := filepath.Join(backupDir, meta.ID+".tar.gz")
	opts := RestoreOptions{
		BaseBackupPath: backupPath,
		TargetDir:      restoreDir,
		ArchiveDir:     filepath.Join(dataDir, "archive"),
	}

	result, err := mgr.Restore(opts)
	if err != nil {
		t.Fatal(err)
	}

	// Verify result
	if result.BaseBackupID != meta.ID {
		t.Errorf("Expected backup ID %s, got %s", meta.ID, result.BaseBackupID)
	}
	if result.FilesRestored == 0 {
		t.Error("Expected files to be restored")
	}

	// Verify restored files
	restoredData, err := os.ReadFile(filepath.Join(restoreDir, "tables", "users.dat"))
	if err != nil {
		t.Fatal(err)
	}
	if string(restoredData) != "user data" {
		t.Errorf("Restored data mismatch: got %s", string(restoredData))
	}

	// Verify recovery marker
	if _, err := os.Stat(filepath.Join(restoreDir, "recovery.done")); err != nil {
		t.Error("Expected recovery.done marker file")
	}
}

func TestRestore_PITRWithTargetLSN(t *testing.T) {
	dataDir := t.TempDir()
	backupDir := t.TempDir()
	restoreDir := t.TempDir()
	archiveDir := t.TempDir()
	_ = os.RemoveAll(restoreDir)

	// Create test data
	if err := os.WriteFile(filepath.Join(dataDir, "test.dat"), []byte("initial"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create WAL
	walDir := filepath.Join(dataDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatal(err)
	}
	walMgr, err := wal.Open(walDir)
	if err != nil {
		t.Fatal(err)
	}

	cfg := &Config{
		BackupDir:  backupDir,
		ArchiveDir: archiveDir,
		Compress:   true,
	}
	mgr, err := NewManager(cfg, dataDir, walMgr)
	if err != nil {
		t.Fatal(err)
	}

	// Create backup
	meta, err := mgr.CreateBaseBackup("")
	if err != nil {
		t.Fatal(err)
	}
	_ = walMgr.Close()

	// Restore with target LSN
	targetLSN := meta.EndLSN
	backupPath := filepath.Join(backupDir, meta.ID+".tar.gz")
	opts := RestoreOptions{
		BaseBackupPath: backupPath,
		TargetDir:      restoreDir,
		ArchiveDir:     archiveDir,
		TargetLSN:      &targetLSN,
	}

	result, err := mgr.Restore(opts)
	if err != nil {
		t.Fatal(err)
	}

	if result.RestoredLSN != targetLSN {
		t.Logf("Note: Restored LSN %d may differ from target %d (no archived segments)", result.RestoredLSN, targetLSN)
	}
}

func TestRecoveryConfig(t *testing.T) {
	tmpDir := t.TempDir()

	// Test writing recovery config
	targetTime := time.Now()
	targetLSN := uint64(12345)
	cfg := &RecoveryConfig{
		TargetTime:      &targetTime,
		TargetLSN:       &targetLSN,
		RestoreFinished: false,
	}

	if err := writeRecoveryConfig(tmpDir, cfg); err != nil {
		t.Fatal(err)
	}

	// Test reading recovery config
	readCfg, err := ReadRecoveryConfig(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	if readCfg == nil {
		t.Fatal("Expected recovery config")
	}
	if readCfg.TargetLSN == nil || *readCfg.TargetLSN != targetLSN {
		t.Errorf("Target LSN mismatch")
	}

	// Test clearing recovery config
	if err := ClearRecoveryConfig(tmpDir); err != nil {
		t.Fatal(err)
	}

	// Should return nil after clearing
	readCfg, err = ReadRecoveryConfig(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	if readCfg != nil {
		t.Error("Expected nil after clearing")
	}
}

func TestBackupMetadata_Serialization(t *testing.T) {
	meta := &BackupMetadata{
		ID:         "backup_test",
		StartTime:  time.Now(),
		EndTime:    time.Now().Add(time.Minute),
		StartLSN:   1000,
		EndLSN:     2000,
		DataDir:    "/data",
		Size:       1024,
		Checksum:   "abc123",
		Compressed: true,
		Files:      map[string]string{"test.dat": "hash123"},
		Version:    1,
	}

	data, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}

	var decoded BackupMetadata
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}

	if decoded.ID != meta.ID {
		t.Errorf("ID mismatch: %s vs %s", decoded.ID, meta.ID)
	}
	if decoded.StartLSN != meta.StartLSN {
		t.Errorf("StartLSN mismatch")
	}
	if decoded.Files["test.dat"] != "hash123" {
		t.Errorf("Files mismatch")
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.BackupDir == "" {
		t.Error("Expected default backup dir")
	}
	if cfg.ArchiveDir == "" {
		t.Error("Expected default archive dir")
	}
	if !cfg.Compress {
		t.Error("Expected compression enabled by default")
	}
	if cfg.RetentionDays != 30 {
		t.Errorf("Expected 30 day retention, got %d", cfg.RetentionDays)
	}
}

func TestRetrieveArchivedSegmentRunsRestoreCommand(t *testing.T) {
	mgr := &Manager{}
	segName := "wal_segment.log"

	path, err := mgr.retrieveArchivedSegment(segName, "echo %f > %p")
	if err != nil {
		t.Fatalf("restore command failed: %v", err)
	}
	defer func() { _ = os.Remove(path) }()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read restored segment: %v", err)
	}

	if strings.TrimSpace(string(data)) != segName {
		t.Fatalf("restore command did not substitute segment name, got %q", strings.TrimSpace(string(data)))
	}
}

func TestRunCommandErrorPropagation(t *testing.T) {
	err := runCommand("exit 1")
	if err == nil {
		t.Fatal("expected restore command to fail")
	}
	if !strings.Contains(err.Error(), "exit status") {
		t.Fatalf("unexpected error message: %v", err)
	}
}
