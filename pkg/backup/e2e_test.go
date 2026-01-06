package backup

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/wal"
)

// TestE2E_BackupArchiveRestore tests the full backup → archive → restore flow.
func TestE2E_BackupArchiveRestore(t *testing.T) {
	// Setup directories
	dataDir := t.TempDir()
	backupDir := t.TempDir()
	archiveDir := t.TempDir()
	restoreDir := t.TempDir()

	// Clear restore dir (TempDir creates it)
	_ = os.RemoveAll(restoreDir)

	// Create test data structure
	tablesDir := filepath.Join(dataDir, "tables")
	if err := os.MkdirAll(tablesDir, 0755); err != nil {
		t.Fatalf("Failed to create tables dir: %v", err)
	}

	// Write initial data
	initialData := []byte("initial table data - row 1\ninitial table data - row 2\n")
	if err := os.WriteFile(filepath.Join(tablesDir, "users.dat"), initialData, 0644); err != nil {
		t.Fatalf("Failed to write initial data: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "meta.json"), []byte(`{"version": 1}`), 0644); err != nil {
		t.Fatalf("Failed to write meta: %v", err)
	}

	// Setup WAL
	walDir := filepath.Join(dataDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatalf("Failed to create WAL dir: %v", err)
	}
	walMgr, err := wal.Open(walDir)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	// Write some initial WAL records
	for i := 0; i < 3; i++ {
		rec := &wal.Record{
			Type:      wal.RecordInsert,
			TxID:      uint64(i + 1),
			TableName: "users",
			Data:      []byte("initial insert"),
		}
		if _, err := walMgr.AppendAndFlush(rec); err != nil {
			t.Fatalf("Failed to write WAL: %v", err)
		}
	}

	// Create backup manager
	cfg := &Config{
		BackupDir:  backupDir,
		ArchiveDir: archiveDir,
		Compress:   true,
	}
	mgr, err := NewManager(cfg, dataDir, walMgr)
	if err != nil {
		t.Fatalf("Failed to create backup manager: %v", err)
	}

	// STEP 1: Create base backup
	t.Log("Step 1: Creating base backup...")
	meta, err := mgr.CreateBaseBackup("")
	if err != nil {
		t.Fatalf("Failed to create base backup: %v", err)
	}
	t.Logf("  Backup ID: %s, StartLSN: %d, EndLSN: %d", meta.ID, meta.StartLSN, meta.EndLSN)

	backupStartLSN := meta.StartLSN

	// STEP 2: Write more data (simulating ongoing transactions after backup)
	t.Log("Step 2: Writing additional WAL records after backup...")
	for i := 0; i < 5; i++ {
		rec := &wal.Record{
			Type:      wal.RecordInsert,
			TxID:      uint64(100 + i),
			TableName: "users",
			Data:      []byte("post-backup insert"),
		}
		if _, err := walMgr.AppendAndFlush(rec); err != nil {
			t.Fatalf("Failed to write post-backup WAL: %v", err)
		}
	}

	// Update data file (simulating changes)
	updatedData := append(initialData, []byte("post-backup row 3\npost-backup row 4\n")...)
	if err := os.WriteFile(filepath.Join(tablesDir, "users.dat"), updatedData, 0644); err != nil {
		t.Fatalf("Failed to update data: %v", err)
	}

	// STEP 3: Archive WAL
	t.Log("Step 3: Archiving WAL...")
	archiver, err := NewArchiver(cfg, walDir, walMgr)
	if err != nil {
		t.Fatalf("Failed to create archiver: %v", err)
	}

	if err := archiver.ArchiveCurrentWAL(); err != nil {
		t.Fatalf("Failed to archive WAL: %v", err)
	}
	archivedLSN := archiver.LastArchivedLSN()
	t.Logf("  Archived LSN: %d", archivedLSN)

	// List archived segments
	segments, err := archiver.ListArchivedSegments()
	if err != nil {
		t.Fatalf("Failed to list archived segments: %v", err)
	}
	t.Logf("  Archived segments: %d", len(segments))

	// Close WAL before restore
	_ = walMgr.Close()

	// STEP 4: Restore from backup
	t.Log("Step 4: Restoring from backup...")
	backupPath := filepath.Join(backupDir, meta.ID+".tar.gz")

	restoreOpts := RestoreOptions{
		BaseBackupPath: backupPath,
		TargetDir:      restoreDir,
		ArchiveDir:     archiveDir,
	}

	result, err := mgr.Restore(restoreOpts)
	if err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	t.Logf("  Files restored: %d", result.FilesRestored)
	t.Logf("  WAL segments applied: %d", result.WALSegmentsApplied)
	t.Logf("  Restored LSN: %d", result.RestoredLSN)

	// STEP 5: Verify restored data
	t.Log("Step 5: Verifying restored data...")

	// Check that data file exists
	restoredDataPath := filepath.Join(restoreDir, "tables", "users.dat")
	restoredData, err := os.ReadFile(restoredDataPath)
	if err != nil {
		t.Fatalf("Failed to read restored data: %v", err)
	}

	// Should have the initial data (from backup)
	if len(restoredData) == 0 {
		t.Error("Restored data is empty")
	}
	t.Logf("  Restored data size: %d bytes", len(restoredData))

	// Check meta.json
	restoredMeta, err := os.ReadFile(filepath.Join(restoreDir, "meta.json"))
	if err != nil {
		t.Fatalf("Failed to read restored meta: %v", err)
	}
	if string(restoredMeta) != `{"version": 1}` {
		t.Errorf("Meta mismatch: got %q", string(restoredMeta))
	}

	// Check recovery marker
	recoveryMarker := filepath.Join(restoreDir, "recovery.done")
	if _, err := os.Stat(recoveryMarker); os.IsNotExist(err) {
		t.Error("Expected recovery.done marker file")
	}

	// STEP 6: Verify backup
	t.Log("Step 6: Verifying backup integrity...")
	if err := mgr.VerifyBackup(backupPath); err != nil {
		t.Errorf("Backup verification failed: %v", err)
	}

	// Summary
	t.Log("E2E test completed successfully!")
	t.Logf("  Backup start LSN: %d", backupStartLSN)
	t.Logf("  Final archived LSN: %d", archivedLSN)
	t.Logf("  Restored LSN: %d", result.RestoredLSN)
}

// TestE2E_PITRWithTargetLSN tests point-in-time recovery to a specific LSN.
func TestE2E_PITRWithTargetLSN(t *testing.T) {
	dataDir := t.TempDir()
	backupDir := t.TempDir()
	archiveDir := t.TempDir()
	restoreDir := t.TempDir()
	_ = os.RemoveAll(restoreDir)

	// Create minimal data
	if err := os.WriteFile(filepath.Join(dataDir, "data.txt"), []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	// Setup WAL
	walDir := filepath.Join(dataDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatal(err)
	}
	walMgr, err := wal.Open(walDir)
	if err != nil {
		t.Fatal(err)
	}

	// Write WAL records
	for i := 0; i < 3; i++ {
		rec := &wal.Record{
			Type:      wal.RecordInsert,
			TxID:      uint64(i + 1),
			TableName: "test",
			Data:      []byte("data"),
		}
		if _, err := walMgr.AppendAndFlush(rec); err != nil {
			t.Fatal(err)
		}
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

	// Archive WAL
	archiver, err := NewArchiver(cfg, walDir, walMgr)
	if err != nil {
		t.Fatal(err)
	}
	if err := archiver.ArchiveCurrentWAL(); err != nil {
		t.Fatal(err)
	}

	_ = walMgr.Close()

	// Restore with target LSN
	targetLSN := meta.EndLSN
	backupPath := filepath.Join(backupDir, meta.ID+".tar.gz")

	result, err := mgr.Restore(RestoreOptions{
		BaseBackupPath: backupPath,
		TargetDir:      restoreDir,
		ArchiveDir:     archiveDir,
		TargetLSN:      &targetLSN,
	})
	if err != nil {
		t.Fatalf("PITR restore failed: %v", err)
	}

	t.Logf("PITR to LSN %d completed, restored LSN: %d", targetLSN, result.RestoredLSN)
}

// TestE2E_PITRWithTargetTime tests point-in-time recovery to a specific time.
func TestE2E_PITRWithTargetTime(t *testing.T) {
	dataDir := t.TempDir()
	backupDir := t.TempDir()
	archiveDir := t.TempDir()
	restoreDir := t.TempDir()
	_ = os.RemoveAll(restoreDir)

	// Create minimal data
	if err := os.WriteFile(filepath.Join(dataDir, "data.txt"), []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	// Setup WAL
	walDir := filepath.Join(dataDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatal(err)
	}
	walMgr, err := wal.Open(walDir)
	if err != nil {
		t.Fatal(err)
	}

	// Write WAL records
	for i := 0; i < 3; i++ {
		rec := &wal.Record{
			Type:      wal.RecordInsert,
			TxID:      uint64(i + 1),
			TableName: "test",
			Data:      []byte("data"),
		}
		if _, err := walMgr.AppendAndFlush(rec); err != nil {
			t.Fatal(err)
		}
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

	// Archive WAL
	archiver, err := NewArchiver(cfg, walDir, walMgr)
	if err != nil {
		t.Fatal(err)
	}
	if err := archiver.ArchiveCurrentWAL(); err != nil {
		t.Fatal(err)
	}

	_ = walMgr.Close()

	// Restore with target time (future time to include all archives)
	targetTime := time.Now().Add(time.Hour)
	backupPath := filepath.Join(backupDir, meta.ID+".tar.gz")

	result, err := mgr.Restore(RestoreOptions{
		BaseBackupPath: backupPath,
		TargetDir:      restoreDir,
		ArchiveDir:     archiveDir,
		TargetTime:     &targetTime,
	})
	if err != nil {
		t.Fatalf("PITR restore failed: %v", err)
	}

	t.Logf("PITR to time %v completed, restored LSN: %d", targetTime, result.RestoredLSN)
}

// TestE2E_MultipleArchivesRestore tests restoring with multiple WAL archives.
func TestE2E_MultipleArchivesRestore(t *testing.T) {
	dataDir := t.TempDir()
	backupDir := t.TempDir()
	archiveDir := t.TempDir()
	restoreDir := t.TempDir()
	_ = os.RemoveAll(restoreDir)

	// Create minimal data
	if err := os.WriteFile(filepath.Join(dataDir, "data.txt"), []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	// Setup WAL
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

	archiver, err := NewArchiver(cfg, walDir, walMgr)
	if err != nil {
		t.Fatal(err)
	}

	// Write and archive multiple times
	for batch := 0; batch < 3; batch++ {
		for i := 0; i < 2; i++ {
			rec := &wal.Record{
				Type:      wal.RecordInsert,
				TxID:      uint64(batch*10 + i + 1),
				TableName: "test",
				Data:      []byte("batch data"),
			}
			if _, err := walMgr.AppendAndFlush(rec); err != nil {
				t.Fatal(err)
			}
		}

		// Archive after each batch (small delay to ensure different timestamps)
		time.Sleep(10 * time.Millisecond)
		if err := archiver.ArchiveCurrentWAL(); err != nil {
			t.Fatal(err)
		}
	}

	// Create backup after all writes
	meta, err := mgr.CreateBaseBackup("")
	if err != nil {
		t.Fatal(err)
	}

	_ = walMgr.Close()

	// Verify multiple archives exist
	segments, err := archiver.ListArchivedSegments()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Created %d WAL archives", len(segments))

	// Restore
	backupPath := filepath.Join(backupDir, meta.ID+".tar.gz")
	result, err := mgr.Restore(RestoreOptions{
		BaseBackupPath: backupPath,
		TargetDir:      restoreDir,
		ArchiveDir:     archiveDir,
	})
	if err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	t.Logf("Restored with %d WAL segments applied", result.WALSegmentsApplied)
}
