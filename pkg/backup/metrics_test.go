package backup

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestMetrics_RecordBackup(t *testing.T) {
	m := &Metrics{}

	m.RecordBackup(5*time.Second, 1024, 12345, nil)

	if m.BackupCount != 1 {
		t.Errorf("Expected BackupCount 1, got %d", m.BackupCount)
	}
	if m.BackupErrorCount != 0 {
		t.Errorf("Expected BackupErrorCount 0, got %d", m.BackupErrorCount)
	}
	if m.LastBackupSize != 1024 {
		t.Errorf("Expected LastBackupSize 1024, got %d", m.LastBackupSize)
	}
	if m.LastBackupLSN != 12345 {
		t.Errorf("Expected LastBackupLSN 12345, got %d", m.LastBackupLSN)
	}
	if m.LastBackupDuration != 5*time.Second {
		t.Errorf("Expected duration 5s, got %v", m.LastBackupDuration)
	}
}

func TestMetrics_RecordBackupError(t *testing.T) {
	m := &Metrics{}

	m.RecordBackup(0, 0, 0, fmt.Errorf("backup failed"))

	if m.BackupCount != 1 {
		t.Errorf("Expected BackupCount 1, got %d", m.BackupCount)
	}
	if m.BackupErrorCount != 1 {
		t.Errorf("Expected BackupErrorCount 1, got %d", m.BackupErrorCount)
	}
}

func TestMetrics_RecordArchive(t *testing.T) {
	m := &Metrics{}

	m.RecordArchive(54321, nil)

	if m.ArchiveCount != 1 {
		t.Errorf("Expected ArchiveCount 1, got %d", m.ArchiveCount)
	}
	if m.LastArchivedLSN != 54321 {
		t.Errorf("Expected LastArchivedLSN 54321, got %d", m.LastArchivedLSN)
	}
}

func TestMetrics_RecordRestore(t *testing.T) {
	m := &Metrics{}

	m.RecordRestore(10*time.Second, nil)

	if m.RestoreCount != 1 {
		t.Errorf("Expected RestoreCount 1, got %d", m.RestoreCount)
	}
	if m.LastRestoreDuration != 10*time.Second {
		t.Errorf("Expected duration 10s, got %v", m.LastRestoreDuration)
	}
}

func TestMetrics_RecordVerify(t *testing.T) {
	m := &Metrics{}

	m.RecordVerify(true, 0)
	if !m.LastVerifySuccess {
		t.Error("Expected LastVerifySuccess true")
	}
	if m.VerifyErrorCount != 0 {
		t.Errorf("Expected VerifyErrorCount 0, got %d", m.VerifyErrorCount)
	}

	m.RecordVerify(false, 2)
	if m.LastVerifySuccess {
		t.Error("Expected LastVerifySuccess false")
	}
	if m.VerifyErrorCount != 1 {
		t.Errorf("Expected VerifyErrorCount 1, got %d", m.VerifyErrorCount)
	}
	if m.ChecksumMismatches != 2 {
		t.Errorf("Expected ChecksumMismatches 2, got %d", m.ChecksumMismatches)
	}
}

func TestMetrics_UpdateArchiveLag(t *testing.T) {
	m := &Metrics{}

	// No lag when archived matches current
	m.UpdateArchiveLag(1000, 1000, time.Minute)
	if m.ArchiveLagSeconds != 0 {
		t.Errorf("Expected 0 lag, got %f", m.ArchiveLagSeconds)
	}

	// Lag when behind
	m.UpdateArchiveLag(3000, 1000, time.Minute)
	if m.ArchiveLagSeconds <= 0 {
		t.Errorf("Expected positive lag, got %f", m.ArchiveLagSeconds)
	}
}

func TestMetrics_Snapshot(t *testing.T) {
	m := &Metrics{}
	m.RecordBackup(1*time.Second, 100, 500, nil)
	m.RecordArchive(600, nil)

	snapshot := m.Snapshot()

	if snapshot.BackupCount != 1 {
		t.Errorf("Snapshot BackupCount mismatch")
	}
	if snapshot.ArchiveCount != 1 {
		t.Errorf("Snapshot ArchiveCount mismatch")
	}
}

func TestMetrics_Reset(t *testing.T) {
	m := &Metrics{}
	m.RecordBackup(1*time.Second, 100, 500, nil)
	m.Reset()

	if m.BackupCount != 0 {
		t.Errorf("Expected BackupCount 0 after reset, got %d", m.BackupCount)
	}
}

func TestMetrics_PrometheusMetrics(t *testing.T) {
	m := &Metrics{}
	m.RecordBackup(2*time.Second, 2048, 1000, nil)
	m.RecordArchive(1500, nil)

	output := m.PrometheusMetrics()

	// Check for expected metric names
	expectedMetrics := []string{
		"veridicaldb_backup_last_timestamp_seconds",
		"veridicaldb_backup_duration_seconds",
		"veridicaldb_backup_size_bytes",
		"veridicaldb_backup_lsn",
		"veridicaldb_backup_total",
		"veridicaldb_backup_errors_total",
		"veridicaldb_archive_lsn",
		"veridicaldb_archive_total",
		"veridicaldb_restore_total",
		"veridicaldb_verify_total",
	}

	for _, metric := range expectedMetrics {
		if !strings.Contains(output, metric) {
			t.Errorf("Missing metric: %s", metric)
		}
	}

	// Check for HELP and TYPE annotations
	if !strings.Contains(output, "# HELP") {
		t.Error("Missing HELP annotations")
	}
	if !strings.Contains(output, "# TYPE") {
		t.Error("Missing TYPE annotations")
	}
}

func TestGlobalMetrics(t *testing.T) {
	// Ensure GlobalMetrics is initialized
	if GlobalMetrics == nil {
		t.Fatal("GlobalMetrics should not be nil")
	}

	// Reset to ensure clean state
	GlobalMetrics.Reset()

	GlobalMetrics.RecordBackup(1*time.Second, 100, 50, nil)
	if GlobalMetrics.BackupCount != 1 {
		t.Error("GlobalMetrics should be usable")
	}

	// Clean up
	GlobalMetrics.Reset()
}
