// Package backup provides metrics for backup and archiving operations.
package backup

import (
	"fmt"
	"sync"
	"time"
)

// Metrics holds backup-related metrics for monitoring.
type Metrics struct {
	mu sync.RWMutex

	// Backup metrics
	LastBackupTime     time.Time
	LastBackupDuration time.Duration
	LastBackupSize     int64
	LastBackupLSN      uint64
	BackupCount        int64
	BackupErrorCount   int64

	// Archive metrics
	LastArchiveTime     time.Time
	LastArchivedLSN     uint64
	ArchiveCount        int64
	ArchiveErrorCount   int64
	ArchiveLagSeconds   float64
	PendingArchiveBytes int64

	// Restore metrics
	LastRestoreTime     time.Time
	LastRestoreDuration time.Duration
	RestoreCount        int64
	RestoreErrorCount   int64

	// Verification metrics
	LastVerifyTime     time.Time
	VerifyCount        int64
	VerifyErrorCount   int64
	LastVerifySuccess  bool
	ChecksumMismatches int64
}

// GlobalMetrics is the default metrics instance.
var GlobalMetrics = &Metrics{}

// RecordBackup records a backup operation.
func (m *Metrics) RecordBackup(duration time.Duration, size int64, lsn uint64, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.LastBackupTime = time.Now()
	m.LastBackupDuration = duration
	m.LastBackupSize = size
	m.LastBackupLSN = lsn
	m.BackupCount++

	if err != nil {
		m.BackupErrorCount++
	}
}

// RecordArchive records an archive operation.
func (m *Metrics) RecordArchive(lsn uint64, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.LastArchiveTime = time.Now()
	m.LastArchivedLSN = lsn
	m.ArchiveCount++

	if err != nil {
		m.ArchiveErrorCount++
	}
}

// RecordRestore records a restore operation.
func (m *Metrics) RecordRestore(duration time.Duration, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.LastRestoreTime = time.Now()
	m.LastRestoreDuration = duration
	m.RestoreCount++

	if err != nil {
		m.RestoreErrorCount++
	}
}

// RecordVerify records a verification operation.
func (m *Metrics) RecordVerify(success bool, checksumMismatches int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.LastVerifyTime = time.Now()
	m.VerifyCount++
	m.LastVerifySuccess = success

	if !success {
		m.VerifyErrorCount++
	}
	m.ChecksumMismatches += checksumMismatches
}

// UpdateArchiveLag updates the archive lag metric.
func (m *Metrics) UpdateArchiveLag(currentLSN, archivedLSN uint64, archiveInterval time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if archivedLSN > 0 && currentLSN > archivedLSN {
		// Estimate lag based on LSN difference and archive interval
		lsnDiff := currentLSN - archivedLSN
		// Rough estimate: assume each archive covers ~1000 LSN units
		estimatedIntervals := float64(lsnDiff) / 1000.0
		m.ArchiveLagSeconds = estimatedIntervals * archiveInterval.Seconds()
	} else {
		m.ArchiveLagSeconds = 0
	}
}

// Snapshot returns a copy of the current metrics.
func (m *Metrics) Snapshot() Metrics {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return Metrics{
		LastBackupTime:      m.LastBackupTime,
		LastBackupDuration:  m.LastBackupDuration,
		LastBackupSize:      m.LastBackupSize,
		LastBackupLSN:       m.LastBackupLSN,
		BackupCount:         m.BackupCount,
		BackupErrorCount:    m.BackupErrorCount,
		LastArchiveTime:     m.LastArchiveTime,
		LastArchivedLSN:     m.LastArchivedLSN,
		ArchiveCount:        m.ArchiveCount,
		ArchiveErrorCount:   m.ArchiveErrorCount,
		ArchiveLagSeconds:   m.ArchiveLagSeconds,
		PendingArchiveBytes: m.PendingArchiveBytes,
		LastRestoreTime:     m.LastRestoreTime,
		LastRestoreDuration: m.LastRestoreDuration,
		RestoreCount:        m.RestoreCount,
		RestoreErrorCount:   m.RestoreErrorCount,
		LastVerifyTime:      m.LastVerifyTime,
		VerifyCount:         m.VerifyCount,
		VerifyErrorCount:    m.VerifyErrorCount,
		LastVerifySuccess:   m.LastVerifySuccess,
		ChecksumMismatches:  m.ChecksumMismatches,
	}
}

// Reset resets all metrics to zero values.
func (m *Metrics) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.LastBackupTime = time.Time{}
	m.LastBackupDuration = 0
	m.LastBackupSize = 0
	m.LastBackupLSN = 0
	m.BackupCount = 0
	m.BackupErrorCount = 0
	m.LastArchiveTime = time.Time{}
	m.LastArchivedLSN = 0
	m.ArchiveCount = 0
	m.ArchiveErrorCount = 0
	m.ArchiveLagSeconds = 0
	m.PendingArchiveBytes = 0
	m.LastRestoreTime = time.Time{}
	m.LastRestoreDuration = 0
	m.RestoreCount = 0
	m.RestoreErrorCount = 0
	m.LastVerifyTime = time.Time{}
	m.VerifyCount = 0
	m.VerifyErrorCount = 0
	m.LastVerifySuccess = false
	m.ChecksumMismatches = 0
}

// PrometheusMetrics returns metrics in Prometheus exposition format.
func (m *Metrics) PrometheusMetrics() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var lastBackupUnix, lastArchiveUnix, lastRestoreUnix, lastVerifyUnix int64
	if !m.LastBackupTime.IsZero() {
		lastBackupUnix = m.LastBackupTime.Unix()
	}
	if !m.LastArchiveTime.IsZero() {
		lastArchiveUnix = m.LastArchiveTime.Unix()
	}
	if !m.LastRestoreTime.IsZero() {
		lastRestoreUnix = m.LastRestoreTime.Unix()
	}
	if !m.LastVerifyTime.IsZero() {
		lastVerifyUnix = m.LastVerifyTime.Unix()
	}

	verifySuccess := 0
	if m.LastVerifySuccess {
		verifySuccess = 1
	}

	return `# HELP veridicaldb_backup_last_timestamp_seconds Unix timestamp of last backup
# TYPE veridicaldb_backup_last_timestamp_seconds gauge
veridicaldb_backup_last_timestamp_seconds ` + formatInt(lastBackupUnix) + `

# HELP veridicaldb_backup_duration_seconds Duration of last backup in seconds
# TYPE veridicaldb_backup_duration_seconds gauge
veridicaldb_backup_duration_seconds ` + formatFloat(m.LastBackupDuration.Seconds()) + `

# HELP veridicaldb_backup_size_bytes Size of last backup in bytes
# TYPE veridicaldb_backup_size_bytes gauge
veridicaldb_backup_size_bytes ` + formatInt(m.LastBackupSize) + `

# HELP veridicaldb_backup_lsn LSN of last backup
# TYPE veridicaldb_backup_lsn gauge
veridicaldb_backup_lsn ` + formatUint(m.LastBackupLSN) + `

# HELP veridicaldb_backup_total Total number of backups
# TYPE veridicaldb_backup_total counter
veridicaldb_backup_total ` + formatInt(m.BackupCount) + `

# HELP veridicaldb_backup_errors_total Total number of backup errors
# TYPE veridicaldb_backup_errors_total counter
veridicaldb_backup_errors_total ` + formatInt(m.BackupErrorCount) + `

# HELP veridicaldb_archive_last_timestamp_seconds Unix timestamp of last WAL archive
# TYPE veridicaldb_archive_last_timestamp_seconds gauge
veridicaldb_archive_last_timestamp_seconds ` + formatInt(lastArchiveUnix) + `

# HELP veridicaldb_archive_lsn LSN of last archived WAL
# TYPE veridicaldb_archive_lsn gauge
veridicaldb_archive_lsn ` + formatUint(m.LastArchivedLSN) + `

# HELP veridicaldb_archive_total Total number of WAL archives
# TYPE veridicaldb_archive_total counter
veridicaldb_archive_total ` + formatInt(m.ArchiveCount) + `

# HELP veridicaldb_archive_errors_total Total number of archive errors
# TYPE veridicaldb_archive_errors_total counter
veridicaldb_archive_errors_total ` + formatInt(m.ArchiveErrorCount) + `

# HELP veridicaldb_archive_lag_seconds Estimated archive lag in seconds
# TYPE veridicaldb_archive_lag_seconds gauge
veridicaldb_archive_lag_seconds ` + formatFloat(m.ArchiveLagSeconds) + `

# HELP veridicaldb_restore_last_timestamp_seconds Unix timestamp of last restore
# TYPE veridicaldb_restore_last_timestamp_seconds gauge
veridicaldb_restore_last_timestamp_seconds ` + formatInt(lastRestoreUnix) + `

# HELP veridicaldb_restore_duration_seconds Duration of last restore in seconds
# TYPE veridicaldb_restore_duration_seconds gauge
veridicaldb_restore_duration_seconds ` + formatFloat(m.LastRestoreDuration.Seconds()) + `

# HELP veridicaldb_restore_total Total number of restores
# TYPE veridicaldb_restore_total counter
veridicaldb_restore_total ` + formatInt(m.RestoreCount) + `

# HELP veridicaldb_restore_errors_total Total number of restore errors
# TYPE veridicaldb_restore_errors_total counter
veridicaldb_restore_errors_total ` + formatInt(m.RestoreErrorCount) + `

# HELP veridicaldb_verify_last_timestamp_seconds Unix timestamp of last verification
# TYPE veridicaldb_verify_last_timestamp_seconds gauge
veridicaldb_verify_last_timestamp_seconds ` + formatInt(lastVerifyUnix) + `

# HELP veridicaldb_verify_success Last verification success (1=success, 0=failure)
# TYPE veridicaldb_verify_success gauge
veridicaldb_verify_success ` + formatInt(int64(verifySuccess)) + `

# HELP veridicaldb_verify_total Total number of verifications
# TYPE veridicaldb_verify_total counter
veridicaldb_verify_total ` + formatInt(m.VerifyCount) + `

# HELP veridicaldb_verify_errors_total Total number of verification errors
# TYPE veridicaldb_verify_errors_total counter
veridicaldb_verify_errors_total ` + formatInt(m.VerifyErrorCount) + `

# HELP veridicaldb_checksum_mismatches_total Total checksum mismatches detected
# TYPE veridicaldb_checksum_mismatches_total counter
veridicaldb_checksum_mismatches_total ` + formatInt(m.ChecksumMismatches) + `
`
}

func formatInt(v int64) string {
	return fmt.Sprintf("%d", v)
}

func formatUint(v uint64) string {
	return fmt.Sprintf("%d", v)
}

func formatFloat(v float64) string {
	return fmt.Sprintf("%.6f", v)
}
