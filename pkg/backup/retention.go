// Package backup provides automated retention and pruning for backups and WAL archives.
package backup

import (
	"fmt"
	"sync"
	"time"
)

// RetentionPolicy defines backup and WAL retention rules.
type RetentionPolicy struct {
	// KeepBackups is the number of base backups to retain (0 = use KeepDays only)
	KeepBackups int

	// KeepDays is the number of days to keep backups (0 = keep forever)
	KeepDays int

	// KeepWALForBackups keeps WAL segments needed by retained backups
	KeepWALForBackups bool

	// MinWALSegments is the minimum number of WAL segments to keep regardless of age
	MinWALSegments int
}

// DefaultRetentionPolicy returns a sensible default retention policy.
func DefaultRetentionPolicy() *RetentionPolicy {
	return &RetentionPolicy{
		KeepBackups:       7,  // Keep 7 most recent backups
		KeepDays:          30, // Keep backups for 30 days
		KeepWALForBackups: true,
		MinWALSegments:    10,
	}
}

// RetentionManager handles automated pruning of old backups and WAL archives.
type RetentionManager struct {
	mu sync.Mutex

	backupMgr *Manager
	archiver  *Archiver
	policy    *RetentionPolicy

	running  bool
	stopCh   chan struct{}
	interval time.Duration
}

// NewRetentionManager creates a new retention manager.
func NewRetentionManager(backupMgr *Manager, archiver *Archiver, policy *RetentionPolicy) *RetentionManager {
	if policy == nil {
		policy = DefaultRetentionPolicy()
	}
	return &RetentionManager{
		backupMgr: backupMgr,
		archiver:  archiver,
		policy:    policy,
		interval:  24 * time.Hour, // Daily by default
	}
}

// SetInterval sets the pruning interval.
func (r *RetentionManager) SetInterval(interval time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.interval = interval
}

// Start begins the background retention daemon.
func (r *RetentionManager) Start() {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return
	}
	r.running = true
	r.stopCh = make(chan struct{})
	interval := r.interval
	r.mu.Unlock()

	go r.runLoop(interval)
}

// Stop stops the background retention daemon.
func (r *RetentionManager) Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	close(r.stopCh)
	r.mu.Unlock()
}

func (r *RetentionManager) runLoop(interval time.Duration) {
	// Run immediately on start
	r.RunOnce()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.RunOnce()
		case <-r.stopCh:
			return
		}
	}
}

// RunOnce executes a single retention pass.
func (r *RetentionManager) RunOnce() *RetentionResult {
	result := &RetentionResult{
		StartTime: time.Now(),
	}

	// Prune old backups
	if r.backupMgr != nil {
		prunedBackups, err := r.pruneBackups()
		result.BackupsPruned = prunedBackups
		if err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("prune backups: %w", err))
		}
	}

	// Prune old WAL segments
	if r.archiver != nil {
		prunedWAL, err := r.pruneWALSegments()
		result.WALSegmentsPruned = prunedWAL
		if err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("prune wal: %w", err))
		}
	}

	result.EndTime = time.Now()
	return result
}

// RetentionResult contains the results of a retention pass.
type RetentionResult struct {
	StartTime         time.Time
	EndTime           time.Time
	BackupsPruned     int
	WALSegmentsPruned int
	Errors            []error
}

// Duration returns the duration of the retention pass.
func (r *RetentionResult) Duration() time.Duration {
	return r.EndTime.Sub(r.StartTime)
}

// Success returns true if no errors occurred.
func (r *RetentionResult) Success() bool {
	return len(r.Errors) == 0
}

func (r *RetentionManager) pruneBackups() (int, error) {
	backups, err := r.backupMgr.ListBackups()
	if err != nil {
		return 0, err
	}

	if len(backups) == 0 {
		return 0, nil
	}

	// Determine which backups to delete
	toDelete := make([]string, 0)
	cutoffTime := time.Now().AddDate(0, 0, -r.policy.KeepDays)

	for i, backup := range backups {
		// Keep minimum number of backups
		if r.policy.KeepBackups > 0 && i < r.policy.KeepBackups {
			continue
		}

		// Delete if older than retention period
		if r.policy.KeepDays > 0 && backup.StartTime.Before(cutoffTime) {
			toDelete = append(toDelete, backup.ID)
		}
	}

	// Delete marked backups
	deleted := 0
	for _, id := range toDelete {
		if err := r.backupMgr.DeleteBackup(id); err != nil {
			return deleted, fmt.Errorf("delete backup %s: %w", id, err)
		}
		deleted++
	}

	return deleted, nil
}

func (r *RetentionManager) pruneWALSegments() (int, error) {
	if r.archiver == nil {
		return 0, nil
	}

	// Find the oldest backup we're keeping
	var oldestBackupLSN uint64
	if r.policy.KeepWALForBackups && r.backupMgr != nil {
		backups, err := r.backupMgr.ListBackups()
		if err != nil {
			return 0, err
		}

		// Find oldest retained backup's start LSN
		keepCount := r.policy.KeepBackups
		if keepCount == 0 || keepCount > len(backups) {
			keepCount = len(backups)
		}

		for i := 0; i < keepCount && i < len(backups); i++ {
			if backups[i].StartLSN < oldestBackupLSN || oldestBackupLSN == 0 {
				oldestBackupLSN = backups[i].StartLSN
			}
		}
	}

	// List WAL segments
	segments, err := r.archiver.ListArchivedSegments()
	if err != nil {
		return 0, err
	}

	// Keep minimum segments
	if len(segments) <= r.policy.MinWALSegments {
		return 0, nil
	}

	// Determine which to prune
	pruned := 0
	for i, seg := range segments {
		// Keep minimum segments (segments are sorted by LSN)
		if len(segments)-i <= r.policy.MinWALSegments {
			break
		}

		// Don't prune if needed by retained backups
		if r.policy.KeepWALForBackups && seg.LSN >= oldestBackupLSN {
			continue
		}

		// Prune this segment
		if _, err := r.archiver.PruneArchivedSegments(seg.LSN + 1); err != nil {
			return pruned, err
		}
		pruned++
	}

	return pruned, nil
}

// PruneCommand provides a CLI-friendly interface for manual pruning.
type PruneCommand struct {
	BackupMgr *Manager
	Archiver  *Archiver
	Policy    *RetentionPolicy
	DryRun    bool
}

// Execute runs the prune command.
func (p *PruneCommand) Execute() (*PruneResult, error) {
	result := &PruneResult{
		DryRun: p.DryRun,
	}

	// List backups to prune
	if p.BackupMgr != nil {
		backups, err := p.BackupMgr.ListBackups()
		if err != nil {
			return nil, fmt.Errorf("list backups: %w", err)
		}

		cutoffTime := time.Now().AddDate(0, 0, -p.Policy.KeepDays)
		for i, backup := range backups {
			shouldDelete := false

			if p.Policy.KeepBackups > 0 && i >= p.Policy.KeepBackups {
				shouldDelete = true
			}
			if p.Policy.KeepDays > 0 && backup.StartTime.Before(cutoffTime) {
				shouldDelete = true
			}

			if shouldDelete {
				result.BackupsToDelete = append(result.BackupsToDelete, backup.ID)
				result.BytesToFree += backup.Size
			}
		}

		// Actually delete if not dry run
		if !p.DryRun {
			for _, id := range result.BackupsToDelete {
				if err := p.BackupMgr.DeleteBackup(id); err != nil {
					return result, fmt.Errorf("delete backup %s: %w", id, err)
				}
				result.BackupsDeleted++
			}
		}
	}

	// List WAL segments to prune
	if p.Archiver != nil {
		segments, err := p.Archiver.ListArchivedSegments()
		if err != nil {
			return nil, fmt.Errorf("list segments: %w", err)
		}

		for i, seg := range segments {
			if len(segments)-i <= p.Policy.MinWALSegments {
				break
			}
			result.WALSegmentsToDelete = append(result.WALSegmentsToDelete, seg.Name)
			result.BytesToFree += seg.Size
		}

		// Actually delete if not dry run
		if !p.DryRun && len(result.WALSegmentsToDelete) > 0 {
			lastToKeep := segments[len(segments)-p.Policy.MinWALSegments-1]
			pruned, err := p.Archiver.PruneArchivedSegments(lastToKeep.LSN + 1)
			if err != nil {
				return result, fmt.Errorf("prune wal: %w", err)
			}
			result.WALSegmentsDeleted = pruned
		}
	}

	return result, nil
}

// PruneResult contains the results of a prune operation.
type PruneResult struct {
	DryRun              bool
	BackupsToDelete     []string
	BackupsDeleted      int
	WALSegmentsToDelete []string
	WALSegmentsDeleted  int
	BytesToFree         int64
}
