package backup

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/wal"
)

// Archiver manages WAL segment archiving for point-in-time recovery.
type Archiver struct {
	mu sync.RWMutex

	config *Config
	walDir string
	walMgr *wal.WAL

	// running indicates if the archiver is active
	running bool
	stopCh  chan struct{}

	// archiveInterval is how often to check for WAL segments to archive
	archiveInterval time.Duration

	// lastArchivedLSN tracks the last archived position
	lastArchivedLSN uint64
}

// NewArchiver creates a new WAL archiver.
func NewArchiver(config *Config, walDir string, walMgr *wal.WAL) (*Archiver, error) {
	if config == nil {
		config = DefaultConfig()
	}

	if err := os.MkdirAll(config.ArchiveDir, 0755); err != nil {
		return nil, fmt.Errorf("create archive dir: %w", err)
	}

	return &Archiver{
		config:          config,
		walDir:          walDir,
		walMgr:          walMgr,
		archiveInterval: 1 * time.Minute,
		stopCh:          make(chan struct{}),
	}, nil
}

// SetArchiveInterval sets how often to check for WAL segments.
func (a *Archiver) SetArchiveInterval(interval time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.archiveInterval = interval
}

// Start begins the background archiving process.
func (a *Archiver) Start() {
	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		return
	}
	a.running = true
	a.stopCh = make(chan struct{})
	interval := a.archiveInterval
	a.mu.Unlock()

	go a.archiveLoop(interval)
}

// Stop stops the background archiving process.
func (a *Archiver) Stop() {
	a.mu.Lock()
	if !a.running {
		a.mu.Unlock()
		return
	}
	a.running = false
	close(a.stopCh)
	a.mu.Unlock()
}

func (a *Archiver) archiveLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_ = a.ArchiveCurrentWAL()
		case <-a.stopCh:
			return
		}
	}
}

// ArchiveCurrentWAL archives the current WAL state.
// This creates a snapshot of the WAL for backup purposes.
func (a *Archiver) ArchiveCurrentWAL() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Flush WAL to ensure all data is on disk
	if err := a.walMgr.Flush(); err != nil {
		return fmt.Errorf("flush wal: %w", err)
	}

	currentLSN := uint64(a.walMgr.CurrentLSN())
	if currentLSN <= a.lastArchivedLSN {
		return nil // Nothing new to archive
	}

	// Create archive filename with timestamp and LSN
	timestamp := time.Now().Format("20060102_150405")
	archiveName := fmt.Sprintf("wal_%s_%016x.log", timestamp, currentLSN)
	archivePath := filepath.Join(a.config.ArchiveDir, archiveName)

	// Copy WAL file to archive
	walPath := filepath.Join(a.walDir, "wal.log")
	if err := copyFile(walPath, archivePath); err != nil {
		return fmt.Errorf("copy wal: %w", err)
	}

	// Execute archive command if configured
	if a.config.ArchiveCommand != "" {
		if err := a.executeArchiveCommand(archivePath, archiveName); err != nil {
			return fmt.Errorf("archive command: %w", err)
		}
	}

	// Write metadata for this archive segment
	metaPath := archivePath + ".meta"
	meta := fmt.Sprintf("lsn=%d\ntimestamp=%s\nprev_lsn=%d\n",
		currentLSN, time.Now().Format(time.RFC3339), a.lastArchivedLSN)
	if err := os.WriteFile(metaPath, []byte(meta), 0644); err != nil {
		return fmt.Errorf("write meta: %w", err)
	}

	a.lastArchivedLSN = currentLSN
	return nil
}

func (a *Archiver) executeArchiveCommand(path, filename string) error {
	cmd := a.config.ArchiveCommand
	cmd = strings.ReplaceAll(cmd, "%p", path)
	cmd = strings.ReplaceAll(cmd, "%f", filename)

	return exec.Command("sh", "-c", cmd).Run()
}

// ListArchivedSegments returns all archived WAL segments sorted by LSN.
func (a *Archiver) ListArchivedSegments() ([]ArchivedSegment, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	entries, err := os.ReadDir(a.config.ArchiveDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var segments []ArchivedSegment
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".log") {
			continue
		}
		if strings.HasSuffix(entry.Name(), ".meta") {
			continue
		}

		// Parse filename: wal_YYYYMMDD_HHMMSS_XXXXXXXXXXXXXXXX.log
		name := entry.Name()
		parts := strings.Split(strings.TrimSuffix(name, ".log"), "_")
		if len(parts) < 4 {
			continue
		}

		var lsn uint64
		if _, err := fmt.Sscanf(parts[3], "%x", &lsn); err != nil {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		segments = append(segments, ArchivedSegment{
			Name:      name,
			Path:      filepath.Join(a.config.ArchiveDir, name),
			LSN:       lsn,
			Size:      info.Size(),
			Timestamp: info.ModTime(),
		})
	}

	// Sort by LSN
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].LSN < segments[j].LSN
	})

	return segments, nil
}

// ArchivedSegment represents an archived WAL segment.
type ArchivedSegment struct {
	Name      string
	Path      string
	LSN       uint64
	Size      int64
	Timestamp time.Time
}

// FindSegmentsForPITR returns segments needed to recover to a target time or LSN.
func (a *Archiver) FindSegmentsForPITR(startLSN uint64, targetTime *time.Time, targetLSN *uint64) ([]ArchivedSegment, error) {
	segments, err := a.ListArchivedSegments()
	if err != nil {
		return nil, err
	}

	var needed []ArchivedSegment
	for _, seg := range segments {
		if seg.LSN < startLSN {
			continue // Skip segments before backup start
		}

		needed = append(needed, seg)

		// Check if we've reached target
		if targetLSN != nil && seg.LSN >= *targetLSN {
			break
		}
		if targetTime != nil && seg.Timestamp.After(*targetTime) {
			break
		}
	}

	return needed, nil
}

// PruneArchivedSegments removes old archived segments based on retention.
func (a *Archiver) PruneArchivedSegments(keepAfterLSN uint64) (int, error) {
	segments, err := a.ListArchivedSegments()
	if err != nil {
		return 0, err
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	pruned := 0
	for _, seg := range segments {
		if seg.LSN >= keepAfterLSN {
			continue
		}

		if err := os.Remove(seg.Path); err != nil {
			return pruned, fmt.Errorf("remove %s: %w", seg.Name, err)
		}
		metaPath := seg.Path + ".meta"
		_ = os.Remove(metaPath)
		pruned++
	}

	return pruned, nil
}

// LastArchivedLSN returns the last archived WAL position.
func (a *Archiver) LastArchivedLSN() uint64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.lastArchivedLSN
}

// copyFile copies a file from src to dst.
func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = srcFile.Close() }()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() { _ = dstFile.Close() }()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return err
	}

	return dstFile.Sync()
}
