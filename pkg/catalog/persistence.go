// Package catalog provides persistence for database and TUI metadata.
package catalog

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

// TUIPersistenceVersion is the current schema version for TUI persistence. v1.1 as if i ever made a 1.0
const TUIPersistenceVersion = "1.1"

// MaxQueryHistorySize is the maximum number of queries to keep in history.
const MaxQueryHistorySize = 500

// TUIMetadata holds all TUI-related persistent data. lots of thse are yet to be added
type TUIMetadata struct {
	Metadata       SchemaMetadata `json:"metadata"`
	QueryHistory   []QueryRecord  `json:"query_history"`
	SavedQueries   []SavedQuery   `json:"saved_queries"`
	BackupMetadata []BackupRecord `json:"backup_metadata"`
	UserActions    []UserAction   `json:"user_actions"`
	Preferences    TUIPreferences `json:"preferences"`
}

// SchemaMetadata contains versioning info for migration support.
type SchemaMetadata struct {
	Version   string    `json:"version"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// QueryRecord represents a single executed query in history.
type QueryRecord struct {
	ID              string    `json:"id"`
	QueryText       string    `json:"query_text"`
	ExecutedAt      time.Time `json:"executed_at"`
	ExecutionTimeMs int64     `json:"execution_time_ms"`
	Status          string    `json:"status"` // "success", "error"
	ErrorMessage    string    `json:"error_message,omitempty"`
	RowsAffected    int64     `json:"rows_affected,omitempty"`
	Database        string    `json:"database,omitempty"`
}

// SavedQuery represents a user-saved query for reuse.
type SavedQuery struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	QueryText   string    `json:"query_text"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	Tags        []string  `json:"tags,omitempty"`
	IsFavorite  bool      `json:"is_favorite,omitempty"`
}

// BackupRecord represents metadata about a backup operation.
type BackupRecord struct {
	ID              string    `json:"id"`
	BackupID        string    `json:"backup_id"`
	BackupPath      string    `json:"backup_path,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
	SizeBytes       int64     `json:"size_bytes"`
	Status          string    `json:"status"` // "success", "failed", "in_progress"
	RetentionPolicy string    `json:"retention_policy,omitempty"`
	ErrorMessage    string    `json:"error_message,omitempty"`
	Database        string    `json:"database,omitempty"`
	Type            string    `json:"type,omitempty"` // "full", "incremental", "wal"
}

// UserAction represents an audit log entry for user management operations.
type UserAction struct {
	ID          string    `json:"id"`
	Action      string    `json:"action"` // "create", "edit", "delete", "grant", "revoke"
	TargetUser  string    `json:"target_user"`
	PerformedBy string    `json:"performed_by,omitempty"`
	Timestamp   time.Time `json:"timestamp"`
	Status      string    `json:"status"` // "success", "failed"
	Details     string    `json:"details,omitempty"`
}

// TUIPreferences stores user preferences for the TUI.
type TUIPreferences struct {
	Theme            string `json:"theme,omitempty"`
	SidebarCollapsed bool   `json:"sidebar_collapsed,omitempty"`
	EditorFontSize   int    `json:"editor_font_size,omitempty"`
	AutoSaveQueries  bool   `json:"auto_save_queries,omitempty"`
}

// TUIPersistence manages TUI metadata persistence with atomic operations.
type TUIPersistence struct {
	mu       sync.RWMutex
	dataDir  string
	metadata *TUIMetadata
	dirty    bool
}

// NewTUIPersistence creates a new TUI persistence manager.
func NewTUIPersistence(dataDir string) (*TUIPersistence, error) {
	p := &TUIPersistence{
		dataDir: dataDir,
		metadata: &TUIMetadata{
			Metadata: SchemaMetadata{
				Version:   TUIPersistenceVersion,
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
			},
			QueryHistory:   make([]QueryRecord, 0),
			SavedQueries:   make([]SavedQuery, 0),
			BackupMetadata: make([]BackupRecord, 0),
			UserActions:    make([]UserAction, 0),
			Preferences:    TUIPreferences{},
		},
	}

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	if err := p.load(); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to load TUI metadata: %w", err)
	}

	return p, nil
}

func (p *TUIPersistence) metadataPath() string {
	return filepath.Join(p.dataDir, "tui_metadata.json")
}

func (p *TUIPersistence) tempPath() string {
	return filepath.Join(p.dataDir, "tui_metadata.json.tmp")
}

// load reads TUI metadata from disk with migration support.
func (p *TUIPersistence) load() error {
	data, err := os.ReadFile(p.metadataPath())
	if err != nil {
		return err
	}

	var metadata TUIMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return fmt.Errorf("failed to parse TUI metadata: %w", err)
	}

	// Migrate if needed
	if metadata.Metadata.Version != TUIPersistenceVersion {
		if err := p.migrate(&metadata); err != nil {
			return fmt.Errorf("failed to migrate TUI metadata: %w", err)
		}
	}

	p.metadata = &metadata
	return nil
}

// migrate handles schema migrations from older versions.
func (p *TUIPersistence) migrate(metadata *TUIMetadata) error {
	oldVersion := metadata.Metadata.Version

	// v1.0 -> v1.1: Add preferences field
	if oldVersion == "" || oldVersion == "1.0" {
		metadata.Metadata.Version = "1.1"
		metadata.Metadata.UpdatedAt = time.Now()
		if metadata.Preferences.Theme == "" {
			metadata.Preferences.Theme = "dark"
		}
	}

	// Mark as dirty to save migrated data
	p.dirty = true
	return nil
}

// save writes TUI metadata to disk atomically.
func (p *TUIPersistence) save() error {
	p.metadata.Metadata.UpdatedAt = time.Now()

	data, err := json.MarshalIndent(p.metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal TUI metadata: %w", err)
	}

	// Write to temp file first
	tempPath := p.tempPath()
	f, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	if _, err := f.Write(data); err != nil {
		if cerr := f.Close(); cerr != nil {
			return fmt.Errorf("failed to write temp file: %w; also failed to close: %v", err, cerr)
		}
		if rerr := os.Remove(tempPath); rerr != nil {
			return fmt.Errorf("failed to write temp file: %w; also failed to remove: %v", err, rerr)
		}
		return fmt.Errorf("failed to write temp file: %w", err)
	}

	// Sync to disk
	if err := f.Sync(); err != nil {
		if cerr := f.Close(); cerr != nil {
			return fmt.Errorf("failed to sync temp file: %w; also failed to close: %v", err, cerr)
		}
		if rerr := os.Remove(tempPath); rerr != nil {
			return fmt.Errorf("failed to sync temp file: %w; also failed to remove: %v", err, rerr)
		}
		return fmt.Errorf("failed to sync temp file: %w", err)
	}

	if err := f.Close(); err != nil {
		if rerr := os.Remove(tempPath); rerr != nil {
			return fmt.Errorf("failed to close temp file: %w; also failed to remove: %v", err, rerr)
		}
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tempPath, p.metadataPath()); err != nil {
		if rerr := os.Remove(tempPath); rerr != nil {
			return fmt.Errorf("failed to rename temp file: %w; also failed to remove: %v", err, rerr)
		}
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	p.dirty = false
	return nil
}

// Save persists all changes to disk.
func (p *TUIPersistence) Save() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.save()
}

// IsDirty returns true if there are unsaved changes.
func (p *TUIPersistence) IsDirty() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.dirty
}

// ============================================================================
// Query History Operations
// ============================================================================

// AddQueryToHistory adds a query execution record to history.
func (p *TUIPersistence) AddQueryToHistory(queryText string, executionTimeMs int64, status string, errorMessage string, rowsAffected int64, database string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	record := QueryRecord{
		ID:              uuid.New().String(),
		QueryText:       queryText,
		ExecutedAt:      time.Now(),
		ExecutionTimeMs: executionTimeMs,
		Status:          status,
		ErrorMessage:    errorMessage,
		RowsAffected:    rowsAffected,
		Database:        database,
	}

	// Prepend to history (most recent first)
	p.metadata.QueryHistory = append([]QueryRecord{record}, p.metadata.QueryHistory...)

	// Enforce max size with LRU eviction
	if len(p.metadata.QueryHistory) > MaxQueryHistorySize {
		p.metadata.QueryHistory = p.metadata.QueryHistory[:MaxQueryHistorySize]
	}

	p.dirty = true
	return nil
}

// GetRecentQueries returns the most recent queries from history.
func (p *TUIPersistence) GetRecentQueries(limit int) []QueryRecord {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if limit <= 0 || limit > len(p.metadata.QueryHistory) {
		limit = len(p.metadata.QueryHistory)
	}

	result := make([]QueryRecord, limit)
	copy(result, p.metadata.QueryHistory[:limit])
	return result
}

// GetQueryHistory returns all query history.
func (p *TUIPersistence) GetQueryHistory() []QueryRecord {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make([]QueryRecord, len(p.metadata.QueryHistory))
	copy(result, p.metadata.QueryHistory)
	return result
}

// ClearQueryHistory removes all query history.
func (p *TUIPersistence) ClearQueryHistory() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.metadata.QueryHistory = make([]QueryRecord, 0)
	p.dirty = true
	return nil
}

// ============================================================================
// Saved Queries Operations
// ============================================================================

// SaveQuery stores a named query for reuse.
func (p *TUIPersistence) SaveQuery(name, description, queryText string, tags []string) (*SavedQuery, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check for duplicate name
	for _, q := range p.metadata.SavedQueries {
		if q.Name == name {
			return nil, errors.New("a saved query with this name already exists")
		}
	}

	query := SavedQuery{
		ID:          uuid.New().String(),
		Name:        name,
		Description: description,
		QueryText:   queryText,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		Tags:        tags,
	}

	p.metadata.SavedQueries = append(p.metadata.SavedQueries, query)
	p.dirty = true

	return &query, nil
}

// UpdateSavedQuery updates an existing saved query.
func (p *TUIPersistence) UpdateSavedQuery(id, name, description, queryText string, tags []string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i, q := range p.metadata.SavedQueries {
		if q.ID == id {
			// Check for duplicate name (excluding current)
			for _, other := range p.metadata.SavedQueries {
				if other.ID != id && other.Name == name {
					return errors.New("a saved query with this name already exists")
				}
			}

			p.metadata.SavedQueries[i].Name = name
			p.metadata.SavedQueries[i].Description = description
			p.metadata.SavedQueries[i].QueryText = queryText
			p.metadata.SavedQueries[i].Tags = tags
			p.metadata.SavedQueries[i].UpdatedAt = time.Now()
			p.dirty = true
			return nil
		}
	}

	return errors.New("saved query not found")
}

// DeleteSavedQuery removes a saved query by ID.
func (p *TUIPersistence) DeleteSavedQuery(id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i, q := range p.metadata.SavedQueries {
		if q.ID == id {
			p.metadata.SavedQueries = append(p.metadata.SavedQueries[:i], p.metadata.SavedQueries[i+1:]...)
			p.dirty = true
			return nil
		}
	}

	return errors.New("saved query not found")
}

// GetSavedQueries returns all saved queries.
func (p *TUIPersistence) GetSavedQueries() []SavedQuery {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make([]SavedQuery, len(p.metadata.SavedQueries))
	copy(result, p.metadata.SavedQueries)
	return result
}

// GetSavedQueryByID returns a saved query by ID.
func (p *TUIPersistence) GetSavedQueryByID(id string) (*SavedQuery, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, q := range p.metadata.SavedQueries {
		if q.ID == id {
			query := q
			return &query, nil
		}
	}

	return nil, errors.New("saved query not found")
}

// SearchSavedQueries searches saved queries by name or tags.
func (p *TUIPersistence) SearchSavedQueries(searchTerm string) []SavedQuery {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var results []SavedQuery
	for _, q := range p.metadata.SavedQueries {
		// Search in name
		if containsIgnoreCase(q.Name, searchTerm) {
			results = append(results, q)
			continue
		}
		// Search in description
		if containsIgnoreCase(q.Description, searchTerm) {
			results = append(results, q)
			continue
		}
		// Search in tags
		for _, tag := range q.Tags {
			if containsIgnoreCase(tag, searchTerm) {
				results = append(results, q)
				break
			}
		}
	}

	return results
}

// ToggleFavorite toggles the favorite status of a saved query.
func (p *TUIPersistence) ToggleFavorite(id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i, q := range p.metadata.SavedQueries {
		if q.ID == id {
			p.metadata.SavedQueries[i].IsFavorite = !p.metadata.SavedQueries[i].IsFavorite
			p.dirty = true
			return nil
		}
	}

	return errors.New("saved query not found")
}

// ============================================================================
// Backup Metadata Operations
// ============================================================================

// LogBackup records a backup operation.
func (p *TUIPersistence) LogBackup(backupID, backupPath string, sizeBytes int64, status, retentionPolicy, database, backupType string) (*BackupRecord, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	record := BackupRecord{
		ID:              uuid.New().String(),
		BackupID:        backupID,
		BackupPath:      backupPath,
		CreatedAt:       time.Now(),
		SizeBytes:       sizeBytes,
		Status:          status,
		RetentionPolicy: retentionPolicy,
		Database:        database,
		Type:            backupType,
	}

	p.metadata.BackupMetadata = append(p.metadata.BackupMetadata, record)
	p.dirty = true

	return &record, nil
}

// UpdateBackupStatus updates the status of a backup record.
func (p *TUIPersistence) UpdateBackupStatus(id, status, errorMessage string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i, b := range p.metadata.BackupMetadata {
		if b.ID == id {
			p.metadata.BackupMetadata[i].Status = status
			p.metadata.BackupMetadata[i].ErrorMessage = errorMessage
			p.dirty = true
			return nil
		}
	}

	return errors.New("backup record not found")
}

// GetBackupHistory returns all backup records sorted by creation time (newest first).
func (p *TUIPersistence) GetBackupHistory() []BackupRecord {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make([]BackupRecord, len(p.metadata.BackupMetadata))
	copy(result, p.metadata.BackupMetadata)

	// Sort by creation time (newest first)
	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt.After(result[j].CreatedAt)
	})

	return result
}

// GetBackupByID returns a backup record by ID.
func (p *TUIPersistence) GetBackupByID(id string) (*BackupRecord, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, b := range p.metadata.BackupMetadata {
		if b.ID == id {
			record := b
			return &record, nil
		}
	}

	return nil, errors.New("backup record not found")
}

// DeleteBackupRecord removes a backup metadata record.
func (p *TUIPersistence) DeleteBackupRecord(id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i, b := range p.metadata.BackupMetadata {
		if b.ID == id {
			p.metadata.BackupMetadata = append(p.metadata.BackupMetadata[:i], p.metadata.BackupMetadata[i+1:]...)
			p.dirty = true
			return nil
		}
	}

	return errors.New("backup record not found")
}

// ============================================================================
// User Actions (Audit Log) Operations
// ============================================================================

// LogUserAction records a user management operation.
func (p *TUIPersistence) LogUserAction(action, targetUser, performedBy, status, details string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	record := UserAction{
		ID:          uuid.New().String(),
		Action:      action,
		TargetUser:  targetUser,
		PerformedBy: performedBy,
		Timestamp:   time.Now(),
		Status:      status,
		Details:     details,
	}

	p.metadata.UserActions = append(p.metadata.UserActions, record)
	p.dirty = true

	return nil
}

// GetUserActions returns all user action records sorted by timestamp (newest first).
func (p *TUIPersistence) GetUserActions() []UserAction {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make([]UserAction, len(p.metadata.UserActions))
	copy(result, p.metadata.UserActions)

	// Sort by timestamp (newest first)
	sort.Slice(result, func(i, j int) bool {
		return result[i].Timestamp.After(result[j].Timestamp)
	})

	return result
}

// GetUserActionsByUser returns all actions related to a specific user.
func (p *TUIPersistence) GetUserActionsByUser(targetUser string) []UserAction {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var results []UserAction
	for _, a := range p.metadata.UserActions {
		if a.TargetUser == targetUser {
			results = append(results, a)
		}
	}

	// Sort by timestamp (newest first)
	sort.Slice(results, func(i, j int) bool {
		return results[i].Timestamp.After(results[j].Timestamp)
	})

	return results
}

// ============================================================================
// Preferences Operations
// ============================================================================

// GetPreferences returns the current TUI preferences.
func (p *TUIPersistence) GetPreferences() TUIPreferences {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.metadata.Preferences
}

// SetPreferences updates the TUI preferences.
func (p *TUIPersistence) SetPreferences(prefs TUIPreferences) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.metadata.Preferences = prefs
	p.dirty = true
	return nil
}

// SetTheme updates the preferred theme.
func (p *TUIPersistence) SetTheme(theme string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.metadata.Preferences.Theme = theme
	p.dirty = true
	return nil
}

// ============================================================================
// Helper Functions
// ============================================================================

// containsIgnoreCase checks if s contains substr (case-insensitive).
func containsIgnoreCase(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr ||
		len(substr) == 0 ||
		findIgnoreCase(s, substr) >= 0)
}

// findIgnoreCase finds substr in s (case-insensitive).
func findIgnoreCase(s, substr string) int {
	if len(substr) == 0 {
		return 0
	}
	if len(s) < len(substr) {
		return -1
	}

	// Simple case-insensitive search
	sLower := toLower(s)
	substrLower := toLower(substr)

	for i := 0; i <= len(sLower)-len(substrLower); i++ {
		if sLower[i:i+len(substrLower)] == substrLower {
			return i
		}
	}
	return -1
}

// toLower converts a string to lowercase (ASCII only for performance).
func toLower(s string) string {
	result := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			result[i] = c + 32
		} else {
			result[i] = c
		}
	}
	return string(result)
}
