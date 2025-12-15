// Package catalog provides database and table metadata management.
package catalog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// DatabaseInfo describes a database namespace.
type DatabaseInfo struct {
	Name     string    `json:"name"`
	Owner    string    `json:"owner"`
	Path     string    `json:"path"`     // relative to data root
	Encoding string    `json:"encoding"` // e.g., "UTF-8"
	Created  time.Time `json:"created"`
}

// DatabaseManager manages multiple databases (namespaces) within an instance.
type DatabaseManager struct {
	mu        sync.RWMutex
	rootPath  string                   // data root directory
	databases map[string]*DatabaseInfo // name -> info
	catalogs  map[string]*Catalog      // name -> per-db catalog (lazy loaded)
}

// NewDatabaseManager creates or loads a database manager from the root data directory.
func NewDatabaseManager(rootPath string) (*DatabaseManager, error) {
	dm := &DatabaseManager{
		rootPath:  rootPath,
		databases: make(map[string]*DatabaseInfo),
		catalogs:  make(map[string]*Catalog),
	}

	// Ensure root path exists
	if err := os.MkdirAll(rootPath, 0o755); err != nil {
		return nil, fmt.Errorf("create root path: %w", err)
	}

	// Load existing databases
	if err := dm.load(); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("load databases: %w", err)
	}

	// Create default database if none exist
	if len(dm.databases) == 0 {
		if _, err := dm.CreateDatabase("default", ""); err != nil {
			return nil, fmt.Errorf("create default database: %w", err)
		}
	}

	return dm, nil
}

// databasesPath returns the path to the databases registry file.
func (dm *DatabaseManager) databasesPath() string {
	return filepath.Join(dm.rootPath, "databases.json")
}

// load reads the database registry from disk.
func (dm *DatabaseManager) load() error {
	data, err := os.ReadFile(dm.databasesPath())
	if err != nil {
		return err
	}

	var dbs []*DatabaseInfo
	if err := json.Unmarshal(data, &dbs); err != nil {
		return err
	}

	for _, db := range dbs {
		dm.databases[db.Name] = db
	}
	return nil
}

// save writes the database registry to disk.
func (dm *DatabaseManager) save() error {
	dbs := make([]*DatabaseInfo, 0, len(dm.databases))
	for _, db := range dm.databases {
		dbs = append(dbs, db)
	}

	data, err := json.MarshalIndent(dbs, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(dm.databasesPath(), data, 0o644)
}

// CreateDatabase creates a new database with the given name.
func (dm *DatabaseManager) CreateDatabase(name, owner string) (*DatabaseInfo, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Check if already exists
	if _, exists := dm.databases[name]; exists {
		return nil, fmt.Errorf("database %q already exists", name)
	}

	// Create database directory structure
	dbPath := filepath.Join(dm.rootPath, name)
	subdirs := []string{"tables", "indexes", "wal"}
	for _, sub := range subdirs {
		if err := os.MkdirAll(filepath.Join(dbPath, sub), 0o755); err != nil {
			return nil, fmt.Errorf("create %s directory: %w", sub, err)
		}
	}

	// Create database info
	info := &DatabaseInfo{
		Name:     name,
		Owner:    owner,
		Path:     name, // relative path
		Encoding: "UTF-8",
		Created:  time.Now(),
	}

	// Write metadata file
	metaPath := filepath.Join(dbPath, "meta.json")
	metaData, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal metadata: %w", err)
	}
	if err := os.WriteFile(metaPath, metaData, 0o644); err != nil {
		return nil, fmt.Errorf("write metadata: %w", err)
	}

	// Register in memory and persist
	dm.databases[name] = info
	if err := dm.save(); err != nil {
		return nil, fmt.Errorf("save registry: %w", err)
	}

	return info, nil
}

// CreateDatabaseIfNotExists creates a database only if it doesn't exist.
func (dm *DatabaseManager) CreateDatabaseIfNotExists(name, owner string) (*DatabaseInfo, error) {
	dm.mu.RLock()
	if info, exists := dm.databases[name]; exists {
		dm.mu.RUnlock()
		return info, nil
	}
	dm.mu.RUnlock()

	return dm.CreateDatabase(name, owner)
}

// DropDatabase removes a database.
func (dm *DatabaseManager) DropDatabase(name string, ifExists bool) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	info, exists := dm.databases[name]
	if !exists {
		if ifExists {
			return nil
		}
		return fmt.Errorf("database %q does not exist", name)
	}

	// Prevent dropping default database
	if name == "default" {
		return fmt.Errorf("cannot drop the default database")
	}

	// Check if database directory is empty (except for meta.json and empty subdirs)
	dbPath := filepath.Join(dm.rootPath, info.Path)
	tablesPath := filepath.Join(dbPath, "tables")
	entries, err := os.ReadDir(tablesPath)
	if err == nil && len(entries) > 0 {
		return fmt.Errorf("database %q is not empty (contains tables)", name)
	}

	// Close and remove cached catalog
	delete(dm.catalogs, name)

	// Remove from registry
	delete(dm.databases, name)
	if err := dm.save(); err != nil {
		return fmt.Errorf("save registry: %w", err)
	}

	// Remove database directory
	if err := os.RemoveAll(dbPath); err != nil {
		return fmt.Errorf("remove database directory: %w", err)
	}

	return nil
}

// GetDatabase returns info about a database.
func (dm *DatabaseManager) GetDatabase(name string) (*DatabaseInfo, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	info, exists := dm.databases[name]
	if !exists {
		return nil, fmt.Errorf("database %q does not exist", name)
	}
	return info, nil
}

// ListDatabases returns all database names.
func (dm *DatabaseManager) ListDatabases() []string {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	names := make([]string, 0, len(dm.databases))
	for name := range dm.databases {
		names = append(names, name)
	}
	return names
}

// DatabaseExists checks if a database exists.
func (dm *DatabaseManager) DatabaseExists(name string) bool {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	_, exists := dm.databases[name]
	return exists
}

// GetCatalog returns the catalog for a database, creating it if needed.
func (dm *DatabaseManager) GetCatalog(dbName string) (*Catalog, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Check if database exists
	info, exists := dm.databases[dbName]
	if !exists {
		return nil, fmt.Errorf("database %q does not exist", dbName)
	}

	// Return cached catalog if available
	if cat, ok := dm.catalogs[dbName]; ok {
		return cat, nil
	}

	// Create new catalog for this database
	dbPath := filepath.Join(dm.rootPath, info.Path)
	cat, err := NewCatalog(dbPath)
	if err != nil {
		return nil, fmt.Errorf("create catalog for %q: %w", dbName, err)
	}

	dm.catalogs[dbName] = cat
	return cat, nil
}

// GetDatabasePath returns the filesystem path for a database.
func (dm *DatabaseManager) GetDatabasePath(dbName string) (string, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	info, exists := dm.databases[dbName]
	if !exists {
		return "", fmt.Errorf("database %q does not exist", dbName)
	}
	return filepath.Join(dm.rootPath, info.Path), nil
}

// RootPath returns the root data directory.
func (dm *DatabaseManager) RootPath() string {
	return dm.rootPath
}
