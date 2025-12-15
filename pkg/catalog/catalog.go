package catalog

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// TableMeta holds metadata for a table.
type TableMeta struct {
	ID            int            `json:"id"`
	Name          string         `json:"name"`
	StorageType   string         `json:"storage_type"` // "row" or "column" (future)
	Schema        *Schema        `json:"-"`
	Columns       []Column       `json:"columns"`
	ForeignKeys   []ForeignKey   `json:"foreign_keys"`
	PartitionSpec *PartitionSpec `json:"partition_spec,omitempty"` // Optional partitioning
}

// Catalog manages table metadata, persisted to a JSON file.
type Catalog struct {
	mu      sync.RWMutex
	dataDir string
	tables  map[string]*TableMeta
	nextID  int
}

// NewCatalog creates or loads a catalog from dataDir.
func NewCatalog(dataDir string) (*Catalog, error) {
	c := &Catalog{
		dataDir: dataDir,
		tables:  make(map[string]*TableMeta),
		nextID:  1,
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	if err := c.load(); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	return c, nil
}

func (c *Catalog) catalogPath() string {
	return filepath.Join(c.dataDir, "catalog.json")
}

// load reads catalog from disk.
func (c *Catalog) load() error {
	data, err := os.ReadFile(c.catalogPath())
	if err != nil {
		return err
	}
	var state struct {
		Tables []*TableMeta `json:"tables"`
		NextID int          `json:"next_id"`
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	c.nextID = state.NextID
	for _, t := range state.Tables {
		t.Schema = NewSchema(t.Columns)
		t.Schema.ForeignKeys = t.ForeignKeys
		c.tables[t.Name] = t
	}
	return nil
}

// save writes catalog to disk.
func (c *Catalog) save() error {
	tables := make([]*TableMeta, 0, len(c.tables))
	for _, t := range c.tables {
		tables = append(tables, t)
	}
	state := struct {
		Tables []*TableMeta `json:"tables"`
		NextID int          `json:"next_id"`
	}{
		Tables: tables,
		NextID: c.nextID,
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(c.catalogPath(), data, 0o644)
}

// CreateTable registers a new table with the given schema.
func (c *Catalog) CreateTable(name string, cols []Column, foreignKeys []ForeignKey, storageType string) (*TableMeta, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.tables[name]; exists {
		return nil, fmt.Errorf("table %q already exists", name)
	}

	if storageType == "" {
		storageType = "row"
	}

	meta := &TableMeta{
		ID:          c.nextID,
		Name:        name,
		StorageType: storageType,
		Columns:     cols,
		ForeignKeys: foreignKeys,
		Schema:      NewSchema(cols),
	}
	meta.Schema.ForeignKeys = foreignKeys
	c.nextID++
	c.tables[name] = meta

	if err := c.save(); err != nil {
		delete(c.tables, name)
		c.nextID--
		return nil, err
	}
	return meta, nil
}

// DropTable removes a table from the catalog.
func (c *Catalog) DropTable(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.tables[name]; !exists {
		return fmt.Errorf("table %q does not exist", name)
	}
	delete(c.tables, name)
	return c.save()
}

// GetTable returns metadata for a table.
func (c *Catalog) GetTable(name string) (*TableMeta, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	t, exists := c.tables[name]
	if !exists {
		return nil, fmt.Errorf("table %q does not exist", name)
	}
	return t, nil
}

// ListTables returns all table names.
func (c *Catalog) ListTables() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	names := make([]string, 0, len(c.tables))
	for name := range c.tables {
		names = append(names, name)
	}
	return names
}

// UpdateTable updates table metadata.
func (c *Catalog) UpdateTable(meta *TableMeta) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.tables[meta.Name]; !exists {
		return fmt.Errorf("table %q does not exist", meta.Name)
	}

	// Update schema from columns
	meta.Schema = NewSchema(meta.Columns)
	c.tables[meta.Name] = meta
	return c.save()
}

// RenameTable renames a table in the catalog.
func (c *Catalog) RenameTable(oldName, newName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	meta, exists := c.tables[oldName]
	if !exists {
		return fmt.Errorf("table %q does not exist", oldName)
	}

	if _, exists := c.tables[newName]; exists {
		return fmt.Errorf("table %q already exists", newName)
	}

	// Update the metadata
	meta.Name = newName
	delete(c.tables, oldName)
	c.tables[newName] = meta
	return c.save()
}

// ErrTableNotFound is returned when a table doesn't exist.
var ErrTableNotFound = errors.New("table not found")
