package catalog

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDatabaseManager_CreateDatabase(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dbmgr_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dm, err := NewDatabaseManager(tmpDir)
	if err != nil {
		t.Fatalf("failed to create database manager: %v", err)
	}

	// Default database should exist
	if !dm.DatabaseExists("default") {
		t.Error("expected 'default' database to exist")
	}

	// Create a new database
	info, err := dm.CreateDatabase("testdb", "alice")
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	if info.Name != "testdb" {
		t.Errorf("expected name 'testdb', got %q", info.Name)
	}
	if info.Owner != "alice" {
		t.Errorf("expected owner 'alice', got %q", info.Owner)
	}

	// Verify directory structure was created
	dbPath := filepath.Join(tmpDir, "testdb")
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Error("expected database directory to be created")
	}
	if _, err := os.Stat(filepath.Join(dbPath, "tables")); os.IsNotExist(err) {
		t.Error("expected tables subdirectory")
	}
	if _, err := os.Stat(filepath.Join(dbPath, "indexes")); os.IsNotExist(err) {
		t.Error("expected indexes subdirectory")
	}
	if _, err := os.Stat(filepath.Join(dbPath, "wal")); os.IsNotExist(err) {
		t.Error("expected wal subdirectory")
	}
	if _, err := os.Stat(filepath.Join(dbPath, "meta.json")); os.IsNotExist(err) {
		t.Error("expected meta.json file")
	}

	// Creating duplicate should fail
	_, err = dm.CreateDatabase("testdb", "bob")
	if err == nil {
		t.Error("expected error creating duplicate database")
	}
}

func TestDatabaseManager_CreateDatabaseIfNotExists(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dbmgr_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dm, err := NewDatabaseManager(tmpDir)
	if err != nil {
		t.Fatalf("failed to create database manager: %v", err)
	}

	// Create database
	info1, err := dm.CreateDatabaseIfNotExists("mydb", "alice")
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	// Create again with IF NOT EXISTS - should succeed and return existing
	info2, err := dm.CreateDatabaseIfNotExists("mydb", "bob")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should be the same database (owner remains alice)
	if info1.Name != info2.Name {
		t.Error("expected same database to be returned")
	}
	if info2.Owner != "alice" {
		t.Errorf("expected owner to remain 'alice', got %q", info2.Owner)
	}
}

func TestDatabaseManager_DropDatabase(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dbmgr_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dm, err := NewDatabaseManager(tmpDir)
	if err != nil {
		t.Fatalf("failed to create database manager: %v", err)
	}

	// Create a database
	_, err = dm.CreateDatabase("todelete", "")
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	// Verify it exists
	if !dm.DatabaseExists("todelete") {
		t.Error("expected database to exist")
	}

	// Drop it
	err = dm.DropDatabase("todelete", false)
	if err != nil {
		t.Fatalf("failed to drop database: %v", err)
	}

	// Verify it's gone
	if dm.DatabaseExists("todelete") {
		t.Error("expected database to be dropped")
	}

	// Directory should be removed
	dbPath := filepath.Join(tmpDir, "todelete")
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Error("expected database directory to be removed")
	}
}

func TestDatabaseManager_DropDatabase_IfExists(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dbmgr_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dm, err := NewDatabaseManager(tmpDir)
	if err != nil {
		t.Fatalf("failed to create database manager: %v", err)
	}

	// Drop non-existent without IF EXISTS - should fail
	err = dm.DropDatabase("nonexistent", false)
	if err == nil {
		t.Error("expected error dropping non-existent database")
	}

	// Drop non-existent with IF EXISTS - should succeed
	err = dm.DropDatabase("nonexistent", true)
	if err != nil {
		t.Errorf("expected no error with IF EXISTS, got: %v", err)
	}
}

func TestDatabaseManager_DropDatabase_Default(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dbmgr_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dm, err := NewDatabaseManager(tmpDir)
	if err != nil {
		t.Fatalf("failed to create database manager: %v", err)
	}

	// Cannot drop default database when it's the only database
	err = dm.DropDatabase("default", false)
	if err == nil {
		t.Error("expected error dropping default database when only one exists")
	}

	// Create another database and now dropping default should be allowed
	_, err = dm.CreateDatabase("other", "")
	if err != nil {
		t.Fatalf("failed to create other database: %v", err)
	}

	// Now drop default should succeed
	err = dm.DropDatabase("default", false)
	if err != nil {
		t.Fatalf("expected dropping default to succeed when other DBs exist: %v", err)
	}
	if dm.DatabaseExists("default") {
		t.Error("expected default to be dropped")
	}
}

func TestDatabaseManager_GetDatabase(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dbmgr_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dm, err := NewDatabaseManager(tmpDir)
	if err != nil {
		t.Fatalf("failed to create database manager: %v", err)
	}

	_, err = dm.CreateDatabase("mydb", "owner1")
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	// Get existing database
	info, err := dm.GetDatabase("mydb")
	if err != nil {
		t.Fatalf("failed to get database: %v", err)
	}
	if info.Name != "mydb" {
		t.Errorf("expected name 'mydb', got %q", info.Name)
	}
	if info.Owner != "owner1" {
		t.Errorf("expected owner 'owner1', got %q", info.Owner)
	}

	// Get non-existent database
	_, err = dm.GetDatabase("nonexistent")
	if err == nil {
		t.Error("expected error getting non-existent database")
	}
}

func TestDatabaseManager_ListDatabases(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dbmgr_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dm, err := NewDatabaseManager(tmpDir)
	if err != nil {
		t.Fatalf("failed to create database manager: %v", err)
	}

	// Create additional databases
	_, _ = dm.CreateDatabase("db1", "")
	_, _ = dm.CreateDatabase("db2", "")

	// List should include default + db1 + db2
	dbs := dm.ListDatabases()
	if len(dbs) != 3 {
		t.Errorf("expected 3 databases, got %d", len(dbs))
	}

	// Check all are present
	dbMap := make(map[string]bool)
	for _, name := range dbs {
		dbMap[name] = true
	}
	if !dbMap["default"] || !dbMap["db1"] || !dbMap["db2"] {
		t.Error("expected default, db1, db2 in list")
	}
}

func TestDatabaseManager_GetCatalog(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dbmgr_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dm, err := NewDatabaseManager(tmpDir)
	if err != nil {
		t.Fatalf("failed to create database manager: %v", err)
	}

	// Get catalog for default database
	cat, err := dm.GetCatalog("default")
	if err != nil {
		t.Fatalf("failed to get catalog: %v", err)
	}
	if cat == nil {
		t.Error("expected non-nil catalog")
	}

	// Get catalog again - should return cached
	cat2, err := dm.GetCatalog("default")
	if err != nil {
		t.Fatalf("failed to get catalog second time: %v", err)
	}
	if cat != cat2 {
		t.Error("expected same catalog instance (cached)")
	}

	// Get catalog for non-existent database
	_, err = dm.GetCatalog("nonexistent")
	if err == nil {
		t.Error("expected error getting catalog for non-existent database")
	}
}

func TestDatabaseManager_Persistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dbmgr_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create manager and databases
	dm1, err := NewDatabaseManager(tmpDir)
	if err != nil {
		t.Fatalf("failed to create database manager: %v", err)
	}

	_, err = dm1.CreateDatabase("persistent1", "owner1")
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	_, err = dm1.CreateDatabase("persistent2", "owner2")
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	// Create new manager instance (simulates restart)
	dm2, err := NewDatabaseManager(tmpDir)
	if err != nil {
		t.Fatalf("failed to create second database manager: %v", err)
	}

	// Verify databases were loaded
	if !dm2.DatabaseExists("default") {
		t.Error("expected 'default' to exist after reload")
	}
	if !dm2.DatabaseExists("persistent1") {
		t.Error("expected 'persistent1' to exist after reload")
	}
	if !dm2.DatabaseExists("persistent2") {
		t.Error("expected 'persistent2' to exist after reload")
	}

	// Verify metadata was preserved
	info, err := dm2.GetDatabase("persistent1")
	if err != nil {
		t.Fatalf("failed to get database: %v", err)
	}
	if info.Owner != "owner1" {
		t.Errorf("expected owner 'owner1', got %q", info.Owner)
	}
}

func TestDatabaseManager_GetDatabasePath(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dbmgr_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dm, err := NewDatabaseManager(tmpDir)
	if err != nil {
		t.Fatalf("failed to create database manager: %v", err)
	}

	// Get path for default database
	path, err := dm.GetDatabasePath("default")
	if err != nil {
		t.Fatalf("failed to get database path: %v", err)
	}
	expectedPath := filepath.Join(tmpDir, "default")
	if path != expectedPath {
		t.Errorf("expected path %q, got %q", expectedPath, path)
	}

	// Get path for non-existent database
	_, err = dm.GetDatabasePath("nonexistent")
	if err == nil {
		t.Error("expected error getting path for non-existent database")
	}
}
