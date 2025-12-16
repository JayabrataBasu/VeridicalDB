package sql

import (
	"strings"
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

func newTestSession(t *testing.T) *Session {
	t.Helper()
	dataDir := t.TempDir()
	tm, err := catalog.NewTableManager(dataDir, 4096, nil)
	if err != nil {
		t.Fatalf("NewTableManager error: %v", err)
	}
	txnMgr := txn.NewManager()
	mtm := catalog.NewMVCCTableManager(tm, txnMgr, nil)
	return NewSessionWithLocks(mtm, nil)
}

func TestCreateTableRequiresDatabase(t *testing.T) {
	s := newTestSession(t)

	// Attach a DatabaseManager (server mode) but do not select a database
	dm, err := catalog.NewDatabaseManager(t.TempDir())
	if err != nil {
		t.Fatalf("NewDatabaseManager error: %v", err)
	}
	s.SetDatabaseManager(dm)

	// Attempt to create table without selecting a database
	_, err = s.ExecuteSQL("CREATE TABLE foo (id INT);")
	if err == nil || !strings.Contains(err.Error(), "no database selected") {
		t.Fatalf("expected error about no database selected, got: %v", err)
	}
}

func TestCreateTableAfterCreateAndUseDatabase(t *testing.T) {
	s := newTestSession(t)

	// Create DatabaseManager and set it on session
	root := t.TempDir()
	dm, err := catalog.NewDatabaseManager(root)
	if err != nil {
		t.Fatalf("NewDatabaseManager error: %v", err)
	}
	s.SetDatabaseManager(dm)

	// Create a database and use it
	if _, err := s.ExecuteSQL("CREATE DATABASE mydb;"); err != nil {
		t.Fatalf("CREATE DATABASE failed: %v", err)
	}
	if _, err := s.ExecuteSQL("USE mydb;"); err != nil {
		t.Fatalf("USE mydb failed: %v", err)
	}

	// Now CREATE TABLE should succeed
	if _, err := s.ExecuteSQL("CREATE TABLE foo (id INT);"); err != nil {
		t.Fatalf("CREATE TABLE failed even after selecting DB: %v", err)
	}
}
