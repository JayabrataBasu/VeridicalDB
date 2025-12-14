package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCreateInsertFetchAcrossRestart(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "data")
	s := NewStorage(dataDir, 4096, nil)

	tableName := "users"
	if err := s.CreateTable(tableName); err != nil {
		t.Fatalf("CreateTable error: %v", err)
	}

	// insert some rows
	rows := [][]byte{[]byte("Alice"), []byte("Bob"), []byte("Carol")}
	rids := make([]RID, 0, len(rows))
	for _, r := range rows {
		rid, err := s.Insert(tableName, r)
		if err != nil {
			t.Fatalf("Insert error: %v", err)
		}
		rids = append(rids, rid)
	}

	// reopen storage (new instance pointing to same directory)
	s2 := NewStorage(dataDir, 4096, nil)
	for i, rid := range rids {
		got, err := s2.Fetch(rid)
		if err != nil {
			t.Fatalf("Fetch error: %v", err)
		}
		if string(got) != string(rows[i]) {
			t.Fatalf("mismatch: got %q want %q", string(got), string(rows[i]))
		}
	}

	// cleanup check: ensure table file exists
	tblPath := filepath.Join(dataDir, tableFileName(tableName))
	if _, err := os.Stat(tblPath); err != nil {
		t.Fatalf("table file missing: %v", err)
	}
}
