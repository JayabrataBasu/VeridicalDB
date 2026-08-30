package engine

import (
	"strings"
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/exec"
)

func openTemp(t *testing.T, durable bool) *DB {
	t.Helper()
	db, err := Open(Config{DataDir: t.TempDir(), Durable: durable})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// sessionOn returns a fresh session with the given database selected. Multi-
// database support is wired in, so every session must select a database before
// DDL/DML — the same contract the REPL and TUI have always had.
func sessionOn(t *testing.T, db *DB, database string) *exec.Session {
	t.Helper()
	s := db.NewSession()
	t.Cleanup(s.Close)
	if _, err := s.ExecuteSQL("USE " + database); err != nil {
		t.Fatalf("USE %s: %v", database, err)
	}
	return s
}

func mustExec(t *testing.T, s *exec.Session, query string) {
	t.Helper()
	if _, err := s.ExecuteSQL(query); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// TestSessionHasEveryCapability is the P1 guarantee: a session from the engine
// can reach every optional feature. If any of these fails with a "not
// configured" / "not enabled" error, NewSession forgot a setter.
func TestSessionHasEveryCapability(t *testing.T) {
	db := openTemp(t, false)

	setup := db.NewSession()
	defer setup.Close()
	mustExec(t, setup, `CREATE DATABASE app`)
	mustExec(t, setup, `USE app`)
	mustExec(t, setup, `CREATE TABLE t (id INT PRIMARY KEY, name TEXT, body TEXT)`)
	mustExec(t, setup, `INSERT INTO t (id, name, body) VALUES (1, 'a', 'hello world')`)

	cases := []struct{ name, sql string }{
		{"secondary_index", `CREATE INDEX idx_t_name ON t (name)`},
		{"full_text_search", `CREATE FULLTEXT INDEX fts_t_body ON t (body)`},
		{"trigger", `CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION noop()`},
		{"stored_procedure", `CREATE PROCEDURE bump() AS $$ BEGIN UPDATE t SET id = id; END $$ LANGUAGE plpgsql`},
		{"multi_database", `CREATE DATABASE scratch`},
	}

	notWired := []string{"not configured", "not enabled"}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := sessionOn(t, db, "app")
			_, err := s.ExecuteSQL(tc.sql)
			if err == nil {
				return
			}
			for _, marker := range notWired {
				if strings.Contains(err.Error(), marker) {
					t.Fatalf("capability not wired into NewSession: %v", err)
				}
			}
			// A different error (parser quirk, missing referenced function) is
			// out of scope — the capability itself was reachable.
			t.Logf("reached the feature; downstream error: %v", err)
		})
	}
}

// TestDurableSurvivesReopen checks that a committed row survives a Durable
// close/reopen cycle: the session writes a COMMIT record through the WAL txn
// logger, and recovery redoes it.
func TestDurableSurvivesReopen(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Config{DataDir: dir, Durable: true})
	if err != nil {
		t.Fatalf("Open #1: %v", err)
	}
	s := db.NewSession()
	mustExec(t, s, `CREATE DATABASE d`)
	mustExec(t, s, `USE d`)
	mustExec(t, s, `CREATE TABLE k (id INT PRIMARY KEY, v TEXT)`)
	mustExec(t, s, `INSERT INTO k (id, v) VALUES (1, 'persisted')`)
	s.Close()
	if err := db.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}

	db2, err := Open(Config{DataDir: dir, Durable: true})
	if err != nil {
		t.Fatalf("Open #2 (recovery) failed: %v", err)
	}
	defer func() { _ = db2.Close() }()

	s2 := db2.NewSession()
	defer s2.Close()
	mustExec(t, s2, `USE d`)
	res, err := s2.ExecuteSQL(`SELECT v FROM k WHERE id = 1`)
	if err != nil {
		t.Fatalf("select after reopen: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("committed row did not survive reopen: want 1 row, got %d", len(res.Rows))
	}
}

// TestDurableRollbackDiscardedOnReopen checks the other direction: an explicitly
// rolled-back write must not reappear after recovery.
func TestDurableRollbackDiscardedOnReopen(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Config{DataDir: dir, Durable: true})
	if err != nil {
		t.Fatalf("Open #1: %v", err)
	}
	s := db.NewSession()
	mustExec(t, s, `CREATE DATABASE d`)
	mustExec(t, s, `USE d`)
	mustExec(t, s, `CREATE TABLE k (id INT PRIMARY KEY, v TEXT)`)
	mustExec(t, s, `INSERT INTO k (id, v) VALUES (1, 'keep')`)
	mustExec(t, s, `BEGIN`)
	mustExec(t, s, `INSERT INTO k (id, v) VALUES (2, 'discard')`)
	mustExec(t, s, `ROLLBACK`)
	s.Close()
	if err := db.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}

	db2, err := Open(Config{DataDir: dir, Durable: true})
	if err != nil {
		t.Fatalf("Open #2: %v", err)
	}
	defer func() { _ = db2.Close() }()

	s2 := db2.NewSession()
	defer s2.Close()
	mustExec(t, s2, `USE d`)
	res, err := s2.ExecuteSQL(`SELECT id FROM k ORDER BY id`)
	if err != nil {
		t.Fatalf("select after reopen: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("want only the committed row after reopen, got %d rows", len(res.Rows))
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	db := openTemp(t, true)
	if err := db.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close #2: %v", err)
	}
}

func TestOpenRequiresDataDir(t *testing.T) {
	if _, err := Open(Config{}); err == nil {
		t.Fatal("expected an error when DataDir is empty")
	}
}
