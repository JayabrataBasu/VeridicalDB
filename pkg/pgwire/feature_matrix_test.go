package pgwire

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/engine"
	"github.com/JayabrataBasu/VeridicalDB/pkg/log"
)

// TestWireProtocolFeatureMatrix drives the full SQL feature surface over a real
// socket, against a server assembled the way cmd/server assembles it: an
// engine.DB whose NewSession wires in every optional capability.
//
// This is the acceptance test for plan phase P1 (engine.Open). Before P1 the
// "advanced" cases failed over the wire ("index manager not configured", etc.)
// because pgwire built a bare sql.NewSession; they now pass because the server
// is handed a fully-wired session factory.
//
// See: VeridicalDB Atlas, findings F1 + F2.
func TestWireProtocolFeatureMatrix(t *testing.T) {
	conn := startFeatureMatrixServer(t)

	if _, err := startupAndDrainReady(conn); err != nil {
		t.Fatalf("startup handshake failed: %v", err)
	}

	// Multi-database support is wired in, so a database must be selected first
	// — the same contract the REPL and TUI have always had.
	mustWireQuery(t, conn, `CREATE DATABASE app`)
	mustWireQuery(t, conn, `USE app`)

	t.Run("core/ddl_dml", func(t *testing.T) {
		mustWireQuery(t, conn, `CREATE TABLE t (id INT PRIMARY KEY, name TEXT, age INT)`)
		mustWireQuery(t, conn, `INSERT INTO t (id, name, age) VALUES (1, 'ada', 36), (2, 'alan', 41)`)
		mustWireQuery(t, conn, `SELECT id, name FROM t WHERE age > 38`)
		mustWireQuery(t, conn, `UPDATE t SET age = 37 WHERE id = 1`)
		mustWireQuery(t, conn, `DELETE FROM t WHERE id = 2`)
	})

	t.Run("core/aggregates", func(t *testing.T) {
		mustWireQuery(t, conn, `SELECT COUNT(*), AVG(age) FROM t`)
	})

	t.Run("core/transactions", func(t *testing.T) {
		mustWireQuery(t, conn, `BEGIN`)
		mustWireQuery(t, conn, `INSERT INTO t (id, name, age) VALUES (9, 'temp', 1)`)
		mustWireQuery(t, conn, `ROLLBACK`)
	})

	// These reach capabilities that a bare NewSession leaves nil. They are the
	// P1 acceptance criteria: each must succeed over the wire.
	t.Run("advanced/secondary_index", func(t *testing.T) {
		mustWireQuery(t, conn, `CREATE INDEX idx_t_name ON t (name)`)
		mustWireQuery(t, conn, `SELECT name FROM t WHERE age = 37`)
	})

	t.Run("advanced/full_text_search", func(t *testing.T) {
		mustWireQuery(t, conn, `CREATE FULLTEXT INDEX fts_t_name ON t (name)`)
	})

	t.Run("advanced/multi_database", func(t *testing.T) {
		mustWireQuery(t, conn, `CREATE DATABASE scratch`)
	})

	// Terminate cleanly.
	_, _ = conn.Write([]byte{MsgTerminate, 0, 0, 0, 4})
}

// mustWireQuery fails the test if the query produces an ErrorResponse.
func mustWireQuery(t *testing.T, conn net.Conn, query string) {
	t.Helper()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	msgs, err := sendSimpleQueryAndDrainReady(conn, query)
	if err != nil {
		t.Fatalf("transport error running %q: %v", query, err)
	}
	if msg, ok := firstMessage(msgs, MsgErrorResponse); ok {
		m := readErrorField(msg.payload, 'M')
		if m == "" {
			m = "<no message field>"
		}
		t.Fatalf("query %q failed: %s", query, m)
	}
}

func startFeatureMatrixServer(t *testing.T) net.Conn {
	t.Helper()

	db, err := engine.Open(engine.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("engine.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	server := NewServer(ServerConfig{
		Logger:     log.New(io.Discard, log.LevelError, log.FormatText),
		NewSession: db.NewSession,
	})
	if err := server.Start(0); err != nil {
		t.Fatalf("server.Start: %v", err)
	}
	t.Cleanup(func() { _ = server.Stop() })

	addr := server.listener.Addr().(*net.TCPAddr)
	conn, err := net.DialTimeout("tcp", addr.String(), 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}
