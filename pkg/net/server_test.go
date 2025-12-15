package net

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/lock"
	"github.com/JayabrataBasu/VeridicalDB/pkg/log"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

func setupTestServer(t *testing.T) (*Server, int, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "server_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	tm, err := catalog.NewTableManager(dir, 8192, nil)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("failed to create table manager: %v", err)
	}

	txnMgr := txn.NewManager()
	mtm := catalog.NewMVCCTableManager(tm, txnMgr, nil)
	lockMgr := lock.NewManager()
	logger := log.New(os.Stderr, log.LevelError, log.FormatText)

	cfg := ServerConfig{
		Logger:  logger,
		MTM:     mtm,
		TxnMgr:  txnMgr,
		LockMgr: lockMgr,
	}

	server := NewServer(cfg)

	// Find a free port
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("failed to find free port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	err = server.Start(port)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("failed to start server: %v", err)
	}

	// Give server time to start
	time.Sleep(50 * time.Millisecond)

	cleanup := func() {
		server.Stop()
		os.RemoveAll(dir)
	}

	return server, port, cleanup
}

func connectToServer(t *testing.T, port int) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	return conn
}

func sendAndReceive(t *testing.T, conn net.Conn, command string) string {
	t.Helper()

	// Set timeout
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	// Send command
	_, err := conn.Write([]byte(command + "\n"))
	if err != nil {
		t.Fatalf("failed to send: %v", err)
	}

	// Read response
	reader := bufio.NewReader(conn)
	var response strings.Builder
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		response.WriteString(line)
		// Check if we've received a complete response
		if strings.Contains(line, "row(s))") ||
			strings.Contains(line, "ERROR") ||
			strings.Contains(line, "Table") ||
			strings.Contains(line, "Goodbye") ||
			strings.Contains(line, "BEGIN") ||
			strings.Contains(line, "COMMIT") ||
			strings.Contains(line, "ROLLBACK") ||
			strings.Contains(line, "inserted") ||
			strings.Contains(line, "updated") ||
			strings.Contains(line, "deleted") ||
			strings.Contains(line, "Commands:") {
			break
		}
	}
	return response.String()
}

func TestServerStartStop(t *testing.T) {
	server, _, cleanup := setupTestServer(t)
	defer cleanup()

	if server.ActiveConnections() != 0 {
		t.Errorf("expected 0 active connections, got %d", server.ActiveConnections())
	}
}

func TestServerConnection(t *testing.T) {
	server, port, cleanup := setupTestServer(t)
	defer cleanup()

	conn := connectToServer(t, port)
	defer conn.Close()

	// Read welcome message
	reader := bufio.NewReader(conn)
	welcome, _ := reader.ReadString('\n')
	if !strings.Contains(welcome, "VeridicalDB") {
		t.Errorf("expected welcome message, got: %s", welcome)
	}

	// Give server time to register connection
	time.Sleep(50 * time.Millisecond)

	if server.ActiveConnections() != 1 {
		t.Errorf("expected 1 active connection, got %d", server.ActiveConnections())
	}
}

func TestServerHelp(t *testing.T) {
	_, port, cleanup := setupTestServer(t)
	defer cleanup()

	conn := connectToServer(t, port)
	defer conn.Close()

	// Skip welcome
	reader := bufio.NewReader(conn)
	reader.ReadString('\n')

	response := sendAndReceive(t, conn, "HELP;")
	if !strings.Contains(response, "Commands:") {
		t.Errorf("expected help text, got: %s", response)
	}
}

func TestServerCreateAndQuery(t *testing.T) {
	_, port, cleanup := setupTestServer(t)
	defer cleanup()

	conn := connectToServer(t, port)
	defer conn.Close()

	// Skip welcome
	reader := bufio.NewReader(conn)
	reader.ReadString('\n')

	// Create table
	response := sendAndReceive(t, conn, "CREATE TABLE users (id INT, name TEXT);")
	if !strings.Contains(response, "Table") {
		t.Errorf("expected table creation message, got: %s", response)
	}

	// Insert
	response = sendAndReceive(t, conn, "INSERT INTO users VALUES (1, 'Alice');")
	if !strings.Contains(response, "inserted") {
		t.Errorf("expected insert confirmation, got: %s", response)
	}

	// Select
	response = sendAndReceive(t, conn, "SELECT * FROM users;")
	if !strings.Contains(response, "Alice") {
		t.Errorf("expected to see Alice, got: %s", response)
	}
	if !strings.Contains(response, "(1 row(s))") {
		t.Errorf("expected 1 row, got: %s", response)
	}
}

func TestServerTransaction(t *testing.T) {
	_, port, cleanup := setupTestServer(t)
	defer cleanup()

	conn := connectToServer(t, port)
	defer conn.Close()

	// Skip welcome
	reader := bufio.NewReader(conn)
	reader.ReadString('\n')

	// Create table
	sendAndReceive(t, conn, "CREATE TABLE items (id INT);")

	// Begin transaction
	response := sendAndReceive(t, conn, "BEGIN;")
	if !strings.Contains(response, "BEGIN") {
		t.Errorf("expected BEGIN confirmation, got: %s", response)
	}

	// Insert
	sendAndReceive(t, conn, "INSERT INTO items VALUES (1);")

	// Commit
	response = sendAndReceive(t, conn, "COMMIT;")
	if !strings.Contains(response, "COMMIT") {
		t.Errorf("expected COMMIT confirmation, got: %s", response)
	}

	// Verify data persisted
	response = sendAndReceive(t, conn, "SELECT * FROM items;")
	if !strings.Contains(response, "(1 row(s))") {
		t.Errorf("expected 1 row after commit, got: %s", response)
	}
}

func TestServerMultipleConnections(t *testing.T) {
	server, port, cleanup := setupTestServer(t)
	defer cleanup()

	// Connect multiple clients
	conn1 := connectToServer(t, port)
	defer conn1.Close()

	conn2 := connectToServer(t, port)
	defer conn2.Close()

	// Give server time to register connections
	time.Sleep(100 * time.Millisecond)

	if server.ActiveConnections() != 2 {
		t.Errorf("expected 2 active connections, got %d", server.ActiveConnections())
	}

	// Skip welcome messages
	reader1 := bufio.NewReader(conn1)
	reader1.ReadString('\n')
	reader2 := bufio.NewReader(conn2)
	reader2.ReadString('\n')

	// Create table from conn1
	sendAndReceive(t, conn1, "CREATE TABLE shared (id INT);")

	// Insert from conn1
	sendAndReceive(t, conn1, "INSERT INTO shared VALUES (1);")

	// Query from conn2 should see the data
	response := sendAndReceive(t, conn2, "SELECT * FROM shared;")
	if !strings.Contains(response, "(1 row(s))") {
		t.Errorf("conn2 should see data from conn1, got: %s", response)
	}
}
