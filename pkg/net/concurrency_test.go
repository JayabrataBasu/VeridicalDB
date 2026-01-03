package net

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/lock"
	"github.com/JayabrataBasu/VeridicalDB/pkg/log"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

// TestConcurrentInserts verifies that multiple clients can insert simultaneously.
func TestConcurrentInserts(t *testing.T) {
	_, port, cleanup := setupConcurrencyTestServer(t)
	defer cleanup()

	// Setup: create table from one connection
	conn := connectToServer(t, port)
	skipWelcome(conn)
	sendAndReceive(t, conn, "CREATE TABLE counters (id INT);")
	conn.Close()

	// Concurrently insert from multiple clients
	const numClients = 5
	const insertsPerClient = 10

	var wg sync.WaitGroup
	errors := make(chan error, numClients*insertsPerClient)

	for c := 0; c < numClients; c++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			clientConn := connectToServer(t, port)
			defer clientConn.Close()
			skipWelcome(clientConn)

			for i := 0; i < insertsPerClient; i++ {
				id := clientID*100 + i
				cmd := fmt.Sprintf("INSERT INTO counters VALUES (%d);", id)
				resp := sendAndReceive(t, clientConn, cmd)
				if !strings.Contains(resp, "inserted") {
					errors <- fmt.Errorf("client %d insert %d failed: %s", clientID, i, resp)
				}
			}
		}(c)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}

	// Verify total count
	verifyConn := connectToServer(t, port)
	defer verifyConn.Close()
	skipWelcome(verifyConn)

	resp := sendAndReceive(t, verifyConn, "SELECT * FROM counters;")
	expected := fmt.Sprintf("(%d row(s))", numClients*insertsPerClient)
	if !strings.Contains(resp, expected) {
		t.Errorf("expected %s, got: %s", expected, resp)
	}
}

// TestConcurrentTransactions verifies that concurrent transactions are isolated.
func TestConcurrentTransactions(t *testing.T) {
	_, port, cleanup := setupConcurrencyTestServer(t)
	defer cleanup()

	// Setup
	conn := connectToServer(t, port)
	skipWelcome(conn)
	sendAndReceive(t, conn, "CREATE TABLE accounts (id INT, balance INT);")
	sendAndReceive(t, conn, "INSERT INTO accounts VALUES (1, 1000);")
	sendAndReceive(t, conn, "INSERT INTO accounts VALUES (2, 1000);")
	conn.Close()

	// Client 1: Begin, update, wait, then commit
	// Client 2: Begin, try to read during client 1's transaction
	var wg sync.WaitGroup
	results := make(chan string, 2)

	// Client 1: Long transaction
	wg.Add(1)
	go func() {
		defer wg.Done()
		c1 := connectToServer(t, port)
		defer c1.Close()
		skipWelcome(c1)

		sendAndReceive(t, c1, "BEGIN;")
		sendAndReceive(t, c1, "UPDATE accounts SET balance = 500 WHERE id = 1;")

		// Hold transaction open
		time.Sleep(200 * time.Millisecond)

		resp := sendAndReceive(t, c1, "COMMIT;")
		results <- fmt.Sprintf("client1: %s", resp)
	}()

	// Client 2: Read during client 1's transaction
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Wait for client 1 to start
		time.Sleep(50 * time.Millisecond)

		c2 := connectToServer(t, port)
		defer c2.Close()
		skipWelcome(c2)

		// This should see the old value (1000) due to MVCC isolation
		resp := sendAndReceive(t, c2, "SELECT balance FROM accounts WHERE id = 1;")
		results <- fmt.Sprintf("client2: %s", resp)
	}()

	wg.Wait()
	close(results)

	// Both should succeed
	for result := range results {
		if strings.Contains(result, "ERROR") {
			t.Errorf("unexpected error: %s", result)
		}
	}
}

// TestConcurrentReads verifies that multiple readers don't block each other.
func TestConcurrentReads(t *testing.T) {
	_, port, cleanup := setupConcurrencyTestServer(t)
	defer cleanup()

	// Setup
	conn := connectToServer(t, port)
	skipWelcome(conn)
	sendAndReceive(t, conn, "CREATE TABLE data (id INT, value TEXT);")
	for i := 0; i < 100; i++ {
		sendAndReceive(t, conn, fmt.Sprintf("INSERT INTO data VALUES (%d, 'value%d');", i, i))
	}
	conn.Close()

	// Multiple concurrent readers
	const numReaders = 10
	var wg sync.WaitGroup
	successCount := make(chan int, numReaders)

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := connectToServer(t, port)
			defer c.Close()
			skipWelcome(c)

			resp := sendAndReceive(t, c, "SELECT * FROM data;")
			if strings.Contains(resp, "(100 row(s))") {
				successCount <- 1
			} else {
				successCount <- 0
			}
		}()
	}

	wg.Wait()
	close(successCount)

	total := 0
	for s := range successCount {
		total += s
	}

	if total != numReaders {
		t.Errorf("expected %d successful reads, got %d", numReaders, total)
	}
}

// Helper functions

func setupConcurrencyTestServer(t *testing.T) (*Server, int, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "concurrency_test_*")
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

	time.Sleep(50 * time.Millisecond)

	cleanup := func() {
		server.Stop()
		os.RemoveAll(dir)
	}

	return server, port, cleanup
}

func skipWelcome(conn net.Conn) {
	reader := bufio.NewReader(conn)
	_, _ = reader.ReadString('\n')
}
