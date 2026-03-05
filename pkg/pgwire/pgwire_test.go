package pgwire

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/log"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

type backendMessage struct {
	msgType byte
	payload []byte
}

// TestProtocolMessages tests the message encoding/decoding functions.
func TestProtocolMessages(t *testing.T) {
	// Test Buffer operations
	buf := NewBuffer()
	buf.WriteInt32(12345)
	buf.WriteInt16(100)
	_ = buf.WriteByte('X')
	buf.WriteString("hello")
	buf.WriteBytes([]byte{1, 2, 3})

	data := buf.Bytes()

	// Verify Int32
	if ReadInt32(data[0:4]) != 12345 {
		t.Errorf("Expected 12345, got %d", ReadInt32(data[0:4]))
	}

	// Verify Int16
	if ReadInt16(data[4:6]) != 100 {
		t.Errorf("Expected 100, got %d", ReadInt16(data[4:6]))
	}

	// Verify byte
	if data[6] != 'X' {
		t.Errorf("Expected 'X', got %c", data[6])
	}

	// Verify string (null-terminated)
	str, n := ReadCString(data[7:])
	if str != "hello" {
		t.Errorf("Expected 'hello', got %q", str)
	}
	if n != 6 { // 5 chars + null
		t.Errorf("Expected 6 bytes consumed, got %d", n)
	}
}

// TestMessageWriter tests writing messages.
func TestMessageWriter(t *testing.T) {
	var buf bytes.Buffer
	mw := NewMessageWriter(&buf)

	payload := []byte("test payload")
	if err := mw.WriteMessage('Q', payload); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}

	data := buf.Bytes()

	// Check type byte
	if data[0] != 'Q' {
		t.Errorf("Expected type 'Q', got %c", data[0])
	}

	// Check length (4 bytes for length + payload)
	length := binary.BigEndian.Uint32(data[1:5])
	if length != uint32(4+len(payload)) {
		t.Errorf("Expected length %d, got %d", 4+len(payload), length)
	}

	// Check payload
	if !bytes.Equal(data[5:], payload) {
		t.Errorf("Payload mismatch")
	}
}

// TestMessageReader tests reading messages.
func TestMessageReader(t *testing.T) {
	// Build a valid message
	var buf bytes.Buffer

	// Type byte
	_ = buf.WriteByte('Q')

	// Length (including self)
	payload := []byte("SELECT 1")
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(4+len(payload)))
	buf.Write(lenBuf)

	// Payload
	buf.Write(payload)

	mr := NewMessageReader(&buf)
	msgType, msgPayload, err := mr.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage failed: %v", err)
	}

	if msgType != 'Q' {
		t.Errorf("Expected type 'Q', got %c", msgType)
	}

	if !bytes.Equal(msgPayload, payload) {
		t.Errorf("Payload mismatch: expected %q, got %q", payload, msgPayload)
	}
}

// TestStartupMessage tests reading startup messages.
func TestStartupMessage(t *testing.T) {
	var buf bytes.Buffer

	// Build startup message
	// Length (4 bytes) + protocol version (4 bytes) + params
	params := []byte("user\x00testuser\x00database\x00testdb\x00\x00")
	totalLen := 4 + 4 + len(params)

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(totalLen))
	buf.Write(lenBuf)

	// Protocol version 3.0
	verBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(verBuf, ProtocolVersionNumber)
	buf.Write(verBuf)

	// Parameters
	buf.Write(params)

	mr := NewMessageReader(&buf)
	length, payload, err := mr.ReadStartup()
	if err != nil {
		t.Fatalf("ReadStartup failed: %v", err)
	}

	if length != int32(totalLen) {
		t.Errorf("Expected length %d, got %d", totalLen, length)
	}

	// Check protocol version
	ver := ReadInt32(payload[0:4])
	if ver != ProtocolVersionNumber {
		t.Errorf("Expected protocol version %d, got %d", ProtocolVersionNumber, ver)
	}
}

// TestServerStartStop tests basic server lifecycle.
func TestServerStartStop(t *testing.T) {
	dir, err := os.MkdirTemp("", "pgwire_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	dataDir := filepath.Join(dir, "data")

	// Setup
	tm, err := catalog.NewTableManager(dataDir, 4096, nil)
	if err != nil {
		t.Fatalf("NewTableManager failed: %v", err)
	}

	txnMgr := txn.NewManager()
	mtm := catalog.NewMVCCTableManager(tm, txnMgr, nil)
	logger := log.New(os.Stderr, log.LevelError, log.FormatText)

	server := NewServer(ServerConfig{
		Port:   0, // Will be assigned
		Logger: logger,
		MTM:    mtm,
		TxnMgr: txnMgr,
	})

	// Start on a random port
	if err := server.Start(0); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Get the actual port
	addr := server.listener.Addr().(*net.TCPAddr)
	t.Logf("Server listening on port %d", addr.Port)

	// Stop
	if err := server.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
}

// TestSimpleConnection tests a basic client connection and query.
func TestSimpleConnection(t *testing.T) {
	dir, err := os.MkdirTemp("", "pgwire_conn_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	dataDir := filepath.Join(dir, "data")

	// Setup server
	tm, err := catalog.NewTableManager(dataDir, 4096, nil)
	if err != nil {
		t.Fatalf("NewTableManager failed: %v", err)
	}

	txnMgr := txn.NewManager()
	mtm := catalog.NewMVCCTableManager(tm, txnMgr, nil)
	logger := log.New(io.Discard, log.LevelError, log.FormatText)

	server := NewServer(ServerConfig{
		Logger: logger,
		MTM:    mtm,
		TxnMgr: txnMgr,
	})

	if err := server.Start(0); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = server.Stop() }()

	addr := server.listener.Addr().(*net.TCPAddr)

	// Connect as client
	conn, err := net.DialTimeout("tcp", addr.String(), 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// Send startup message
	var startupBuf bytes.Buffer

	// Length placeholder
	startupBuf.Write([]byte{0, 0, 0, 0})

	// Protocol version 3.0
	_ = binary.Write(&startupBuf, binary.BigEndian, int32(ProtocolVersionNumber))

	// Parameters
	startupBuf.WriteString("user")
	startupBuf.WriteByte(0)
	startupBuf.WriteString("test")
	startupBuf.WriteByte(0)
	startupBuf.WriteString("database")
	startupBuf.WriteByte(0)
	startupBuf.WriteString("test")
	startupBuf.WriteByte(0)
	startupBuf.WriteByte(0) // terminator

	// Write length
	startupBytes := startupBuf.Bytes()
	binary.BigEndian.PutUint32(startupBytes[0:4], uint32(len(startupBytes)))

	if _, err := conn.Write(startupBytes); err != nil {
		t.Fatalf("Failed to send startup: %v", err)
	}

	// Read response (should get AuthenticationOK, ParameterStatus messages, BackendKeyData, ReadyForQuery)
	response := make([]byte, 4096)
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := conn.Read(response)
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	t.Logf("Received %d bytes in response", n)

	// Look for AuthenticationOK (R + 4-byte length + 4-byte auth type 0)
	foundAuth := false
	for i := 0; i < n-8; i++ {
		if response[i] == MsgAuthentication {
			authType := binary.BigEndian.Uint32(response[i+5 : i+9])
			if authType == AuthOK {
				foundAuth = true
				break
			}
		}
	}

	if !foundAuth {
		t.Error("Did not receive AuthenticationOK")
	}

	// Look for ReadyForQuery
	foundReady := false
	for i := 0; i < n-5; i++ {
		if response[i] == MsgReadyForQuery {
			foundReady = true
			break
		}
	}

	if !foundReady {
		t.Error("Did not receive ReadyForQuery")
	}

	// Send a simple query
	query := "CREATE TABLE test (id INT, name TEXT);\x00"
	queryMsg := make([]byte, 1+4+len(query))
	queryMsg[0] = MsgQuery
	binary.BigEndian.PutUint32(queryMsg[1:5], uint32(4+len(query)))
	copy(queryMsg[5:], query)

	if _, err := conn.Write(queryMsg); err != nil {
		t.Fatalf("Failed to send query: %v", err)
	}

	// Read response
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err = conn.Read(response)
	if err != nil {
		t.Fatalf("Failed to read query response: %v", err)
	}

	t.Logf("Query response: %d bytes", n)

	// Look for CommandComplete or ErrorResponse
	foundComplete := false
	foundError := false
	for i := 0; i < n; i++ {
		if response[i] == MsgCommandComplete {
			foundComplete = true
		}
		if response[i] == MsgErrorResponse {
			foundError = true
		}
	}

	if foundError {
		t.Log("Query returned an error (this may be expected for certain queries)")
	} else if !foundComplete {
		t.Error("Did not receive CommandComplete for query")
	}

	// Send Terminate
	terminate := []byte{MsgTerminate, 0, 0, 0, 4}
	_, _ = conn.Write(terminate)
}

func TestCancelRequestInvalidSecretDoesNotCloseConnection(t *testing.T) {
	dir, err := os.MkdirTemp("", "pgwire_cancel_invalid_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	dataDir := filepath.Join(dir, "data")
	tm, err := catalog.NewTableManager(dataDir, 4096, nil)
	if err != nil {
		t.Fatalf("NewTableManager failed: %v", err)
	}

	txnMgr := txn.NewManager()
	mtm := catalog.NewMVCCTableManager(tm, txnMgr, nil)
	logger := log.New(io.Discard, log.LevelError, log.FormatText)

	server := NewServer(ServerConfig{Logger: logger, MTM: mtm, TxnMgr: txnMgr})
	if err := server.Start(0); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = server.Stop() }()

	addr := server.listener.Addr().(*net.TCPAddr)
	conn, err := net.DialTimeout("tcp", addr.String(), 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	msgs, err := startupAndDrainReady(conn)
	if err != nil {
		t.Fatalf("startup failed: %v", err)
	}

	pid, secret, err := backendKeyDataFromMessages(msgs)
	if err != nil {
		t.Fatalf("missing BackendKeyData: %v", err)
	}

	cancelConn, err := net.DialTimeout("tcp", addr.String(), 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to open cancel connection: %v", err)
	}
	_ = sendCancelRequest(cancelConn, pid, secret+1)
	_ = cancelConn.Close()

	msgs, err = sendSimpleQueryAndDrainReady(conn, "SELECT 1")
	if err != nil {
		t.Fatalf("connection should remain usable after invalid CancelRequest: %v", err)
	}
	if !containsMsgType(msgs, MsgReadyForQuery) {
		t.Fatalf("expected ReadyForQuery after invalid CancelRequest")
	}
}

func TestCancelRequestValidSecretClosesConnection(t *testing.T) {
	dir, err := os.MkdirTemp("", "pgwire_cancel_valid_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	dataDir := filepath.Join(dir, "data")
	tm, err := catalog.NewTableManager(dataDir, 4096, nil)
	if err != nil {
		t.Fatalf("NewTableManager failed: %v", err)
	}

	txnMgr := txn.NewManager()
	mtm := catalog.NewMVCCTableManager(tm, txnMgr, nil)
	logger := log.New(io.Discard, log.LevelError, log.FormatText)

	server := NewServer(ServerConfig{Logger: logger, MTM: mtm, TxnMgr: txnMgr})
	if err := server.Start(0); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = server.Stop() }()

	addr := server.listener.Addr().(*net.TCPAddr)
	conn, err := net.DialTimeout("tcp", addr.String(), 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	msgs, err := startupAndDrainReady(conn)
	if err != nil {
		t.Fatalf("startup failed: %v", err)
	}

	pid, secret, err := backendKeyDataFromMessages(msgs)
	if err != nil {
		t.Fatalf("missing BackendKeyData: %v", err)
	}

	cancelConn, err := net.DialTimeout("tcp", addr.String(), 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to open cancel connection: %v", err)
	}
	if err := sendCancelRequest(cancelConn, pid, secret); err != nil {
		t.Fatalf("send cancel request failed: %v", err)
	}
	_ = cancelConn.Close()

	var lastErr error
	for i := 0; i < 10; i++ {
		_, lastErr = sendSimpleQueryAndDrainReady(conn, "SELECT 1")
		if lastErr != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if lastErr == nil {
		t.Fatalf("expected connection to be closed after valid CancelRequest")
	}
}

// TestSSLRequest tests that the server correctly rejects SSL requests.
func TestSSLRequest(t *testing.T) {
	dir, err := os.MkdirTemp("", "pgwire_ssl_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	dataDir := filepath.Join(dir, "data")

	tm, err := catalog.NewTableManager(dataDir, 4096, nil)
	if err != nil {
		t.Fatalf("NewTableManager failed: %v", err)
	}

	txnMgr := txn.NewManager()
	mtm := catalog.NewMVCCTableManager(tm, txnMgr, nil)
	logger := log.New(io.Discard, log.LevelError, log.FormatText)

	server := NewServer(ServerConfig{
		Logger: logger,
		MTM:    mtm,
		TxnMgr: txnMgr,
	})

	if err := server.Start(0); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = server.Stop() }()

	addr := server.listener.Addr().(*net.TCPAddr)

	conn, err := net.DialTimeout("tcp", addr.String(), 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// Send SSL request
	sslRequest := make([]byte, 8)
	binary.BigEndian.PutUint32(sslRequest[0:4], 8)              // length
	binary.BigEndian.PutUint32(sslRequest[4:8], SSLRequestCode) // SSL request code

	if _, err := conn.Write(sslRequest); err != nil {
		t.Fatalf("Failed to send SSL request: %v", err)
	}

	// Should receive 'N' (SSL not supported)
	response := make([]byte, 1)
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Read(response); err != nil {
		t.Fatalf("Failed to read SSL response: %v", err)
	}

	if response[0] != 'N' {
		t.Errorf("Expected 'N' for SSL rejection, got %c", response[0])
	}
}

// TestTLSConnection tests TLS-enabled server connections.
func TestTLSConnection(t *testing.T) {
	dir, err := os.MkdirTemp("", "pgwire_tls_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	dataDir := filepath.Join(dir, "data")

	// Generate test certificates
	certPEM, keyPEM, caPEM := generateTestCerts(t)

	certFile := filepath.Join(dir, "server.crt")
	keyFile := filepath.Join(dir, "server.key")
	caFile := filepath.Join(dir, "ca.crt")

	if err := os.WriteFile(certFile, certPEM, 0600); err != nil {
		t.Fatalf("failed to write cert file: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0600); err != nil {
		t.Fatalf("failed to write key file: %v", err)
	}
	if err := os.WriteFile(caFile, caPEM, 0600); err != nil {
		t.Fatalf("failed to write ca file: %v", err)
	}

	// Load server TLS config
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("failed to load cert: %v", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	// Setup server with TLS
	tm, err := catalog.NewTableManager(dataDir, 4096, nil)
	if err != nil {
		t.Fatalf("NewTableManager failed: %v", err)
	}

	txnMgr := txn.NewManager()
	mtm := catalog.NewMVCCTableManager(tm, txnMgr, nil)
	logger := log.New(io.Discard, log.LevelDebug, log.FormatText)

	server := NewServer(ServerConfig{
		Logger:    logger,
		MTM:       mtm,
		TxnMgr:    txnMgr,
		TLSConfig: tlsConfig,
	})

	if !server.TLSEnabled() {
		t.Fatal("Server should report TLS as enabled")
	}

	if err := server.Start(0); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = server.Stop() }()

	addr := server.listener.Addr().(*net.TCPAddr)

	t.Run("SSLRequest gets accepted", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", addr.String(), 5*time.Second)
		if err != nil {
			t.Fatalf("Failed to connect: %v", err)
		}
		defer func() { _ = conn.Close() }()

		// Send SSL request
		sslRequest := make([]byte, 8)
		binary.BigEndian.PutUint32(sslRequest[0:4], 8)
		binary.BigEndian.PutUint32(sslRequest[4:8], SSLRequestCode)

		if _, err := conn.Write(sslRequest); err != nil {
			t.Fatalf("Failed to send SSL request: %v", err)
		}

		// Should receive 'S' (SSL supported)
		response := make([]byte, 1)
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Read(response); err != nil {
			t.Fatalf("Failed to read SSL response: %v", err)
		}

		if response[0] != 'S' {
			t.Errorf("Expected 'S' for SSL acceptance, got %c", response[0])
		}
	})

	t.Run("TLS handshake and query", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", addr.String(), 5*time.Second)
		if err != nil {
			t.Fatalf("Failed to connect: %v", err)
		}
		defer func() { _ = conn.Close() }()

		// Send SSL request
		sslRequest := make([]byte, 8)
		binary.BigEndian.PutUint32(sslRequest[0:4], 8)
		binary.BigEndian.PutUint32(sslRequest[4:8], SSLRequestCode)

		if _, err := conn.Write(sslRequest); err != nil {
			t.Fatalf("Failed to send SSL request: %v", err)
		}

		// Read 'S' response
		response := make([]byte, 1)
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Read(response); err != nil {
			t.Fatalf("Failed to read SSL response: %v", err)
		}
		if response[0] != 'S' {
			t.Fatalf("Expected 'S', got %c", response[0])
		}

		// Perform TLS handshake
		clientTLS := tls.Client(conn, &tls.Config{
			InsecureSkipVerify: true, // Skip verification for test
		})
		if err := clientTLS.Handshake(); err != nil {
			t.Fatalf("TLS handshake failed: %v", err)
		}

		// Verify TLS is active
		state := clientTLS.ConnectionState()
		if !state.HandshakeComplete {
			t.Error("TLS handshake not complete")
		}
		t.Logf("TLS version: 0x%04x, cipher: %s", state.Version, tls.CipherSuiteName(state.CipherSuite))

		// Send startup message over TLS
		var startupBuf bytes.Buffer
		startupBuf.Write([]byte{0, 0, 0, 0}) // Length placeholder
		_ = binary.Write(&startupBuf, binary.BigEndian, int32(ProtocolVersionNumber))
		startupBuf.WriteString("user")
		startupBuf.WriteByte(0)
		startupBuf.WriteString("test")
		startupBuf.WriteByte(0)
		startupBuf.WriteString("database")
		startupBuf.WriteByte(0)
		startupBuf.WriteString("test")
		startupBuf.WriteByte(0)
		startupBuf.WriteByte(0)

		startupBytes := startupBuf.Bytes()
		binary.BigEndian.PutUint32(startupBytes[0:4], uint32(len(startupBytes)))

		if _, err := clientTLS.Write(startupBytes); err != nil {
			t.Fatalf("Failed to send startup: %v", err)
		}

		// Read response
		respBuf := make([]byte, 4096)
		_ = clientTLS.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, err := clientTLS.Read(respBuf)
		if err != nil {
			t.Fatalf("Failed to read response: %v", err)
		}

		// Look for AuthenticationOK
		foundAuth := false
		for i := 0; i < n-8; i++ {
			if respBuf[i] == MsgAuthentication {
				authType := binary.BigEndian.Uint32(respBuf[i+5 : i+9])
				if authType == AuthOK {
					foundAuth = true
					break
				}
			}
		}
		if !foundAuth {
			t.Error("Did not receive AuthenticationOK over TLS")
		}

		// Send Terminate
		terminate := []byte{MsgTerminate, 0, 0, 0, 4}
		_, _ = clientTLS.Write(terminate)
	})
}

// TestTLSWithoutRequest tests that clients can still connect without TLS when server supports it.
func TestTLSWithoutRequest(t *testing.T) {
	dir, err := os.MkdirTemp("", "pgwire_tls_optional_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	dataDir := filepath.Join(dir, "data")

	// Generate test certificates
	certPEM, keyPEM, _ := generateTestCerts(t)

	certFile := filepath.Join(dir, "server.crt")
	keyFile := filepath.Join(dir, "server.key")

	if err := os.WriteFile(certFile, certPEM, 0600); err != nil {
		t.Fatalf("failed to write cert file: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0600); err != nil {
		t.Fatalf("failed to write key file: %v", err)
	}

	// Load server TLS config
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("failed to load cert: %v", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	// Setup server with TLS
	tm, err := catalog.NewTableManager(dataDir, 4096, nil)
	if err != nil {
		t.Fatalf("NewTableManager failed: %v", err)
	}

	txnMgr := txn.NewManager()
	mtm := catalog.NewMVCCTableManager(tm, txnMgr, nil)
	logger := log.New(io.Discard, log.LevelError, log.FormatText)

	server := NewServer(ServerConfig{
		Logger:    logger,
		MTM:       mtm,
		TxnMgr:    txnMgr,
		TLSConfig: tlsConfig,
	})

	if err := server.Start(0); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = server.Stop() }()

	addr := server.listener.Addr().(*net.TCPAddr)

	// Connect without SSL request (plaintext startup)
	conn, err := net.DialTimeout("tcp", addr.String(), 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// Send startup message directly (no SSL request)
	var startupBuf bytes.Buffer
	startupBuf.Write([]byte{0, 0, 0, 0})
	_ = binary.Write(&startupBuf, binary.BigEndian, int32(ProtocolVersionNumber))
	startupBuf.WriteString("user")
	startupBuf.WriteByte(0)
	startupBuf.WriteString("test")
	startupBuf.WriteByte(0)
	startupBuf.WriteByte(0)

	startupBytes := startupBuf.Bytes()
	binary.BigEndian.PutUint32(startupBytes[0:4], uint32(len(startupBytes)))

	if _, err := conn.Write(startupBytes); err != nil {
		t.Fatalf("Failed to send startup: %v", err)
	}

	// Should still get a valid response (plaintext)
	response := make([]byte, 4096)
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := conn.Read(response)
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	// Look for AuthenticationOK
	foundAuth := false
	for i := 0; i < n-8; i++ {
		if response[i] == MsgAuthentication {
			authType := binary.BigEndian.Uint32(response[i+5 : i+9])
			if authType == AuthOK {
				foundAuth = true
				break
			}
		}
	}
	if !foundAuth {
		t.Error("Did not receive AuthenticationOK for plaintext connection")
	}
}

func TestExtendedQueryProtocolDescribeAndExecute(t *testing.T) {
	dir, err := os.MkdirTemp("", "pgwire_extended_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	dataDir := filepath.Join(dir, "data")
	tm, err := catalog.NewTableManager(dataDir, 4096, nil)
	if err != nil {
		t.Fatalf("NewTableManager failed: %v", err)
	}
	txnMgr := txn.NewManager()
	mtm := catalog.NewMVCCTableManager(tm, txnMgr, nil)
	logger := log.New(io.Discard, log.LevelError, log.FormatText)

	server := NewServer(ServerConfig{Logger: logger, MTM: mtm, TxnMgr: txnMgr})
	if err := server.Start(0); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = server.Stop() }()

	addr := server.listener.Addr().(*net.TCPAddr)
	conn, err := net.DialTimeout("tcp", addr.String(), 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := startupAndDrainReady(conn); err != nil {
		t.Fatalf("startup failed: %v", err)
	}
	if _, err := sendSimpleQueryAndDrainReady(conn, "CREATE TABLE t_ext (id INT)"); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	if _, err := sendSimpleQueryAndDrainReady(conn, "INSERT INTO t_ext VALUES (1)"); err != nil {
		t.Fatalf("insert row failed: %v", err)
	}

	if err := sendFrontendMessage(conn, MsgParse, buildParsePayload("s1", "SELECT id AS one FROM t_ext", nil)); err != nil {
		t.Fatalf("send parse failed: %v", err)
	}
	if err := sendFrontendMessage(conn, MsgDescribe, buildDescribePayload('S', "s1")); err != nil {
		t.Fatalf("send describe failed: %v", err)
	}
	if err := sendFrontendMessage(conn, MsgSync, nil); err != nil {
		t.Fatalf("send sync failed: %v", err)
	}

	msgs, err := readMessagesUntilReady(conn)
	if err != nil {
		t.Fatalf("read describe responses failed: %v", err)
	}

	if !containsMsgType(msgs, MsgParseComplete) {
		t.Fatalf("missing ParseComplete")
	}
	if !containsMsgType(msgs, MsgParameterDesc) {
		t.Fatalf("missing ParameterDescription")
	}

	rowDesc, ok := firstMessage(msgs, MsgRowDescription)
	if !ok {
		t.Fatalf("missing RowDescription")
	}
	colName, err := firstColumnName(rowDesc.payload)
	if err != nil {
		t.Fatalf("decode RowDescription failed: %v", err)
	}
	if colName != "one" {
		t.Fatalf("expected described column 'one', got %q", colName)
	}

	if err := sendFrontendMessage(conn, MsgBind, buildBindPayload("", "s1", nil, nil, nil)); err != nil {
		t.Fatalf("send bind failed: %v", err)
	}
	if err := sendFrontendMessage(conn, MsgExecute, buildExecutePayload("", 0)); err != nil {
		t.Fatalf("send execute failed: %v", err)
	}
	if err := sendFrontendMessage(conn, MsgSync, nil); err != nil {
		t.Fatalf("send sync failed: %v", err)
	}

	msgs, err = readMessagesUntilReady(conn)
	if err != nil {
		t.Fatalf("read execute responses failed: %v", err)
	}

	if !containsMsgType(msgs, MsgBindComplete) {
		t.Fatalf("missing BindComplete")
	}
	if !containsMsgType(msgs, MsgDataRow) {
		t.Fatalf("missing DataRow")
	}
	if !containsMsgType(msgs, MsgCommandComplete) {
		t.Fatalf("missing CommandComplete")
	}
}

func TestExtendedQueryProtocolInvalidTypedParam(t *testing.T) {
	dir, err := os.MkdirTemp("", "pgwire_extended_param_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	dataDir := filepath.Join(dir, "data")
	tm, err := catalog.NewTableManager(dataDir, 4096, nil)
	if err != nil {
		t.Fatalf("NewTableManager failed: %v", err)
	}
	txnMgr := txn.NewManager()
	mtm := catalog.NewMVCCTableManager(tm, txnMgr, nil)
	logger := log.New(io.Discard, log.LevelError, log.FormatText)

	server := NewServer(ServerConfig{Logger: logger, MTM: mtm, TxnMgr: txnMgr})
	if err := server.Start(0); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = server.Stop() }()

	addr := server.listener.Addr().(*net.TCPAddr)
	conn, err := net.DialTimeout("tcp", addr.String(), 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := startupAndDrainReady(conn); err != nil {
		t.Fatalf("startup failed: %v", err)
	}
	if _, err := sendSimpleQueryAndDrainReady(conn, "CREATE TABLE t_ext (id INT)"); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	if _, err := sendSimpleQueryAndDrainReady(conn, "INSERT INTO t_ext VALUES (1)"); err != nil {
		t.Fatalf("insert row failed: %v", err)
	}

	if err := sendFrontendMessage(conn, MsgParse, buildParsePayload("sbad", "SELECT id FROM t_ext WHERE id = $1", []int32{OIDInt4})); err != nil {
		t.Fatalf("send parse failed: %v", err)
	}
	if err := sendFrontendMessage(conn, MsgBind, buildBindPayload("", "sbad", nil, [][]byte{[]byte("abc")}, nil)); err != nil {
		t.Fatalf("send bind failed: %v", err)
	}
	if err := sendFrontendMessage(conn, MsgExecute, buildExecutePayload("", 0)); err != nil {
		t.Fatalf("send execute failed: %v", err)
	}
	if err := sendFrontendMessage(conn, MsgSync, nil); err != nil {
		t.Fatalf("send sync failed: %v", err)
	}

	msgs, err := readMessagesUntilReady(conn)
	if err != nil {
		t.Fatalf("read responses failed: %v", err)
	}

	errMsg, ok := firstMessage(msgs, MsgErrorResponse)
	if !ok {
		t.Fatalf("expected ErrorResponse for invalid int parameter")
	}

	code := readErrorField(errMsg.payload, FieldSQLStateCode)
	if code != "22P02" {
		t.Fatalf("expected SQLSTATE 22P02, got %q", code)
	}
}

func startupAndDrainReady(conn net.Conn) ([]backendMessage, error) {
	var startupBuf bytes.Buffer
	startupBuf.Write([]byte{0, 0, 0, 0})
	_ = binary.Write(&startupBuf, binary.BigEndian, int32(ProtocolVersionNumber))
	startupBuf.WriteString("user")
	startupBuf.WriteByte(0)
	startupBuf.WriteString("test")
	startupBuf.WriteByte(0)
	startupBuf.WriteString("database")
	startupBuf.WriteByte(0)
	startupBuf.WriteString("test")
	startupBuf.WriteByte(0)
	startupBuf.WriteByte(0)

	startupBytes := startupBuf.Bytes()
	binary.BigEndian.PutUint32(startupBytes[0:4], uint32(len(startupBytes)))

	if _, err := conn.Write(startupBytes); err != nil {
		return nil, err
	}

	return readMessagesUntilReady(conn)
}

func backendKeyDataFromMessages(msgs []backendMessage) (uint32, int32, error) {
	msg, ok := firstMessage(msgs, MsgBackendKeyData)
	if !ok {
		return 0, 0, fmt.Errorf("BackendKeyData not found")
	}
	if len(msg.payload) < 8 {
		return 0, 0, fmt.Errorf("BackendKeyData payload too short: %d", len(msg.payload))
	}

	pid := binary.BigEndian.Uint32(msg.payload[0:4])
	secret := int32(binary.BigEndian.Uint32(msg.payload[4:8]))
	return pid, secret, nil
}

func sendCancelRequest(conn net.Conn, pid uint32, secret int32) error {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint32(buf[0:4], uint32(16))
	binary.BigEndian.PutUint32(buf[4:8], uint32(CancelRequestCode))
	binary.BigEndian.PutUint32(buf[8:12], pid)
	binary.BigEndian.PutUint32(buf[12:16], uint32(secret))
	_, err := conn.Write(buf)
	return err
}

func sendSimpleQueryAndDrainReady(conn net.Conn, query string) ([]backendMessage, error) {
	payload := append([]byte(query), 0)
	if err := sendFrontendMessage(conn, MsgQuery, payload); err != nil {
		return nil, err
	}
	return readMessagesUntilReady(conn)
}

func sendFrontendMessage(conn net.Conn, msgType byte, payload []byte) error {
	msg := make([]byte, 1+4+len(payload))
	msg[0] = msgType
	binary.BigEndian.PutUint32(msg[1:5], uint32(4+len(payload)))
	copy(msg[5:], payload)
	_, err := conn.Write(msg)
	return err
}

func readMessagesUntilReady(conn net.Conn) ([]backendMessage, error) {
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var out []backendMessage

	for {
		hdr := make([]byte, 5)
		if _, err := io.ReadFull(conn, hdr); err != nil {
			return nil, err
		}
		msgType := hdr[0]
		msgLen := int(binary.BigEndian.Uint32(hdr[1:5]))
		if msgLen < 4 {
			return nil, fmt.Errorf("invalid backend message length: %d", msgLen)
		}
		payloadLen := msgLen - 4
		payload := make([]byte, payloadLen)
		if payloadLen > 0 {
			if _, err := io.ReadFull(conn, payload); err != nil {
				return nil, err
			}
		}

		out = append(out, backendMessage{msgType: msgType, payload: payload})
		if msgType == MsgReadyForQuery {
			return out, nil
		}
	}
}

func buildParsePayload(stmtName, query string, paramOIDs []int32) []byte {
	buf := NewBuffer()
	buf.WriteString(stmtName)
	buf.WriteString(query)
	buf.WriteInt16(int16(len(paramOIDs)))
	for _, oid := range paramOIDs {
		buf.WriteInt32(oid)
	}
	return buf.Bytes()
}

func buildDescribePayload(descType byte, name string) []byte {
	buf := NewBuffer()
	_ = buf.WriteByte(descType)
	buf.WriteString(name)
	return buf.Bytes()
}

func buildBindPayload(portalName, stmtName string, paramFormats []int16, params [][]byte, resultFormats []int16) []byte {
	buf := NewBuffer()
	buf.WriteString(portalName)
	buf.WriteString(stmtName)

	buf.WriteInt16(int16(len(paramFormats)))
	for _, f := range paramFormats {
		buf.WriteInt16(f)
	}

	buf.WriteInt16(int16(len(params)))
	for _, p := range params {
		if p == nil {
			buf.WriteInt32(-1)
			continue
		}
		buf.WriteInt32(int32(len(p)))
		buf.WriteBytes(p)
	}

	buf.WriteInt16(int16(len(resultFormats)))
	for _, f := range resultFormats {
		buf.WriteInt16(f)
	}

	return buf.Bytes()
}

func buildExecutePayload(portalName string, maxRows int32) []byte {
	buf := NewBuffer()
	buf.WriteString(portalName)
	buf.WriteInt32(maxRows)
	return buf.Bytes()
}

func containsMsgType(msgs []backendMessage, msgType byte) bool {
	for _, m := range msgs {
		if m.msgType == msgType {
			return true
		}
	}
	return false
}

func firstMessage(msgs []backendMessage, msgType byte) (backendMessage, bool) {
	for _, m := range msgs {
		if m.msgType == msgType {
			return m, true
		}
	}
	return backendMessage{}, false
}

func firstColumnName(payload []byte) (string, error) {
	if len(payload) < 2 {
		return "", fmt.Errorf("row description payload too short")
	}
	n := ReadInt16(payload[:2])
	if n < 1 {
		return "", fmt.Errorf("no described columns")
	}
	name, consumed := ReadCString(payload[2:])
	if consumed == 0 {
		return "", fmt.Errorf("invalid described column name")
	}
	return name, nil
}

func readErrorField(payload []byte, field byte) string {
	i := 0
	for i < len(payload) {
		if payload[i] == 0 {
			break
		}
		f := payload[i]
		i++
		start := i
		for i < len(payload) && payload[i] != 0 {
			i++
		}
		if i >= len(payload) {
			break
		}
		if f == field {
			return string(payload[start:i])
		}
		i++ // skip terminator
	}
	return ""
}

// generateTestCerts creates self-signed certificates for testing.
func generateTestCerts(t *testing.T) (certPEM, keyPEM, caPEM []byte) {
	t.Helper()

	// Generate CA private key
	caPrivKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate CA key: %v", err)
	}

	// Create CA certificate template
	caTemplate := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test CA"},
			CommonName:   "Test CA",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}

	// Create CA certificate
	caCertDER, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &caPrivKey.PublicKey, caPrivKey)
	if err != nil {
		t.Fatalf("failed to create CA cert: %v", err)
	}

	caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})

	// Generate server private key
	serverPrivKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate server key: %v", err)
	}

	// Create server certificate template
	serverTemplate := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"Test Server"},
			CommonName:   "localhost",
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:    []string{"localhost"},
	}

	// Create server certificate signed by CA
	serverCertDER, err := x509.CreateCertificate(rand.Reader, &serverTemplate, &caTemplate, &serverPrivKey.PublicKey, caPrivKey)
	if err != nil {
		t.Fatalf("failed to create server cert: %v", err)
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverCertDER})

	// Encode server private key
	serverKeyBytes, err := x509.MarshalECPrivateKey(serverPrivKey)
	if err != nil {
		t.Fatalf("failed to marshal server key: %v", err)
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: serverKeyBytes})

	return certPEM, keyPEM, caPEM
}
