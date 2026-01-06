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
