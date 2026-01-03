// Package replication provides primary-replica replication for VeridicalDB.
// It implements streaming replication using the Write-Ahead Log (WAL) to
// keep replicas in sync with the primary server.
//
// Architecture:
// - Primary: Accepts writes, streams WAL to replicas
// - Replica: Receives WAL stream, applies changes, serves read queries
//
// Features:
// - Asynchronous streaming replication
// - Automatic failover support
// - Configurable replication lag monitoring
// - Replica promotion to primary
package replication

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/wal"
)

// Role represents the replication role of a node.
type Role int

const (
	// RolePrimary indicates the node accepts writes and streams to replicas.
	RolePrimary Role = iota
	// RoleReplica indicates the node receives WAL stream and serves reads.
	RoleReplica
	// RoleCandidate indicates the node is participating in failover election.
	RoleCandidate
)

// String returns the string representation of a Role.
func (r Role) String() string {
	switch r {
	case RolePrimary:
		return "PRIMARY"
	case RoleReplica:
		return "REPLICA"
	case RoleCandidate:
		return "CANDIDATE"
	default:
		return "UNKNOWN"
	}
}

// ReplicationState represents the current state of replication.
type ReplicationState int

const (
	// StateDisconnected means no replication connection.
	StateDisconnected ReplicationState = iota
	// StateConnecting means attempting to connect.
	StateConnecting
	// StateStreaming means actively receiving WAL stream.
	StateStreaming
	// StateCatchingUp means replica is catching up with primary.
	StateCatchingUp
	// StateSynced means replica is fully synced.
	StateSynced
)

// String returns the string representation of ReplicationState.
func (s ReplicationState) String() string {
	switch s {
	case StateDisconnected:
		return "DISCONNECTED"
	case StateConnecting:
		return "CONNECTING"
	case StateStreaming:
		return "STREAMING"
	case StateCatchingUp:
		return "CATCHING_UP"
	case StateSynced:
		return "SYNCED"
	default:
		return "UNKNOWN"
	}
}

// Message types for replication protocol.
const (
	// MsgIdentify is sent by replica to identify itself.
	MsgIdentify byte = 0x01
	// MsgIdentifyAck acknowledges identification.
	MsgIdentifyAck byte = 0x02
	// MsgStartReplication requests WAL streaming from a specific LSN.
	MsgStartReplication byte = 0x03
	// MsgWALData contains WAL record data.
	MsgWALData byte = 0x04
	// MsgStandbyStatus is sent by replica to report its position.
	MsgStandbyStatus byte = 0x05
	// MsgHeartbeat is a keepalive message.
	MsgHeartbeat byte = 0x06
	// MsgHeartbeatAck acknowledges a heartbeat.
	MsgHeartbeatAck byte = 0x07
	// MsgSwitchover requests a controlled failover.
	MsgSwitchover byte = 0x08
	// MsgPromote tells a replica to become primary.
	MsgPromote byte = 0x09
	// MsgDemote tells a primary to become replica.
	MsgDemote byte = 0x0A
	// MsgError indicates an error occurred.
	MsgError byte = 0xFF
)

// Protocol constants.
const (
	// ProtocolVersion is the current replication protocol version.
	ProtocolVersion uint16 = 1

	// DefaultHeartbeatInterval is the default time between heartbeats.
	DefaultHeartbeatInterval = 10 * time.Second

	// DefaultStatusInterval is the default time between status reports.
	DefaultStatusInterval = 5 * time.Second

	// DefaultConnectTimeout is the default connection timeout.
	DefaultConnectTimeout = 10 * time.Second

	// DefaultReconnectDelay is the delay before reconnecting after failure.
	DefaultReconnectDelay = 5 * time.Second

	// MaxReconnectDelay is the maximum reconnect delay with backoff.
	MaxReconnectDelay = 60 * time.Second
)

// Common errors.
var (
	ErrNotPrimary         = errors.New("node is not primary")
	ErrNotReplica         = errors.New("node is not replica")
	ErrAlreadyConnected   = errors.New("already connected")
	ErrNotConnected       = errors.New("not connected")
	ErrProtocolMismatch   = errors.New("protocol version mismatch")
	ErrInvalidMessage     = errors.New("invalid message")
	ErrReplicationLag     = errors.New("replication lag too high")
	ErrFailoverInProgress = errors.New("failover in progress")
	ErrNoHealthyReplica   = errors.New("no healthy replica available")
)

// ReplicaInfo contains information about a connected replica.
type ReplicaInfo struct {
	// ID is the unique identifier for the replica.
	ID string

	// Address is the network address of the replica.
	Address string

	// ReceivedLSN is the last LSN acknowledged by the replica.
	ReceivedLSN wal.LSN

	// AppliedLSN is the last LSN applied by the replica.
	AppliedLSN wal.LSN

	// FlushLSN is the last LSN flushed to disk by the replica.
	FlushLSN wal.LSN

	// State is the current replication state.
	State ReplicationState

	// LagBytes is the replication lag in bytes.
	LagBytes int64

	// LagTime is the estimated replication lag time.
	LagTime time.Duration

	// LastContact is the time of last communication.
	LastContact time.Time

	// ConnectedAt is when the replica connected.
	ConnectedAt time.Time
}

// Config holds replication configuration.
type Config struct {
	// NodeID is the unique identifier for this node.
	NodeID string

	// Role is the initial role (PRIMARY or REPLICA).
	Role Role

	// ListenAddr is the address to listen for replication connections.
	ListenAddr string

	// PrimaryAddr is the address of the primary (for replicas).
	PrimaryAddr string

	// WAL is the Write-Ahead Log instance.
	WAL *wal.WAL

	// ApplyFunc is called to apply WAL records on replicas.
	ApplyFunc func(rec *wal.Record) error

	// HeartbeatInterval is the time between heartbeats.
	HeartbeatInterval time.Duration

	// StatusInterval is the time between status reports.
	StatusInterval time.Duration

	// ConnectTimeout is the connection timeout.
	ConnectTimeout time.Duration

	// SyncMode determines if replication is synchronous.
	// If true, commits wait for at least one replica acknowledgment.
	SyncMode bool

	// MinReplicas is the minimum number of replicas required for sync mode.
	MinReplicas int
}

// DefaultConfig returns a Config with default values.
func DefaultConfig() Config {
	return Config{
		Role:              RolePrimary,
		HeartbeatInterval: DefaultHeartbeatInterval,
		StatusInterval:    DefaultStatusInterval,
		ConnectTimeout:    DefaultConnectTimeout,
		SyncMode:          false,
		MinReplicas:       1,
	}
}

// Manager manages replication for a database node.
type Manager struct {
	mu sync.RWMutex

	config Config
	role   Role
	wal    *wal.WAL

	// Primary-side state
	listener   net.Listener
	replicas   map[string]*replicaConn
	replicasMu sync.RWMutex

	// Replica-side state
	primaryConn *primaryConn
	appliedLSN  wal.LSN
	receivedLSN wal.LSN

	// Sync replication channels
	syncWaiters map[wal.LSN]chan struct{}
	syncMu      sync.Mutex

	// Lifecycle
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	running atomic.Bool

	// Metrics
	bytesSent     atomic.Uint64
	bytesReceived atomic.Uint64
	recordsSent   atomic.Uint64
	recordsRecv   atomic.Uint64
}

// replicaConn represents a connection to a replica (on primary).
type replicaConn struct {
	info       ReplicaInfo
	conn       net.Conn
	writer     *bufferWriter
	reader     *bufferReader
	mu         sync.Mutex
	sendLSN    wal.LSN
	lastStatus time.Time
	ctx        context.Context
	cancel     context.CancelFunc
}

// primaryConn represents a connection to the primary (on replica).
type primaryConn struct {
	addr   string
	conn   net.Conn
	writer *bufferWriter
	reader *bufferReader
	state  ReplicationState
	ctx    context.Context
	cancel context.CancelFunc
}

// bufferWriter is a buffered writer with length-prefixed messages.
type bufferWriter struct {
	conn net.Conn
	buf  []byte
}

func newBufferWriter(conn net.Conn) *bufferWriter {
	return &bufferWriter{
		conn: conn,
		buf:  make([]byte, 0, 64*1024),
	}
}

func (w *bufferWriter) writeMessage(msgType byte, data []byte) error {
	// Message format: [length:4][type:1][data:var]
	length := uint32(1 + len(data))
	header := make([]byte, 5)
	binary.BigEndian.PutUint32(header[0:4], length)
	header[4] = msgType

	if _, err := w.conn.Write(header); err != nil {
		return err
	}
	if len(data) > 0 {
		if _, err := w.conn.Write(data); err != nil {
			return err
		}
	}
	return nil
}

// bufferReader is a buffered reader for length-prefixed messages.
type bufferReader struct {
	conn   net.Conn
	header [5]byte
}

func newBufferReader(conn net.Conn) *bufferReader {
	return &bufferReader{conn: conn}
}

func (r *bufferReader) readMessage() (byte, []byte, error) {
	// Read header: [length:4][type:1]
	if _, err := io.ReadFull(r.conn, r.header[:]); err != nil {
		return 0, nil, err
	}

	length := binary.BigEndian.Uint32(r.header[0:4])
	msgType := r.header[4]

	if length < 1 {
		return 0, nil, ErrInvalidMessage
	}

	// Read data
	dataLen := length - 1
	if dataLen == 0 {
		return msgType, nil, nil
	}

	data := make([]byte, dataLen)
	if _, err := io.ReadFull(r.conn, data); err != nil {
		return 0, nil, err
	}

	return msgType, data, nil
}

// NewManager creates a new replication manager.
func NewManager(config Config) (*Manager, error) {
	if config.NodeID == "" {
		return nil, fmt.Errorf("NodeID is required")
	}
	if config.WAL == nil {
		return nil, fmt.Errorf("WAL is required")
	}
	if config.Role == RoleReplica && config.PrimaryAddr == "" {
		return nil, fmt.Errorf("PrimaryAddr is required for replica")
	}
	if config.Role == RoleReplica && config.ApplyFunc == nil {
		return nil, fmt.Errorf("ApplyFunc is required for replica")
	}

	// Apply defaults
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if config.StatusInterval == 0 {
		config.StatusInterval = DefaultStatusInterval
	}
	if config.ConnectTimeout == 0 {
		config.ConnectTimeout = DefaultConnectTimeout
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Manager{
		config:      config,
		role:        config.Role,
		wal:         config.WAL,
		replicas:    make(map[string]*replicaConn),
		syncWaiters: make(map[wal.LSN]chan struct{}),
		ctx:         ctx,
		cancel:      cancel,
	}, nil
}

// Start starts the replication manager.
func (m *Manager) Start() error {
	if m.running.Load() {
		return errors.New("already running")
	}
	m.running.Store(true)

	if m.role == RolePrimary {
		return m.startPrimary()
	}
	return m.startReplica()
}

// Stop stops the replication manager.
func (m *Manager) Stop() error {
	if !m.running.Load() {
		return nil
	}
	m.running.Store(false)
	m.cancel()

	// Close listener
	m.mu.Lock()
	if m.listener != nil {
		_ = m.listener.Close()
	}
	m.mu.Unlock()

	// Close all replica connections
	m.replicasMu.Lock()
	for _, rc := range m.replicas {
		rc.cancel()
		_ = rc.conn.Close()
	}
	m.replicasMu.Unlock()

	// Close primary connection
	m.mu.Lock()
	if m.primaryConn != nil {
		m.primaryConn.cancel()
		_ = m.primaryConn.conn.Close()
	}
	m.mu.Unlock()

	// Wait for goroutines
	m.wg.Wait()

	return nil
}

// Role returns the current role of this node.
func (m *Manager) Role() Role {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.role
}

// IsPrimary returns true if this node is the primary.
func (m *Manager) IsPrimary() bool {
	return m.Role() == RolePrimary
}

// IsReplica returns true if this node is a replica.
func (m *Manager) IsReplica() bool {
	return m.Role() == RoleReplica
}

// Replicas returns information about connected replicas.
func (m *Manager) Replicas() []ReplicaInfo {
	m.replicasMu.RLock()
	defer m.replicasMu.RUnlock()

	result := make([]ReplicaInfo, 0, len(m.replicas))
	for _, rc := range m.replicas {
		result = append(result, rc.info)
	}
	return result
}

// AppliedLSN returns the last applied LSN (for replicas).
func (m *Manager) AppliedLSN() wal.LSN {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.appliedLSN
}

// ReceivedLSN returns the last received LSN (for replicas).
func (m *Manager) ReceivedLSN() wal.LSN {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.receivedLSN
}

// ReplicationLag returns the replication lag in bytes.
func (m *Manager) ReplicationLag() int64 {
	if m.IsPrimary() {
		// For primary, return max lag across replicas
		var maxLag int64
		currentLSN := m.wal.CurrentLSN()
		m.replicasMu.RLock()
		for _, rc := range m.replicas {
			lag := int64(currentLSN) - int64(rc.info.AppliedLSN)
			if lag > maxLag {
				maxLag = lag
			}
		}
		m.replicasMu.RUnlock()
		return maxLag
	}

	// For replica, return lag from primary
	m.mu.RLock()
	lag := int64(m.receivedLSN) - int64(m.appliedLSN)
	m.mu.RUnlock()
	return lag
}

// WaitForSync waits until a specific LSN is replicated to at least n replicas.
// This is used for synchronous replication.
func (m *Manager) WaitForSync(lsn wal.LSN, timeout time.Duration) error {
	if !m.config.SyncMode {
		return nil
	}

	m.syncMu.Lock()
	ch := make(chan struct{})
	m.syncWaiters[lsn] = ch
	m.syncMu.Unlock()

	defer func() {
		m.syncMu.Lock()
		delete(m.syncWaiters, lsn)
		m.syncMu.Unlock()
	}()

	select {
	case <-ch:
		return nil
	case <-time.After(timeout):
		return ErrReplicationLag
	case <-m.ctx.Done():
		return m.ctx.Err()
	}
}

// notifySyncWaiters notifies waiters when an LSN is acknowledged.
func (m *Manager) notifySyncWaiters(ackLSN wal.LSN) {
	m.syncMu.Lock()
	defer m.syncMu.Unlock()

	for lsn, ch := range m.syncWaiters {
		if ackLSN >= lsn {
			close(ch)
			delete(m.syncWaiters, lsn)
		}
	}
}
