package replication

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/wal"
)

// startReplica starts the replication client for a replica node.
func (m *Manager) startReplica() error {
	m.wg.Add(1)
	go m.replicaLoop()
	return nil
}

// replicaLoop manages the connection to the primary and handles reconnection.
func (m *Manager) replicaLoop() {
	defer m.wg.Done()

	reconnectDelay := DefaultReconnectDelay

	for {
		select {
		case <-m.ctx.Done():
			return
		default:
		}

		err := m.connectToPrimary()
		if err == nil {
			// Successfully connected and ran, reset reconnect delay
			reconnectDelay = DefaultReconnectDelay
		} else {
			// Connection failed or lost, apply backoff
			reconnectDelay = min(reconnectDelay*2, MaxReconnectDelay)
		}

		// Wait before reconnecting
		select {
		case <-m.ctx.Done():
			return
		case <-time.After(reconnectDelay):
		}
	}
}

// connectToPrimary establishes a connection to the primary and starts streaming.
func (m *Manager) connectToPrimary() error {
	m.mu.Lock()
	if m.primaryConn != nil {
		m.primaryConn.cancel()
		m.primaryConn.conn.Close()
	}
	m.mu.Unlock()

	// Connect to primary
	conn, err := net.DialTimeout("tcp", m.config.PrimaryAddr, m.config.ConnectTimeout)
	if err != nil {
		return fmt.Errorf("connect to primary: %w", err)
	}

	ctx, cancel := m.ctx, m.cancel

	pc := &primaryConn{
		addr:   m.config.PrimaryAddr,
		conn:   conn,
		writer: newBufferWriter(conn),
		reader: newBufferReader(conn),
		state:  StateConnecting,
		ctx:    ctx,
		cancel: cancel,
	}

	m.mu.Lock()
	m.primaryConn = pc
	m.mu.Unlock()

	// Perform handshake
	if err := m.handshakeWithPrimary(pc); err != nil {
		conn.Close()
		return err
	}

	// Start streaming
	return m.streamFromPrimary(pc)
}

// handshakeWithPrimary performs the replication handshake.
func (m *Manager) handshakeWithPrimary(pc *primaryConn) error {
	// Send identify message: [version:2][idLen:4][id:var]
	idBytes := []byte(m.config.NodeID)
	data := make([]byte, 6+len(idBytes))
	binary.BigEndian.PutUint16(data[0:2], ProtocolVersion)
	binary.BigEndian.PutUint32(data[2:6], uint32(len(idBytes)))
	copy(data[6:], idBytes)

	if err := pc.writer.writeMessage(MsgIdentify, data); err != nil {
		return fmt.Errorf("send identify: %w", err)
	}

	// Wait for acknowledgment
	pc.conn.SetReadDeadline(time.Now().Add(m.config.ConnectTimeout))
	msgType, data, err := pc.reader.readMessage()
	if err != nil {
		return fmt.Errorf("read identify ack: %w", err)
	}
	pc.conn.SetReadDeadline(time.Time{}) // Clear deadline

	if msgType == MsgError {
		return fmt.Errorf("primary error: %s", string(data))
	}
	if msgType != MsgIdentifyAck {
		return fmt.Errorf("expected IDENTIFY_ACK, got %d", msgType)
	}

	if len(data) < 10 {
		return fmt.Errorf("invalid IDENTIFY_ACK message")
	}

	version := binary.BigEndian.Uint16(data[0:2])
	if version != ProtocolVersion {
		return ErrProtocolMismatch
	}

	primaryLSN := wal.LSN(binary.BigEndian.Uint64(data[2:10]))
	_ = primaryLSN // Could be used for initial state

	// Request replication from our current position
	m.mu.RLock()
	startLSN := m.appliedLSN
	m.mu.RUnlock()

	// If starting fresh, start from beginning
	if startLSN == 0 {
		startLSN = 8 // Skip magic header
	}

	// Send start replication request: [startLSN:8]
	reqData := make([]byte, 8)
	binary.BigEndian.PutUint64(reqData[0:8], uint64(startLSN))
	if err := pc.writer.writeMessage(MsgStartReplication, reqData); err != nil {
		return fmt.Errorf("send start replication: %w", err)
	}

	pc.state = StateStreaming
	return nil
}

// streamFromPrimary receives WAL stream from the primary.
func (m *Manager) streamFromPrimary(pc *primaryConn) error {
	statusTicker := time.NewTicker(m.config.StatusInterval)
	defer statusTicker.Stop()

	// Start status sender goroutine
	go m.sendStatusLoop(pc, statusTicker)

	// Main receive loop
	for {
		select {
		case <-pc.ctx.Done():
			return pc.ctx.Err()
		default:
		}

		// Set read deadline
		pc.conn.SetReadDeadline(time.Now().Add(m.config.HeartbeatInterval * 3))

		msgType, data, err := pc.reader.readMessage()
		if err != nil {
			if err == io.EOF {
				return fmt.Errorf("primary disconnected")
			}
			return fmt.Errorf("read from primary: %w", err)
		}

		switch msgType {
		case MsgWALData:
			if err := m.handleWALData(data); err != nil {
				return fmt.Errorf("handle WAL data: %w", err)
			}

		case MsgHeartbeat:
			// Respond to heartbeat
			if err := pc.writer.writeMessage(MsgHeartbeatAck, nil); err != nil {
				return err
			}

		case MsgPromote:
			// Primary is telling us to become the new primary
			return m.handlePromote()

		case MsgError:
			return fmt.Errorf("primary error: %s", string(data))

		default:
			// Ignore unknown messages
		}
	}
}

// handleWALData processes a WAL data message from the primary.
func (m *Manager) handleWALData(data []byte) error {
	if len(data) < 8 {
		return fmt.Errorf("invalid WAL data message")
	}

	lsn := wal.LSN(binary.BigEndian.Uint64(data[0:8]))
	recordData := data[8:]

	// Decode the record
	rec, err := wal.DecodeRecord(recordData, lsn)
	if err != nil {
		return fmt.Errorf("decode WAL record: %w", err)
	}

	// Update received LSN
	m.mu.Lock()
	m.receivedLSN = lsn + wal.LSN(len(recordData))
	m.mu.Unlock()

	m.recordsRecv.Add(1)
	m.bytesReceived.Add(uint64(len(recordData)))

	// Apply the record using the configured apply function
	if m.config.ApplyFunc != nil {
		if err := m.config.ApplyFunc(rec); err != nil {
			return fmt.Errorf("apply WAL record: %w", err)
		}
	}

	// Update applied LSN
	m.mu.Lock()
	m.appliedLSN = lsn + wal.LSN(len(recordData))
	m.mu.Unlock()

	return nil
}

// sendStatusLoop periodically sends status updates to the primary.
func (m *Manager) sendStatusLoop(pc *primaryConn, ticker *time.Ticker) {
	for {
		select {
		case <-pc.ctx.Done():
			return
		case <-ticker.C:
			m.mu.RLock()
			received := m.receivedLSN
			applied := m.appliedLSN
			flushed := applied // For now, assume applied = flushed
			m.mu.RUnlock()

			// Status message: [received:8][applied:8][flushed:8]
			data := make([]byte, 24)
			binary.BigEndian.PutUint64(data[0:8], uint64(received))
			binary.BigEndian.PutUint64(data[8:16], uint64(applied))
			binary.BigEndian.PutUint64(data[16:24], uint64(flushed))

			if err := pc.writer.writeMessage(MsgStandbyStatus, data); err != nil {
				return
			}
		}
	}
}

// handlePromote handles a promotion request from the primary.
func (m *Manager) handlePromote() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Close connection to old primary
	if m.primaryConn != nil {
		m.primaryConn.conn.Close()
		m.primaryConn = nil
	}

	// Change role to primary
	m.role = RolePrimary

	// Start listening for replicas
	if m.config.ListenAddr != "" {
		return m.startPrimary()
	}

	return nil
}

// ReplicationState returns the current replication state (for replicas).
func (m *Manager) ReplicationState() ReplicationState {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.primaryConn == nil {
		return StateDisconnected
	}
	return m.primaryConn.state
}

// PrimaryAddress returns the primary's address (for replicas).
func (m *Manager) PrimaryAddress() string {
	return m.config.PrimaryAddr
}
