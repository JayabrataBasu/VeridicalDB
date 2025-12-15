package replication

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/wal"
)

// startPrimary starts the replication listener for the primary node.
func (m *Manager) startPrimary() error {
	if m.config.ListenAddr == "" {
		return nil // No replication listener configured
	}

	listener, err := net.Listen("tcp", m.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen for replication: %w", err)
	}

	m.mu.Lock()
	m.listener = listener
	m.mu.Unlock()

	m.wg.Add(1)
	go m.acceptLoop()

	return nil
}

// acceptLoop accepts incoming replica connections.
func (m *Manager) acceptLoop() {
	defer m.wg.Done()

	for {
		m.mu.RLock()
		listener := m.listener
		m.mu.RUnlock()

		if listener == nil {
			return
		}

		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-m.ctx.Done():
				return
			default:
				// Listener was closed, check context
				m.mu.RLock()
				closed := m.listener == nil
				m.mu.RUnlock()
				if closed {
					return
				}
				continue
			}
		}

		m.wg.Add(1)
		go m.handleReplicaConnection(conn)
	}
}

// handleReplicaConnection handles a connection from a replica.
func (m *Manager) handleReplicaConnection(conn net.Conn) {
	defer m.wg.Done()
	defer conn.Close()

	ctx, cancel := context.WithCancel(m.ctx)
	defer cancel()

	writer := newBufferWriter(conn)
	reader := newBufferReader(conn)

	// Step 1: Receive identification
	msgType, data, err := reader.readMessage()
	if err != nil {
		return
	}
	if msgType != MsgIdentify {
		writer.writeMessage(MsgError, []byte("expected IDENTIFY message"))
		return
	}

	if len(data) < 10 {
		writer.writeMessage(MsgError, []byte("invalid IDENTIFY message"))
		return
	}

	// Parse identify message: [version:2][idLen:4][id:var]
	version := binary.BigEndian.Uint16(data[0:2])
	if version != ProtocolVersion {
		writer.writeMessage(MsgError, []byte("protocol version mismatch"))
		return
	}

	idLen := binary.BigEndian.Uint32(data[2:6])
	if int(idLen) > len(data)-6 {
		writer.writeMessage(MsgError, []byte("invalid replica ID"))
		return
	}
	replicaID := string(data[6 : 6+idLen])

	// Send acknowledgment
	ackData := make([]byte, 10)
	binary.BigEndian.PutUint16(ackData[0:2], ProtocolVersion)
	binary.BigEndian.PutUint64(ackData[2:10], uint64(m.wal.CurrentLSN()))
	if err := writer.writeMessage(MsgIdentifyAck, ackData); err != nil {
		return
	}

	// Step 2: Wait for start replication request
	msgType, data, err = reader.readMessage()
	if err != nil {
		return
	}
	if msgType != MsgStartReplication {
		writer.writeMessage(MsgError, []byte("expected START_REPLICATION message"))
		return
	}

	if len(data) < 8 {
		writer.writeMessage(MsgError, []byte("invalid START_REPLICATION message"))
		return
	}

	startLSN := wal.LSN(binary.BigEndian.Uint64(data[0:8]))

	// Create replica connection record
	rc := &replicaConn{
		info: ReplicaInfo{
			ID:          replicaID,
			Address:     conn.RemoteAddr().String(),
			ReceivedLSN: startLSN,
			State:       StateCatchingUp,
			ConnectedAt: time.Now(),
			LastContact: time.Now(),
		},
		conn:    conn,
		writer:  writer,
		reader:  reader,
		sendLSN: startLSN,
		ctx:     ctx,
		cancel:  cancel,
	}

	// Register replica
	m.replicasMu.Lock()
	m.replicas[replicaID] = rc
	m.replicasMu.Unlock()

	defer func() {
		m.replicasMu.Lock()
		delete(m.replicas, replicaID)
		m.replicasMu.Unlock()
	}()

	// Start streaming and status handling
	errCh := make(chan error, 2)

	go func() {
		errCh <- m.streamWALToReplica(rc)
	}()

	go func() {
		errCh <- m.handleReplicaStatus(rc)
	}()

	// Wait for either goroutine to fail or context to be canceled
	select {
	case <-ctx.Done():
	case <-errCh:
	}
}

// streamWALToReplica streams WAL records to a replica.
func (m *Manager) streamWALToReplica(rc *replicaConn) error {
	heartbeatTicker := time.NewTicker(m.config.HeartbeatInterval)
	defer heartbeatTicker.Stop()

	// First, catch up from the requested LSN
	if err := m.catchUpReplica(rc); err != nil {
		return err
	}

	// Update state to streaming
	m.replicasMu.Lock()
	rc.info.State = StateStreaming
	m.replicasMu.Unlock()

	// Stream new WAL records as they come
	for {
		select {
		case <-rc.ctx.Done():
			return rc.ctx.Err()

		case <-heartbeatTicker.C:
			// Send heartbeat
			data := make([]byte, 8)
			binary.BigEndian.PutUint64(data[0:8], uint64(m.wal.CurrentLSN()))
			if err := rc.writer.writeMessage(MsgHeartbeat, data); err != nil {
				return err
			}

		default:
			// Check for new WAL data
			currentLSN := m.wal.CurrentLSN()
			if rc.sendLSN < currentLSN {
				if err := m.sendWALRecords(rc, rc.sendLSN, currentLSN); err != nil {
					return err
				}
			} else {
				// No new data, sleep briefly
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
}

// catchUpReplica sends historical WAL records to catch up a replica.
func (m *Manager) catchUpReplica(rc *replicaConn) error {
	currentLSN := m.wal.CurrentLSN()
	if rc.sendLSN >= currentLSN {
		return nil
	}

	return m.sendWALRecords(rc, rc.sendLSN, currentLSN)
}

// sendWALRecords sends WAL records in a range to a replica.
func (m *Manager) sendWALRecords(rc *replicaConn, startLSN, endLSN wal.LSN) error {
	iter, err := m.wal.NewIteratorFrom(startLSN)
	if err != nil {
		return fmt.Errorf("create WAL iterator: %w", err)
	}
	defer iter.Close()

	for {
		rec, err := iter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read WAL record: %w", err)
		}

		// Check if we've passed the end
		if rec.LSN >= endLSN {
			break
		}

		// Encode and send the record
		data, err := rec.Encode()
		if err != nil {
			return fmt.Errorf("encode WAL record: %w", err)
		}

		// WAL data message: [lsn:8][data:var]
		msg := make([]byte, 8+len(data))
		binary.BigEndian.PutUint64(msg[0:8], uint64(rec.LSN))
		copy(msg[8:], data)

		if err := rc.writer.writeMessage(MsgWALData, msg); err != nil {
			return err
		}

		rc.sendLSN = rec.LSN + wal.LSN(len(data))
		m.recordsSent.Add(1)
		m.bytesSent.Add(uint64(len(data)))
	}

	return nil
}

// handleReplicaStatus handles status messages from a replica.
func (m *Manager) handleReplicaStatus(rc *replicaConn) error {
	for {
		select {
		case <-rc.ctx.Done():
			return rc.ctx.Err()
		default:
		}

		// Set read deadline for status messages
		rc.conn.SetReadDeadline(time.Now().Add(m.config.HeartbeatInterval * 3))

		msgType, data, err := rc.reader.readMessage()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// Timeout waiting for status, replica may be slow
				continue
			}
			return err
		}

		switch msgType {
		case MsgStandbyStatus:
			if len(data) < 24 {
				continue
			}

			// Parse status: [received:8][applied:8][flushed:8]
			receivedLSN := wal.LSN(binary.BigEndian.Uint64(data[0:8]))
			appliedLSN := wal.LSN(binary.BigEndian.Uint64(data[8:16]))
			flushedLSN := wal.LSN(binary.BigEndian.Uint64(data[16:24]))

			m.replicasMu.Lock()
			rc.info.ReceivedLSN = receivedLSN
			rc.info.AppliedLSN = appliedLSN
			rc.info.FlushLSN = flushedLSN
			rc.info.LastContact = time.Now()
			rc.info.LagBytes = int64(m.wal.CurrentLSN()) - int64(appliedLSN)

			// Update state based on lag
			if rc.info.LagBytes == 0 {
				rc.info.State = StateSynced
			} else if rc.info.LagBytes < 1024*1024 { // < 1MB
				rc.info.State = StateStreaming
			} else {
				rc.info.State = StateCatchingUp
			}
			m.replicasMu.Unlock()

			// Notify sync waiters
			m.notifySyncWaiters(flushedLSN)

		case MsgHeartbeatAck:
			m.replicasMu.Lock()
			rc.info.LastContact = time.Now()
			m.replicasMu.Unlock()

		default:
			// Ignore unknown messages
		}
	}
}

// BroadcastWAL sends a WAL record to all connected replicas.
// This is called by the transaction manager after writing to WAL.
func (m *Manager) BroadcastWAL(rec *wal.Record) error {
	if !m.IsPrimary() {
		return ErrNotPrimary
	}

	data, err := rec.Encode()
	if err != nil {
		return err
	}

	// WAL data message: [lsn:8][data:var]
	msg := make([]byte, 8+len(data))
	binary.BigEndian.PutUint64(msg[0:8], uint64(rec.LSN))
	copy(msg[8:], data)

	m.replicasMu.RLock()
	defer m.replicasMu.RUnlock()

	for _, rc := range m.replicas {
		// Best-effort broadcast, don't fail if one replica is slow
		rc.writer.writeMessage(MsgWALData, msg)
		rc.sendLSN = rec.LSN + wal.LSN(len(data))
	}

	m.recordsSent.Add(uint64(len(m.replicas)))
	m.bytesSent.Add(uint64(len(data) * len(m.replicas)))

	return nil
}

// ReplicaCount returns the number of connected replicas.
func (m *Manager) ReplicaCount() int {
	m.replicasMu.RLock()
	defer m.replicasMu.RUnlock()
	return len(m.replicas)
}

// HealthyReplicaCount returns the number of healthy (synced or streaming) replicas.
func (m *Manager) HealthyReplicaCount() int {
	m.replicasMu.RLock()
	defer m.replicasMu.RUnlock()

	count := 0
	for _, rc := range m.replicas {
		if rc.info.State == StateSynced || rc.info.State == StateStreaming {
			count++
		}
	}
	return count
}
