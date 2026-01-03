package replication

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"time"
)

// FailoverManager handles automatic and manual failover between primary and replicas.
type FailoverManager struct {
	mu sync.RWMutex

	// replicationMgr is the replication manager
	replicationMgr *Manager

	// healthCheckInterval is how often to check replica health
	healthCheckInterval time.Duration

	// healthCheckTimeout is the timeout for health checks
	healthCheckTimeout time.Duration

	// maxLagBytes is the maximum allowed lag for a replica to be considered healthy
	maxLagBytes int64

	// maxLagTime is the maximum allowed lag time
	maxLagTime time.Duration

	// minHealthyReplicas is the minimum number of healthy replicas required
	minHealthyReplicas int

	// autoFailover enables automatic failover when primary fails
	autoFailover bool

	// failoverInProgress indicates a failover is currently happening
	failoverInProgress bool

	// callbacks for failover events
	onFailoverStart func()
	onFailoverEnd   func(newPrimary string, err error)
	onRoleChange    func(newRole Role)
}

// FailoverConfig holds configuration for the failover manager.
type FailoverConfig struct {
	// HealthCheckInterval is how often to check replica health.
	HealthCheckInterval time.Duration

	// HealthCheckTimeout is the timeout for health checks.
	HealthCheckTimeout time.Duration

	// MaxLagBytes is the maximum allowed replication lag in bytes.
	MaxLagBytes int64

	// MaxLagTime is the maximum allowed replication lag time.
	MaxLagTime time.Duration

	// MinHealthyReplicas is the minimum number of healthy replicas.
	MinHealthyReplicas int

	// AutoFailover enables automatic failover.
	AutoFailover bool
}

// DefaultFailoverConfig returns a FailoverConfig with default values.
func DefaultFailoverConfig() FailoverConfig {
	return FailoverConfig{
		HealthCheckInterval: 5 * time.Second,
		HealthCheckTimeout:  3 * time.Second,
		MaxLagBytes:         10 * 1024 * 1024, // 10MB
		MaxLagTime:          30 * time.Second,
		MinHealthyReplicas:  1,
		AutoFailover:        false,
	}
}

// NewFailoverManager creates a new failover manager.
func NewFailoverManager(replicationMgr *Manager, config FailoverConfig) *FailoverManager {
	if config.HealthCheckInterval == 0 {
		config.HealthCheckInterval = 5 * time.Second
	}
	if config.HealthCheckTimeout == 0 {
		config.HealthCheckTimeout = 3 * time.Second
	}
	if config.MaxLagBytes == 0 {
		config.MaxLagBytes = 10 * 1024 * 1024
	}
	if config.MaxLagTime == 0 {
		config.MaxLagTime = 30 * time.Second
	}
	if config.MinHealthyReplicas == 0 {
		config.MinHealthyReplicas = 1
	}

	return &FailoverManager{
		replicationMgr:      replicationMgr,
		healthCheckInterval: config.HealthCheckInterval,
		healthCheckTimeout:  config.HealthCheckTimeout,
		maxLagBytes:         config.MaxLagBytes,
		maxLagTime:          config.MaxLagTime,
		minHealthyReplicas:  config.MinHealthyReplicas,
		autoFailover:        config.AutoFailover,
	}
}

// OnFailoverStart sets a callback for when failover starts.
func (fm *FailoverManager) OnFailoverStart(cb func()) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	fm.onFailoverStart = cb
}

// OnFailoverEnd sets a callback for when failover ends.
func (fm *FailoverManager) OnFailoverEnd(cb func(newPrimary string, err error)) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	fm.onFailoverEnd = cb
}

// OnRoleChange sets a callback for when the node's role changes.
func (fm *FailoverManager) OnRoleChange(cb func(newRole Role)) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	fm.onRoleChange = cb
}

// IsHealthy checks if a replica is healthy based on lag and last contact.
func (fm *FailoverManager) IsHealthy(info ReplicaInfo) bool {
	// Check lag
	if info.LagBytes > fm.maxLagBytes {
		return false
	}

	// Check last contact time
	if time.Since(info.LastContact) > fm.maxLagTime {
		return false
	}

	// Check state
	return info.State == StateSynced || info.State == StateStreaming
}

// HealthyReplicas returns a list of healthy replicas sorted by lag.
func (fm *FailoverManager) HealthyReplicas() []ReplicaInfo {
	replicas := fm.replicationMgr.Replicas()
	healthy := make([]ReplicaInfo, 0, len(replicas))

	for _, r := range replicas {
		if fm.IsHealthy(r) {
			healthy = append(healthy, r)
		}
	}

	// Sort by lag (lowest first)
	sort.Slice(healthy, func(i, j int) bool {
		return healthy[i].LagBytes < healthy[j].LagBytes
	})

	return healthy
}

// BestFailoverCandidate returns the best replica for failover.
func (fm *FailoverManager) BestFailoverCandidate() (ReplicaInfo, error) {
	healthy := fm.HealthyReplicas()
	if len(healthy) == 0 {
		return ReplicaInfo{}, ErrNoHealthyReplica
	}

	// Return the one with lowest lag
	return healthy[0], nil
}

// Failover performs a controlled failover to a specified replica.
// If targetID is empty, the best candidate is chosen automatically.
func (fm *FailoverManager) Failover(targetID string) error {
	fm.mu.Lock()
	if fm.failoverInProgress {
		fm.mu.Unlock()
		return ErrFailoverInProgress
	}
	fm.failoverInProgress = true
	fm.mu.Unlock()

	defer func() {
		fm.mu.Lock()
		fm.failoverInProgress = false
		fm.mu.Unlock()
	}()

	// Notify failover start
	fm.mu.RLock()
	if fm.onFailoverStart != nil {
		fm.onFailoverStart()
	}
	fm.mu.RUnlock()

	var target ReplicaInfo
	var err error

	if targetID == "" {
		target, err = fm.BestFailoverCandidate()
		if err != nil {
			fm.notifyFailoverEnd("", err)
			return err
		}
	} else {
		// Find the specified replica
		replicas := fm.replicationMgr.Replicas()
		found := false
		for _, r := range replicas {
			if r.ID == targetID {
				target = r
				found = true
				break
			}
		}
		if !found {
			err = fmt.Errorf("replica %s not found", targetID)
			fm.notifyFailoverEnd("", err)
			return err
		}
		if !fm.IsHealthy(target) {
			err = fmt.Errorf("replica %s is not healthy", targetID)
			fm.notifyFailoverEnd("", err)
			return err
		}
	}

	// Perform the failover
	if err := fm.performFailover(target); err != nil {
		fm.notifyFailoverEnd("", err)
		return err
	}

	fm.notifyFailoverEnd(target.ID, nil)
	return nil
}

// performFailover executes the actual failover to a target replica.
func (fm *FailoverManager) performFailover(target ReplicaInfo) error {
	// Step 1: Send promote message to target replica
	fm.replicationMgr.replicasMu.RLock()
	rc, ok := fm.replicationMgr.replicas[target.ID]
	fm.replicationMgr.replicasMu.RUnlock()

	if !ok {
		return fmt.Errorf("replica connection not found")
	}

	// Send promote message
	if err := rc.writer.writeMessage(MsgPromote, nil); err != nil {
		return fmt.Errorf("send promote message: %w", err)
	}

	// Step 2: Demote self (if we're the primary)
	if fm.replicationMgr.IsPrimary() {
		fm.replicationMgr.mu.Lock()
		fm.replicationMgr.role = RoleReplica
		fm.replicationMgr.config.PrimaryAddr = target.Address
		fm.replicationMgr.mu.Unlock()

		// Close listener to stop accepting replicas
		fm.replicationMgr.mu.Lock()
		if fm.replicationMgr.listener != nil {
			_ = fm.replicationMgr.listener.Close()
			fm.replicationMgr.listener = nil
		}
		fm.replicationMgr.mu.Unlock()

		// Notify role change
		fm.mu.RLock()
		if fm.onRoleChange != nil {
			fm.onRoleChange(RoleReplica)
		}
		fm.mu.RUnlock()

		// Start replicating from new primary
		_ = fm.replicationMgr.startReplica()
	}

	return nil
}

// Switchover performs a controlled switchover where the current primary
// gracefully hands over to a replica.
func (fm *FailoverManager) Switchover(targetID string) error {
	if !fm.replicationMgr.IsPrimary() {
		return ErrNotPrimary
	}

	// Ensure all replicas are caught up before switching
	healthy := fm.HealthyReplicas()
	if len(healthy) == 0 {
		return ErrNoHealthyReplica
	}

	// Find target or choose best
	var target ReplicaInfo
	if targetID == "" {
		target = healthy[0]
	} else {
		found := false
		for _, r := range healthy {
			if r.ID == targetID {
				target = r
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("target replica %s not found or not healthy", targetID)
		}
	}

	// Wait for target to be fully synced
	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

WaitLoop:
	for {
		select {
		case <-timeout:
			return fmt.Errorf("timeout waiting for replica to sync")
		case <-ticker.C:
			replicas := fm.replicationMgr.Replicas()
			for _, r := range replicas {
				if r.ID == target.ID && r.State == StateSynced {
					break WaitLoop
				}
			}
		}
	}

	// Perform failover
	return fm.Failover(target.ID)
}

// notifyFailoverEnd notifies the failover end callback.
func (fm *FailoverManager) notifyFailoverEnd(newPrimary string, err error) {
	fm.mu.RLock()
	if fm.onFailoverEnd != nil {
		fm.onFailoverEnd(newPrimary, err)
	}
	fm.mu.RUnlock()
}

// Promote promotes this node from replica to primary.
// This is typically called after receiving a promote message or manually.
func (fm *FailoverManager) Promote() error {
	if fm.replicationMgr.IsPrimary() {
		return nil // Already primary
	}

	fm.replicationMgr.mu.Lock()

	// Close connection to old primary
	if fm.replicationMgr.primaryConn != nil {
		_ = fm.replicationMgr.primaryConn.conn.Close()
		fm.replicationMgr.primaryConn = nil
	}

	// Change role
	fm.replicationMgr.role = RolePrimary
	fm.replicationMgr.mu.Unlock()

	// Notify role change
	fm.mu.RLock()
	if fm.onRoleChange != nil {
		fm.onRoleChange(RolePrimary)
	}
	fm.mu.RUnlock()

	// Start primary listener if configured
	if fm.replicationMgr.config.ListenAddr != "" {
		return fm.replicationMgr.startPrimary()
	}

	return nil
}

// Demote demotes this node from primary to replica.
func (fm *FailoverManager) Demote(newPrimaryAddr string) error {
	if !fm.replicationMgr.IsPrimary() {
		return nil // Already replica
	}

	// Cancel the context first
	fm.replicationMgr.cancel()

	// Close listener to unblock acceptLoop - this must happen before Wait()
	fm.replicationMgr.mu.Lock()
	if fm.replicationMgr.listener != nil {
		_ = fm.replicationMgr.listener.Close()
		fm.replicationMgr.listener = nil
	}
	fm.replicationMgr.mu.Unlock()

	// Close all replica connections
	fm.replicationMgr.replicasMu.Lock()
	for _, rc := range fm.replicationMgr.replicas {
		// Send demote message
		if err := rc.writer.writeMessage(MsgDemote, []byte(newPrimaryAddr)); err != nil {
			fmt.Printf("replication: failed to send demote to %s: %v\n", rc.info.ID, err)
		}
		rc.cancel()
		_ = rc.conn.Close()
	}
	fm.replicationMgr.replicas = make(map[string]*replicaConn)
	fm.replicationMgr.replicasMu.Unlock()

	// Wait for goroutines to finish
	fm.replicationMgr.wg.Wait()

	// Recreate context for replica mode
	fm.replicationMgr.ctx, fm.replicationMgr.cancel = context.WithCancel(context.Background())

	// Change role
	fm.replicationMgr.mu.Lock()
	fm.replicationMgr.role = RoleReplica
	fm.replicationMgr.config.PrimaryAddr = newPrimaryAddr
	fm.replicationMgr.mu.Unlock()

	// Notify role change
	fm.mu.RLock()
	if fm.onRoleChange != nil {
		fm.onRoleChange(RoleReplica)
	}
	fm.mu.RUnlock()

	// Start replicating from new primary (use the proper function that adds to WaitGroup)
	_ = fm.replicationMgr.startReplica()

	return nil
}

// ReplicationStatus returns a summary of the replication status.
type ReplicationStatus struct {
	Role            Role
	State           ReplicationState
	PrimaryAddr     string
	Replicas        []ReplicaInfo
	AppliedLSN      uint64
	ReceivedLSN     uint64
	CurrentLSN      uint64
	LagBytes        int64
	BytesSent       uint64
	BytesReceived   uint64
	RecordsSent     uint64
	RecordsReceived uint64
}

// Status returns the current replication status.
func (fm *FailoverManager) Status() ReplicationStatus {
	mgr := fm.replicationMgr

	status := ReplicationStatus{
		Role:            mgr.Role(),
		Replicas:        mgr.Replicas(),
		AppliedLSN:      uint64(mgr.AppliedLSN()),
		ReceivedLSN:     uint64(mgr.ReceivedLSN()),
		CurrentLSN:      uint64(mgr.wal.CurrentLSN()),
		LagBytes:        mgr.ReplicationLag(),
		BytesSent:       mgr.bytesSent.Load(),
		BytesReceived:   mgr.bytesReceived.Load(),
		RecordsSent:     mgr.recordsSent.Load(),
		RecordsReceived: mgr.recordsRecv.Load(),
	}

	if mgr.IsReplica() {
		status.State = mgr.ReplicationState()
		status.PrimaryAddr = mgr.PrimaryAddress()
	}

	return status
}

// ClusterInfo represents information about the replication cluster.
type ClusterInfo struct {
	// Primary is the ID of the current primary.
	Primary string

	// PrimaryAddr is the address of the current primary.
	PrimaryAddr string

	// Replicas is the list of replica nodes.
	Replicas []ReplicaInfo

	// TotalNodes is the total number of nodes in the cluster.
	TotalNodes int

	// HealthyNodes is the number of healthy nodes.
	HealthyNodes int
}

// ClusterInfo returns information about the replication cluster.
func (fm *FailoverManager) ClusterInfo() ClusterInfo {
	mgr := fm.replicationMgr
	replicas := mgr.Replicas()

	info := ClusterInfo{
		Replicas:   replicas,
		TotalNodes: len(replicas) + 1, // +1 for primary
	}

	if mgr.IsPrimary() {
		info.Primary = mgr.config.NodeID
		info.PrimaryAddr = mgr.config.ListenAddr
		info.HealthyNodes = 1 // Primary is healthy
		for _, r := range replicas {
			if fm.IsHealthy(r) {
				info.HealthyNodes++
			}
		}
	} else {
		info.PrimaryAddr = mgr.config.PrimaryAddr
		// Can't determine primary ID from replica side easily
		if mgr.ReplicationState() == StateSynced || mgr.ReplicationState() == StateStreaming {
			info.HealthyNodes = 1 // We're healthy
		}
	}

	return info
}

// RegisterReplica registers a new replica address to the cluster.
// This is used when dynamically adding replicas.
func (fm *FailoverManager) RegisterReplica(id, address string) error {
	// This just records the address; the replica still needs to connect
	// In a more complete implementation, this could notify the replica
	// to start replicating.
	return nil
}

// UnregisterReplica removes a replica from the cluster.
func (fm *FailoverManager) UnregisterReplica(id string) error {
	mgr := fm.replicationMgr

	mgr.replicasMu.Lock()
	defer mgr.replicasMu.Unlock()

	if rc, ok := mgr.replicas[id]; ok {
		rc.cancel()
		_ = rc.conn.Close()
		delete(mgr.replicas, id)
	}

	return nil
}

// encodeClusterInfo encodes cluster info for transmission.
func encodeClusterInfo(info ClusterInfo) []byte {
	// Simple encoding: [primaryLen:2][primary:var][addrLen:2][addr:var][replicaCount:2][replicas...]
	data := make([]byte, 0, 256)

	// Primary ID
	primaryBytes := []byte(info.Primary)
	lenBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(lenBuf, uint16(len(primaryBytes)))
	data = append(data, lenBuf...)
	data = append(data, primaryBytes...)

	// Primary address
	addrBytes := []byte(info.PrimaryAddr)
	binary.BigEndian.PutUint16(lenBuf, uint16(len(addrBytes)))
	data = append(data, lenBuf...)
	data = append(data, addrBytes...)

	// Replica count
	binary.BigEndian.PutUint16(lenBuf, uint16(len(info.Replicas)))
	data = append(data, lenBuf...)

	// Each replica: [idLen:2][id:var][addrLen:2][addr:var][state:1]
	for _, r := range info.Replicas {
		idBytes := []byte(r.ID)
		binary.BigEndian.PutUint16(lenBuf, uint16(len(idBytes)))
		data = append(data, lenBuf...)
		data = append(data, idBytes...)

		rAddrBytes := []byte(r.Address)
		binary.BigEndian.PutUint16(lenBuf, uint16(len(rAddrBytes)))
		data = append(data, lenBuf...)
		data = append(data, rAddrBytes...)

		data = append(data, byte(r.State))
	}

	return data
}
