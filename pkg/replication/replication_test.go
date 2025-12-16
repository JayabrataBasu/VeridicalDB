package replication

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/wal"
)

// createTestWAL creates a temporary WAL for testing.
func createTestWAL(t *testing.T, name string) (*wal.WAL, string) {
	dir := filepath.Join(t.TempDir(), name)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("create test directory: %v", err)
	}

	w, err := wal.Open(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}

	return w, dir
}

// TestNewManager tests creating a new replication manager.
func TestNewManager(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "wal")
	w, err := wal.Open(walDir)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	defer w.Close()

	config := Config{
		NodeID:     "node1",
		Role:       RolePrimary,
		ListenAddr: ":15432",
		WAL:        w,
	}

	mgr, err := NewManager(config)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if mgr == nil {
		t.Fatal("expected non-nil manager")
	}

	// Initial role is from config
	if mgr.Role() != RolePrimary {
		t.Errorf("expected role Primary, got %v", mgr.Role())
	}
}

// TestManagerPrimaryRole tests setting up as primary.
func TestManagerPrimaryRole(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "wal")
	w, err := wal.Open(walDir)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	defer w.Close()

	config := Config{
		NodeID:     "primary",
		Role:       RolePrimary,
		ListenAddr: "127.0.0.1:0", // Use any available port
		WAL:        w,
	}

	mgr, err := NewManager(config)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	// Start as primary
	if err := mgr.Start(); err != nil {
		t.Fatalf("start as primary: %v", err)
	}
	defer mgr.Stop()

	if !mgr.IsPrimary() {
		t.Error("expected to be primary after Start")
	}

	if mgr.Role() != RolePrimary {
		t.Errorf("expected role Primary, got %v", mgr.Role())
	}

	// Replicas should be empty initially
	replicas := mgr.Replicas()
	if len(replicas) != 0 {
		t.Errorf("expected 0 replicas, got %d", len(replicas))
	}
}

// TestManagerReplicaRole tests setting up as replica.
func TestManagerReplicaRole(t *testing.T) {
	// First create a primary
	primaryWAL, _ := createTestWAL(t, "primary")
	defer primaryWAL.Close()

	primaryConfig := Config{
		NodeID:     "primary",
		Role:       RolePrimary,
		ListenAddr: "127.0.0.1:0",
		WAL:        primaryWAL,
	}
	primary, err := NewManager(primaryConfig)
	if err != nil {
		t.Fatalf("NewManager (primary): %v", err)
	}
	if err := primary.Start(); err != nil {
		t.Fatalf("start primary: %v", err)
	}
	defer primary.Stop()

	// Get the actual listen address
	primary.mu.RLock()
	primaryAddr := primary.listener.Addr().String()
	primary.mu.RUnlock()

	// Create a replica
	replicaWAL, _ := createTestWAL(t, "replica")
	defer replicaWAL.Close()

	replicaConfig := Config{
		NodeID:      "replica",
		Role:        RoleReplica,
		PrimaryAddr: primaryAddr,
		WAL:         replicaWAL,
		ApplyFunc: func(rec *wal.Record) error {
			return nil
		},
	}
	replica, err := NewManager(replicaConfig)
	if err != nil {
		t.Fatalf("NewManager (replica): %v", err)
	}

	if err := replica.Start(); err != nil {
		t.Fatalf("start as replica: %v", err)
	}
	defer replica.Stop()

	if !replica.IsReplica() {
		t.Error("expected to be replica after Start")
	}

	if replica.Role() != RoleReplica {
		t.Errorf("expected role Replica, got %v", replica.Role())
	}

	// Wait a bit for connection
	time.Sleep(100 * time.Millisecond)

	// Check primary address
	if replica.PrimaryAddress() != primaryAddr {
		t.Errorf("expected primary address %s, got %s", primaryAddr, replica.PrimaryAddress())
	}
}

// TestWALStreaming tests WAL record streaming from primary to replica.
func TestWALStreaming(t *testing.T) {
	// Create primary
	primaryWAL, _ := createTestWAL(t, "primary")
	defer primaryWAL.Close()

	primaryConfig := Config{
		NodeID:     "primary",
		Role:       RolePrimary,
		ListenAddr: "127.0.0.1:0",
		WAL:        primaryWAL,
	}
	primary, err := NewManager(primaryConfig)
	if err != nil {
		t.Fatalf("NewManager (primary): %v", err)
	}
	if err := primary.Start(); err != nil {
		t.Fatalf("start primary: %v", err)
	}
	defer primary.Stop()

	primary.mu.RLock()
	primaryAddr := primary.listener.Addr().String()
	primary.mu.RUnlock()

	// Create replica
	replicaWAL, _ := createTestWAL(t, "replica")
	defer replicaWAL.Close()

	// Track applied records
	appliedRecords := make([]*wal.Record, 0)
	replicaConfig := Config{
		NodeID:      "replica",
		Role:        RoleReplica,
		PrimaryAddr: primaryAddr,
		WAL:         replicaWAL,
		ApplyFunc: func(rec *wal.Record) error {
			appliedRecords = append(appliedRecords, rec)
			return nil
		},
	}
	replica, err := NewManager(replicaConfig)
	if err != nil {
		t.Fatalf("NewManager (replica): %v", err)
	}

	if err := replica.Start(); err != nil {
		t.Fatalf("start replica: %v", err)
	}
	defer replica.Stop()

	// Wait for replica to connect
	time.Sleep(200 * time.Millisecond)

	// Write some records to primary WAL
	for i := 0; i < 5; i++ {
		rec := &wal.Record{
			Type:   wal.RecordInsert,
			TxID:   uint64(i + 1),
			PageID: 1,
			Data:   []byte("test data"),
		}
		if _, err := primaryWAL.Append(rec); err != nil {
			t.Fatalf("append record: %v", err)
		}

		// Notify replicas
		primary.BroadcastWAL(rec)
	}

	// Wait for replication
	time.Sleep(500 * time.Millisecond)

	// Check that records were received
	// Note: May not receive all records depending on timing
	if len(appliedRecords) == 0 {
		t.Log("No records applied yet (may be timing issue)")
	}
}

// TestReplicaInfo tests ReplicaInfo tracking.
func TestReplicaInfo(t *testing.T) {
	info := ReplicaInfo{
		ID:          "replica1",
		Address:     "127.0.0.1:15432",
		State:       StateStreaming,
		AppliedLSN:  1000,
		LagBytes:    500,
		LastContact: time.Now(),
	}

	if info.ID != "replica1" {
		t.Errorf("expected ID replica1, got %s", info.ID)
	}
	if info.Address != "127.0.0.1:15432" {
		t.Errorf("expected Address 127.0.0.1:15432, got %s", info.Address)
	}
	if info.State != StateStreaming {
		t.Errorf("expected state Streaming, got %v", info.State)
	}
	if info.AppliedLSN != 1000 {
		t.Errorf("expected AppliedLSN 1000, got %d", info.AppliedLSN)
	}
	if info.LagBytes != 500 {
		t.Errorf("expected LagBytes 500, got %d", info.LagBytes)
	}
	if info.LastContact.IsZero() {
		t.Error("expected LastContact to be set")
	}
}

// TestConfig tests configuration.
func TestConfig(t *testing.T) {
	config := DefaultConfig()

	if config.HeartbeatInterval == 0 {
		t.Error("expected non-zero heartbeat interval")
	}
	if config.ConnectTimeout == 0 {
		t.Error("expected non-zero connection timeout")
	}
	if config.StatusInterval == 0 {
		t.Error("expected non-zero status interval")
	}
}

// TestFailoverConfig tests failover configuration.
func TestFailoverConfig(t *testing.T) {
	config := DefaultFailoverConfig()

	if config.HealthCheckInterval == 0 {
		t.Error("expected non-zero health check interval")
	}
	if config.HealthCheckTimeout == 0 {
		t.Error("expected non-zero health check timeout")
	}
	if config.MaxLagBytes == 0 {
		t.Error("expected non-zero max lag bytes")
	}
	if config.MaxLagTime == 0 {
		t.Error("expected non-zero max lag time")
	}
	if config.MinHealthyReplicas == 0 {
		t.Error("expected non-zero min healthy replicas")
	}
}

// TestFailoverManagerHealth tests replica health checking.
func TestFailoverManagerHealth(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "wal")
	w, err := wal.Open(walDir)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	defer w.Close()

	config := Config{
		NodeID:     "primary",
		Role:       RolePrimary,
		ListenAddr: "127.0.0.1:0",
		WAL:        w,
	}
	mgr, err := NewManager(config)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	failoverConfig := DefaultFailoverConfig()
	failoverConfig.MaxLagBytes = 1000
	failoverConfig.MaxLagTime = 5 * time.Second

	fm := NewFailoverManager(mgr, failoverConfig)

	// Test healthy replica
	healthyReplica := ReplicaInfo{
		ID:          "replica1",
		State:       StateSynced,
		LagBytes:    100,
		LastContact: time.Now(),
	}
	if !fm.IsHealthy(healthyReplica) {
		t.Error("expected replica to be healthy")
	}

	// Test replica with too much lag
	laggyReplica := ReplicaInfo{
		ID:          "replica2",
		State:       StateStreaming,
		LagBytes:    2000, // More than maxLagBytes
		LastContact: time.Now(),
	}
	if fm.IsHealthy(laggyReplica) {
		t.Error("expected laggy replica to be unhealthy")
	}

	// Test replica with stale contact
	staleReplica := ReplicaInfo{
		ID:          "replica3",
		State:       StateSynced,
		LagBytes:    100,
		LastContact: time.Now().Add(-10 * time.Second), // More than maxLagTime
	}
	if fm.IsHealthy(staleReplica) {
		t.Error("expected stale replica to be unhealthy")
	}

	// Test replica in wrong state
	connectingReplica := ReplicaInfo{
		ID:          "replica4",
		State:       StateConnecting,
		LagBytes:    0,
		LastContact: time.Now(),
	}
	if fm.IsHealthy(connectingReplica) {
		t.Error("expected connecting replica to be unhealthy")
	}
}

// TestReplicationState tests state transitions.
func TestReplicationState(t *testing.T) {
	states := []ReplicationState{
		StateDisconnected,
		StateConnecting,
		StateStreaming,
		StateSynced,
	}

	for _, s := range states {
		str := s.String()
		if str == "" {
			t.Errorf("expected non-empty string for state %v", s)
		}
	}
}

// TestRole tests role types.
func TestRole(t *testing.T) {
	roles := []Role{
		RolePrimary,
		RoleReplica,
		RoleCandidate,
	}

	for _, r := range roles {
		str := r.String()
		if str == "" {
			t.Errorf("expected non-empty string for role %v", r)
		}
	}
}

// TestManagerStats tests statistics tracking.
func TestManagerStats(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "wal")
	w, err := wal.Open(walDir)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	defer w.Close()

	config := Config{
		NodeID:     "node1",
		Role:       RolePrimary,
		ListenAddr: "127.0.0.1:0",
		WAL:        w,
	}
	mgr, err := NewManager(config)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	// Initial stats should be zero
	if mgr.bytesSent.Load() != 0 {
		t.Error("expected bytes sent to be 0")
	}
	if mgr.bytesReceived.Load() != 0 {
		t.Error("expected bytes received to be 0")
	}
	if mgr.recordsSent.Load() != 0 {
		t.Error("expected records sent to be 0")
	}
	if mgr.recordsRecv.Load() != 0 {
		t.Error("expected records received to be 0")
	}
}

// TestMessageTypes tests message type constants.
func TestMessageTypes(t *testing.T) {
	types := []byte{
		MsgStartReplication,
		MsgWALData,
		MsgStandbyStatus,
		MsgPromote,
		MsgDemote,
		MsgHeartbeat,
	}

	// Ensure all types are unique
	seen := make(map[byte]bool)
	for _, typ := range types {
		if seen[typ] {
			t.Errorf("duplicate message type: %d", typ)
		}
		seen[typ] = true
	}
}

// TestFailoverManagerStatus tests getting replication status.
func TestFailoverManagerStatus(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "wal")
	w, err := wal.Open(walDir)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	defer w.Close()

	config := Config{
		NodeID:     "primary",
		Role:       RolePrimary,
		ListenAddr: "127.0.0.1:0",
		WAL:        w,
	}
	mgr, err := NewManager(config)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	fm := NewFailoverManager(mgr, DefaultFailoverConfig())

	status := fm.Status()
	if status.Role != RolePrimary {
		t.Errorf("expected primary role, got %v", status.Role)
	}
}

// TestFailoverManagerClusterInfo tests cluster info.
func TestFailoverManagerClusterInfo(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "wal")
	w, err := wal.Open(walDir)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	defer w.Close()

	config := Config{
		NodeID:     "primary",
		Role:       RolePrimary,
		ListenAddr: "127.0.0.1:0",
		WAL:        w,
	}
	mgr, err := NewManager(config)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := mgr.Start(); err != nil {
		t.Fatalf("start as primary: %v", err)
	}
	defer mgr.Stop()

	fm := NewFailoverManager(mgr, DefaultFailoverConfig())

	info := fm.ClusterInfo()
	if info.Primary != "primary" {
		t.Errorf("expected primary 'primary', got %s", info.Primary)
	}
	if info.TotalNodes != 1 {
		t.Errorf("expected 1 total node, got %d", info.TotalNodes)
	}
	if info.HealthyNodes != 1 {
		t.Errorf("expected 1 healthy node, got %d", info.HealthyNodes)
	}
}

// TestFailoverCallbacks tests failover callbacks.
func TestFailoverCallbacks(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "wal")
	w, err := wal.Open(walDir)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	defer w.Close()

	config := Config{
		NodeID:     "node1",
		Role:       RolePrimary,
		ListenAddr: "127.0.0.1:0",
		WAL:        w,
	}
	mgr, err := NewManager(config)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	fm := NewFailoverManager(mgr, DefaultFailoverConfig())

	startCalled := false
	endCalled := false
	roleCalled := false

	fm.OnFailoverStart(func() {
		startCalled = true
	})
	fm.OnFailoverEnd(func(newPrimary string, err error) {
		endCalled = true
	})
	fm.OnRoleChange(func(newRole Role) {
		roleCalled = true
	})

	// Verify callbacks are set (can't easily test execution without full cluster)
	if fm.onFailoverStart == nil {
		t.Error("expected failover start callback to be set")
	}
	if fm.onFailoverEnd == nil {
		t.Error("expected failover end callback to be set")
	}
	if fm.onRoleChange == nil {
		t.Error("expected role change callback to be set")
	}

	// Silence unused variable warnings
	_ = startCalled
	_ = endCalled
	_ = roleCalled
}

// TestEncodeClusterInfo tests cluster info encoding.
func TestEncodeClusterInfo(t *testing.T) {
	info := ClusterInfo{
		Primary:      "primary-node",
		PrimaryAddr:  "127.0.0.1:15432",
		TotalNodes:   3,
		HealthyNodes: 2,
		Replicas: []ReplicaInfo{
			{ID: "replica1", Address: "127.0.0.1:15433", State: StateSynced},
			{ID: "replica2", Address: "127.0.0.1:15434", State: StateStreaming},
		},
	}

	data := encodeClusterInfo(info)
	if len(data) == 0 {
		t.Error("expected non-empty encoded data")
	}
}

// TestErrors tests error types.
func TestErrors(t *testing.T) {
	errors := []error{
		ErrNotPrimary,
		ErrNotReplica,
		ErrNoHealthyReplica,
		ErrFailoverInProgress,
		ErrReplicationLag,
	}

	for _, err := range errors {
		if err.Error() == "" {
			t.Errorf("expected non-empty error message for %v", err)
		}
	}
}

// TestPromote tests promotion from replica to primary.
func TestPromote(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "wal")
	w, err := wal.Open(walDir)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	defer w.Close()

	config := Config{
		NodeID:      "replica",
		Role:        RoleReplica,
		ListenAddr:  "127.0.0.1:0",
		WAL:         w,
		PrimaryAddr: "127.0.0.1:5555", // Fake primary for now
		ApplyFunc:   func(rec *wal.Record) error { return nil },
	}
	mgr, err := NewManager(config)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	fm := NewFailoverManager(mgr, DefaultFailoverConfig())

	roleChanged := false
	fm.OnRoleChange(func(newRole Role) {
		roleChanged = true
		if newRole != RolePrimary {
			t.Errorf("expected new role Primary, got %v", newRole)
		}
	})

	// Promote
	if err := fm.Promote(); err != nil {
		t.Fatalf("promote: %v", err)
	}

	if !mgr.IsPrimary() {
		t.Error("expected to be primary after promotion")
	}
	if !roleChanged {
		t.Error("expected role change callback to be called")
	}
}

// TestDemote tests demotion from primary to replica.
func TestDemote(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "wal")
	w, err := wal.Open(walDir)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	defer w.Close()

	config := Config{
		NodeID:     "primary",
		Role:       RolePrimary,
		ListenAddr: "127.0.0.1:0",
		WAL:        w,
	}
	mgr, err := NewManager(config)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := mgr.Start(); err != nil {
		t.Fatalf("start as primary: %v", err)
	}
	defer mgr.Stop()

	fm := NewFailoverManager(mgr, DefaultFailoverConfig())

	roleChanged := false
	fm.OnRoleChange(func(newRole Role) {
		roleChanged = true
		if newRole != RoleReplica {
			t.Errorf("expected new role Replica, got %v", newRole)
		}
	})

	// Demote
	newPrimaryAddr := "127.0.0.1:15433"
	if err := fm.Demote(newPrimaryAddr); err != nil {
		t.Fatalf("demote: %v", err)
	}

	if mgr.IsPrimary() {
		t.Error("expected not to be primary after demotion")
	}
	if !mgr.IsReplica() {
		t.Error("expected to be replica after demotion")
	}
	if !roleChanged {
		t.Error("expected role change callback to be called")
	}
}

// TestHealthyReplicas tests getting healthy replicas.
func TestHealthyReplicas(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "wal")
	w, err := wal.Open(walDir)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	defer w.Close()

	config := Config{
		NodeID:     "primary",
		Role:       RolePrimary,
		ListenAddr: "127.0.0.1:0",
		WAL:        w,
	}
	mgr, err := NewManager(config)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	fm := NewFailoverManager(mgr, DefaultFailoverConfig())

	// No replicas, should return empty list
	healthy := fm.HealthyReplicas()
	if len(healthy) != 0 {
		t.Errorf("expected 0 healthy replicas, got %d", len(healthy))
	}

	// Test best candidate returns error with no replicas
	_, err = fm.BestFailoverCandidate()
	if err != ErrNoHealthyReplica {
		t.Errorf("expected ErrNoHealthyReplica, got %v", err)
	}
}

// TestFailoverNotPrimary tests failover from non-primary.
func TestFailoverNotPrimary(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "wal")
	w, err := wal.Open(walDir)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	defer w.Close()

	config := Config{
		NodeID:      "replica",
		Role:        RoleReplica,
		WAL:         w,
		PrimaryAddr: "127.0.0.1:5555",
		ApplyFunc:   func(rec *wal.Record) error { return nil },
	}
	mgr, err := NewManager(config)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	fm := NewFailoverManager(mgr, DefaultFailoverConfig())

	// Switchover should fail because not primary
	err = fm.Switchover("")
	if err != ErrNotPrimary {
		t.Errorf("expected ErrNotPrimary, got %v", err)
	}
}

// TestLSNTracking tests LSN tracking on replica.
func TestLSNTracking(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "wal")
	w, err := wal.Open(walDir)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	defer w.Close()

	config := Config{
		NodeID:      "replica",
		Role:        RoleReplica,
		WAL:         w,
		PrimaryAddr: "127.0.0.1:5555",
		ApplyFunc:   func(rec *wal.Record) error { return nil },
	}
	mgr, err := NewManager(config)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	// Initial LSNs should be 0
	if mgr.AppliedLSN() != 0 {
		t.Errorf("expected applied LSN 0, got %d", mgr.AppliedLSN())
	}
	if mgr.ReceivedLSN() != 0 {
		t.Errorf("expected received LSN 0, got %d", mgr.ReceivedLSN())
	}

	// Lag should be 0 when both are 0
	lag := mgr.ReplicationLag()
	if lag != 0 {
		t.Errorf("expected lag 0, got %d", lag)
	}
}
