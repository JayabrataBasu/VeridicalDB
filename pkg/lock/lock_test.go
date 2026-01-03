package lock

import (
	"sync"
	"testing"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/storage"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

func TestBasicLockAcquireRelease(t *testing.T) {
	mgr := NewManager()
	var txID txn.TxID = 10

	resource := TableResource("users")

	// Acquire exclusive lock
	err := mgr.Acquire(txID, resource, ModeExclusive)
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}

	// Check stats
	active, waiting := mgr.Stats()
	if active != 1 || waiting != 0 {
		t.Errorf("expected 1 active, 0 waiting; got %d, %d", active, waiting)
	}

	// Release
	err = mgr.Release(txID, resource)
	if err != nil {
		t.Fatalf("Release failed: %v", err)
	}

	active, waiting = mgr.Stats()
	if active != 0 || waiting != 0 {
		t.Errorf("expected 0 active, 0 waiting after release; got %d, %d", active, waiting)
	}
}

func TestMultipleSharedLocks(t *testing.T) {
	mgr := NewManager()

	resource := TableResource("orders")

	// Multiple transactions can hold shared locks
	for i := txn.TxID(1); i <= 5; i++ {
		err := mgr.Acquire(i, resource, ModeShared)
		if err != nil {
			t.Fatalf("Acquire shared lock %d failed: %v", i, err)
		}
	}

	active, _ := mgr.Stats()
	if active != 5 {
		t.Errorf("expected 5 active shared locks, got %d", active)
	}

	// Release all
	for i := txn.TxID(1); i <= 5; i++ {
		_ = mgr.Release(i, resource)
	}

	active, _ = mgr.Stats()
	if active != 0 {
		t.Errorf("expected 0 active after release, got %d", active)
	}
}

func TestExclusiveBlocksShared(t *testing.T) {
	mgr := NewManager()
	mgr.SetTimeout(100 * time.Millisecond)

	resource := TableResource("products")
	var tx1 txn.TxID = 1
	var tx2 txn.TxID = 2

	// tx1 acquires exclusive
	err := mgr.Acquire(tx1, resource, ModeExclusive)
	if err != nil {
		t.Fatalf("tx1 Acquire failed: %v", err)
	}

	// tx2 tries to acquire shared - should timeout
	err = mgr.Acquire(tx2, resource, ModeShared)
	if err == nil {
		t.Error("expected timeout error for tx2, got nil")
	}

	// Release tx1, now tx2 should be able to acquire
	_ = mgr.Release(tx1, resource)

	err = mgr.Acquire(tx2, resource, ModeShared)
	if err != nil {
		t.Fatalf("tx2 Acquire after release failed: %v", err)
	}
}

func TestSharedBlocksExclusive(t *testing.T) {
	mgr := NewManager()
	mgr.SetTimeout(100 * time.Millisecond)

	resource := TableResource("accounts")
	var tx1 txn.TxID = 1
	var tx2 txn.TxID = 2

	// tx1 acquires shared
	err := mgr.Acquire(tx1, resource, ModeShared)
	if err != nil {
		t.Fatalf("tx1 Acquire failed: %v", err)
	}

	// tx2 tries to acquire exclusive - should timeout
	err = mgr.Acquire(tx2, resource, ModeExclusive)
	if err == nil {
		t.Error("expected timeout error for tx2, got nil")
	}

	// Release tx1, now tx2 should be able to acquire
	_ = mgr.Release(tx1, resource)

	err = mgr.Acquire(tx2, resource, ModeExclusive)
	if err != nil {
		t.Fatalf("tx2 Acquire after release failed: %v", err)
	}
}

func TestRowLocks(t *testing.T) {
	mgr := NewManager()

	rid1 := storage.RID{Table: "users", Page: 1, Slot: 0}
	rid2 := storage.RID{Table: "users", Page: 1, Slot: 1}

	resource1 := RowResource("users", rid1)
	resource2 := RowResource("users", rid2)

	var tx1 txn.TxID = 1
	var tx2 txn.TxID = 2

	// tx1 locks row 1 exclusively
	err := mgr.Acquire(tx1, resource1, ModeExclusive)
	if err != nil {
		t.Fatalf("tx1 Acquire row1 failed: %v", err)
	}

	// tx2 can lock row 2 (different row)
	err = mgr.Acquire(tx2, resource2, ModeExclusive)
	if err != nil {
		t.Fatalf("tx2 Acquire row2 failed: %v", err)
	}

	active, _ := mgr.Stats()
	if active != 2 {
		t.Errorf("expected 2 active locks, got %d", active)
	}
}

func TestLockUpgrade(t *testing.T) {
	mgr := NewManager()

	resource := TableResource("items")
	var txID txn.TxID = 1

	// Acquire shared
	err := mgr.Acquire(txID, resource, ModeShared)
	if err != nil {
		t.Fatalf("Acquire shared failed: %v", err)
	}

	// Upgrade to exclusive (should succeed since only holder)
	err = mgr.Acquire(txID, resource, ModeExclusive)
	if err != nil {
		t.Fatalf("Upgrade to exclusive failed: %v", err)
	}

	// Verify we have exclusive lock now
	active, _ := mgr.Stats()
	if active != 1 {
		t.Errorf("expected 1 active lock after upgrade, got %d", active)
	}
}

func TestReleaseAll(t *testing.T) {
	mgr := NewManager()
	var txID txn.TxID = 1

	// Acquire multiple locks
	resources := []ResourceID{
		TableResource("table1"),
		TableResource("table2"),
		RowResource("table1", storage.RID{Table: "table1", Page: 1, Slot: 0}),
		RowResource("table1", storage.RID{Table: "table1", Page: 1, Slot: 1}),
	}

	for _, r := range resources {
		err := mgr.Acquire(txID, r, ModeExclusive)
		if err != nil {
			t.Fatalf("Acquire %s failed: %v", r, err)
		}
	}

	active, _ := mgr.Stats()
	if active != 4 {
		t.Errorf("expected 4 active locks, got %d", active)
	}

	// Release all at once
	mgr.ReleaseAll(txID)

	active, _ = mgr.Stats()
	if active != 0 {
		t.Errorf("expected 0 active locks after ReleaseAll, got %d", active)
	}
}

func TestConcurrentLocking(t *testing.T) {
	mgr := NewManager()
	mgr.SetTimeout(2 * time.Second)

	resource := TableResource("concurrent_test")
	const numGoroutines = 10
	const iterations = 100

	var wg sync.WaitGroup
	var counter int
	var counterMu sync.Mutex

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(txID txn.TxID) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				err := mgr.Acquire(txID, resource, ModeExclusive)
				if err != nil {
					t.Errorf("goroutine %d: Acquire failed: %v", txID, err)
					return
				}

				// Critical section
				counterMu.Lock()
				counter++
				counterMu.Unlock()

				_ = mgr.Release(txID, resource)
			}
		}(txn.TxID(i + 1))
	}

	wg.Wait()

	expected := numGoroutines * iterations
	if counter != expected {
		t.Errorf("expected counter %d, got %d", expected, counter)
	}
}

func TestWaitAndGrant(t *testing.T) {
	mgr := NewManager()
	mgr.SetTimeout(2 * time.Second)

	resource := TableResource("wait_test")
	var tx1 txn.TxID = 1
	var tx2 txn.TxID = 2

	// tx1 holds exclusive lock
	err := mgr.Acquire(tx1, resource, ModeExclusive)
	if err != nil {
		t.Fatalf("tx1 Acquire failed: %v", err)
	}

	// tx2 waits in background
	var tx2Err error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tx2Err = mgr.Acquire(tx2, resource, ModeShared)
	}()

	// Give tx2 time to start waiting
	time.Sleep(50 * time.Millisecond)

	_, waiting := mgr.Stats()
	if waiting != 1 {
		t.Errorf("expected 1 waiting request, got %d", waiting)
	}

	// Release tx1's lock
	_ = mgr.Release(tx1, resource)

	// Wait for tx2 to complete
	wg.Wait()

	if tx2Err != nil {
		t.Errorf("tx2 should have acquired lock, got error: %v", tx2Err)
	}

	active, waiting := mgr.Stats()
	if active != 1 || waiting != 0 {
		t.Errorf("expected 1 active (tx2), 0 waiting; got %d, %d", active, waiting)
	}
}

func TestResourceIDString(t *testing.T) {
	tableRes := TableResource("users")
	if s := tableRes.String(); s != "table:users" {
		t.Errorf("expected 'table:users', got %q", s)
	}

	rowRes := RowResource("users", storage.RID{Table: "users", Page: 5, Slot: 3})
	// Just check it includes key parts
	s := rowRes.String()
	if s == "" {
		t.Error("row resource string should not be empty")
	}
}

func TestModeString(t *testing.T) {
	if ModeShared.String() != "S" {
		t.Errorf("expected 'S', got %q", ModeShared.String())
	}
	if ModeExclusive.String() != "X" {
		t.Errorf("expected 'X', got %q", ModeExclusive.String())
	}
}
