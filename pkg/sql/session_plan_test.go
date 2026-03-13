package sql

import "testing"

func TestSessionExecuteSQLCachesPreparedPlanForIndexedSelect(t *testing.T) {
	session, _, cleanup := setupIndexTest(t)
	defer cleanup()

	if _, err := session.ExecuteSQL("CREATE TABLE users (id INT, email TEXT);"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := session.ExecuteSQL("INSERT INTO users (id, email) VALUES (1, 'a@example.com');"); err != nil {
		t.Fatalf("insert row: %v", err)
	}
	if _, err := session.ExecuteSQL("CREATE INDEX idx_users_email ON users (email);"); err != nil {
		t.Fatalf("create index: %v", err)
	}

	query := "SELECT id FROM users WHERE email = 'a@example.com';"
	result, err := session.ExecuteSQL(query)
	if err != nil {
		t.Fatalf("execute indexed select: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	entry, found := session.queryCache.Get(query)
	if !found {
		t.Fatal("expected cached query entry")
	}
	plan, ok := entry.PreparedPlan.(*ExecutionPlan)
	if !ok || plan == nil {
		t.Fatal("expected cached execution plan")
	}
	if plan.Type != PlanIndexScan {
		t.Fatalf("expected cached index scan plan, got %v", plan.Type)
	}
}

func TestSessionExecuteSQLReusesPreparedPlanCache(t *testing.T) {
	session, _, cleanup := setupIndexTest(t)
	defer cleanup()

	if _, err := session.ExecuteSQL("CREATE TABLE users (id INT, email TEXT);"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := session.ExecuteSQL("INSERT INTO users (id, email) VALUES (1, 'a@example.com');"); err != nil {
		t.Fatalf("insert row: %v", err)
	}
	if _, err := session.ExecuteSQL("CREATE INDEX idx_users_email ON users (email);"); err != nil {
		t.Fatalf("create index: %v", err)
	}

	query := "SELECT id FROM users WHERE email = 'a@example.com';"
	if _, err := session.ExecuteSQL(query); err != nil {
		t.Fatalf("first select: %v", err)
	}
	statsBefore := session.QueryCacheStats()

	result, err := session.ExecuteSQL(query)
	if err != nil {
		t.Fatalf("second select: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	statsAfter := session.QueryCacheStats()
	if statsAfter.Hits <= statsBefore.Hits {
		t.Fatalf("expected cache hits to increase, before=%d after=%d", statsBefore.Hits, statsAfter.Hits)
	}
}

func TestSessionInvalidatesPreparedPlanOnDropIndex(t *testing.T) {
	session, _, cleanup := setupIndexTest(t)
	defer cleanup()

	if _, err := session.ExecuteSQL("CREATE TABLE users (id INT, email TEXT);"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := session.ExecuteSQL("INSERT INTO users (id, email) VALUES (1, 'a@example.com');"); err != nil {
		t.Fatalf("insert row: %v", err)
	}
	if _, err := session.ExecuteSQL("CREATE INDEX idx_users_email ON users (email);"); err != nil {
		t.Fatalf("create index: %v", err)
	}

	query := "SELECT id FROM users WHERE email = 'a@example.com';"
	if _, err := session.ExecuteSQL(query); err != nil {
		t.Fatalf("execute select: %v", err)
	}
	if _, found := session.queryCache.Get(query); !found {
		t.Fatal("expected cached query before drop index")
	}

	if _, err := session.ExecuteSQL("DROP INDEX idx_users_email;"); err != nil {
		t.Fatalf("drop index: %v", err)
	}

	if _, found := session.queryCache.Get(query); found {
		t.Fatal("expected cached prepared plan to be invalidated after dropping index")
	}
}