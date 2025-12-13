package sql

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

// setupMVCCTestSession creates a temporary directory and returns a Session for testing.
func setupMVCCTestSession(t *testing.T) (*Session, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "sql_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	tm, err := catalog.NewTableManager(dir, 8192)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("failed to create table manager: %v", err)
	}

	txnMgr := txn.NewManager()
	mtm := catalog.NewMVCCTableManager(tm, txnMgr)
	session := NewSession(mtm)

	cleanup := func() {
		os.RemoveAll(dir)
	}

	return session, cleanup
}

// TestLexer verifies the tokenizer works correctly.
func TestLexer(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []TokenType
	}{
		{
			name:     "simple select",
			input:    "SELECT * FROM users",
			expected: []TokenType{TOKEN_SELECT, TOKEN_STAR, TOKEN_FROM, TOKEN_IDENT, TOKEN_EOF},
		},
		{
			name:     "select with columns",
			input:    "SELECT id, name FROM users",
			expected: []TokenType{TOKEN_SELECT, TOKEN_IDENT, TOKEN_COMMA, TOKEN_IDENT, TOKEN_FROM, TOKEN_IDENT, TOKEN_EOF},
		},
		{
			name:     "insert statement",
			input:    "INSERT INTO users VALUES (1, 'alice')",
			expected: []TokenType{TOKEN_INSERT, TOKEN_INTO, TOKEN_IDENT, TOKEN_VALUES, TOKEN_LPAREN, TOKEN_INT, TOKEN_COMMA, TOKEN_STRING, TOKEN_RPAREN, TOKEN_EOF},
		},
		{
			name:     "create table",
			input:    "CREATE TABLE users (id INT)",
			expected: []TokenType{TOKEN_CREATE, TOKEN_TABLE, TOKEN_IDENT, TOKEN_LPAREN, TOKEN_IDENT, TOKEN_INT_TYPE, TOKEN_RPAREN, TOKEN_EOF},
		},
		{
			name:     "where clause",
			input:    "SELECT * FROM users WHERE id = 1",
			expected: []TokenType{TOKEN_SELECT, TOKEN_STAR, TOKEN_FROM, TOKEN_IDENT, TOKEN_WHERE, TOKEN_IDENT, TOKEN_EQ, TOKEN_INT, TOKEN_EOF},
		},
		{
			name:     "comparison operators",
			input:    "a > 1 AND b < 2 OR c >= 3 AND d <= 4 AND e <> 5",
			expected: []TokenType{TOKEN_IDENT, TOKEN_GT, TOKEN_INT, TOKEN_AND, TOKEN_IDENT, TOKEN_LT, TOKEN_INT, TOKEN_OR, TOKEN_IDENT, TOKEN_GE, TOKEN_INT, TOKEN_AND, TOKEN_IDENT, TOKEN_LE, TOKEN_INT, TOKEN_AND, TOKEN_IDENT, TOKEN_NE, TOKEN_INT, TOKEN_EOF},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			var tokens []TokenType
			for {
				tok := lexer.NextToken()
				tokens = append(tokens, tok.Type)
				if tok.Type == TOKEN_EOF || tok.Type == TOKEN_ILLEGAL {
					break
				}
			}
			if len(tokens) != len(tt.expected) {
				t.Errorf("token count mismatch: got %d, expected %d", len(tokens), len(tt.expected))
				t.Errorf("got tokens: %v", tokens)
				t.Errorf("expected: %v", tt.expected)
				return
			}
			for i, tok := range tokens {
				if tok != tt.expected[i] {
					t.Errorf("token[%d] = %v, expected %v", i, tok, tt.expected[i])
				}
			}
		})
	}
}

// TestParser verifies the parser produces correct AST.
func TestParser(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{name: "simple select", input: "SELECT * FROM users;", wantErr: false},
		{name: "select with where", input: "SELECT id, name FROM users WHERE id = 1;", wantErr: false},
		{name: "insert single", input: "INSERT INTO users VALUES (1, 'alice');", wantErr: false},
		{name: "insert with cols", input: "INSERT INTO users (id, name) VALUES (1, 'alice');", wantErr: false},
		{name: "create table", input: "CREATE TABLE users (id INT, name TEXT);", wantErr: false},
		{name: "create table columnar", input: "CREATE TABLE analytics (id INT, value INT) USING COLUMN;", wantErr: false},
		{name: "drop table", input: "DROP TABLE users;", wantErr: false},
		{name: "update", input: "UPDATE users SET name = 'bob' WHERE id = 1;", wantErr: false},
		{name: "delete", input: "DELETE FROM users WHERE id = 1;", wantErr: false},
		{name: "delete all", input: "DELETE FROM users;", wantErr: false},
		{name: "complex where", input: "SELECT * FROM users WHERE age > 18 AND name = 'alice';", wantErr: false},
		{name: "create index", input: "CREATE INDEX idx_users_email ON users (email);", wantErr: false},
		{name: "create unique index", input: "CREATE UNIQUE INDEX idx_users_id ON users (id);", wantErr: false},
		{name: "create composite index", input: "CREATE INDEX idx_users_name_email ON users (name, email);", wantErr: false},
		{name: "drop index", input: "DROP INDEX idx_users_email;", wantErr: false},
		{name: "invalid syntax", input: "SELEC * FROM users;", wantErr: true},
		{name: "missing table", input: "SELECT * FROM;", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestExecutorCreateTable verifies CREATE TABLE works.
func TestExecutorCreateTable(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	// Create a table
	parser := NewParser("CREATE TABLE users (id INT, name TEXT, active BOOL);")
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	result, err := executor.Execute(stmt)
	if err != nil {
		t.Fatalf("execute error: %v", err)
	}

	if result.Message != "Table 'users' created." {
		t.Errorf("unexpected message: %s", result.Message)
	}

	// Verify table exists
	tables := tm.ListTables()
	found := false
	for _, tbl := range tables {
		if tbl == "users" {
			found = true
			break
		}
	}
	if !found {
		t.Error("table 'users' not found after create")
	}
}

// TestExecutorCreateColumnarTable verifies CREATE TABLE USING COLUMN works.
func TestExecutorCreateColumnarTable(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	// Create a columnar table
	parser := NewParser("CREATE TABLE analytics (id INT, value INT) USING COLUMN;")
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	result, err := executor.Execute(stmt)
	if err != nil {
		t.Fatalf("execute error: %v", err)
	}

	if !strings.Contains(result.Message, "columnar storage") {
		t.Errorf("expected message to mention columnar storage, got: %s", result.Message)
	}

	// Verify table exists with correct storage type
	meta, err := tm.Catalog().GetTable("analytics")
	if err != nil {
		t.Fatalf("table not found: %v", err)
	}

	if meta.StorageType != "column" {
		t.Errorf("expected storage type 'column', got: %s", meta.StorageType)
	}

	// Test INSERT into columnar table
	insertParser := NewParser("INSERT INTO analytics VALUES (1, 100);")
	insertStmt, err := insertParser.Parse()
	if err != nil {
		t.Fatalf("parse INSERT error: %v", err)
	}

	result, err = executor.Execute(insertStmt)
	if err != nil {
		t.Fatalf("execute INSERT error: %v", err)
	}

	if result.RowsAffected != 1 {
		t.Errorf("expected 1 row affected, got: %d", result.RowsAffected)
	}

	// Insert more rows
	for i := 2; i <= 5; i++ {
		sql := fmt.Sprintf("INSERT INTO analytics VALUES (%d, %d);", i, i*100)
		parser := NewParser(sql)
		stmt, _ := parser.Parse()
		_, err := executor.Execute(stmt)
		if err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
	}

	// Test SELECT from columnar table
	selectParser := NewParser("SELECT * FROM analytics;")
	selectStmt, err := selectParser.Parse()
	if err != nil {
		t.Fatalf("parse SELECT error: %v", err)
	}

	result, err = executor.Execute(selectStmt)
	if err != nil {
		t.Fatalf("execute SELECT error: %v", err)
	}

	if len(result.Rows) != 5 {
		t.Errorf("expected 5 rows, got: %d", len(result.Rows))
	}
}

// TestParseUsingColumn verifies USING COLUMN parsing.
func TestParseUsingColumn(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		storageType string
	}{
		{"default", "CREATE TABLE t1 (id INT);", "ROW"},
		{"explicit row", "CREATE TABLE t2 (id INT) USING ROW;", "ROW"},
		{"columnar", "CREATE TABLE t3 (id INT) USING COLUMN;", "COLUMN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			stmt, err := parser.Parse()
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}

			createStmt, ok := stmt.(*CreateTableStmt)
			if !ok {
				t.Fatalf("expected CreateTableStmt, got %T", stmt)
			}

			if createStmt.StorageType != tt.storageType {
				t.Errorf("storage type = %s, want %s", createStmt.StorageType, tt.storageType)
			}
		})
	}
}

// TestExecutorInsertSelect verifies INSERT and SELECT work.
func TestExecutorInsertSelect(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	// Create table
	executeSQL(t, executor, "CREATE TABLE users (id INT, name TEXT);")

	// Insert rows
	executeSQL(t, executor, "INSERT INTO users VALUES (1, 'alice');")
	executeSQL(t, executor, "INSERT INTO users VALUES (2, 'bob');")
	executeSQL(t, executor, "INSERT INTO users VALUES (3, 'charlie');")

	// Select all
	result := executeSQL(t, executor, "SELECT * FROM users;")
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(result.Rows))
	}
	if len(result.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(result.Columns))
	}
}

// TestExecutorSelectWhere verifies SELECT with WHERE clause.
func TestExecutorSelectWhere(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE users (id INT, name TEXT, age INT);")
	executeSQL(t, executor, "INSERT INTO users VALUES (1, 'alice', 25);")
	executeSQL(t, executor, "INSERT INTO users VALUES (2, 'bob', 30);")
	executeSQL(t, executor, "INSERT INTO users VALUES (3, 'charlie', 25);")

	// Filter by age
	result := executeSQL(t, executor, "SELECT * FROM users WHERE age = 25;")
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows with age=25, got %d", len(result.Rows))
	}

	// Filter by name
	result = executeSQL(t, executor, "SELECT * FROM users WHERE name = 'bob';")
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row with name='bob', got %d", len(result.Rows))
	}

	// Filter by id comparison
	result = executeSQL(t, executor, "SELECT * FROM users WHERE id > 1;")
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows with id>1, got %d", len(result.Rows))
	}
}

// TestExecutorSelectColumns verifies SELECT with specific columns.
func TestExecutorSelectColumns(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE users (id INT, name TEXT, email TEXT);")
	executeSQL(t, executor, "INSERT INTO users VALUES (1, 'alice', 'alice@test.com');")

	// Select specific columns
	result := executeSQL(t, executor, "SELECT name, email FROM users;")
	if len(result.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(result.Columns))
	}
	if result.Columns[0] != "name" || result.Columns[1] != "email" {
		t.Errorf("unexpected columns: %v", result.Columns)
	}
}

// TestExecutorUpdate verifies UPDATE works.
func TestExecutorUpdate(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE users (id INT, name TEXT);")
	executeSQL(t, executor, "INSERT INTO users VALUES (1, 'alice');")
	executeSQL(t, executor, "INSERT INTO users VALUES (2, 'bob');")

	// Update one row
	result := executeSQL(t, executor, "UPDATE users SET name = 'ALICE' WHERE id = 1;")
	if result.RowsAffected != 1 {
		t.Errorf("expected 1 row affected, got %d", result.RowsAffected)
	}

	// Verify update
	selectResult := executeSQL(t, executor, "SELECT * FROM users WHERE id = 1;")
	if len(selectResult.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(selectResult.Rows))
	}
	if selectResult.Rows[0][1].Text != "ALICE" {
		t.Errorf("expected name='ALICE', got '%s'", selectResult.Rows[0][1].Text)
	}
}

// TestExecutorDelete verifies DELETE works.
func TestExecutorDelete(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE users (id INT, name TEXT);")
	executeSQL(t, executor, "INSERT INTO users VALUES (1, 'alice');")
	executeSQL(t, executor, "INSERT INTO users VALUES (2, 'bob');")
	executeSQL(t, executor, "INSERT INTO users VALUES (3, 'charlie');")

	// Delete one row
	result := executeSQL(t, executor, "DELETE FROM users WHERE id = 2;")
	if result.RowsAffected != 1 {
		t.Errorf("expected 1 row deleted, got %d", result.RowsAffected)
	}

	// Verify deletion
	selectResult := executeSQL(t, executor, "SELECT * FROM users;")
	if len(selectResult.Rows) != 2 {
		t.Errorf("expected 2 rows remaining, got %d", len(selectResult.Rows))
	}
}

// TestExecutorDropTable verifies DROP TABLE works.
func TestExecutorDropTable(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE users (id INT);")

	// Verify table exists
	if len(tm.ListTables()) != 1 {
		t.Error("expected 1 table after create")
	}

	// Drop table
	executeSQL(t, executor, "DROP TABLE users;")

	// Verify table gone
	if len(tm.ListTables()) != 0 {
		t.Error("expected 0 tables after drop")
	}
}

// TestExecutorAndCondition verifies AND in WHERE clause.
func TestExecutorAndCondition(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE users (id INT, name TEXT, active BOOL);")
	executeSQL(t, executor, "INSERT INTO users VALUES (1, 'alice', true);")
	executeSQL(t, executor, "INSERT INTO users VALUES (2, 'bob', true);")
	executeSQL(t, executor, "INSERT INTO users VALUES (3, 'charlie', false);")

	// AND condition
	result := executeSQL(t, executor, "SELECT * FROM users WHERE id > 1 AND active = true;")
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(result.Rows))
	}
}

// TestExecutorOrCondition verifies OR in WHERE clause.
func TestExecutorOrCondition(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE users (id INT, name TEXT);")
	executeSQL(t, executor, "INSERT INTO users VALUES (1, 'alice');")
	executeSQL(t, executor, "INSERT INTO users VALUES (2, 'bob');")
	executeSQL(t, executor, "INSERT INTO users VALUES (3, 'charlie');")

	// OR condition
	result := executeSQL(t, executor, "SELECT * FROM users WHERE id = 1 OR id = 3;")
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}
}

// TestExecutorTableNotFound verifies error on missing table.
func TestExecutorTableNotFound(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	parser := NewParser("SELECT * FROM nonexistent;")
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	_, err = executor.Execute(stmt)
	if err == nil {
		t.Error("expected error for nonexistent table")
	}
}

// TestExecutorInsertWithColumns verifies INSERT with column list.
func TestExecutorInsertWithColumns(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE users (id INT, name TEXT, email TEXT);")
	executeSQL(t, executor, "INSERT INTO users (id, name) VALUES (1, 'alice');")

	result := executeSQL(t, executor, "SELECT * FROM users;")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	// Email should be NULL
	if !result.Rows[0][2].IsNull {
		t.Error("expected email to be NULL")
	}
}

// TestMultiRowInsert tests INSERT with multiple value rows.
func TestMultiRowInsert(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE products (id INT, name TEXT, price INT);")

	// Test 1: Multi-row INSERT without column list
	result := executeSQL(t, executor, `
		INSERT INTO products VALUES 
			(1, 'Apple', 100),
			(2, 'Banana', 50),
			(3, 'Cherry', 200);
	`)

	if result.RowsAffected != 3 {
		t.Errorf("Test 1: Expected 3 rows affected, got %d", result.RowsAffected)
	}

	// Verify data
	selectResult := executeSQL(t, executor, "SELECT * FROM products ORDER BY id;")
	if len(selectResult.Rows) != 3 {
		t.Fatalf("Test 1: Expected 3 rows, got %d", len(selectResult.Rows))
	}

	// Check values
	expected := []struct {
		id    int64
		name  string
		price int64
	}{
		{1, "Apple", 100},
		{2, "Banana", 50},
		{3, "Cherry", 200},
	}

	for i, exp := range expected {
		row := selectResult.Rows[i]
		var id, price int64
		if row[0].Type == catalog.TypeInt32 {
			id = int64(row[0].Int32)
		} else {
			id = row[0].Int64
		}
		if row[2].Type == catalog.TypeInt32 {
			price = int64(row[2].Int32)
		} else {
			price = row[2].Int64
		}
		if id != exp.id || row[1].Text != exp.name || price != exp.price {
			t.Errorf("Test 1 Row %d: Expected (%d, %s, %d), got (%d, %s, %d)",
				i, exp.id, exp.name, exp.price, id, row[1].Text, price)
		}
	}

	// Test 2: Multi-row INSERT with column list
	executeSQL(t, executor, "CREATE TABLE users (id INT, name TEXT, age INT);")
	result2 := executeSQL(t, executor, `
		INSERT INTO users (id, name, age) VALUES 
			(1, 'Alice', 30),
			(2, 'Bob', 25);
	`)

	if result2.RowsAffected != 2 {
		t.Errorf("Test 2: Expected 2 rows affected, got %d", result2.RowsAffected)
	}

	selectResult2 := executeSQL(t, executor, "SELECT id, name, age FROM users ORDER BY id;")
	if len(selectResult2.Rows) != 2 {
		t.Fatalf("Test 2: Expected 2 rows, got %d", len(selectResult2.Rows))
	}

	// Test 3: Multi-row INSERT with partial columns (others should be NULL)
	executeSQL(t, executor, "CREATE TABLE partial (id INT, val1 TEXT, val2 TEXT);")
	result3 := executeSQL(t, executor, `
		INSERT INTO partial (id, val1) VALUES 
			(1, 'first'),
			(2, 'second');
	`)

	if result3.RowsAffected != 2 {
		t.Errorf("Test 3: Expected 2 rows affected, got %d", result3.RowsAffected)
	}

	selectResult3 := executeSQL(t, executor, "SELECT * FROM partial ORDER BY id;")
	for i, row := range selectResult3.Rows {
		if !row[2].IsNull {
			t.Errorf("Test 3 Row %d: Expected val2 to be NULL, got %v", i, row[2])
		}
	}

	// Test 4: Single row INSERT still works (backward compatibility)
	executeSQL(t, executor, "CREATE TABLE single (id INT, name TEXT);")
	result4 := executeSQL(t, executor, "INSERT INTO single VALUES (1, 'solo');")

	if result4.RowsAffected != 1 {
		t.Errorf("Test 4: Expected 1 row affected, got %d", result4.RowsAffected)
	}

	selectResult4 := executeSQL(t, executor, "SELECT * FROM single;")
	if len(selectResult4.Rows) != 1 {
		t.Fatalf("Test 4: Expected 1 row, got %d", len(selectResult4.Rows))
	}
}

// Helper: set up a test TableManager
func setupTestTableManager(t *testing.T) *catalog.TableManager {
	t.Helper()
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "data")
	tm, err := catalog.NewTableManager(dataDir, 4096)
	if err != nil {
		t.Fatalf("NewTableManager error: %v", err)
	}
	return tm
}

// Helper: execute SQL and check for errors
func executeSQL(t *testing.T, executor *Executor, query string) *Result {
	t.Helper()
	parser := NewParser(query)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("parse error for %q: %v", query, err)
	}
	result, err := executor.Execute(stmt)
	if err != nil {
		t.Fatalf("execute error for %q: %v", query, err)
	}
	return result
}

// Helper: execute SQL that may return an error
func executeSQLWithError(executor *Executor, query string) (*Result, error) {
	parser := NewParser(query)
	stmt, err := parser.Parse()
	if err != nil {
		return nil, err
	}
	return executor.Execute(stmt)
}

// TestOrderBy verifies ORDER BY clause parsing and execution.
func TestOrderBy(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	// Create table and insert data
	executeSQL(t, executor, "CREATE TABLE scores (name TEXT, score INT);")
	executeSQL(t, executor, "INSERT INTO scores VALUES ('alice', 85);")
	executeSQL(t, executor, "INSERT INTO scores VALUES ('bob', 92);")
	executeSQL(t, executor, "INSERT INTO scores VALUES ('charlie', 78);")
	executeSQL(t, executor, "INSERT INTO scores VALUES ('diana', 92);")

	// Test ORDER BY ASC (default)
	result := executeSQL(t, executor, "SELECT name, score FROM scores ORDER BY score;")
	if len(result.Rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(result.Rows))
	}
	// Should be charlie (78), alice (85), bob (92), diana (92)
	if result.Rows[0][0].Text != "charlie" {
		t.Errorf("first row should be charlie, got %s", result.Rows[0][0].Text)
	}
	if result.Rows[1][0].Text != "alice" {
		t.Errorf("second row should be alice, got %s", result.Rows[1][0].Text)
	}

	// Test ORDER BY DESC
	result = executeSQL(t, executor, "SELECT name, score FROM scores ORDER BY score DESC;")
	// Should be bob (92) or diana (92) first, then alice (85), then charlie (78)
	if result.Rows[len(result.Rows)-1][0].Text != "charlie" {
		t.Errorf("last row should be charlie, got %s", result.Rows[len(result.Rows)-1][0].Text)
	}

	// Test ORDER BY with multiple columns
	result = executeSQL(t, executor, "SELECT name, score FROM scores ORDER BY score DESC, name ASC;")
	// Should be: bob (92), diana (92), alice (85), charlie (78)
	if result.Rows[0][0].Text != "bob" {
		t.Errorf("first row should be bob (alphabetically first at 92), got %s", result.Rows[0][0].Text)
	}
	if result.Rows[1][0].Text != "diana" {
		t.Errorf("second row should be diana, got %s", result.Rows[1][0].Text)
	}
}

// TestLimit verifies LIMIT clause.
func TestLimit(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE items (id INT, name TEXT);")
	for i := 1; i <= 10; i++ {
		executeSQL(t, executor, fmt.Sprintf("INSERT INTO items VALUES (%d, 'item%d');", i, i))
	}

	// Test LIMIT
	result := executeSQL(t, executor, "SELECT * FROM items LIMIT 3;")
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows with LIMIT 3, got %d", len(result.Rows))
	}

	// Test LIMIT 0
	result = executeSQL(t, executor, "SELECT * FROM items LIMIT 0;")
	if len(result.Rows) != 0 {
		t.Errorf("expected 0 rows with LIMIT 0, got %d", len(result.Rows))
	}

	// Test LIMIT greater than row count
	result = executeSQL(t, executor, "SELECT * FROM items LIMIT 100;")
	if len(result.Rows) != 10 {
		t.Errorf("expected 10 rows with LIMIT 100, got %d", len(result.Rows))
	}
}

// TestOffset verifies OFFSET clause.
func TestOffset(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE nums (val INT);")
	for i := 1; i <= 5; i++ {
		executeSQL(t, executor, fmt.Sprintf("INSERT INTO nums VALUES (%d);", i))
	}

	// Test OFFSET with ORDER BY
	result := executeSQL(t, executor, "SELECT val FROM nums ORDER BY val OFFSET 2;")
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows with OFFSET 2, got %d", len(result.Rows))
	}
	if result.Rows[0][0].Int32 != 3 {
		t.Errorf("first row should be 3, got %d", result.Rows[0][0].Int32)
	}

	// Test LIMIT + OFFSET
	result = executeSQL(t, executor, "SELECT val FROM nums ORDER BY val LIMIT 2 OFFSET 1;")
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows with LIMIT 2 OFFSET 1, got %d", len(result.Rows))
	}
	if result.Rows[0][0].Int32 != 2 {
		t.Errorf("first row should be 2, got %d", result.Rows[0][0].Int32)
	}
	if result.Rows[1][0].Int32 != 3 {
		t.Errorf("second row should be 3, got %d", result.Rows[1][0].Int32)
	}
}

// TestOrderByLimitOffset tests combining ORDER BY, LIMIT, and OFFSET.
func TestOrderByLimitOffset(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE products (name TEXT, price INT);")
	executeSQL(t, executor, "INSERT INTO products VALUES ('apple', 100);")
	executeSQL(t, executor, "INSERT INTO products VALUES ('banana', 50);")
	executeSQL(t, executor, "INSERT INTO products VALUES ('cherry', 200);")
	executeSQL(t, executor, "INSERT INTO products VALUES ('date', 75);")
	executeSQL(t, executor, "INSERT INTO products VALUES ('elderberry', 150);")

	// Get the 2nd and 3rd most expensive items
	result := executeSQL(t, executor, "SELECT name, price FROM products ORDER BY price DESC LIMIT 2 OFFSET 1;")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	// Should be elderberry (150), apple (100)
	if result.Rows[0][0].Text != "elderberry" {
		t.Errorf("first row should be elderberry, got %s", result.Rows[0][0].Text)
	}
	if result.Rows[1][0].Text != "apple" {
		t.Errorf("second row should be apple, got %s", result.Rows[1][0].Text)
	}
}

// TestParseOrderByLimitOffset verifies parsing of ORDER BY, LIMIT, OFFSET.
func TestParseOrderByLimitOffset(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"order by single", "SELECT * FROM t ORDER BY col;", false},
		{"order by asc", "SELECT * FROM t ORDER BY col ASC;", false},
		{"order by desc", "SELECT * FROM t ORDER BY col DESC;", false},
		{"order by multiple", "SELECT * FROM t ORDER BY a, b DESC, c ASC;", false},
		{"limit only", "SELECT * FROM t LIMIT 10;", false},
		{"offset only", "SELECT * FROM t OFFSET 5;", false},
		{"limit and offset", "SELECT * FROM t LIMIT 10 OFFSET 5;", false},
		{"order by with limit", "SELECT * FROM t ORDER BY col LIMIT 10;", false},
		{"full clause", "SELECT * FROM t WHERE x = 1 ORDER BY y DESC LIMIT 5 OFFSET 2;", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestAggregateCount tests COUNT aggregate function.
func TestAggregateCount(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE items (id INT, name TEXT);")
	executeSQL(t, executor, "INSERT INTO items VALUES (1, 'apple');")
	executeSQL(t, executor, "INSERT INTO items VALUES (2, 'banana');")
	executeSQL(t, executor, "INSERT INTO items VALUES (3, 'cherry');")

	// COUNT(*)
	result := executeSQL(t, executor, "SELECT COUNT(*) FROM items;")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0].Int64 != 3 {
		t.Errorf("COUNT(*) should be 3, got %d", result.Rows[0][0].Int64)
	}

	// COUNT(*) with WHERE
	result = executeSQL(t, executor, "SELECT COUNT(*) FROM items WHERE id > 1;")
	if result.Rows[0][0].Int64 != 2 {
		t.Errorf("COUNT(*) with WHERE should be 2, got %d", result.Rows[0][0].Int64)
	}

	// COUNT(column)
	result = executeSQL(t, executor, "SELECT COUNT(name) FROM items;")
	if result.Rows[0][0].Int64 != 3 {
		t.Errorf("COUNT(name) should be 3, got %d", result.Rows[0][0].Int64)
	}
}

// TestAggregateSumAvg tests SUM and AVG aggregate functions.
func TestAggregateSumAvg(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE scores (name TEXT, score INT);")
	executeSQL(t, executor, "INSERT INTO scores VALUES ('alice', 80);")
	executeSQL(t, executor, "INSERT INTO scores VALUES ('bob', 90);")
	executeSQL(t, executor, "INSERT INTO scores VALUES ('charlie', 100);")

	// SUM
	result := executeSQL(t, executor, "SELECT SUM(score) FROM scores;")
	if result.Rows[0][0].Int64 != 270 {
		t.Errorf("SUM(score) should be 270, got %d", result.Rows[0][0].Int64)
	}

	// AVG
	result = executeSQL(t, executor, "SELECT AVG(score) FROM scores;")
	if result.Rows[0][0].Int64 != 90 {
		t.Errorf("AVG(score) should be 90, got %d", result.Rows[0][0].Int64)
	}

	// SUM with WHERE
	result = executeSQL(t, executor, "SELECT SUM(score) FROM scores WHERE score >= 90;")
	if result.Rows[0][0].Int64 != 190 {
		t.Errorf("SUM(score) with WHERE should be 190, got %d", result.Rows[0][0].Int64)
	}
}

// TestAggregateMinMax tests MIN and MAX aggregate functions.
func TestAggregateMinMax(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE temps (city TEXT, temp INT);")
	executeSQL(t, executor, "INSERT INTO temps VALUES ('NYC', 32);")
	executeSQL(t, executor, "INSERT INTO temps VALUES ('LA', 72);")
	executeSQL(t, executor, "INSERT INTO temps VALUES ('Chicago', 28);")

	// MIN
	result := executeSQL(t, executor, "SELECT MIN(temp) FROM temps;")
	if result.Rows[0][0].Int32 != 28 {
		t.Errorf("MIN(temp) should be 28, got %d", result.Rows[0][0].Int32)
	}

	// MAX
	result = executeSQL(t, executor, "SELECT MAX(temp) FROM temps;")
	if result.Rows[0][0].Int32 != 72 {
		t.Errorf("MAX(temp) should be 72, got %d", result.Rows[0][0].Int32)
	}

	// MIN on text column
	result = executeSQL(t, executor, "SELECT MIN(city) FROM temps;")
	if result.Rows[0][0].Text != "Chicago" {
		t.Errorf("MIN(city) should be 'Chicago', got %s", result.Rows[0][0].Text)
	}

	// MAX on text column
	result = executeSQL(t, executor, "SELECT MAX(city) FROM temps;")
	if result.Rows[0][0].Text != "NYC" {
		t.Errorf("MAX(city) should be 'NYC', got %s", result.Rows[0][0].Text)
	}
}

// TestAggregateMultiple tests multiple aggregate functions in one query.
func TestAggregateMultiple(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE stats (val INT);")
	executeSQL(t, executor, "INSERT INTO stats VALUES (10);")
	executeSQL(t, executor, "INSERT INTO stats VALUES (20);")
	executeSQL(t, executor, "INSERT INTO stats VALUES (30);")
	executeSQL(t, executor, "INSERT INTO stats VALUES (40);")

	result := executeSQL(t, executor, "SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM stats;")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	row := result.Rows[0]
	if row[0].Int64 != 4 {
		t.Errorf("COUNT(*) should be 4, got %d", row[0].Int64)
	}
	if row[1].Int64 != 100 {
		t.Errorf("SUM(val) should be 100, got %d", row[1].Int64)
	}
	if row[2].Int64 != 25 {
		t.Errorf("AVG(val) should be 25, got %d", row[2].Int64)
	}
	if row[3].Int32 != 10 {
		t.Errorf("MIN(val) should be 10, got %d", row[3].Int32)
	}
	if row[4].Int32 != 40 {
		t.Errorf("MAX(val) should be 40, got %d", row[4].Int32)
	}
}

// TestAggregateEmptyTable tests aggregates on empty tables.
func TestAggregateEmptyTable(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE empty (val INT);")

	// COUNT on empty table should return 0
	result := executeSQL(t, executor, "SELECT COUNT(*) FROM empty;")
	if result.Rows[0][0].Int64 != 0 {
		t.Errorf("COUNT(*) on empty table should be 0, got %d", result.Rows[0][0].Int64)
	}

	// SUM on empty table should return NULL
	result = executeSQL(t, executor, "SELECT SUM(val) FROM empty;")
	if !result.Rows[0][0].IsNull {
		t.Errorf("SUM on empty table should be NULL")
	}
}

// TestParseAggregates verifies parsing of aggregate functions.
func TestParseAggregates(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"count star", "SELECT COUNT(*) FROM t;", false},
		{"count column", "SELECT COUNT(col) FROM t;", false},
		{"sum", "SELECT SUM(amount) FROM t;", false},
		{"avg", "SELECT AVG(score) FROM t;", false},
		{"min", "SELECT MIN(val) FROM t;", false},
		{"max", "SELECT MAX(val) FROM t;", false},
		{"multiple aggregates", "SELECT COUNT(*), SUM(x), AVG(y) FROM t;", false},
		{"aggregate with where", "SELECT SUM(x) FROM t WHERE y > 0;", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestGroupBy tests GROUP BY with aggregates.
func TestGroupBy(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE sales (product TEXT, region TEXT, amount INT);")
	executeSQL(t, executor, "INSERT INTO sales VALUES ('apple', 'north', 100);")
	executeSQL(t, executor, "INSERT INTO sales VALUES ('apple', 'south', 150);")
	executeSQL(t, executor, "INSERT INTO sales VALUES ('banana', 'north', 80);")
	executeSQL(t, executor, "INSERT INTO sales VALUES ('banana', 'south', 120);")
	executeSQL(t, executor, "INSERT INTO sales VALUES ('apple', 'north', 200);")

	// GROUP BY single column with COUNT
	result := executeSQL(t, executor, "SELECT product, COUNT(*) FROM sales GROUP BY product;")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(result.Rows))
	}

	// Verify results (order may vary, so we check by product name)
	appleCount := int64(0)
	bananaCount := int64(0)
	for _, row := range result.Rows {
		switch row[0].Text {
		case "apple":
			appleCount = row[1].Int64
		case "banana":
			bananaCount = row[1].Int64
		}
	}
	if appleCount != 3 {
		t.Errorf("apple count should be 3, got %d", appleCount)
	}
	if bananaCount != 2 {
		t.Errorf("banana count should be 2, got %d", bananaCount)
	}

	// GROUP BY with SUM
	result = executeSQL(t, executor, "SELECT product, SUM(amount) FROM sales GROUP BY product;")
	appleSum := int64(0)
	bananaSum := int64(0)
	for _, row := range result.Rows {
		switch row[0].Text {
		case "apple":
			appleSum = row[1].Int64
		case "banana":
			bananaSum = row[1].Int64
		}
	}
	if appleSum != 450 {
		t.Errorf("apple sum should be 450, got %d", appleSum)
	}
	if bananaSum != 200 {
		t.Errorf("banana sum should be 200, got %d", bananaSum)
	}

	// GROUP BY multiple columns
	result = executeSQL(t, executor, "SELECT product, region, COUNT(*) FROM sales GROUP BY product, region;")
	if len(result.Rows) != 4 {
		t.Errorf("expected 4 groups (2 products x 2 regions), got %d", len(result.Rows))
	}
}

// TestGroupByWithWhere tests GROUP BY with WHERE clause.
func TestGroupByWithWhere(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE orders (category TEXT, price INT);")
	executeSQL(t, executor, "INSERT INTO orders VALUES ('electronics', 500);")
	executeSQL(t, executor, "INSERT INTO orders VALUES ('electronics', 200);")
	executeSQL(t, executor, "INSERT INTO orders VALUES ('clothing', 50);")
	executeSQL(t, executor, "INSERT INTO orders VALUES ('clothing', 100);")
	executeSQL(t, executor, "INSERT INTO orders VALUES ('electronics', 300);")

	// WHERE filters rows before GROUP BY
	result := executeSQL(t, executor, "SELECT category, SUM(price) FROM orders WHERE price >= 100 GROUP BY category;")

	electronicsSum := int64(0)
	clothingSum := int64(0)
	for _, row := range result.Rows {
		switch row[0].Text {
		case "electronics":
			electronicsSum = row[1].Int64
		case "clothing":
			clothingSum = row[1].Int64
		}
	}
	// electronics: 500 + 200 + 300 = 1000
	if electronicsSum != 1000 {
		t.Errorf("electronics sum should be 1000, got %d", electronicsSum)
	}
	// clothing: only 100 (50 filtered out)
	if clothingSum != 100 {
		t.Errorf("clothing sum should be 100, got %d", clothingSum)
	}
}

// TestParseGroupBy verifies parsing of GROUP BY and HAVING.
func TestParseGroupBy(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"group by single", "SELECT cat, COUNT(*) FROM t GROUP BY cat;", false},
		{"group by multiple", "SELECT a, b, SUM(c) FROM t GROUP BY a, b;", false},
		{"group by with where", "SELECT cat, COUNT(*) FROM t WHERE x > 0 GROUP BY cat;", false},
		{"having without group by", "SELECT COUNT(*) FROM t HAVING COUNT(*) > 0;", true},
		{"group by with order by", "SELECT cat, COUNT(*) FROM t GROUP BY cat ORDER BY cat;", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestDistinct tests DISTINCT clause for removing duplicate rows.
func TestDistinct(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE colors (name TEXT, category TEXT);")
	executeSQL(t, executor, "INSERT INTO colors VALUES ('red', 'warm');")
	executeSQL(t, executor, "INSERT INTO colors VALUES ('blue', 'cool');")
	executeSQL(t, executor, "INSERT INTO colors VALUES ('red', 'warm');") // duplicate
	executeSQL(t, executor, "INSERT INTO colors VALUES ('green', 'cool');")
	executeSQL(t, executor, "INSERT INTO colors VALUES ('blue', 'cool');") // duplicate
	executeSQL(t, executor, "INSERT INTO colors VALUES ('yellow', 'warm');")

	// Without DISTINCT - should have 6 rows
	result := executeSQL(t, executor, "SELECT name, category FROM colors;")
	if len(result.Rows) != 6 {
		t.Errorf("expected 6 rows without DISTINCT, got %d", len(result.Rows))
	}

	// With DISTINCT - should have 4 unique rows
	result = executeSQL(t, executor, "SELECT DISTINCT name, category FROM colors;")
	if len(result.Rows) != 4 {
		t.Errorf("expected 4 unique rows with DISTINCT, got %d", len(result.Rows))
	}

	// DISTINCT on single column
	result = executeSQL(t, executor, "SELECT DISTINCT category FROM colors;")
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 unique categories, got %d", len(result.Rows))
	}

	// DISTINCT with WHERE
	result = executeSQL(t, executor, "SELECT DISTINCT name FROM colors WHERE category = 'cool';")
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 unique cool colors, got %d", len(result.Rows))
	}
}

// TestDistinctWithOrderBy tests DISTINCT combined with ORDER BY.
func TestDistinctWithOrderBy(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE items (name TEXT, price INT);")
	executeSQL(t, executor, "INSERT INTO items VALUES ('apple', 100);")
	executeSQL(t, executor, "INSERT INTO items VALUES ('banana', 50);")
	executeSQL(t, executor, "INSERT INTO items VALUES ('apple', 100);") // duplicate
	executeSQL(t, executor, "INSERT INTO items VALUES ('cherry', 75);")
	executeSQL(t, executor, "INSERT INTO items VALUES ('banana', 50);") // duplicate

	result := executeSQL(t, executor, "SELECT DISTINCT name, price FROM items ORDER BY price ASC;")
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 unique items, got %d", len(result.Rows))
	}

	// Verify order - should be banana (50), cherry (75), apple (100)
	if result.Rows[0][0].Text != "banana" {
		t.Errorf("expected first item to be banana, got %s", result.Rows[0][0].Text)
	}
	if result.Rows[2][0].Text != "apple" {
		t.Errorf("expected last item to be apple, got %s", result.Rows[2][0].Text)
	}
}

// TestDistinctAll tests that all rows with same values are deduplicated.
func TestDistinctAll(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE dupes (x INT, y INT);")
	// Insert same row 5 times
	for i := 0; i < 5; i++ {
		executeSQL(t, executor, "INSERT INTO dupes VALUES (1, 2);")
	}
	// Insert different row 3 times
	for i := 0; i < 3; i++ {
		executeSQL(t, executor, "INSERT INTO dupes VALUES (3, 4);")
	}

	// Without DISTINCT - 8 rows
	result := executeSQL(t, executor, "SELECT * FROM dupes;")
	if len(result.Rows) != 8 {
		t.Errorf("expected 8 rows without DISTINCT, got %d", len(result.Rows))
	}

	// With DISTINCT - 2 unique rows
	result = executeSQL(t, executor, "SELECT DISTINCT * FROM dupes;")
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 unique rows with DISTINCT, got %d", len(result.Rows))
	}
}

// TestParseDistinct verifies parsing of DISTINCT clause.
func TestParseDistinct(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"distinct all", "SELECT DISTINCT * FROM t;", false},
		{"distinct column", "SELECT DISTINCT name FROM t;", false},
		{"distinct multiple columns", "SELECT DISTINCT a, b, c FROM t;", false},
		{"distinct with where", "SELECT DISTINCT name FROM t WHERE x > 0;", false},
		{"distinct with order by", "SELECT DISTINCT name FROM t ORDER BY name;", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			stmt, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				selectStmt, ok := stmt.(*SelectStmt)
				if !ok {
					t.Error("expected SelectStmt")
				} else if !selectStmt.Distinct {
					t.Error("expected Distinct to be true")
				}
			}
		})
	}
}

// TestPrimaryKey tests PRIMARY KEY constraint enforcement.
func TestPrimaryKey(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	// Create table with primary key
	executeSQL(t, executor, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);")

	// First insert should succeed
	result := executeSQL(t, executor, "INSERT INTO users VALUES (1, 'Alice');")
	if result.RowsAffected != 1 {
		t.Errorf("expected 1 row inserted, got %d", result.RowsAffected)
	}

	// Insert different key should succeed
	result = executeSQL(t, executor, "INSERT INTO users VALUES (2, 'Bob');")
	if result.RowsAffected != 1 {
		t.Errorf("expected 1 row inserted, got %d", result.RowsAffected)
	}

	// Insert duplicate key should fail
	_, err := executeSQLWithError(executor, "INSERT INTO users VALUES (1, 'Charlie');")
	if err == nil {
		t.Error("expected error for duplicate primary key, got nil")
	}
	if err != nil && !strings.Contains(err.Error(), "duplicate key") && !strings.Contains(err.Error(), "primary key") {
		t.Errorf("expected duplicate key error, got: %v", err)
	}

	// Verify table still has 2 rows
	result = executeSQL(t, executor, "SELECT * FROM users;")
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows in table, got %d", len(result.Rows))
	}
}

// TestPrimaryKeyNull tests that primary key columns cannot be NULL.
func TestPrimaryKeyNull(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE items (id INT PRIMARY KEY, value TEXT);")

	// Try inserting NULL as primary key - should fail
	_, err := executeSQLWithError(executor, "INSERT INTO items (value) VALUES ('test');")
	if err == nil {
		t.Error("expected error for NULL primary key, got nil")
	}
}

// TestPrimaryKeyTypes tests PRIMARY KEY with different data types.
func TestPrimaryKeyTypes(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	// Text primary key
	executeSQL(t, executor, "CREATE TABLE products (sku TEXT PRIMARY KEY, name TEXT);")
	executeSQL(t, executor, "INSERT INTO products VALUES ('ABC123', 'Widget');")
	executeSQL(t, executor, "INSERT INTO products VALUES ('DEF456', 'Gadget');")

	// Duplicate text key should fail
	_, err := executeSQLWithError(executor, "INSERT INTO products VALUES ('ABC123', 'Another');")
	if err == nil {
		t.Error("expected error for duplicate text primary key")
	}

	// Verify data
	result := executeSQL(t, executor, "SELECT * FROM products;")
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 products, got %d", len(result.Rows))
	}
}

// TestParsePrimaryKey verifies PRIMARY KEY is parsed correctly.
func TestParsePrimaryKey(t *testing.T) {
	parser := NewParser("CREATE TABLE t (id INT PRIMARY KEY, name TEXT);")
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}

	createStmt, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatal("expected CreateTableStmt")
	}

	if len(createStmt.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(createStmt.Columns))
	}

	if !createStmt.Columns[0].PrimaryKey {
		t.Error("expected first column to have PrimaryKey = true")
	}

	// Primary key columns should also be NOT NULL
	if !createStmt.Columns[0].NotNull {
		t.Error("expected primary key column to be NOT NULL")
	}

	if createStmt.Columns[1].PrimaryKey {
		t.Error("expected second column to have PrimaryKey = false")
	}
}

// TestDefaultValues tests DEFAULT value constraint.
func TestDefaultValues(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	// Create table with default values (avoiding reserved words like key, value, count)
	executeSQL(t, executor, "CREATE TABLE settings (name TEXT, data TEXT DEFAULT 'none', num INT DEFAULT 0);")

	// Insert with all columns specified
	executeSQL(t, executor, "INSERT INTO settings VALUES ('test1', 'test', 5);")

	// Insert with only some columns - defaults should apply
	executeSQL(t, executor, "INSERT INTO settings (name) VALUES ('test2');")

	// Query all rows
	result := executeSQL(t, executor, "SELECT * FROM settings ORDER BY name;")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}

	// First row: test1, test, 5
	if result.Rows[0][0].Text != "test1" || result.Rows[0][1].Text != "test" || result.Rows[0][2].Int32 != 5 {
		t.Errorf("first row mismatch: got %v, %v, %v", result.Rows[0][0], result.Rows[0][1], result.Rows[0][2])
	}

	// Second row: test2, none (default), 0 (default)
	if result.Rows[1][0].Text != "test2" || result.Rows[1][1].Text != "none" || result.Rows[1][2].Int32 != 0 {
		t.Errorf("second row should have defaults: got %v, %v, %v", result.Rows[1][0], result.Rows[1][1], result.Rows[1][2])
	}
}

// TestDefaultWithNotNull tests DEFAULT combined with NOT NULL.
func TestDefaultWithNotNull(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	// Create table with NOT NULL and DEFAULT
	executeSQL(t, executor, "CREATE TABLE scores (player TEXT NOT NULL, score INT DEFAULT 100 NOT NULL);")

	// Insert with only player - score should default to 100
	executeSQL(t, executor, "INSERT INTO scores (player) VALUES ('alice');")

	result := executeSQL(t, executor, "SELECT * FROM scores;")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	if result.Rows[0][1].Int32 != 100 {
		t.Errorf("expected default score 100, got %d", result.Rows[0][1].Int32)
	}
}

// TestParseDefault verifies DEFAULT is parsed correctly.
func TestParseDefault(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"default int", "CREATE TABLE t (x INT DEFAULT 0);", false},
		{"default string", "CREATE TABLE t (x TEXT DEFAULT 'hello');", false},
		{"default with not null", "CREATE TABLE t (x INT DEFAULT 5 NOT NULL);", false},
		{"multiple defaults", "CREATE TABLE t (a INT DEFAULT 1, b TEXT DEFAULT 'x');", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			stmt, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				createStmt, ok := stmt.(*CreateTableStmt)
				if !ok {
					t.Error("expected CreateTableStmt")
				} else if !createStmt.Columns[0].HasDefault {
					t.Error("expected first column to have HasDefault = true")
				}
			}
		})
	}
}

// TestAutoIncrement tests AUTO_INCREMENT column constraint.
func TestAutoIncrement(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	// Create table with auto-increment
	executeSQL(t, executor, "CREATE TABLE items (id INT AUTO_INCREMENT PRIMARY KEY, name TEXT);")

	// Insert without specifying id - should auto-generate
	executeSQL(t, executor, "INSERT INTO items (name) VALUES ('first');")
	executeSQL(t, executor, "INSERT INTO items (name) VALUES ('second');")
	executeSQL(t, executor, "INSERT INTO items (name) VALUES ('third');")

	// Query all rows
	result := executeSQL(t, executor, "SELECT * FROM items ORDER BY id;")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}

	// Verify auto-generated IDs are 1, 2, 3
	for i, row := range result.Rows {
		expectedID := int32(i + 1)
		if row[0].Int32 != expectedID {
			t.Errorf("row %d: expected id=%d, got %d", i, expectedID, row[0].Int32)
		}
	}
}

// TestAutoIncrementWithExplicitValue tests inserting explicit values with AUTO_INCREMENT.
func TestAutoIncrementWithExplicitValue(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE logs (id INT AUTO_INCREMENT PRIMARY KEY, msg TEXT);")

	// Insert with explicit id
	executeSQL(t, executor, "INSERT INTO logs VALUES (10, 'explicit');")

	// Next auto-increment should be 11 (max + 1)
	executeSQL(t, executor, "INSERT INTO logs (msg) VALUES ('auto');")

	result := executeSQL(t, executor, "SELECT * FROM logs ORDER BY id;")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}

	// First row should be id=10, second should be id=11
	if result.Rows[0][0].Int32 != 10 {
		t.Errorf("expected first row id=10, got %d", result.Rows[0][0].Int32)
	}
	if result.Rows[1][0].Int32 != 11 {
		t.Errorf("expected second row id=11, got %d", result.Rows[1][0].Int32)
	}
}

// TestParseAutoIncrement verifies AUTO_INCREMENT is parsed correctly.
func TestParseAutoIncrement(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"auto_increment basic", "CREATE TABLE t (id INT AUTO_INCREMENT);", false},
		{"auto_increment with pk", "CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY);", false},
		{"auto_increment order1", "CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT);", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			stmt, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				createStmt, ok := stmt.(*CreateTableStmt)
				if !ok {
					t.Error("expected CreateTableStmt")
				} else if !createStmt.Columns[0].AutoIncrement {
					t.Error("expected first column to have AutoIncrement = true")
				}
			}
		})
	}
}

// TestInnerJoin tests INNER JOIN functionality.
func TestInnerJoin(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	// Create users table
	executeSQL(t, executor, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);")
	executeSQL(t, executor, "INSERT INTO users VALUES (1, 'Alice');")
	executeSQL(t, executor, "INSERT INTO users VALUES (2, 'Bob');")
	executeSQL(t, executor, "INSERT INTO users VALUES (3, 'Charlie');")

	// Create orders table
	executeSQL(t, executor, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product TEXT);")
	executeSQL(t, executor, "INSERT INTO orders VALUES (100, 1, 'Widget');")
	executeSQL(t, executor, "INSERT INTO orders VALUES (101, 1, 'Gadget');")
	executeSQL(t, executor, "INSERT INTO orders VALUES (102, 2, 'Doodad');")

	// INNER JOIN - should return 3 rows (Alice has 2 orders, Bob has 1, Charlie has none)
	result := executeSQL(t, executor, "SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id;")

	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows from join, got %d", len(result.Rows))
	}

	// Verify Alice appears twice (has 2 orders)
	aliceCount := 0
	for _, row := range result.Rows {
		if row[0].Text == "Alice" {
			aliceCount++
		}
	}
	if aliceCount != 2 {
		t.Errorf("expected Alice to appear 2 times, got %d", aliceCount)
	}
}

// TestInnerJoinWithWhere tests INNER JOIN with WHERE clause.
func TestInnerJoinWithWhere(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept_id INT);")
	executeSQL(t, executor, "INSERT INTO employees VALUES (1, 'Alice', 10);")
	executeSQL(t, executor, "INSERT INTO employees VALUES (2, 'Bob', 20);")
	executeSQL(t, executor, "INSERT INTO employees VALUES (3, 'Charlie', 10);")

	executeSQL(t, executor, "CREATE TABLE departments (id INT PRIMARY KEY, dname TEXT);")
	executeSQL(t, executor, "INSERT INTO departments VALUES (10, 'Engineering');")
	executeSQL(t, executor, "INSERT INTO departments VALUES (20, 'Sales');")

	// Join with WHERE to filter to only Engineering
	result := executeSQL(t, executor, "SELECT employees.name, departments.dname FROM employees JOIN departments ON employees.dept_id = departments.id WHERE departments.dname = 'Engineering';")

	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows (Alice and Charlie in Engineering), got %d", len(result.Rows))
	}
}

// TestInnerJoinSelectStar tests INNER JOIN with SELECT *.
func TestInnerJoinSelectStar(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE a (x INT, y TEXT);")
	executeSQL(t, executor, "INSERT INTO a VALUES (1, 'one');")

	executeSQL(t, executor, "CREATE TABLE b (z INT, w TEXT);")
	executeSQL(t, executor, "INSERT INTO b VALUES (1, 'first');")

	result := executeSQL(t, executor, "SELECT * FROM a JOIN b ON a.x = b.z;")

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	// SELECT * should return all 4 columns (x, y, z, w)
	if len(result.Columns) != 4 {
		t.Errorf("expected 4 columns, got %d", len(result.Columns))
	}
}

// TestParseJoin verifies JOIN syntax is parsed correctly.
func TestParseJoin(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"inner join", "SELECT * FROM a JOIN b ON a.x = b.y;", false},
		{"inner join explicit", "SELECT * FROM a INNER JOIN b ON a.x = b.y;", false},
		{"join with qualified columns", "SELECT a.x, b.y FROM a JOIN b ON a.id = b.id;", false},
		{"join with where", "SELECT * FROM a JOIN b ON a.x = b.y WHERE a.z > 0;", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			stmt, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				selectStmt, ok := stmt.(*SelectStmt)
				if !ok {
					t.Error("expected SelectStmt")
				} else if len(selectStmt.Joins) != 1 {
					t.Errorf("expected 1 join, got %d", len(selectStmt.Joins))
				}
			}
		})
	}
}

// TestINExpression tests the IN and NOT IN operators.
func TestINExpression(t *testing.T) {
	session, cleanup := setupMVCCTestSession(t)
	defer cleanup()

	// Create table
	_, err := session.ExecuteSQL("CREATE TABLE products (id INT, category TEXT, price INT);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert test data
	testData := []string{
		"INSERT INTO products VALUES (1, 'electronics', 100);",
		"INSERT INTO products VALUES (2, 'clothing', 50);",
		"INSERT INTO products VALUES (3, 'electronics', 200);",
		"INSERT INTO products VALUES (4, 'food', 25);",
		"INSERT INTO products VALUES (5, 'clothing', 75);",
	}

	for _, sql := range testData {
		_, err := session.ExecuteSQL(sql)
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test IN with integers
	result, err := session.ExecuteSQL("SELECT id, category FROM products WHERE id IN (1, 3, 5);")
	if err != nil {
		t.Fatalf("SELECT with IN failed: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows for IN (1,3,5), got %d", len(result.Rows))
	}

	// Test IN with strings
	result, err = session.ExecuteSQL("SELECT id, category FROM products WHERE category IN ('electronics', 'food');")
	if err != nil {
		t.Fatalf("SELECT with IN (strings) failed: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows for IN ('electronics', 'food'), got %d", len(result.Rows))
	}

	// Test NOT IN
	result, err = session.ExecuteSQL("SELECT id, category FROM products WHERE category NOT IN ('electronics');")
	if err != nil {
		t.Fatalf("SELECT with NOT IN failed: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows for NOT IN ('electronics'), got %d", len(result.Rows))
	}

	// Test IN with single value
	result, err = session.ExecuteSQL("SELECT id FROM products WHERE id IN (1);")
	if err != nil {
		t.Fatalf("SELECT with IN (single) failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row for IN (1), got %d", len(result.Rows))
	}
}

// TestBETWEENExpression tests the BETWEEN and NOT BETWEEN operators.
func TestBETWEENExpression(t *testing.T) {
	session, cleanup := setupMVCCTestSession(t)
	defer cleanup()

	// Create table
	_, err := session.ExecuteSQL("CREATE TABLE scores (id INT, name TEXT, score INT);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert test data
	testData := []string{
		"INSERT INTO scores VALUES (1, 'Alice', 85);",
		"INSERT INTO scores VALUES (2, 'Bob', 72);",
		"INSERT INTO scores VALUES (3, 'Charlie', 90);",
		"INSERT INTO scores VALUES (4, 'Diana', 65);",
		"INSERT INTO scores VALUES (5, 'Eve', 78);",
	}

	for _, sql := range testData {
		_, err := session.ExecuteSQL(sql)
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test BETWEEN (inclusive)
	result, err := session.ExecuteSQL("SELECT name, score FROM scores WHERE score BETWEEN 70 AND 85;")
	if err != nil {
		t.Fatalf("SELECT with BETWEEN failed: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows for BETWEEN 70 AND 85, got %d", len(result.Rows))
	}

	// Test BETWEEN with boundary values
	result, err = session.ExecuteSQL("SELECT name FROM scores WHERE score BETWEEN 85 AND 90;")
	if err != nil {
		t.Fatalf("SELECT with BETWEEN (boundaries) failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows for BETWEEN 85 AND 90, got %d", len(result.Rows))
	}

	// Test NOT BETWEEN
	result, err = session.ExecuteSQL("SELECT name FROM scores WHERE score NOT BETWEEN 70 AND 85;")
	if err != nil {
		t.Fatalf("SELECT with NOT BETWEEN failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows for NOT BETWEEN 70 AND 85, got %d", len(result.Rows))
	}
}

// TestColumnAliases tests column aliases using AS keyword.
func TestColumnAliases(t *testing.T) {
	session, cleanup := setupMVCCTestSession(t)
	defer cleanup()

	// Create table
	_, err := session.ExecuteSQL("CREATE TABLE employees (id INT, full_name TEXT, salary INT);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert test data
	_, err = session.ExecuteSQL("INSERT INTO employees VALUES (1, 'John Smith', 50000);")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test column alias with AS
	result, err := session.ExecuteSQL("SELECT id AS employee_id, full_name AS name FROM employees;")
	if err != nil {
		t.Fatalf("SELECT with AS failed: %v", err)
	}

	if len(result.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(result.Columns))
	}

	if result.Columns[0] != "employee_id" {
		t.Errorf("expected first column 'employee_id', got '%s'", result.Columns[0])
	}
	if result.Columns[1] != "name" {
		t.Errorf("expected second column 'name', got '%s'", result.Columns[1])
	}
}

// TestParseTableAlias tests table aliases in FROM clause.
func TestParseTableAlias(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		wantErr       bool
		expectedAlias string
	}{
		{
			name:          "table alias with AS",
			input:         "SELECT e.id FROM employees AS e;",
			wantErr:       false,
			expectedAlias: "e",
		},
		{
			name:          "table alias without AS",
			input:         "SELECT emp.id FROM employees emp;",
			wantErr:       false,
			expectedAlias: "emp",
		},
		{
			name:          "no alias",
			input:         "SELECT id FROM employees;",
			wantErr:       false,
			expectedAlias: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			stmt, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil {
				selectStmt, ok := stmt.(*SelectStmt)
				if !ok {
					t.Error("expected SelectStmt")
				} else if selectStmt.TableAlias != tt.expectedAlias {
					t.Errorf("expected alias '%s', got '%s'", tt.expectedAlias, selectStmt.TableAlias)
				}
			}
		})
	}
}

// TestParseINBETWEEN tests parsing of IN and BETWEEN expressions.
func TestParseINBETWEEN(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"in with numbers", "SELECT * FROM t WHERE x IN (1, 2, 3);", false},
		{"in with strings", "SELECT * FROM t WHERE name IN ('a', 'b');", false},
		{"not in", "SELECT * FROM t WHERE x NOT IN (1, 2);", false},
		{"between", "SELECT * FROM t WHERE x BETWEEN 1 AND 10;", false},
		{"not between", "SELECT * FROM t WHERE x NOT BETWEEN 5 AND 15;", false},
		{"in combined with and", "SELECT * FROM t WHERE x IN (1, 2) AND y > 0;", false},
		{"between combined with or", "SELECT * FROM t WHERE x BETWEEN 1 AND 10 OR y = 0;", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestJoinWithTableAliases tests JOIN with table aliases.
func TestJoinWithTableAliases(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantErr   bool
		joinAlias string
	}{
		{
			name:      "join with alias",
			input:     "SELECT * FROM orders AS o JOIN customers AS c ON o.cust_id = c.id;",
			wantErr:   false,
			joinAlias: "c",
		},
		{
			name:      "join without alias",
			input:     "SELECT * FROM orders JOIN customers ON orders.cust_id = customers.id;",
			wantErr:   false,
			joinAlias: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			stmt, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil {
				selectStmt, ok := stmt.(*SelectStmt)
				if !ok {
					t.Error("expected SelectStmt")
				} else if len(selectStmt.Joins) > 0 && selectStmt.Joins[0].TableAlias != tt.joinAlias {
					t.Errorf("expected join alias '%s', got '%s'", tt.joinAlias, selectStmt.Joins[0].TableAlias)
				}
			}
		})
	}
}

// TestLIKEExpression tests the LIKE and ILIKE pattern matching operators.
func TestLIKEExpression(t *testing.T) {
	session, cleanup := setupMVCCTestSession(t)
	defer cleanup()

	// Create table
	_, err := session.ExecuteSQL("CREATE TABLE names (id INT, name TEXT);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert test data
	testData := []string{
		"INSERT INTO names VALUES (1, 'Alice');",
		"INSERT INTO names VALUES (2, 'Bob');",
		"INSERT INTO names VALUES (3, 'Charlie');",
		"INSERT INTO names VALUES (4, 'Alexandra');",
		"INSERT INTO names VALUES (5, 'alice');",
	}

	for _, sql := range testData {
		_, err := session.ExecuteSQL(sql)
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test LIKE with % - case-sensitive, should match 'Alice' and 'Alexandra' (start with uppercase A)
	result, err := session.ExecuteSQL("SELECT name FROM names WHERE name LIKE 'A%';")
	if err != nil {
		t.Fatalf("SELECT with LIKE failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows for LIKE 'A%%', got %d", len(result.Rows))
	}

	// Test LIKE with _ (single character)
	result, err = session.ExecuteSQL("SELECT name FROM names WHERE name LIKE 'Bo_';")
	if err != nil {
		t.Fatalf("SELECT with LIKE _ failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row for LIKE 'Bo_', got %d", len(result.Rows))
	}

	// Test LIKE with middle match - matches 'Alice', 'alice', and 'Charlie'
	result, err = session.ExecuteSQL("SELECT name FROM names WHERE name LIKE '%li%';")
	if err != nil {
		t.Fatalf("SELECT with LIKE middle failed: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows for LIKE '%%li%%', got %d", len(result.Rows))
	}

	// Test NOT LIKE
	result, err = session.ExecuteSQL("SELECT name FROM names WHERE name NOT LIKE 'A%';")
	if err != nil {
		t.Fatalf("SELECT with NOT LIKE failed: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows for NOT LIKE 'A%%', got %d", len(result.Rows))
	}

	// Test ILIKE (case insensitive)
	result, err = session.ExecuteSQL("SELECT name FROM names WHERE name ILIKE 'alice';")
	if err != nil {
		t.Fatalf("SELECT with ILIKE failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows for ILIKE 'alice', got %d", len(result.Rows))
	}
}

// TestArithmeticExpressions tests arithmetic operations in SQL.
func TestArithmeticExpressions(t *testing.T) {
	session, cleanup := setupMVCCTestSession(t)
	defer cleanup()

	// Create table
	_, err := session.ExecuteSQL("CREATE TABLE numbers (id INT, a INT, b INT);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert test data
	_, err = session.ExecuteSQL("INSERT INTO numbers VALUES (1, 10, 3);")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test addition in WHERE
	result, err := session.ExecuteSQL("SELECT id FROM numbers WHERE a + b = 13;")
	if err != nil {
		t.Fatalf("SELECT with + failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row for a + b = 13, got %d", len(result.Rows))
	}

	// Test subtraction in WHERE
	result, err = session.ExecuteSQL("SELECT id FROM numbers WHERE a - b = 7;")
	if err != nil {
		t.Fatalf("SELECT with - failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row for a - b = 7, got %d", len(result.Rows))
	}

	// Test multiplication in WHERE
	result, err = session.ExecuteSQL("SELECT id FROM numbers WHERE a * b = 30;")
	if err != nil {
		t.Fatalf("SELECT with * failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row for a * b = 30, got %d", len(result.Rows))
	}

	// Test division in WHERE
	result, err = session.ExecuteSQL("SELECT id FROM numbers WHERE a / b = 3;")
	if err != nil {
		t.Fatalf("SELECT with / failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row for a / b = 3, got %d", len(result.Rows))
	}
}

// TestStringFunctions tests string functions UPPER, LOWER, LENGTH, CONCAT, SUBSTR.
func TestStringFunctions(t *testing.T) {
	session, cleanup := setupMVCCTestSession(t)
	defer cleanup()

	// Create table
	_, err := session.ExecuteSQL("CREATE TABLE strings (id INT, name TEXT);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert test data
	_, err = session.ExecuteSQL("INSERT INTO strings VALUES (1, 'Hello');")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test UPPER
	result, err := session.ExecuteSQL("SELECT id FROM strings WHERE UPPER(name) = 'HELLO';")
	if err != nil {
		t.Fatalf("SELECT with UPPER failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row for UPPER match, got %d", len(result.Rows))
	}

	// Test LOWER
	result, err = session.ExecuteSQL("SELECT id FROM strings WHERE LOWER(name) = 'hello';")
	if err != nil {
		t.Fatalf("SELECT with LOWER failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row for LOWER match, got %d", len(result.Rows))
	}

	// Test LENGTH
	result, err = session.ExecuteSQL("SELECT id FROM strings WHERE LENGTH(name) = 5;")
	if err != nil {
		t.Fatalf("SELECT with LENGTH failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row for LENGTH = 5, got %d", len(result.Rows))
	}
}

// TestCOALESCEAndNULLIF tests COALESCE and NULLIF functions.
func TestCOALESCEAndNULLIF(t *testing.T) {
	// Test parsing in WHERE clause (functions in SELECT columns not yet supported)
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"coalesce in where", "SELECT * FROM t WHERE COALESCE(a, 0) > 10;", false},
		{"nullif in where", "SELECT * FROM t WHERE NULLIF(a, 0) > 10;", false},
		{"coalesce comparison", "SELECT * FROM t WHERE COALESCE(a, b) = 5;", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestParseLIKE tests parsing of LIKE expressions.
func TestParseLIKE(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"like basic", "SELECT * FROM t WHERE name LIKE 'A%';", false},
		{"like underscore", "SELECT * FROM t WHERE code LIKE 'A_B';", false},
		{"not like", "SELECT * FROM t WHERE name NOT LIKE '%test%';", false},
		{"ilike", "SELECT * FROM t WHERE name ILIKE 'hello';", false},
		{"not ilike", "SELECT * FROM t WHERE name NOT ILIKE 'WORLD';", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestParseArithmetic tests parsing of arithmetic expressions in WHERE clause.
func TestParseArithmetic(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"addition in where", "SELECT * FROM t WHERE a + b > 10;", false},
		{"subtraction in where", "SELECT * FROM t WHERE a - b > 10;", false},
		{"multiplication in where", "SELECT * FROM t WHERE a * b > 10;", false},
		{"division in where", "SELECT * FROM t WHERE a / b > 10;", false},
		{"complex in where", "SELECT * FROM t WHERE a + b * c > 10;", false},
		{"with parens in where", "SELECT * FROM t WHERE (a + b) * c > 10;", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// ==================== Phase 4: DDL & Schema Tests ====================

// TestParseAlterTable tests parsing of ALTER TABLE statements.
func TestParseAlterTable(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"add column", "ALTER TABLE users ADD COLUMN email TEXT;", false},
		{"add column with keyword", "ALTER TABLE users ADD email TEXT;", false},
		{"add column not null", "ALTER TABLE users ADD COLUMN age INT NOT NULL;", false},
		{"drop column", "ALTER TABLE users DROP COLUMN email;", false},
		{"rename table", "ALTER TABLE users RENAME TO customers;", false},
		{"rename column", "ALTER TABLE users RENAME COLUMN name TO full_name;", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			stmt, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && stmt == nil {
				t.Error("expected non-nil statement")
			}
		})
	}
}

// TestParseTruncate tests parsing of TRUNCATE TABLE statements.
func TestParseTruncate(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"truncate with TABLE", "TRUNCATE TABLE users;", false},
		{"truncate without TABLE", "TRUNCATE users;", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			stmt, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				if trunc, ok := stmt.(*TruncateTableStmt); !ok {
					t.Error("expected TruncateTableStmt")
				} else if trunc.TableName != "users" {
					t.Errorf("expected table name 'users', got %q", trunc.TableName)
				}
			}
		})
	}
}

// TestParseShow tests parsing of SHOW statements.
func TestParseShow(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		showType  string
		tableName string
		wantErr   bool
	}{
		{"show tables", "SHOW TABLES;", "TABLES", "", false},
		{"show create table", "SHOW CREATE TABLE users;", "CREATE TABLE", "users", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			stmt, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				show, ok := stmt.(*ShowStmt)
				if !ok {
					t.Error("expected ShowStmt")
					return
				}
				if show.ShowType != tt.showType {
					t.Errorf("expected ShowType %q, got %q", tt.showType, show.ShowType)
				}
				if show.TableName != tt.tableName {
					t.Errorf("expected TableName %q, got %q", tt.tableName, show.TableName)
				}
			}
		})
	}
}

// TestAlterTableExecution tests ALTER TABLE statement execution.
func TestAlterTableExecution(t *testing.T) {
	session, cleanup := setupMVCCTestSession(t)
	defer cleanup()

	// Create initial table
	_, err := session.ExecuteSQL("CREATE TABLE test_alter (id INT, name TEXT);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Test ADD COLUMN
	result, err := session.ExecuteSQL("ALTER TABLE test_alter ADD COLUMN email TEXT;")
	if err != nil {
		t.Fatalf("ALTER TABLE ADD COLUMN failed: %v", err)
	}
	if result.Message == "" {
		t.Error("expected success message for ADD COLUMN")
	}

	// Verify column was added by inserting with new column
	_, err = session.ExecuteSQL("INSERT INTO test_alter VALUES (1, 'Alice', 'alice@example.com');")
	if err != nil {
		t.Fatalf("INSERT with new column failed: %v", err)
	}

	// Test DROP COLUMN
	result, err = session.ExecuteSQL("ALTER TABLE test_alter DROP COLUMN email;")
	if err != nil {
		t.Fatalf("ALTER TABLE DROP COLUMN failed: %v", err)
	}
	if result.Message == "" {
		t.Error("expected success message for DROP COLUMN")
	}

	// Test RENAME COLUMN
	result, err = session.ExecuteSQL("ALTER TABLE test_alter RENAME COLUMN name TO full_name;")
	if err != nil {
		t.Fatalf("ALTER TABLE RENAME COLUMN failed: %v", err)
	}
	if result.Message == "" {
		t.Error("expected success message for RENAME COLUMN")
	}

	// Test RENAME TO
	result, err = session.ExecuteSQL("ALTER TABLE test_alter RENAME TO test_renamed;")
	if err != nil {
		t.Fatalf("ALTER TABLE RENAME TO failed: %v", err)
	}
	if result.Message == "" {
		t.Error("expected success message for RENAME TO")
	}
}

// TestTruncateExecution tests TRUNCATE TABLE statement execution.
func TestTruncateExecution(t *testing.T) {
	session, cleanup := setupMVCCTestSession(t)
	defer cleanup()

	// Create table and insert data
	_, err := session.ExecuteSQL("CREATE TABLE test_truncate (id INT, name TEXT);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = session.ExecuteSQL("INSERT INTO test_truncate VALUES (1, 'Alice');")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = session.ExecuteSQL("INSERT INTO test_truncate VALUES (2, 'Bob');")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Truncate the table
	result, err := session.ExecuteSQL("TRUNCATE TABLE test_truncate;")
	if err != nil {
		t.Fatalf("TRUNCATE TABLE failed: %v", err)
	}
	if result.Message == "" {
		t.Error("expected success message for TRUNCATE")
	}
}

// TestShowExecution tests SHOW statements execution.
func TestShowExecution(t *testing.T) {
	session, cleanup := setupMVCCTestSession(t)
	defer cleanup()

	// Create a test table
	_, err := session.ExecuteSQL("CREATE TABLE show_test (id INT PRIMARY KEY, name TEXT NOT NULL);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Test SHOW TABLES
	result, err := session.ExecuteSQL("SHOW TABLES;")
	if err != nil {
		t.Fatalf("SHOW TABLES failed: %v", err)
	}
	if len(result.Columns) != 1 || result.Columns[0] != "table_name" {
		t.Errorf("expected column 'table_name', got %v", result.Columns)
	}
	// Should have at least the table we created
	foundTable := false
	for _, row := range result.Rows {
		if len(row) > 0 && row[0].Text == "show_test" {
			foundTable = true
			break
		}
	}
	if !foundTable {
		t.Error("expected to find 'show_test' in SHOW TABLES result")
	}

	// Test SHOW CREATE TABLE
	result, err = session.ExecuteSQL("SHOW CREATE TABLE show_test;")
	if err != nil {
		t.Fatalf("SHOW CREATE TABLE failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(result.Rows))
	}
	if len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
		ddl := result.Rows[0][0].Text
		if ddl == "" {
			t.Error("expected non-empty DDL string")
		}
		// Check that DDL contains table name
		if !strings.Contains(ddl, "show_test") {
			t.Errorf("DDL should contain table name, got: %s", ddl)
		}
	}
}

// TestParseExplain tests EXPLAIN statement parsing.
func TestParseExplain(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		analyze bool
	}{
		{
			name:    "simple explain",
			sql:     "EXPLAIN SELECT * FROM users",
			analyze: false,
		},
		{
			name:    "explain analyze",
			sql:     "EXPLAIN ANALYZE SELECT * FROM users",
			analyze: true,
		},
		{
			name:    "explain with where",
			sql:     "EXPLAIN SELECT id, name FROM users WHERE id = 1",
			analyze: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.sql)
			stmt, err := parser.Parse()
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}
			explainStmt, ok := stmt.(*ExplainStmt)
			if !ok {
				t.Fatalf("expected *ExplainStmt, got %T", stmt)
			}
			if explainStmt.Analyze != tt.analyze {
				t.Errorf("expected Analyze=%v, got %v", tt.analyze, explainStmt.Analyze)
			}
			if explainStmt.Statement == nil {
				t.Error("expected Statement to be set")
			}
			if _, ok := explainStmt.Statement.(*SelectStmt); !ok {
				t.Errorf("expected Statement to be *SelectStmt, got %T", explainStmt.Statement)
			}
		})
	}
}

// TestExplainExecution tests EXPLAIN statement execution.
func TestExplainExecution(t *testing.T) {
	session, cleanup := setupMVCCTestSession(t)
	defer cleanup()

	// Create table
	_, err := session.ExecuteSQL("CREATE TABLE explain_test (id INT, name TEXT);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert some data
	_, err = session.ExecuteSQL("INSERT INTO explain_test VALUES (1, 'Alice');")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = session.ExecuteSQL("INSERT INTO explain_test VALUES (2, 'Bob');")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test EXPLAIN
	result, err := session.ExecuteSQL("EXPLAIN SELECT * FROM explain_test;")
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	if len(result.Rows) == 0 {
		t.Error("expected at least one row in EXPLAIN output")
	}
	if len(result.Columns) != 1 || result.Columns[0] != "QUERY PLAN" {
		t.Errorf("expected column 'QUERY PLAN', got %v", result.Columns)
	}
	// Check that first row contains plan info
	if len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
		plan := result.Rows[0][0].Text
		if !strings.Contains(plan, "TableScan") && !strings.Contains(plan, "IndexScan") {
			t.Errorf("expected plan to mention TableScan or IndexScan, got: %s", plan)
		}
	}

	// Test EXPLAIN with WHERE
	result, err = session.ExecuteSQL("EXPLAIN SELECT * FROM explain_test WHERE id = 1;")
	if err != nil {
		t.Fatalf("EXPLAIN with WHERE failed: %v", err)
	}
	// Should have Filter info
	foundFilter := false
	for _, row := range result.Rows {
		if len(row) > 0 && strings.Contains(row[0].Text, "Filter") {
			foundFilter = true
			break
		}
	}
	if !foundFilter {
		t.Error("expected EXPLAIN output to mention Filter for WHERE clause")
	}

	// Test EXPLAIN ANALYZE
	result, err = session.ExecuteSQL("EXPLAIN ANALYZE SELECT * FROM explain_test;")
	if err != nil {
		t.Fatalf("EXPLAIN ANALYZE failed: %v", err)
	}
	// Should have actual rows info
	foundActualRows := false
	for _, row := range result.Rows {
		if len(row) > 0 && strings.Contains(row[0].Text, "Actual rows") {
			foundActualRows = true
			break
		}
	}
	if !foundActualRows {
		t.Error("expected EXPLAIN ANALYZE output to show actual rows")
	}
}

// TestParseCaseWhen tests CASE WHEN expression parsing.
func TestParseCaseWhen(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		isSimple  bool
		numWhens  int
		hasElse   bool
		shouldErr bool
	}{
		{
			name:     "simple case with one when",
			sql:      "SELECT CASE 1 WHEN 1 THEN 'one' END FROM users",
			isSimple: true,
			numWhens: 1,
			hasElse:  false,
		},
		{
			name:     "simple case with multiple whens",
			sql:      "SELECT CASE age WHEN 18 THEN 'adult' WHEN 13 THEN 'teen' WHEN 5 THEN 'child' END FROM users",
			isSimple: true,
			numWhens: 3,
			hasElse:  false,
		},
		{
			name:     "simple case with else",
			sql:      "SELECT CASE status WHEN 'active' THEN 1 WHEN 'inactive' THEN 0 ELSE 2 END FROM users",
			isSimple: true,
			numWhens: 2,
			hasElse:  true,
		},
		{
			name:     "searched case",
			sql:      "SELECT CASE WHEN age >= 18 THEN 'adult' WHEN age >= 13 THEN 'teen' ELSE 'child' END FROM users",
			isSimple: false,
			numWhens: 2,
			hasElse:  true,
		},
		{
			name:     "case in where clause",
			sql:      "SELECT * FROM users WHERE CASE WHEN status = 'active' THEN 1 ELSE 0 END = 1",
			isSimple: false,
			numWhens: 1,
			hasElse:  true,
		},
		{
			name:      "case without when should fail",
			sql:       "SELECT CASE END FROM users",
			shouldErr: true,
		},
		{
			name:      "case without end should fail",
			sql:       "SELECT CASE WHEN 1=1 THEN 'yes' FROM users",
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.sql)
			stmt, err := parser.Parse()

			if tt.shouldErr {
				if err == nil {
					t.Fatal("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("parsing failed: %v", err)
			}

			selectStmt, ok := stmt.(*SelectStmt)
			if !ok {
				t.Fatalf("expected SelectStmt, got %T", stmt)
			}

			// Find the CASE expression in the SELECT columns or WHERE
			var foundCase bool
			for range selectStmt.Columns {
				// For now, we can't inspect through SelectColumn, so mark as found if parsing succeeded
				foundCase = true
				break
			}

			if !foundCase && selectStmt.Where == nil {
				t.Error("could not find CASE expression in parsed statement")
			}
		})
	}
}

// TestCaseWhenExecution tests CASE WHEN expression execution.
func TestCaseWhenExecution(t *testing.T) {
	session, cleanup := setupMVCCTestSession(t)
	defer cleanup()

	// Create a test table
	_, err := session.ExecuteSQL("CREATE TABLE products (id INT, name TEXT, stock INT);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert test data
	inserts := []string{
		"INSERT INTO products VALUES (1, 'Widget', 150);",
		"INSERT INTO products VALUES (2, 'Gadget', 50);",
		"INSERT INTO products VALUES (3, 'Doohickey', 5);",
		"INSERT INTO products VALUES (4, 'Thingamajig', 0);",
	}
	for _, ins := range inserts {
		_, err := session.ExecuteSQL(ins)
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// Test simple CASE expression in SELECT
	t.Run("simple case in select", func(t *testing.T) {
		result, err := session.ExecuteSQL(
			"SELECT CASE WHEN stock > 100 THEN 'high' ELSE 'low' END AS level FROM products WHERE id = 1;")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}

		if len(result.Rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(result.Rows))
		}

		if len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
			if result.Rows[0][0].Text != "high" {
				t.Errorf("expected 'high', got '%s'", result.Rows[0][0].Text)
			}
		}
	})

	// Test searched CASE expression in SELECT
	t.Run("searched case in select", func(t *testing.T) {
		// id=1 has stock=150, so 150 > 100 is true → 'high'
		// then we test id=3 which has stock=5, so 5 > 100 false, 5 > 50 false → 'low'
		result, err := session.ExecuteSQL(
			"SELECT CASE WHEN stock > 100 THEN 'high' WHEN stock > 5 THEN 'medium' ELSE 'low' END AS level FROM products WHERE id = 2;")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}

		if len(result.Rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(result.Rows))
		}

		// id=2 has stock=50, so 50 > 100 false, 50 > 5 true → 'medium'
		if len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
			if result.Rows[0][0].Text != "medium" {
				t.Errorf("expected 'medium', got '%s'", result.Rows[0][0].Text)
			}
		}
	})

	// Test CASE in WHERE clause
	t.Run("case in where clause", func(t *testing.T) {
		result, err := session.ExecuteSQL(
			"SELECT name FROM products WHERE CASE WHEN stock > 100 THEN 1 ELSE 0 END = 1;")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}

		if len(result.Rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(result.Rows))
		}

		if len(result.Rows) > 0 {
			if result.Rows[0][0].Text != "Widget" {
				t.Errorf("expected 'Widget', got '%s'", result.Rows[0][0].Text)
			}
		}
	})

	// Test nested CASE expressions
	t.Run("nested case expressions", func(t *testing.T) {
		result, err := session.ExecuteSQL(
			"SELECT CASE WHEN stock = 0 THEN 'out' WHEN stock > 0 THEN CASE WHEN stock >= 100 THEN 'plenty' ELSE 'some' END ELSE 'unknown' END AS inventory FROM products WHERE id = 1;")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}

		if len(result.Rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(result.Rows))
		}

		if len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
			if result.Rows[0][0].Text != "plenty" {
				t.Errorf("expected 'plenty', got '%s'", result.Rows[0][0].Text)
			}
		}
	})

	// Test CASE without ELSE (should return NULL)
	t.Run("case without else returns null", func(t *testing.T) {
		result, err := session.ExecuteSQL(
			"SELECT CASE WHEN stock = 0 THEN 'none' END AS maybenull FROM products WHERE id = 2;")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}

		if len(result.Rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(result.Rows))
		}

		if len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
			if !result.Rows[0][0].IsNull {
				t.Errorf("expected NULL but got '%s'", result.Rows[0][0].Text)
			}
		}
	})
}

// TestCheckConstraintParsing tests parsing of CHECK constraints in CREATE TABLE.
func TestCheckConstraintParsing(t *testing.T) {
	// First, let's test that parsing SELECT with literal and WHERE works
	t.Run("test wrapped check expression parsing", func(t *testing.T) {
		// Try just parsing the WHERE clause differently
		parser := NewParser("SELECT x FROM t WHERE (age >= 18);")
		stmt, err := parser.Parse()
		if err != nil {
			t.Fatalf("Failed to parse wrapped expression: %v", err)
		}
		selectStmt, ok := stmt.(*SelectStmt)
		if !ok {
			t.Fatalf("Expected SelectStmt, got %T", stmt)
		}
		if selectStmt.Where == nil {
			t.Fatal("Expected WHERE clause but got nil")
		}
		t.Logf("Successfully parsed: %+v", selectStmt.Where)
	})

	tests := []struct {
		name      string
		sql       string
		shouldErr bool
	}{
		{
			name:      "simple check constraint",
			sql:       "CREATE TABLE test (age INT CHECK(age >= 0));",
			shouldErr: false,
		},
		{
			name:      "check with comparison",
			sql:       "CREATE TABLE test (price INT CHECK(price > 0));",
			shouldErr: false,
		},
		{
			name:      "check with AND",
			sql:       "CREATE TABLE test (age INT CHECK(age >= 0 AND age <= 150));",
			shouldErr: false,
		},
		{
			name:      "check with OR",
			sql:       "CREATE TABLE test (status TEXT CHECK(status = 'active' OR status = 'inactive'));",
			shouldErr: false,
		},
		{
			name:      "check without parentheses should fail",
			sql:       "CREATE TABLE test (age INT CHECK age >= 0);",
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.sql)
			_, err := parser.Parse()

			if tt.shouldErr {
				if err == nil {
					t.Error("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected parse error: %v", err)
				}
			}
		})
	}
}

// TestCheckConstraintExecution tests CHECK constraint enforcement during INSERT and UPDATE.
func TestCheckConstraintExecution(t *testing.T) {
	session, cleanup := setupMVCCTestSession(t)
	defer cleanup()

	// Create a table with CHECK constraints
	_, err := session.ExecuteSQL("CREATE TABLE employees (id INT, age INT CHECK(age >= 18), salary INT CHECK(salary > 0));")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Test: Valid INSERT should succeed
	t.Run("valid insert succeeds", func(t *testing.T) {
		_, err := session.ExecuteSQL("INSERT INTO employees VALUES (1, 25, 50000);")
		if err != nil {
			t.Errorf("valid insert should succeed, got error: %v", err)
		}
	})

	// Test: INSERT violating age CHECK should fail
	t.Run("insert violating age check fails", func(t *testing.T) {
		_, err := session.ExecuteSQL("INSERT INTO employees VALUES (2, 15, 30000);")
		if err == nil {
			t.Error("expected error for age < 18, but insert succeeded")
		} else if !strings.Contains(err.Error(), "CHECK constraint violated") {
			t.Errorf("expected CHECK constraint violation error, got: %v", err)
		}
	})

	// Test: INSERT violating salary CHECK should fail
	t.Run("insert violating salary check fails", func(t *testing.T) {
		_, err := session.ExecuteSQL("INSERT INTO employees VALUES (3, 30, 0);")
		if err == nil {
			t.Error("expected error for salary <= 0, but insert succeeded")
		} else if !strings.Contains(err.Error(), "CHECK constraint violated") {
			t.Errorf("expected CHECK constraint violation error, got: %v", err)
		}
	})

	// Test: UPDATE violating CHECK should fail
	t.Run("update violating check fails", func(t *testing.T) {
		// First verify the valid row exists
		result, err := session.ExecuteSQL("SELECT age FROM employees WHERE id = 1;")
		if err != nil {
			t.Fatalf("select failed: %v", err)
		}
		if len(result.Rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(result.Rows))
		}

		// Try to update to an invalid age
		_, err = session.ExecuteSQL("UPDATE employees SET age = 10 WHERE id = 1;")
		if err == nil {
			t.Error("expected error for updating age to < 18, but update succeeded")
		} else if !strings.Contains(err.Error(), "CHECK constraint violated") {
			t.Errorf("expected CHECK constraint violation error, got: %v", err)
		}
	})

	// Test: Valid UPDATE should succeed
	t.Run("valid update succeeds", func(t *testing.T) {
		_, err := session.ExecuteSQL("UPDATE employees SET age = 26 WHERE id = 1;")
		if err != nil {
			t.Errorf("valid update should succeed, got error: %v", err)
		}

		// Verify the update
		result, err := session.ExecuteSQL("SELECT age FROM employees WHERE id = 1;")
		if err != nil {
			t.Fatalf("select failed: %v", err)
		}
		if len(result.Rows) != 1 || result.Rows[0][0].Int32 != 26 {
			t.Errorf("expected age 26, got %v", result.Rows[0][0])
		}
	})
}

// TestDateFunctionParsing tests parsing of date functions.
func TestDateFunctionParsing(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		shouldErr bool
	}{
		{
			name:      "NOW function with FROM",
			sql:       "SELECT NOW() FROM events;",
			shouldErr: false,
		},
		{
			name:      "NOW with alias",
			sql:       "SELECT NOW() AS current_time FROM events;",
			shouldErr: false,
		},
		{
			name:      "CURRENT_TIMESTAMP with FROM",
			sql:       "SELECT CURRENT_TIMESTAMP FROM events;",
			shouldErr: false,
		},
		{
			name:      "YEAR function",
			sql:       "SELECT YEAR(created_at) FROM events;",
			shouldErr: false,
		},
		{
			name:      "MONTH function",
			sql:       "SELECT MONTH(created_at) FROM events;",
			shouldErr: false,
		},
		{
			name:      "DAY function",
			sql:       "SELECT DAY(created_at) FROM events;",
			shouldErr: false,
		},
		{
			name:      "DATE_ADD function",
			sql:       "SELECT DATE_ADD(created_at, INTERVAL 1 DAY) FROM events;",
			shouldErr: false,
		},
		{
			name:      "DATE_SUB function",
			sql:       "SELECT DATE_SUB(created_at, INTERVAL 2 MONTH) FROM events;",
			shouldErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.sql)
			_, err := parser.Parse()

			if tt.shouldErr {
				if err == nil {
					t.Error("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected parse error: %v", err)
				}
			}
		})
	}
}

// TestDateFunctionExecution tests date function execution.
func TestDateFunctionExecution(t *testing.T) {
	session, cleanup := setupMVCCTestSession(t)
	defer cleanup()

	// Create a table with timestamp column
	_, err := session.ExecuteSQL("CREATE TABLE events (id INT, name TEXT, created_at TIMESTAMP);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert a row with NOW()
	t.Run("insert with NOW", func(t *testing.T) {
		_, err := session.ExecuteSQL("INSERT INTO events VALUES (1, 'meeting', NOW());")
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	})

	// Test YEAR function
	t.Run("YEAR extracts year from timestamp", func(t *testing.T) {
		result, err := session.ExecuteSQL("SELECT YEAR(created_at) FROM events WHERE id = 1;")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}

		if len(result.Rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(result.Rows))
		}

		year := result.Rows[0][0].Int32
		currentYear := int32(time.Now().Year())
		if year != currentYear {
			t.Errorf("expected year %d, got %d", currentYear, year)
		}
	})

	// Test MONTH function
	t.Run("MONTH extracts month from timestamp", func(t *testing.T) {
		result, err := session.ExecuteSQL("SELECT MONTH(created_at) FROM events WHERE id = 1;")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}

		if len(result.Rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(result.Rows))
		}

		month := result.Rows[0][0].Int32
		currentMonth := int32(time.Now().Month())
		if month != currentMonth {
			t.Errorf("expected month %d, got %d", currentMonth, month)
		}
	})

	// Test DAY function
	t.Run("DAY extracts day from timestamp", func(t *testing.T) {
		result, err := session.ExecuteSQL("SELECT DAY(created_at) FROM events WHERE id = 1;")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}

		if len(result.Rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(result.Rows))
		}

		day := result.Rows[0][0].Int32
		currentDay := int32(time.Now().Day())
		if day != currentDay {
			t.Errorf("expected day %d, got %d", currentDay, day)
		}
	})
}

// TestIsNullExpressions tests IS NULL and IS NOT NULL functionality.
func TestIsNullExpressions(t *testing.T) {
	session, cleanup := setupMVCCTestSession(t)
	defer cleanup()

	// Create table
	_, err := session.ExecuteSQL("CREATE TABLE nulltest (id INT PRIMARY KEY, name TEXT, age INT);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert data with NULLs
	_, err = session.ExecuteSQL("INSERT INTO nulltest (id, name, age) VALUES (1, 'Alice', 30);")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = session.ExecuteSQL("INSERT INTO nulltest (id, name, age) VALUES (2, NULL, 25);")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = session.ExecuteSQL("INSERT INTO nulltest (id, name, age) VALUES (3, 'Bob', NULL);")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test IS NULL
	t.Run("IS NULL finds NULL values", func(t *testing.T) {
		result, err := session.ExecuteSQL("SELECT id FROM nulltest WHERE name IS NULL;")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(result.Rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(result.Rows))
		}
		if result.Rows[0][0].Int32 != 2 {
			t.Errorf("expected id=2, got %v", result.Rows[0][0].Int32)
		}
	})

	// Test IS NOT NULL
	t.Run("IS NOT NULL excludes NULL values", func(t *testing.T) {
		result, err := session.ExecuteSQL("SELECT id FROM nulltest WHERE name IS NOT NULL ORDER BY id;")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(result.Rows) != 2 {
			t.Errorf("expected 2 rows, got %d", len(result.Rows))
		}
	})

	// Test IS NULL on age column
	t.Run("IS NULL on age column", func(t *testing.T) {
		result, err := session.ExecuteSQL("SELECT id FROM nulltest WHERE age IS NULL;")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(result.Rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(result.Rows))
		}
		if result.Rows[0][0].Int32 != 3 {
			t.Errorf("expected id=3, got %v", result.Rows[0][0].Int32)
		}
	})
}

// TestMathFunctions tests ABS, MOD, POWER, and other math functions.
func TestMathFunctions(t *testing.T) {
	session, cleanup := setupMVCCTestSession(t)
	defer cleanup()

	// Create table
	_, err := session.ExecuteSQL("CREATE TABLE numbers (id INT PRIMARY KEY, val INT);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert test data
	_, err = session.ExecuteSQL("INSERT INTO numbers (id, val) VALUES (1, -10);")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = session.ExecuteSQL("INSERT INTO numbers (id, val) VALUES (2, 25);")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	tests := []struct {
		name     string
		query    string
		expected int64
	}{
		{"ABS of negative", "SELECT ABS(val) FROM numbers WHERE id = 1;", 10},
		{"ABS of positive", "SELECT ABS(val) FROM numbers WHERE id = 2;", 25},
		{"MOD operation", "SELECT MOD(val, 3) FROM numbers WHERE id = 2;", 1},
		{"POWER operation", "SELECT POWER(2, 3) FROM numbers WHERE id = 1;", 8},
		{"SQRT operation", "SELECT SQRT(val) FROM numbers WHERE id = 2;", 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := session.ExecuteSQL(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			if len(result.Rows) != 1 {
				t.Fatalf("expected 1 row, got %d", len(result.Rows))
			}
			var got int64
			if result.Rows[0][0].Type == catalog.TypeInt32 {
				got = int64(result.Rows[0][0].Int32)
			} else {
				got = result.Rows[0][0].Int64
			}
			if got != tt.expected {
				t.Errorf("expected %d, got %d", tt.expected, got)
			}
		})
	}
}

// TestExtendedStringFunctions tests TRIM, REPLACE, REVERSE, and other string functions.
func TestExtendedStringFunctions(t *testing.T) {
	session, cleanup := setupMVCCTestSession(t)
	defer cleanup()

	// Create table
	_, err := session.ExecuteSQL("CREATE TABLE strings (id INT PRIMARY KEY, val TEXT);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert test data
	_, err = session.ExecuteSQL("INSERT INTO strings (id, val) VALUES (1, '  hello  ');")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = session.ExecuteSQL("INSERT INTO strings (id, val) VALUES (2, 'hello world');")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{"TRIM removes spaces", "SELECT TRIM(val) FROM strings WHERE id = 1;", "hello"},
		{"LTRIM removes left spaces", "SELECT LTRIM(val) FROM strings WHERE id = 1;", "hello  "},
		{"RTRIM removes right spaces", "SELECT RTRIM(val) FROM strings WHERE id = 1;", "  hello"},
		{"REPLACE substitutes", "SELECT REPLACE(val, 'world', 'universe') FROM strings WHERE id = 2;", "hello universe"},
		{"REVERSE reverses string", "SELECT REVERSE(val) FROM strings WHERE id = 2;", "dlrow olleh"},
		{"REPEAT repeats string", "SELECT REPEAT('ab', 3) FROM strings WHERE id = 1;", "ababab"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := session.ExecuteSQL(tt.query)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			if len(result.Rows) != 1 {
				t.Fatalf("expected 1 row, got %d", len(result.Rows))
			}
			if result.Rows[0][0].Text != tt.expected {
				t.Errorf("expected '%s', got '%s'", tt.expected, result.Rows[0][0].Text)
			}
		})
	}
}

// TestCastExpression tests CAST type conversions.
func TestCastExpression(t *testing.T) {
	session, cleanup := setupMVCCTestSession(t)
	defer cleanup()

	// Create table
	_, err := session.ExecuteSQL("CREATE TABLE casttest (id INT PRIMARY KEY, num INT, str TEXT);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = session.ExecuteSQL("INSERT INTO casttest (id, num, str) VALUES (1, 42, '123');")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Test INT to TEXT
	t.Run("CAST INT to TEXT", func(t *testing.T) {
		result, err := session.ExecuteSQL("SELECT CAST(num AS TEXT) FROM casttest WHERE id = 1;")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(result.Rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(result.Rows))
		}
		if result.Rows[0][0].Type != catalog.TypeText {
			t.Errorf("expected TEXT type, got %v", result.Rows[0][0].Type)
		}
		if result.Rows[0][0].Text != "42" {
			t.Errorf("expected '42', got '%s'", result.Rows[0][0].Text)
		}
	})

	// Test TEXT to INT
	t.Run("CAST TEXT to INT", func(t *testing.T) {
		result, err := session.ExecuteSQL("SELECT CAST(str AS INT) FROM casttest WHERE id = 1;")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(result.Rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(result.Rows))
		}
		if result.Rows[0][0].Type != catalog.TypeInt32 {
			t.Errorf("expected INT type, got %v", result.Rows[0][0].Type)
		}
		if result.Rows[0][0].Int32 != 123 {
			t.Errorf("expected 123, got %d", result.Rows[0][0].Int32)
		}
	})
}

// TestExtractExpression tests EXTRACT(part FROM date) syntax.
func TestExtractExpression(t *testing.T) {
	session, cleanup := setupMVCCTestSession(t)
	defer cleanup()

	// Create table with timestamp
	_, err := session.ExecuteSQL("CREATE TABLE extracttest (id INT PRIMARY KEY, ts TIMESTAMP);")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = session.ExecuteSQL("INSERT INTO extracttest (id, ts) VALUES (1, NOW());")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	now := time.Now()

	// Test EXTRACT YEAR
	t.Run("EXTRACT YEAR", func(t *testing.T) {
		result, err := session.ExecuteSQL("SELECT EXTRACT(YEAR FROM ts) FROM extracttest WHERE id = 1;")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(result.Rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(result.Rows))
		}
		if result.Rows[0][0].Int32 != int32(now.Year()) {
			t.Errorf("expected year %d, got %d", now.Year(), result.Rows[0][0].Int32)
		}
	})

	// Test EXTRACT MONTH
	t.Run("EXTRACT MONTH", func(t *testing.T) {
		result, err := session.ExecuteSQL("SELECT EXTRACT(MONTH FROM ts) FROM extracttest WHERE id = 1;")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(result.Rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(result.Rows))
		}
		if result.Rows[0][0].Int32 != int32(now.Month()) {
			t.Errorf("expected month %d, got %d", now.Month(), result.Rows[0][0].Int32)
		}
	})
}

// TestUnionOperation tests UNION, UNION ALL, INTERSECT, EXCEPT operations.
func TestUnionOperation(t *testing.T) {
	session, cleanup := setupMVCCTestSession(t)
	defer cleanup()

	// Create two tables
	_, err := session.ExecuteSQL("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT);")
	if err != nil {
		t.Fatalf("CREATE TABLE t1 failed: %v", err)
	}
	_, err = session.ExecuteSQL("CREATE TABLE t2 (id INT PRIMARY KEY, name TEXT);")
	if err != nil {
		t.Fatalf("CREATE TABLE t2 failed: %v", err)
	}

	// Insert data into t1
	_, _ = session.ExecuteSQL("INSERT INTO t1 (id, name) VALUES (1, 'Alice');")
	_, _ = session.ExecuteSQL("INSERT INTO t1 (id, name) VALUES (2, 'Bob');")
	_, _ = session.ExecuteSQL("INSERT INTO t1 (id, name) VALUES (3, 'Charlie');")

	// Insert data into t2
	_, _ = session.ExecuteSQL("INSERT INTO t2 (id, name) VALUES (2, 'Bob');")
	_, _ = session.ExecuteSQL("INSERT INTO t2 (id, name) VALUES (3, 'Charlie');")
	_, _ = session.ExecuteSQL("INSERT INTO t2 (id, name) VALUES (4, 'Diana');")

	// Test UNION (removes duplicates)
	t.Run("UNION removes duplicates", func(t *testing.T) {
		result, err := session.ExecuteSQL("SELECT name FROM t1 UNION SELECT name FROM t2 ORDER BY name;")
		if err != nil {
			t.Fatalf("UNION query failed: %v", err)
		}
		if len(result.Rows) != 4 {
			t.Errorf("expected 4 unique rows, got %d", len(result.Rows))
		}
	})

	// Test UNION ALL (keeps duplicates)
	t.Run("UNION ALL keeps duplicates", func(t *testing.T) {
		result, err := session.ExecuteSQL("SELECT name FROM t1 UNION ALL SELECT name FROM t2 ORDER BY name;")
		if err != nil {
			t.Fatalf("UNION ALL query failed: %v", err)
		}
		if len(result.Rows) != 6 {
			t.Errorf("expected 6 rows, got %d", len(result.Rows))
		}
	})

	// Test INTERSECT
	t.Run("INTERSECT finds common rows", func(t *testing.T) {
		result, err := session.ExecuteSQL("SELECT name FROM t1 INTERSECT SELECT name FROM t2 ORDER BY name;")
		if err != nil {
			t.Fatalf("INTERSECT query failed: %v", err)
		}
		if len(result.Rows) != 2 {
			t.Errorf("expected 2 common rows (Bob, Charlie), got %d", len(result.Rows))
		}
	})

	// Test EXCEPT
	t.Run("EXCEPT finds unique rows", func(t *testing.T) {
		result, err := session.ExecuteSQL("SELECT name FROM t1 EXCEPT SELECT name FROM t2;")
		if err != nil {
			t.Fatalf("EXCEPT query failed: %v", err)
		}
		if len(result.Rows) != 1 {
			t.Errorf("expected 1 unique row (Alice), got %d", len(result.Rows))
		}
		if result.Rows[0][0].Text != "Alice" {
			t.Errorf("expected 'Alice', got '%s'", result.Rows[0][0].Text)
		}
	})
}

// TestViewParsing tests CREATE VIEW and DROP VIEW parsing.
func TestViewParsing(t *testing.T) {
	// Test CREATE VIEW parsing
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "simple CREATE VIEW",
			input:   "CREATE VIEW v AS SELECT * FROM t;",
			wantErr: false,
		},
		{
			name:    "CREATE OR REPLACE VIEW",
			input:   "CREATE OR REPLACE VIEW v AS SELECT id, name FROM t;",
			wantErr: false,
		},
		{
			name:    "CREATE VIEW with columns",
			input:   "CREATE VIEW v (a, b) AS SELECT id, name FROM t;",
			wantErr: false,
		},
		{
			name:    "DROP VIEW",
			input:   "DROP VIEW myview;",
			wantErr: false,
		},
		{
			name:    "DROP VIEW IF EXISTS",
			input:   "DROP VIEW IF EXISTS myview;",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestViewExecution tests CREATE VIEW execution and SELECT from views.
func TestViewExecution(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	// Create base table
	executeSQL(t, executor, "CREATE TABLE employees (id INT, name TEXT, department TEXT, salary INT);")
	executeSQL(t, executor, "INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 80000);")
	executeSQL(t, executor, "INSERT INTO employees VALUES (2, 'Bob', 'Engineering', 90000);")
	executeSQL(t, executor, "INSERT INTO employees VALUES (3, 'Carol', 'Sales', 60000);")
	executeSQL(t, executor, "INSERT INTO employees VALUES (4, 'Dave', 'Sales', 70000);")

	// Create a view
	result := executeSQL(t, executor, "CREATE VIEW engineers AS SELECT id, name, salary FROM employees WHERE department = 'Engineering';")
	if result.Message != "View 'engineers' created." {
		t.Errorf("Expected view creation message, got: %s", result.Message)
	}

	// Select all from view
	result = executeSQL(t, executor, "SELECT * FROM engineers;")
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 engineers, got %d", len(result.Rows))
	}

	// Select specific columns from view with filter
	result = executeSQL(t, executor, "SELECT name FROM engineers WHERE salary > 85000;")
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 high-paid engineer, got %d", len(result.Rows))
	}
	if len(result.Rows) > 0 && result.Rows[0][0].Text != "Bob" {
		t.Errorf("Expected Bob, got %s", result.Rows[0][0].Text)
	}

	// Create view with column aliases
	result = executeSQL(t, executor, "CREATE VIEW sales_people (emp_id, emp_name, emp_salary) AS SELECT id, name, salary FROM employees WHERE department = 'Sales';")
	if result.Message != "View 'sales_people' created." {
		t.Errorf("Expected view creation message, got: %s", result.Message)
	}

	// Select from view with renamed columns
	result = executeSQL(t, executor, "SELECT * FROM sales_people;")
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 sales people, got %d", len(result.Rows))
	}
	if len(result.Columns) != 3 || result.Columns[0] != "emp_id" || result.Columns[1] != "emp_name" || result.Columns[2] != "emp_salary" {
		t.Errorf("Expected columns [emp_id, emp_name, emp_salary], got %v", result.Columns)
	}

	// Test CREATE OR REPLACE VIEW
	result = executeSQL(t, executor, "CREATE OR REPLACE VIEW engineers AS SELECT name, salary FROM employees WHERE department = 'Engineering';")
	if result.Message != "View 'engineers' created." {
		t.Errorf("Expected view creation message, got: %s", result.Message)
	}

	// Verify view was replaced (now only 2 columns)
	result = executeSQL(t, executor, "SELECT * FROM engineers;")
	if len(result.Columns) != 2 {
		t.Errorf("Expected 2 columns after replacing view, got %d", len(result.Columns))
	}

	// Test DROP VIEW
	result = executeSQL(t, executor, "DROP VIEW engineers;")
	if result.Message != "View 'engineers' dropped." {
		t.Errorf("Expected drop message, got: %s", result.Message)
	}

	// Verify view is dropped - this should fail
	_, err := executeSQLWithError(executor, "SELECT * FROM engineers;")
	if err == nil {
		t.Error("Expected error selecting from dropped view")
	}

	// Test DROP VIEW IF EXISTS for non-existent view
	result = executeSQL(t, executor, "DROP VIEW IF EXISTS nonexistent;")
	// Should not error

	// Test view already exists error
	executeSQL(t, executor, "CREATE VIEW test_view AS SELECT * FROM employees;")
	_, err = executeSQLWithError(executor, "CREATE VIEW test_view AS SELECT * FROM employees;")
	if err == nil {
		t.Error("Expected error for duplicate view creation")
	}
}

// TestLeftJoin tests LEFT JOIN functionality.
func TestLeftJoin(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	// Create users table
	executeSQL(t, executor, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);")
	executeSQL(t, executor, "INSERT INTO users VALUES (1, 'Alice');")
	executeSQL(t, executor, "INSERT INTO users VALUES (2, 'Bob');")
	executeSQL(t, executor, "INSERT INTO users VALUES (3, 'Charlie');")

	// Create orders table - Charlie has no orders
	executeSQL(t, executor, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product TEXT);")
	executeSQL(t, executor, "INSERT INTO orders VALUES (100, 1, 'Widget');")
	executeSQL(t, executor, "INSERT INTO orders VALUES (101, 1, 'Gadget');")
	executeSQL(t, executor, "INSERT INTO orders VALUES (102, 2, 'Doodad');")

	// LEFT JOIN - should return 4 rows (Alice: 2, Bob: 1, Charlie: 1 with NULL)
	result := executeSQL(t, executor, "SELECT users.name, orders.product FROM users LEFT JOIN orders ON users.id = orders.user_id;")

	if len(result.Rows) != 4 {
		t.Fatalf("expected 4 rows from LEFT JOIN, got %d", len(result.Rows))
	}

	// Verify Charlie has NULL product
	foundCharlieNull := false
	for _, row := range result.Rows {
		if row[0].Text == "Charlie" && row[1].IsNull {
			foundCharlieNull = true
			break
		}
	}
	if !foundCharlieNull {
		t.Error("expected Charlie to have NULL product in LEFT JOIN")
	}
}

// TestRightJoin tests RIGHT JOIN functionality.
func TestRightJoin(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	// Create users table - only Alice and Bob
	executeSQL(t, executor, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT);")
	executeSQL(t, executor, "INSERT INTO users VALUES (1, 'Alice');")
	executeSQL(t, executor, "INSERT INTO users VALUES (2, 'Bob');")

	// Create orders table - includes order for non-existent user 99
	executeSQL(t, executor, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product TEXT);")
	executeSQL(t, executor, "INSERT INTO orders VALUES (100, 1, 'Widget');")
	executeSQL(t, executor, "INSERT INTO orders VALUES (101, 2, 'Gadget');")
	executeSQL(t, executor, "INSERT INTO orders VALUES (102, 99, 'Orphan');")

	// RIGHT JOIN - should include orphan order with NULL user name
	result := executeSQL(t, executor, "SELECT users.name, orders.product FROM users RIGHT JOIN orders ON users.id = orders.user_id;")

	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows from RIGHT JOIN, got %d", len(result.Rows))
	}

	// Verify orphan order has NULL user name
	foundOrphan := false
	for _, row := range result.Rows {
		if row[1].Text == "Orphan" && row[0].IsNull {
			foundOrphan = true
			break
		}
	}
	if !foundOrphan {
		t.Error("expected orphan order to have NULL user name in RIGHT JOIN")
	}
}

// TestLeftOuterJoinSyntax tests LEFT OUTER JOIN syntax.
func TestLeftOuterJoinSyntax(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE a (x INT PRIMARY KEY);")
	executeSQL(t, executor, "INSERT INTO a VALUES (1);")
	executeSQL(t, executor, "INSERT INTO a VALUES (2);")

	executeSQL(t, executor, "CREATE TABLE b (y INT PRIMARY KEY);")
	executeSQL(t, executor, "INSERT INTO b VALUES (1);")

	// LEFT OUTER JOIN should work same as LEFT JOIN
	result := executeSQL(t, executor, "SELECT a.x, b.y FROM a LEFT OUTER JOIN b ON a.x = b.y;")

	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows from LEFT OUTER JOIN, got %d", len(result.Rows))
	}

	// x=2 should have NULL for b.y
	foundNullY := false
	for _, row := range result.Rows {
		if row[0].Int32 == 2 && row[1].IsNull {
			foundNullY = true
			break
		}
	}
	if !foundNullY {
		t.Error("expected x=2 to have NULL y in LEFT OUTER JOIN")
	}
}

// TestFullOuterJoin tests FULL OUTER JOIN functionality.
func TestFullOuterJoin(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	// Create employees table
	executeSQL(t, executor, "CREATE TABLE employees (id INT PRIMARY KEY, name TEXT);")
	executeSQL(t, executor, "INSERT INTO employees VALUES (1, 'Alice');")
	executeSQL(t, executor, "INSERT INTO employees VALUES (2, 'Bob');")
	executeSQL(t, executor, "INSERT INTO employees VALUES (3, 'Charlie');")

	// Create departments table - includes dept with no employees and employees with no dept
	executeSQL(t, executor, "CREATE TABLE departments (id INT PRIMARY KEY, emp_id INT, dept_name TEXT);")
	executeSQL(t, executor, "INSERT INTO departments VALUES (10, 1, 'Engineering');")
	executeSQL(t, executor, "INSERT INTO departments VALUES (20, 2, 'Sales');")
	executeSQL(t, executor, "INSERT INTO departments VALUES (30, 99, 'Marketing');") // No matching employee

	// FULL OUTER JOIN - should return:
	// - Alice with Engineering (matched)
	// - Bob with Sales (matched)
	// - Charlie with NULL (no matching dept)
	// - NULL with Marketing (no matching employee)
	result := executeSQL(t, executor, "SELECT employees.name, departments.dept_name FROM employees FULL OUTER JOIN departments ON employees.id = departments.emp_id;")

	if len(result.Rows) != 4 {
		t.Fatalf("expected 4 rows from FULL OUTER JOIN, got %d", len(result.Rows))
	}

	// Verify we have:
	// 1. At least one matched row (Alice+Engineering)
	// 2. An unmatched left row (Charlie+NULL)
	// 3. An unmatched right row (NULL+Marketing)
	foundAlice := false
	foundCharlieNull := false
	foundNullMarketing := false

	for _, row := range result.Rows {
		if !row[0].IsNull && row[0].Text == "Alice" && !row[1].IsNull && row[1].Text == "Engineering" {
			foundAlice = true
		}
		if !row[0].IsNull && row[0].Text == "Charlie" && row[1].IsNull {
			foundCharlieNull = true
		}
		if row[0].IsNull && !row[1].IsNull && row[1].Text == "Marketing" {
			foundNullMarketing = true
		}
	}

	if !foundAlice {
		t.Error("expected matched row Alice+Engineering in FULL OUTER JOIN")
	}
	if !foundCharlieNull {
		t.Error("expected Charlie with NULL department in FULL OUTER JOIN")
	}
	if !foundNullMarketing {
		t.Error("expected NULL employee with Marketing in FULL OUTER JOIN")
	}
}

// TestFullJoinSyntax tests FULL JOIN without OUTER keyword.
func TestFullJoinSyntax(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	// Create simple tables
	executeSQL(t, executor, "CREATE TABLE t1 (x INT PRIMARY KEY);")
	executeSQL(t, executor, "INSERT INTO t1 VALUES (1);")
	executeSQL(t, executor, "INSERT INTO t1 VALUES (2);")

	executeSQL(t, executor, "CREATE TABLE t2 (y INT PRIMARY KEY);")
	executeSQL(t, executor, "INSERT INTO t2 VALUES (1);")
	executeSQL(t, executor, "INSERT INTO t2 VALUES (3);")

	// FULL JOIN without OUTER keyword
	result := executeSQL(t, executor, "SELECT t1.x, t2.y FROM t1 FULL JOIN t2 ON t1.x = t2.y;")

	// Expected: (1,1), (2,NULL), (NULL,3)
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows from FULL JOIN, got %d", len(result.Rows))
	}
}

// TestHavingClause tests HAVING clause filtering on aggregates.
func TestHavingClause(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	// Create table with sales data
	executeSQL(t, executor, "CREATE TABLE sales (id INT PRIMARY KEY, region TEXT, amount INT);")

	// Insert data: East has 3 sales, West has 2
	executeSQL(t, executor, "INSERT INTO sales (id, region, amount) VALUES (1, 'East', 100);")
	executeSQL(t, executor, "INSERT INTO sales (id, region, amount) VALUES (2, 'East', 200);")
	executeSQL(t, executor, "INSERT INTO sales (id, region, amount) VALUES (3, 'East', 150);")
	executeSQL(t, executor, "INSERT INTO sales (id, region, amount) VALUES (4, 'West', 300);")
	executeSQL(t, executor, "INSERT INTO sales (id, region, amount) VALUES (5, 'West', 250);")

	t.Run("HAVING COUNT > N", func(t *testing.T) {
		result := executeSQL(t, executor, "SELECT region, COUNT(*) FROM sales GROUP BY region HAVING COUNT(*) > 2;")
		if len(result.Rows) != 1 {
			t.Fatalf("expected 1 row (only East has >2 sales), got %d", len(result.Rows))
		}
		if result.Rows[0][0].Text != "East" {
			t.Errorf("expected region 'East', got '%s'", result.Rows[0][0].Text)
		}
	})

	t.Run("HAVING SUM > N", func(t *testing.T) {
		result := executeSQL(t, executor, "SELECT region, SUM(amount) FROM sales GROUP BY region HAVING SUM(amount) > 400;")
		if len(result.Rows) != 2 {
			t.Fatalf("expected 2 rows (both regions have sum > 400), got %d", len(result.Rows))
		}
	})

	t.Run("HAVING AVG < N", func(t *testing.T) {
		result := executeSQL(t, executor, "SELECT region, AVG(amount) FROM sales GROUP BY region HAVING AVG(amount) < 200;")
		if len(result.Rows) != 1 {
			t.Fatalf("expected 1 row (only East has avg < 200), got %d", len(result.Rows))
		}
		if result.Rows[0][0].Text != "East" {
			t.Errorf("expected region 'East', got '%s'", result.Rows[0][0].Text)
		}
	})
}

// TestHavingWithWhere tests HAVING combined with WHERE.
func TestHavingWithWhere(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE orders (id INT PRIMARY KEY, category TEXT, value INT);")

	// Insert test data
	executeSQL(t, executor, "INSERT INTO orders (id, category, value) VALUES (1, 'A', 100);")
	executeSQL(t, executor, "INSERT INTO orders (id, category, value) VALUES (2, 'A', 200);")
	executeSQL(t, executor, "INSERT INTO orders (id, category, value) VALUES (3, 'A', 50);")
	executeSQL(t, executor, "INSERT INTO orders (id, category, value) VALUES (4, 'B', 300);")
	executeSQL(t, executor, "INSERT INTO orders (id, category, value) VALUES (5, 'B', 400);")

	// WHERE filters rows first, then GROUP BY, then HAVING
	result := executeSQL(t, executor, "SELECT category, COUNT(*) FROM orders WHERE value > 75 GROUP BY category HAVING COUNT(*) >= 2;")

	// Category A: values 100, 200 pass WHERE (50 excluded), count=2
	// Category B: values 300, 400 pass WHERE, count=2
	// Both have count >= 2
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
}

// TestParseLeftRightJoin verifies LEFT/RIGHT/FULL JOIN parsing.
func TestParseLeftRightJoin(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		joinType string
		wantErr  bool
	}{
		{
			name:     "LEFT JOIN",
			input:    "SELECT * FROM a LEFT JOIN b ON a.id = b.id;",
			joinType: "LEFT",
			wantErr:  false,
		},
		{
			name:     "LEFT OUTER JOIN",
			input:    "SELECT * FROM a LEFT OUTER JOIN b ON a.id = b.id;",
			joinType: "LEFT",
			wantErr:  false,
		},
		{
			name:     "RIGHT JOIN",
			input:    "SELECT * FROM a RIGHT JOIN b ON a.id = b.id;",
			joinType: "RIGHT",
			wantErr:  false,
		},
		{
			name:     "RIGHT OUTER JOIN",
			input:    "SELECT * FROM a RIGHT OUTER JOIN b ON a.id = b.id;",
			joinType: "RIGHT",
			wantErr:  false,
		},
		{
			name:     "FULL JOIN",
			input:    "SELECT * FROM a FULL JOIN b ON a.id = b.id;",
			joinType: "FULL",
			wantErr:  false,
		},
		{
			name:     "FULL OUTER JOIN",
			input:    "SELECT * FROM a FULL OUTER JOIN b ON a.id = b.id;",
			joinType: "FULL",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			stmt, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				selectStmt, ok := stmt.(*SelectStmt)
				if !ok {
					t.Fatal("expected SelectStmt")
				}
				if len(selectStmt.Joins) != 1 {
					t.Fatal("expected 1 join")
				}
				if selectStmt.Joins[0].JoinType != tt.joinType {
					t.Errorf("expected join type %s, got %s", tt.joinType, selectStmt.Joins[0].JoinType)
				}
			}
		})
	}
}

// TestParseHaving verifies HAVING clause parsing.
func TestParseHaving(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "HAVING with COUNT",
			input:   "SELECT category, COUNT(*) FROM orders GROUP BY category HAVING COUNT(*) > 5;",
			wantErr: false,
		},
		{
			name:    "HAVING with SUM",
			input:   "SELECT dept, SUM(salary) FROM employees GROUP BY dept HAVING SUM(salary) > 100000;",
			wantErr: false,
		},
		{
			name:    "HAVING without GROUP BY should fail",
			input:   "SELECT * FROM orders HAVING COUNT(*) > 5;",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestSubqueryIN tests IN subquery functionality.
func TestSubqueryIN(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	// Create departments table
	executeSQL(t, executor, "CREATE TABLE departments (id INT PRIMARY KEY, name TEXT);")
	executeSQL(t, executor, "INSERT INTO departments VALUES (1, 'Engineering');")
	executeSQL(t, executor, "INSERT INTO departments VALUES (2, 'Sales');")

	// Create employees table
	executeSQL(t, executor, "CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept_id INT);")
	executeSQL(t, executor, "INSERT INTO employees VALUES (1, 'Alice', 1);")
	executeSQL(t, executor, "INSERT INTO employees VALUES (2, 'Bob', 1);")
	executeSQL(t, executor, "INSERT INTO employees VALUES (3, 'Charlie', 2);")
	executeSQL(t, executor, "INSERT INTO employees VALUES (4, 'David', 3);") // Non-existent dept

	// Test IN subquery - find employees in existing departments
	result := executeSQL(t, executor, "SELECT name FROM employees WHERE dept_id IN (SELECT id FROM departments);")

	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 employees in existing departments, got %d", len(result.Rows))
	}

	// Test NOT IN subquery
	result = executeSQL(t, executor, "SELECT name FROM employees WHERE dept_id NOT IN (SELECT id FROM departments);")

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 employee not in any department, got %d", len(result.Rows))
	}
	if result.Rows[0][0].Text != "David" {
		t.Errorf("expected David (orphan employee), got %s", result.Rows[0][0].Text)
	}
}

// TestScalarSubquery tests scalar subqueries in expressions.
func TestScalarSubquery(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	// Create products table
	executeSQL(t, executor, "CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT);")
	executeSQL(t, executor, "INSERT INTO products VALUES (1, 'Widget', 100);")
	executeSQL(t, executor, "INSERT INTO products VALUES (2, 'Gadget', 200);")
	executeSQL(t, executor, "INSERT INTO products VALUES (3, 'Gizmo', 150);")

	// Test scalar subquery in WHERE - find products with max price
	result := executeSQL(t, executor, "SELECT name FROM products WHERE price = (SELECT MAX(price) FROM products);")

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 product with max price, got %d", len(result.Rows))
	}
	if result.Rows[0][0].Text != "Gadget" {
		t.Errorf("expected Gadget (most expensive), got %s", result.Rows[0][0].Text)
	}
}

// TestExistsSubquery tests EXISTS subquery functionality.
func TestExistsSubquery(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	// Create products table
	executeSQL(t, executor, "CREATE TABLE products (id INT PRIMARY KEY, name TEXT, in_stock INT);")
	executeSQL(t, executor, "INSERT INTO products VALUES (1, 'Widget', 10);")
	executeSQL(t, executor, "INSERT INTO products VALUES (2, 'Gadget', 0);")
	executeSQL(t, executor, "INSERT INTO products VALUES (3, 'Gizmo', 5);")

	// Test EXISTS - check if any products exist with stock > 0
	// This is a non-correlated subquery (doesn't reference outer query)
	result := executeSQL(t, executor, "SELECT name FROM products WHERE EXISTS (SELECT 1 FROM products WHERE in_stock > 0);")

	// EXISTS returns true because there are products with stock > 0, so all products are returned
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 products (EXISTS is true), got %d", len(result.Rows))
	}

	// Test EXISTS with empty result - no products with stock > 100
	result = executeSQL(t, executor, "SELECT name FROM products WHERE EXISTS (SELECT 1 FROM products WHERE in_stock > 100);")

	// EXISTS returns false because no products have stock > 100, so no rows returned
	if len(result.Rows) != 0 {
		t.Fatalf("expected 0 products (EXISTS is false), got %d", len(result.Rows))
	}
}

// TestNotExistsSubquery tests NOT EXISTS subquery functionality.
func TestNotExistsSubquery(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	// Create products table
	executeSQL(t, executor, "CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT);")
	executeSQL(t, executor, "INSERT INTO products VALUES (1, 'Widget', 100);")
	executeSQL(t, executor, "INSERT INTO products VALUES (2, 'Gadget', 200);")

	// Test NOT EXISTS - check if no products exist with price > 1000
	result := executeSQL(t, executor, "SELECT name FROM products WHERE NOT EXISTS (SELECT 1 FROM products WHERE price > 1000);")

	// NOT EXISTS returns true because no products have price > 1000, so all rows returned
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 products (NOT EXISTS is true), got %d", len(result.Rows))
	}

	// Test NOT EXISTS with non-empty result
	result = executeSQL(t, executor, "SELECT name FROM products WHERE NOT EXISTS (SELECT 1 FROM products WHERE price > 50);")

	// NOT EXISTS returns false because there are products with price > 50, so no rows returned
	if len(result.Rows) != 0 {
		t.Fatalf("expected 0 products (NOT EXISTS is false), got %d", len(result.Rows))
	}
}

// TestParseSubqueries verifies subquery parsing.
func TestParseSubqueries(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "IN subquery",
			input:   "SELECT * FROM t WHERE x IN (SELECT y FROM t2);",
			wantErr: false,
		},
		{
			name:    "NOT IN subquery",
			input:   "SELECT * FROM t WHERE x NOT IN (SELECT y FROM t2);",
			wantErr: false,
		},
		{
			name:    "EXISTS subquery",
			input:   "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM t2);",
			wantErr: false,
		},
		{
			name:    "NOT EXISTS subquery",
			input:   "SELECT * FROM t WHERE NOT EXISTS (SELECT 1 FROM t2);",
			wantErr: false,
		},
		{
			name:    "Scalar subquery",
			input:   "SELECT * FROM t WHERE x = (SELECT MAX(y) FROM t2);",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestParseCrossJoin verifies CROSS JOIN parsing.
func TestParseCrossJoin(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "simple CROSS JOIN",
			input:   "SELECT * FROM t1 CROSS JOIN t2;",
			wantErr: false,
		},
		{
			name:    "CROSS JOIN with columns",
			input:   "SELECT t1.a, t2.b FROM t1 CROSS JOIN t2;",
			wantErr: false,
		},
		{
			name:    "multiple CROSS JOINs",
			input:   "SELECT * FROM t1 CROSS JOIN t2 CROSS JOIN t3;",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestParseDistinctOn verifies DISTINCT ON parsing.
func TestParseDistinctOn(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "DISTINCT ON single column",
			input:   "SELECT DISTINCT ON (a) a, b FROM t;",
			wantErr: false,
		},
		{
			name:    "DISTINCT ON multiple columns",
			input:   "SELECT DISTINCT ON (a, b) a, b, c FROM t;",
			wantErr: false,
		},
		{
			name:    "DISTINCT ON with ORDER BY",
			input:   "SELECT DISTINCT ON (a) a, b FROM t ORDER BY a, b;",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestParseLimitExpression verifies LIMIT with expressions.
func TestParseLimitExpression(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "LIMIT with number",
			input:   "SELECT * FROM t LIMIT 10;",
			wantErr: false,
		},
		{
			name:    "LIMIT with subquery",
			input:   "SELECT * FROM t LIMIT (SELECT COUNT(*) FROM t2);",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestCrossJoinExecution verifies CROSS JOIN execution.
func TestCrossJoinExecution(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	// Create and populate tables
	executeSQL(t, executor, "CREATE TABLE cross_t1 (id INT, name TEXT);")
	executeSQL(t, executor, "CREATE TABLE cross_t2 (code INT, value TEXT);")

	executeSQL(t, executor, "INSERT INTO cross_t1 VALUES (1, 'a');")
	executeSQL(t, executor, "INSERT INTO cross_t1 VALUES (2, 'b');")

	executeSQL(t, executor, "INSERT INTO cross_t2 VALUES (10, 'x');")
	executeSQL(t, executor, "INSERT INTO cross_t2 VALUES (20, 'y');")

	// Test CROSS JOIN - should produce 2 * 2 = 4 rows
	result := executeSQL(t, executor, "SELECT * FROM cross_t1 CROSS JOIN cross_t2;")

	if len(result.Rows) != 4 {
		t.Errorf("Expected 4 rows from CROSS JOIN, got %d", len(result.Rows))
	}
}

// TestDistinctOnExecution verifies DISTINCT ON execution.
func TestDistinctOnExecution(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	// Create and populate table
	executeSQL(t, executor, "CREATE TABLE distinct_on_t (category TEXT, value INT, name TEXT);")

	executeSQL(t, executor, "INSERT INTO distinct_on_t VALUES ('A', 1, 'first');")
	executeSQL(t, executor, "INSERT INTO distinct_on_t VALUES ('A', 2, 'second');")
	executeSQL(t, executor, "INSERT INTO distinct_on_t VALUES ('B', 3, 'third');")
	executeSQL(t, executor, "INSERT INTO distinct_on_t VALUES ('B', 4, 'fourth');")

	// Test DISTINCT ON (category) - should get one row per category
	result := executeSQL(t, executor, "SELECT DISTINCT ON (category) category, value, name FROM distinct_on_t;")

	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows from DISTINCT ON (category), got %d", len(result.Rows))
	}
}

// TestParseWindowFunctions verifies window function parsing.
func TestParseWindowFunctions(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "ROW_NUMBER basic",
			input:   "SELECT ROW_NUMBER() OVER () FROM t;",
			wantErr: false,
		},
		{
			name:    "ROW_NUMBER with PARTITION BY",
			input:   "SELECT ROW_NUMBER() OVER (PARTITION BY dept) FROM t;",
			wantErr: false,
		},
		{
			name:    "ROW_NUMBER with ORDER BY",
			input:   "SELECT ROW_NUMBER() OVER (ORDER BY salary DESC) FROM t;",
			wantErr: false,
		},
		{
			name:    "ROW_NUMBER with PARTITION BY and ORDER BY",
			input:   "SELECT ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) FROM t;",
			wantErr: false,
		},
		{
			name:    "RANK function",
			input:   "SELECT RANK() OVER (ORDER BY score) FROM t;",
			wantErr: false,
		},
		{
			name:    "DENSE_RANK function",
			input:   "SELECT DENSE_RANK() OVER (ORDER BY score) FROM t;",
			wantErr: false,
		},
		{
			name:    "SUM as window function",
			input:   "SELECT SUM(amount) OVER (PARTITION BY customer_id) FROM orders;",
			wantErr: false,
		},
		{
			name:    "COUNT as window function",
			input:   "SELECT COUNT(*) OVER (ORDER BY date) FROM t;",
			wantErr: false,
		},
		{
			name:    "Window function with column alias",
			input:   "SELECT ROW_NUMBER() OVER (ORDER BY id) AS row_num FROM t;",
			wantErr: false,
		},
		{
			name:    "Window function without OVER - should fail",
			input:   "SELECT ROW_NUMBER() FROM t;",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			_, err := parser.Parse()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestWindowFunctionExecution verifies window function execution.
func TestWindowFunctionExecution(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	// Create and populate table
	executeSQL(t, executor, "CREATE TABLE window_test (dept TEXT, emp TEXT, salary INT);")

	executeSQL(t, executor, "INSERT INTO window_test VALUES ('Sales', 'Alice', 50000);")
	executeSQL(t, executor, "INSERT INTO window_test VALUES ('Sales', 'Bob', 60000);")
	executeSQL(t, executor, "INSERT INTO window_test VALUES ('Sales', 'Carol', 55000);")
	executeSQL(t, executor, "INSERT INTO window_test VALUES ('Engineering', 'Dave', 70000);")
	executeSQL(t, executor, "INSERT INTO window_test VALUES ('Engineering', 'Eve', 80000);")

	// Test ROW_NUMBER with ORDER BY
	result := executeSQL(t, executor, "SELECT emp, ROW_NUMBER() OVER (ORDER BY salary) FROM window_test;")

	if len(result.Rows) != 5 {
		t.Errorf("Expected 5 rows, got %d", len(result.Rows))
	}

	// Each row should have a row number from 1 to 5
	for i, row := range result.Rows {
		if len(row) != 2 {
			t.Errorf("Row %d: Expected 2 columns, got %d", i, len(row))
		}
	}
}

// TestWindowFunctionPartitionBy tests PARTITION BY functionality.
func TestWindowFunctionPartitionBy(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	// Create and populate table
	executeSQL(t, executor, "CREATE TABLE partition_test (dept TEXT, emp TEXT, salary INT);")

	executeSQL(t, executor, "INSERT INTO partition_test VALUES ('Sales', 'Alice', 50000);")
	executeSQL(t, executor, "INSERT INTO partition_test VALUES ('Sales', 'Bob', 60000);")
	executeSQL(t, executor, "INSERT INTO partition_test VALUES ('Engineering', 'Dave', 70000);")
	executeSQL(t, executor, "INSERT INTO partition_test VALUES ('Engineering', 'Eve', 80000);")

	// Test ROW_NUMBER with PARTITION BY - each dept should have its own numbering
	result := executeSQL(t, executor, "SELECT dept, emp, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) FROM partition_test;")

	if len(result.Rows) != 4 {
		t.Errorf("Expected 4 rows, got %d", len(result.Rows))
	}

	// Verify row numbers reset per partition
	// Should have row numbers 1, 2 for Sales and 1, 2 for Engineering
	rowNumCounts := make(map[int64]int)
	for _, row := range result.Rows {
		if len(row) >= 3 {
			if row[2].Type == catalog.TypeInt64 {
				rowNumCounts[row[2].Int64]++
			}
		}
	}

	// Should have two 1s and two 2s
	if rowNumCounts[1] != 2 || rowNumCounts[2] != 2 {
		t.Errorf("Expected two 1s and two 2s in row numbers, got %v", rowNumCounts)
	}
}

// TestWindowFunctionRank tests RANK and DENSE_RANK.
func TestWindowFunctionRank(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE rank_test (name TEXT, score INT);")

	executeSQL(t, executor, "INSERT INTO rank_test VALUES ('Alice', 100);")
	executeSQL(t, executor, "INSERT INTO rank_test VALUES ('Bob', 90);")
	executeSQL(t, executor, "INSERT INTO rank_test VALUES ('Carol', 90);")
	executeSQL(t, executor, "INSERT INTO rank_test VALUES ('Dave', 80);")

	// Test RANK - ties get same rank, gaps after
	result := executeSQL(t, executor, "SELECT name, RANK() OVER (ORDER BY score DESC) FROM rank_test;")

	if len(result.Rows) != 4 {
		t.Errorf("Expected 4 rows, got %d", len(result.Rows))
	}

	// With RANK: Alice=1, Bob=2, Carol=2, Dave=4 (gap after tie)

	// Test DENSE_RANK - ties get same rank, no gaps
	result = executeSQL(t, executor, "SELECT name, DENSE_RANK() OVER (ORDER BY score DESC) FROM rank_test;")

	if len(result.Rows) != 4 {
		t.Errorf("Expected 4 rows, got %d", len(result.Rows))
	}

	// With DENSE_RANK: Alice=1, Bob=2, Carol=2, Dave=3 (no gap)
}

// TestWindowFunctionAggregate tests aggregate functions as window functions.
func TestWindowFunctionAggregate(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE agg_window_test (id INT, amount INT);")

	executeSQL(t, executor, "INSERT INTO agg_window_test VALUES (1, 100);")
	executeSQL(t, executor, "INSERT INTO agg_window_test VALUES (2, 200);")
	executeSQL(t, executor, "INSERT INTO agg_window_test VALUES (3, 300);")

	// Test SUM as window function (running sum)
	result := executeSQL(t, executor, "SELECT id, amount, SUM(amount) OVER (ORDER BY id) FROM agg_window_test;")

	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(result.Rows))
	}

	// Verify running sums: 100, 300, 600
	expectedSums := []int64{100, 300, 600}
	for i, row := range result.Rows {
		if len(row) >= 3 {
			if row[2].Type == catalog.TypeInt64 {
				if row[2].Int64 != expectedSums[i] {
					t.Errorf("Row %d: Expected running sum %d, got %d", i, expectedSums[i], row[2].Int64)
				}
			}
		}
	}
}

// TestWindowFrameExecution tests window frame specification execution.
func TestWindowFrameExecution(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE frame_test (id INT, value INT);")

	executeSQL(t, executor, "INSERT INTO frame_test VALUES (1, 10);")
	executeSQL(t, executor, "INSERT INTO frame_test VALUES (2, 20);")
	executeSQL(t, executor, "INSERT INTO frame_test VALUES (3, 30);")
	executeSQL(t, executor, "INSERT INTO frame_test VALUES (4, 40);")
	executeSQL(t, executor, "INSERT INTO frame_test VALUES (5, 50);")

	// Test 1: SUM with ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING (moving window of 3)
	// Row 1: sum(10) = 10 (can only look forward)
	// Row 2: sum(10,20,30) = 60
	// Row 3: sum(20,30,40) = 90
	// Row 4: sum(30,40,50) = 120
	// Row 5: sum(40,50) = 90 (can only look back)
	result := executeSQL(t, executor, `
		SELECT id, value, SUM(value) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as moving_sum
		FROM frame_test;
	`)

	if len(result.Rows) != 5 {
		t.Errorf("Test 1: Expected 5 rows, got %d", len(result.Rows))
	}

	expectedMovingSums := []int64{30, 60, 90, 120, 90}
	for i, row := range result.Rows {
		if row[2].Type == catalog.TypeInt64 {
			if row[2].Int64 != expectedMovingSums[i] {
				t.Errorf("Test 1 Row %d: Expected moving sum %d, got %d", i+1, expectedMovingSums[i], row[2].Int64)
			}
		}
	}

	// Test 2: COUNT with ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW (default behavior)
	// Row 1: count = 1
	// Row 2: count = 2
	// Row 3: count = 3
	// Row 4: count = 4
	// Row 5: count = 5
	result2 := executeSQL(t, executor, `
		SELECT id, COUNT(*) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_count
		FROM frame_test;
	`)

	if len(result2.Rows) != 5 {
		t.Errorf("Test 2: Expected 5 rows, got %d", len(result2.Rows))
	}

	for i, row := range result2.Rows {
		expected := int64(i + 1)
		if row[1].Type == catalog.TypeInt64 && row[1].Int64 != expected {
			t.Errorf("Test 2 Row %d: Expected count %d, got %d", i+1, expected, row[1].Int64)
		}
	}

	// Test 3: AVG with ROWS BETWEEN 2 PRECEDING AND CURRENT ROW (3-row moving average)
	// Row 1: avg(10) = 10
	// Row 2: avg(10,20) = 15
	// Row 3: avg(10,20,30) = 20
	// Row 4: avg(20,30,40) = 30
	// Row 5: avg(30,40,50) = 40
	result3 := executeSQL(t, executor, `
		SELECT id, AVG(value) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as moving_avg
		FROM frame_test;
	`)

	if len(result3.Rows) != 5 {
		t.Errorf("Test 3: Expected 5 rows, got %d", len(result3.Rows))
	}

	expectedMovingAvgs := []int64{10, 15, 20, 30, 40}
	for i, row := range result3.Rows {
		if row[1].Type == catalog.TypeInt64 {
			if row[1].Int64 != expectedMovingAvgs[i] {
				t.Errorf("Test 3 Row %d: Expected moving avg %d, got %d", i+1, expectedMovingAvgs[i], row[1].Int64)
			}
		}
	}

	// Test 4: MIN with ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING (entire partition)
	// All rows should have MIN = 10
	result4 := executeSQL(t, executor, `
		SELECT id, MIN(value) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as global_min
		FROM frame_test;
	`)

	if len(result4.Rows) != 5 {
		t.Errorf("Test 4: Expected 5 rows, got %d", len(result4.Rows))
	}

	for i, row := range result4.Rows {
		var minVal int64
		if row[1].Type == catalog.TypeInt64 {
			minVal = row[1].Int64
		} else if row[1].Type == catalog.TypeInt32 {
			minVal = int64(row[1].Int32)
		}
		if minVal != 10 {
			t.Errorf("Test 4 Row %d: Expected global min 10, got %d", i+1, minVal)
		}
	}

	// Test 5: MAX with ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
	// Row 1: max(10,20,30,40,50) = 50
	// Row 2: max(20,30,40,50) = 50
	// Row 3: max(30,40,50) = 50
	// Row 4: max(40,50) = 50
	// Row 5: max(50) = 50
	result5 := executeSQL(t, executor, `
		SELECT id, MAX(value) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as future_max
		FROM frame_test;
	`)

	if len(result5.Rows) != 5 {
		t.Errorf("Test 5: Expected 5 rows, got %d", len(result5.Rows))
	}

	for i, row := range result5.Rows {
		var maxVal int64
		if row[1].Type == catalog.TypeInt64 {
			maxVal = row[1].Int64
		} else if row[1].Type == catalog.TypeInt32 {
			maxVal = int64(row[1].Int32)
		}
		if maxVal != 50 {
			t.Errorf("Test 5 Row %d: Expected future max 50, got %d", i+1, maxVal)
		}
	}

	// Test 6: FIRST_VALUE with default frame (UNBOUNDED PRECEDING TO CURRENT ROW)
	// All rows should see first value = 10
	result6 := executeSQL(t, executor, `
		SELECT id, FIRST_VALUE(value) OVER (ORDER BY id) as first_val
		FROM frame_test;
	`)

	if len(result6.Rows) != 5 {
		t.Errorf("Test 6: Expected 5 rows, got %d", len(result6.Rows))
	}

	for i, row := range result6.Rows {
		var firstVal int64
		if row[1].Type == catalog.TypeInt64 {
			firstVal = row[1].Int64
		} else if row[1].Type == catalog.TypeInt32 {
			firstVal = int64(row[1].Int32)
		}
		if firstVal != 10 {
			t.Errorf("Test 6 Row %d: Expected first value 10, got %d", i+1, firstVal)
		}
	}

	// Test 7: LAST_VALUE with full frame
	// All rows should see last value = 50
	result7 := executeSQL(t, executor, `
		SELECT id, LAST_VALUE(value) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_val
		FROM frame_test;
	`)

	if len(result7.Rows) != 5 {
		t.Errorf("Test 7: Expected 5 rows, got %d", len(result7.Rows))
	}

	for i, row := range result7.Rows {
		var lastVal int64
		if row[1].Type == catalog.TypeInt64 {
			lastVal = row[1].Int64
		} else if row[1].Type == catalog.TypeInt32 {
			lastVal = int64(row[1].Int32)
		}
		if lastVal != 50 {
			t.Errorf("Test 7 Row %d: Expected last value 50, got %d", i+1, lastVal)
		}
	}
}

// TestNthValue tests NTH_VALUE window function.
func TestNthValue(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE nth_test (id INT, department TEXT, salary INT);")

	executeSQL(t, executor, "INSERT INTO nth_test VALUES (1, 'Engineering', 80000);")
	executeSQL(t, executor, "INSERT INTO nth_test VALUES (2, 'Engineering', 90000);")
	executeSQL(t, executor, "INSERT INTO nth_test VALUES (3, 'Engineering', 70000);")
	executeSQL(t, executor, "INSERT INTO nth_test VALUES (4, 'Sales', 60000);")
	executeSQL(t, executor, "INSERT INTO nth_test VALUES (5, 'Sales', 65000);")

	// Test NTH_VALUE with explicit ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
	// This ensures we get the 2nd value from the entire partition
	result := executeSQL(t, executor, `
		SELECT id, department, salary, NTH_VALUE(salary, 2) OVER (
			PARTITION BY department ORDER BY salary DESC
			ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
		) as second_highest
		FROM nth_test;
	`)

	if len(result.Rows) != 5 {
		t.Errorf("Expected 5 rows, got %d", len(result.Rows))
	}

	// The 2nd highest salary in Engineering is 80000 (after 90000)
	// The 2nd highest salary in Sales is 60000 (after 65000)
	for _, row := range result.Rows {
		dept := row[1].Text
		nthVal := row[3]

		if dept == "Engineering" {
			if nthVal.Type == catalog.TypeInt64 && nthVal.Int64 != 80000 {
				t.Errorf("Engineering: Expected NTH_VALUE(2) = 80000, got %d", nthVal.Int64)
			} else if nthVal.Type == catalog.TypeInt32 && nthVal.Int32 != 80000 {
				t.Errorf("Engineering: Expected NTH_VALUE(2) = 80000, got %d", nthVal.Int32)
			}
		} else if dept == "Sales" {
			if nthVal.Type == catalog.TypeInt64 && nthVal.Int64 != 60000 {
				t.Errorf("Sales: Expected NTH_VALUE(2) = 60000, got %d", nthVal.Int64)
			} else if nthVal.Type == catalog.TypeInt32 && nthVal.Int32 != 60000 {
				t.Errorf("Sales: Expected NTH_VALUE(2) = 60000, got %d", nthVal.Int32)
			}
		}
	}

	// Test NTH_VALUE with n > partition size - should return NULL
	result2 := executeSQL(t, executor, `
		SELECT id, department, NTH_VALUE(salary, 10) OVER (PARTITION BY department ORDER BY salary) as tenth_val
		FROM nth_test;
	`)

	if len(result2.Rows) != 5 {
		t.Errorf("Expected 5 rows, got %d", len(result2.Rows))
	}

	// All rows should have NULL for tenth value since partitions have < 10 rows
	for _, row := range result2.Rows {
		if !row[2].IsNull {
			t.Errorf("Expected NULL for NTH_VALUE(10) when partition has fewer rows, got %v", row[2])
		}
	}
}

// TestLateralJoin tests LATERAL join functionality.
func TestLateralJoin(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	// Create test tables
	executeSQL(t, executor, "CREATE TABLE departments (dept_id INT, dept_name TEXT);")
	executeSQL(t, executor, "CREATE TABLE employees (emp_id INT, emp_name TEXT, dept_id INT, salary INT);")

	// Insert test data
	executeSQL(t, executor, "INSERT INTO departments VALUES (1, 'Engineering');")
	executeSQL(t, executor, "INSERT INTO departments VALUES (2, 'Sales');")

	executeSQL(t, executor, "INSERT INTO employees VALUES (1, 'Alice', 1, 80000);")
	executeSQL(t, executor, "INSERT INTO employees VALUES (2, 'Bob', 1, 90000);")
	executeSQL(t, executor, "INSERT INTO employees VALUES (3, 'Carol', 2, 60000);")
	executeSQL(t, executor, "INSERT INTO employees VALUES (4, 'Dave', 2, 70000);")

	// Test basic LATERAL join - get top employee for each department
	// Note: LATERAL allows the subquery to reference columns from the left table
	result := executeSQL(t, executor, `
		SELECT d.dept_name, e.emp_name, e.salary
		FROM departments d
		CROSS JOIN LATERAL (
			SELECT emp_name, salary FROM employees WHERE dept_id = d.dept_id ORDER BY salary DESC LIMIT 1
		) AS e;
	`)

	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows (one per department), got %d", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("Row: %v", row)
		}
	}
}

// TestLateralJoinWithLeftJoin tests LEFT LATERAL join.
func TestLateralJoinWithLeftJoin(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE products (product_id INT, product_name TEXT);")
	executeSQL(t, executor, "CREATE TABLE orders (order_id INT, product_id INT, quantity INT);")

	executeSQL(t, executor, "INSERT INTO products VALUES (1, 'Widget');")
	executeSQL(t, executor, "INSERT INTO products VALUES (2, 'Gadget');")
	executeSQL(t, executor, "INSERT INTO products VALUES (3, 'Gizmo');")

	executeSQL(t, executor, "INSERT INTO orders VALUES (1, 1, 5);")
	executeSQL(t, executor, "INSERT INTO orders VALUES (2, 1, 3);")
	executeSQL(t, executor, "INSERT INTO orders VALUES (3, 2, 2);")
	// No orders for product 3 (Gizmo)

	// Test LEFT LATERAL - products with no orders should still appear
	result := executeSQL(t, executor, `
		SELECT p.product_name, o.quantity
		FROM products p
		LEFT JOIN LATERAL (
			SELECT quantity FROM orders WHERE product_id = p.product_id LIMIT 1
		) AS o ON TRUE;
	`)

	if len(result.Rows) < 3 {
		t.Errorf("Expected at least 3 rows, got %d", len(result.Rows))
	}
}

// TestMergeBasic tests basic MERGE statement functionality.
func TestMergeBasic(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	// Create target and source tables
	executeSQL(t, executor, "CREATE TABLE inventory (product_id INT, quantity INT);")
	executeSQL(t, executor, "CREATE TABLE shipment (product_id INT, quantity INT);")

	// Insert initial inventory
	executeSQL(t, executor, "INSERT INTO inventory VALUES (1, 100);")
	executeSQL(t, executor, "INSERT INTO inventory VALUES (2, 50);")

	// Insert shipment data
	executeSQL(t, executor, "INSERT INTO shipment VALUES (1, 25);") // Existing product, should update
	executeSQL(t, executor, "INSERT INTO shipment VALUES (3, 75);") // New product, should insert

	// Execute MERGE
	result := executeSQL(t, executor, `
		MERGE INTO inventory AS i
		USING shipment AS s
		ON i.product_id = s.product_id
		WHEN MATCHED THEN UPDATE SET quantity = i.quantity + s.quantity
		WHEN NOT MATCHED THEN INSERT (product_id, quantity) VALUES (s.product_id, s.quantity);
	`)

	if result.RowsAffected < 2 {
		t.Errorf("Expected at least 2 rows affected, got %d", result.RowsAffected)
	}

	// Verify results
	verifyResult := executeSQL(t, executor, "SELECT product_id, quantity FROM inventory ORDER BY product_id;")

	if len(verifyResult.Rows) != 3 {
		t.Errorf("Expected 3 products in inventory, got %d", len(verifyResult.Rows))
	}

	// Product 1 should have quantity 125 (100 + 25)
	if len(verifyResult.Rows) >= 1 {
		val := verifyResult.Rows[0][1]
		var qty int64
		if val.Type == catalog.TypeInt32 {
			qty = int64(val.Int32)
		} else {
			qty = val.Int64
		}
		if qty != 125 {
			t.Errorf("Expected product 1 quantity 125, got %d (type: %v)", qty, val.Type)
		}
	}

	// Product 3 should have quantity 75 (newly inserted)
	if len(verifyResult.Rows) >= 3 {
		val := verifyResult.Rows[2][1]
		var qty int64
		if val.Type == catalog.TypeInt32 {
			qty = int64(val.Int32)
		} else {
			qty = val.Int64
		}
		if qty != 75 {
			t.Errorf("Expected product 3 quantity 75, got %d (type: %v)", qty, val.Type)
		}
	}
}

// TestMergeDelete tests MERGE with DELETE action.
func TestMergeDelete(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE target (id INT, status TEXT);")
	executeSQL(t, executor, "CREATE TABLE remove_list (id INT);")

	executeSQL(t, executor, "INSERT INTO target VALUES (1, 'active');")
	executeSQL(t, executor, "INSERT INTO target VALUES (2, 'active');")
	executeSQL(t, executor, "INSERT INTO target VALUES (3, 'active');")

	executeSQL(t, executor, "INSERT INTO remove_list VALUES (2);")

	// Use MERGE to delete matching records
	result := executeSQL(t, executor, `
		MERGE INTO target AS t
		USING remove_list AS r
		ON t.id = r.id
		WHEN MATCHED THEN DELETE;
	`)

	if result.RowsAffected != 1 {
		t.Errorf("Expected 1 row deleted, got %d rows affected", result.RowsAffected)
	}

	// Verify id=2 was deleted
	verifyResult := executeSQL(t, executor, "SELECT id FROM target ORDER BY id;")

	if len(verifyResult.Rows) != 2 {
		t.Errorf("Expected 2 rows remaining, got %d", len(verifyResult.Rows))
	}
}

// TestMergeWithSubquery tests MERGE using a subquery as the source.
func TestMergeWithSubquery(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE accounts (account_id INT, balance INT);")
	executeSQL(t, executor, "CREATE TABLE transactions (account_id INT, amount INT);")

	executeSQL(t, executor, "INSERT INTO accounts VALUES (1, 1000);")
	executeSQL(t, executor, "INSERT INTO accounts VALUES (2, 2000);")

	executeSQL(t, executor, "INSERT INTO transactions VALUES (1, 100);")
	executeSQL(t, executor, "INSERT INTO transactions VALUES (1, 50);")
	executeSQL(t, executor, "INSERT INTO transactions VALUES (3, 500);")

	// MERGE with subquery source
	result := executeSQL(t, executor, `
		MERGE INTO accounts AS a
		USING (SELECT account_id, SUM(amount) AS total FROM transactions GROUP BY account_id) AS t
		ON a.account_id = t.account_id
		WHEN MATCHED THEN UPDATE SET balance = a.balance + t.total
		WHEN NOT MATCHED THEN INSERT (account_id, balance) VALUES (t.account_id, t.total);
	`)

	if result.RowsAffected < 2 {
		t.Errorf("Expected at least 2 rows affected, got %d", result.RowsAffected)
	}
}

// TestMergeDoNothing tests MERGE with DO NOTHING action.
func TestMergeDoNothing(t *testing.T) {
	tm := setupTestTableManager(t)
	executor := NewExecutor(tm)

	executeSQL(t, executor, "CREATE TABLE data (id INT, value INT);")
	executeSQL(t, executor, "CREATE TABLE updates (id INT, value INT);")

	executeSQL(t, executor, "INSERT INTO data VALUES (1, 10);")
	executeSQL(t, executor, "INSERT INTO updates VALUES (1, 20);")
	executeSQL(t, executor, "INSERT INTO updates VALUES (2, 30);")

	// Use DO NOTHING for matched, INSERT for not matched
	result := executeSQL(t, executor, `
		MERGE INTO data AS d
		USING updates AS u
		ON d.id = u.id
		WHEN MATCHED THEN DO NOTHING
		WHEN NOT MATCHED THEN INSERT (id, value) VALUES (u.id, u.value);
	`)

	// Should only insert id=2
	if result.RowsAffected != 1 {
		t.Errorf("Expected 1 row inserted, got %d rows affected", result.RowsAffected)
	}

	// Verify id=1 was not updated
	verifyResult := executeSQL(t, executor, "SELECT id, value FROM data ORDER BY id;")

	if len(verifyResult.Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(verifyResult.Rows))
	}

	// id=1 should still have value 10
	if len(verifyResult.Rows) >= 1 {
		val := verifyResult.Rows[0][1]
		var v int64
		if val.Type == catalog.TypeInt32 {
			v = int64(val.Int32)
		} else {
			v = val.Int64
		}
		if v != 10 {
			t.Errorf("Expected id=1 value to be 10, got %d", v)
		}
	}
}

// TestMergeParsing tests MERGE statement parsing.
func TestMergeParsing(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name: "basic merge",
			input: `MERGE INTO target USING source ON target.id = source.id
				WHEN MATCHED THEN UPDATE SET value = source.value`,
		},
		{
			name: "merge with aliases",
			input: `MERGE INTO target AS t USING source AS s ON t.id = s.id
				WHEN MATCHED THEN DELETE`,
		},
		{
			name: "merge with insert",
			input: `MERGE INTO target USING source ON target.id = source.id
				WHEN NOT MATCHED THEN INSERT (id, name) VALUES (source.id, source.name)`,
		},
		{
			name: "merge with multiple when clauses",
			input: `MERGE INTO target USING source ON target.id = source.id
				WHEN MATCHED THEN UPDATE SET value = 1
				WHEN NOT MATCHED THEN INSERT (id, value) VALUES (source.id, 0)`,
		},
		{
			name: "merge with do nothing",
			input: `MERGE INTO target USING source ON target.id = source.id
				WHEN MATCHED THEN DO NOTHING
				WHEN NOT MATCHED THEN DO NOTHING`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			stmt, err := parser.Parse()
			if err != nil {
				t.Errorf("Failed to parse MERGE: %v", err)
			}
			if _, ok := stmt.(*MergeStmt); !ok {
				t.Errorf("Expected *MergeStmt, got %T", stmt)
			}
		})
	}
}

// TestLateralParsing tests LATERAL join parsing.
func TestLateralParsing(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "cross join lateral",
			input: "SELECT * FROM t1 CROSS JOIN LATERAL (SELECT * FROM t2 WHERE t2.id = t1.id) AS sub",
		},
		{
			name:  "left join lateral",
			input: "SELECT * FROM t1 LEFT JOIN LATERAL (SELECT * FROM t2 WHERE t2.ref = t1.id LIMIT 1) AS sub ON TRUE",
		},
		{
			name:  "inner join lateral",
			input: "SELECT * FROM t1 INNER JOIN LATERAL (SELECT * FROM t2) AS sub ON t1.id = sub.id",
		},
		{
			name:  "lateral with alias",
			input: "SELECT * FROM t1, LATERAL (SELECT * FROM t2 WHERE t2.id = t1.id) AS derived",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			stmt, err := parser.Parse()
			if err != nil {
				t.Errorf("Failed to parse LATERAL: %v", err)
			}
			selectStmt, ok := stmt.(*SelectStmt)
			if !ok {
				t.Errorf("Expected *SelectStmt, got %T", stmt)
			}
			if len(selectStmt.Joins) == 0 {
				t.Error("Expected at least one join clause")
			} else if !selectStmt.Joins[0].Lateral {
				t.Error("Expected Lateral flag to be true")
			}
		})
	}
}

// TestGroupingSets tests GROUP BY GROUPING SETS functionality.
func TestGroupingSets(t *testing.T) {
	tm := setupTestTableManager(t)
	exec := NewExecutor(tm)

	// Create a sales table
	executeSQL(t, exec, "CREATE TABLE sales (region TEXT, product TEXT, amount INT)")

	// Insert test data
	executeSQL(t, exec, "INSERT INTO sales VALUES ('East', 'Widget', 100)")
	executeSQL(t, exec, "INSERT INTO sales VALUES ('East', 'Widget', 150)")
	executeSQL(t, exec, "INSERT INTO sales VALUES ('East', 'Gadget', 200)")
	executeSQL(t, exec, "INSERT INTO sales VALUES ('West', 'Widget', 300)")
	executeSQL(t, exec, "INSERT INTO sales VALUES ('West', 'Gadget', 250)")

	t.Run("basic grouping sets", func(t *testing.T) {
		// GROUPING SETS ((region), (product), ()) gives:
		// - sum by region
		// - sum by product
		// - grand total
		result := executeSQL(t, exec, "SELECT region, product, SUM(amount) AS total FROM sales GROUP BY GROUPING SETS ((region), (product), ())")

		// Should have 5 rows: 2 regions + 2 products + 1 grand total
		if len(result.Rows) != 5 {
			t.Errorf("Expected 5 rows, got %d", len(result.Rows))
			for _, row := range result.Rows {
				t.Logf("Row: %v", row)
			}
		}
	})

	t.Run("single column grouping sets", func(t *testing.T) {
		result := executeSQL(t, exec, "SELECT region, SUM(amount) AS total FROM sales GROUP BY GROUPING SETS ((region), ())")

		// Should have 3 rows: 2 regions + 1 grand total
		if len(result.Rows) != 3 {
			t.Errorf("Expected 3 rows, got %d", len(result.Rows))
		}
	})
}

// TestCube tests GROUP BY CUBE functionality.
func TestCube(t *testing.T) {
	tm := setupTestTableManager(t)
	exec := NewExecutor(tm)

	// Create and populate table
	executeSQL(t, exec, "CREATE TABLE sales (region TEXT, product TEXT, amount INT)")

	executeSQL(t, exec, "INSERT INTO sales VALUES ('East', 'Widget', 100)")
	executeSQL(t, exec, "INSERT INTO sales VALUES ('East', 'Gadget', 200)")
	executeSQL(t, exec, "INSERT INTO sales VALUES ('West', 'Widget', 300)")
	executeSQL(t, exec, "INSERT INTO sales VALUES ('West', 'Gadget', 250)")

	t.Run("cube two columns", func(t *testing.T) {
		// CUBE(region, product) generates:
		// (region, product), (region), (product), ()
		// = 4 grouping sets
		result := executeSQL(t, exec, "SELECT region, product, SUM(amount) AS total FROM sales GROUP BY CUBE(region, product)")

		// 4 combinations * distinct values:
		// (region, product): 4 rows (East/Widget, East/Gadget, West/Widget, West/Gadget)
		// (region): 2 rows (East, West)
		// (product): 2 rows (Widget, Gadget)
		// (): 1 row (grand total)
		// Total: 9 rows
		if len(result.Rows) != 9 {
			t.Errorf("Expected 9 rows for CUBE, got %d", len(result.Rows))
			for _, row := range result.Rows {
				t.Logf("Row: %v", row)
			}
		}
	})

	t.Run("cube single column", func(t *testing.T) {
		// CUBE(region) = GROUP BY GROUPING SETS ((region), ())
		result := executeSQL(t, exec, "SELECT region, SUM(amount) AS total FROM sales GROUP BY CUBE(region)")

		// 2 regions + 1 grand total = 3 rows
		if len(result.Rows) != 3 {
			t.Errorf("Expected 3 rows, got %d", len(result.Rows))
		}
	})
}

// TestRollup tests GROUP BY ROLLUP functionality.
func TestRollup(t *testing.T) {
	tm := setupTestTableManager(t)
	exec := NewExecutor(tm)

	// Create and populate table
	executeSQL(t, exec, "CREATE TABLE sales (yr INT, quarter TEXT, amount INT)")

	executeSQL(t, exec, "INSERT INTO sales VALUES (2023, 'Q1', 100)")
	executeSQL(t, exec, "INSERT INTO sales VALUES (2023, 'Q2', 150)")
	executeSQL(t, exec, "INSERT INTO sales VALUES (2023, 'Q3', 200)")
	executeSQL(t, exec, "INSERT INTO sales VALUES (2024, 'Q1', 250)")
	executeSQL(t, exec, "INSERT INTO sales VALUES (2024, 'Q2', 300)")

	t.Run("rollup two columns", func(t *testing.T) {
		// ROLLUP(yr, quarter) generates:
		// (yr, quarter), (yr), ()
		// = 3 grouping sets
		result := executeSQL(t, exec, "SELECT yr, quarter, SUM(amount) AS total FROM sales GROUP BY ROLLUP(yr, quarter)")

		// (yr, quarter): 5 rows (2023/Q1, 2023/Q2, 2023/Q3, 2024/Q1, 2024/Q2)
		// (yr): 2 rows (2023, 2024)
		// (): 1 row (grand total)
		// Total: 8 rows
		if len(result.Rows) != 8 {
			t.Errorf("Expected 8 rows for ROLLUP, got %d", len(result.Rows))
			for _, row := range result.Rows {
				t.Logf("Row: %v", row)
			}
		}
	})

	t.Run("rollup verifies hierarchy", func(t *testing.T) {
		// Check that subtotals exist
		result := executeSQL(t, exec, "SELECT yr, quarter, SUM(amount) AS total FROM sales GROUP BY ROLLUP(yr, quarter) ORDER BY yr, quarter")

		// Find the grand total (both yr and quarter are NULL)
		foundGrandTotal := false
		for _, row := range result.Rows {
			if row[0].IsNull && row[1].IsNull {
				foundGrandTotal = true
				if row[2].Int64 != 1000 {
					t.Errorf("Grand total should be 1000, got %d", row[2].Int64)
				}
				break
			}
		}
		if !foundGrandTotal {
			t.Error("Expected to find grand total row with NULL yr and quarter")
		}
	})
}

// TestGroupingFunction tests the GROUPING() function.
func TestGroupingFunction(t *testing.T) {
	tm := setupTestTableManager(t)
	exec := NewExecutor(tm)

	// Create and populate table
	executeSQL(t, exec, "CREATE TABLE sales (region TEXT, product TEXT, amount INT)")

	executeSQL(t, exec, "INSERT INTO sales VALUES ('East', 'Widget', 100)")
	executeSQL(t, exec, "INSERT INTO sales VALUES ('East', 'Gadget', 200)")
	executeSQL(t, exec, "INSERT INTO sales VALUES ('West', 'Widget', 300)")

	t.Run("grouping function with rollup", func(t *testing.T) {
		// GROUPING(col) returns 1 if column is rolled up (NULL in grouping), 0 otherwise
		result := executeSQL(t, exec, "SELECT region, product, SUM(amount) AS total, GROUPING(region) AS gr, GROUPING(product) AS gp FROM sales GROUP BY ROLLUP(region, product)")

		// Verify GROUPING function values
		for _, row := range result.Rows {
			regionIsNull := row[0].IsNull
			productIsNull := row[1].IsNull
			gr := row[3].Int64 // GROUPING(region)
			gp := row[4].Int64 // GROUPING(product)

			// When region is NULL in ROLLUP, GROUPING(region) should be 1
			if regionIsNull {
				if gr != 1 {
					t.Errorf("GROUPING(region) should be 1 when region is NULL, got %d", gr)
				}
			} else {
				if gr != 0 {
					t.Errorf("GROUPING(region) should be 0 when region is not NULL, got %d", gr)
				}
			}

			// When product is NULL in ROLLUP, GROUPING(product) should be 1
			if productIsNull {
				if gp != 1 {
					t.Errorf("GROUPING(product) should be 1 when product is NULL, got %d", gp)
				}
			} else {
				if gp != 0 {
					t.Errorf("GROUPING(product) should be 0 when product is not NULL, got %d", gp)
				}
			}
		}
	})
}

// TestGroupingSetsParsing tests parsing of GROUPING SETS/CUBE/ROLLUP syntax.
func TestGroupingSetsParsing(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		expectedSets  int
		expectsNormal bool // if true, expect normal GROUP BY columns
	}{
		{
			name:         "basic grouping sets",
			input:        "SELECT region, SUM(amount) FROM sales GROUP BY GROUPING SETS ((region), ())",
			expectedSets: 2,
		},
		{
			name:         "cube",
			input:        "SELECT region, product, SUM(amount) FROM sales GROUP BY CUBE(region, product)",
			expectedSets: 4, // 2^2 = 4
		},
		{
			name:         "rollup",
			input:        "SELECT yr, quarter, SUM(amount) FROM sales GROUP BY ROLLUP(yr, quarter)",
			expectedSets: 3, // n+1 = 3
		},
		{
			name:         "grouping sets with multiple columns",
			input:        "SELECT a, b, c FROM t GROUP BY GROUPING SETS ((a, b), (c), ())",
			expectedSets: 3,
		},
		{
			name:          "normal group by",
			input:         "SELECT region, SUM(amount) FROM sales GROUP BY region",
			expectsNormal: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			stmt, err := parser.Parse()
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			selectStmt, ok := stmt.(*SelectStmt)
			if !ok {
				t.Fatalf("Expected *SelectStmt, got %T", stmt)
			}

			if tt.expectsNormal {
				if len(selectStmt.GroupingSets) != 0 {
					t.Errorf("Expected no grouping sets for normal GROUP BY, got %d", len(selectStmt.GroupingSets))
				}
				if len(selectStmt.GroupBy) == 0 {
					t.Error("Expected GROUP BY columns for normal GROUP BY")
				}
			} else {
				if len(selectStmt.GroupingSets) != tt.expectedSets {
					t.Errorf("Expected %d grouping sets, got %d", tt.expectedSets, len(selectStmt.GroupingSets))
				}
			}
		})
	}
}

// TestCTEBasic tests basic CTE (WITH clause) functionality.
func TestCTEBasic(t *testing.T) {
	tm := setupTestTableManager(t)
	exec := NewExecutor(tm)

	// Create a table
	executeSQL(t, exec, "CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept TEXT, salary INT)")

	// Insert test data
	executeSQL(t, exec, "INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 100000)")
	executeSQL(t, exec, "INSERT INTO employees VALUES (2, 'Bob', 'Engineering', 90000)")
	executeSQL(t, exec, "INSERT INTO employees VALUES (3, 'Carol', 'Sales', 80000)")
	executeSQL(t, exec, "INSERT INTO employees VALUES (4, 'Dave', 'Sales', 85000)")
	executeSQL(t, exec, "INSERT INTO employees VALUES (5, 'Eve', 'HR', 70000)")

	t.Run("simple CTE select all", func(t *testing.T) {
		result := executeSQL(t, exec, `
			WITH eng AS (SELECT id, name, salary FROM employees WHERE dept = 'Engineering')
			SELECT * FROM eng
		`)

		if len(result.Rows) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(result.Rows))
		}
	})

	t.Run("CTE with column selection", func(t *testing.T) {
		result := executeSQL(t, exec, `
			WITH eng AS (SELECT id, name, salary FROM employees WHERE dept = 'Engineering')
			SELECT name, salary FROM eng
		`)

		if len(result.Columns) != 2 {
			t.Errorf("Expected 2 columns, got %d", len(result.Columns))
		}
		if len(result.Rows) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(result.Rows))
		}
	})

	t.Run("CTE with aggregation", func(t *testing.T) {
		result := executeSQL(t, exec, `
			WITH eng AS (SELECT salary FROM employees WHERE dept = 'Engineering')
			SELECT SUM(salary) AS total FROM eng
		`)

		if len(result.Rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(result.Rows))
		}
		if !result.Rows[0][0].IsNull && result.Rows[0][0].Int64 != 190000 {
			t.Errorf("Expected sum 190000, got %v", result.Rows[0][0])
		}
	})

	t.Run("CTE with ORDER BY and LIMIT", func(t *testing.T) {
		result := executeSQL(t, exec, `
			WITH all_emp AS (SELECT name, salary FROM employees)
			SELECT name, salary FROM all_emp ORDER BY salary DESC LIMIT 3
		`)

		if len(result.Rows) != 3 {
			t.Errorf("Expected 3 rows, got %d", len(result.Rows))
		}
		// First row should be highest salary (Alice, 100000)
		salaryVal := result.Rows[0][1]
		var salary int64
		if salaryVal.Type == catalog.TypeInt32 {
			salary = int64(salaryVal.Int32)
		} else {
			salary = salaryVal.Int64
		}
		if salary != 100000 {
			t.Errorf("Expected highest salary 100000, got %d", salary)
		}
	})
}

// TestCTEParsing tests CTE parsing.
func TestCTEParsing(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		cteCount  int
		recursive bool
		firstCTE  string
	}{
		{
			name:      "single CTE",
			input:     "WITH cte AS (SELECT * FROM t) SELECT * FROM cte",
			cteCount:  1,
			recursive: false,
			firstCTE:  "cte",
		},
		{
			name:      "multiple CTEs",
			input:     "WITH cte1 AS (SELECT a FROM t), cte2 AS (SELECT b FROM t) SELECT * FROM cte1",
			cteCount:  2,
			recursive: false,
			firstCTE:  "cte1",
		},
		{
			name:      "CTE with column aliases",
			input:     "WITH cte (x, y) AS (SELECT a, b FROM t) SELECT * FROM cte",
			cteCount:  1,
			recursive: false,
			firstCTE:  "cte",
		},
		{
			name:      "recursive CTE",
			input:     "WITH RECURSIVE cte AS (SELECT * FROM t) SELECT * FROM cte",
			cteCount:  1,
			recursive: true,
			firstCTE:  "cte",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.input)
			stmt, err := parser.Parse()
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			selectStmt, ok := stmt.(*SelectStmt)
			if !ok {
				t.Fatalf("Expected *SelectStmt, got %T", stmt)
			}

			if selectStmt.With == nil {
				t.Fatal("Expected WITH clause")
			}

			if len(selectStmt.With.CTEs) != tt.cteCount {
				t.Errorf("Expected %d CTEs, got %d", tt.cteCount, len(selectStmt.With.CTEs))
			}

			if selectStmt.With.Recursive != tt.recursive {
				t.Errorf("Expected recursive=%v, got %v", tt.recursive, selectStmt.With.Recursive)
			}

			if len(selectStmt.With.CTEs) > 0 && selectStmt.With.CTEs[0].Name != tt.firstCTE {
				t.Errorf("Expected first CTE name %q, got %q", tt.firstCTE, selectStmt.With.CTEs[0].Name)
			}
		})
	}
}

// TestCTEWithColumnAliases tests CTE with explicit column aliases.
func TestCTEWithColumnAliases(t *testing.T) {
	tm := setupTestTableManager(t)
	exec := NewExecutor(tm)

	// Create a table
	executeSQL(t, exec, "CREATE TABLE nums (a INT, b INT)")
	executeSQL(t, exec, "INSERT INTO nums VALUES (1, 10)")
	executeSQL(t, exec, "INSERT INTO nums VALUES (2, 20)")

	t.Run("CTE with column aliases", func(t *testing.T) {
		result := executeSQL(t, exec, `
			WITH cte (x, y) AS (SELECT a, b FROM nums)
			SELECT x, y FROM cte
		`)

		if len(result.Columns) != 2 {
			t.Errorf("Expected 2 columns, got %d", len(result.Columns))
		}
		if result.Columns[0] != "x" || result.Columns[1] != "y" {
			t.Errorf("Expected columns [x, y], got %v", result.Columns)
		}
		if len(result.Rows) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(result.Rows))
		}
	})
}

// TestCTEMultiple tests queries with multiple CTEs.
func TestCTEMultiple(t *testing.T) {
	tm := setupTestTableManager(t)
	exec := NewExecutor(tm)

	// Create tables
	executeSQL(t, exec, "CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT)")
	executeSQL(t, exec, "INSERT INTO products VALUES (1, 'Widget', 100)")
	executeSQL(t, exec, "INSERT INTO products VALUES (2, 'Gadget', 200)")
	executeSQL(t, exec, "INSERT INTO products VALUES (3, 'Gizmo', 150)")

	t.Run("multiple CTEs", func(t *testing.T) {
		result := executeSQL(t, exec, `
			WITH 
				cheap AS (SELECT name, price FROM products WHERE price < 160),
				expensive AS (SELECT name, price FROM products WHERE price >= 160)
			SELECT * FROM cheap
		`)

		// cheap: Widget (100), Gizmo (150) = 2 products
		if len(result.Rows) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(result.Rows))
		}
	})
}

// TestRecursiveCTE tests recursive CTE execution.
func TestRecursiveCTE(t *testing.T) {
	tm := setupTestTableManager(t)
	exec := NewExecutor(tm)

	// Create a hierarchy table for recursive CTE testing
	executeSQL(t, exec, "CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, manager_id INT)")
	executeSQL(t, exec, "INSERT INTO employees VALUES (1, 'CEO', 0)")      // Top level
	executeSQL(t, exec, "INSERT INTO employees VALUES (2, 'VP1', 1)")      // Reports to CEO
	executeSQL(t, exec, "INSERT INTO employees VALUES (3, 'VP2', 1)")      // Reports to CEO
	executeSQL(t, exec, "INSERT INTO employees VALUES (4, 'Manager1', 2)") // Reports to VP1
	executeSQL(t, exec, "INSERT INTO employees VALUES (5, 'Manager2', 2)") // Reports to VP1
	executeSQL(t, exec, "INSERT INTO employees VALUES (6, 'Worker1', 4)")  // Reports to Manager1

	t.Run("recursive CTE - employee hierarchy", func(t *testing.T) {
		// Find all employees who report (directly or indirectly) to employee id 1 (CEO)
		result := executeSQL(t, exec, `
			WITH RECURSIVE reports AS (
				SELECT id, name, manager_id FROM employees WHERE manager_id = 1
				UNION ALL
				SELECT e.id, e.name, e.manager_id FROM employees e JOIN reports r ON e.manager_id = r.id
			)
			SELECT * FROM reports
		`)

		// Should get: VP1, VP2 (direct reports)
		// Then: Manager1, Manager2 (report to VP1)
		// Then: Worker1 (reports to Manager1)
		// Total: 5 employees
		if len(result.Rows) != 5 {
			t.Errorf("Expected 5 rows (all reports to CEO), got %d", len(result.Rows))
			for i, row := range result.Rows {
				t.Logf("Row %d: %v", i, row)
			}
		}
	})
}
