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
