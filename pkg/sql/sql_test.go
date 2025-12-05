package sql

import (
	"path/filepath"
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
)

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
		{name: "drop table", input: "DROP TABLE users;", wantErr: false},
		{name: "update", input: "UPDATE users SET name = 'bob' WHERE id = 1;", wantErr: false},
		{name: "delete", input: "DELETE FROM users WHERE id = 1;", wantErr: false},
		{name: "delete all", input: "DELETE FROM users;", wantErr: false},
		{name: "complex where", input: "SELECT * FROM users WHERE age > 18 AND name = 'alice';", wantErr: false},
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
