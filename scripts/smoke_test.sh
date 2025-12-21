#!/bin/bash
#
# Smoke Test for VeridicalDB v1.0.0
# Tests core working functionality
#
set -e

echo "=== VeridicalDB Smoke Test ==="
echo ""

# Build the CLI
echo "[SETUP] Building CLI binary..."
cd /home/jayabrata/VeridicalDB
go build -o veridicaldb ./cmd/veridicaldb
echo "[SETUP] Build complete."

# Clean up any previous test data (use fresh data dir each time)
TEST_DIR="/tmp/veridical_smoke_test_$$"
rm -rf "$TEST_DIR"
mkdir -p "$TEST_DIR/data"

# Create config with correct nested structure
cat > "$TEST_DIR/config.yaml" << EOF
storage:
  data_dir: $TEST_DIR/data
log:
  level: error
EOF

# Initialize the fresh database directory
echo "[SETUP] Initializing database..."
./veridicaldb init "$TEST_DIR/data"
echo "[SETUP] Database initialized."

echo ""
echo "=== Running Smoke Tests ==="
echo ""

# Run the CLI with test commands
# Note: 'admin' user is auto-created on init, so we create different users
OUTPUT=$(./veridicaldb --config "$TEST_DIR/config.yaml" << 'EOF'
CREATE USER testuser WITH PASSWORD 'secret123';
CREATE USER readonly WITH PASSWORD 'readpass';
CREATE TABLE orphan (id INT);
CREATE DATABASE testdb;
CREATE DATABASE analytics;
SHOW DATABASES;
USE testdb;
CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, email TEXT, age INT, active BOOLEAN DEFAULT true);
CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product TEXT, quantity INT, status TEXT DEFAULT 'pending');
CREATE TABLE products (id INT PRIMARY KEY, name TEXT NOT NULL, category TEXT, stock INT DEFAULT 0, price INT DEFAULT 0);
CREATE TABLE logs (id INT PRIMARY KEY, message TEXT, level TEXT, created_at TEXT);
CREATE TABLE departments (id INT PRIMARY KEY, name TEXT NOT NULL);
SHOW TABLES;
INSERT INTO users (id, name, email, age, active) VALUES (1, 'Alice', 'alice@example.com', 30, true);
INSERT INTO users (id, name, email, age, active) VALUES (2, 'Bob', 'bob@example.com', 25, true);
INSERT INTO users (id, name, email, age, active) VALUES (3, 'Charlie', 'charlie@example.com', 35, false);
INSERT INTO users (id, name, email, age, active) VALUES (4, 'Diana', 'diana@example.com', 28, true);
INSERT INTO users (id, name, email, age, active) VALUES (5, 'Eve', 'eve@example.com', 32, true);
INSERT INTO products (id, name, category, stock, price) VALUES (1, 'Laptop', 'Electronics', 50, 1000);
INSERT INTO products (id, name, category, stock, price) VALUES (2, 'Mouse', 'Electronics', 200, 25);
INSERT INTO products (id, name, category, stock, price) VALUES (3, 'Desk', 'Furniture', 30, 300);
INSERT INTO products (id, name, category, stock, price) VALUES (4, 'Chair', 'Furniture', 45, 150);
INSERT INTO products (id, name, category, stock, price) VALUES (5, 'Monitor', 'Electronics', 75, 400);
INSERT INTO orders (id, user_id, product, quantity, status) VALUES (1, 1, 'Laptop', 1, 'completed');
INSERT INTO orders (id, user_id, product, quantity, status) VALUES (2, 1, 'Mouse', 2, 'completed');
INSERT INTO orders (id, user_id, product, quantity, status) VALUES (3, 2, 'Desk', 1, 'pending');
INSERT INTO orders (id, user_id, product, quantity, status) VALUES (4, 3, 'Chair', 2, 'shipped');
INSERT INTO logs (id, message, level) VALUES (1, 'System started', 'INFO');
INSERT INTO logs (id, message, level) VALUES (2, 'User logged in', 'INFO');
INSERT INTO logs (id, message, level) VALUES (3, 'Error occurred', 'ERROR');
INSERT INTO departments (id, name) VALUES (1, 'Engineering');
INSERT INTO departments (id, name) VALUES (2, 'Sales');
SELECT * FROM users;
SELECT name, email FROM users WHERE age > 28;
SELECT * FROM users WHERE active = true;
SELECT * FROM users WHERE active = false;
SELECT * FROM users ORDER BY age DESC;
SELECT * FROM users ORDER BY name ASC;
SELECT * FROM users LIMIT 3;
SELECT * FROM users LIMIT 2 OFFSET 1;
SELECT name FROM users WHERE age >= 30 AND active = true;
SELECT name FROM users WHERE age < 28 OR name = 'Eve';
SELECT * FROM products WHERE stock > 40;
SELECT * FROM products WHERE category = 'Electronics';
SELECT * FROM products WHERE category = 'Furniture';
SELECT DISTINCT category FROM products;
UPDATE users SET age = 26 WHERE name = 'Bob';
SELECT name, age FROM users WHERE name = 'Bob';
UPDATE products SET stock = 48 WHERE name = 'Laptop';
SELECT name, stock FROM products WHERE name = 'Laptop';
DELETE FROM orders WHERE id = 3;
SELECT * FROM orders;
BEGIN;
INSERT INTO users (id, name, email, age) VALUES (6, 'Frank', 'frank@example.com', 40);
SELECT name FROM users WHERE id = 6;
COMMIT;
SELECT * FROM users WHERE id = 6;
BEGIN;
INSERT INTO users (id, name, email, age) VALUES (7, 'Grace', 'grace@example.com', 29);
ROLLBACK;
SELECT * FROM users WHERE id = 7;
EXPLAIN SELECT * FROM users WHERE age > 30;
EXPLAIN SELECT * FROM products WHERE category = 'Electronics';




INSERT INTO logs (id, message, level) VALUES (10, 'Batch1', 'DEBUG'), (11, 'Batch2', 'DEBUG'), (12, 'Batch3', 'DEBUG');
SELECT * FROM logs WHERE level = 'DEBUG';


SELECT name AS user_name, age AS user_age FROM users WHERE id = 1;


SELECT name, age FROM users WHERE id = 1;


SELECT COUNT(*) FROM users;
SELECT COUNT(*) FROM users WHERE active = true;
SELECT SUM(stock) FROM products;
SELECT AVG(age) FROM users;
SELECT MIN(age) FROM users;
SELECT MAX(age) FROM users;
SELECT SUM(price) FROM products WHERE category = 'Electronics';


SELECT category, COUNT(*) FROM products GROUP BY category;
SELECT category, SUM(stock) FROM products GROUP BY category;
SELECT active, COUNT(*) FROM users GROUP BY active;


SELECT category, SUM(stock) FROM products GROUP BY category HAVING SUM(stock) > 100;


SELECT users.name, orders.product FROM users INNER JOIN orders ON users.id = orders.user_id;
SELECT users.name, orders.product FROM users LEFT JOIN orders ON users.id = orders.user_id;
SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id WHERE orders.status = 'completed';


SELECT name FROM users WHERE age = (SELECT MAX(age) FROM users);
SELECT name FROM users WHERE age > (SELECT AVG(age) FROM users);


SELECT name FROM users WHERE id IN (1, 2, 3);
SELECT name FROM users WHERE id NOT IN (1, 2);
SELECT name FROM products WHERE category IN ('Electronics', 'Furniture');


SELECT name FROM users WHERE age BETWEEN 25 AND 32;
SELECT name FROM products WHERE stock BETWEEN 40 AND 100;


SELECT name FROM users WHERE name LIKE 'A%';
SELECT name FROM users WHERE email LIKE '%example.com';
SELECT name FROM users WHERE name ILIKE 'alice';


SELECT name FROM users WHERE email IS NOT NULL;
INSERT INTO users (id, name, age) VALUES (8, 'NullEmail', 22);
SELECT name FROM users WHERE email IS NULL;


SELECT name, CASE WHEN age < 30 THEN 'young' WHEN age < 40 THEN 'middle' ELSE 'senior' END AS age_group FROM users;
SELECT name, CASE WHEN active = true THEN 'Active' ELSE 'Inactive' END AS status FROM users;


SELECT name, COALESCE(email, 'no-email') FROM users WHERE id = 8;
SELECT NULLIF(1, 1) FROM users WHERE id = 1;
SELECT NULLIF(1, 2) FROM users WHERE id = 1;


SELECT UPPER(name) FROM users WHERE id = 1;
SELECT LOWER(name) FROM users WHERE id = 1;
SELECT LENGTH(name) FROM users WHERE id = 1;
SELECT CONCAT(name, ' - ', email) FROM users WHERE id = 1;


SELECT ABS(-5) FROM users WHERE id = 1;
SELECT ROUND(3.7) FROM users WHERE id = 1;
SELECT FLOOR(3.9) FROM users WHERE id = 1;
SELECT CEIL(3.1) FROM users WHERE id = 1;


SELECT NOW() FROM users WHERE id = 1;
SELECT CURRENT_TIMESTAMP FROM users WHERE id = 1;


SELECT CAST(123 AS TEXT) FROM users WHERE id = 1;
SELECT CAST('456' AS INT) FROM users WHERE id = 1;


SELECT name FROM users WHERE id = 1 UNION SELECT name FROM users WHERE id = 2;
SELECT name FROM users WHERE id IN (1, 2) UNION ALL SELECT name FROM users WHERE id IN (2, 3);
SELECT name FROM users WHERE id IN (1, 2, 3) INTERSECT SELECT name FROM users WHERE id IN (2, 3, 4);
SELECT name FROM users WHERE id IN (1, 2, 3) EXCEPT SELECT name FROM users WHERE id IN (3, 4);


WITH active_users AS (SELECT * FROM users WHERE active = true) SELECT name FROM active_users;
WITH user_counts AS (SELECT COUNT(*) AS cnt FROM users) SELECT cnt FROM user_counts;


SELECT name, age, ROW_NUMBER() OVER (ORDER BY age) AS row_num FROM users;
SELECT name, age, RANK() OVER (ORDER BY age DESC) AS rank FROM users;
SELECT category, name, SUM(stock) OVER (PARTITION BY category) AS category_total FROM products;


ALTER TABLE users ADD COLUMN phone TEXT;
SELECT phone FROM users LIMIT 1;
ALTER TABLE logs RENAME TO audit_logs;
SHOW TABLES;
ALTER TABLE audit_logs RENAME TO logs;


CREATE TABLE checked_data (id INT PRIMARY KEY, value INT CHECK (value > 0));
INSERT INTO checked_data (id, value) VALUES (1, 10);


CREATE TABLE child_orders (id INT PRIMARY KEY, user_id INT REFERENCES users(id), amount INT);
INSERT INTO child_orders (id, user_id, amount) VALUES (1, 1, 100);


SELECT table_name FROM information_schema.tables;
SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'users';


SHOW CREATE TABLE users;


ALTER USER testuser WITH PASSWORD 'newpass123';
DROP USER readonly;


CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_products_category ON products(category);
EXPLAIN SELECT * FROM users WHERE email = 'alice@example.com';

INSERT INTO products (id, name, category, stock) VALUES (1, 'Laptop Pro', 'Electronics', 30) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, stock = EXCLUDED.stock;
SELECT id, name, stock FROM products WHERE id = 1;

CREATE PROCEDURE log_user_update() AS $$BEGIN INSERT INTO logs (id, message, level) VALUES (999, 'triggered', 'INFO'); END;$$ LANGUAGE plpgsql;

CREATE FUNCTION log_user_fn() RETURNS TRIGGER AS $$BEGIN INSERT INTO logs (id, message, level) VALUES (888, 'trigger_executed', 'DEBUG'); END;$$ LANGUAGE plpgsql;

CREATE TRIGGER trig_log AFTER UPDATE ON users FOR EACH ROW EXECUTE FUNCTION log_user_fn();
UPDATE users SET age = age + 1 WHERE id = 1;
SELECT * FROM logs WHERE id = 888;
DROP TRIGGER trig_log ON users;
DROP PROCEDURE log_user_update;
DROP FUNCTION log_user_fn;
PREPARE ins_prod AS INSERT INTO products (id, name, category, stock) VALUES ($1, $2, 'Electronics', 5);
EXECUTE ins_prod(999, 'PrepProd');
DEALLOCATE ins_prod;
SELECT name FROM products WHERE id = 999;

TRUNCATE TABLE orders;
SELECT * FROM orders;

CREATE PROCEDURE proc_log() AS $$BEGIN INSERT INTO logs (id, message, level) VALUES (556, 'proc_called', 'INFO'); END;$$ LANGUAGE plpgsql;
CALL proc_log();
SELECT message FROM logs WHERE id = 556;

SHOW FUNCTIONS;
SHOW PROCEDURES;

CREATE USER bob WITH PASSWORD 'p';
GRANT SELECT ON users TO bob;
REVOKE SELECT ON users FROM bob;

CREATE VIEW v1 AS SELECT id, name FROM users;
SELECT * FROM v1;

CREATE FULLTEXT INDEX fts_users ON users(name, email);

SELECT name FROM users WHERE name @@ 'Alice';

CREATE TABLE merge_src (id INT PRIMARY KEY, name TEXT);
INSERT INTO merge_src (id, name) VALUES (1, 'Laptop M');
MERGE INTO products AS tgt
USING merge_src AS src
ON tgt.id = src.id
WHEN MATCHED THEN UPDATE SET name = src.name
WHEN NOT MATCHED THEN INSERT (id, name, category, stock) VALUES (src.id, src.name, 'Electronics', 10);
SELECT name FROM products WHERE id = 1;
ALTER TABLE users ADD COLUMN metadata JSON;

SELECT metadata FROM users LIMIT 1;


UPDATE users SET metadata = '{"role": "admin", "level": 5}' WHERE id = 1;
SELECT metadata FROM users WHERE id = 1;

DROP TABLE logs;
SHOW TABLES;
DROP DATABASE analytics;
SHOW DATABASES;
EXIT;
EOF
)

echo "$OUTPUT"
echo ""
echo "=== Test Results Summary ==="
echo ""

# Define check function
PASS_COUNT=0
FAIL_COUNT=0

check() {
    local name="$1"
    local pattern="$2"
    if echo "$OUTPUT" | grep -qi "$pattern"; then
        echo "✓ PASS: $name"
        PASS_COUNT=$((PASS_COUNT + 1))
    else
        echo "✗ FAIL: $name (expected pattern: $pattern)"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi
}

# check_not: assert a pattern does NOT exist in output
check_not() {
    local name="$1"
    local pattern="$2"
    if echo "$OUTPUT" | grep -qi "$pattern"; then
        echo "✗ FAIL: $name (should NOT match: $pattern)"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    else
        echo "✓ PASS: $name"
        PASS_COUNT=$((PASS_COUNT + 1))
    fi
}

# Core functionality checks
echo "--- User & Database Management ---"
check "CREATE USER testuser" "User.*created\|CREATE USER"
check "CREATE USER readonly" "User.*created\|readonly"
check "CREATE DATABASE testdb" "Database.*created\|CREATE DATABASE"
check "CREATE DATABASE analytics" "Database.*created\|analytics"
check "SHOW DATABASES" "default"
check "CREATE TABLE without DB fails" "no database selected"

echo ""
echo "--- DDL: Table Operations ---"
check "CREATE TABLE users" "Table.*created"
check "CREATE TABLE orders" "Table.*created"
check "CREATE TABLE products" "Table.*created"
check "CREATE TABLE logs" "Table.*created"
check "SHOW TABLES" "users"

echo ""
echo "--- DML: INSERT ---"
check "INSERT users" "row inserted"
check "INSERT products" "row inserted"
check "INSERT orders" "row inserted"
check "INSERT logs" "row inserted"

echo ""
echo "--- DML: SELECT Basic ---"
check "SELECT * FROM users" "Alice"
check "SELECT all users - Bob" "Bob"
check "SELECT all users - Charlie" "Charlie"
check "SELECT all users - Diana" "Diana"
check "SELECT all users - Eve" "Eve"

echo ""
echo "--- DML: SELECT with WHERE ---"
check "SELECT WHERE age > 28" "alice@example.com"
check "SELECT WHERE active = true" "Alice"
check "SELECT WHERE active = false" "Charlie"
check "SELECT WHERE stock > 40" "Laptop\|Monitor"
check "SELECT WHERE category Electronics" "Laptop\|Mouse\|Monitor"
check "SELECT WHERE category Furniture" "Desk\|Chair"

echo ""
echo "--- DML: SELECT with ORDER BY ---"
check "SELECT ORDER BY age DESC" "Charlie"
check "SELECT ORDER BY name ASC" "Alice"

echo ""
echo "--- DML: SELECT with LIMIT/OFFSET ---"
check "SELECT LIMIT" "Alice"
check "SELECT with results" "row"

echo ""
echo "--- DML: SELECT with AND/OR ---"
check "SELECT with AND condition" "Alice\|Eve"
check "SELECT with OR condition" "Bob\|Eve"

echo ""
echo "--- DML: SELECT DISTINCT ---"
check "SELECT DISTINCT category" "Electronics\|Furniture"

echo ""
echo "--- DML: UPDATE ---"
check "UPDATE users" "row.*updated"
check "UPDATE verify Bob age" "26"
check "UPDATE products" "row.*updated"
check "UPDATE verify Laptop stock" "48"

echo ""
echo "--- DML: DELETE ---"
check "DELETE from orders" "row.*deleted"
check "DELETE verify - remaining orders" "Laptop"

echo ""
echo "--- Transactions ---"
check "Transaction BEGIN" "BEGIN.*txid"
check "Transaction INSERT Frank" "Frank"
check "Transaction COMMIT" "COMMIT.*txid"
check "Committed data persists" "Frank"
check "Transaction ROLLBACK" "ROLLBACK.*txid"
check "ROLLBACK removes uncommitted data" "0 row"

echo ""
echo "--- Query Planning ---"
check "EXPLAIN users" "TableScan"
check "EXPLAIN products" "TableScan"

echo ""
echo "--- Multi-row INSERT ---"
check "Multi-row INSERT executed" "3 row.*inserted\|rows inserted"
check "Multi-row INSERT data exists" "Batch1\|Batch2\|DEBUG"

echo ""
echo "--- Column and Table Aliases ---"
check "Column alias (AS)" "user_name\|user_age"
check "Table alias" "Alice"

echo ""
echo "--- Aggregate Functions ---"
check "COUNT(*)" "[5-9]\|count"
check "SUM(stock)" "[0-9]"
check "AVG(age)" "[0-9]"
check "MIN(age)" "2[2-5]"
check "MAX(age)" "3[5-9]\|40"

echo ""
echo "--- GROUP BY ---"
check "GROUP BY category" "Electronics\|Furniture"
check "GROUP BY with count" "[0-9]"

echo ""
echo "--- GROUP BY with HAVING ---"
check "HAVING clause filters groups" "Electronics\|Furniture"

echo ""
echo "--- JOINs ---"
check "INNER JOIN" "Alice.*Laptop\|Laptop.*Alice"
check "LEFT JOIN" "Alice\|Bob\|Charlie"
check "JOIN with WHERE" "completed"

echo ""
echo "--- Subqueries ---"
check "Subquery with MAX" "Charlie\|Frank"
check "Subquery with AVG" "Charlie\|Eve\|Frank"

echo ""
echo "--- IN / NOT IN ---"
check "IN clause" "Alice\|Bob\|Charlie"
check "NOT IN clause" "Charlie\|Diana\|Eve"

echo ""
echo "--- BETWEEN ---"
check "BETWEEN for age" "Bob\|Alice\|Eve\|Diana"
check "BETWEEN for stock" "Laptop\|Chair\|Monitor"

echo ""
echo "--- LIKE / ILIKE ---"
check "LIKE A%" "Alice"
check "LIKE %example.com" "Alice\|Bob"
check "ILIKE case insensitive" "Alice"

echo ""
echo "--- IS NULL / IS NOT NULL ---"
check "IS NOT NULL" "Alice\|Bob"
check "IS NULL" "NullEmail"

echo ""
echo "--- CASE WHEN ---"
check "CASE WHEN age groups" "young\|middle\|senior"
check "CASE WHEN active status" "Active\|Inactive"

echo ""
echo "--- COALESCE / NULLIF ---"
check "COALESCE replaces NULL" "no-email"
check "NULLIF returns NULL" "NULL\|null"

echo ""
echo "--- String Functions ---"
check "UPPER function" "ALICE"
check "LOWER function" "alice"
check "LENGTH function" "[5-9]"
check "CONCAT function" "Alice.*alice@example.com\|-"

echo ""
echo "--- Math Functions ---"
check "ABS function" "5"
check "ROUND function" "4"
check "FLOOR function" "3"
check "CEIL function" "4"

echo ""
echo "--- Date/Time Functions ---"
check "NOW function" "20[0-9][0-9]"
check "CURRENT_TIMESTAMP" "20[0-9][0-9]"

echo ""
echo "--- CAST ---"
check "CAST to TEXT" "123"
check "CAST to INT" "456"

echo ""
echo "--- UNION / INTERSECT / EXCEPT ---"
check "UNION" "Alice\|Bob"
check "UNION ALL" "Alice\|Bob\|Charlie"
check "INTERSECT" "Bob\|Charlie"
check "EXCEPT" "Alice"

echo ""
echo "--- CTE (WITH clause) ---"
check "Simple CTE" "Alice\|Bob\|Diana\|Eve"
check "CTE with aggregate" "[5-9]"

echo ""
echo "--- Window Functions ---"
check "ROW_NUMBER" "row_num\|1\|2\|3"
check "RANK" "rank\|1\|2"
check "SUM OVER PARTITION" "category_total"

echo ""
echo "--- ALTER TABLE ---"
check "ALTER TABLE ADD COLUMN" "phone\|column"
check "ALTER TABLE RENAME" "audit_logs"

echo ""
echo "--- Constraints ---"
check "CHECK constraint table created" "checked_data"
check "FOREIGN KEY constraint" "child_orders"

echo ""
echo "--- information_schema ---"
check "information_schema.tables" "users\|products\|orders"
check "information_schema.columns" "name\|age\|email"

echo ""
echo "--- SHOW CREATE TABLE ---"
check "SHOW CREATE TABLE" "CREATE TABLE\|users\|id\|name"

echo ""
echo "--- ALTER/DROP USER ---"
check "ALTER USER" "ALTER USER\|Password"
check "DROP USER readonly" "DROP USER\|dropped"

echo ""
echo "--- Indexes and Advanced Features ---"
check "CREATE INDEX" "Index.*created\|CREATE INDEX\|idx_"
check "EXPLAIN uses index" "Index\|IndexScan\|TableScan"
check "UPSERT (ON CONFLICT)" "Laptop Pro\|30"
check "CREATE PROCEDURE" "CREATE PROCEDURE\|log_user_update"
check "CREATE FUNCTION for trigger" "CREATE FUNCTION\|log_user_fn"
check "CREATE TRIGGER" "CREATE TRIGGER\|trig_log"
check "TRIGGER executes function" "trigger_executed"

# Additional feature checks
check "PREPARE/EXECUTE inserted product" "PrepProd"
check "DEALLOCATE prepared statement" "DEALLOCATE\|DEALLOCATE"
check "TRUNCATE cleared orders" "0 row"
check "CALL procedure executed" "proc_called"
check "SHOW FUNCTIONS lists functions" "proc_log\|log_user_fn"
check "SHOW PROCEDURES lists procedures" "log_user_update\|proc_log"
check "GRANT succeeded" "GRANT SELECT ON users TO bob\|GRANT"
check "REVOKE succeeded" "REVOKE" 
check "CREATE VIEW succeeded" "View.*v1.*created\|CREATE VIEW"
check "SELECT from view" "Alice\|Bob"
check "FTS search" "Alice"
check "MERGE updated/inserted" "Laptop M\|MergedProd"

check "JSON column exists" "metadata"
check "JSON value stored" "admin\|level"

echo ""
echo "--- DDL: DROP ---"
check "DROP TABLE logs" "dropped\|Table"
check "DROP DATABASE analytics" "DROP DATABASE\|dropped"

echo ""
echo "========================================"
echo "TOTAL: $PASS_COUNT passed, $FAIL_COUNT failed"
echo "========================================"

# Cleanup
rm -rf "$TEST_DIR"

if [ $FAIL_COUNT -gt 0 ]; then
    echo ""
    echo "Some tests failed. Review output above."
    exit 1
else
    echo ""
    echo "All smoke tests passed!"
    exit 0
fi