# VeridicalDB: Remaining Features Implementation Plan

**Created:** December 13, 2025  
**Purpose:** Track and complete all remaining SQL features in priority order

---

## Table of Contents

1. [Partially Implemented Features](#1-partially-implemented-features)
2. [High Priority Features](#2-high-priority-features)
3. [Medium Priority Features](#3-medium-priority-features)
4. [Low Priority Features](#4-low-priority-features)
5. [Progress Tracker](#5-progress-tracker)

---

## 1. Partially Implemented Features

These features have parsing support but incomplete execution. **Complete these first.**

### 1.1 Recursive CTEs ✅ COMPLETED
- **Status:** ✅ Fully implemented
- **Completed:** Iterative fixed-point execution with UNION support
- **Test:** `TestRecursiveCTE` in sql_test.go

### 1.2 View Execution (SELECT FROM view) ✅ COMPLETED
- **Status:** ✅ Fully implemented
- **Completed:** CREATE VIEW, DROP VIEW, SELECT FROM view with WHERE/JOIN
- **Test:** `TestViewExecution` in sql_test.go

### 1.3 Window Frame Execution ✅ COMPLETED
- **Status:** ✅ Fully implemented
- **Completed:** ROWS BETWEEN with all bound types (UNBOUNDED PRECEDING/FOLLOWING, CURRENT ROW, n PRECEDING/FOLLOWING)
- **Functions:** SUM, COUNT, AVG, MIN, MAX, FIRST_VALUE, LAST_VALUE, NTH_VALUE all support frames
- **Test:** `TestWindowFrameExecution` in sql_test.go

### 1.4 NTH_VALUE() Window Function ✅ COMPLETED
- **Status:** ✅ Fully implemented
- **Completed:** NTH_VALUE(col, n) returns nth value within window frame
- **Test:** `TestNthValue` in sql_test.go

---

## 2. High Priority Features

Common SQL features needed for practical use.

### 2.1 FOREIGN KEY Constraints
- **Status:** Not implemented
- **What's Needed:**
  - Parser: `REFERENCES table(column)` in column definition
  - Parser: `FOREIGN KEY (col) REFERENCES table(col)` as table constraint
  - AST: Add `ForeignKey` struct to `ColumnDef` or separate constraint list
  - Catalog: Store foreign key relationships in table metadata
  - Executor: On INSERT/UPDATE, verify referenced row exists
  - Executor: On DELETE/UPDATE of referenced table, check for violations
- **ON DELETE/UPDATE actions (future):**
  - `CASCADE` - Delete/update referencing rows
  - `SET NULL` - Set FK column to NULL
  - `SET DEFAULT` - Set FK column to default
  - `RESTRICT` / `NO ACTION` - Prevent operation
- **Files to Modify:**
  - `pkg/sql/lexer.go` - Add `TOKEN_REFERENCES`, `TOKEN_FOREIGN`
  - `pkg/sql/ast.go` - Add `ForeignKeyDef` struct
  - `pkg/sql/parser.go` - Parse FK syntax
  - `pkg/catalog/types.go` - Add FK to schema
  - `pkg/sql/executor.go` - Enforce FK on INSERT/UPDATE/DELETE
- **Difficulty:** Hard
- **Estimated Time:** 8-12 hours

### 2.2 INSERT ... ON CONFLICT (UPSERT) ✅ COMPLETED
- **Status:** ✅ Fully implemented
- **Completed:** Parser supports ON CONFLICT DO NOTHING and DO UPDATE SET with EXCLUDED.column references, executor detects conflicts and applies updates
- **Syntax:**
  ```sql
  INSERT INTO t (id, name) VALUES (1, 'Alice')
  ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;
  
  INSERT INTO t (id, name) VALUES (1, 'Alice')
  ON CONFLICT DO NOTHING;
  ```
- **Test:** `TestInsertOnConflict` in sql_test.go

### 2.3 Multi-Row INSERT ✅ COMPLETED
- **Status:** ✅ Fully implemented
- **Completed:** Parser supports multiple value tuples, AST uses `ValuesList [][]Expression`, executor loops through all rows
- **Syntax:**
  ```sql
  INSERT INTO t (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');
  ```
- **Test:** `TestMultiRowInsert` in sql_test.go

### 2.4 UPDATE with JOIN / FROM ✅ COMPLETED
- **Status:** ✅ Fully implemented
- **Completed:** Parser supports UPDATE ... FROM ... WHERE syntax, executor joins tables and applies updates with combined schema support
- **Syntax:**
  ```sql
  UPDATE orders SET status = 'shipped'
  FROM customers
  WHERE orders.customer_id = customers.id AND customers.country = 'USA';
  ```
- **Test:** `TestUpdateWithFrom` in sql_test.go

### 2.5 DELETE with USING / JOIN ✅ COMPLETED
- **Status:** ✅ Fully implemented
- **Completed:** Parser supports DELETE ... USING ... WHERE syntax, executor joins tables and applies deletes with combined schema support
- **Syntax:**
  ```sql
  DELETE FROM orders
  USING customers
  WHERE orders.customer_id = customers.id AND customers.status = 'inactive';
  ```
- **Test:** `TestDeleteWithUsing` in sql_test.go

---

### 2.6 Foreign Key Constraints ✅ COMPLETED
- **Status:** ✅ Fully implemented
- **Completed:** Lexer, Parser, Catalog, and Executor support for Foreign Key constraints.
- **Features:**
  - Inline `REFERENCES table(col)` syntax.
  - Table-level `FOREIGN KEY (cols) REFERENCES table(cols)` syntax.
  - Enforcement on INSERT/UPDATE (check referenced row exists).
  - Enforcement on DELETE/UPDATE (check referencing rows do not exist - RESTRICT behavior).
- **Test:** `TestForeignKeyConstraints` in `pkg/sql/foreign_key_test.go`.

## 3. Medium Priority (Enhancements)


Production readiness and tooling compatibility.

### 3.1 Information Schema ✅ COMPLETED
- **Status:** ✅ Fully implemented
- **Completed:**
  - Virtual table mechanism in `pkg/catalog/system_catalog.go`.
  - `information_schema.tables` and `information_schema.columns` implemented.
  - Integration with `SELECT` queries via `SystemCatalog`.
- **Test:** `TestInformationSchema` in `pkg/observability/system_catalog_test.go`.

### 3.2 Prepared Statements

### 3.2 Prepared Statements
- **Status:** Not implemented
- **What's Needed:**
  - Parser: Handle `$1`, `$2` or `?` placeholders
  - Session: Store prepared statement with name
  - Executor: Bind parameters at execution time
- **Syntax:**
  ```sql
  PREPARE get_user AS SELECT * FROM users WHERE id = $1;
  EXECUTE get_user(42);
  DEALLOCATE get_user;
  ```
- **Files to Modify:**
  - `pkg/sql/lexer.go` - Add placeholder tokens
  - `pkg/sql/ast.go` - Add `PrepareStmt`, `ExecuteStmt`, `DeallocateStmt`
  - `pkg/sql/parser.go` - Parse PREPARE/EXECUTE
  - `pkg/sql/session.go` - Store prepared statements
  - `pkg/sql/executor.go` - Parameter binding
- **Difficulty:** Medium
- **Estimated Time:** 6-8 hours

### 3.3 Crash Recovery (WAL Replay)
- **Status:** WAL exists, recovery not implemented
- **What's Needed:**
  - On startup, scan WAL for uncommitted transactions
  - Replay committed transactions not reflected in data files
  - Handle incomplete transactions (rollback)
- **Files to Modify:**
  - `pkg/wal/recovery.go` - Already exists, enhance
  - `pkg/catalog/table_manager.go` - Call recovery on startup
- **Difficulty:** Hard
- **Estimated Time:** 8-12 hours

### 3.4 Checkpointing ✅ COMPLETED
- **Status:** ✅ Fully implemented
- **Completed:**
  - `Checkpointer` runs in background and triggers periodic checkpoints.
  - `TableManager.Checkpoint()` flushes all dirty pages (columnar buffers).
  - WAL writes `CHECKPOINT_BEGIN` and `CHECKPOINT_END` records.
  - WAL handles magic header to ensure valid LSNs.
  - Integration tests verify checkpoint records and persistence.
- **Test:** `TestCheckpointIntegration` in `pkg/wal/checkpoint_integration_test.go`.

### 3.5 PostgreSQL Wire Protocol
- **Status:** Not implemented (basic TCP framework exists)
- **What's Needed:**
  - Implement PostgreSQL frontend/backend protocol
  - Message types: Query, Parse, Bind, Execute, Sync, etc.
  - Authentication handshake
  - Row description and data row messages
- **Reference:** https://www.postgresql.org/docs/current/protocol.html
- **Files to Modify:**
  - `pkg/net/protocol.go` - New file for PG protocol
  - `pkg/net/server.go` - Use PG protocol handler
- **Difficulty:** Hard
- **Estimated Time:** 16-24 hours

### 3.6 User Authentication
- **Status:** Not implemented
- **What's Needed:**
  - User table in system catalog
  - Password hashing (bcrypt or similar)
  - Session authentication check
  - GRANT/REVOKE for privileges (future)
- **Syntax:**
  ```sql
  CREATE USER alice WITH PASSWORD 'secret';
  DROP USER alice;
  ALTER USER alice WITH PASSWORD 'newsecret';
  ```
- **Files to Modify:**
  - `pkg/sql/lexer.go` - Add USER, PASSWORD, GRANT, REVOKE tokens
  - `pkg/sql/ast.go` - Add user management statements
  - `pkg/sql/parser.go` - Parse user statements
  - `pkg/catalog/` - User storage
  - `pkg/sql/session.go` - Authentication
- **Difficulty:** Medium
- **Estimated Time:** 6-8 hours

---

## 4. Low Priority Features

Advanced features for future enhancement.

### 4.1 JSON Data Type
- **What's Needed:**
  - New `TypeJSON` in type system
  - JSON encoding/decoding in storage
  - Operators: `->` (object field), `->>` (field as text), `@>` (contains)
  - Functions: `json_extract()`, `json_array_length()`, etc.
- **Difficulty:** Hard
- **Estimated Time:** 12-16 hours

### 4.2 Full-Text Search
- **What's Needed:**
  - Inverted index structure
  - Text tokenization and stemming
  - `MATCH ... AGAINST` or `@@` operator
  - Ranking functions
- **Difficulty:** Very Hard
- **Estimated Time:** 20-30 hours

### 4.3 Table Partitioning
- **What's Needed:**
  - Parser: `PARTITION BY RANGE/HASH/LIST`
  - Catalog: Partition metadata
  - Executor: Route queries to correct partitions
  - DDL: `CREATE TABLE ... PARTITION OF`
- **Difficulty:** Hard
- **Estimated Time:** 16-24 hours

### 4.4 Triggers
- **What's Needed:**
  - Parser: `CREATE TRIGGER ... BEFORE/AFTER INSERT/UPDATE/DELETE`
  - Catalog: Trigger storage
  - Executor: Fire triggers at appropriate times
  - Possibly a simple expression language
- **Difficulty:** Hard
- **Estimated Time:** 12-16 hours

### 4.5 Stored Procedures
- **What's Needed:**
  - PL/pgSQL-like language parser
  - Variable declarations, control flow
  - `CREATE PROCEDURE`, `CALL`
- **Difficulty:** Very Hard
- **Estimated Time:** 40+ hours

### 4.6 Multi-Database Namespaces
- **Status:** Designed in DESIGN.md, not implemented
- **What's Needed:**
  - `CREATE DATABASE`, `DROP DATABASE`, `USE database`
  - Directory-per-database layout
  - Session-level current database tracking
- **Files to Modify:** As detailed in DESIGN.md
- **Difficulty:** Medium
- **Estimated Time:** 16-24 hours

### 4.7 Replication
- **What's Needed:**
  - WAL streaming to replicas
  - Primary-replica protocol
  - Failover handling
- **Difficulty:** Very Hard
- **Estimated Time:** 60+ hours

---

## 5. Progress Tracker

### Partially Implemented
| Feature | Status | Completed Date |
|---------|--------|----------------|
| Recursive CTEs | ✅ Complete | Dec 2024 |
| View Execution | ✅ Complete | Dec 2024 |
| Window Frame Execution | ✅ Complete | Dec 2024 |
| NTH_VALUE() | ✅ Complete | Dec 2024 |

### High Priority
| Feature | Status | Completed Date |
|---------|--------|----------------|
| FOREIGN KEY | ✅ Complete | Dec 2025 |
| INSERT ON CONFLICT | ✅ Complete | Dec 2025 |
| Multi-Row INSERT | ✅ Complete | Dec 2025 |
| UPDATE with JOIN | ✅ Complete | Dec 2025 |
| DELETE with USING | ✅ Complete | Dec 2025 |

### Medium Priority
| Feature | Status | Completed Date |
|---------|--------|----------------|
| Information Schema | ✅ Complete | Dec 2025 |
| Prepared Statements | ⬜ Not Started | |
| Crash Recovery | ⬜ Not Started | |
| Checkpointing | ✅ Complete | Dec 2025 |
| PostgreSQL Wire Protocol | ⬜ Not Started | |
| User Authentication | ⬜ Not Started | |

### Low Priority
| Feature | Status | Completed Date |
|---------|--------|----------------|
| JSON Data Type | ⬜ Not Started | |
| Full-Text Search | ⬜ Not Started | |
| Table Partitioning | ⬜ Not Started | |
| Triggers | ⬜ Not Started | |
| Stored Procedures | ⬜ Not Started | |
| Multi-Database | ⬜ Not Started | |
| Replication | ⬜ Not Started | |

---

## Implementation Order

1. **Phase 1: Complete Partial Implementations** ✅ COMPLETED
   - [x] 1.4 NTH_VALUE() ✅
   - [x] 1.2 View Execution ✅
   - [x] 1.1 Recursive CTEs ✅
   - [x] 1.3 Window Frame Execution ✅

2. **Phase 2: High Priority Features**
   - [x] 2.3 Multi-Row INSERT ✅
   - [x] 2.2 INSERT ON CONFLICT ✅
   - [x] 2.4 UPDATE with JOIN ✅
   - [x] 2.5 DELETE with USING ✅
   - [x] 2.1 FOREIGN KEY ✅

3. **Phase 3: Medium Priority Features**
   - [x] 3.1 Information Schema ✅
   - [x] 3.4 Checkpointing ✅
   - [ ] 3.2 Prepared Statements (~6-8 hours)
   - [ ] 3.6 User Authentication (~6-8 hours)
   - [ ] 3.3 Crash Recovery (~8-12 hours)
   - [ ] 3.5 PostgreSQL Wire Protocol (~16-24 hours)

4. **Phase 4: Low Priority Features**
   - (As time and interest permits)

---

*Update this file as features are completed. Mark items with ✅ when done.*
