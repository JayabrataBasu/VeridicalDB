# VeridicalDB: Remaining Features Implementation Plan

**Created:** December 13, 2025  
**Updated:** December 15, 2025
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
- **Status:** ✅ Fully implemented
- **Completed:**
  - Lexer, Parser, Catalog, and Executor support for Foreign Key constraints.
  - Inline `REFERENCES table(col)` syntax and table-level `FOREIGN KEY (cols) REFERENCES table(cols)` parsing.
  - Enforcement on INSERT/UPDATE to validate referenced rows and on DELETE/UPDATE to prevent violations (RESTRICT behavior).
  - Tests: `TestForeignKeyConstraints` in `pkg/sql/foreign_key_test.go`.
- **Files Modified:**
  - `pkg/sql/lexer.go` - Added `TOKEN_REFERENCES`, `TOKEN_FOREIGN`
  - `pkg/sql/ast.go` - Added `ForeignKeyDef` struct and related AST updates
  - `pkg/sql/parser.go` - Parsing support for FK syntax
  - `pkg/catalog/types.go` - FK metadata in schema
  - `pkg/sql/executor.go` - Enforcement logic on data-modifying statements
**Difficulty:** Hard
**Completed Date:** Dec 15, 2025

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
- **Status:** ✅ Fully implemented
- **Completed:**
  - Parser: Handles `$1`, `$2` placeholders and parses `PREPARE`/`EXECUTE`/`DEALLOCATE`.
  - Session: `Session` stores prepared statements by name and parameters.
  - Executor: Parameters are bound at execution time and used by the planner/executor.
- **Files Modified:**
  - `pkg/sql/parser.go`, `pkg/sql/session.go`, `pkg/sql/executor.go`, `pkg/sql/prepared_stmt_test.go`
- **Tests:** `TestPreparedStatements` and `TestPreparedInsert` in `pkg/sql/prepared_stmt_test.go`.
- **Completed Date:** Dec 15, 2025

### 3.3 Crash Recovery (WAL Replay)
- **Status:** ✅ Fully implemented
- **Completed:**
  - Page-level deterministic replay via `insertTupleAt` in `pkg/storage/heap_page.go`.
  - Storage replay methods: `ReplayInsert`, `ReplayDelete`, `ReplayUpdate` in `pkg/storage/storage.go`.
  - `TableManager.StartRecovery()` wires `wal.Recovery` with Redo/Undo handlers to apply/rollback records on startup.
  - MVCC layer writes WAL records via `TxnLogger` to capture old/new row images for redo/undo.
- **Tests:** `TestCrashRecoveryIntegration` in `pkg/catalog/recovery_test.go`.
- **Completed Date:** Dec 15, 2025

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
- **Status:** ✅ Fully implemented
- **Completed:**
  - Implemented `pkg/pgwire` with `protocol.go` and `server.go` for the PostgreSQL 3.0 wire protocol.
  - Startup/auth handshake implemented (SSL rejection, AuthenticationOK), ParameterStatus and BackendKeyData sent on startup.
  - Simple Query protocol (`Q`) executed via the existing SQL executor; RowDescription/DataRow/CommandComplete messages implemented.
  - Extended Query protocol implemented with `Parse`/`Bind`/`Describe`/`Execute`/`Sync`/`Close` message handling (basic features and tests).
  - Integrated into `cmd/server/main.go` to run as a pgwire server in non-interactive mode.
- **Tests:** `pkg/pgwire/pgwire_test.go` (startup, SSL request handling, simple query execution, message encoding/decoding).
- **Completed Date:** Dec 15, 2025

### 3.6 User Authentication
- **Status:** ✅ Fully implemented
- **Completed:**
  - User catalog in `pkg/auth/auth.go` with `User` struct and `UserCatalog` management.
  - Password hashing with SHA256 and random salt.
  - Session authentication via `Session.Authenticate()` method.
  - GRANT/REVOKE privilege management for per-table access control.
  - Superuser role with automatic all-privileges access.
  - Default admin user created on first initialization.
  - JSON persistence for user data.
- **Syntax:**
  ```sql
  CREATE USER alice WITH PASSWORD 'secret';
  CREATE USER admin WITH PASSWORD 'pass' SUPERUSER;
  DROP USER [IF EXISTS] alice;
  ALTER USER alice WITH PASSWORD 'newsecret';
  ALTER USER alice WITH SUPERUSER;
  ALTER USER alice WITH NOSUPERUSER;
  GRANT SELECT ON table TO alice;
  GRANT ALL ON table TO alice;
  REVOKE INSERT ON table FROM alice;
  ```
- **Files Modified:**
  - `pkg/sql/lexer.go` - Added USER, PASSWORD, GRANT, REVOKE, SUPERUSER tokens.
  - `pkg/sql/ast.go` - Added CreateUserStmt, DropUserStmt, AlterUserStmt, GrantStmt, RevokeStmt.
  - `pkg/sql/parser.go` - Parse user management statements.
  - `pkg/auth/auth.go` - User storage, hashing, authentication, privileges.
  - `pkg/sql/session.go` - Authentication integration, handler methods.
- **Tests:** `TestUserCatalog_*` in `pkg/auth/auth_test.go`.
- **Completed Date:** Dec 16, 2025

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
- **Status:** ✅ Fully implemented
- **Completed:**
  - `CREATE DATABASE [IF NOT EXISTS] name [WITH OWNER = 'owner']`
  - `DROP DATABASE [IF EXISTS] name`
  - `USE database` to switch current database
  - `SHOW DATABASES` to list all databases
  - Directory-per-database layout with tables/, indexes/, wal/, meta.json
  - Session-level current database tracking (`currentDatabase` field)
  - DatabaseManager in `pkg/catalog/database_manager.go` with persistence
  - Default database created on first initialization
- **Files Modified:**
  - `pkg/sql/lexer.go` - Added DATABASE, USE, OWNER tokens
  - `pkg/sql/ast.go` - Added CreateDatabaseStmt, DropDatabaseStmt, UseDatabaseStmt, ShowDatabasesStmt
  - `pkg/sql/parser.go` - Parse database DDL statements
  - `pkg/catalog/database_manager.go` - New file for multi-database management
  - `pkg/sql/session.go` - Added dbMgr, currentDatabase, handlers
- **Tests:** `TestDatabaseManager_*` in `pkg/catalog/database_manager_test.go`, `TestParseDatabaseStatements` in `pkg/sql/database_test.go`
- **Completed Date:** Dec 15, 2025

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
| Recursive CTEs | ✅ Complete | Dec 2025 |
| View Execution | ✅ Complete | Dec 2025 |
| Window Frame Execution | ✅ Complete | Dec 2025 |
| NTH_VALUE() | ✅ Complete | Dec 2025 |

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
| Prepared Statements | ✅ Complete | Dec 15, 2025 |
| Crash Recovery | ✅ Complete | Dec 15, 2025 |
| Checkpointing | ✅ Complete | Dec 2025 |
| PostgreSQL Wire Protocol | ✅ Complete | Dec 15, 2025 |
| User Authentication | ✅ Complete | Dec 16, 2025 |

### Low Priority
| Feature | Status | Completed Date |
|---------|--------|----------------|
| JSON Data Type | ⬜ Not Started | |
| Full-Text Search | ⬜ Not Started | |
| Table Partitioning | ⬜ Not Started | |
| Triggers | ⬜ Not Started | |
| Stored Procedures | ⬜ Not Started | |
| Multi-Database | ✅ Complete | Dec 15, 2025 |
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
  - [x] 3.2 Prepared Statements ✅ (Dec 15, 2025)
  - [x] 3.6 User Authentication ✅ (Dec 15, 2025)
  - [x] 3.3 Crash Recovery ✅ (Dec 15, 2025)
  - [x] 3.5 PostgreSQL Wire Protocol ✅ (Dec 15, 2025)

4. **Phase 4: Low Priority Features**
   - [x] 4.6 Multi-Database Namespaces ✅ (Dec 15, 2025)
   - [ ] 4.4 Triggers
   - [ ] 4.1 JSON Data Type
   - [ ] 4.3 Table Partitioning
   - [ ] 4.2 Full-Text Search
   - [ ] 4.5 Stored Procedures
   - [ ] 4.7 Replication

---

*Update this file as features are completed. Mark items with ✅ when done.*
