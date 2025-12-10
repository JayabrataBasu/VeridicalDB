# VeridicalDB Design Document

**Version:** 0.1.0-beta  
**Last Updated:** December 2025  
**Author:** Jayabrata Basu  
**Repository:** https://github.com/JayabrataBasu/VeridicalDB

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Completed Features](#completed-features)
4. [Package Reference](#package-reference)
5. [How to Build & Run](#how-to-build--run)
6. [Feature Roadmap](#feature-roadmap)
7. [Implementation Guidelines](#implementation-guidelines)
8. [Testing](#testing)
9. [Known Issues & Technical Debt](#known-issues--technical-debt)

---

## Project Overview

VeridicalDB is a modern, embeddable database engine written in Go. It aims to provide:

- **Row-based and columnar storage** for different workloads
- **MVCC (Multi-Version Concurrency Control)** for snapshot isolation
- **SQL query support** with a hand-written parser and executor
- **B-tree indexes** for efficient lookups
- **Write-Ahead Logging (WAL)** for durability
- **Interactive REPL** for direct SQL execution

### Tech Stack

- **Language:** Go 1.23+
- **Dependencies:** 
  - `github.com/chzyer/readline` - REPL line editing
  - `github.com/spf13/cobra` - CLI framework
  - `github.com/spf13/viper` - Configuration
  - `go.uber.org/zap` - Structured logging
- **Codebase Size:** ~22,000 lines of Go code
- **Test Coverage:** All packages have passing tests

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        CLI / REPL                                │
│                   (cmd/veridicaldb, internal/cli)                │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                      SQL Layer (pkg/sql)                         │
│  ┌─────────┐  ┌─────────┐  ┌──────────┐  ┌─────────────────┐   │
│  │  Lexer  │→ │ Parser  │→ │ Planner  │→ │ Executor/MVCC   │   │
│  └─────────┘  └─────────┘  └──────────┘  └─────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Catalog & Schema (pkg/catalog)                 │
│         TableManager │ MVCCTableManager │ Schema │ Types         │
└─────────────────────────────────────────────────────────────────┘
                                │
                ┌───────────────┼───────────────┐
                ▼               ▼               ▼
┌───────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│  Storage Layer    │ │   Index Layer   │ │  Transaction    │
│   (pkg/storage)   │ │   (pkg/btree)   │ │   (pkg/txn)     │
│  Row │ Columnar   │ │  B-tree Index   │ │  MVCC Manager   │
└───────────────────┘ └─────────────────┘ └─────────────────┘
        │                                         │
        ▼                                         ▼
┌───────────────────┐                   ┌─────────────────┐
│   WAL (pkg/wal)   │                   │ Lock (pkg/lock) │
│  Write-Ahead Log  │                   │  Lock Manager   │
└───────────────────┘                   └─────────────────┘
```

### Data Flow

1. **Input:** SQL string enters via REPL or (future) network protocol
2. **Lexer:** Tokenizes SQL into tokens (keywords, identifiers, literals)
3. **Parser:** Builds AST (Abstract Syntax Tree) from tokens
4. **Planner:** (Optional) Optimizes query, chooses indexes
5. **Executor:** Executes AST against storage via TableManager
6. **Storage:** Reads/writes rows, manages pages
7. **WAL:** Logs changes for durability before commit
8. **Result:** Returns rows/message to caller

---

## Completed Features

### ✅ SQL Parser & Lexer (`pkg/sql/lexer.go`, `pkg/sql/parser.go`)

A hand-written recursive descent parser supporting:

| Statement | Syntax | Status |
|-----------|--------|--------|
| CREATE TABLE | `CREATE TABLE name (col TYPE [NOT NULL] [PRIMARY KEY], ...)` | ✅ Works |
| DROP TABLE | `DROP TABLE name` | ✅ Works |
| INSERT | `INSERT INTO table [(cols)] VALUES (...)` | ✅ Works |
| SELECT | `SELECT cols FROM table [WHERE condition]` | ✅ Works |
| UPDATE | `UPDATE table SET col=val [WHERE condition]` | ✅ Works |
| DELETE | `DELETE FROM table [WHERE condition]` | ✅ Works |
| CREATE INDEX | `CREATE [UNIQUE] INDEX name ON table (cols)` | ✅ Works |
| DROP INDEX | `DROP INDEX name` | ✅ Works |
| BEGIN | `BEGIN` | ✅ Works |
| COMMIT | `COMMIT` | ✅ Works |
| ROLLBACK | `ROLLBACK` | ✅ Works |

**Supported Data Types:**
- `INT` / `INTEGER` / `INT32` → 32-bit integer
- `BIGINT` / `INT64` → 64-bit integer
- `TEXT` / `STRING` / `VARCHAR` → Variable-length string
- `BOOL` / `BOOLEAN` → Boolean
- `TIMESTAMP` / `DATETIME` → Date/time

**Supported WHERE Operators:**
- Comparison: `=`, `<>`, `!=`, `<`, `>`, `<=`, `>=`
- Logical: `AND`, `OR`, `NOT`
- Literals: integers, strings (single quotes), `TRUE`, `FALSE`, `NULL`

### ✅ Storage Engine (`pkg/storage/`)

**Row-Based Storage (`storage.go`):**
- Page-based storage (configurable page size, default 4KB)
- Slotted page format for variable-length rows
- Row encoding/decoding with null bitmap
- Table-level file management

**Columnar Storage (`columnar.go`):**
- Column-oriented storage for analytics workloads
- Separate files per column
- Efficient for column scans and aggregations
- Created via `CREATE TABLE ... WITH STORAGE = COLUMN`

### ✅ Catalog & Schema (`pkg/catalog/`)

- **Catalog** (`catalog.go`): Persists table metadata to JSON, tracks table IDs
- **TableManager** (`table_manager.go`): High-level CRUD operations on tables
- **MVCCTableManager** (`mvcc_table_manager.go`): MVCC-aware table operations
- **Schema** (`types.go`): Column definitions, type validation
- **Encoding** (`encoding.go`): Row serialization with null bitmap

### ✅ B-Tree Indexes (`pkg/btree/`)

- In-memory B-tree implementation
- Supports single and composite keys
- Key encoding for proper sort order (integers, strings)
- `IndexManager` for managing multiple indexes per table
- Unique index support
- Persistence to disk (JSON-based for simplicity)

### ✅ MVCC & Transactions (`pkg/txn/`)

- **Transaction Manager**: Assigns transaction IDs, tracks active transactions
- **MVCC Rows**: Each row has `CreatedBy`, `DeletedBy` transaction IDs
- **Snapshot Isolation**: Transactions see consistent snapshot at start time
- **Visibility Rules**: Proper MVCC visibility checking for reads
- **Autocommit Mode**: Single statements auto-commit by default

### ✅ Lock Manager (`pkg/lock/`)

- Row-level locking (shared/exclusive modes)
- Lock acquisition with timeout
- Deadlock prevention via timeout (no detection yet)
- Lock release on transaction end

### ✅ Write-Ahead Log (`pkg/wal/`)

- Append-only log file
- Log records for INSERT, UPDATE, DELETE
- Sync-on-commit for durability
- Log sequence numbers (LSN) for ordering
- Basic recovery scanning

### ✅ Query Planner (`pkg/sql/planner.go`)

- Index selection for WHERE clauses
- Simple cost-based decisions
- Falls back to table scan when no index available

### ✅ Session Management (`pkg/sql/session.go`)

- Per-connection session state
- Transaction lifecycle management
- Optional lock manager integration
- Optional index manager integration

### ✅ CLI & REPL (`internal/cli/`, `cmd/veridicaldb/`)

- Interactive SQL shell with readline support
- Command history (persisted to `~/.veridicaldb_history`)
- Tab completion for SQL keywords
- Backslash commands: `\dt`, `\d table`, `\status`, `\config`, `\q`
- Multi-line statement support (continues until `;`)
- Pretty-printed table output

### ✅ Configuration (`internal/config/`, `pkg/config/`)

- YAML configuration file support
- Command-line flags via Cobra
- Environment variable overrides via Viper
- Sensible defaults for all settings

### ✅ Logging (`internal/logger/`, `pkg/log/`)

- Structured logging with Zap
- Configurable log levels
- JSON or console output formats

### ✅ Observability (`pkg/observability/`)

- System catalog queries
- Active transaction listing
- Lock information retrieval
- Basic server status

### ✅ Network Protocol (`pkg/net/`)

- Basic TCP server framework
- Request/response message types
- Connection handling (foundation for future wire protocol)

### ✅ Sharding (`pkg/shard/`)

- Consistent hash ring implementation
- Shard assignment logic
- Foundation for distributed queries

---

## Package Reference

| Package | Purpose | Key Files |
|---------|---------|-----------|
| `cmd/veridicaldb` | Main CLI entry point | `main.go` |
| `cmd/server` | Standalone server (WIP) | `main.go` |
| `internal/cli` | REPL implementation | `repl.go` |
| `internal/config` | Configuration loading | `config.go` |
| `internal/logger` | Zap logger wrapper | `logger.go` |
| `pkg/btree` | B-tree index implementation | `btree.go`, `index.go` |
| `pkg/catalog` | Schema and table metadata | `catalog.go`, `table_manager.go`, `mvcc_table_manager.go` |
| `pkg/cli` | Shared CLI utilities | `repl.go` |
| `pkg/config` | Shared config types | `config.go` |
| `pkg/lock` | Lock manager | `lock.go` |
| `pkg/log` | Shared logging | `log.go` |
| `pkg/net` | Network protocol | `server.go`, `protocol.go` |
| `pkg/observability` | System monitoring | `system_catalog.go` |
| `pkg/shard` | Sharding utilities | `consistent_hash.go` |
| `pkg/sql` | SQL parser & executor | `lexer.go`, `parser.go`, `executor.go`, `mvcc_executor.go`, `session.go`, `planner.go` |
| `pkg/storage` | Storage engines | `storage.go`, `columnar.go` |
| `pkg/txn` | Transaction management | `txn.go` |
| `pkg/wal` | Write-ahead logging | `wal.go` |

---

## How to Build & Run

### Prerequisites

- Go 1.23 or later
- Git

### Build from Source

```bash
# Clone the repository
git clone https://github.com/JayabrataBasu/VeridicalDB.git
cd VeridicalDB

# Build the binary
go build -o veridicaldb ./cmd/veridicaldb

# Initialize a new database
./veridicaldb init

# Start the REPL
./veridicaldb
```

### Run Tests

```bash
# Run all tests
go test ./...

# Run tests with verbose output
go test -v ./...

# Run tests for a specific package
go test -v ./pkg/sql/...

# Run with race detection
go test -race ./...
```

### Build Release Artifacts

```bash
# Build for all platforms (Linux, Windows, macOS)
./scripts/release.sh v0.1.0-beta

# Output in build/release/
ls build/release/
# veridicaldb-linux.tar.gz
# veridicaldb-windows.zip
# veridicaldb-mac-silicon.tar.gz
# SHA256SUMS
```

---

## Feature Roadmap

### Phase 1: SQL Completeness (Priority: HIGH)

These features are essential for basic SQL usability.

| Feature | Difficulty | Description | Files to Modify |
|---------|------------|-------------|-----------------|
| **ORDER BY** | Medium | Sort query results | `pkg/sql/parser.go`, `pkg/sql/executor.go`, `pkg/sql/ast.go` |
| **LIMIT/OFFSET** | Easy | Pagination | Same as ORDER BY |
| **COUNT/SUM/AVG/MIN/MAX** | Medium | Aggregate functions | Parser + new aggregate evaluation in executor |
| **GROUP BY** | Medium | Grouping for aggregates | Parser + executor grouping logic |
| **HAVING** | Easy | Filter on aggregates | Same as GROUP BY |
| **DISTINCT** | Easy | Eliminate duplicates | Executor deduplication |
| **INNER JOIN** | Hard | Join two tables | Parser + new join executor |
| **LEFT/RIGHT JOIN** | Hard | Outer joins | Extension of INNER JOIN |
| **Subqueries** | Hard | Nested SELECT | Parser recursion + executor nesting |

**Implementation Notes:**
- Start with ORDER BY + LIMIT as they're the most requested
- Aggregates require a new evaluation phase after scanning
- JOINs need careful memory management for large tables

### Phase 2: Data Integrity (Priority: HIGH)

These ensure data correctness and consistency.

| Feature | Difficulty | Description | Files to Modify |
|---------|------------|-------------|-----------------|
| **PRIMARY KEY enforcement** | Medium | Auto-create unique index, enforce on INSERT | `pkg/catalog/catalog.go`, `pkg/sql/executor.go` |
| **UNIQUE constraints** | Medium | Candidate keys | Similar to PRIMARY KEY |
| **FOREIGN KEY** | Hard | Referential integrity | Parser + catalog + executor checks |
| **CHECK constraints** | Medium | Boolean conditions | Parser + executor validation |
| **DEFAULT values** | Easy | Default on INSERT | Parser + executor |
| **AUTO_INCREMENT** | Medium | Auto-generated IDs | Catalog + executor |

**Implementation Notes:**
- PRIMARY KEY: Store in `TableMeta`, auto-create index in `CreateTable`, check in `executeInsert`
- FOREIGN KEY: New `ForeignKey` struct in catalog, check referenced row exists on INSERT, handle ON DELETE/UPDATE

### Phase 3: Query Power (Priority: MEDIUM)

Advanced query capabilities.

| Feature | Difficulty | Description |
|---------|------------|-------------|
| **IN / NOT IN** | Easy | Set membership |
| **BETWEEN** | Easy | Range check |
| **LIKE / ILIKE** | Medium | Pattern matching |
| **CASE WHEN** | Medium | Conditional expressions |
| **COALESCE / NULLIF** | Easy | NULL handling |
| **String functions** | Medium | CONCAT, SUBSTR, LENGTH, UPPER, LOWER |
| **Date functions** | Medium | NOW, DATE_ADD, EXTRACT |
| **Arithmetic in expressions** | Easy | +, -, *, / in SELECT |
| **Column aliases (AS)** | Easy | `SELECT col AS name` |
| **Table aliases** | Easy | `SELECT t.col FROM table t` |

### Phase 4: DDL & Schema (Priority: MEDIUM)

Schema modification capabilities.

| Feature | Difficulty | Description |
|---------|------------|-------------|
| **ALTER TABLE ADD COLUMN** | Medium | Add new columns |
| **ALTER TABLE DROP COLUMN** | Medium | Remove columns |
| **ALTER TABLE RENAME** | Easy | Rename table |
| **TRUNCATE TABLE** | Easy | Fast table clear |
| **CREATE VIEW** | Medium | Virtual tables |
| **SHOW TABLES** | Easy | List tables (SQL syntax) |
| **SHOW CREATE TABLE** | Easy | Show DDL |
| **Information schema** | Medium | SQL-standard metadata |

### Phase 5: Production Readiness (Priority: HIGH)

Required for real deployments.

| Feature | Difficulty | Description |
|---------|------------|-------------|
| **Crash recovery** | Hard | Replay WAL on startup |
| **Checkpointing** | Medium | Compact WAL periodically |
| **PostgreSQL wire protocol** | Hard | Compatible with psql, drivers |
| **Authentication** | Medium | Username/password |
| **TLS/SSL** | Medium | Encrypted connections |
| **Connection pooling** | Medium | Efficient connection reuse |
| **Prepared statements** | Medium | Parameterized queries |
| **EXPLAIN** | Medium | Query plan visualization |

### Phase 6: Advanced Features (Priority: LOW)

Nice-to-have for competitive advantage.

| Feature | Difficulty | Description |
|---------|------------|-------------|
| **Window functions** | Hard | ROW_NUMBER, RANK, LAG, LEAD |
| **CTEs (WITH clause)** | Hard | Common table expressions |
| **UNION/INTERSECT/EXCEPT** | Medium | Set operations |
| **Full-text search** | Hard | Inverted indexes, MATCH |
| **JSON data type** | Hard | JSON storage and queries |
| **Partitioning** | Hard | Range/hash partitions |
| **Replication** | Very Hard | Primary-replica sync |
| **Distributed queries** | Very Hard | Cross-shard queries |

---

## Implementation Guidelines

### Adding a New SQL Statement

1. **Add tokens** to `pkg/sql/lexer.go` if new keywords needed
2. **Add AST node** to `pkg/sql/ast.go`:
   ```go
   type NewStmt struct {
       Field1 string
       Field2 Expression
   }
   func (s *NewStmt) statementNode() {}
   ```
3. **Add parser method** to `pkg/sql/parser.go`:
   ```go
   func (p *Parser) parseNew() (*NewStmt, error) {
       // Parse syntax
   }
   ```
4. **Add to Parse()** switch in `parser.go`
5. **Add executor method** to `pkg/sql/executor.go`:
   ```go
   func (e *Executor) executeNew(stmt *NewStmt) (*Result, error) {
       // Execute logic
   }
   ```
6. **Add to Execute()** switch in `executor.go`
7. **Write tests** in `pkg/sql/sql_test.go`

### Adding a New Data Type

1. Add constant to `pkg/catalog/types.go`:
   ```go
   const TypeNewType DataType = ...
   ```
2. Update `String()` and `ParseDataType()` in `types.go`
3. Update `IsFixedWidth()` and `FixedWidth()` if applicable
4. Add to `Value` struct and create `NewXxx()` constructor
5. Update `EncodeRow()` and `DecodeRow()` in `encoding.go`
6. Update lexer if new literal syntax needed

### Code Style

- Use `go fmt` for formatting
- Run `go vet` before committing
- Keep functions under 50 lines when possible
- Write table-driven tests
- Document exported functions
- Use meaningful variable names

---

## Testing

### Test Structure

Each package has `*_test.go` files with:
- Unit tests for individual functions
- Integration tests for combined functionality
- Table-driven tests for multiple cases

### Running Specific Tests

```bash
# Run a specific test
go test -v -run TestCreateTable ./pkg/sql/

# Run tests matching pattern
go test -v -run ".*Insert.*" ./pkg/sql/

# Run with coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

### Test Database Setup

Tests use temporary directories:
```go
dir := t.TempDir()
tm, _ := catalog.NewTableManager(dir, 4096)
// ... test code
// Cleanup automatic via t.TempDir()
```

---

## Known Issues & Technical Debt

### Current Limitations

1. **No deadlock detection** - Transactions may hang indefinitely on deadlock (timeout only)
2. **In-memory indexes** - B-tree indexes rebuilt on startup (persistence is basic JSON)
3. **Single-threaded executor** - No parallel query execution
4. **No query cache** - Parsed queries not cached
5. **Limited error messages** - Some errors lack context
6. **No VACUUM/compaction** - Deleted rows not reclaimed automatically

### Technical Debt

1. **Duplicate code** - `pkg/cli` and `internal/cli` have overlap
2. **Inconsistent error handling** - Some functions return errors, some panic
3. **Missing validation** - Some edge cases not handled
4. **Hardcoded limits** - Some buffer sizes are hardcoded

### Future Refactoring

1. Consolidate `pkg/cli` and `internal/cli`
2. Add context.Context for cancellation
3. Implement proper buffer pool management
4. Add metrics collection throughout
5. Standardize error types

---

## Contact & Resources

- **Repository:** https://github.com/JayabrataBasu/VeridicalDB
- **Issues:** https://github.com/JayabrataBasu/VeridicalDB/issues
- **Original Author:** Jayabrata Basu

### Useful References

- [SQLite Architecture](https://www.sqlite.org/arch.html)
- [PostgreSQL Internals](https://www.interdb.jp/pg/)
- [CMU Database Systems Course](https://15445.courses.cs.cmu.edu/)
- [Let's Build a Simple Database](https://cstack.github.io/db_tutorial/)

---

*This document should be updated as features are added or architecture changes.*

## Multi-Database Support (Namespaces)

### Goals

- Provide first-class support for multiple logical databases (namespaces) within a VeridicalDB instance.
- Allow users to `CREATE DATABASE`, `DROP DATABASE`, and `USE <name>` to switch the current working database.
- Ensure tables, indexes, WAL segments and catalog metadata are scoped by database.
- Keep cross-database operations explicit and safe (no implicit cross-db FK references).

### High-level concepts

- Database (namespace): a logical container for tables, indexes, and other schema objects. Each database has independent schema and storage files.
- System catalog: adds a `pg_database`-like table recording known databases and metadata (owner, encoding, creation time, location).
- Current database (session-level): each `Session` tracks the active database used to resolve unqualified table names.
- Qualified names: allow `db.schema.table` or `db.table` notation in SQL (initially `db.table`).

### File and storage layout

Use a simple directory-per-database layout under the engine's `data` directory (configurable):

- `data/` (engine root)
   - `pg_catalog/` (system catalog files shared across instance)
   - `db1/` (database `db1` data directory)
      - `tables/` (table data files)
      - `indexes/` (index files)
      - `wal/` (per-database WAL segments or instance-wide WAL with DB-scoped records)
      - `meta.json` (database metadata)
   - `db2/`

Notes:
- Using directory-per-db keeps backups and file-per-table simple.
- WAL can be per-database (simpler) or instance-wide with a DB field in records. Start with per-database WAL for simplicity.

### System catalog additions

Add a database-level system catalog table, e.g., `pg_database`, stored under `pg_catalog/`:

Columns:
- `datname` (TEXT) — database name (unique)
- `datdba` (TEXT) — owner
- `datpath` (TEXT) — filesystem path (relative to data root)
- `datencoding` (TEXT) — encoding (UTF-8 default)
- `datcreated` (TIMESTAMP)
- `datconfig` (JSON) — optional DB-level config

`pg_database` is read at engine start to populate known databases and to validate `CREATE/DROP` operations.

### Parser & grammar

Add statements to the SQL grammar and parser:

- `CREATE DATABASE name [WITH OWNER = ident]` — create a new database directory and register in `pg_database`.
- `DROP DATABASE name` — remove an empty database (or require `IF EXISTS` and an explicit `FORCE` flag later).
- `USE name` — change session current database (alternative: `SET DATABASE = name`).

Parser AST nodes:

```go
type CreateDatabaseStmt struct {
      Name  string
      Owner string // optional
}

type DropDatabaseStmt struct {
      Name string
      IfExists bool
}

type UseDatabaseStmt struct {
      Name string
}
```

Add parse methods in `pkg/sql/parser.go` and hook into the main Parse() dispatch.

### Executor semantics

- `CREATE DATABASE`:
   - Validate name and that a directory with that name does not already exist.
   - Create a new directory `data/<name>/` with the required subfolders (`tables`, `indexes`, `wal`) and write `meta.json`.
   - Insert a row into `pg_catalog.pg_database` (persisted to `pg_catalog/` storage).
   - Return success.

- `DROP DATABASE`:
   - Ensure database is not the current database for any active session (reject if in use).
   - Optionally require directory to be empty or `FORCE` to remove files — start by refusing to drop non-empty DBs.
   - Remove entry from `pg_database` and optionally delete files (configurable safety).

- `USE` / `SET DATABASE`:
   - Validate the database exists in `pg_database`.
   - Update `Session.CurrentDatabase` to the target.
   - Future unqualified table lookups resolve against `Session.CurrentDatabase`.

Name resolution rules:
- Unqualified name `table` resolves to `current_db.table`.
- Qualified name `db.table` resolves to the named database regardless of session.

Cross-database operations & constraints:
- Do not allow foreign keys that reference tables in another database in the first implementation.
- Disallow `CREATE INDEX` or DDL that mixes databases unless fully qualified and explicitly supported later.

### Catalog API changes (suggested)

Extend `pkg/catalog` with database-aware APIs. Example additions:

```go
// Catalog is the instance-level catalog for the engine
type Catalog struct {
      RootPath string // data root
      SysPath  string // pg_catalog path
      // ... existing fields
}

// DatabaseInfo describes a database
type DatabaseInfo struct {
      Name     string
      Owner    string
      Path     string
      Encoding string
      Created  time.Time
}

func (c *Catalog) CreateDatabase(ctx context.Context, name, owner string) (*DatabaseInfo, error)
func (c *Catalog) DropDatabase(ctx context.Context, name string, force bool) error
func (c *Catalog) ListDatabases(ctx context.Context) ([]DatabaseInfo, error)
func (c *Catalog) GetDatabase(ctx context.Context, name string) (*DatabaseInfo, error)
```

Table managers remain per-database: `catalog.NewTableManager(dbPath)` so the session will create/use table managers for `Session.CurrentDatabase`.

### Session changes

Extend `pkg/sql/session.go`:

- Add `CurrentDatabase string` to `Session` struct (persisted only in-memory).
- When a session is created, set `CurrentDatabase` to a configured default (e.g., `default` or `postgres`), or `""` meaning no DB selected — REPL should call `CREATE DATABASE`/`USE` as needed.
- Table resolution in executor should use `session.ResolveTable(name)` that returns the appropriate `TableManager` for the session's current DB.

### REPL & CLI UX

- Add backslash and SQL commands:
   - SQL: `CREATE DATABASE testdb;` `USE testdb;` `DROP DATABASE testdb;`
   - REPL: `\c testdb` as alias for `USE testdb`
- Display current database in REPL prompt, e.g., `veridicaldb(testdb)=>`

### WAL & MVCC considerations

- Start with per-database WAL directories to avoid adding DB field to existing WAL record format.
- Each database's WAL is replayed when that DB is opened.
- Ensure `CreateDatabase` initializes WAL and any checkpoint files.

### Concurrency & safety

- Ensure `CreateDatabase` and `DropDatabase` take an instance-wide catalog lock (serialize catalog changes).
- Prevent dropping a database while sessions are connected to it.

### Backups & restore

- With directory-per-db, backups can simply copy `data/<db>/` for a physical backup.
- Document recommended steps to snapshot a DB safely (flush WAL, checkpoint, copy files).

### Testing

- Unit tests for parser: `TestParseCreateDatabase`, `TestParseUseDatabase`, `TestParseDropDatabase`.
- Integration tests:
   - `TestCreateUseDropDatabaseLifecycle` — create DB, use it, create table, insert rows, drop DB (fail while in use), drop after closing sessions.
   - `TestNamespaceIsolation` — create same table name in two DBs and ensure they don't interfere.

### Security / Privileges (future)

- Database ownership and privileges: record owner in `pg_database` and check `CREATE/DROP` privileges as future work.

### Migration & backward compatibility

- Existing single-data-directory setups should be migrated to the new layout on first engine start: move existing tables into `data/default/` and register `default` in `pg_database`.
- Provide a migration utility or automatic one-time migration step guarded by a flag.

### Example SQL

```sql
CREATE DATABASE demo WITH OWNER = 'alice';
USE demo;
CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
INSERT INTO users VALUES (1, 'alice');
SELECT * FROM users;
\c otherdb  -- REPL alias to switch
```

### Implementation plan & estimate

1. **Design & docs** (this section) — done (1-2 hours to review).  
2. **Parser & AST** — add statements and tests (2-4 hours).  
3. **Catalog API & syscatalog** — implement `pg_database`, filesystem layout, migration (4-8 hours).  
4. **Executor support** — implement handlers for `CREATE/DROP/USE` (2-4 hours).  
5. **Session & TableManager wiring** — ensure per-session table resolution (3-6 hours).  
6. **REPL UX** — prompt and `\c` support (1-2 hours).  
7. **Tests & integration** — add unit and integration tests (3-6 hours).  
8. **Migration tests & docs** — manual/automated migration steps (2-4 hours).

Total rough estimate: 2–4 days of focused work to implement and test a solid per-database namespace feature.

### Open questions / choices (for you to decide)

- WAL strategy: per-database WAL (simpler) vs instance-wide WAL with DB-scoped records (more complex but centralizes recovery). Recommendation: start per-database.
- Default database name: create `default` or `postgres` automatically during migration/startup. Recommendation: create `default` to minimize surprises.
- Force drop: implement a `DROP DATABASE name FORCE` later to remove files forcibly; initially refuse to drop non-empty DBs.

---

*End of multi-database design additions.*
