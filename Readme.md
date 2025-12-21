# VeridicalDB

> *Veridical: truthful, corresponding to facts*

A modern, embeddable database engine built from scratch in Go.

VeridicalDB supports both row-based (heap) and columnar storage, MVCC transactions, SQL queries, and is designed for both embedded and client-server use cases.

## Current Status: Production Ready Features ✅

VeridicalDB has evolved significantly beyond the initial roadmap. The database now supports a comprehensive set of SQL features, advanced storage engines, and enterprise-grade capabilities.

## Features

### 🗄️ Storage Engines
- **Row-based Storage**: Traditional heap storage with page-based organization
- **Columnar Storage**: Efficient analytical queries with column-oriented data layout
- **Hybrid Storage**: Automatic storage selection based on workload patterns

### 🔄 Transactions & Concurrency
- **MVCC (Multi-Version Concurrency Control)**: Non-blocking reads, snapshot isolation
- **ACID Compliance**: Atomicity, Consistency, Isolation, Durability
- **Lock Manager**: Row-level locking with deadlock detection
- **Transaction Isolation Levels**: Read Committed, Repeatable Read, Serializable

### 📊 SQL Support

#### Data Definition Language (DDL)
- `CREATE DATABASE`, `DROP DATABASE`
- `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`
- `CREATE INDEX`, `DROP INDEX`
- `CREATE VIEW`, `DROP VIEW`
- `CREATE USER`, `ALTER USER`, `DROP USER`
- `GRANT`, `REVOKE` permissions

#### Data Manipulation Language (DML)
- `INSERT`, `UPDATE`, `DELETE`
- `UPSERT` (ON CONFLICT DO UPDATE)
- `MERGE` statements
- `TRUNCATE TABLE`

#### Data Query Language (DQL)
- `SELECT` with complex expressions
- `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`
- `GROUP BY`, `HAVING`
- `DISTINCT`
- Subqueries (scalar, correlated, EXISTS)
- Common Table Expressions (CTEs) with `WITH` clause

#### Advanced SQL Features
- **JOINs**: `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `FULL JOIN`
- **Set Operations**: `UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT`
- **Window Functions**: `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, aggregate functions over windows
- **Prepared Statements**: `PREPARE`, `EXECUTE`, `DEALLOCATE`
- **Stored Procedures & Functions**: `CREATE PROCEDURE`, `CREATE FUNCTION`, `CALL`
- **Triggers**: `CREATE TRIGGER`, `DROP TRIGGER` with BEFORE/AFTER timing

### 📋 Data Types
- **Numeric**: `INT`, `BIGINT`, `FLOAT`, `DOUBLE`
- **Text**: `TEXT`, `VARCHAR`
- **Boolean**: `BOOL`
- **Date/Time**: `TIMESTAMP`, `DATE`, `TIME`
- **JSON**: Native JSON support with operators
- **Arrays**: Array types (planned)

### 🔧 Functions & Operators

#### Aggregate Functions
- `COUNT()`, `SUM()`, `AVG()`, `MIN()`, `MAX()`
- Statistical functions (planned)

#### String Functions
- `UPPER()`, `LOWER()`, `LENGTH()`, `CONCAT()`
- `SUBSTRING()`, `TRIM()`, `REPLACE()` (planned)

#### Mathematical Functions
- `ABS()`, `ROUND()`, `FLOOR()`, `CEIL()`
- `SQRT()`, `POWER()`, `LOG()` (planned)

#### Date/Time Functions
- `NOW()`, `CURRENT_TIMESTAMP`, `CURRENT_DATE`
- `EXTRACT()`, `DATE_ADD()`, `DATE_SUB()`
- `YEAR()`, `MONTH()`, `DAY()`, `HOUR()`, `MINUTE()`, `SECOND()`

#### Type Conversion
- `CAST(expression AS type)`
- Implicit type coercion

#### JSON Operators
- JSON column storage and querying
- JSON path operators (planned)

### 🏗️ Indexing & Performance
- **B+ Tree Indexes**: Primary keys, unique constraints, secondary indexes
- **Full-Text Search (FTS)**: `CREATE FULLTEXT INDEX`, `@@` operator
- **Index Types**: B-Tree, Hash (planned), Bitmap (planned)
- **Query Optimizer**: Cost-based optimization with EXPLAIN plans
- **Statistics**: Automatic statistics collection for query planning

### 🔒 Security & Access Control
- **User Management**: Create, alter, drop users with password authentication
- **Role-Based Access Control**: GRANT/REVOKE permissions on databases, tables, views
- **Authentication**: Password-based authentication
- **Authorization**: Table-level and column-level permissions (planned)

### 📈 Observability & Monitoring
- **System Catalog**: `information_schema` tables for metadata queries
- **Performance Metrics**: Query execution statistics, buffer pool metrics
- **Logging**: Structured logging with configurable levels
- **Health Checks**: Database connectivity and performance monitoring

### 🔄 Replication & High Availability
- **Primary-Replica Replication**: Asynchronous replication with failover
- **WAL (Write-Ahead Logging)**: Crash recovery and point-in-time recovery
- **Backup & Restore**: Logical backups with `pg_dump` compatibility (planned)

### 🛠️ Developer Experience
- **Interactive CLI**: Rich command-line interface with auto-completion
- **SQL Shell**: Interactive SQL execution with history
- **Configuration**: YAML/JSON/TOML configuration files
- **Embedded Mode**: Library usage for Go applications
- **Client Libraries**: PostgreSQL wire protocol compatibility (`pgwire`)
- **Docker Support**: Containerized deployment

### 📚 Advanced Features
- **Partitioning**: Table partitioning by range, hash, list
- **Foreign Keys**: Referential integrity with CASCADE/RESTRICT actions
- **Check Constraints**: Column and table-level constraints
- **Sequences**: Auto-incrementing sequences
- **Views**: Virtual tables with updatable views (planned)
- **Materialized Views** (planned)
- **Extensions**: Pluggable extensions system (planned)

## Quick Start

## Quick Start

### Prerequisites

- Go 1.21 or later
- Make (optional, but recommended)

### Build

```bash
# Clone the repository
git clone https://github.com/JayabrataBasu/VeridicalDB.git
cd VeridicalDB

# Download dependencies
go mod download

# Build
make build
# Or without make:
go build -o build/veridicaldb ./cmd/veridicaldb
```

### Initialize a Database

```bash
# Create a new database directory
./build/veridicaldb init ./data

# This creates:
#   ./data/           - Main data directory
#   ./data/wal/       - Write-ahead log
#   ./data/tables/    - Table storage
#   ./data/indexes/   - Index storage
#   ./veridicaldb.yaml - Configuration file
```

### Run

```bash
# Start the interactive shell
./build/veridicaldb --config veridicaldb.yaml
```

You'll see:

```
 __      __        _     _ _           _ ____  ____  
 \ \    / /       (_)   | (_)         | |  _ \|  _ \ 
  \ \  / /__ _ __  _  __| |_  ___ __ _| | | | | |_) |
   \ \/ / _ \ '__|| |/ _' | |/ __/ _' | | | | |  _ < 
    \  /  __/ |   | | (_| | | (_| (_| | | |_| | |_) |
     \/ \___|_|   |_|\__,_|_|\___\__,_|_|____/|____/ 

    Version 0.1.0-beta - All Stages Complete
    Type HELP; or \? for available commands

veridicaldb> 
```

### Basic SQL Usage

```sql
-- Create a database and table
CREATE DATABASE myapp;
USE myapp;

CREATE TABLE users (
    id INT PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT,
    age INT,
    active BOOLEAN DEFAULT true
);

-- Insert data
INSERT INTO users (id, name, email, age) VALUES 
(1, 'Alice', 'alice@example.com', 30),
(2, 'Bob', 'bob@example.com', 25);

-- Query data
SELECT * FROM users WHERE active = true;
SELECT name, age FROM users ORDER BY age DESC LIMIT 5;

-- Advanced queries
SELECT 
    category, 
    COUNT(*) as count, 
    AVG(price) as avg_price 
FROM products 
GROUP BY category 
HAVING COUNT(*) > 5;

-- JOINs
SELECT u.name, o.product, o.quantity
FROM users u
INNER JOIN orders o ON u.id = o.user_id
WHERE o.status = 'completed';

-- CTEs and window functions
WITH user_stats AS (
    SELECT 
        name,
        age,
        ROW_NUMBER() OVER (ORDER BY age DESC) as age_rank
    FROM users
)
SELECT * FROM user_stats WHERE age_rank <= 3;
```

## Available Commands

VeridicalDB supports a comprehensive set of SQL commands across all major categories:

### Data Definition Language (DDL)
- **CREATE DATABASE** - Create new databases
- **DROP DATABASE** - Remove databases
- **USE** - Switch between databases
- **CREATE TABLE** - Define table schemas with constraints
- **ALTER TABLE** - Modify table structure (ADD/DROP/RENAME columns)
- **DROP TABLE** - Remove tables
- **CREATE INDEX** - Create B+ tree indexes for performance
- **DROP INDEX** - Remove indexes
- **CREATE VIEW** - Define virtual tables
- **DROP VIEW** - Remove views

### Data Manipulation Language (DML)
- **INSERT** - Add single or multiple rows
- **UPDATE** - Modify existing data with WHERE conditions
- **DELETE** - Remove data with WHERE conditions
- **TRUNCATE** - Quickly empty tables

### Data Query Language (DQL)
- **SELECT** - Query data with complex expressions
  - Column selection and aliases
  - WHERE clauses with complex conditions
  - ORDER BY with ASC/DESC and NULLS FIRST/LAST
  - LIMIT and OFFSET for pagination
  - DISTINCT for unique results
- **JOIN** operations: INNER, LEFT, RIGHT, FULL OUTER
- **Subqueries** - Nested SELECT statements
- **Common Table Expressions (CTEs)** with WITH clause
- **Window Functions** - ROW_NUMBER, RANK, DENSE_RANK, etc.

### Data Control Language (DCL)
- **CREATE USER** - Create database users
- **ALTER USER** - Modify user properties
- **DROP USER** - Remove users
- **GRANT** - Assign permissions
- **REVOKE** - Remove permissions

### Transaction Control Language (TCL)
- **BEGIN** - Start transactions
- **COMMIT** - Save transaction changes
- **ROLLBACK** - Undo transaction changes
- **SAVEPOINT** - Create transaction checkpoints
- **RELEASE SAVEPOINT** - Remove checkpoints

### Procedural Language (PL/SQL)
- **CREATE PROCEDURE** - Define stored procedures
- **DROP PROCEDURE** - Remove procedures
- **CREATE FUNCTION** - Define user-defined functions
- **DROP FUNCTION** - Remove functions
- **CREATE TRIGGER** - Define automatic triggers
- **DROP TRIGGER** - Remove triggers

### Advanced SQL Features
- **Set Operations**: UNION, INTERSECT, EXCEPT with ALL/DISTINCT
- **Aggregate Functions**: COUNT, SUM, AVG, MIN, MAX
- **Grouping**: GROUP BY with HAVING clauses
- **Conditional Logic**: CASE WHEN expressions
- **NULL Handling**: COALESCE, NULLIF, IS NULL/IS NOT NULL
- **Pattern Matching**: LIKE, ILIKE with wildcards
- **Range Queries**: BETWEEN, IN, NOT IN
- **Type Casting**: CAST and :: operators
- **String Functions**: CONCAT, SUBSTR, UPPER, LOWER, LENGTH
- **Math Functions**: ABS, ROUND, CEIL, FLOOR, POWER
- **Date/Time Functions**: NOW, CURRENT_DATE, EXTRACT
- **JSON Operations**: JSON_EXTRACT, JSON operators (->, ->>, #>)
- **Full-Text Search**: MATCH AGAINST with analyzers

### System Commands
- **SHOW DATABASES** - List all databases
- **SHOW TABLES** - List tables in current database
- **SHOW CREATE TABLE** - Display table DDL
- **DESCRIBE** - Show table structure
- **EXPLAIN** - Show query execution plans
- **HELP** or **\?** - Display help information

## Configuration

VeridicalDB can be configured via:

1. **Config file** (YAML, JSON, or TOML):
   ```yaml
   server:
     host: localhost
     port: 5433
   storage:
     data_dir: ./data
     page_size: 8192
     buffer_pool_mb: 128
   log:
     level: info
     format: text
   ```

2. **Environment variables** (prefix: `VERIDICAL_`):
   ```bash
   export VERIDICAL_SERVER_PORT=5433
   export VERIDICAL_STORAGE_DATA_DIR=/var/lib/veridicaldb
   export VERIDICAL_LOG_LEVEL=debug
   ```

## Testing

VeridicalDB includes a comprehensive smoke test suite that validates all major features:

```bash
# Run the full smoke test suite (130+ tests)
./scripts/smoke_test.sh

# Run unit tests
make test

# Run tests with coverage
make test-coverage
```

The smoke test covers:
- All SQL DDL/DML/DQL operations
- Advanced features like JOINs, CTEs, window functions
- Transactions, indexes, and constraints
- Stored procedures and triggers
- JSON support and full-text search

## Project Structure

```
VeridicalDB/
├── cmd/
│   ├── server/           # Server binary
│   └── veridicaldb/      # CLI binary entry point
├── internal/
│   ├── cli/              # REPL and command handling
│   ├── config/           # Configuration management
│   └── logger/           # Structured logging
├── pkg/
│   ├── auth/             # Authentication & authorization
│   ├── btree/            # B+ tree implementation
│   ├── catalog/          # Metadata management & schema
│   ├── cli/              # Command-line interface
│   ├── config/           # Configuration parsing
│   ├── fts/              # Full-text search
│   ├── lock/             # Lock manager
│   ├── log/              # Write-ahead logging
│   ├── net/              # Network protocols
│   ├── observability/    # Metrics & monitoring
│   ├── partition/        # Table partitioning
│   ├── pgwire/           # PostgreSQL wire protocol
│   ├── replication/      # Replication system
│   ├── shard/            # Sharding support
│   ├── sql/              # SQL parser & executor
│   ├── storage/          # Storage engines
│   ├── txn/              # Transaction manager
│   └── wal/              # WAL implementation
├── scripts/
│   ├── smoke_test.sh     # Comprehensive test suite
│   └── release.sh        # Release automation
├── test_data/            # Test fixtures
├── Makefile
├── Roadmap.md
└── README.md
```

## Roadmap

| Stage | Description | Status |
|-------|-------------|--------|
| 0 | Foundation (CLI, Config, Logging) | ✅ Complete |
| 1 | Heap Storage Engine | ✅ Complete |
| 2 | Catalog & Schema | ✅ Complete |
| 3 | SQL Parser & Executor | ✅ Complete |
| 4 | MVCC Transactions | ✅ Complete |
| 5 | Concurrency & Locking | ✅ Complete |
| 6 | B+ Tree Indexes | ✅ Complete |
| 7 | WAL & Recovery | ✅ Complete |
| 8 | Columnar Storage | ✅ Complete |
| 9 | Advanced SQL Features (JOINs, CTEs, Window Functions) | ✅ Complete |
| 10 | Replication & High Availability | ✅ Complete |
| 11 | Full-Text Search & JSON Support | ✅ Complete |
| 12 | Stored Procedures & Triggers | ✅ Complete |
| 13 | Observability & Monitoring | ✅ Complete |
| 14 | Enterprise Features (Partitioning, Security) | ✅ Complete |
| 15 | Polish & Production Readiness | ✅ Complete |

**Note**: The project has exceeded the original roadmap scope. All major database features are now implemented and tested.


## License

MIT License - see LICENSE file for details.

## Contributing

Contributions are welcome! Please read the roadmap first to understand the project direction.