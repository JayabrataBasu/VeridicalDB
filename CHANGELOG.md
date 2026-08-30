# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

Structural cleanup and the start of a phased remediation plan (see the
"VeridicalDB Atlas" document).

### Changed (P4 — splitting `pkg/sql`, in progress)

- The SQL lexer moved out of the monolithic `pkg/sql` package. `pkg/sql/lexer.go`
  is now two leaf packages: `pkg/sql/token` (the `TokenType` enum, the keyword
  table, `LookupKeyword`, and the `Token` struct) and `pkg/sql/lex` (the `Lexer`).
  `pkg/sql` and `pkg/shard` reference them through `token.` / `lex.` qualifiers.
- The SQL abstract syntax tree moved to `pkg/sql/ast` (`pkg/sql/ast.go` →
  `pkg/sql/ast/ast.go`). Every AST node type, the `Statement` / `Expression` /
  `PLStatement` interfaces, and the partition / grouping-set / trigger / parameter
  enums now live there; the parser, planner, executors, session, `pkg/pgwire`, and
  `pkg/shard` reference them through `ast.` qualifiers.
- No behavior change — token values, lexer output, and AST shape are identical;
  this is plan phase P4, which splits `pkg/sql` into
  `token / lex / ast / parse / plan / exec / session` in dependency order.

### Changed (P3 — config & logging consolidation)

- One config package. `internal/config` (which used viper) was merged into
  `pkg/config`; the **viper dependency was dropped**. `pkg/config.Config` is now
  the single schema for the server, CLI, and TUI, with `server`, `storage`,
  `logging`, `pgwire`, `backup`, and `sharding` sections, layered as
  defaults → file (YAML/JSON) → `VERIDICAL_*` environment overrides. Data-dir
  init/validation (`InitDataDir`, `ValidateDataDir`, `CreateDefaultConfig`) moved
  here too. `config.example.yaml` is the annotated reference.
- One logger. `internal/logger` (a zap wrapper) was removed; everything uses
  `pkg/log`, which gained `NewNop()` and a no-op `Sync()`. The **zap dependency
  was dropped**.
- **Behavior note:** the CLI's default client port is now 5432 (was 5433); the
  default log output is `stderr`. Set them explicitly in the config if you
  relied on the old values.

### Removed

- Vendored `tools/golangci-lint` (39 MB), the checked-in `server` binary,
  `nohup.out`, a stray `:memory:/` directory, the tracked `test_data/` fixtures,
  and five packages that were unreachable from either binary: `pkg/cli`,
  `pkg/display`, `pkg/net`, `pkg/partition`, `pkg/replication`. **`pkg/replication`
  was never wired into a running binary** despite earlier notes describing it as
  production-ready; treat replication as unshipped.

### Changed

- Single source of truth for the version string in `internal/build`, injected at
  link time by the `Makefile` and `scripts/release.sh` (previously the ldflags
  targeted a symbol nothing read).
- `.golangci.yml` migrated to the v2 schema (`run.skip-dirs` was a no-op under
  `version: "2"`).

### Added

- `pkg/engine` — `Open(Config)` assembles the WAL/txn/catalog/session object
  graph once, `DB.NewSession()` returns a session with every optional capability
  wired in, and `DB.Close()` owns the checkpointer/WAL lifecycle. The five call
  sites that each hand-assembled the graph (`cmd/server`, `cmd/veridicaldb`'s TUI
  path, `internal/cli` twice, `pkg/shard`) now go through it.
- `pgwire.ServerConfig.NewSession` — a session factory; the server uses it
  instead of building a bare `sql.NewSession`.
- `TestWireProtocolFeatureMatrix` in `pkg/pgwire` — exercises the SQL feature
  surface over a real socket.

### Fixed

- The PostgreSQL wire protocol now has secondary indexes, full-text search,
  triggers, stored procedures, and multi-database support — previously these
  worked only in the REPL/TUI because pgwire built an unwired session
  (plan phase P1). Note: a wire client must now `CREATE DATABASE` / `USE`
  before DDL, matching the REPL.
- Committed transactions now survive a restart in durable mode. `Session`
  committed and aborted through the raw `txn.Manager`, so no COMMIT/ABORT
  records reached the WAL and crash recovery undid every transaction as a
  "loser". Commits and aborts now route through `wal.TxnLogger` (via new
  `MVCCTableManager.CommitTxn` / `AbortTxn`), which fsyncs a COMMIT record
  before returning. Read-only transactions still skip the WAL entirely, so
  `SELECT` stays off the fsync path.

- Constraint enforcement on the shipping (MVCC / `Session`) path, which
  previously accepted violations the pre-MVCC executor rejected (plan phase
  P2.1):
  - duplicate **PRIMARY KEY** / **UNIQUE** values, on `INSERT` and `UPDATE`
  - **FOREIGN KEY** referential integrity, on `INSERT`/`UPDATE`, plus RESTRICT
    on `DELETE` of a referenced row
  - **VARCHAR(n)** length
  - `INSERT ... ON CONFLICT DO NOTHING` / `DO UPDATE` — previously ignored, so a
    conflicting insert silently created a duplicate-key row
  - `CREATE TABLE` / `ALTER TABLE ADD COLUMN` now record a column's `UNIQUE` flag
    and `VARCHAR` length (the MVCC catalog path had been dropping both)
  - `ALTER TABLE ... ADD CONSTRAINT ... UNIQUE`, and `TEXT` → `DATE` / `TIMESTAMP`
    coercion on insert, are now handled on the MVCC path
- More MVCC-path insert fixes (plan phase P2.6):
  - column **DEFAULT** values are now applied before the NOT NULL check
  - **AUTO_INCREMENT** columns are now filled (they were left NULL, so an
    `AUTO_INCREMENT` primary key rejected every insert)
  - `INSERT ... ON CONFLICT DO UPDATE` now reports `RowsAffected: 1`
  - `AVG()` of an integer column now returns a float; the pre-MVCC executor did
    integer division (`AVG(80,90,101)` returned `90`, not `90.33`)

### Removed

- The unreachable `sql.NewExecutor` fallback path in the REPL (`internal/cli`).

### Internal

- `sqlExec` test interface + `TestExecutorSessionParity` — the P2 consolidation
  harness that runs the same SQL against both executors. `foreign_key_test.go`
  and `date_varchar_unique_test.go` now run against `Session`.

## v2.0.0 - 2026-02-01

Halcyon — A major release with a production-ready TUI foundation, Backup & PITR completion, MVCC improvements, preliminary driver support for Python and Java and several polish and maintenance updates.

### Highlights

- TUI: new `--tui` entry point, theme engine with 8 themes, styled sidebar and refined palettes, main layout composition, and Bubble Tea foundation.
- TUI components: interactive table sorting, smart pagination, detail view, and home dashboard with real-time metrics.
- Backup & PITR: basebackup, WAL archiver, restore CLI, and related documentation.
- MVCC: auto vacuum support.
- Refactor: extracted display for formatting and reuse across CLI/TUI.
- Preliminary support for Java and Python.

### Files

- See `build/release/` after running `./scripts/release.sh v2.0.0` for release artifacts and `SHA256SUMS`.

## v1.0.0 - 2025-12-16

Initial stable release (v1.0.0).

### Highlights

- Production-ready replication: Primary/Replica streaming WAL, failover and promotion support.
- Comprehensive MVCC and SQL features with a full test-suite.
- Linter cleanups and improved code quality (fixed numerous gopls/staticcheck warnings).
- Packaging support: cross-platform release script (Linux/Windows/macOS) producing archives and checksums.

### Files

- See `build/release/` after running `./scripts/release.sh v1.0.0` for release artifacts and `SHA256SUMS`.

