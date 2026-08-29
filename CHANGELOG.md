# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

Structural cleanup and the start of a phased remediation plan (see the
"VeridicalDB Atlas" document).

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

### Removed

- The unreachable `sql.NewExecutor` fallback path in the REPL (`internal/cli`).

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

