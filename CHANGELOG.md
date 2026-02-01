# Changelog

All notable changes to this project will be documented in this file.

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

