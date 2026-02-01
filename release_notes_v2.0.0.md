# Draft release notes — v2.0.0 — Halcyon 🔖

Summary
A major release with a production-ready TUI foundation, Backup & PITR completion, MVCC improvements, and several polish and maintenance updates.

Highlights & Improvements (Features)

TUI: new --tui entry point, theme engine with 8 themes, styled sidebar and refined palettes, main layout composition, and Bubble Tea foundation.
TUI components: interactive table sorting, smart pagination, detail view, and home dashboard with real-time metrics.
Backup & PITR: basebackup, WAL archiver, restore CLI, and related documentation.
MVCC: auto vacuum support.
Refactor: extracted display for formatting and reuse across CLI/TUI.
Preliminary driver support for Python and Java languages. (more to follow)

Fixes

Fix ASCII logo rendering and error display in TUI; add placeholder screens.
Linter and pointer-fix cleanups (nil checks, unused params, silence unused-parameter linter).
Dependency bumps: golang.org/x/crypto and actions/cache.
Misc test and CI improvements.

Docs & Tests

TUI testing and usage guide added; moved advanced usage to OPERATIONS.md. Many tests added and passing across TUI components.

Contributors

Jayabrata Basu

Downloads (Assets)

- `veridicaldb_v2.0.0_linux_amd64.tar.gz` — SHA256: `e53da51b0a7a86393d58a9735758dde5d15f8ce118c24d9ba767b5acb4c816b0`
- `veridicaldb_v2.0.0_darwin_amd64.tar.gz` — SHA256: `28bffb56bd5a1f5bbb0f2aa90a0d2c06c3bbb68ae8c1408fd8eeb28a044e561c`
- `veridicaldb_v2.0.0_windows_amd64.zip` — SHA256: `06d9a90204f79a294fce29a0dde6d92ba30c8e37bf3384adbf66174997cc30b8`

You can verify the checksums after downloading with:

`sha256sum -c SHA256SUMS.txt`

(`SHA256SUMS.txt` is also attached to the release.)
