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

- `veridicaldb_v2.0.0_linux_amd64.tar.gz` — SHA256: `995ed2e35a817feedc5cb93d66c84533f67e5e79ffa5fe477c5bb8069a889874`
- `veridicaldb_v2.0.0_darwin_amd64.tar.gz` — SHA256: `1fe0d8137864899fabf3b43d0660d74abd4e7b235c49ba23fb91264d40f4c4c2`
- `veridicaldb_v2.0.0_windows_amd64.zip` — SHA256: `eff5f91e3c53dd680d5a207b3fdd312d1029f6e9df7858714df60dc80a099680`

You can verify the checksums after downloading with:

`sha256sum -c SHA256SUMS.txt`

(`SHA256SUMS.txt` is also attached to the release.)
