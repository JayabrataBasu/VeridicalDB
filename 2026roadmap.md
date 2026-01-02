# VeridicalDB — 2026 Roadmap

This roadmap captures high-value items to bring VeridicalDB closer to PostgreSQL-like feature parity and production readiness. Items are ranked by priority and annotated with difficulty, short rationale, suggested start points in the codebase, and acceptance criteria.

---

## How to read this document
- Priority: High / Medium / Low (higher means do sooner)
- Difficulty: Small / Medium / Large (rough implementation effort)
- Start points: suggested files or packages to begin work

---

## Ranked roadmap (top-level overview) ✅

| Rank | Feature | Priority | Difficulty | Suggested start points |
|------|---------|----------|------------|------------------------|
| 1 | Strong authentication (bcrypt/Argon2) + remove default admin (`admin/admin`) | High | Small | `pkg/auth/auth.go`, `pkg/cli/repl.go` |
| 2 | CI + tests + lint + coverage automation | High | Small | Add `.github/workflows/*` to run `go test`, `go vet`, `golangci-lint` |
| 3 | TLS support for `pgwire` (client TLS) | High | Medium | `pkg/pgwire/server.go`, `pkg/pgwire/protocol.go` |
| 4 | Backup & PITR (logical basebackup + WAL retention & restore docs) | High | Large | `pkg/wal/*`, `cmd/veridicaldb` |
| 5 | pgwire extended protocol completion (parameter substitution, CancelRequest, portal row-limits) | High | Medium | `pkg/pgwire/server.go` |
| 6 | Autovacuum / MVCC GC & compaction | High | Medium | `pkg/txn`, `pkg/catalog/mvcc_table_manager.go` |
| 7 | Observability: Prometheus metrics + EXPLAIN ANALYZE + pprof | Medium | Small | `pkg/observability/*`, add metrics in hot paths (`pkg/sql`, `pkg/wal`) |
| 8 | Query optimizer improvements: stats, ANALYZE, histograms | Medium | Large | `pkg/sql/planner.go`, statistics storage in `pkg/catalog` |
| 9 | Logical replication (pub/sub, logical decoding) | Medium | Large | `pkg/replication`, `pkg/wal` |
| 10 | Advanced index types: GIN (for JSON/FTS), GiST | Medium | Large | `pkg/btree` (new packages) and `pkg/fts` |
| 11 | Materialized views + refresh support | Medium | Medium | `pkg/sql` (planner/executor), `pkg/catalog` |
| 12 | Extension system (pluggable modules/FDW) | Low | Large | `pkg/extensions` (new) |
| 13 | Prepared transactions (two-phase commit) | Low | Medium | `pkg/txn`, `pkg/wal` |
| 14 | Parallel query / vectorized execution | Low | Large | `pkg/sql` (executor) |
| 15 | Rich types (arrays, ranges, geo) & JSON enhancements | Low | Medium | `pkg/sql/ast.go`, `pkg/catalog/types.go` |

---

## Top 6 items — details, acceptance criteria & first steps 🔧

### 1) Strong authentication & remove default admin (Rank 1 — High / Small) ✅ (Completed)
- Why: SHA-256 with salt is insufficient for password storage; having `admin/admin` on init is insecure.
- Status: **Completed** — passwords are now stored using bcrypt; legacy SHA-256 passwords are migrated on successful authentication. Initial admin user is created with a secure random password (printed once on init) or can be set via `VERIDICALDB_DEFAULT_ADMIN_PASSWORD` env var.
- Acceptance criteria (met):
  - Passwords stored using bcrypt with tests and migration path for existing `users.json` (see `pkg/auth/auth_test.go`).
  - On first `init`, a secure admin password is generated and printed if not provided via env var.
  - Tests updated to reflect new behavior.
- Next steps:
  - Optionally add interactive prompt flag for `init` to allow setting admin password during initialization.
- First steps taken:
  - Replaced legacy hashing with bcrypt, implemented migration logic for legacy SHA-256 + salt hashes.
  - Added secure default admin generation and updated documentation.
- Suggested files: `pkg/auth/auth.go`, tests in `pkg/auth/auth_test.go`, `pkg/cli/repl.go`.
### 2) CI pipeline (Rank 2 — High / Small) ✅
- Why: Ensure regressions are caught early and provide badge/status for contributors.
- Acceptance criteria:
  - GitHub Actions workflow that runs `go test ./... -coverprofile`, `go vet`, `golangci-lint` with configured linters.
  - Coverage artifact uploaded and displayed/recorded.
- First steps:
  - Add `.github/workflows/ci.yml` with matrix Go versions (1.21–1.23), cache `GOMODCACHE`.

### 3) TLS for `pgwire` (Rank 3 — High / Medium)
- Why: Required for secure client connections in production.
- Acceptance criteria:
  - `pgwire` server accepts TLS connections when configured (cert and key paths in config).
  - Tests include an end-to-end TLS connect (self-signed cert) and a TLS-disabled path.
- First steps:
  - Add TLS config options to `veridicaldb.yaml` and `pkg/pgwire.NewServer` options.
  - Update `pkg/pgwire/server.go` to wrap `net.Listener` with `tls.NewListener` when configured.

### 4) Backup & PITR (Rank 4 — High / Large)
- Why: Operational safety — backups + point-in-time recovery are essential.
- Acceptance criteria:
  - `veridicaldb backup --basebackup` that produces a consistent snapshot and WAL starting point.
  - `veridicaldb restore --pitr --target-time` to restore to a point-in-time with automated WAL replay.
  - Docs with step-by-step examples.
- First steps:
  - Implement basebackup that copies data files and records WAL start LSN.
  - Ensure WAL retention and archiving hooks exist (configurable `archive_command`).
- Suggested files: `cmd/veridicaldb/*`, `pkg/wal/*`.

### 5) pgwire extended protocol completion (Rank 5 — High / Medium)
- Why: Compatibility with `psql` and client drivers depends on correct extended flow.
- Acceptance criteria:
  - `Parse`/`Bind`/`Execute` properly bind parameters (`$1` etc.) to prepared statements and `Execute` respects portal row limits.
  - `CancelRequest` implementation cancels a running query (close connection or signal session for graceful cancel) and is tested.
- First steps:
  - Implement parameter substitution in `handleExecute` to use stored `Portal.Params`.
  - Implement a connection lookup for CancelRequest; tests in `pkg/pgwire`.

### 6) Autovacuum & MVCC GC (Rank 6 — High / Medium)
- Why: Without background vacuum/compaction, MVCC leads to table bloat and poor performance.
- Acceptance criteria:
  - Background daemon that identifies dead tuples and reclaims space with tunable thresholds.
  - Metrics and tests to assert reclaimed tuples under load.
- First steps:
  - Add a `vacuum` worker integrated into `pkg/catalog`/`pkg/txn` that scans MVCC visibility and compacts pages.

---

## Implementation notes & rough estimates ⚖️
- Small: ~1–2 weeks of focused work + tests
- Medium: ~3–8 weeks, may require multiple PRs and design iterations
- Large: multi-month effort, often phased (MVP → improvements)


- Add labels: `security`, `high-priority`, `release-blocker`, `good-first-issue`.

---

