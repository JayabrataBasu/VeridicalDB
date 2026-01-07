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

### 3) TLS for `pgwire` (Rank 3 — High / Medium) ✅ (Completed)
- Why: Required for secure client connections in production.
- Status: **Completed** — server-side TLS for `pgwire` implemented with optional mTLS support, tests and docs added.
- Acceptance criteria (met):
  - `pgwire` server accepts TLS connections when configured (cert and key paths in config).
  - Optional client certificate authentication (mTLS) is supported and validated against a CA when configured.
  - Unit and integration tests include end-to-end TLS connect (self-signed cert), SSLRequest behavior, and mTLS negative/positive cases.
  - Documentation updated with examples (psql usage, certificate generation) and `config.example.yaml` and `veridicaldb.yaml` updated.
- First steps taken / Implementation notes:
  - Added TLS config options to `veridicaldb.yaml` and `pkg/config` (`PgWire.TLS`), including `enabled`, `cert_file`, `key_file`, `ca_file`, `client_auth`, and TLS version settings.
  - Implemented TLS handling in `pkg/pgwire/server.go`: responds to `SSLRequest` with `S`/`N`, performs a per-connection TLS handshake, and wraps the connection for encrypted communication (preserving plaintext fallback).
  - Added unit tests for TLS config and integration tests in `pkg/pgwire` that exercise TLS handshake, query flow over TLS, and mTLS requirements.
  - Updated docs (`USAGE.md`, `Readme.md`) with TLS usage, psql examples, and security guidance.
- Next steps / follow-ups:
  - Add an automated e2e smoke test to CI (optional) that runs a short TLS client connect (psql or Go client) in the integration suite.
  - Consider certificate hot-reload and ACME/Let's Encrypt support in future iterations.



### 4) Backup & PITR (Rank 4 — High / Large) ✅ (Completed)
- Why: Operational safety — backups + point-in-time recovery are essential.
- Status: **Completed** — core features implemented, tested, and documented:
  - `veridicaldb backup --basebackup` produces consistent snapshot with LSN markers
  - WAL archiving via configurable `archive_command` and built-in S3 Archiver (AWS CLI-based)
  - `veridicaldb restore --pitr` supports PITR with automated WAL replay to target time/LSN
  - End-to-end tests covering backup → archive → PITR (4 E2E tests)
  - Prometheus-style metrics for backups/archiving and verification (exposition + tests)
  - Automated retention/pruning (daemon + `veridicaldb backup prune` CLI) and docs
  - CI: added `backup-e2e` job and unit tests for backup package
  - Operations docs updated (`OPERATIONS.md`) with S3 examples, pruning, metrics, and alerting recommendations
- Acceptance criteria (met):
  - `veridicaldb backup --basebackup` and `veridicaldb restore --pitr` work end-to-end
  - Tests include base backup, PITR to LSN/time, multiple archives restore, verification
  - Documentation and examples present in `OPERATIONS.md` and `USAGE.md`
- Remaining follow-ups (non-blocking):
  - S3 integration tests are skipped by default unless `AWS_TEST_BUCKET` and credentials are provided — consider enabling an integration runner with secrets for full S3 verification in CI if desired.
  - (Optional) Configure production alerting using the suggested Prometheus rules in `OPERATIONS.md` and add a short monitoring playbook.
  - (Optional) Add a minimal containerized e2e smoke job in CI for fast regression checks.
- Next steps: Close outstanding tickets referencing "Backup & PITR caveats" and mark this item as done in project tracking.
- Suggested files: `cmd/veridicaldb/*`, `pkg/wal/*`, `pkg/backup/*`

### 5) pgwire extended protocol completion (Rank 5 — High / Medium)
- Why: Compatibility with `psql` and client drivers depends on correct extended flow.
- Acceptance criteria:
  - `Parse`/`Bind`/`Execute` properly bind parameters (`$1` etc.) to prepared statements and `Execute` respects portal row limits.
  - `CancelRequest` implementation cancels a running query (close connection or signal session for graceful cancel) and is tested.
- First steps:
  - Implement parameter substitution in `handleExecute` to use stored `Portal.Params`.
  - Implement a connection lookup for CancelRequest; tests in `pkg/pgwire`.

### 6) Autovacuum & MVCC GC (Rank 6 — High / Medium) ✅ (Completed)
- Why: Without background vacuum/compaction, MVCC leads to table bloat and poor performance.
- Status: **Completed** — core vacuum functionality implemented:
  - `pkg/vacuum` package with `Manager`, `Worker`, `Config`, and `Metrics`
  - Background daemon that runs periodically and checks thresholds
  - Dead tuple detection using MVCC visibility rules (`isTupleDead`)
  - Page scanning and compaction to reclaim space
  - Cost-based throttling to avoid impacting foreground workloads
  - Prometheus-style metrics (runs, tuples removed, bytes reclaimed, errors)
  - Storage and catalog adapters for integration
  - CLI commands: `veridicaldb vacuum run [--table t] [--full]` and `veridicaldb vacuum status`
- Acceptance criteria (met):
  - Background daemon identifies dead tuples and reclaims space with tunable thresholds
  - Metrics track vacuum runs, reclaimed bytes, removed tuples, and errors
  - Unit tests cover visibility logic, config, metrics, start/stop, and dead tuple detection
- Files added:
  - `pkg/vacuum/vacuum.go` — Manager, Config, Metrics, TableStats, VacuumResult
  - `pkg/vacuum/worker.go` — Worker with page scanning and dead tuple detection
  - `pkg/vacuum/adapter.go` — StorageAdapter and CatalogAdapter for integration
  - `pkg/vacuum/vacuum_test.go` — Comprehensive unit tests (11 tests)
- Next steps / follow-ups:
  - Wire vacuum into the running database session for SQL `VACUUM` command
  - Add integration tests that create bloat and verify space reclamation
  - Consider adding freeze/anti-wraparound logic for long-running systems

---

## Implementation notes & rough estimates ⚖️
- Small: ~1–2 weeks of focused work + tests
- Medium: ~3–8 weeks, may require multiple PRs and design iterations
- Large: multi-month effort, often phased (MVP → improvements)


- Add labels: `security`, `high-priority`, `release-blocker`, `good-first-issue`.

---

