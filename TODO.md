# TODOs and Priorities (auto-generated snapshot: Dec 16, 2025)

This file enumerates current actionable TODOs discovered in a quick codebase pass and suggests priorities and owners.

## High priority

- [x] Implement MVCC executor support for subqueries (pkg/sql/mvcc_executor.go)
  - Location: lines around "subqueries not yet supported in MVCC executor" (executeSelect/evalCondition paths).
  - Why: Several query cases rely on subqueries; unit tests show parsing but some runtime paths return "not yet supported".

- [x] Implement CREATE VIEW execution and DROP VIEW in the MVCC executor (pkg/sql/mvcc_executor.go)
  - Location: `executeCreateView` returns "not yet fully implemented".
  - Why: CREATE VIEW parsing works, but execution through the server returns an error; fix to support view persistence or runtime expansion.

## Medium priority

- [x] Implement row limiting for Extended Query protocol `Execute` message (pkg/pgwire/server.go)
  - Location: `handleExecute` – `_ = maxRows // TODO: implement row limiting`.
  - Why: Support portal-level row limiting for extended protocol semantics.

- [x] Implement parameter substitution for pgwire `Execute` (pkg/pgwire/server.go)
  - Location: `handleExecute` – parameter binding comment; currently query executed directly without substitution.
  - Why: Extended protocol `Parse/Bind/Execute` should use provided parameter bytes to bind $1/$2 placeholders.

- [x] Support CancelRequest (pgwire CancelRequestCode) (pkg/pgwire/server.go)
  - Location: `handleStartup` returns "cancel request not implemented"
  - Why: Implement cancellation via BackendKeyData/secret key and connection lookup for cancelling running statements.

## Low priority / backlog

- [x] Planner: improve AND expression handling (pkg/sql/planner.go)
  - Location: TODO comment: "Handle AND expressions - could match multiple predicates".
  - Why: Improve selectivity and index utilization for ANDed predicates.

- [ ] Update docs: ensure README, Roadmap, Verdict, Leftovers reflect accurate feature statuses (done: partial update on Dec 16, 2025)

---

If you'd like, I can start implementing any of the above (pick one) and mark it in-progress in the repo TODO & our internal todo list.
