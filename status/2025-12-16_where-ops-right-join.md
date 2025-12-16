# 2025-12-16: WHERE ops + RIGHT JOIN

## Meta
- Date (UTC): 2025-12-16 13:32
- Agent: GPT-5.2 (Codex CLI)
- Goal: Expand WHERE predicate support (IN/LIKE/BETWEEN) and add RIGHT JOIN, with tests and CI-safe verification.

## Changes
- Added tri-valued `WHERE` support for `IN (...)`, `LIKE` (with `ESCAPE`), and `BETWEEN` (`src/sql.rs`).
- Added `RIGHT JOIN` semantics (NULL-extension on unmatched right rows) and a small equi-join fast path for `ON` predicates that are pure `AND` of `=` comparisons (`src/sql.rs`).
- Added integration coverage for the new WHERE operators (`tests/verify_where_ops.rs`).
- Extended join integration tests for `RIGHT JOIN` (including `USING(...)`) (`tests/verify_joins.rs`).
- Updated docs/roadmap to reflect the new capabilities (`README.md`, `TODO.md`).

## TODO.md Updates
- Touched: `0) Current State (What Exists Today)` -> Query subset / DML subset wording
- Touched: `9.2) Operators -> Joins -> RIGHT JOIN semantics` (checked)

## Verification
- `cargo fmt`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --all --all-features`

## Notes / Follow-ups
- WHERE still lacks many core operators (`IN (subquery)`, `LIKE` collations, `REGEXP`, `BETWEEN` on typed temporals, etc.).
- FULL OUTER JOIN remains unsupported; hash join/optimizer work is still pending beyond the equi-join fast path.
