# 2025-12-16: WHERE NULL semantics + IS NULL

## Meta
- Date (UTC): 2025-12-16 13:03
- Agent: GPT-5.2 (Codex CLI)
- Goal: Make predicate evaluation more MySQL-like by handling NULL correctly and supporting `IS NULL`/`IS NOT NULL` and `NOT`.

## Changes
- Implemented tri-valued predicate evaluation (TRUE/FALSE/UNKNOWN) so `NULL` comparisons become UNKNOWN and do not match in `WHERE`/`ON` (`src/sql.rs`).
- Added support for `expr IS NULL`, `expr IS NOT NULL`, and `NOT <predicate>` in `WHERE`/`JOIN ... ON` (`src/sql.rs`).
- Added integration tests for `IS NULL`, `IS NOT NULL`, and `NOT(… = NULL)` behavior (common LEFT JOIN anti-join patterns) (`tests/verify_joins.rs`).
- Updated current-state wording to reflect improved WHERE/null behavior (`TODO.md`).

## TODO.md Updates
- Touched: `0) Current State (What Exists Today)` -> Query subset (`WHERE` capabilities + NULL semantics)

## Verification
- `cargo test --test verify_joins`
- `cargo fmt`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --all --all-features`

## Notes / Follow-ups
- This is still far from “full WHERE semantics” (no `IN`, `LIKE`, `BETWEEN`, subqueries, etc.).
