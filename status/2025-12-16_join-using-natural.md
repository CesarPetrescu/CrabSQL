# 2025-12-16: JOIN USING + NATURAL

## Meta
- Date (UTC): 2025-12-16 12:56
- Agent: GPT-5.2 (Codex CLI)
- Goal: Improve MySQL join compatibility by supporting `JOIN ... USING(...)` and `NATURAL [LEFT] JOIN`, with tests.

## Changes
- Implemented `JOIN ... USING(col1, ...)` by translating to qualified `ON left_table.col = right_table.col [AND ...]` (`src/sql.rs`).
- Implemented `NATURAL [LEFT] JOIN` by translating common-column equality predicates (conservative: errors if a NATURAL column name is ambiguous on the left side) (`src/sql.rs`).
- Extended join integration coverage with `USING`, `LEFT ... USING`, `NATURAL JOIN`, and `NATURAL LEFT JOIN` cases (`tests/verify_joins.rs`).
- Updated supported-feature docs and roadmap checkboxes (`README.md`, `TODO.md`).

## TODO.md Updates
- Touched: `9.2) Operators -> Joins -> JOIN ... USING(...) constraints`
- Touched: `9.2) Operators -> Joins -> NATURAL [LEFT] JOIN constraints`

## Verification
- `cargo fmt`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --all --all-features`

## Notes / Follow-ups
- Result-column coalescing for `USING`/`NATURAL` (MySQL-style single joined column in `SELECT *`) is not implemented.
- `USING(...)` currently requires unqualified column names and will error if the column name is ambiguous on the left side.
