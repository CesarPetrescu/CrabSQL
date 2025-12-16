# 2025-12-16: CI + clippy gate + LEFT JOIN

## Meta
- Date (UTC): 2025-12-16 12:34
- Agent: GPT-5.2 (Codex CLI)
- Goal: Add CI quality gates, make clippy clean, and extend joins with correct LEFT JOIN semantics.

## Changes
- Fixed clippy warnings in projection/aggregation and join code (`src/sql.rs`).
- Implemented nested-loop `LEFT JOIN ... ON ...` with NULL-extension for unmatched rows (`src/sql.rs`).
- Added integration coverage for LEFT JOIN (including alias form) (`tests/verify_joins.rs`).
- Added GitHub Actions CI workflow running fmt/clippy/test (`.github/workflows/ci.yml`).
- Updated capability docs and roadmap to reflect LEFT JOIN + CI gates (`README.md`, `TODO.md`).

## TODO.md Updates
- Touched: `0) Current State (What Exists Today)` (updated multi-table SELECT subset wording).
- Touched: `9.2) Operators -> Joins -> LEFT JOIN semantics` (checked).
- Touched: `18) Testing, CI, and Quality` (checked CI gates + clippy gate).

## Verification
- `cargo fmt`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --all --all-features`

## Notes / Follow-ups
- RIGHT/FULL OUTER joins remain unsupported.
- `JOIN ... USING(...)` and `NATURAL JOIN` remain unsupported.
