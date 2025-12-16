# 2025-12-16: agent workflow + readability/robustness

## Meta
- Date (UTC): 2025-12-16 11:04
- Agent: GPT-5.2 (Codex CLI)
- Goal: Make the repo easier to maintain (agent workflow + cleaner code) while keeping behavior verified by tests.

## Changes
- Added agent workflow contract: `status.md`, `status/`, and a README pointer (`README.md`, `status.md`).
- Added/updated TODO tracking for agent workflow (`TODO.md`).
- Refactored integration tests to share server spawn + connection helpers and removed fixed-port assumptions (`tests/common/mod.rs`, `tests/verify.rs`, `tests/verify_aggregates.rs`, `tests/verify_datatypes.rs`).
- Fixed recent SELECT regressions and made SELECT evaluation more robust (session functions in `WHERE`, base-column `ORDER BY`, `DISTINCT`, safer parsing and ordering) (`src/sql.rs`).
- Removed a stray snippet file (`src/sql.rs_temp_snippet`).

## TODO.md Updates
- Touched: `0.1) Repo Workflow (Agents)`

## Verification
- `cargo fmt`
- `cargo test --all --all-features`

## Notes / Follow-ups
- The server still isn’t production-ready (no WAL/crash recovery policy, no MVCC/true isolation, limited SQL/auth parity). See `status.md`.

