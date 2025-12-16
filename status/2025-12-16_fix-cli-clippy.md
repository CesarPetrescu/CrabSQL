# 2025-12-16: fix-cli-clippy

## Meta
- Date (UTC): 2025-12-16 15:59
- Agent: GPT-5.2 (Codex CLI)
- Goal: Restore CLI/test harness compatibility and keep CI green.

## Changes
- Restore clap CLI args + expected stderr banner; remove hardcoded listen/data (`src/main.rs`).
- Refactor `CREATE INDEX` execution handler to satisfy clippy (`src/sql.rs`).
- Remove unused bits and simplify index backfill prefix logic (`src/store.rs`, `src/model.rs`).
- Ignore local `data_dir/` DB directory (`.gitignore`).
- Sync docs with current capabilities and gaps (`README.md`, `status.md`, `TODO.md`).

## TODO.md Updates
- Touched: "0) Current State (What Exists Today)" (DDL subset includes `CREATE INDEX`; transactions bullet updated).
- Touched: "`SHOW INDEX` (PRIMARY + secondary indexes; subset)."
- Touched: "10.1 Index Types" note about minimal `CREATE INDEX`.
- Touched: "11.2 MVCC" note about current MVCC limitations.

## Verification
- `cargo fmt -- --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --all --all-features`

## Notes / Follow-ups
- Secondary indexes are currently maintained but not used by the planner; MVCC-correct index scans + purge/GC remain major blockers for “real DB” semantics.
