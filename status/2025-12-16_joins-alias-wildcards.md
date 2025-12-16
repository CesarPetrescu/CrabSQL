# 2025-12-16: joins aliases + wildcard robustness

## Meta
- Date (UTC): 2025-12-16 12:07
- Agent: GPT-5.2 (Codex CLI)
- Goal: Make joins usable in real queries (aliases and `*`/`table.*`).

## Changes
- Added table alias support in multi-table `FROM`/`JOIN` column resolution (`src/sql.rs`).
- Made `SELECT *` safe for multi-table queries by expanding wildcards with qualified column expressions (prevents ambiguity like `id`/`id`) (`src/sql.rs`).
- Added support for `table_alias.*` / `table.*` wildcard expansion (object-name form) (`src/sql.rs`).
- Extended join integration tests to cover aliases and wildcard queries (`tests/verify_joins.rs`).

## TODO.md Updates
- Touched: `9.2) Operators -> Joins -> Table aliases in FROM/JOIN` (checked).

## Verification
- `cargo test --all --all-features`

