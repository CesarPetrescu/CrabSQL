# Agent Status (Read This First)

This file is the **single source of truth for how agents should work in this repo**.
If you are an AI agent (or a human acting like one), **read this before changing anything**.

## Current Reality (Do Not Overclaim)

As of 2025-12-16 (UTC):
- `cargo test --all --all-features` passes.
- The server is a **useful MySQL-protocol-compatible toy/MVP**, not a production MySQL/MariaDB replacement.

**It is NOT production-ready yet**, because core “real database” requirements are missing:
- No WAL / crash-recovery guarantees / fsync policy / verified durability story.
- No InnoDB-class MVCC/undo/purge or full isolation semantics (snapshot/serializable); no deadlock detection; minimal locking.
- No replication, backups/restore story, upgrade/migrations, or operational tooling parity.
- SQL dialect coverage is far from MySQL 8.0 / MariaDB latest (joins, subqueries, optimizer, collations, etc.).
- MySQL 8.0 auth/TLS/session features are incomplete (`caching_sha2_password`, TLS, session tracking, etc.).

## Mandatory Workflow for All Agents

### 1) Update the Roadmap (`TODO.md`)
`TODO.md` is the authoritative backlog + compatibility roadmap.

Rules:
- If you discover a missing capability, add it to `TODO.md`.
- If you implement something, **check it off** (or move it to a “done” section) and keep wording accurate.
- Do not remove items unless explicitly agreed; de-scope by moving to a “deferred” section instead.

### 2) Log Every Run in `status/`
For **every agent run that changes code or behavior**, create a new markdown file in `status/`.

Naming:
- `status/YYYY-MM-DD_<short-title>.md` (UTC date; keep title short, `kebab-case`).

Your run log must include:
- Date/time (UTC), who/what agent, and the goal.
- What changed (high-level) and **where** (file paths).
- Which `TODO.md` items you touched (quote the checkbox text or section name).
- Tests/commands run and their results.
- Follow-ups / known issues introduced.

### 3) Keep Claims in Sync
If you change capabilities, keep these files consistent:
- `README.md` (“What it supports / does not support”)
- `TODO.md` (roadmap truth)
- `status/` run log (what actually happened)

## Run Log Template

Copy/paste this into a new file under `status/`:

```md
# YYYY-MM-DD: <short title>

## Meta
- Date (UTC): YYYY-MM-DD HH:MM
- Agent: <name/model>
- Goal: <1 sentence>

## Changes
- <what changed> (`path/to/file.rs:line`)
- <what changed> (`path/to/file.rs:line`)

## TODO.md Updates
- Touched: "<checkbox text or section>"
- Touched: "<checkbox text or section>"

## Verification
- `cargo test --all --all-features`
- <any targeted test commands>

## Notes / Follow-ups
- <known limitations, next steps>
```
