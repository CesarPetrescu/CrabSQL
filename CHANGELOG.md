# Changelog

All notable changes to this project will be documented in this file.

## [0.1.0] - 2025-12-16

### Features
- **Storage Engine**: Implemented MVCC support for row changes and secondary index maintenance.
- **SQL Support**: Added support for `CREATE INDEX`, basic `JOIN` operations, and improved `WHERE` clause semantics.
- **CLI**: Restored and fixed CLI functionality for interacting with the database.
- **CI**: Added initial CI workflow.

### Improvements
- Refactored `create_table`, `get_table`, and `apply_row_changes_mvcc` in `store.rs`.
- Fixed clippy warnings and synchronized documentation.

### Notes
- This release supersedes the `alpha-mvp` tag.
