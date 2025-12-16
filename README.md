# rusty-mini-mysql

A minimal MySQL protocol-compatible server in Rust (MVP). Clients like the `mysql` CLI can connect over the MySQL protocol.

## Agent workflow (required)

If you (human or AI agent) are making changes:
- Read `status.md` first.
- Keep `TODO.md` accurate.
- For every change run, add a new log entry under `status/` as described in `status.md`.

## What it supports

- MySQL protocol handshake + `mysql_native_password` authentication
- Multiple databases
- Basic DDL:
  - `CREATE DATABASE [IF NOT EXISTS]`, `DROP DATABASE [IF EXISTS]`, `USE`
  - `CREATE TABLE [IF NOT EXISTS]` (subset; **requires an INT/BIGINT PRIMARY KEY**)
  - `ALTER TABLE ... ADD COLUMN ...` (subset; appends columns only)
  - `CREATE INDEX ...` (subset; non-unique, single-column; maintained on writes)
  - `DROP TABLE [IF EXISTS]`
- Basic DML:
  - `INSERT INTO ... VALUES ...`
  - `SELECT ... FROM ...` with multi-table `FROM` (comma joins) and `INNER`/`LEFT`/`RIGHT JOIN` with `ON`, `USING(...)`, and `NATURAL` constraints (subset; supports table aliases)
  - `SELECT ... [WHERE ...] [ORDER BY ...] [LIMIT/OFFSET]`
  - `SELECT DISTINCT ...`
  - Aggregates: `COUNT/SUM/AVG/MIN/MAX`, `GROUP BY`, `HAVING` (subset)
  - `UPDATE ... SET ... WHERE col = literal`
  - `DELETE FROM ... WHERE col = literal`
- Prepared statements (binary protocol) for supported queries, with integer/string/NULL parameters
- Basic transactions:
  - `BEGIN` / `START TRANSACTION`, `COMMIT`, `ROLLBACK`
  - `SAVEPOINT`, `ROLLBACK TO SAVEPOINT`, `RELEASE SAVEPOINT`
- Basic schema introspection:
  - `information_schema.{SCHEMATA,TABLES,COLUMNS,STATISTICS}`
  - `SHOW TABLE STATUS`, `SHOW INDEX`, `SHOW [FULL] COLUMNS`, `SHOW CREATE TABLE`

## What it intentionally does NOT support

- Full SQL grammar / optimizer
- FULL OUTER joins, join reordering/optimization, and most planner features
- Subqueries, window functions, CTEs, views, triggers, stored routines
- Full MySQL type/collation system (current types are a small subset)
- Index scans / optimizer use of secondary indexes (indexes are not used for query planning yet)
- InnoDB-class MVCC/undo/purge and full isolation semantics (current MVCC is basic; locking is minimal)

## Run

```bash
cargo run -- --listen 127.0.0.1:3307 --data ./data --root-password root
```

Connect:

```bash
mysql -h 127.0.0.1 -P 3307 -u root -proot
```

Example session:

```sql
CREATE DATABASE demo;
USE demo;
CREATE TABLE t (id BIGINT NOT NULL, name TEXT, PRIMARY KEY (id));
INSERT INTO t (id, name) VALUES (1,'alice'),(2,'bob');
SELECT * FROM t;
SELECT name FROM t WHERE id = 2;
UPDATE t SET name='bobby' WHERE id = 2;
DELETE FROM t WHERE id = 1;
SHOW TABLES;
DESCRIBE t;
```

## Storage

This MVP uses `sled` as an embedded KV store and persists all data into the `--data` directory.
