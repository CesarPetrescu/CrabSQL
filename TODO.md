# TODO / Roadmap: Production-Grade MySQL 8.0 + MariaDB-Compatible Server

This repository currently implements a small subset of SQL and enough of the MySQL wire protocol for
real clients to connect. The goal of this document is to enumerate what is required to become:

1) **Production-ready as a database server**, and
2) **Highly compatible with MySQL 8.0**, plus **broad coverage of MariaDB (latest) features**.

Important reality check:
- “MySQL 8.0 compatible + MariaDB latest” is effectively a superset target with real semantic divergence.
- “Production-ready” means correctness under concurrency + durability + security + operational excellence,
  not just “it compiles and passes a few tests”.

This is a living checklist. If you find a gap, add it.

---

## 0.1) Repo Workflow (Agents)

These items keep the project maintainable as multiple agents iterate.

- [x] Require agents to read `status.md` (documented in `README.md`).
- [x] Log every change run under `status/` (see `status.md` for format).
- [x] GitHub Actions release workflow builds `alpha-mvp` binaries (Linux/Windows/macOS) and publishes a prerelease.

## 0) Current State (What Exists Today)

These are **not** compatibility guarantees — they are simply what currently works.

- [x] MySQL protocol handshake (basic) + `mysql_native_password` auth.
- [x] Basic catalog: databases + tables stored in embedded KV (`sled`).
- [x] DDL subset: `CREATE/DROP DATABASE`, `USE`, `CREATE/DROP TABLE`, `ALTER TABLE ... ADD COLUMN`, `CREATE INDEX` (subset; non-unique, single-column).
- [x] DML subset: `INSERT ... VALUES` (multi-row), basic `UPDATE`/`DELETE`, `SELECT ... FROM ...` (multi-table subset with INNER/LEFT/RIGHT JOIN + comma joins).
- [x] Query subset: basic `WHERE` (`AND/OR/NOT`, comparisons, `IS NULL/IS NOT NULL`, `IN (...)`, `LIKE`, `BETWEEN` with tri-valued NULL semantics), `ORDER BY`, `LIMIT/OFFSET`, `DISTINCT`.
- [x] Aggregation subset: `COUNT/SUM/AVG/MIN/MAX`, `GROUP BY`, `HAVING` (limited semantics).
- [x] Type subset: integers/text plus `FLOAT`, `DATE`, `DATETIME` (limited semantics).
- [x] Prepared statements: COM_STMT_PREPARE/EXECUTE bridged to text queries (limited types).
- [x] Transactions (partial): MVCC row versions + snapshot read views; session-local write buffering overlay; savepoints; basic row write locks.

---

## 1) “Production-Ready” Definition (Release Gates)

Treat these as non-negotiable gates before calling anything “prod ready”.

### 1.1 Correctness Gates
- [ ] Run a curated subset of **MySQL 8.0 MTR** (mysql-test-run) in CI; track pass rate over time.
- [ ] Run a curated subset of the **MariaDB test suite** in CI; track pass rate over time.
- [ ] Deterministic correctness under concurrency (stress tests + randomized workloads).
- [ ] Validate SQL semantics on tricky areas (NULL logic, collations, time zones, locking, isolation).
- [ ] Fuzz the SQL parser and wire protocol layer (no panics, no OOM, no hangs).

### 1.2 Durability Gates
- [ ] Crash safety: kill -9 during write-heavy workloads; recover to a consistent state.
- [ ] Durable commit semantics: configurable fsync policy; documented guarantees.
- [ ] Backups/restores verified (logical + physical) and compatible with standard tooling expectations.

### 1.3 Security Gates
- [ ] TLS supported (and required where needed by auth plugins).
- [ ] MySQL 8.0 default auth support (or a clearly documented compatibility mode).
- [ ] Privilege model correctness and enforcement; least privilege by default.
- [ ] No SQL injection hazards in any protocol bridging paths (prepared statements must not string-concatenate).

### 1.4 Operational Gates
- [ ] Metrics (Prometheus) + structured logs + tracing.
- [ ] Resource governance: memory limits, query timeouts, cancellation, connection limits.
- [ ] Introspection: `SHOW PROCESSLIST`, `KILL`, slow query log, `EXPLAIN`.
- [ ] Upgrade story: on-disk format versioning and migrations.

### 1.5 Performance Gates
- [ ] Baseline performance tracked (sysbench-like OLTP + analytical queries).
- [ ] p99 latency targets defined and measured.
- [ ] Capacity tests: max connections, max table sizes, large rows, large transactions.

---

## 2) Compatibility Targets & Modes (MySQL 8.0 + MariaDB)

### 2.1 Target Versions
- [ ] Primary target: **MySQL 8.0** (wire behavior, SQL semantics, system schemas, error codes).
- [ ] Secondary target: **MariaDB latest** (SQL dialect extensions and common client expectations).

### 2.2 Compatibility Mode Strategy
Because MySQL and MariaDB diverge, decide and implement a strategy:
- [ ] **Single-server identity** (default to MySQL 8.0 identity) with optional “MariaDB dialect mode”.
- [ ] Or **dual identity** at handshake level (server version string/capabilities), selected by config.
- [ ] Document all intentional incompatibilities with examples and error codes.

### 2.3 Compatibility Matrix (Must Build and Maintain)
- [ ] Add a `COMPAT_MATRIX.md` listing:
  - [ ] Supported SQL statements (MySQL vs MariaDB).
  - [ ] Supported functions (MySQL vs MariaDB).
  - [ ] Data types and coercions.
  - [ ] Transaction/isolation semantics.
  - [ ] System variables and their defaults.
  - [ ] System tables (`mysql.*`, `information_schema.*`, `performance_schema.*`, `sys.*`).

---

## 3) MySQL Wire Protocol (MySQL 8.0 + MariaDB Clients)

### 3.1 Handshake / Capabilities / Session Tracking
- [ ] Correct capability negotiation (CLIENT_* flags), incl. edge cases and connector quirks.
- [ ] CLIENT_DEPRECATE_EOF behavior (OK vs EOF packets for resultsets).
- [ ] Session state tracking (MySQL 8.0 session track feature) where clients expect it.
- [ ] Character set/collation negotiation at handshake + per-connection state.
- [ ] Compression (optional but common): zlib / zstd (as applicable).

### 3.2 Authentication Plugins
MySQL 8.0:
- [ ] `caching_sha2_password` (including fast auth + full auth flow).
- [ ] `sha256_password` (often requires TLS or RSA key exchange).
- [ ] `mysql_native_password` compatibility mode (even if deprecated in MySQL).

MariaDB (commonly used in the ecosystem):
- [ ] MariaDB auth plugin expectations (e.g., `ed25519` in MariaDB deployments).
- [ ] Auth switch / plugin negotiation correctness.

### 3.3 Command Coverage (COM_*)
- [ ] COM_QUERY (multi-statement + server-side multi-result handling).
- [ ] COM_STMT_PREPARE / COM_STMT_EXECUTE / COM_STMT_CLOSE / COM_STMT_RESET.
- [ ] COM_PING, COM_QUIT, COM_INIT_DB, COM_CHANGE_USER, COM_RESET_CONNECTION.
- [ ] COM_FIELD_LIST (used by some clients/metadata paths).
- [ ] Error + warnings diagnostics area behavior (SHOW WARNINGS/ERRORS support).

### 3.4 Prepared Statements (Binary Protocol Correctness)
- [ ] Proper server-side parameter typing and conversions (not “string interpolation”).
- [ ] Typed metadata for params and result columns (including collation flags).
- [ ] Handle NULL bitmap, signedness, length-encoded ints, decimals, temporal encodings.
- [ ] Plan caching and reprepare behavior on schema changes.

---

## 4) SQL Parser, Lexer, and MySQL/MariaDB Dialect Frontend

### 4.1 Lexer / Tokenizer Compatibility
- [ ] MySQL quoting rules: backticks, ANSI_QUOTES mode, identifier rules.
- [ ] String literal rules: backslash escapes (respect SQL modes), doubled quotes, charset introducers.
- [ ] Comments: `--`, `#`, `/* */` everywhere; optimizer hints `/*+ ... */`.
- [ ] Delimiter handling (`DELIMITER` for routines in CLI workflows).
- [ ] User variables (`@x`) and system variables (`@@var`, `@@session.var`, `@@global.var`).

### 4.2 Grammar Coverage (MySQL 8.0)
- [ ] Multi-statement parsing with correct statement boundaries and result streaming.
- [ ] Full expression grammar (precedence, boolean logic, casts, collations, subqueries).
- [ ] `WITH [RECURSIVE]` CTEs.
- [ ] Window functions grammar.
- [ ] `LATERAL` derived tables (where supported).
- [ ] Partitioning grammar.
- [ ] `LOCK TABLES` / `UNLOCK TABLES`.

### 4.3 MariaDB Dialect Extensions (Latest)
- [ ] `SEQUENCE` statements (`CREATE/ALTER/DROP SEQUENCE`, `NEXT VALUE FOR`).
- [ ] `RETURNING` for DML (MariaDB feature; not MySQL 8.0).
- [ ] System-versioned tables / temporal syntax (`FOR SYSTEM_TIME`, as applicable).
- [ ] Oracle mode / SQL_MODE extensions (if targeting MariaDB compatibility modes).

---

## 5) Type System, Values, and Collations

### 5.1 Core Types (MySQL 8.0 parity)
- [ ] Integers: TINYINT/SMALLINT/MEDIUMINT/INT/BIGINT, signed/unsigned.
- [ ] Numeric: DECIMAL, FLOAT, DOUBLE.
- [ ] Text: CHAR/VARCHAR/TINYTEXT/TEXT/MEDIUMTEXT/LONGTEXT.
- [ ] Binary: BINARY/VARBINARY/TINYBLOB/BLOB/MEDIUMBLOB/LONGBLOB.
- [ ] Temporal: DATE, TIME, DATETIME, TIMESTAMP, YEAR; fractional seconds.
- [ ] JSON type (MySQL-native behavior) + JSON path semantics and functions.
- [ ] ENUM/SET.
- [ ] BIT, BOOL/BOOLEAN.
- [ ] Spatial: GEOMETRY + subtypes, SRIDs (large).

### 5.2 Semantics
- [ ] NULL and 3-valued logic correctness.
- [ ] Type coercion rules (MySQL-style “numeric context”, string-to-number, etc.).
- [ ] SQL modes affecting coercion/overflow/invalid dates (STRICT_* and related).
- [ ] Collations and charsets: utf8mb4 as default, per-column/per-connection collation.
- [ ] Sorting and comparisons under collation rules; accent/case sensitivity behaviors.

---

## 6) Catalog, Metadata, and System Schemas

### 6.1 DDL Catalog
- [ ] Transactional catalog changes (atomic DDL, consistent reads).
- [ ] Table definitions: columns, defaults, generated columns, comments, charset/collation.
- [ ] Index definitions: primary/secondary/unique/fulltext/spatial/functional.
- [ ] Table options: engine, row format, partitioning, stats persistence.

### 6.2 System Schemas
MySQL 8.0 expects these shapes/behaviors:
- [ ] `information_schema` tables (TABLES, COLUMNS, STATISTICS, KEY_COLUMN_USAGE, etc.).
- [ ] `mysql` schema: user/privilege storage and views (varies across versions).
- [ ] `performance_schema` (big scope; but many tools expect at least partial).
- [ ] `sys` schema views (optional but very common in MySQL 8.0 installs).

MariaDB adds/changes metadata expectations:
- [ ] MariaDB-specific `information_schema` and status variables where clients expect them.

### 6.3 SHOW/DESCRIBE/EXPLAIN Parity
- [x] `SHOW DATABASES`, `SHOW TABLES`, `SHOW FULL TABLES`.
- [x] `SHOW COLUMNS` / `SHOW FULL COLUMNS` / `DESCRIBE`.
- [x] `SHOW INDEX` (PRIMARY + secondary indexes; subset).
- [x] `SHOW CREATE TABLE` (simplified output).
- [x] `SHOW VARIABLES` (minimal curated set + `SELECT @@var` support).
- [ ] `SHOW STATUS`.
- [ ] `SHOW ENGINE INNODB STATUS` (or compatible output).
- [ ] `EXPLAIN [FORMAT=...]` (and eventually `EXPLAIN ANALYZE`).

---

## 7) DDL Completeness (MySQL 8.0 + MariaDB)

- [ ] `CREATE TABLE` full feature set:
  - [ ] constraints: PRIMARY KEY, UNIQUE, CHECK (enforced), FOREIGN KEY.
  - [ ] generated columns (VIRTUAL/STORED).
  - [ ] `AUTO_INCREMENT`, default expressions.
  - [ ] table options: ENGINE, CHARSET/COLLATE, ROW_FORMAT, etc.
  - [ ] partitioning clauses.
- [ ] `ALTER TABLE` (core):
  - [ ] add/drop/modify columns and constraints.
  - [ ] rename columns/tables, change charset/collation.
  - [ ] add/drop indexes, invisible indexes (MySQL), etc.
  - [ ] online DDL behaviors and metadata locks.
- [ ] Views:
  - [ ] `CREATE/ALTER/DROP VIEW`, `ALGORITHM`, `SQL SECURITY`.
- [ ] Triggers:
  - [ ] BEFORE/AFTER INSERT/UPDATE/DELETE; row vs statement semantics.
- [ ] Stored routines:
  - [ ] `CREATE PROCEDURE/FUNCTION`, parameter modes, determinism, SQL security.
  - [ ] SQL/PSM execution engine, variable scoping, handlers, cursors.
- [ ] Events scheduler:
  - [ ] `CREATE EVENT`, scheduling, persistence.

MariaDB-specific DDL surface:
- [ ] `CREATE SEQUENCE` and related DDL.
- [ ] System-versioned tables DDL (if targeting).

---

## 8) DML Completeness (INSERT/UPDATE/DELETE + Upserts)

### 8.1 INSERT
- [ ] `INSERT ... VALUES`, multi-row, defaults, expressions.
- [ ] `INSERT ... SELECT`.
- [ ] `INSERT IGNORE` semantics (warning vs error behavior).
- [ ] `INSERT ... ON DUPLICATE KEY UPDATE` semantics (MySQL/MariaDB).
- [ ] `REPLACE INTO` semantics.
- [ ] Bulk load: `LOAD DATA [LOCAL] INFILE` (security implications).

### 8.2 UPDATE
- [ ] `UPDATE ... SET ... WHERE ...` (full expressions).
- [ ] `UPDATE ... ORDER BY ... LIMIT ...` (MySQL).
- [ ] Multi-table update / update with joins (MySQL/MariaDB).
- [ ] Correct affected rows semantics (matched vs changed).

### 8.3 DELETE
- [ ] `DELETE ... WHERE ...`.
- [ ] `DELETE ... ORDER BY ... LIMIT ...` (MySQL).
- [ ] Multi-table deletes / deletes with joins.

### 8.4 RETURNING (MariaDB)
- [ ] `INSERT/UPDATE/DELETE ... RETURNING ...` (MariaDB feature).

---

## 9) Query Engine (Planner/Optimizer/Executor)

### 9.1 Core Logical/Physical Plan
- [ ] Parser -> AST -> logical plan -> physical plan pipeline.
- [ ] Expression evaluation engine (typed, vector/scalar).
- [ ] Constant folding, predicate/projection pushdown.

### 9.2 Operators
- [ ] Table scans + index scans.
- [ ] Filters, projections, computed expressions.
- [ ] Joins:
  - [x] Nested loop INNER JOIN (and comma joins / CROSS join).
  - [x] Table aliases in FROM/JOIN.
  - [x] LEFT JOIN semantics.
  - [x] `JOIN ... USING(...)` constraints (converted to `ON`).
  - [x] `NATURAL [LEFT] JOIN` constraints (converted to `USING`-style equality on common columns).
  - [x] RIGHT JOIN semantics.
  - [ ] FULL OUTER JOIN semantics.
  - [ ] Hash join.
  - [ ] Merge join.
- [ ] Aggregations: hash/sort aggregate; GROUP BY/HAVING; rollup/cube where applicable.
- [ ] Sort (in-memory + spill), ORDER BY with collations.
- [ ] DISTINCT (full correctness with ORDER BY/LIMIT and expressions).
- [ ] Subqueries: scalar, IN, EXISTS, correlated.
- [ ] Set operations: UNION/UNION ALL, etc.
- [ ] Window functions execution.
- [ ] CTE evaluation (including recursive).

### 9.3 Optimizer / Statistics
- [ ] Table stats, histograms (MySQL 8.0 supports histogram stats).
- [ ] Cardinality estimation, cost model.
- [ ] Index selection and join order optimization.

---

## 10) Indexing, Constraints, and Storage Layout

### 10.1 Index Types
Note: a minimal non-unique, single-column `CREATE INDEX` exists and is maintained on writes, but indexes are not used for query planning yet.
- [ ] Primary index (clustered, InnoDB-like).
- [ ] Secondary indexes (unique/non-unique).
- [ ] Composite indexes, prefix indexes.
- [ ] Functional indexes (MySQL 8.0).
- [ ] Descending indexes (MySQL 8.0).
- [ ] Full-text indexes (big).
- [ ] Spatial indexes (big).

### 10.2 Constraints
- [ ] NOT NULL enforcement, default values.
- [ ] UNIQUE enforcement.
- [ ] CHECK constraints (enforced).
- [ ] FOREIGN KEY enforcement and cascades.

### 10.3 Storage Layout
- [ ] Row format, record headers, variable-length fields.
- [ ] Large values overflow pages (BLOB/TEXT off-page).
- [ ] Page caching/buffer pool and eviction.
- [ ] Compaction / fragmentation control (depending on storage engine choice).

---

## 11) Transactions, MVCC, and Locking (InnoDB-Class Semantics)

This is the biggest gap between “toy SQL engine” and “real MySQL”.

### 11.1 Transaction Manager
- [ ] Transaction IDs, commit sequence numbers, active transaction table.
- [ ] Autocommit behavior identical to MySQL 8.0 (including implicit commits).
- [ ] Savepoints semantics identical to MySQL/MariaDB.
- [ ] Statement atomicity for all DML (not only some statements).

### 11.2 MVCC
Note: the storage layer persists per-row version chains keyed by TxID and uses read views for snapshot-style reads, but there is no undo log, no purge/GC, and no InnoDB-level MVCC semantics.
- [ ] Multi-version records (undo logs / version chains).
- [ ] Consistent snapshot reads (REPEATABLE READ default).
- [ ] Read view creation rules (MySQL InnoDB-like).
- [ ] Purge/GC of old versions without violating snapshots.

### 11.3 Isolation Levels
- [ ] READ UNCOMMITTED.
- [ ] READ COMMITTED.
- [ ] REPEATABLE READ (MySQL default).
- [ ] SERIALIZABLE (locking reads / predicate semantics).
- [ ] `SET TRANSACTION ISOLATION LEVEL ...` (session + per-tx).

### 11.4 Locking
- [ ] Row locks (shared/exclusive).
- [ ] Next-key / gap locks (InnoDB range locking for RR/Serializable).
- [ ] Intention locks.
- [ ] Metadata locks for DDL concurrency.
- [ ] Lock wait + configurable lock wait timeout.
- [ ] Deadlock detection and victim selection.
- [ ] `SELECT ... FOR UPDATE` / `LOCK IN SHARE MODE` / `NOWAIT` / `SKIP LOCKED`.

---

## 12) Durability, WAL/Redo/Undo, Crash Recovery

- [ ] Define durability contract (fsync policy, group commit behavior).
- [ ] Redo log (WAL) for page/record changes; log sequence numbers; checkpoints.
- [ ] Undo logs for MVCC + rollback.
- [ ] Doublewrite buffer or equivalent protection against torn pages.
- [ ] Crash recovery procedure (redo + undo).
- [ ] DDL durability and recovery (metadata).

Backups:
- [ ] Logical backup (mysqldump-compatible outputs, including routines if supported).
- [ ] Physical backup/snapshot + restore.
- [ ] PITR support if binlog exists (optional but typical).

---

## 13) Replication / HA (MySQL + MariaDB Ecosystem Expectations)

MySQL-style:
- [ ] Binlog generation (ROW/STATEMENT/MIXED formats).
- [ ] GTID support.
- [ ] Replica protocol compatibility (or a documented alternative).
- [ ] Semi-sync replication (optional).
- [ ] Group replication / InnoDB cluster (very large).

MariaDB-style:
- [ ] MariaDB GTID flavor and replication differences.
- [ ] Galera cluster compatibility (very large).

---

## 14) Server Variables, SQL Modes, and Compatibility Knobs

MySQL 8.0 clients rely heavily on session/global variables:
- [ ] `@@version`, `@@version_comment`, `@@sql_mode`, `@@time_zone`, `@@autocommit`, etc.
- [ ] `SET [GLOBAL|SESSION] ...` with correct privilege checks.
- [ ] SQL modes: ONLY_FULL_GROUP_BY, STRICT_TRANS_TABLES, NO_ZERO_DATE, etc.
- [ ] `SET NAMES`, `SET CHARACTER SET`, collation variables.
- [ ] `character_set_client`, `character_set_connection`, `collation_connection`, etc.

MariaDB adds additional variables/modes:
- [ ] MariaDB-specific SQL_MODE and variable names expected by tooling.

---

## 15) Security, Users, Roles, and Privileges

- [ ] Users: create/alter/drop; password expiry policies; account locking.
- [ ] Authentication plugins parity and secure defaults.
- [ ] Roles (MySQL 8.0 and MariaDB), default roles, role activation.
- [ ] Privileges:
  - [ ] global/db/table/column privileges.
  - [ ] routine privileges, proxy privileges.
  - [ ] `GRANT OPTION` and proper error codes.
- [ ] Audit logging and security event hooks.
- [ ] Encryption:
  - [ ] TLS in transit.
  - [ ] at-rest encryption (keys, rotation) (optional but common).

---

## 16) Operations, Observability, and Admin UX

- [ ] Configuration: file + CLI + env; reload where safe.
- [ ] Structured logging, query IDs, connection IDs.
- [ ] Metrics: QPS, latency histograms, memory, cache hit rates, compactions, locks, deadlocks.
- [ ] Admin SQL:
  - [ ] `SHOW PROCESSLIST`, `KILL [QUERY|CONNECTION]`.
  - [ ] `SHOW ENGINE ...` outputs where applicable.
  - [ ] `ANALYZE TABLE`, `OPTIMIZE TABLE`, `CHECK TABLE` (where meaningful).
- [ ] Slow query log (threshold, sampling).
- [ ] Query cancellation and timeouts.
- [ ] Resource limits: per-user/per-connection quotas, max packet, max result size.
- [ ] Telemetry around OOM/overload protection; graceful degradation.

---

## 17) Client/Tool Compatibility (“It Works With Real Stuff”)

- [ ] `mysql` CLI parity for common workflows.
- [ ] `mysqldump` basic functionality (logical backup).
- [ ] JDBC + popular drivers (Connector/J), mysql2, Go sql driver, etc.
- [ ] ORM sanity: Prisma/Sequelize/TypeORM (documented limitations).
- [ ] Migration tools: Flyway, Liquibase (DDL idempotency, metadata expectations).

---

## 18) Testing, CI, and Quality

- [ ] Expand integration tests to cover each newly implemented feature.
- [x] Add CI gates: GitHub Actions runs `cargo fmt --check`, `cargo clippy -D warnings`, and `cargo test`.
- [ ] Concurrency tests:
  - [ ] randomized transactions, deadlocks, lock waits, isolation anomalies.
- [ ] Crash/recovery tests:
  - [ ] fault injection (fsync failures, partial writes, kill -9).
- [ ] Fuzzing:
  - [ ] SQL parser fuzz.
  - [ ] wire protocol fuzz.
- [ ] Compatibility harness:
  - [ ] run subset of MySQL MTR and MariaDB tests in CI.
- [ ] Static analysis:
  - [x] `cargo clippy --all-targets --all-features -- -D warnings` (CI-gated).
  - [ ] `cargo audit` (supply chain).
  - [ ] Sanitizer runs (ASAN/TSAN where possible).

---

## 19) Milestones (Practical Execution Order)

These are “sane chunks” that each produce a meaningful upgrade.

- [ ] M1: Full MySQL 8.0 lexer/parser + AST; multi-statement + variables.
- [ ] M2: Typed expression engine + full WHERE semantics + correct NULL behavior.
- [ ] M3: Planner/executor for joins + aggregates + GROUP BY/HAVING + windows + CTEs.
- [ ] M4: Secondary indexes + constraints + stats + optimizer baseline.
- [ ] M5: InnoDB-class transactions: MVCC + RR snapshots + deadlocks + locking reads.
- [ ] M6: Durability: redo/undo + crash recovery + backups.
- [ ] M7: Protocol completeness: caching_sha2_password + TLS + prepared statements correctness.
- [ ] M8: System schemas + SHOW/EXPLAIN parity for real tooling.
- [ ] M9: Operational hardening: metrics, limits, slowlog, admin UX.
- [ ] M10: Replication/binlog + (optional) HA features.
