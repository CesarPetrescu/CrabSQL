mod common;

use mysql::prelude::*;
use mysql::*;

#[test]
fn verify_product_features() -> anyhow::Result<()> {
    let (_server, addr) = common::spawn_server()?;
    let url = format!("mysql://root:root@127.0.0.1:{}", addr.port());

    // 0. Connect
    let pool = common::pool_for_url(&url)?;
    let mut conn = common::get_conn_with_retry(&pool, &url)?;
    println!("Connected to server");

    // 1. DDL: Robust CREATE DATABASE with IF NOT EXISTS
    // First ensure clean state
    conn.query_drop("DROP DATABASE IF EXISTS product_db")?;

    // Create twice (second should not fail)
    conn.query_drop("CREATE DATABASE IF NOT EXISTS product_db")?;
    conn.query_drop("CREATE DATABASE IF NOT EXISTS product_db")?;
    println!("Database created (idempotent)");

    conn.query_drop("USE product_db")?;

    // 2. DDL: Robust CREATE TABLE
    conn.query_drop("CREATE TABLE IF NOT EXISTS inventory (id BIGINT NOT NULL, item TEXT, qty BIGINT, PRIMARY KEY (id))")?;
    conn.query_drop("CREATE TABLE IF NOT EXISTS inventory (id BIGINT NOT NULL, item TEXT, qty BIGINT, PRIMARY KEY (id))")?;
    println!("Table created (idempotent)");

    // 3. Prepared Statements (Binary Protocol via exec_drop)
    // Insert with placeholders
    conn.exec_drop(
        "INSERT INTO inventory (id, item, qty) VALUES (?, ?, ?)",
        (1, "Apple", 100),
    )?;
    conn.exec_drop(
        "INSERT INTO inventory (id, item, qty) VALUES (?, ?, ?)",
        (2, "Banana", 200),
    )?;
    // Int parameter mixed with string
    conn.exec_drop(
        "INSERT INTO inventory (id, item, qty) VALUES (?, ?, ?)",
        (3, "Cherry", 50),
    )?;
    // Strings with quotes should be safely escaped.
    conn.exec_drop(
        "INSERT INTO inventory (id, item, qty) VALUES (?, ?, ?)",
        (4, "O'Reilly", 10),
    )?;
    // This value would be an injection if we failed to escape it.
    conn.exec_drop(
        "INSERT INTO inventory (id, item, qty) VALUES (?, ?, ?)",
        (5, "x'); DROP DATABASE product_db; --", 1),
    )?;
    // Duplicate value for DISTINCT coverage.
    conn.exec_drop(
        "INSERT INTO inventory (id, item, qty) VALUES (?, ?, ?)",
        (6, "Apple", 999),
    )?;
    println!("Data inserted via Prepared Statements");

    // 4. Aggregation: COUNT(*)
    let count: Option<i64> = conn.query_first("SELECT count(*) FROM inventory")?;
    assert_eq!(count, Some(6));
    println!("COUNT(*) verified: 6");

    // 5. Aggregation with WHERE
    // Note: Project only supports '=' in WHERE clause
    let count_filtered: Option<i64> =
        conn.query_first("SELECT count(*) FROM inventory WHERE item = 'Banana'")?;
    assert_eq!(count_filtered, Some(1));
    println!("COUNT(*) with WHERE verified: 1");

    let count_oreilly: Option<i64> =
        conn.query_first("SELECT count(*) FROM inventory WHERE item = 'O''Reilly'")?;
    assert_eq!(count_oreilly, Some(1));

    let count_injection: Option<i64> = conn.query_first(
        "SELECT count(*) FROM inventory WHERE item = 'x''); DROP DATABASE product_db; --'",
    )?;
    assert_eq!(count_injection, Some(1));

    // 6. Select Data (Text Protocol)
    let items: Vec<(i64, String, i64)> = conn.query("SELECT id, item, qty FROM inventory")?;
    assert_eq!(items.len(), 6);

    // 6b. ORDER BY + LIMIT/OFFSET
    let top_qty_id: Option<i64> =
        conn.query_first("SELECT id FROM inventory ORDER BY qty DESC LIMIT 1")?;
    assert_eq!(top_qty_id, Some(6));
    let ids: Vec<i64> = conn.query("SELECT id FROM inventory ORDER BY id ASC LIMIT 2 OFFSET 1")?;
    assert_eq!(ids, vec![2, 3]);
    let ids2: Vec<i64> = conn.query("SELECT id FROM inventory ORDER BY id ASC LIMIT 1, 2")?;
    assert_eq!(ids2, vec![2, 3]);
    let distinct_items: Vec<String> =
        conn.query("SELECT DISTINCT item FROM inventory ORDER BY item ASC")?;
    assert_eq!(distinct_items.len(), 5);

    // 7. Cleanup
    conn.query_drop("DROP DATABASE IF EXISTS product_db")?;
    println!("Clean up done");

    Ok(())
}

#[test]
fn verify_show_create_and_columns() -> anyhow::Result<()> {
    let (_server, addr) = common::spawn_server()?;
    let url = format!("mysql://root:root@127.0.0.1:{}", addr.port());

    let pool = common::pool_for_url(&url)?;
    let mut conn = common::get_conn_with_retry(&pool, &url)?;

    conn.query_drop("DROP DATABASE IF EXISTS meta_db")?;
    conn.query_drop("CREATE DATABASE meta_db")?;
    conn.query_drop("USE meta_db")?;
    conn.query_drop(
        "CREATE TABLE inventory (id BIGINT NOT NULL, item TEXT, qty BIGINT, PRIMARY KEY (id))",
    )?;

    let fields: Vec<String> = conn.query_map(
        "SHOW COLUMNS FROM inventory",
        |(field, _ty, _null, _key, _default, _extra): (
            String,
            String,
            String,
            String,
            Option<String>,
            String,
        )| field,
    )?;
    assert_eq!(fields, vec!["id", "item", "qty"]);

    let create: Option<(String, String)> = conn.query_first("SHOW CREATE TABLE inventory")?;
    let (_table, ddl) = create.ok_or_else(|| anyhow::anyhow!("missing SHOW CREATE TABLE row"))?;
    assert!(ddl.to_ascii_lowercase().contains("create table"));
    assert!(ddl.to_ascii_lowercase().contains("primary key"));

    conn.query_drop("DROP DATABASE IF EXISTS meta_db")?;
    Ok(())
}

#[test]
fn verify_show_metadata_commands() -> anyhow::Result<()> {
    let (_server, addr) = common::spawn_server()?;
    let url = format!("mysql://root:root@127.0.0.1:{}", addr.port());

    let pool = common::pool_for_url(&url)?;
    let mut conn = common::get_conn_with_retry(&pool, &url)?;

    conn.query_drop("DROP DATABASE IF EXISTS meta_db")?;
    conn.query_drop("DROP DATABASE IF EXISTS meta_db2")?;
    conn.query_drop("CREATE DATABASE meta_db")?;
    conn.query_drop("CREATE DATABASE meta_db2")?;

    let dbs: Vec<String> = conn.query("SHOW DATABASES LIKE 'meta%'")?;
    assert!(dbs.iter().any(|d| d == "meta_db"));
    assert!(dbs.iter().any(|d| d == "meta_db2"));

    conn.query_drop("USE meta_db")?;
    conn.query_drop(
        "CREATE TABLE inventory (id BIGINT NOT NULL, item TEXT, qty BIGINT, PRIMARY KEY (id))",
    )?;
    conn.query_drop("CREATE TABLE other_table (id BIGINT NOT NULL, val TEXT, PRIMARY KEY (id))")?;

    let tables: Vec<String> = conn.query("SHOW TABLES LIKE 'inv%'")?;
    assert_eq!(tables, vec!["inventory"]);

    let full_tables: Vec<(String, String)> = conn.query("SHOW FULL TABLES LIKE 'inv%'")?;
    assert_eq!(full_tables, vec![("inventory".into(), "BASE TABLE".into())]);

    let tables_other_db: Vec<String> = conn.query("SHOW TABLES FROM meta_db2")?;
    assert!(tables_other_db.is_empty());

    let fields: Vec<String> = conn.query_map(
        "DESCRIBE inventory",
        |(field, _ty, _null, _key, _default, _extra): (
            String,
            String,
            String,
            String,
            Option<String>,
            String,
        )| field,
    )?;
    assert_eq!(fields, vec!["id", "item", "qty"]);

    let full_cols: Vec<Row> = conn.query("SHOW FULL COLUMNS FROM inventory")?;
    assert_eq!(full_cols.len(), 3);
    let field0: String = full_cols[0]
        .get("Field")
        .ok_or_else(|| anyhow::anyhow!("missing Field"))?;
    let key0: String = full_cols[0]
        .get("Key")
        .ok_or_else(|| anyhow::anyhow!("missing Key"))?;
    assert_eq!(field0, "id");
    assert_eq!(key0, "PRI");

    conn.query_drop("INSERT INTO inventory (id, item, qty) VALUES (1, 'A', 1), (2, 'B', 2)")?;

    let index_rows: Vec<Row> = conn.query("SHOW INDEX FROM inventory")?;
    assert_eq!(index_rows.len(), 1);
    let non_unique: i64 = index_rows[0]
        .get("Non_unique")
        .ok_or_else(|| anyhow::anyhow!("missing Non_unique"))?;
    let key_name: String = index_rows[0]
        .get("Key_name")
        .ok_or_else(|| anyhow::anyhow!("missing Key_name"))?;
    let seq: i64 = index_rows[0]
        .get("Seq_in_index")
        .ok_or_else(|| anyhow::anyhow!("missing Seq_in_index"))?;
    let col_name: String = index_rows[0]
        .get("Column_name")
        .ok_or_else(|| anyhow::anyhow!("missing Column_name"))?;
    let card: i64 = index_rows[0]
        .get("Cardinality")
        .ok_or_else(|| anyhow::anyhow!("missing Cardinality"))?;
    assert_eq!(non_unique, 0);
    assert_eq!(key_name, "PRIMARY");
    assert_eq!(seq, 1);
    assert_eq!(col_name, "id");
    assert_eq!(card, 2);

    let status_rows: Vec<Row> = conn.query("SHOW TABLE STATUS LIKE 'inv%'")?;
    assert_eq!(status_rows.len(), 1);
    let name: String = status_rows[0]
        .get("Name")
        .ok_or_else(|| anyhow::anyhow!("missing Name"))?;
    let engine: String = status_rows[0]
        .get("Engine")
        .ok_or_else(|| anyhow::anyhow!("missing Engine"))?;
    let rows: i64 = status_rows[0]
        .get("Rows")
        .ok_or_else(|| anyhow::anyhow!("missing Rows"))?;
    assert_eq!(name, "inventory");
    assert_eq!(engine, "InnoDB");
    assert_eq!(rows, 2);

    conn.query_drop("DROP DATABASE IF EXISTS meta_db")?;
    conn.query_drop("DROP DATABASE IF EXISTS meta_db2")?;
    Ok(())
}

#[test]
fn verify_information_schema_introspection() -> anyhow::Result<()> {
    let (_server, addr) = common::spawn_server()?;
    let url = format!("mysql://root:root@127.0.0.1:{}", addr.port());

    let pool = common::pool_for_url(&url)?;
    let mut conn = common::get_conn_with_retry(&pool, &url)?;

    conn.query_drop("DROP DATABASE IF EXISTS introspect_db")?;
    conn.query_drop("CREATE DATABASE introspect_db")?;
    conn.query_drop("USE introspect_db")?;
    conn.query_drop("CREATE TABLE users (id BIGINT NOT NULL, name TEXT, PRIMARY KEY (id))")?;
    conn.query_drop("INSERT INTO users (id, name) VALUES (1, 'a'), (2, 'b')")?;

    let schema: Option<String> = conn.query_first(
        "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = 'introspect_db'",
    )?;
    assert_eq!(schema.as_deref(), Some("introspect_db"));

    let tables: Vec<String> = conn.query(
        "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'introspect_db' AND TABLE_NAME = 'users'",
    )?;
    assert_eq!(tables, vec!["users"]);

    let tables_via_database_fn: Vec<String> = conn.query(
        "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'users'",
    )?;
    assert_eq!(tables_via_database_fn, vec!["users"]);

    let cols: Vec<(String, i64, String, String, String)> = conn.query(
        "SELECT COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, DATA_TYPE, COLUMN_KEY FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'introspect_db' AND TABLE_NAME = 'users' ORDER BY ORDINAL_POSITION",
    )?;
    assert_eq!(
        cols,
        vec![
            ("id".into(), 1, "NO".into(), "bigint".into(), "PRI".into()),
            ("name".into(), 2, "YES".into(), "text".into(), "".into()),
        ]
    );

    let alias_rows: Vec<Row> = conn.query(
        "SELECT COLUMN_NAME AS col FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'introspect_db' AND TABLE_NAME = 'users' ORDER BY ORDINAL_POSITION",
    )?;
    let alias_vals: Vec<String> = alias_rows
        .into_iter()
        .map(|r| r.get("col").unwrap_or_default())
        .collect();
    assert_eq!(alias_vals, vec!["id", "name"]);

    let count: Option<i64> = conn.query_first(
        "SELECT COUNT(*) AS c FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'introspect_db'",
    )?;
    assert_eq!(count, Some(1));

    let stats_rows: Vec<Row> = conn.query(
        "SELECT INDEX_NAME, COLUMN_NAME, NON_UNIQUE FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = 'introspect_db' AND TABLE_NAME = 'users'",
    )?;
    assert_eq!(stats_rows.len(), 1);
    let index_name: String = stats_rows[0]
        .get("INDEX_NAME")
        .ok_or_else(|| anyhow::anyhow!("missing INDEX_NAME"))?;
    let col_name: String = stats_rows[0]
        .get("COLUMN_NAME")
        .ok_or_else(|| anyhow::anyhow!("missing COLUMN_NAME"))?;
    let non_unique: i64 = stats_rows[0]
        .get("NON_UNIQUE")
        .ok_or_else(|| anyhow::anyhow!("missing NON_UNIQUE"))?;
    assert_eq!(index_name, "PRIMARY");
    assert_eq!(col_name, "id");
    assert_eq!(non_unique, 0);

    conn.query_drop("USE information_schema")?;
    let info_tables: Vec<String> = conn.query("SHOW TABLES LIKE 'TAB%'")?;
    assert!(info_tables.iter().any(|t| t == "TABLES"));
    let info_tables2: Vec<String> =
        conn.query("SELECT TABLE_NAME FROM TABLES WHERE TABLE_SCHEMA = 'introspect_db'")?;
    assert_eq!(info_tables2, vec!["users"]);

    conn.query_drop("DROP DATABASE IF EXISTS introspect_db")?;
    Ok(())
}

#[test]
fn verify_auto_increment() -> anyhow::Result<()> {
    let (_server, addr) = common::spawn_server()?;
    let url = format!("mysql://root:root@127.0.0.1:{}", addr.port());

    let pool = common::pool_for_url(&url)?;
    let mut conn = common::get_conn_with_retry(&pool, &url)?;

    conn.query_drop("DROP DATABASE IF EXISTS auto_db")?;
    conn.query_drop("CREATE DATABASE auto_db")?;
    conn.query_drop("USE auto_db")?;
    conn.query_drop(
        "CREATE TABLE t (id BIGINT NOT NULL AUTO_INCREMENT, name TEXT, PRIMARY KEY (id))",
    )?;

    conn.query_drop("INSERT INTO t (name) VALUES ('a'), ('b')")?;
    let rows: Vec<(i64, String)> = conn.query("SELECT id, name FROM t ORDER BY id ASC")?;
    assert_eq!(rows, vec![(1, "a".into()), (2, "b".into())]);

    conn.query_drop("INSERT INTO t (id, name) VALUES (10, 'c')")?;
    conn.query_drop("INSERT INTO t (name) VALUES ('d')")?;
    conn.query_drop("INSERT INTO t (id, name) VALUES (NULL, 'e')")?;

    let ids: Vec<i64> = conn.query("SELECT id FROM t ORDER BY id ASC")?;
    assert_eq!(ids, vec![1, 2, 10, 11, 12]);

    let extras: Vec<(String, String)> = conn.query_map(
        "SHOW COLUMNS FROM t",
        |(field, _ty, _null, _key, _default, extra): (
            String,
            String,
            String,
            String,
            Option<String>,
            String,
        )| (field, extra),
    )?;
    let id_extra = extras
        .iter()
        .find(|(field, _extra)| field == "id")
        .map(|(_field, extra)| extra.clone())
        .unwrap_or_default();
    assert_eq!(id_extra, "auto_increment");

    let status_rows: Vec<Row> = conn.query("SHOW TABLE STATUS LIKE 't'")?;
    assert_eq!(status_rows.len(), 1);
    let next_ai: i64 = status_rows[0]
        .get("Auto_increment")
        .ok_or_else(|| anyhow::anyhow!("missing Auto_increment"))?;
    assert_eq!(next_ai, 13);

    let ai_info: Option<i64> = conn.query_first(
        "SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'auto_db' AND TABLE_NAME = 't'",
    )?;
    assert_eq!(ai_info, Some(13));

    let extra_info: Option<String> = conn.query_first(
        "SELECT EXTRA FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'auto_db' AND TABLE_NAME = 't' AND COLUMN_NAME = 'id'",
    )?;
    assert_eq!(extra_info.as_deref(), Some("auto_increment"));

    let create: Option<(String, String)> = conn.query_first("SHOW CREATE TABLE t")?;
    let (_table, ddl) = create.ok_or_else(|| anyhow::anyhow!("missing SHOW CREATE TABLE row"))?;
    assert!(ddl.to_ascii_lowercase().contains("auto_increment"));

    conn.query_drop("DROP DATABASE IF EXISTS auto_db")?;
    Ok(())
}

#[test]
fn verify_alter_table_add_column() -> anyhow::Result<()> {
    let (_server, addr) = common::spawn_server()?;
    let url = format!("mysql://root:root@127.0.0.1:{}", addr.port());

    let pool = common::pool_for_url(&url)?;
    let mut conn = common::get_conn_with_retry(&pool, &url)?;

    conn.query_drop("DROP DATABASE IF EXISTS alter_db")?;
    conn.query_drop("CREATE DATABASE alter_db")?;
    conn.query_drop("USE alter_db")?;
    conn.query_drop("CREATE TABLE t (id BIGINT NOT NULL, name TEXT, PRIMARY KEY (id))")?;
    conn.query_drop("INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b')")?;

    conn.query_drop("ALTER TABLE t ADD COLUMN age BIGINT")?;

    let rows: Vec<(i64, String, Option<i64>)> =
        conn.query("SELECT id, name, age FROM t ORDER BY id ASC")?;
    assert_eq!(rows, vec![(1, "a".into(), None), (2, "b".into(), None)]);

    conn.query_drop("UPDATE t SET age = 30 WHERE id = 1")?;
    conn.query_drop("INSERT INTO t (id, name, age) VALUES (3, 'c', 40)")?;

    let ages: Vec<(i64, Option<i64>)> = conn.query("SELECT id, age FROM t ORDER BY id ASC")?;
    assert_eq!(ages, vec![(1, Some(30)), (2, None), (3, Some(40))]);

    let cols: Vec<String> = conn.query_map(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'alter_db' AND TABLE_NAME = 't' ORDER BY ORDINAL_POSITION",
        |name: String| name,
    )?;
    assert_eq!(cols, vec!["id", "name", "age"]);

    conn.query_drop("DROP DATABASE IF EXISTS alter_db")?;
    Ok(())
}

#[test]
fn verify_transactions() -> anyhow::Result<()> {
    let (_server, addr) = common::spawn_server()?;
    let url = format!("mysql://root:root@127.0.0.1:{}", addr.port());

    let pool = common::pool_for_url(&url)?;
    let mut conn1 = common::get_conn_with_retry(&pool, &url)?;
    let mut conn2 = common::get_conn_with_retry(&pool, &url)?;

    conn1.query_drop("DROP DATABASE IF EXISTS tx_db")?;
    conn1.query_drop("CREATE DATABASE tx_db")?;
    conn1.query_drop("USE tx_db")?;
    conn1.query_drop(
        "CREATE TABLE inventory (id BIGINT NOT NULL, item TEXT, qty BIGINT, PRIMARY KEY (id))",
    )?;

    conn2.query_drop("USE tx_db")?;

    // Explicit transaction: rollback should discard writes and hide them from other sessions.
    conn1.query_drop("BEGIN")?;
    conn1.exec_drop(
        "INSERT INTO inventory (id, item, qty) VALUES (?, ?, ?)",
        (1, "Apple", 10),
    )?;
    let c1: Option<i64> = conn1.query_first("SELECT count(*) FROM inventory")?;
    let c2: Option<i64> = conn2.query_first("SELECT count(*) FROM inventory")?;
    assert_eq!(c1, Some(1));
    assert_eq!(c2, Some(0));
    conn1.query_drop("ROLLBACK")?;
    let c1: Option<i64> = conn1.query_first("SELECT count(*) FROM inventory")?;
    let c2: Option<i64> = conn2.query_first("SELECT count(*) FROM inventory")?;
    assert_eq!(c1, Some(0));
    assert_eq!(c2, Some(0));

    // Explicit transaction: commit should make writes visible to other sessions.
    conn1.query_drop("START TRANSACTION")?;
    conn1.exec_drop(
        "INSERT INTO inventory (id, item, qty) VALUES (?, ?, ?)",
        (1, "Apple", 10),
    )?;
    conn1.query_drop("COMMIT")?;
    let c2: Option<i64> = conn2.query_first("SELECT count(*) FROM inventory")?;
    assert_eq!(c2, Some(1));

    // Uncommitted updates should not leak across sessions.
    conn1.query_drop("BEGIN")?;
    conn1.query_drop("UPDATE inventory SET qty = 99 WHERE id = 1")?;
    let qty1: Option<i64> = conn1.query_first("SELECT qty FROM inventory WHERE id = 1")?;
    let qty2: Option<i64> = conn2.query_first("SELECT qty FROM inventory WHERE id = 1")?;
    assert_eq!(qty1, Some(99));
    assert_eq!(qty2, Some(10));
    conn1.query_drop("COMMIT")?;
    let qty2_after: Option<i64> = conn2.query_first("SELECT qty FROM inventory WHERE id = 1")?;
    assert_eq!(qty2_after, Some(99));

    // autocommit=0: DML should stay uncommitted until COMMIT/ROLLBACK.
    conn1.query_drop("SET autocommit=0")?;
    conn1.exec_drop(
        "INSERT INTO inventory (id, item, qty) VALUES (?, ?, ?)",
        (2, "Banana", 20),
    )?;
    let c1: Option<i64> = conn1.query_first("SELECT count(*) FROM inventory")?;
    let c2: Option<i64> = conn2.query_first("SELECT count(*) FROM inventory")?;
    assert_eq!(c1, Some(2));
    assert_eq!(c2, Some(1));
    conn1.query_drop("COMMIT")?;
    let c2_after: Option<i64> = conn2.query_first("SELECT count(*) FROM inventory")?;
    assert_eq!(c2_after, Some(2));

    conn1.exec_drop(
        "INSERT INTO inventory (id, item, qty) VALUES (?, ?, ?)",
        (3, "Cherry", 30),
    )?;
    let c1: Option<i64> = conn1.query_first("SELECT count(*) FROM inventory")?;
    assert_eq!(c1, Some(3));
    conn1.query_drop("ROLLBACK")?;
    let c1_after: Option<i64> = conn1.query_first("SELECT count(*) FROM inventory")?;
    let c2_after: Option<i64> = conn2.query_first("SELECT count(*) FROM inventory")?;
    assert_eq!(c1_after, Some(2));
    assert_eq!(c2_after, Some(2));

    // Savepoints: rollback-to should restore to the savepoint snapshot.
    let base: Option<i64> = conn1.query_first("SELECT count(*) FROM inventory")?;
    assert_eq!(base, Some(2));
    conn1.query_drop("START TRANSACTION")?;
    conn1.exec_drop(
        "INSERT INTO inventory (id, item, qty) VALUES (?, ?, ?)",
        (10, "Kiwi", 1),
    )?;
    conn1.query_drop("SAVEPOINT s1")?;
    conn1.exec_drop(
        "INSERT INTO inventory (id, item, qty) VALUES (?, ?, ?)",
        (11, "Lemon", 2),
    )?;
    let c_in_tx: Option<i64> = conn1.query_first("SELECT count(*) FROM inventory")?;
    assert_eq!(c_in_tx, Some(4));
    conn1.query_drop("ROLLBACK TO SAVEPOINT s1")?;
    let c_after_rb: Option<i64> = conn1.query_first("SELECT count(*) FROM inventory")?;
    assert_eq!(c_after_rb, Some(3));
    conn1.query_drop("RELEASE SAVEPOINT s1")?;
    assert!(conn1.query_drop("ROLLBACK TO SAVEPOINT s1").is_err());
    conn1.query_drop("COMMIT")?;
    let c2_after: Option<i64> = conn2.query_first("SELECT count(*) FROM inventory")?;
    assert_eq!(c2_after, Some(3));
    let c11: Option<i64> = conn2.query_first("SELECT count(*) FROM inventory WHERE id = 11")?;
    assert_eq!(c11, Some(0));

    conn1.query_drop("SET autocommit=1")?;
    conn1.query_drop("DROP DATABASE IF EXISTS tx_db")?;
    Ok(())
}

#[test]
fn verify_delete_transactions() -> anyhow::Result<()> {
    let (_server, addr) = common::spawn_server()?;
    let url = format!("mysql://root:root@127.0.0.1:{}", addr.port());

    let pool = common::pool_for_url(&url)?;
    let mut conn1 = common::get_conn_with_retry(&pool, &url)?;
    let mut conn2 = common::get_conn_with_retry(&pool, &url)?;

    conn1.query_drop("DROP DATABASE IF EXISTS del_db")?;
    conn1.query_drop("CREATE DATABASE del_db")?;
    conn1.query_drop("USE del_db")?;
    conn1.query_drop(
        "CREATE TABLE inventory (id BIGINT NOT NULL, item TEXT, qty BIGINT, PRIMARY KEY (id))",
    )?;
    conn2.query_drop("USE del_db")?;

    conn1.exec_drop(
        "INSERT INTO inventory (id, item, qty) VALUES (?, ?, ?)",
        (1, "Apple", 10),
    )?;

    conn1.query_drop("BEGIN")?;
    conn1.query_drop("DELETE FROM inventory WHERE id = 1")?;
    let c1: Option<i64> = conn1.query_first("SELECT count(*) FROM inventory")?;
    let c2: Option<i64> = conn2.query_first("SELECT count(*) FROM inventory")?;
    assert_eq!(c1, Some(0));
    assert_eq!(c2, Some(1));
    conn1.query_drop("ROLLBACK")?;

    let c1_after: Option<i64> = conn1.query_first("SELECT count(*) FROM inventory")?;
    let c2_after: Option<i64> = conn2.query_first("SELECT count(*) FROM inventory")?;
    assert_eq!(c1_after, Some(1));
    assert_eq!(c2_after, Some(1));

    conn1.query_drop("START TRANSACTION")?;
    conn1.query_drop("DELETE FROM inventory WHERE id = 1")?;
    conn1.query_drop("COMMIT")?;
    let c2_final: Option<i64> = conn2.query_first("SELECT count(*) FROM inventory")?;
    assert_eq!(c2_final, Some(0));

    conn1.query_drop("DROP DATABASE IF EXISTS del_db")?;
    Ok(())
}

#[test]
fn verify_statement_atomicity() -> anyhow::Result<()> {
    let (_server, addr) = common::spawn_server()?;
    let url = format!("mysql://root:root@127.0.0.1:{}", addr.port());

    let pool = common::pool_for_url(&url)?;
    let mut conn = common::get_conn_with_retry(&pool, &url)?;
    conn.query_drop("DROP DATABASE IF EXISTS atomic_db")?;
    conn.query_drop("CREATE DATABASE atomic_db")?;
    conn.query_drop("USE atomic_db")?;
    conn.query_drop(
        "CREATE TABLE inventory (id BIGINT NOT NULL, item TEXT, qty BIGINT, PRIMARY KEY (id))",
    )?;

    conn.exec_drop(
        "INSERT INTO inventory (id, item, qty) VALUES (?, ?, ?)",
        (1, "Apple", 1),
    )?;

    assert!(conn
        .query_drop(
            "INSERT INTO inventory (id, item, qty) VALUES (2, 'Banana', 2), (1, 'Dup', 3), (3, 'Cherry', 4)",
        )
        .is_err());

    let count: Option<i64> = conn.query_first("SELECT count(*) FROM inventory")?;
    assert_eq!(count, Some(1));
    let c2: Option<i64> = conn.query_first("SELECT count(*) FROM inventory WHERE id = 2")?;
    assert_eq!(c2, Some(0));
    let c3: Option<i64> = conn.query_first("SELECT count(*) FROM inventory WHERE id = 3")?;
    assert_eq!(c3, Some(0));

    conn.query_drop("DROP DATABASE IF EXISTS atomic_db")?;
    Ok(())
}

#[test]
fn verify_system_variables() -> anyhow::Result<()> {
    let (_server, addr) = common::spawn_server()?;
    let url = format!("mysql://root:root@127.0.0.1:{}", addr.port());

    let pool = common::pool_for_url(&url)?;
    let mut conn = common::get_conn_with_retry(&pool, &url)?;

    let version_comment: Option<String> = conn.query_first("SELECT @@version_comment LIMIT 1")?;
    assert!(version_comment.is_some());

    let autocommit: Option<i64> = conn.query_first("SELECT @@autocommit")?;
    assert_eq!(autocommit, Some(1));

    let multi: Option<(i64, String)> =
        conn.query_first("SELECT @@autocommit, @@version_comment")?;
    let (ac2, vc2) = multi.ok_or_else(|| anyhow::anyhow!("missing multi-var row"))?;
    assert_eq!(ac2, 1);
    assert!(!vc2.is_empty());

    let vars: Vec<(String, String)> = conn.query("SHOW VARIABLES LIKE 'autocommit'")?;
    assert_eq!(vars.len(), 1);
    assert_eq!(vars[0].0.to_ascii_lowercase(), "autocommit");
    assert!(vars[0].1.eq_ignore_ascii_case("on"));

    conn.query_drop("SET time_zone = '+00:00'")?;
    let tz: Option<String> = conn.query_first("SELECT @@time_zone")?;
    assert_eq!(tz.as_deref(), Some("+00:00"));

    conn.query_drop("SET sql_mode = 'ANSI_QUOTES'")?;
    let sql_mode: Option<String> = conn.query_first("SELECT @@sql_mode")?;
    assert_eq!(sql_mode.as_deref(), Some("ANSI_QUOTES"));

    conn.query_drop("SET NAMES utf8mb4")?;
    let cs: Vec<(String, String)> = conn.query("SHOW VARIABLES LIKE 'character_set_client'")?;
    assert_eq!(cs.len(), 1);
    assert_eq!(cs[0].1, "utf8mb4");

    conn.query_drop("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")?;
    let iso: Option<String> = conn.query_first("SELECT @@transaction_isolation")?;
    assert_eq!(iso.as_deref(), Some("READ-COMMITTED"));

    Ok(())
}

#[test]
fn verify_row_locks() -> anyhow::Result<()> {
    let (_server, addr) = common::spawn_server()?;
    let url = format!("mysql://root:root@127.0.0.1:{}", addr.port());

    let pool = common::pool_for_url(&url)?;
    let mut conn1 = common::get_conn_with_retry(&pool, &url)?;
    let mut conn2 = common::get_conn_with_retry(&pool, &url)?;

    conn1.query_drop("DROP DATABASE IF EXISTS lock_db")?;
    conn1.query_drop("CREATE DATABASE lock_db")?;
    conn1.query_drop("USE lock_db")?;
    conn1.query_drop(
        "CREATE TABLE inventory (id BIGINT NOT NULL, item TEXT, qty BIGINT, PRIMARY KEY (id))",
    )?;
    conn2.query_drop("USE lock_db")?;

    conn1.exec_drop(
        "INSERT INTO inventory (id, item, qty) VALUES (?, ?, ?)",
        (1, "Apple", 10),
    )?;

    conn1.query_drop("BEGIN")?;
    conn1.query_drop("UPDATE inventory SET qty = 20 WHERE id = 1")?;

    conn2.query_drop("BEGIN")?;
    let err = conn2
        .query_drop("UPDATE inventory SET qty = 30 WHERE id = 1")
        .unwrap_err();
    match err {
        Error::MySqlError(e) => assert_eq!(e.code, 1205),
        other => anyhow::bail!("expected ER_LOCK_WAIT_TIMEOUT (1205), got: {other:?}"),
    }

    conn1.query_drop("COMMIT")?;
    conn2.query_drop("UPDATE inventory SET qty = 30 WHERE id = 1")?;
    conn2.query_drop("COMMIT")?;

    let qty: Option<i64> = conn1.query_first("SELECT qty FROM inventory WHERE id = 1")?;
    assert_eq!(qty, Some(30));

    conn1.query_drop("DROP DATABASE IF EXISTS lock_db")?;
    Ok(())
}
