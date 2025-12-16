mod common;

use mysql::prelude::*;

#[test]
fn verify_where_ops() -> anyhow::Result<()> {
    let (_server, addr) = common::spawn_server()?;
    let url = format!("mysql://root:root@127.0.0.1:{}", addr.port());

    let pool = common::pool_for_url(&url)?;
    let mut conn = common::get_conn_with_retry(&pool, &url)?;

    conn.query_drop("CREATE DATABASE IF NOT EXISTS test_where")?;
    conn.query_drop("USE test_where")?;

    conn.query_drop("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(100))")?;
    conn.query_drop(
        "INSERT INTO t (id, name) VALUES \
         (1,'Alice'),(2,'Bob'),(3,'Bobby'),(4,'Rob'),(5,NULL),(6,'100% legit')",
    )?;

    let ids: Vec<i64> = conn.query("SELECT id FROM t WHERE id IN (1,3,5) ORDER BY id")?;
    assert_eq!(ids, vec![1, 3, 5]);

    let ids: Vec<i64> = conn.query("SELECT id FROM t WHERE id NOT IN (1,3,5) ORDER BY id")?;
    assert_eq!(ids, vec![2, 4, 6]);

    let rows: Vec<i64> = conn.query("SELECT COUNT(*) FROM t WHERE name IN ('Bob', NULL)")?;
    assert_eq!(rows[0], 1);

    let rows: Vec<i64> = conn.query("SELECT COUNT(*) FROM t WHERE name NOT IN ('Bob', NULL)")?;
    assert_eq!(rows[0], 0);

    let ids: Vec<i64> = conn.query("SELECT id FROM t WHERE id BETWEEN 2 AND 4 ORDER BY id")?;
    assert_eq!(ids, vec![2, 3, 4]);

    let ids: Vec<i64> = conn.query("SELECT id FROM t WHERE id NOT BETWEEN 2 AND 4 ORDER BY id")?;
    assert_eq!(ids, vec![1, 5, 6]);

    let ids: Vec<i64> = conn.query("SELECT id FROM t WHERE name LIKE 'Bob%' ORDER BY id")?;
    assert_eq!(ids, vec![2, 3]);

    let ids: Vec<i64> = conn.query("SELECT id FROM t WHERE name LIKE '%ob' ORDER BY id")?;
    assert_eq!(ids, vec![2, 4]);

    let ids: Vec<i64> = conn.query("SELECT id FROM t WHERE name LIKE '_ob' ORDER BY id")?;
    assert_eq!(ids, vec![2, 4]);

    let ids: Vec<i64> =
        conn.query("SELECT id FROM t WHERE name LIKE '100!% legit' ESCAPE '!' ORDER BY id")?;
    assert_eq!(ids, vec![6]);

    let rows: Vec<i64> = conn.query("SELECT COUNT(*) FROM t WHERE id BETWEEN 1 AND NULL")?;
    assert_eq!(rows[0], 0);

    Ok(())
}
