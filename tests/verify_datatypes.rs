mod common;

use mysql::prelude::*;

#[test]
fn verify_datatypes() -> anyhow::Result<()> {
    let (_server, addr) = common::spawn_server()?;
    let url = format!("mysql://root:root@127.0.0.1:{}", addr.port());

    let pool = common::pool_for_url(&url)?;
    let mut conn = common::get_conn_with_retry(&pool, &url)?;

    // cleanup
    conn.query_drop("DROP DATABASE IF EXISTS test_db")?;
    conn.query_drop("CREATE DATABASE test_db")?;
    conn.query_drop("USE test_db")?;

    // CREATE TABLE
    conn.query_drop(
        "CREATE TABLE types_test (
            id INT PRIMARY KEY,
            val_float FLOAT,
            val_date DATE,
            val_datetime DATETIME
        )",
    )?;

    // INSERT
    conn.exec_drop(
        "INSERT INTO types_test (id, val_float, val_date, val_datetime) VALUES (?, ?, ?, ?)",
        (1, 1.23, "2023-01-01", "2023-01-01 12:00:00"),
    )?;

    conn.exec_drop(
        "INSERT INTO types_test (id, val_float, val_date, val_datetime) VALUES (?, ?, ?, ?)",
        (2, 4.56, "2023-02-01", "2023-02-01 13:00:00"),
    )?;

    // SELECT & VERIFY
    let rows: Vec<(i32, f64, String, String)> =
        conn.query("SELECT id, val_float, val_date, val_datetime FROM types_test ORDER BY id")?;

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, 1);
    assert!((rows[0].1 - 1.23).abs() < 1e-6);
    assert_eq!(rows[0].2, "2023-01-01");
    // Datetime format might vary based on how we store/retrieve.
    // If input was "2023-01-01 12:00:00", output should be same if handled correctly.
    // Cell::DateTime stores millis.
    // 12:00:00 is preserved.
    assert_eq!(rows[0].3, "2023-01-01 12:00:00");

    assert_eq!(rows[1].0, 2);
    assert!((rows[1].1 - 4.56).abs() < 1e-6);
    assert_eq!(rows[1].2, "2023-02-01");

    // WHERE Clause (Float)
    let rows_float: Vec<i32> = conn.query("SELECT id FROM types_test WHERE val_float > 2.0")?;
    assert_eq!(rows_float.len(), 1);
    assert_eq!(rows_float[0], 2);

    // WHERE Clause (Date)
    let rows_date: Vec<i32> =
        conn.query("SELECT id FROM types_test WHERE val_date > '2023-01-15'")?;
    assert_eq!(rows_date.len(), 1);
    assert_eq!(rows_date[0], 2);

    Ok(())
}
