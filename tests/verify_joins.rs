mod common;

use mysql::prelude::*;

#[test]
fn test_joins() -> anyhow::Result<()> {
    let (_server, addr) = common::spawn_server()?;
    let url = format!("mysql://root:root@127.0.0.1:{}", addr.port());

    let pool = common::pool_for_url(&url)?;
    let mut conn = common::get_conn_with_retry(&pool, &url)?;

    // Setup
    conn.query_drop("CREATE DATABASE IF NOT EXISTS test_join")?;
    conn.query_drop("USE test_join")?;

    conn.query_drop("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))")?;
    conn.query_drop("CREATE TABLE posts (id INT PRIMARY KEY, user_id INT, title VARCHAR(100))")?;

    conn.exec_drop("INSERT INTO users (id, name) VALUES (?, ?)", (1, "Alice"))?;
    conn.exec_drop("INSERT INTO users (id, name) VALUES (?, ?)", (2, "Bob"))?;
    conn.exec_drop("INSERT INTO users (id, name) VALUES (?, ?)", (3, "Charlie"))?; // No posts

    conn.exec_drop(
        "INSERT INTO posts (id, user_id, title) VALUES (?, ?, ?)",
        (101, 1, "Alice Post 1"),
    )?;
    conn.exec_drop(
        "INSERT INTO posts (id, user_id, title) VALUES (?, ?, ?)",
        (102, 1, "Alice Post 2"),
    )?;
    conn.exec_drop(
        "INSERT INTO posts (id, user_id, title) VALUES (?, ?, ?)",
        (103, 2, "Bob Post 1"),
    )?;
    conn.exec_drop(
        "INSERT INTO posts (id, user_id, title) VALUES (?, ?, ?)",
        (104, 99, "Orphan Post"),
    )?;

    // Test 1: Implicit Join (Cartesian Product)
    // 3 users * 4 posts = 12 rows
    let rows: Vec<i64> = conn.query("SELECT COUNT(*) FROM users, posts")?;
    assert_eq!(rows[0], 12);

    // Test 2: Explicit Inner Join with ON
    // Alice(2), Bob(1) -> 3 rows
    let res: Vec<(String, String)> = conn.query("SELECT users.name, posts.title FROM users JOIN posts ON users.id = posts.user_id ORDER BY posts.id")?;

    assert_eq!(res.len(), 3);
    assert_eq!(res[0], ("Alice".into(), "Alice Post 1".into()));
    assert_eq!(res[1], ("Alice".into(), "Alice Post 2".into()));
    assert_eq!(res[2], ("Bob".into(), "Bob Post 1".into()));

    // Test 3: Implicit Join with WHERE (Old school join)
    let res: Vec<(String, String)> = conn.query("SELECT users.name, posts.title FROM users, posts WHERE users.id = posts.user_id ORDER BY posts.id")?;
    assert_eq!(res.len(), 3);
    assert_eq!(res[0].0, "Alice");

    // Test 4: Verify qualified names in WHERE
    let res: Vec<String> = conn.query(
        "SELECT title FROM posts JOIN users ON posts.user_id = users.id WHERE users.name = 'Bob'",
    )?;
    assert_eq!(res, vec!["Bob Post 1"]);

    // Test 5: Table aliases (common real-world join style)
    let res: Vec<(String, String)> = conn.query(
        "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id ORDER BY p.id",
    )?;
    assert_eq!(res.len(), 3);
    assert_eq!(res[0], ("Alice".into(), "Alice Post 1".into()));
    assert_eq!(res[2], ("Bob".into(), "Bob Post 1".into()));

    // Test 6: Qualified wildcard expansion
    let res: Vec<(i64, String)> = conn.query(
        "SELECT users.* FROM users JOIN posts ON users.id = posts.user_id ORDER BY posts.id LIMIT 1",
    )?;
    assert_eq!(res, vec![(1, "Alice".into())]);

    // Test 7: Multi-table '*' should not error due to ambiguous column names
    let rows: Vec<mysql::Row> =
        conn.query("SELECT * FROM users JOIN posts ON users.id = posts.user_id ORDER BY posts.id")?;
    assert_eq!(rows.len(), 3);

    Ok(())
}
