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
    conn.query_drop("CREATE TABLE profiles (id INT PRIMARY KEY, bio VARCHAR(100))")?;

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

    conn.exec_drop(
        "INSERT INTO profiles (id, bio) VALUES (?, ?)",
        (1, "alice-bio"),
    )?;
    conn.exec_drop(
        "INSERT INTO profiles (id, bio) VALUES (?, ?)",
        (2, "bob-bio"),
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

    // Test 8: LEFT JOIN should preserve unmatched left rows (and emit NULLs)
    let res: Vec<(i64, Option<i64>)> = conn.query(
        "SELECT users.id, posts.id \
         FROM users LEFT JOIN posts ON users.id = posts.user_id \
         ORDER BY users.id, posts.id",
    )?;
    assert_eq!(
        res,
        vec![(1, Some(101)), (1, Some(102)), (2, Some(103)), (3, None)]
    );

    // Test 9: LEFT JOIN with aliases
    let res: Vec<(i64, Option<i64>)> = conn.query(
        "SELECT u.id, p.id \
         FROM users u LEFT JOIN posts p ON u.id = p.user_id \
         ORDER BY u.id, p.id",
    )?;
    assert_eq!(
        res,
        vec![(1, Some(101)), (1, Some(102)), (2, Some(103)), (3, None)]
    );

    // Test 10: JOIN ... USING(...) (common MySQL syntax)
    let res: Vec<(String, String)> = conn.query(
        "SELECT users.name, profiles.bio \
         FROM users JOIN profiles USING (id) \
         ORDER BY users.id",
    )?;
    assert_eq!(
        res,
        vec![
            ("Alice".into(), "alice-bio".into()),
            ("Bob".into(), "bob-bio".into())
        ]
    );

    // Test 11: LEFT JOIN ... USING(...) should preserve unmatched left rows
    let res: Vec<(i64, Option<String>)> = conn.query(
        "SELECT users.id, profiles.bio \
         FROM users LEFT JOIN profiles USING (id) \
         ORDER BY users.id",
    )?;
    assert_eq!(
        res,
        vec![
            (1, Some("alice-bio".into())),
            (2, Some("bob-bio".into())),
            (3, None)
        ]
    );

    // Test 12: NATURAL JOIN should join on common columns (here: id)
    let res: Vec<(String, String)> = conn.query(
        "SELECT users.name, profiles.bio \
         FROM users NATURAL JOIN profiles \
         ORDER BY users.id",
    )?;
    assert_eq!(
        res,
        vec![
            ("Alice".into(), "alice-bio".into()),
            ("Bob".into(), "bob-bio".into())
        ]
    );

    // Test 13: NATURAL LEFT JOIN should preserve unmatched left rows
    let res: Vec<(i64, Option<String>)> = conn.query(
        "SELECT users.id, profiles.bio \
         FROM users NATURAL LEFT JOIN profiles \
         ORDER BY users.id",
    )?;
    assert_eq!(
        res,
        vec![
            (1, Some("alice-bio".into())),
            (2, Some("bob-bio".into())),
            (3, None)
        ]
    );

    // Test 14: IS NULL works (common LEFT JOIN anti-join pattern)
    let res: Vec<i64> = conn.query(
        "SELECT users.id \
         FROM users LEFT JOIN posts ON users.id = posts.user_id \
         WHERE posts.id IS NULL \
         ORDER BY users.id",
    )?;
    assert_eq!(res, vec![3]);

    // Test 15: IS NOT NULL works
    let rows: Vec<i64> = conn.query(
        "SELECT COUNT(*) \
         FROM users LEFT JOIN posts ON users.id = posts.user_id \
         WHERE posts.id IS NOT NULL",
    )?;
    assert_eq!(rows[0], 3);

    // Test 16: NULL comparisons produce UNKNOWN (filtered out)
    let rows: Vec<i64> = conn.query(
        "SELECT COUNT(*) \
         FROM users LEFT JOIN posts ON users.id = posts.user_id \
         WHERE posts.id = NULL",
    )?;
    assert_eq!(rows[0], 0);

    // Test 17: NOT(UNKNOWN) stays UNKNOWN (still filtered out)
    let rows: Vec<i64> = conn.query(
        "SELECT COUNT(*) \
         FROM users LEFT JOIN posts ON users.id = posts.user_id \
         WHERE NOT (posts.id = NULL)",
    )?;
    assert_eq!(rows[0], 0);

    // Test 18: RIGHT JOIN should preserve unmatched right rows
    let res: Vec<(Option<i64>, i64)> = conn.query(
        "SELECT users.id, posts.id \
         FROM users RIGHT JOIN posts ON users.id = posts.user_id \
         ORDER BY posts.id",
    )?;
    assert_eq!(
        res,
        vec![(Some(1), 101), (Some(1), 102), (Some(2), 103), (None, 104)]
    );

    // Test 19: RIGHT JOIN ... USING(...) (preserve right = users)
    let res: Vec<(Option<String>, String)> = conn.query(
        "SELECT profiles.bio, users.name \
         FROM profiles RIGHT JOIN users USING (id) \
         ORDER BY users.id",
    )?;
    assert_eq!(
        res,
        vec![
            (Some("alice-bio".into()), "Alice".into()),
            (Some("bob-bio".into()), "Bob".into()),
            (None, "Charlie".into())
        ]
    );

    Ok(())
}
