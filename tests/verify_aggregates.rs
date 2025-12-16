mod common;

use mysql::prelude::*;

#[test]
fn test_aggregates() -> anyhow::Result<()> {
    let (_server, addr) = common::spawn_server()?;
    let url = format!("mysql://root:root@127.0.0.1:{}", addr.port());
    let pool = common::pool_for_url(&url)?;
    let mut conn = common::get_conn_with_retry(&pool, &url)?;

    // Create Table
    conn.query_drop("CREATE DATABASE IF NOT EXISTS testdb")?;
    conn.query_drop("USE testdb")?;
    conn.query_drop("DROP TABLE IF EXISTS sales")?;
    conn.query_drop(
        "CREATE TABLE sales (id INT PRIMARY KEY, region VARCHAR(255), amount INT, category VARCHAR(255))",
    )?;

    // Insert Data
    conn.query_drop("INSERT INTO sales VALUES (1, 'North', 100, 'A')")?;
    conn.query_drop("INSERT INTO sales VALUES (2, 'North', 200, 'B')")?;
    conn.query_drop("INSERT INTO sales VALUES (3, 'South', 50, 'A')")?;
    conn.query_drop("INSERT INTO sales VALUES (4, 'South', 150, 'A')")?;
    conn.query_drop("INSERT INTO sales VALUES (5, 'West', 300, 'C')")?;

    // Test 1: Count(*)
    let res: Vec<i64> = conn.query("SELECT COUNT(*) FROM sales")?;
    assert_eq!(res, vec![5]);

    // Test 2: Sum
    let res: Vec<i64> = conn.query("SELECT SUM(amount) FROM sales")?;
    assert_eq!(res, vec![800]);

    // Test 3: Group By
    // Row 1: North -> 300
    // Row 2: South -> 200
    // Row 3: West -> 300
    let res: Vec<(String, i64)> =
        conn.query("SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY region")?;
    assert_eq!(
        res,
        vec![
            ("North".into(), 300),
            ("South".into(), 200),
            ("West".into(), 300)
        ]
    );

    // Test 4: Group By Multiple (Not fully reachable with this data but good check)
    // North, A -> 100
    // North, B -> 200
    // South, A -> 200
    // West, C -> 300
    let res: Vec<(String, String, i64)> = conn.query(
        "SELECT region, category, SUM(amount) FROM sales GROUP BY region, category ORDER BY region, category",
    )?;
    assert_eq!(
        res,
        vec![
            ("North".into(), "A".into(), 100),
            ("North".into(), "B".into(), 200),
            ("South".into(), "A".into(), 200),
            ("West".into(), "C".into(), 300),
        ]
    );

    // Test 5: AVG, MIN, MAX
    // South: 50, 150 -> Min 50, Max 150, Avg 100
    let res: Vec<(String, i64, i64, f64)> = conn.query(
        "SELECT region, MIN(amount), MAX(amount), AVG(amount) FROM sales WHERE region = 'South' GROUP BY region",
    )?;
    // Avg might be float
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].0, "South");
    assert_eq!(res[0].1, 50); // Min
    assert_eq!(res[0].2, 150); // Max
    assert!((res[0].3 - 100.0).abs() < 0.001); // Avg

    // Test 6: HAVING
    // North: 300, South: 200, West: 300
    // HAVING total > 250 -> North, West
    let res: Vec<(String, i64)> = conn.query(
        "SELECT region, SUM(amount) as total FROM sales GROUP BY region HAVING total > 250 ORDER BY region",
    )?;
    assert_eq!(res, vec![("North".into(), 300), ("West".into(), 300)]);
    Ok(())
}
