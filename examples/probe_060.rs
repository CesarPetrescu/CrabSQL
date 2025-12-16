use sqlparser::ast::{OrderByKind, Statement};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

fn main() {
    let dialect = MySqlDialect {};
    let sql = "SELECT * FROM t ORDER BY a DESC, b ASC";
    let ast = Parser::parse_sql(&dialect, sql).unwrap();

    if let Statement::Query(query) = &ast[0] {
        if let Some(order_by) = &query.order_by {
            match &order_by.kind {
                OrderByKind::All(_) | OrderByKind::Expressions(_) => {}
            }
        }
    }
}
