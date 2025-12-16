use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

fn main() {
    let dialect = MySqlDialect {};
    let sql = "SELECT * FROM t ORDER BY a DESC, b ASC";
    match Parser::parse_sql(&dialect, sql) {
        Ok(ast) => println!("Parsed LIMIT: {:#?}", ast),
        Err(e) => println!("Error: {:?}", e),
    }
}
