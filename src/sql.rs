use crate::auth::{has_priv, Priv};
use crate::error::MiniError;
use crate::model::{Cell, ColumnDef, IndexDef, Row, SqlType, TableDef, TransactionId, UserRecord};
use crate::store::{ReadView, Store};
use opensrv_mysql::{Column, ColumnFlags, ColumnType};
use regex::Regex;

use sqlparser::ast::{self, Ident, ObjectName, ObjectNamePart, SetExpr, Statement, TableFactor};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::collections::{BTreeMap, HashSet};
use std::sync::OnceLock;

#[derive(Debug, Clone)]
pub struct SessionState {
    pub conn_id: u32,
    pub username: String,
    pub current_db: Option<String>,
    pub autocommit: bool,
    pub transaction_isolation: String,
    pub transaction_read_only: bool,
    pub sql_mode: String,
    pub time_zone: String,
    pub character_set_client: String,
    pub character_set_connection: String,
    pub character_set_results: String,
    pub collation_connection: String,
    txn: TransactionState,
}

impl SessionState {
    pub fn new(conn_id: u32) -> Self {
        Self {
            conn_id,
            username: "".into(),
            current_db: None,
            autocommit: true,
            transaction_isolation: "REPEATABLE-READ".into(),
            transaction_read_only: false,
            sql_mode: "".into(),
            time_zone: "SYSTEM".into(),
            character_set_client: "utf8".into(),
            character_set_connection: "utf8".into(),
            character_set_results: "utf8".into(),
            collation_connection: "utf8_general_ci".into(),
            txn: TransactionState::default(),
        }
    }

    pub fn in_transaction(&self) -> bool {
        self.txn.in_txn
    }
}

#[derive(Debug, Default, Clone)]
struct TransactionState {
    in_txn: bool,
    tx_id: Option<TransactionId>,
    read_view: Option<ReadView>,
    pending_rows: BTreeMap<RowKey, Option<Row>>,
    savepoints: Vec<(String, BTreeMap<RowKey, Option<Row>>)>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RowKey {
    db: String,
    table: String,
    pk: i64,
}

struct RowLockGuard<'a> {
    store: &'a Store,
    owner: u32,
    acquired: Vec<RowKey>,
    keep: bool,
}

impl<'a> RowLockGuard<'a> {
    fn new(store: &'a Store, owner: u32) -> Self {
        Self {
            store,
            owner,
            acquired: Vec::new(),
            keep: false,
        }
    }

    fn lock_row(&mut self, db: &str, table: &str, pk: i64) -> Result<(), MiniError> {
        let newly_acquired = self.store.lock_row(self.owner, db, table, pk)?;
        if newly_acquired {
            self.acquired.push(RowKey {
                db: db.to_string(),
                table: table.to_string(),
                pk,
            });
        }
        Ok(())
    }

    fn keep_locks(&mut self) {
        self.keep = true;
    }
}

impl Drop for RowLockGuard<'_> {
    fn drop(&mut self) {
        if self.keep {
            return;
        }
        for key in &self.acquired {
            self.store
                .unlock_row(self.owner, &key.db, &key.table, key.pk);
        }
    }
}

#[derive(Debug)]
pub enum ExecOutput {
    Ok {
        affected_rows: u64,
        last_insert_id: u64,
        info: String,
    },
    ResultSet {
        columns: Vec<Column>,
        rows: Vec<Vec<Cell>>,
    },
}

pub const SERVER_VERSION: &str = "8.0.0-rusty-mini-mysql";
pub const VERSION_COMMENT: &str = "rusty-mini-mysql";

fn strip_trailing_semicolon(s: &str) -> &str {
    let s = s.trim();
    if let Some(stripped) = s.strip_suffix(';') {
        stripped.trim()
    } else {
        s
    }
}

fn strip_leading_comments(mut s: &str) -> &str {
    // Handle common /* ... */ prefix comments.
    loop {
        let t = s.trim_start();
        if let Some(rest) = t.strip_prefix("/*") {
            if let Some(end) = rest.find("*/") {
                s = &rest[end + 2..];
                continue;
            }
        }
        return t;
    }
}

fn split_sql_tokens(query: &str) -> Vec<&str> {
    let mut tokens = Vec::new();
    let mut start: Option<usize> = None;
    let mut in_sq = false;
    let mut in_bq = false;
    let mut chars = query.char_indices().peekable();

    while let Some((i, ch)) = chars.next() {
        match ch {
            '\'' if !in_bq => {
                if start.is_none() {
                    start = Some(i);
                }
                if in_sq {
                    if let Some((_, '\'')) = chars.peek() {
                        chars.next();
                    } else {
                        in_sq = false;
                    }
                } else {
                    in_sq = true;
                }
            }
            '`' if !in_sq => {
                if start.is_none() {
                    start = Some(i);
                }
                in_bq = !in_bq;
            }
            w if w.is_whitespace() && !in_sq && !in_bq => {
                if let Some(s) = start.take() {
                    if s < i {
                        tokens.push(&query[s..i]);
                    }
                }
            }
            _ => {
                if start.is_none() {
                    start = Some(i);
                }
            }
        }
    }

    if let Some(s) = start {
        if s < query.len() {
            tokens.push(&query[s..]);
        }
    }

    tokens
}

fn unquote_identifier(token: &str) -> String {
    let t = token.trim();
    if let Some(stripped) = t.strip_prefix('`').and_then(|s| s.strip_suffix('`')) {
        stripped.replace("``", "`")
    } else if let Some(stripped) = t.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
        stripped.replace("\"\"", "\"")
    } else {
        t.to_string()
    }
}

fn unquote_string_literal(token: &str) -> Result<String, MiniError> {
    let t = token.trim();
    if let Some(stripped) = t.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')) {
        Ok(stripped.replace("''", "'"))
    } else if let Some(stripped) = t.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
        Ok(stripped.replace("\"\"", "\""))
    } else {
        Ok(t.to_string())
    }
}

fn parse_db_table_token(token: &str) -> Result<(Option<String>, String), MiniError> {
    let parts: Vec<&str> = token.split('.').collect();
    match parts.len() {
        1 => Ok((None, unquote_identifier(parts[0]))),
        2 => Ok((
            Some(unquote_identifier(parts[0])),
            unquote_identifier(parts[1]),
        )),
        _ => Err(MiniError::NotSupported(
            "qualified names with more than 2 parts are not supported".into(),
        )),
    }
}

fn require_priv(user: &UserRecord, db: Option<&str>, needed: Priv) -> Result<(), MiniError> {
    if has_priv(user, db, needed) {
        Ok(())
    } else {
        Err(MiniError::AccessDenied(format!(
            "missing privilege {needed:?}"
        )))
    }
}

const SYSTEM_VARIABLES: &[&str] = &[
    "autocommit",
    "version",
    "version_comment",
    "transaction_isolation",
    "tx_isolation",
    "transaction_read_only",
    "sql_mode",
    "time_zone",
    "character_set_client",
    "character_set_connection",
    "character_set_results",
    "collation_connection",
    "lower_case_table_names",
    "max_allowed_packet",
    "socket",
];

const SYSTEM_SCHEMAS: &[&str] = &["information_schema", "mysql", "performance_schema", "sys"];

const INFORMATION_SCHEMA_TABLES: &[&str] = &["SCHEMATA", "TABLES", "COLUMNS", "STATISTICS"];

fn is_system_schema(name: &str) -> bool {
    SYSTEM_SCHEMAS
        .iter()
        .any(|s| s.eq_ignore_ascii_case(name.trim()))
}

fn is_information_schema(name: &str) -> bool {
    name.trim().eq_ignore_ascii_case("information_schema")
}

fn list_all_databases(store: &Store) -> Result<Vec<String>, MiniError> {
    let mut dbs = store.list_databases()?;
    for sys in SYSTEM_SCHEMAS {
        if !dbs.iter().any(|d| d.eq_ignore_ascii_case(sys)) {
            dbs.push((*sys).to_string());
        }
    }
    dbs.sort_by_key(|name| name.to_ascii_lowercase());
    Ok(dbs)
}

fn information_schema_table_names() -> Vec<String> {
    let mut out: Vec<String> = INFORMATION_SCHEMA_TABLES
        .iter()
        .map(|t| t.to_string())
        .collect();
    out.sort();
    out
}

fn sysvar_value(session: &SessionState, name: &str) -> Option<Cell> {
    let name = name.trim().to_ascii_lowercase();
    match name.as_str() {
        "autocommit" => Some(Cell::Int(if session.autocommit { 1 } else { 0 })),
        "version" => Some(Cell::Text(SERVER_VERSION.to_string())),
        "version_comment" => Some(Cell::Text(VERSION_COMMENT.to_string())),
        "transaction_isolation" | "tx_isolation" => {
            Some(Cell::Text(session.transaction_isolation.clone()))
        }
        "transaction_read_only" => {
            Some(Cell::Int(if session.transaction_read_only { 1 } else { 0 }))
        }
        "sql_mode" => Some(Cell::Text(session.sql_mode.clone())),
        "time_zone" => Some(Cell::Text(session.time_zone.clone())),
        "character_set_client" => Some(Cell::Text(session.character_set_client.clone())),
        "character_set_connection" => Some(Cell::Text(session.character_set_connection.clone())),
        "character_set_results" => Some(Cell::Text(session.character_set_results.clone())),
        "collation_connection" => Some(Cell::Text(session.collation_connection.clone())),
        "lower_case_table_names" => Some(Cell::Int(0)),
        "max_allowed_packet" => Some(Cell::Int(64 * 1024 * 1024)),
        "socket" => Some(Cell::Text("".into())),
        _ => None,
    }
}

fn sysvar_show_value(session: &SessionState, name: &str) -> Option<String> {
    let name = name.trim().to_ascii_lowercase();
    match name.as_str() {
        "autocommit" => Some(if session.autocommit { "ON" } else { "OFF" }.to_string()),
        "transaction_read_only" => Some(
            if session.transaction_read_only {
                "ON"
            } else {
                "OFF"
            }
            .to_string(),
        ),
        _ => sysvar_value(session, &name).map(|c| cell_to_string(&c)),
    }
}

fn like_matches(pattern: &str, value: &str) -> bool {
    let to_regex = |pat: &str| {
        let mut out = String::with_capacity(pat.len() * 2);
        for ch in pat.chars() {
            match ch {
                '%' => out.push_str(".*"),
                '_' => out.push('.'),
                other => out.push_str(&regex::escape(&other.to_string())),
            }
        }
        out
    };
    let re = format!("(?i)^{}$", to_regex(pattern));
    Regex::new(&re).ok().is_some_and(|r| r.is_match(value))
}

fn try_handle_select_sysvar(
    query: &str,
    session: &SessionState,
) -> Option<Result<ExecOutput, MiniError>> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        Regex::new(
            r#"(?is)^\s*select\s+@@(?:(session|global)\.)?([a-z0-9_]+)\s*(?:as\s+([a-z0-9_`"']+))?\s*(?:limit\s+(\d+))?\s*$"#,
        )
        .expect("valid sysvar select regex")
    });
    let caps = re.captures(query)?;

    let scope = caps.get(1).map(|m| m.as_str().to_ascii_lowercase());
    let var_name = caps.get(2).unwrap().as_str();
    let alias_raw = caps.get(3).map(|m| m.as_str());

    let col_name = if let Some(a) = alias_raw {
        let t = a.trim();
        let unquoted = t
            .trim_start_matches('`')
            .trim_end_matches('`')
            .trim_start_matches('"')
            .trim_end_matches('"')
            .trim_start_matches('\'')
            .trim_end_matches('\'');
        unquoted.to_string()
    } else if let Some(scope) = scope {
        format!("@@{scope}.{var_name}")
    } else {
        format!("@@{var_name}")
    };

    let Some(value) = sysvar_value(session, var_name) else {
        return Some(Err(MiniError::UnknownSystemVariable(var_name.to_string())));
    };

    let coltype = match value {
        Cell::Int(_) => ColumnType::MYSQL_TYPE_LONGLONG,
        _ => ColumnType::MYSQL_TYPE_VAR_STRING,
    };

    Some(Ok(ExecOutput::ResultSet {
        columns: vec![Column {
            table: "".into(),
            column: col_name,
            coltype,
            colflags: ColumnFlags::empty(),
        }],
        rows: vec![vec![value]],
    }))
}

fn try_handle_show_index(
    query: &str,
    store: &Store,
    session: &SessionState,
    user: &UserRecord,
) -> Option<Result<ExecOutput, MiniError>> {
    let tokens = split_sql_tokens(query);
    if tokens.len() < 2 {
        return None;
    }
    if !tokens[0].eq_ignore_ascii_case("show") {
        return None;
    }
    let kind = tokens[1].to_ascii_lowercase();
    if kind != "index" && kind != "indexes" && kind != "keys" {
        return None;
    }
    if tokens.len() < 4 {
        return Some(Err(MiniError::Parse(
            "SHOW INDEX requires FROM <table>".into(),
        )));
    }
    if !tokens[2].eq_ignore_ascii_case("from") && !tokens[2].eq_ignore_ascii_case("in") {
        return Some(Err(MiniError::Parse(
            "SHOW INDEX requires FROM <table>".into(),
        )));
    }

    let table_tok = tokens[3];
    let mut consumed = 4;
    let mut db_override: Option<&str> = None;
    if tokens.len() >= consumed + 2
        && (tokens[consumed].eq_ignore_ascii_case("from")
            || tokens[consumed].eq_ignore_ascii_case("in"))
    {
        db_override = Some(tokens[consumed + 1]);
        consumed += 2;
    }
    if tokens.len() > consumed {
        return Some(Err(MiniError::NotSupported(
            "SHOW INDEX filters are not supported".into(),
        )));
    }

    let (db_from_table, table) = match parse_db_table_token(table_tok) {
        Ok(v) => v,
        Err(e) => return Some(Err(e)),
    };
    let db = db_override
        .map(unquote_identifier)
        .or(db_from_table)
        .or_else(|| session.current_db.clone())
        .ok_or_else(|| MiniError::Invalid("no database selected".into()));
    let db = match db {
        Ok(db) => db,
        Err(e) => return Some(Err(e)),
    };
    if let Err(e) = require_priv(user, Some(&db), Priv::SELECT) {
        return Some(Err(e));
    }

    let def = match store.get_table(&db, &table) {
        Ok(def) => def,
        Err(e) => return Some(Err(e)),
    };
    let pk_name = def.primary_key.clone();
    let pk_nullable = def
        .columns
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(&pk_name))
        .map(|c| c.nullable)
        .unwrap_or(false);
    let cardinality = match store.count_rows(&db, &table) {
        Ok(n) => n.min(i64::MAX as u64) as i64,
        Err(e) => return Some(Err(e)),
    };

    let mut rows = Vec::new();

    // 1. PRIMARY KEY
    rows.push(vec![
        Cell::Text(def.name.clone()),
        Cell::Int(0),
        Cell::Text("PRIMARY".into()),
        Cell::Int(1),
        Cell::Text(pk_name.clone()),
        Cell::Text("A".into()),
        Cell::Int(cardinality),
        Cell::Null,
        Cell::Null,
        Cell::Text(if pk_nullable { "YES" } else { "NO" }.into()),
        Cell::Text("BTREE".into()),
        Cell::Text("".into()),
        Cell::Text("".into()),
        Cell::Text("YES".into()),
        Cell::Null,
    ]);

    // 2. Secondary Indexes
    for idx in &def.indexes {
        for (seq, col) in idx.columns.iter().enumerate() {
            rows.push(vec![
                Cell::Text(def.name.clone()),
                Cell::Int(1), // Non_unique
                Cell::Text(idx.name.clone()),
                Cell::Int((seq + 1) as i64),
                Cell::Text(col.clone()),
                Cell::Text("A".into()),
                Cell::Null,
                Cell::Null,
                Cell::Null,
                Cell::Text("YES".into()),
                Cell::Text("BTREE".into()),
                Cell::Text("".into()),
                Cell::Text("".into()),
                Cell::Text("YES".into()),
                Cell::Null,
            ]);
        }
    }

    Some(Ok(ExecOutput::ResultSet {
        columns: vec![
            Column {
                table: "".into(),
                column: "Table".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Non_unique".into(),
                coltype: ColumnType::MYSQL_TYPE_LONGLONG,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Key_name".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Seq_in_index".into(),
                coltype: ColumnType::MYSQL_TYPE_LONGLONG,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Column_name".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Collation".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Cardinality".into(),
                coltype: ColumnType::MYSQL_TYPE_LONGLONG,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Sub_part".into(),
                coltype: ColumnType::MYSQL_TYPE_LONGLONG,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Packed".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Null".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Index_type".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Comment".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Index_comment".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Visible".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Expression".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
        ],
        rows,
    }))
}
fn try_handle_show_table_status(
    query: &str,
    store: &Store,
    session: &SessionState,
    user: &UserRecord,
) -> Option<Result<ExecOutput, MiniError>> {
    let tokens = split_sql_tokens(query);
    if tokens.len() < 3 {
        return None;
    }
    if !tokens[0].eq_ignore_ascii_case("show")
        || !tokens[1].eq_ignore_ascii_case("table")
        || !tokens[2].eq_ignore_ascii_case("status")
    {
        return None;
    }

    let mut idx = 3usize;
    let mut db_override: Option<String> = None;
    let mut like_pattern: Option<String> = None;
    while idx < tokens.len() {
        if tokens[idx].eq_ignore_ascii_case("from") || tokens[idx].eq_ignore_ascii_case("in") {
            if idx + 1 >= tokens.len() {
                return Some(Err(MiniError::Parse(
                    "SHOW TABLE STATUS requires a database name".into(),
                )));
            }
            db_override = Some(unquote_identifier(tokens[idx + 1]));
            idx += 2;
            continue;
        }
        if tokens[idx].eq_ignore_ascii_case("like") {
            if idx + 1 >= tokens.len() {
                return Some(Err(MiniError::Parse(
                    "SHOW TABLE STATUS LIKE requires a pattern".into(),
                )));
            }
            match unquote_string_literal(tokens[idx + 1]) {
                Ok(pat) => like_pattern = Some(pat),
                Err(e) => return Some(Err(e)),
            }
            idx += 2;
            continue;
        }
        if tokens[idx].eq_ignore_ascii_case("where") {
            return Some(Err(MiniError::NotSupported(
                "SHOW TABLE STATUS WHERE is not supported".into(),
            )));
        }
        return Some(Err(MiniError::NotSupported(format!(
            "SHOW TABLE STATUS option not supported: {}",
            tokens[idx]
        ))));
    }

    let db = db_override
        .or_else(|| session.current_db.clone())
        .ok_or_else(|| MiniError::Invalid("no database selected".into()));
    let db = match db {
        Ok(db) => db,
        Err(e) => return Some(Err(e)),
    };
    if let Err(e) = require_priv(user, Some(&db), Priv::SELECT) {
        return Some(Err(e));
    }

    let tables = match store.list_tables(&db) {
        Ok(t) => t,
        Err(e) => return Some(Err(e)),
    };

    let mut rows = Vec::new();
    for table in tables {
        if let Some(pat) = like_pattern.as_deref() {
            if !like_matches(pat, &table) {
                continue;
            }
        }
        let row_count = match store.count_rows(&db, &table) {
            Ok(n) => n.min(i64::MAX as u64) as i64,
            Err(e) => return Some(Err(e)),
        };
        let auto_inc = match store.get_table(&db, &table) {
            Ok(def) if def.auto_increment => match store.auto_increment_next(&db, &table) {
                Ok(Some(next)) => Cell::Int(next),
                Ok(None) => Cell::Int(1),
                Err(e) => return Some(Err(e)),
            },
            Ok(_) => Cell::Null,
            Err(e) => return Some(Err(e)),
        };
        rows.push(vec![
            Cell::Text(table),
            Cell::Text("InnoDB".into()),
            Cell::Int(10),
            Cell::Text("Dynamic".into()),
            Cell::Int(row_count),
            Cell::Int(0),
            Cell::Int(0),
            Cell::Int(0),
            Cell::Int(0),
            Cell::Int(0),
            auto_inc,
            Cell::Null,
            Cell::Null,
            Cell::Null,
            Cell::Text(session.collation_connection.clone()),
            Cell::Null,
            Cell::Text("".into()),
            Cell::Text("".into()),
        ]);
    }

    Some(Ok(ExecOutput::ResultSet {
        columns: vec![
            Column {
                table: "".into(),
                column: "Name".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Engine".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Version".into(),
                coltype: ColumnType::MYSQL_TYPE_LONGLONG,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Row_format".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Rows".into(),
                coltype: ColumnType::MYSQL_TYPE_LONGLONG,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Avg_row_length".into(),
                coltype: ColumnType::MYSQL_TYPE_LONGLONG,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Data_length".into(),
                coltype: ColumnType::MYSQL_TYPE_LONGLONG,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Max_data_length".into(),
                coltype: ColumnType::MYSQL_TYPE_LONGLONG,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Index_length".into(),
                coltype: ColumnType::MYSQL_TYPE_LONGLONG,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Data_free".into(),
                coltype: ColumnType::MYSQL_TYPE_LONGLONG,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Auto_increment".into(),
                coltype: ColumnType::MYSQL_TYPE_LONGLONG,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Create_time".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Update_time".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Check_time".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Collation".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Checksum".into(),
                coltype: ColumnType::MYSQL_TYPE_LONGLONG,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Create_options".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Comment".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
        ],
        rows,
    }))
}

pub fn execute(
    raw_query: &str,
    store: &Store,
    session: &mut SessionState,
    user: &UserRecord,
) -> Result<ExecOutput, MiniError> {
    let q = strip_trailing_semicolon(strip_leading_comments(raw_query));
    if q.is_empty() {
        return Ok(ExecOutput::Ok {
            affected_rows: 0,
            last_insert_id: 0,
            info: "".into(),
        });
    }

    if let Some(out) = try_handle_select_sysvar(q, session) {
        return out;
    }
    if let Some(out) = try_handle_show_index(q, store, session, user) {
        return out;
    }
    if let Some(out) = try_handle_show_table_status(q, store, session, user) {
        return out;
    }

    let dialect = MySqlDialect {};
    let ast = match Parser::parse_sql(&dialect, q) {
        Ok(ast) => ast,
        Err(e) => {
            return Err(MiniError::Parse(e.to_string()));
        }
    };

    if ast.is_empty() {
        return Ok(ExecOutput::Ok {
            affected_rows: 0,
            last_insert_id: 0,
            info: "".into(),
        });
    }

    let stmt = &ast[0];
    match stmt {
        Statement::StartTransaction { .. } => {
            // Implicitly commit previous if exists (MySQL behavior)
            if session.txn.tx_id.is_some() {
                txn_commit(store, session)?;
            }
            ensure_txn_active(store, session);
            session.txn.in_txn = true;
            Ok(ExecOutput::Ok {
                affected_rows: 0,
                last_insert_id: 0,
                info: "".into(),
            })
        }
        Statement::Commit { .. } => {
            txn_commit(store, session)?;
            // Explicit commit ends the transaction block.
            session.txn.in_txn = false;
            Ok(ExecOutput::Ok {
                affected_rows: 0,
                last_insert_id: 0,
                info: "".into(),
            })
        }
        Statement::Rollback {
            savepoint: Some(name),
            ..
        } => handle_rollback_to_savepoint(session, name),
        Statement::Rollback { .. } => {
            txn_rollback(store, session);
            session.txn.in_txn = false;
            Ok(ExecOutput::Ok {
                affected_rows: 0,
                last_insert_id: 0,
                info: "".into(),
            })
        }
        Statement::Savepoint { name } => handle_savepoint(session, name),
        Statement::ReleaseSavepoint { name } => handle_release_savepoint(session, name),
        Statement::ShowColumns { .. } | Statement::ShowCreate { .. } => {
            // These use internal helpers that don't scan rows usually, or use store.get_table which is catalog.
            // Catalog is not MVCC yet.
            // But let's ensure we are in a txn just in case.
            ensure_txn_active(store, session);
            match stmt {
                Statement::ShowColumns { .. } => handle_show_columns(store, session, user, stmt),
                Statement::ShowCreate { .. } => handle_show_create(store, session, user, stmt),
                _ => unreachable!(),
            }
        }
        // Catch-all for other statements that need implicit txn
        _ => {
            ensure_txn_active(store, session);
            let res = match stmt {
                Statement::Set(set) => handle_set(store, session, set),
                Statement::CreateDatabase {
                    db_name,
                    if_not_exists,
                    ..
                } => handle_create_database(store, session, user, db_name, *if_not_exists),
                Statement::Drop {
                    object_type: ast::ObjectType::Schema | ast::ObjectType::Database,
                    names,
                    if_exists,
                    ..
                } => {
                    if names.is_empty() {
                        return Err(MiniError::Parse("No database name".into()));
                    }
                    handle_drop_database(store, session, user, &names[0], *if_exists)
                }
                Statement::CreateTable(c) => handle_create_table(
                    store,
                    session,
                    user,
                    &c.name,
                    &c.columns,
                    &c.constraints,
                    c.if_not_exists,
                ),
                Statement::AlterTable(alter) => handle_alter_table(store, session, user, alter),
                Statement::Drop {
                    object_type: ast::ObjectType::Table,
                    names,
                    if_exists,
                    ..
                } => {
                    if names.is_empty() {
                        return Err(MiniError::Parse("No table name".into()));
                    }
                    handle_drop_table(store, session, user, &names[0], *if_exists)
                }
                Statement::Use(use_stmt) => handle_use(store, session, use_stmt),
                Statement::ShowDatabases { show_options, .. } => {
                    handle_show_databases(store, session, user, show_options)
                }
                Statement::ShowTables { .. } => handle_show_tables(store, session, user, stmt),
                Statement::CreateIndex(create_index) => {
                    handle_create_index(store, session, user, create_index)
                }
                Statement::ExplainTable { table_name, .. } => {
                    handle_describe_table(store, session, user, table_name)
                }
                Statement::Query(q) => handle_query(store, session, user, q),
                Statement::Insert(insert) => handle_insert(store, session, user, insert),
                Statement::Update(update) => handle_update(store, session, user, update),
                Statement::Delete(delete) => handle_delete(store, session, user, delete),
                Statement::ShowVariables {
                    filter,
                    global,
                    session: session_scope,
                } => handle_show_variables(session, filter.as_ref(), *global, *session_scope),
                _ => Err(MiniError::NotSupported(format!(
                    "Statement not implemented: {:?}",
                    stmt
                ))),
            };

            // Implicit Commit if needed
            if !session.txn.in_txn {
                if res.is_ok() {
                    txn_commit(store, session)?;
                } else {
                    txn_rollback(store, session);
                }
            }
            res
        }
    }
}

fn show_columns_result(
    session: &SessionState,
    def: &TableDef,
    like_pattern: Option<&str>,
    full: bool,
) -> ExecOutput {
    let columns = if full {
        vec![
            Column {
                table: "".into(),
                column: "Field".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Type".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Collation".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Null".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Key".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Default".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Extra".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Privileges".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Comment".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
        ]
    } else {
        vec![
            Column {
                table: "".into(),
                column: "Field".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Type".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Null".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Key".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Default".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Extra".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
        ]
    };

    let mut rows = Vec::new();
    for col in &def.columns {
        if let Some(pat) = like_pattern {
            if !like_matches(pat, &col.name) {
                continue;
            }
        }

        let ty = match col.ty {
            SqlType::Int => "bigint",
            SqlType::Text => "text",
            SqlType::Float => "double",
            SqlType::Date => "date",
            SqlType::DateTime => "datetime",
        };
        let null = if col.nullable { "YES" } else { "NO" };
        let key = if col.name.eq_ignore_ascii_case(&def.primary_key) {
            "PRI"
        } else {
            ""
        };
        let extra = if def.auto_increment && col.name.eq_ignore_ascii_case(&def.primary_key) {
            "auto_increment"
        } else {
            ""
        };

        if full {
            let collation = match col.ty {
                SqlType::Text => Cell::Text(session.collation_connection.clone()),
                _ => Cell::Null,
            };
            rows.push(vec![
                Cell::Text(col.name.clone()),
                Cell::Text(ty.to_string()),
                collation,
                Cell::Text(null.to_string()),
                Cell::Text(key.to_string()),
                Cell::Null,
                Cell::Text(extra.into()),
                Cell::Text("select,insert,update,references".into()),
                Cell::Text("".into()),
            ]);
        } else {
            rows.push(vec![
                Cell::Text(col.name.clone()),
                Cell::Text(ty.to_string()),
                Cell::Text(null.to_string()),
                Cell::Text(key.to_string()),
                Cell::Null,
                Cell::Text(extra.into()),
            ]);
        }
    }

    ExecOutput::ResultSet { columns, rows }
}

fn handle_show_databases(
    store: &Store,
    _session: &SessionState,
    user: &UserRecord,
    show_options: &ast::ShowStatementOptions,
) -> Result<ExecOutput, MiniError> {
    require_priv(user, None, Priv::SELECT)?;
    let dbs = list_all_databases(store)?;

    let filter = show_options.filter_position.as_ref().map(|pos| match pos {
        ast::ShowStatementFilterPosition::Infix(f)
        | ast::ShowStatementFilterPosition::Suffix(f) => f,
    });
    let like_pattern = match filter {
        None => None,
        Some(ast::ShowStatementFilter::Like(p))
        | Some(ast::ShowStatementFilter::ILike(p))
        | Some(ast::ShowStatementFilter::NoKeyword(p)) => Some(p.as_str()),
        Some(ast::ShowStatementFilter::Where(_)) => {
            return Err(MiniError::NotSupported(
                "SHOW DATABASES WHERE is not supported".into(),
            ));
        }
    };

    let cols = vec![Column {
        table: "".into(),
        column: "Database".into(),
        coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
        colflags: ColumnFlags::empty(),
    }];

    let rows = dbs
        .into_iter()
        .filter(|db| like_pattern.is_none_or(|pat| like_matches(pat, db)))
        .map(|d| vec![Cell::Text(d)])
        .collect();
    Ok(ExecOutput::ResultSet {
        columns: cols,
        rows,
    })
}

fn handle_show_tables(
    store: &Store,
    session: &mut SessionState,
    user: &UserRecord,
    stmt: &Statement,
) -> Result<ExecOutput, MiniError> {
    let (full, show_options) = match stmt {
        Statement::ShowTables {
            full, show_options, ..
        } => (*full, show_options),
        _ => unreachable!(),
    };

    let db = show_options
        .show_in
        .as_ref()
        .and_then(|show_in| show_in.parent_name.as_ref())
        .map(|name| get_ident_name(name.0.last().unwrap()))
        .or_else(|| session.current_db.clone())
        .ok_or_else(|| MiniError::Invalid("no database selected".into()))?;
    if is_system_schema(&db) {
        require_priv(user, None, Priv::SELECT)?;
    } else {
        require_priv(user, Some(&db), Priv::SELECT)?;
    }

    let tables = if is_information_schema(&db) {
        information_schema_table_names()
    } else if is_system_schema(&db) {
        Vec::new()
    } else {
        store.list_tables(&db)?
    };

    let filter = show_options.filter_position.as_ref().map(|pos| match pos {
        ast::ShowStatementFilterPosition::Infix(f)
        | ast::ShowStatementFilterPosition::Suffix(f) => f,
    });
    let like_pattern = match filter {
        None => None,
        Some(ast::ShowStatementFilter::Like(p))
        | Some(ast::ShowStatementFilter::ILike(p))
        | Some(ast::ShowStatementFilter::NoKeyword(p)) => Some(p.as_str()),
        Some(ast::ShowStatementFilter::Where(_)) => {
            return Err(MiniError::NotSupported(
                "SHOW TABLES WHERE is not supported".into(),
            ));
        }
    };

    let mut cols = vec![Column {
        table: "".into(),
        column: format!("Tables_in_{db}"),
        coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
        colflags: ColumnFlags::empty(),
    }];
    if full {
        cols.push(Column {
            table: "".into(),
            column: "Table_type".into(),
            coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
            colflags: ColumnFlags::empty(),
        });
    }

    let rows = tables
        .into_iter()
        .filter(|t| like_pattern.is_none_or(|pat| like_matches(pat, t)))
        .map(|t| {
            if full {
                vec![Cell::Text(t), Cell::Text("BASE TABLE".into())]
            } else {
                vec![Cell::Text(t)]
            }
        })
        .collect();
    Ok(ExecOutput::ResultSet {
        columns: cols,
        rows,
    })
}

fn handle_show_columns(
    store: &Store,
    session: &mut SessionState,
    user: &UserRecord,
    stmt: &Statement,
) -> Result<ExecOutput, MiniError> {
    let (_extended, full, show_options) = match stmt {
        Statement::ShowColumns {
            extended,
            full,
            show_options,
        } => (*extended, *full, show_options),
        _ => unreachable!(),
    };

    require_priv(user, session.current_db.as_deref(), Priv::SELECT)?;

    let Some(show_in) = &show_options.show_in else {
        return Err(MiniError::Parse(
            "SHOW COLUMNS requires a table name".into(),
        ));
    };
    let Some(obj_name) = &show_in.parent_name else {
        return Err(MiniError::Parse(
            "SHOW COLUMNS requires a table name".into(),
        ));
    };

    let (db_opt, table) = object_name_to_parts(obj_name)?;
    let db = db_opt
        .or_else(|| session.current_db.clone())
        .ok_or_else(|| MiniError::Invalid("no database selected".into()))?;
    let def = store.get_table(&db, &table)?;

    let filter = show_options.filter_position.as_ref().map(|pos| match pos {
        ast::ShowStatementFilterPosition::Infix(f)
        | ast::ShowStatementFilterPosition::Suffix(f) => f,
    });
    let like_pattern = match filter {
        None => None,
        Some(ast::ShowStatementFilter::Like(p))
        | Some(ast::ShowStatementFilter::ILike(p))
        | Some(ast::ShowStatementFilter::NoKeyword(p)) => Some(p.as_str()),
        Some(ast::ShowStatementFilter::Where(_)) => {
            return Err(MiniError::NotSupported(
                "SHOW COLUMNS WHERE is not supported".into(),
            ));
        }
    };

    Ok(show_columns_result(session, &def, like_pattern, full))
}

fn handle_describe_table(
    store: &Store,
    session: &SessionState,
    user: &UserRecord,
    table_name: &ObjectName,
) -> Result<ExecOutput, MiniError> {
    let (db_opt, table) = object_name_to_parts(table_name)?;
    let db = db_opt
        .or_else(|| session.current_db.clone())
        .ok_or_else(|| MiniError::Invalid("no database selected".into()))?;
    require_priv(user, Some(&db), Priv::SELECT)?;
    let def = store.get_table(&db, &table)?;
    Ok(show_columns_result(session, &def, None, false))
}

fn handle_show_create(
    store: &Store,
    session: &mut SessionState,
    user: &UserRecord,
    stmt: &Statement,
) -> Result<ExecOutput, MiniError> {
    let (obj_type, obj_name) = match stmt {
        Statement::ShowCreate { obj_type, obj_name } => (obj_type, obj_name),
        _ => unreachable!(),
    };

    require_priv(user, session.current_db.as_deref(), Priv::SELECT)?;
    if *obj_type != ast::ShowCreateObject::Table {
        return Err(MiniError::NotSupported(
            "Only SHOW CREATE TABLE is supported".into(),
        ));
    }

    let (db_opt, table) = object_name_to_parts(obj_name)?;
    let db = db_opt
        .or_else(|| session.current_db.clone())
        .ok_or_else(|| MiniError::Invalid("no database selected".into()))?;
    let def = store.get_table(&db, &table)?;

    let mut parts = Vec::new();
    for col in &def.columns {
        let ty = match col.ty {
            SqlType::Int => "BIGINT",
            SqlType::Text => "TEXT",
            SqlType::Float => "DOUBLE",
            SqlType::Date => "DATE",
            SqlType::DateTime => "DATETIME",
        };
        let mut line = format!("`{}` {}", col.name, ty);
        if !col.nullable {
            line.push_str(" NOT NULL");
        }
        if def.auto_increment && col.name.eq_ignore_ascii_case(&def.primary_key) {
            line.push_str(" AUTO_INCREMENT");
        }
        parts.push(line);
    }
    parts.push(format!("PRIMARY KEY (`{}`)", def.primary_key));
    let create = format!("CREATE TABLE `{}` ({})", def.name, parts.join(", "));

    Ok(ExecOutput::ResultSet {
        columns: vec![
            Column {
                table: "".into(),
                column: "Table".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
            Column {
                table: "".into(),
                column: "Create Table".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            },
        ],
        rows: vec![vec![Cell::Text(def.name), Cell::Text(create)]],
    })
}

fn handle_show_variables(
    session: &SessionState,
    filter: Option<&ast::ShowStatementFilter>,
    _global: bool,
    _session_scope: bool,
) -> Result<ExecOutput, MiniError> {
    let cols = vec![
        Column {
            table: "".into(),
            column: "Variable_name".into(),
            coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
            colflags: ColumnFlags::empty(),
        },
        Column {
            table: "".into(),
            column: "Value".into(),
            coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
            colflags: ColumnFlags::empty(),
        },
    ];

    let mut rows = Vec::new();
    for name in SYSTEM_VARIABLES {
        let matches = match filter {
            None => true,
            Some(ast::ShowStatementFilter::Like(p))
            | Some(ast::ShowStatementFilter::ILike(p))
            | Some(ast::ShowStatementFilter::NoKeyword(p)) => like_matches(p, name),
            Some(ast::ShowStatementFilter::Where(_)) => {
                return Err(MiniError::NotSupported(
                    "SHOW VARIABLES WHERE is not supported".into(),
                ));
            }
        };
        if !matches {
            continue;
        }
        let Some(val) = sysvar_show_value(session, name) else {
            continue;
        };
        rows.push(vec![Cell::Text(name.to_string()), Cell::Text(val)]);
    }

    Ok(ExecOutput::ResultSet {
        columns: cols,
        rows,
    })
}

fn handle_use(
    store: &Store,
    session: &mut SessionState,
    use_stmt: &ast::Use,
) -> Result<ExecOutput, MiniError> {
    let name = match use_stmt {
        ast::Use::Object(name) | ast::Use::Database(name) => name,
        _ => return Err(MiniError::NotSupported("Only USE <db> is supported".into())),
    };

    let db = get_ident_name(name.0.last().unwrap());
    let dbs = list_all_databases(store)?;
    if !dbs.iter().any(|d| d.eq_ignore_ascii_case(&db)) {
        return Err(MiniError::NotFound(format!("unknown database: {db}")));
    }
    session.current_db = Some(db);
    Ok(ExecOutput::Ok {
        affected_rows: 0,
        last_insert_id: 0,
        info: "".into(),
    })
}

fn handle_set(
    store: &Store,
    session: &mut SessionState,
    set: &ast::Set,
) -> Result<ExecOutput, MiniError> {
    let mut maybe_commit_on_enable_autocommit = false;

    let parse_bool = |expr: &ast::Expr| -> Result<bool, MiniError> {
        let v = eval_expr(expr)?;
        match v {
            Cell::Int(n) => Ok(n != 0),
            Cell::Text(s) => {
                let t = s.trim();
                if t.eq_ignore_ascii_case("on") || t.eq_ignore_ascii_case("true") || t == "1" {
                    Ok(true)
                } else if t.eq_ignore_ascii_case("off")
                    || t.eq_ignore_ascii_case("false")
                    || t == "0"
                {
                    Ok(false)
                } else {
                    Err(MiniError::Invalid(format!("invalid boolean value: {t}")))
                }
            }
            Cell::Null => Err(MiniError::Invalid("invalid boolean value: NULL".into())),
            _ => Err(MiniError::Invalid("invalid boolean value".into())),
        }
    };

    let normalize_isolation = |s: &str| -> Result<String, MiniError> {
        let t = s.trim().to_ascii_uppercase().replace(' ', "-");
        match t.as_str() {
            "READ-UNCOMMITTED" | "READ-COMMITTED" | "REPEATABLE-READ" | "SERIALIZABLE" => Ok(t),
            other => Err(MiniError::Invalid(format!(
                "unsupported transaction isolation level: {other}"
            ))),
        }
    };

    let mut apply_var = |scope: Option<ast::ContextModifier>,
                         name: &ObjectName,
                         value: &ast::Expr|
     -> Result<(), MiniError> {
        if matches!(scope, Some(ast::ContextModifier::Global)) {
            return Err(MiniError::NotSupported(
                "SET GLOBAL is not supported".into(),
            ));
        }

        let var = get_ident_name(name.0.last().unwrap());
        match var.to_ascii_lowercase().as_str() {
            "autocommit" => {
                let new_autocommit = parse_bool(value)?;
                if new_autocommit
                    && !session.autocommit
                    && (session.txn.in_txn || !session.txn.pending_rows.is_empty())
                {
                    maybe_commit_on_enable_autocommit = true;
                }
                session.autocommit = new_autocommit;
            }
            "sql_mode" => {
                let c = eval_expr(value)?;
                session.sql_mode = cell_to_string(&c);
            }
            "time_zone" => {
                let c = eval_expr(value)?;
                session.time_zone = cell_to_string(&c);
            }
            "transaction_isolation" | "tx_isolation" => {
                let c = eval_expr(value)?;
                let iso = normalize_isolation(&cell_to_string(&c))?;
                session.transaction_isolation = iso;
            }
            "transaction_read_only" => {
                session.transaction_read_only = parse_bool(value)?;
            }
            other => {
                return Err(MiniError::NotSupported(format!(
                    "SET {other} is not supported"
                )))
            }
        }
        Ok(())
    };

    match set {
        ast::Set::MultipleAssignments { assignments } => {
            for a in assignments {
                apply_var(a.scope, &a.name, &a.value)?;
            }
        }
        ast::Set::SingleAssignment {
            scope,
            variable,
            values,
            ..
        } => {
            let expr = values
                .first()
                .ok_or_else(|| MiniError::Parse("SET missing value".into()))?;
            apply_var(*scope, variable, expr)?;
        }
        ast::Set::SetNames {
            charset_name,
            collation_name,
        } => {
            let charset = charset_name.value.clone();
            session.character_set_client = charset.clone();
            session.character_set_connection = charset.clone();
            session.character_set_results = charset;
            if let Some(collation) = collation_name {
                session.collation_connection = collation.clone();
            }
        }
        ast::Set::SetNamesDefault {} => {
            session.character_set_client = "utf8".into();
            session.character_set_connection = "utf8".into();
            session.character_set_results = "utf8".into();
            session.collation_connection = "utf8_general_ci".into();
        }
        ast::Set::SetTransaction { modes, .. } => {
            for mode in modes {
                match mode {
                    ast::TransactionMode::AccessMode(ast::TransactionAccessMode::ReadOnly) => {
                        session.transaction_read_only = true;
                    }
                    ast::TransactionMode::AccessMode(ast::TransactionAccessMode::ReadWrite) => {
                        session.transaction_read_only = false;
                    }
                    ast::TransactionMode::IsolationLevel(level) => {
                        let iso = match level {
                            ast::TransactionIsolationLevel::ReadUncommitted => "READ-UNCOMMITTED",
                            ast::TransactionIsolationLevel::ReadCommitted => "READ-COMMITTED",
                            ast::TransactionIsolationLevel::RepeatableRead => "REPEATABLE-READ",
                            ast::TransactionIsolationLevel::Serializable => "SERIALIZABLE",
                            ast::TransactionIsolationLevel::Snapshot => {
                                return Err(MiniError::NotSupported(
                                    "SNAPSHOT isolation is not supported".into(),
                                ))
                            }
                        };
                        session.transaction_isolation = iso.to_string();
                    }
                }
            }
        }
        _ => return Err(MiniError::NotSupported("Unsupported SET statement".into())),
    }

    if maybe_commit_on_enable_autocommit {
        txn_commit(store, session)?;
    }
    Ok(ExecOutput::Ok {
        affected_rows: 0,
        last_insert_id: 0,
        info: "".into(),
    })
}

fn handle_savepoint(session: &mut SessionState, name: &Ident) -> Result<ExecOutput, MiniError> {
    if !session.txn.in_txn {
        return Err(MiniError::Invalid(
            "SAVEPOINT requires an active transaction".into(),
        ));
    }
    session
        .txn
        .savepoints
        .push((name.value.clone(), session.txn.pending_rows.clone()));
    Ok(ExecOutput::Ok {
        affected_rows: 0,
        last_insert_id: 0,
        info: "".into(),
    })
}

fn handle_rollback_to_savepoint(
    session: &mut SessionState,
    name: &Ident,
) -> Result<ExecOutput, MiniError> {
    if !session.txn.in_txn {
        return Err(MiniError::Invalid(
            "ROLLBACK TO SAVEPOINT requires an active transaction".into(),
        ));
    }
    let pos = session
        .txn
        .savepoints
        .iter()
        .rposition(|(n, _)| n.eq_ignore_ascii_case(&name.value))
        .ok_or_else(|| MiniError::NotFound(format!("unknown savepoint: {}", name.value)))?;

    session.txn.pending_rows = session.txn.savepoints[pos].1.clone();
    session.txn.savepoints.truncate(pos + 1);

    Ok(ExecOutput::Ok {
        affected_rows: 0,
        last_insert_id: 0,
        info: "".into(),
    })
}

fn handle_release_savepoint(
    session: &mut SessionState,
    name: &Ident,
) -> Result<ExecOutput, MiniError> {
    if !session.txn.in_txn {
        return Err(MiniError::Invalid(
            "RELEASE SAVEPOINT requires an active transaction".into(),
        ));
    }
    let pos = session
        .txn
        .savepoints
        .iter()
        .rposition(|(n, _)| n.eq_ignore_ascii_case(&name.value))
        .ok_or_else(|| MiniError::NotFound(format!("unknown savepoint: {}", name.value)))?;
    session.txn.savepoints.truncate(pos);
    Ok(ExecOutput::Ok {
        affected_rows: 0,
        last_insert_id: 0,
        info: "".into(),
    })
}

fn handle_insert(
    store: &Store,
    session: &mut SessionState,
    user: &UserRecord,
    insert: &ast::Insert,
) -> Result<ExecOutput, MiniError> {
    require_priv(user, session.current_db.as_deref(), Priv::INSERT)?;

    let Some(src) = &insert.source else {
        return Err(MiniError::Parse("INSERT missing source".into()));
    };

    let table_name = match &insert.table {
        ast::TableObject::TableName(name) => name,
        _ => {
            return Err(MiniError::NotSupported(
                "Complex table insert not supported".into(),
            ));
        }
    };

    let (db_opt, table) = object_name_to_parts(table_name)?;
    let db = db_opt
        .or_else(|| session.current_db.clone())
        .ok_or_else(|| MiniError::Invalid("no database selected".into()))?;
    let def = store.get_table(&db, &table)?;

    let cols: Vec<String> = if insert.columns.is_empty() {
        def.columns.iter().map(|c| c.name.clone()).collect()
    } else {
        insert.columns.iter().map(|c| c.value.clone()).collect()
    };

    // Extract rows from source
    let rows_exprs = match &src.body.as_ref() {
        SetExpr::Values(values) => &values.rows,
        _ => {
            return Err(MiniError::NotSupported(
                "INSERT only supports VALUES".into(),
            ))
        }
    };

    let buffer_writes = should_buffer_writes(session);
    let mut locks = RowLockGuard::new(store, session.conn_id);
    let mut stmt_rows: BTreeMap<i64, Row> = BTreeMap::new();
    let mut affected = 0u64;
    let mut first_generated_id: Option<i64> = None;
    let pk_index = def
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(&def.primary_key))
        .ok_or_else(|| MiniError::Invalid("corrupt table: missing primary key column".into()))?;
    let mut auto_inc_initialized = false;

    for row_exprs in rows_exprs {
        if row_exprs.len() != cols.len() {
            return Err(MiniError::Invalid("column/value count mismatch".into()));
        }
        let mut map: BTreeMap<String, Cell> = BTreeMap::new();
        for (c, expr) in cols.iter().zip(row_exprs.iter()) {
            map.insert(c.clone(), eval_expr(expr)?);
        }

        let mut row_vals = Vec::with_capacity(def.columns.len());
        for coldef in &def.columns {
            let v = map.get(&coldef.name).cloned().unwrap_or(Cell::Null);
            let coerced = coerce_cell(v, &coldef.ty)?;
            row_vals.push(coerced);
        }

        let mut pk_cell = row_vals
            .get(pk_index)
            .cloned()
            .ok_or_else(|| MiniError::Invalid("corrupt row".into()))?;
        let mut generated = false;
        let pk = match pk_cell.as_i64() {
            Some(pk) => pk,
            None if matches!(pk_cell, Cell::Null) && def.auto_increment => {
                if !auto_inc_initialized {
                    if store.auto_increment_next(&db, &table)?.is_none() {
                        let mut max_pk = 0i64;
                        for (pk, _row) in txn_scan_rows(store, session, &db, &table)? {
                            max_pk = max_pk.max(pk);
                        }
                        store.bump_auto_increment_next(
                            &db,
                            &table,
                            max_pk.saturating_add(1).max(1),
                        )?;
                    }
                    auto_inc_initialized = true;
                }
                let pk = store.allocate_auto_increment(&db, &table)?;
                pk_cell = Cell::Int(pk);
                row_vals[pk_index] = pk_cell.clone();
                generated = true;
                pk
            }
            _ => {
                return Err(MiniError::Invalid(
                    "PRIMARY KEY must be provided (INT)".into(),
                ))
            }
        };

        if def.auto_increment && !generated {
            store.bump_auto_increment_next(&db, &table, pk.saturating_add(1))?;
        }

        locks.lock_row(&db, &table, pk)?;

        if stmt_rows.contains_key(&pk) || txn_get_row(store, session, &db, &table, pk)?.is_some() {
            return Err(MiniError::Invalid(format!(
                "duplicate entry for primary key: {pk}"
            )));
        }

        stmt_rows.insert(pk, Row { values: row_vals });
        affected += 1;
        if generated && first_generated_id.is_none() {
            first_generated_id = Some(pk);
        }
    }

    if buffer_writes {
        session.txn.in_txn = true;
        for (pk, row) in stmt_rows {
            session.txn.pending_rows.insert(
                RowKey {
                    db: db.clone(),
                    table: table.clone(),
                    pk,
                },
                Some(row),
            );
        }
        locks.keep_locks();
    } else {
        let changes = stmt_rows
            .iter()
            .map(|(pk, row)| (db.as_str(), table.as_str(), *pk, Some(row)));
        store.apply_row_changes(changes)?;
    }

    Ok(ExecOutput::Ok {
        affected_rows: affected,
        last_insert_id: first_generated_id.unwrap_or(0).max(0) as u64,
        info: "".into(),
    })
}

fn handle_update(
    store: &Store,
    session: &mut SessionState,
    user: &UserRecord,
    update: &ast::Update,
) -> Result<ExecOutput, MiniError> {
    require_priv(user, session.current_db.as_deref(), Priv::UPDATE)?;

    if update.from.is_some()
        || update.returning.is_some()
        || update.or.is_some()
        || update.limit.is_some()
    {
        return Err(MiniError::NotSupported(
            "UPDATE with FROM/RETURNING/OR/LIMIT is not supported".into(),
        ));
    }
    if !update.table.joins.is_empty() {
        return Err(MiniError::NotSupported(
            "UPDATE with joins is not supported".into(),
        ));
    }

    let (db_opt, table_name) = match &update.table.relation {
        TableFactor::Table { name, .. } => object_name_to_parts(name)?,
        _ => {
            return Err(MiniError::NotSupported(
                "Only simple UPDATE supported".into(),
            ))
        }
    };
    let db = db_opt
        .or_else(|| session.current_db.clone())
        .ok_or_else(|| MiniError::Invalid("no database selected".into()))?;
    let def = store.get_table(&db, &table_name)?;

    let mut assignments: Vec<(usize, Cell)> = Vec::new();
    for a in &update.assignments {
        let col_name = match &a.target {
            ast::AssignmentTarget::ColumnName(name) => get_ident_name(name.0.last().unwrap()),
            ast::AssignmentTarget::Tuple(_) => {
                return Err(MiniError::NotSupported(
                    "UPDATE tuple assignment is not supported".into(),
                ))
            }
        };
        if col_name.eq_ignore_ascii_case(&def.primary_key) {
            return Err(MiniError::NotSupported(
                "Updating PRIMARY KEY is not supported".into(),
            ));
        }
        let idx = def
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&col_name))
            .ok_or_else(|| MiniError::NotFound(format!("unknown column: {col_name}")))?;
        let val = eval_expr(&a.value)?;
        if matches!(val, Cell::Null) && !def.columns[idx].nullable {
            return Err(MiniError::Invalid(format!(
                "column {col_name} cannot be NULL"
            )));
        }
        let coerced = coerce_cell(val, &def.columns[idx].ty)?;
        assignments.push((idx, coerced));
    }

    // WHERE
    let mut target_pks: Vec<i64> = Vec::new();
    if let Some(selection) = &update.selection {
        let (where_col, where_val) = parse_eq_predicate(selection)?;
        if where_col.eq_ignore_ascii_case(&def.primary_key) {
            let pk = where_val
                .as_i64()
                .ok_or_else(|| MiniError::Invalid("PRIMARY KEY must be INT".into()))?;
            target_pks.push(pk);
        } else {
            let idxw = def
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(&where_col))
                .ok_or_else(|| MiniError::NotFound(format!("unknown column: {where_col}")))?;
            for (pk, row) in txn_scan_rows(store, session, &db, &table_name)? {
                if row.values.get(idxw) == Some(&where_val) {
                    target_pks.push(pk);
                }
            }
        }
    } else {
        // MySQL updates all rows without a WHERE clause; keep it explicit for now.
        return Err(MiniError::NotSupported(
            "UPDATE without WHERE is not supported".into(),
        ));
    }

    target_pks.sort_unstable();
    target_pks.dedup();

    let buffer_writes = should_buffer_writes(session);
    let mut locks = RowLockGuard::new(store, session.conn_id);
    let mut stmt_rows: BTreeMap<i64, Row> = BTreeMap::new();
    let mut affected = 0u64;

    for pk in target_pks {
        locks.lock_row(&db, &table_name, pk)?;
        let Some(mut row) = txn_get_row(store, session, &db, &table_name, pk)? else {
            continue;
        };
        for (idx, val) in &assignments {
            if *idx >= row.values.len() {
                return Err(MiniError::Invalid("corrupt row".into()));
            }
            row.values[*idx] = val.clone();
        }
        stmt_rows.insert(pk, row);
        affected += 1;
    }

    if buffer_writes {
        session.txn.in_txn = true;
        for (pk, row) in stmt_rows {
            session.txn.pending_rows.insert(
                RowKey {
                    db: db.clone(),
                    table: table_name.clone(),
                    pk,
                },
                Some(row),
            );
        }
        locks.keep_locks();
    } else {
        let changes = stmt_rows
            .iter()
            .map(|(pk, row)| (db.as_str(), table_name.as_str(), *pk, Some(row)));
        store.apply_row_changes(changes)?;
    }

    Ok(ExecOutput::Ok {
        affected_rows: affected,
        last_insert_id: 0,
        info: "".into(),
    })
}

fn handle_delete(
    store: &Store,
    session: &mut SessionState,
    user: &UserRecord,
    delete: &ast::Delete,
) -> Result<ExecOutput, MiniError> {
    require_priv(user, session.current_db.as_deref(), Priv::DELETE)?;

    if delete.using.is_some()
        || delete.returning.is_some()
        || !delete.order_by.is_empty()
        || delete.limit.is_some()
        || !delete.tables.is_empty()
    {
        return Err(MiniError::NotSupported(
            "Only simple DELETE FROM <table> WHERE ... is supported".into(),
        ));
    }

    let from_tables = match &delete.from {
        ast::FromTable::WithFromKeyword(t) | ast::FromTable::WithoutKeyword(t) => t,
    };
    if from_tables.len() != 1 {
        return Err(MiniError::NotSupported(
            "DELETE supports only a single table".into(),
        ));
    }
    if !from_tables[0].joins.is_empty() {
        return Err(MiniError::NotSupported(
            "DELETE with joins is not supported".into(),
        ));
    }

    let (db_opt, table_name) = match &from_tables[0].relation {
        TableFactor::Table { name, .. } => object_name_to_parts(name)?,
        _ => {
            return Err(MiniError::NotSupported(
                "Only simple DELETE FROM <table> is supported".into(),
            ))
        }
    };
    let db = db_opt
        .or_else(|| session.current_db.clone())
        .ok_or_else(|| MiniError::Invalid("no database selected".into()))?;
    let def = store.get_table(&db, &table_name)?;

    let selection = delete
        .selection
        .as_ref()
        .ok_or_else(|| MiniError::NotSupported("DELETE without WHERE is not supported".into()))?;
    let (where_col, where_val) = parse_eq_predicate(selection)?;

    let mut target_pks: Vec<i64> = Vec::new();
    if where_col.eq_ignore_ascii_case(&def.primary_key) {
        let pk = where_val
            .as_i64()
            .ok_or_else(|| MiniError::Invalid("PRIMARY KEY must be INT".into()))?;
        target_pks.push(pk);
    } else {
        let idxw = def
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&where_col))
            .ok_or_else(|| MiniError::NotFound(format!("unknown column: {where_col}")))?;
        for (pk, row) in txn_scan_rows(store, session, &db, &table_name)? {
            if row.values.get(idxw) == Some(&where_val) {
                target_pks.push(pk);
            }
        }
    }

    target_pks.sort_unstable();
    target_pks.dedup();

    let buffer_writes = should_buffer_writes(session);
    let mut locks = RowLockGuard::new(store, session.conn_id);
    let mut stmt_deletes: Vec<i64> = Vec::new();
    let mut affected = 0u64;

    for pk in target_pks {
        locks.lock_row(&db, &table_name, pk)?;
        if txn_get_row(store, session, &db, &table_name, pk)?.is_none() {
            continue;
        }
        stmt_deletes.push(pk);
        affected += 1;
    }

    if buffer_writes {
        session.txn.in_txn = true;
        for pk in stmt_deletes {
            session.txn.pending_rows.insert(
                RowKey {
                    db: db.clone(),
                    table: table_name.clone(),
                    pk,
                },
                None,
            );
        }
        locks.keep_locks();
    } else {
        let changes = stmt_deletes
            .iter()
            .map(|pk| (db.as_str(), table_name.as_str(), *pk, None));
        store.apply_row_changes(changes)?;
    }

    Ok(ExecOutput::Ok {
        affected_rows: affected,
        last_insert_id: 0,
        info: "".into(),
    })
}

fn handle_query(
    store: &Store,
    session: &SessionState,
    user: &UserRecord,
    query: &ast::Query,
) -> Result<ExecOutput, MiniError> {
    // Only support SELECT
    let select = match &query.body.as_ref() {
        SetExpr::Select(s) => s,
        SetExpr::Values(_) => {
            return Err(MiniError::NotSupported("Only SELECT supported".into()));
        }
        _ => return Err(MiniError::NotSupported("Only SELECT supported".into())),
    };

    // Parse projection
    if select.from.is_empty() {
        let coltype_for_cell = |c: &Cell| match c {
            Cell::Int(_) => ColumnType::MYSQL_TYPE_LONGLONG,
            _ => ColumnType::MYSQL_TYPE_VAR_STRING,
        };

        let mut cols = Vec::new();
        let mut row = Vec::new();

        for (i, item) in select.projection.iter().enumerate() {
            let (expr, alias) = match item {
                ast::SelectItem::UnnamedExpr(e) => (e, None),
                ast::SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
                _ => {
                    return Err(MiniError::NotSupported(
                        "Wildcard in SELECT without FROM".into(),
                    ))
                }
            };

            let mut col_name = alias.clone().unwrap_or_else(|| format!("col{i}"));

            if let ast::Expr::Function(f) = expr {
                if f.name.to_string().eq_ignore_ascii_case("version") {
                    col_name = alias.unwrap_or_else(|| "VERSION()".into());
                    let v = Cell::Text(SERVER_VERSION.to_string());
                    cols.push(Column {
                        table: "".into(),
                        column: col_name,
                        coltype: coltype_for_cell(&v),
                        colflags: ColumnFlags::empty(),
                    });
                    row.push(v);
                    continue;
                }
                if f.name.to_string().eq_ignore_ascii_case("database") {
                    col_name = alias.unwrap_or_else(|| "DATABASE()".into());
                    let v = Cell::Text(session.current_db.clone().unwrap_or_default());
                    cols.push(Column {
                        table: "".into(),
                        column: col_name,
                        coltype: coltype_for_cell(&v),
                        colflags: ColumnFlags::empty(),
                    });
                    row.push(v);
                    continue;
                }
            }

            let sysvar_name = match expr {
                ast::Expr::Identifier(ident) => ident.value.strip_prefix("@@").map(|rest| {
                    let rest = rest.trim();
                    match rest.split_once('.') {
                        Some((scope, name))
                            if scope.eq_ignore_ascii_case("session")
                                || scope.eq_ignore_ascii_case("global") =>
                        {
                            name.to_string()
                        }
                        _ => rest.to_string(),
                    }
                }),
                ast::Expr::CompoundIdentifier(ids) => ids
                    .first()
                    .and_then(|i| i.value.strip_prefix("@@"))
                    .and_then(|scope| {
                        if scope.eq_ignore_ascii_case("session")
                            || scope.eq_ignore_ascii_case("global")
                        {
                            ids.get(1).map(|v| v.value.clone())
                        } else {
                            None
                        }
                    }),
                _ => None,
            };

            if let Some(var) = sysvar_name {
                let value = sysvar_value(session, &var)
                    .ok_or_else(|| MiniError::UnknownSystemVariable(var.clone()))?;
                if alias.is_none() {
                    col_name = expr.to_string();
                }
                cols.push(Column {
                    table: "".into(),
                    column: col_name,
                    coltype: coltype_for_cell(&value),
                    colflags: ColumnFlags::empty(),
                });
                row.push(value);
                continue;
            }

            let value = eval_expr(expr)?;
            cols.push(Column {
                table: "".into(),
                column: col_name,
                coltype: coltype_for_cell(&value),
                colflags: ColumnFlags::empty(),
            });
            row.push(value);
        }

        return Ok(ExecOutput::ResultSet {
            columns: cols,
            rows: vec![row],
        });
    }

    // SELECT .. FROM ..
    if select.from.is_empty() {
        // ... (existing no-from logic handled above? No, wait, line 2109 handled empty from)
        // If we reached here, and from is empty, it's an error or handled by the first block.
        // Actually the first block returned early if from was empty.
        // So here select.from is guaranteed not empty.
        return Err(MiniError::Invalid("Unexpected empty FROM clause".into()));
    }

    let mut accumulated_rows: Vec<Row> = Vec::new();
    let mut accumulated_def_indices: Vec<usize> = Vec::new(); // Indices into loaded_defs
    let mut loaded_defs: Vec<TableDef> = Vec::new();

    // Helper to scan a table relation
    let scan_table = |relation: &TableFactor| -> Result<(TableDef, Vec<Row>), MiniError> {
        let (db_opt, table_name, alias_name) = match relation {
            TableFactor::Table { name, alias, .. } => {
                let (db_opt, table_name) = object_name_to_parts(name)?;
                let alias_name = alias.as_ref().map(|a| a.name.value.clone());
                (db_opt, table_name, alias_name)
            }
            _ => {
                return Err(MiniError::NotSupported(
                    "Only simple table joins supported".into(),
                ))
            }
        };
        let db = db_opt
            .or_else(|| session.current_db.clone())
            .ok_or_else(|| MiniError::Invalid("no database selected".into()))?;

        let mut def;
        let rows;
        if is_information_schema(&db) {
            require_priv(user, None, Priv::SELECT)?;
            (def, rows) = build_information_schema_table(store, session, &table_name)?;
        } else if is_system_schema(&db) {
            return Err(MiniError::NotSupported(format!(
                "Reading system schema {db} is not supported"
            )));
        } else {
            require_priv(user, Some(&db), Priv::SELECT)?;
            def = store.get_table(&db, &table_name)?;
            rows = txn_scan_rows(store, session, &db, &table_name)?
                .into_iter()
                .map(|(_, r)| r)
                .collect();
        }

        if let Some(alias) = alias_name {
            def.name = alias;
        }
        Ok((def, rows))
    };

    // Flatten FROM clause: explicit commas + explicit JOINs
    for (i, table_with_joins) in select.from.iter().enumerate() {
        // 1. Process the main relation
        let (def, rows) = scan_table(&table_with_joins.relation)?;
        loaded_defs.push(def);
        let curr_def_idx = loaded_defs.len() - 1;

        if i == 0 {
            accumulated_rows = rows;
            accumulated_def_indices.push(curr_def_idx);
        } else {
            // Cartesian Product with previous result
            let mut new_rows = Vec::with_capacity(accumulated_rows.len() * rows.len());
            for left in &accumulated_rows {
                for right in &rows {
                    let mut combined = left.values.clone();
                    combined.extend(right.values.clone());
                    new_rows.push(Row { values: combined });
                }
            }
            accumulated_rows = new_rows;
            accumulated_def_indices.push(curr_def_idx);
        }

        // 2. Process chained Joins
        for join in &table_with_joins.joins {
            let (j_def, j_rows) = scan_table(&join.relation)?;
            let right_col_count = j_def.columns.len();
            loaded_defs.push(j_def);
            let j_def_idx = loaded_defs.len() - 1;

            #[derive(Copy, Clone, Debug, PartialEq, Eq)]
            enum JoinKind {
                Inner,
                Left,
                Right,
            }

            let (join_kind, constraint) = match &join.join_operator {
                ast::JoinOperator::Join(c)
                | ast::JoinOperator::Inner(c)
                | ast::JoinOperator::CrossJoin(c)
                | ast::JoinOperator::StraightJoin(c) => (JoinKind::Inner, c),
                ast::JoinOperator::Left(c) | ast::JoinOperator::LeftOuter(c) => (JoinKind::Left, c),
                ast::JoinOperator::Right(c) | ast::JoinOperator::RightOuter(c) => {
                    (JoinKind::Right, c)
                }
                ast::JoinOperator::FullOuter(_) => {
                    return Err(MiniError::NotSupported(
                        "FULL OUTER joins are not supported".into(),
                    ))
                }
                other => {
                    return Err(MiniError::NotSupported(format!(
                        "JOIN operator not supported: {other:?}"
                    )))
                }
            };

            let right_def = &loaded_defs[j_def_idx];
            let left_defs: Vec<&TableDef> = accumulated_def_indices
                .iter()
                .map(|&idx| &loaded_defs[idx])
                .collect();
            let left_col_count: usize = left_defs.iter().map(|d| d.columns.len()).sum();

            let derived_on_expr: Option<ast::Expr> = match constraint {
                ast::JoinConstraint::Using(cols) => {
                    Some(build_using_join_on_expr(&left_defs, right_def, cols)?)
                }
                ast::JoinConstraint::Natural => build_natural_join_on_expr(&left_defs, right_def)?,
                _ => None,
            };

            let on_expr: Option<&ast::Expr> = match constraint {
                ast::JoinConstraint::On(expr) => Some(expr),
                ast::JoinConstraint::None => None,
                ast::JoinConstraint::Using(_) | ast::JoinConstraint::Natural => {
                    derived_on_expr.as_ref()
                }
            };

            // JOIN output shape always appends the right table's columns.
            accumulated_def_indices.push(j_def_idx);
            let temp_defs: Vec<&TableDef> = accumulated_def_indices
                .iter()
                .map(|&idx| &loaded_defs[idx])
                .collect();
            let temp_col_map = build_col_map(&temp_defs);

            let left_rows = std::mem::take(&mut accumulated_rows);
            let equi_join_pairs = on_expr
                .and_then(|expr| extract_equi_join_pairs(expr, &temp_col_map, left_col_count));

            let mut new_rows = Vec::with_capacity(
                left_rows
                    .len()
                    .saturating_mul(std::cmp::max(1, j_rows.len())),
            );

            let right_nulls = vec![Cell::Null; right_col_count];
            let left_nulls = vec![Cell::Null; left_col_count];

            match join_kind {
                JoinKind::Inner | JoinKind::Left => {
                    for left in &left_rows {
                        let mut matched = false;
                        for right in &j_rows {
                            if let Some(pairs) = &equi_join_pairs {
                                if eval_equi_join_pairs(left, right, pairs) {
                                    matched = true;
                                    let mut combined = left.values.clone();
                                    combined.extend(right.values.clone());
                                    new_rows.push(Row { values: combined });
                                }
                            } else {
                                let mut combined = left.values.clone();
                                combined.extend(right.values.clone());
                                let row = Row { values: combined };
                                let ok = match on_expr {
                                    Some(expr) => {
                                        eval_condition(session, expr, &row, &temp_col_map)?
                                    }
                                    None => true,
                                };
                                if ok {
                                    matched = true;
                                    new_rows.push(row);
                                }
                            }
                        }

                        if join_kind == JoinKind::Left && !matched {
                            let mut combined = left.values.clone();
                            combined.extend(right_nulls.clone());
                            new_rows.push(Row { values: combined });
                        }
                    }
                }
                JoinKind::Right => {
                    let mut new_rows = Vec::with_capacity(
                        j_rows
                            .len()
                            .saturating_mul(std::cmp::max(1, left_rows.len())),
                    );
                    for right in &j_rows {
                        let mut matched = false;
                        for left in &left_rows {
                            if let Some(pairs) = &equi_join_pairs {
                                if eval_equi_join_pairs(left, right, pairs) {
                                    matched = true;
                                    let mut combined = left.values.clone();
                                    combined.extend(right.values.clone());
                                    new_rows.push(Row { values: combined });
                                }
                            } else {
                                let mut combined = left.values.clone();
                                combined.extend(right.values.clone());
                                let row = Row { values: combined };
                                let ok = match on_expr {
                                    Some(expr) => {
                                        eval_condition(session, expr, &row, &temp_col_map)?
                                    }
                                    None => true,
                                };
                                if ok {
                                    matched = true;
                                    new_rows.push(row);
                                }
                            }
                        }

                        if !matched {
                            let mut combined = left_nulls.clone();
                            combined.extend(right.values.clone());
                            new_rows.push(Row { values: combined });
                        }
                    }
                    accumulated_rows = new_rows;
                    continue;
                }
            }
            accumulated_rows = new_rows;
        }
    }

    let final_defs: Vec<&TableDef> = accumulated_def_indices
        .iter()
        .map(|&idx| &loaded_defs[idx])
        .collect();
    execute_select_from_rows(session, &final_defs, accumulated_rows, select, query)
}

fn build_information_schema_table(
    store: &Store,
    session: &SessionState,
    table_name: &str,
) -> Result<(TableDef, Vec<Row>), MiniError> {
    let table_lc = table_name.to_ascii_lowercase();
    match table_lc.as_str() {
        "schemata" => {
            let def = information_schema_schemata_def();
            let rows = list_all_databases(store)?
                .into_iter()
                .map(|schema| Row {
                    values: vec![
                        Cell::Text("def".into()),
                        Cell::Text(schema),
                        Cell::Text(session.character_set_connection.clone()),
                        Cell::Text(session.collation_connection.clone()),
                        Cell::Null,
                    ],
                })
                .collect();
            Ok((def, rows))
        }
        "tables" => {
            let def = information_schema_tables_def();
            let mut rows = Vec::new();

            for db in store.list_databases()? {
                for table in store.list_tables(&db)? {
                    let row_count = store.count_rows(&db, &table)?.min(i64::MAX as u64) as i64;
                    let tdef = store.get_table(&db, &table)?;
                    let auto_inc = if tdef.auto_increment {
                        store.auto_increment_next(&db, &table)?.unwrap_or(1)
                    } else {
                        0
                    };
                    rows.push(Row {
                        values: vec![
                            Cell::Text("def".into()),
                            Cell::Text(db.clone()),
                            Cell::Text(table),
                            Cell::Text("BASE TABLE".into()),
                            Cell::Text("InnoDB".into()),
                            Cell::Int(10),
                            Cell::Text("Dynamic".into()),
                            Cell::Int(row_count),
                            Cell::Int(0),
                            Cell::Int(0),
                            Cell::Int(0),
                            Cell::Int(0),
                            Cell::Int(0),
                            if tdef.auto_increment {
                                Cell::Int(auto_inc)
                            } else {
                                Cell::Null
                            },
                            Cell::Null,
                            Cell::Null,
                            Cell::Null,
                            Cell::Text(session.collation_connection.clone()),
                            Cell::Null,
                            Cell::Text("".into()),
                            Cell::Text("".into()),
                        ],
                    });
                }
            }

            for table in information_schema_table_names() {
                rows.push(Row {
                    values: vec![
                        Cell::Text("def".into()),
                        Cell::Text("information_schema".into()),
                        Cell::Text(table),
                        Cell::Text("SYSTEM VIEW".into()),
                        Cell::Null,
                        Cell::Null,
                        Cell::Null,
                        Cell::Int(0),
                        Cell::Int(0),
                        Cell::Int(0),
                        Cell::Int(0),
                        Cell::Int(0),
                        Cell::Int(0),
                        Cell::Null,
                        Cell::Null,
                        Cell::Null,
                        Cell::Null,
                        Cell::Text(session.collation_connection.clone()),
                        Cell::Null,
                        Cell::Text("".into()),
                        Cell::Text("".into()),
                    ],
                });
            }

            Ok((def, rows))
        }
        "columns" => {
            let def = information_schema_columns_def();
            let mut rows = Vec::new();

            for db in store.list_databases()? {
                for table in store.list_tables(&db)? {
                    let tdef = store.get_table(&db, &table)?;
                    for (pos, col) in tdef.columns.iter().enumerate() {
                        let ordinal = i64::try_from(pos + 1)
                            .map_err(|_| MiniError::Invalid("ordinal position too large".into()))?;
                        let (data_type, col_type) = match col.ty {
                            SqlType::Int => ("bigint", "bigint"),
                            SqlType::Text => ("text", "text"),
                            SqlType::Float => ("double", "double"),
                            SqlType::Date => ("date", "date"),
                            SqlType::DateTime => ("datetime", "datetime"),
                        };
                        let is_nullable = if col.nullable { "YES" } else { "NO" };
                        let (charset, coll) = match col.ty {
                            SqlType::Text => (
                                Cell::Text(session.character_set_connection.clone()),
                                Cell::Text(session.collation_connection.clone()),
                            ),
                            _ => (Cell::Null, Cell::Null),
                        };
                        let column_key = if col.name.eq_ignore_ascii_case(&tdef.primary_key) {
                            "PRI"
                        } else {
                            ""
                        };
                        let extra = if tdef.auto_increment
                            && col.name.eq_ignore_ascii_case(&tdef.primary_key)
                        {
                            "auto_increment"
                        } else {
                            ""
                        };
                        rows.push(Row {
                            values: vec![
                                Cell::Text("def".into()),
                                Cell::Text(db.clone()),
                                Cell::Text(table.clone()),
                                Cell::Text(col.name.clone()),
                                Cell::Int(ordinal),
                                Cell::Null,
                                Cell::Text(is_nullable.into()),
                                Cell::Text(data_type.into()),
                                Cell::Null,
                                Cell::Null,
                                if col.ty == SqlType::Int {
                                    Cell::Int(64)
                                } else {
                                    Cell::Null
                                },
                                if col.ty == SqlType::Int {
                                    Cell::Int(0)
                                } else {
                                    Cell::Null
                                },
                                Cell::Null,
                                charset,
                                coll,
                                Cell::Text(col_type.into()),
                                Cell::Text(column_key.into()),
                                Cell::Text(extra.into()),
                                Cell::Text("select,insert,update,references".into()),
                                Cell::Text("".into()),
                            ],
                        });
                    }
                }
            }

            for (table_name, tdef) in information_schema_defs() {
                for (pos, col) in tdef.columns.iter().enumerate() {
                    let ordinal = i64::try_from(pos + 1)
                        .map_err(|_| MiniError::Invalid("ordinal position too large".into()))?;
                    let (data_type, col_type) = match col.ty {
                        SqlType::Int => ("bigint", "bigint"),
                        SqlType::Text => ("text", "text"),
                        SqlType::Float => ("double", "double"),
                        SqlType::Date => ("date", "date"),
                        SqlType::DateTime => ("datetime", "datetime"),
                    };
                    let is_nullable = if col.nullable { "YES" } else { "NO" };
                    let (charset, coll) = match col.ty {
                        SqlType::Text => (
                            Cell::Text(session.character_set_connection.clone()),
                            Cell::Text(session.collation_connection.clone()),
                        ),
                        _ => (Cell::Null, Cell::Null),
                    };
                    let column_key = if col.name.eq_ignore_ascii_case(&tdef.primary_key) {
                        "PRI"
                    } else {
                        ""
                    };
                    rows.push(Row {
                        values: vec![
                            Cell::Text("def".into()),
                            Cell::Text("information_schema".into()),
                            Cell::Text(table_name.clone()),
                            Cell::Text(col.name.clone()),
                            Cell::Int(ordinal),
                            Cell::Null,
                            Cell::Text(is_nullable.into()),
                            Cell::Text(data_type.into()),
                            Cell::Null,
                            Cell::Null,
                            if col.ty == SqlType::Int {
                                Cell::Int(64)
                            } else {
                                Cell::Null
                            },
                            if col.ty == SqlType::Int {
                                Cell::Int(0)
                            } else {
                                Cell::Null
                            },
                            Cell::Null,
                            charset,
                            coll,
                            Cell::Text(col_type.into()),
                            Cell::Text(column_key.into()),
                            Cell::Text("".into()),
                            Cell::Text("select,insert,update,references".into()),
                            Cell::Text("".into()),
                        ],
                    });
                }
            }

            Ok((def, rows))
        }
        "statistics" => {
            let def = information_schema_statistics_def();
            let mut rows = Vec::new();

            for db in store.list_databases()? {
                for table in store.list_tables(&db)? {
                    let tdef = store.get_table(&db, &table)?;
                    let pk_name = tdef.primary_key.clone();
                    let pk_nullable = tdef
                        .columns
                        .iter()
                        .find(|c| c.name.eq_ignore_ascii_case(&pk_name))
                        .map(|c| c.nullable)
                        .unwrap_or(false);
                    let row_count = store.count_rows(&db, &table)?.min(i64::MAX as u64) as i64;
                    rows.push(Row {
                        values: vec![
                            Cell::Text("def".into()),
                            Cell::Text(db.clone()),
                            Cell::Text(table.clone()),
                            Cell::Int(0),
                            Cell::Text(db.clone()),
                            Cell::Text("PRIMARY".into()),
                            Cell::Int(1),
                            Cell::Text(pk_name),
                            Cell::Text("A".into()),
                            Cell::Int(row_count),
                            Cell::Null,
                            Cell::Null,
                            Cell::Text(if pk_nullable { "YES" } else { "NO" }.into()),
                            Cell::Text("BTREE".into()),
                            Cell::Text("".into()),
                            Cell::Text("".into()),
                            Cell::Text("YES".into()),
                            Cell::Null,
                        ],
                    });
                }
            }

            Ok((def, rows))
        }
        _ => Err(MiniError::NotFound(format!(
            "unknown table: information_schema.{table_name}"
        ))),
    }
}

fn information_schema_schemata_def() -> TableDef {
    TableDef {
        db: "information_schema".into(),
        name: "SCHEMATA".into(),
        columns: vec![
            ColumnDef {
                name: "CATALOG_NAME".into(),
                ty: SqlType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "SCHEMA_NAME".into(),
                ty: SqlType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "DEFAULT_CHARACTER_SET_NAME".into(),
                ty: SqlType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "DEFAULT_COLLATION_NAME".into(),
                ty: SqlType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "SQL_PATH".into(),
                ty: SqlType::Text,
                nullable: true,
            },
        ],
        primary_key: "SCHEMA_NAME".into(),
        auto_increment: false,
        indexes: vec![],
    }
}

fn information_schema_tables_def() -> TableDef {
    TableDef {
        db: "information_schema".into(),
        name: "TABLES".into(),
        columns: vec![
            ColumnDef {
                name: "TABLE_CATALOG".into(),
                ty: SqlType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "TABLE_SCHEMA".into(),
                ty: SqlType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "TABLE_NAME".into(),
                ty: SqlType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "TABLE_TYPE".into(),
                ty: SqlType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "ENGINE".into(),
                ty: SqlType::Text,
                nullable: true,
            },
            ColumnDef {
                name: "VERSION".into(),
                ty: SqlType::Int,
                nullable: true,
            },
            ColumnDef {
                name: "ROW_FORMAT".into(),
                ty: SqlType::Text,
                nullable: true,
            },
            ColumnDef {
                name: "TABLE_ROWS".into(),
                ty: SqlType::Int,
                nullable: true,
            },
            ColumnDef {
                name: "AVG_ROW_LENGTH".into(),
                ty: SqlType::Int,
                nullable: true,
            },
            ColumnDef {
                name: "DATA_LENGTH".into(),
                ty: SqlType::Int,
                nullable: true,
            },
            ColumnDef {
                name: "MAX_DATA_LENGTH".into(),
                ty: SqlType::Int,
                nullable: true,
            },
            ColumnDef {
                name: "INDEX_LENGTH".into(),
                ty: SqlType::Int,
                nullable: true,
            },
            ColumnDef {
                name: "DATA_FREE".into(),
                ty: SqlType::Int,
                nullable: true,
            },
            ColumnDef {
                name: "AUTO_INCREMENT".into(),
                ty: SqlType::Int,
                nullable: true,
            },
            ColumnDef {
                name: "CREATE_TIME".into(),
                ty: SqlType::Text,
                nullable: true,
            },
            ColumnDef {
                name: "UPDATE_TIME".into(),
                ty: SqlType::Text,
                nullable: true,
            },
            ColumnDef {
                name: "CHECK_TIME".into(),
                ty: SqlType::Text,
                nullable: true,
            },
            ColumnDef {
                name: "TABLE_COLLATION".into(),
                ty: SqlType::Text,
                nullable: true,
            },
            ColumnDef {
                name: "CHECKSUM".into(),
                ty: SqlType::Int,
                nullable: true,
            },
            ColumnDef {
                name: "CREATE_OPTIONS".into(),
                ty: SqlType::Text,
                nullable: true,
            },
            ColumnDef {
                name: "TABLE_COMMENT".into(),
                ty: SqlType::Text,
                nullable: false,
            },
        ],
        primary_key: "TABLE_NAME".into(),
        auto_increment: false,
        indexes: vec![],
    }
}

fn information_schema_columns_def() -> TableDef {
    TableDef {
        db: "information_schema".into(),
        name: "COLUMNS".into(),
        columns: vec![
            ColumnDef {
                name: "TABLE_CATALOG".into(),
                ty: SqlType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "TABLE_SCHEMA".into(),
                ty: SqlType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "TABLE_NAME".into(),
                ty: SqlType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "COLUMN_NAME".into(),
                ty: SqlType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "ORDINAL_POSITION".into(),
                ty: SqlType::Int,
                nullable: false,
            },
            ColumnDef {
                name: "COLUMN_DEFAULT".into(),
                ty: SqlType::Text,
                nullable: true,
            },
            ColumnDef {
                name: "IS_NULLABLE".into(),
                ty: SqlType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "DATA_TYPE".into(),
                ty: SqlType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "CHARACTER_MAXIMUM_LENGTH".into(),
                ty: SqlType::Int,
                nullable: true,
            },
            ColumnDef {
                name: "CHARACTER_OCTET_LENGTH".into(),
                ty: SqlType::Int,
                nullable: true,
            },
            ColumnDef {
                name: "NUMERIC_PRECISION".into(),
                ty: SqlType::Int,
                nullable: true,
            },
            ColumnDef {
                name: "NUMERIC_SCALE".into(),
                ty: SqlType::Int,
                nullable: true,
            },
            ColumnDef {
                name: "DATETIME_PRECISION".into(),
                ty: SqlType::Int,
                nullable: true,
            },
            ColumnDef {
                name: "CHARACTER_SET_NAME".into(),
                ty: SqlType::Text,
                nullable: true,
            },
            ColumnDef {
                name: "COLLATION_NAME".into(),
                ty: SqlType::Text,
                nullable: true,
            },
            ColumnDef {
                name: "COLUMN_TYPE".into(),
                ty: SqlType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "COLUMN_KEY".into(),
                ty: SqlType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "EXTRA".into(),
                ty: SqlType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "PRIVILEGES".into(),
                ty: SqlType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "COLUMN_COMMENT".into(),
                ty: SqlType::Text,
                nullable: false,
            },
        ],
        primary_key: "COLUMN_NAME".into(),
        auto_increment: false,
        indexes: vec![],
    }
}

fn information_schema_statistics_def() -> TableDef {
    TableDef {
        db: "information_schema".into(),
        name: "STATISTICS".into(),
        columns: vec![
            ColumnDef {
                name: "TABLE_CATALOG".into(),
                ty: SqlType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "TABLE_SCHEMA".into(),
                ty: SqlType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "TABLE_NAME".into(),
                ty: SqlType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "NON_UNIQUE".into(),
                ty: SqlType::Int,
                nullable: false,
            },
            ColumnDef {
                name: "INDEX_SCHEMA".into(),
                ty: SqlType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "INDEX_NAME".into(),
                ty: SqlType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "SEQ_IN_INDEX".into(),
                ty: SqlType::Int,
                nullable: false,
            },
            ColumnDef {
                name: "COLUMN_NAME".into(),
                ty: SqlType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "COLLATION".into(),
                ty: SqlType::Text,
                nullable: true,
            },
            ColumnDef {
                name: "CARDINALITY".into(),
                ty: SqlType::Int,
                nullable: true,
            },
            ColumnDef {
                name: "SUB_PART".into(),
                ty: SqlType::Int,
                nullable: true,
            },
            ColumnDef {
                name: "PACKED".into(),
                ty: SqlType::Text,
                nullable: true,
            },
            ColumnDef {
                name: "NULLABLE".into(),
                ty: SqlType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "INDEX_TYPE".into(),
                ty: SqlType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "COMMENT".into(),
                ty: SqlType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "INDEX_COMMENT".into(),
                ty: SqlType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "IS_VISIBLE".into(),
                ty: SqlType::Text,
                nullable: false,
            },
            ColumnDef {
                name: "EXPRESSION".into(),
                ty: SqlType::Text,
                nullable: true,
            },
        ],
        primary_key: "INDEX_NAME".into(),
        auto_increment: false,
        indexes: vec![],
    }
}

fn information_schema_defs() -> Vec<(String, TableDef)> {
    vec![
        ("SCHEMATA".into(), information_schema_schemata_def()),
        ("TABLES".into(), information_schema_tables_def()),
        ("COLUMNS".into(), information_schema_columns_def()),
        ("STATISTICS".into(), information_schema_statistics_def()),
    ]
}

fn build_col_map(defs: &[&TableDef]) -> std::collections::HashMap<String, usize> {
    let mut map = std::collections::HashMap::new();
    let mut offset = 0;

    for def in defs {
        for (i, c) in def.columns.iter().enumerate() {
            let idx = offset + i;
            // 1. Unqualified name (mark ambiguous on collision).
            let unqualified = c.name.to_ascii_lowercase();
            match map.get(&unqualified).copied() {
                None => {
                    map.insert(unqualified, idx);
                }
                Some(existing) if existing != usize::MAX => {
                    map.insert(unqualified, usize::MAX);
                }
                Some(_) => {}
            }

            // 2. Qualified name: table.col
            map.insert(format!("{}.{}", def.name, c.name).to_ascii_lowercase(), idx);
        }
        offset += def.columns.len();
    }
    map
}

fn order_by_expr_to_base_col_idx(
    expr: &ast::Expr,
    col_map: &std::collections::HashMap<String, usize>,
) -> Option<usize> {
    match expr {
        ast::Expr::Identifier(ident) => col_map
            .get(&ident.value.to_ascii_lowercase())
            .copied()
            .filter(|idx| *idx != usize::MAX),
        ast::Expr::CompoundIdentifier(ids) => {
            let full_name = ids
                .iter()
                .map(|i| i.value.clone())
                .collect::<Vec<_>>()
                .join(".")
                .to_ascii_lowercase();
            if let Some(&idx) = col_map.get(&full_name) {
                if idx != usize::MAX {
                    return Some(idx);
                }
            }

            if ids.len() > 2 {
                let last_two = format!("{}.{}", ids[ids.len() - 2].value, ids[ids.len() - 1].value)
                    .to_ascii_lowercase();
                if let Some(&idx) = col_map.get(&last_two) {
                    if idx != usize::MAX {
                        return Some(idx);
                    }
                }
            }

            ids.last()
                .and_then(|ident| col_map.get(&ident.value.to_ascii_lowercase()).copied())
                .filter(|idx| *idx != usize::MAX)
        }
        _ => None,
    }
}

fn try_apply_order_by_on_base_rows(
    rows: &mut [Row],
    query: &ast::Query,
    col_map: &std::collections::HashMap<String, usize>,
) -> Result<bool, MiniError> {
    let Some(order_by) = &query.order_by else {
        return Ok(false);
    };
    let exprs = match &order_by.kind {
        ast::OrderByKind::Expressions(e) => e,
        _ => return Err(MiniError::NotSupported("Order By ALL not supported".into())),
    };

    let mut sort_keys: Vec<(usize, bool)> = Vec::new(); // (col idx, desc)
    for e in exprs {
        let Some(idx) = order_by_expr_to_base_col_idx(&e.expr, col_map) else {
            return Ok(false);
        };
        let desc = e.options.asc == Some(false);
        sort_keys.push((idx, desc));
    }

    if sort_keys.is_empty() {
        return Ok(false);
    }

    rows.sort_by(|a, b| {
        for (idx, desc) in &sort_keys {
            let cmp = compare_cell_for_order(&a.values[*idx], &b.values[*idx]);
            let cmp = if *desc { cmp.reverse() } else { cmp };
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        std::cmp::Ordering::Equal
    });
    Ok(true)
}

fn apply_distinct_rows(rows: Vec<Vec<Cell>>) -> Vec<Vec<Cell>> {
    let mut seen: std::collections::HashSet<Vec<Cell>> = std::collections::HashSet::new();
    let mut out = Vec::new();
    for row in rows {
        if seen.insert(row.clone()) {
            out.push(row);
        }
    }
    out
}

fn execute_select_from_rows(
    session: &SessionState,
    defs: &[&TableDef],
    mut rows: Vec<Row>,
    select: &ast::Select,
    query: &ast::Query,
) -> Result<ExecOutput, MiniError> {
    use std::collections::HashMap;

    let col_map = build_col_map(defs);

    // 1. WHERE Filtering
    if let Some(selection) = &select.selection {
        let mut new_rows = Vec::with_capacity(rows.len());
        for row in rows {
            if eval_condition(session, selection, &row, &col_map)? {
                new_rows.push(row);
            }
        }
        rows = new_rows;
    }

    // 2. Projections & Aggregation Analysis
    #[derive(Clone, Debug)]
    enum ProjKind {
        Scalar(Box<ast::Expr>), // Standard expression
        Aggregate(usize),       // Index into accumulators
    }

    let mut projection_plan: Vec<(String, ProjKind)> = Vec::new(); // (Alias, Kind)
    let mut aggs_to_compute: Vec<(String, Option<ast::Expr>)> = Vec::new(); // (Func, ArgExpr)

    // 3. Projections Analysis
    fn is_agg(expr: &ast::Expr) -> Option<(String, Option<ast::Expr>)> {
        match expr {
            ast::Expr::Function(f) => {
                let name = f.name.to_string().to_ascii_lowercase();
                if matches!(name.as_str(), "count" | "sum" | "avg" | "min" | "max") {
                    let arg = match &f.args {
                        ast::FunctionArguments::List(l) => {
                            if l.args.len() == 1 {
                                match &l.args[0] {
                                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                                        Some(e.clone())
                                    }
                                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => {
                                        None
                                    } // count(*)
                                    _ => None,
                                }
                            } else {
                                None
                            }
                        }
                        _ => None,
                    };
                    return Some((name, arg));
                }
                None
            }
            _ => None,
        }
    }

    for item in &select.projection {
        match item {
            ast::SelectItem::Wildcard(_) => {
                if defs.len() == 1 {
                    // Expand * to all cols from the single table.
                    for c in &defs[0].columns {
                        projection_plan.push((
                            c.name.clone(),
                            ProjKind::Scalar(Box::new(ast::Expr::Identifier(ast::Ident::new(
                                &c.name,
                            )))),
                        ));
                    }
                } else {
                    // For multi-table queries, qualify wildcards to avoid ambiguous column names
                    // (e.g. `id` from two tables).
                    for def in defs {
                        for c in &def.columns {
                            projection_plan.push((
                                c.name.clone(),
                                ProjKind::Scalar(Box::new(ast::Expr::CompoundIdentifier(vec![
                                    ast::Ident::new(&def.name),
                                    ast::Ident::new(&c.name),
                                ]))),
                            ));
                        }
                    }
                }
            }
            ast::SelectItem::QualifiedWildcard(kind, _) => {
                let obj_name = match kind {
                    ast::SelectItemQualifiedWildcardKind::ObjectName(obj_name) => obj_name,
                    ast::SelectItemQualifiedWildcardKind::Expr(_) => {
                        return Err(MiniError::NotSupported(
                            "Wildcard on expression is not supported".into(),
                        ));
                    }
                };

                let (_db_opt, qualifier) = object_name_to_parts(obj_name)?;
                let def = defs
                    .iter()
                    .find(|d| d.name.eq_ignore_ascii_case(&qualifier));
                let Some(def) = def else {
                    return Err(MiniError::NotFound(format!(
                        "unknown table in wildcard: {qualifier}"
                    )));
                };

                for c in &def.columns {
                    projection_plan.push((
                        c.name.clone(),
                        ProjKind::Scalar(Box::new(ast::Expr::CompoundIdentifier(vec![
                            ast::Ident::new(&def.name),
                            ast::Ident::new(&c.name),
                        ]))),
                    ));
                }
            }
            ast::SelectItem::UnnamedExpr(expr) => {
                let alias = match expr {
                    ast::Expr::Identifier(i) => i.value.clone(),
                    _ => format!("col_{}", projection_plan.len()),
                };
                if let Some((fname, arg)) = is_agg(expr) {
                    let idx = aggs_to_compute.len();
                    aggs_to_compute.push((fname, arg));
                    projection_plan.push((alias, ProjKind::Aggregate(idx)));
                } else {
                    projection_plan.push((alias, ProjKind::Scalar(Box::new(expr.clone()))));
                }
            }
            ast::SelectItem::ExprWithAlias { expr, alias } => {
                if let Some((fname, arg)) = is_agg(expr) {
                    let idx = aggs_to_compute.len();
                    aggs_to_compute.push((fname, arg));
                    projection_plan.push((alias.value.clone(), ProjKind::Aggregate(idx)));
                } else {
                    projection_plan.push((
                        alias.value.clone(),
                        ProjKind::Scalar(Box::new(expr.clone())),
                    ));
                }
            }
        }
    }

    // 3. Group By Analysis
    let group_by_exprs = match &select.group_by {
        ast::GroupByExpr::Expressions(exprs, _) => exprs.clone(),
        ast::GroupByExpr::All(_) => {
            return Err(MiniError::NotSupported("GROUP BY ALL not supported".into()))
        }
    };

    let is_grouped = !group_by_exprs.is_empty() || !aggs_to_compute.is_empty();

    if !is_grouped {
        let order_applied_pre_projection =
            try_apply_order_by_on_base_rows(&mut rows, query, &col_map)?;

        // Simple case: Just Map standard rows
        let mut final_rows = Vec::new();
        for row in rows {
            let mut out_row = Vec::new();
            for (_, kind) in &projection_plan {
                if let ProjKind::Scalar(e) = kind {
                    out_row.push(eval_row_expr(session, e.as_ref(), &row, &col_map)?);
                } else {
                    return Err(MiniError::Invalid(
                        "Unexpected aggregate in non-grouped query".into(),
                    ));
                }
            }
            final_rows.push(out_row);
        }

        if select.distinct.is_some() {
            final_rows = apply_distinct_rows(final_rows);
        }

        let aliases: Vec<String> = projection_plan.into_iter().map(|(a, _)| a).collect();
        return finish_select(
            defs, // Fixed: def -> defs
            final_rows,
            aliases,
            query,
            order_applied_pre_projection,
        );
    }

    // 4. Grouping Execution
    #[derive(Debug, PartialEq, Eq, Hash, Clone)]
    struct GroupKey(Vec<Cell>);

    struct GroupState {
        first_row: Row,
        accumulators: Vec<Box<dyn Accumulator>>,
    }

    trait Accumulator {
        fn add(&mut self, val: Cell);
        fn inc(&mut self);
        fn finish(&self) -> Cell;
    }

    struct CountAcc(i64);
    impl Accumulator for CountAcc {
        fn add(&mut self, _v: Cell) {
            self.0 += 1;
        }
        fn inc(&mut self) {
            self.0 += 1;
        }
        fn finish(&self) -> Cell {
            Cell::Int(self.0)
        }
    }

    struct SumAcc(Cell);
    impl Accumulator for SumAcc {
        fn add(&mut self, v: Cell) {
            if matches!(v, Cell::Null) {
                return;
            }
            if let Some(res) = self.0.add(&v) {
                self.0 = res;
            }
        }
        fn inc(&mut self) {}
        fn finish(&self) -> Cell {
            self.0.clone()
        }
    }

    struct AVGAcc {
        sum: Cell,
        count: i64,
    }
    impl Accumulator for AVGAcc {
        fn add(&mut self, v: Cell) {
            if matches!(v, Cell::Null) {
                return;
            }
            if let Some(res) = self.sum.add(&v) {
                self.sum = res;
                self.count += 1;
            }
        }
        fn inc(&mut self) {}
        fn finish(&self) -> Cell {
            if self.count == 0 {
                return Cell::Null;
            }
            self.sum
                .div_count(self.count as usize)
                .unwrap_or(Cell::Null)
        }
    }

    struct MinMaxAcc {
        val: Cell,
        is_min: bool,
    }
    impl Accumulator for MinMaxAcc {
        fn add(&mut self, v: Cell) {
            if matches!(v, Cell::Null) {
                return;
            }
            if matches!(self.val, Cell::Null) {
                self.val = v;
            } else {
                let cmp = compare_cell_for_order(&v, &self.val);
                if self.is_min {
                    if cmp == std::cmp::Ordering::Less {
                        self.val = v;
                    }
                } else if cmp == std::cmp::Ordering::Greater {
                    self.val = v;
                }
            }
        }
        fn inc(&mut self) {}
        fn finish(&self) -> Cell {
            self.val.clone()
        }
    }

    let mut groups: HashMap<GroupKey, GroupState> = HashMap::new();

    // Initialize implicit single group if needed (Standard SQL: SELECT count(*) FROM t returns 0 if empty)
    if rows.is_empty() && group_by_exprs.is_empty() {
        let mut accs: Vec<Box<dyn Accumulator>> = Vec::new();
        for (fname, _) in &aggs_to_compute {
            match fname.as_str() {
                "count" => accs.push(Box::new(CountAcc(0))),
                "sum" => accs.push(Box::new(SumAcc(Cell::Null))),
                "avg" => accs.push(Box::new(AVGAcc {
                    sum: Cell::Int(0),
                    count: 0,
                })), // Init at 0/0 -> Null
                "min" | "max" => accs.push(Box::new(MinMaxAcc {
                    val: Cell::Null,
                    is_min: fname == "min",
                })),
                _ => accs.push(Box::new(CountAcc(0))),
            }
        }
        groups.insert(
            GroupKey(vec![]),
            GroupState {
                first_row: Row { values: vec![] },
                accumulators: accs,
            },
        );
    }

    for row in rows {
        // Calc Key
        let mut key_cells = Vec::new();
        for expr in &group_by_exprs {
            key_cells.push(eval_row_expr(session, expr, &row, &col_map)?);
        }
        let key = GroupKey(key_cells);

        let entry = groups.entry(key).or_insert_with(|| {
            let mut accs: Vec<Box<dyn Accumulator>> = Vec::new();
            for (fname, _) in &aggs_to_compute {
                match fname.as_str() {
                    "count" => accs.push(Box::new(CountAcc(0))),
                    "sum" => accs.push(Box::new(SumAcc(Cell::Int(0)))),
                    "avg" => accs.push(Box::new(AVGAcc {
                        sum: Cell::Int(0),
                        count: 0,
                    })),
                    "min" | "max" => accs.push(Box::new(MinMaxAcc {
                        val: Cell::Null,
                        is_min: fname == "min",
                    })),
                    _ => accs.push(Box::new(CountAcc(0))),
                }
            }
            GroupState {
                first_row: row.clone(),
                accumulators: accs,
            }
        });

        // Update Accumulators
        for (i, (fname, arg_expr)) in aggs_to_compute.iter().enumerate() {
            if fname == "count" && arg_expr.is_none() {
                entry.accumulators[i].inc();
            } else if let Some(expr) = arg_expr {
                let val = eval_row_expr(session, expr, &row, &col_map)?;
                entry.accumulators[i].add(val);
            }
        }
    }

    // 5. Generate Results
    let mut result_rows = Vec::new();
    for (_key, state) in groups {
        let mut out_row = Vec::new();
        for (_, kind) in &projection_plan {
            match kind {
                ProjKind::Scalar(expr) => {
                    // Evaluate against representative row
                    out_row.push(eval_row_expr(
                        session,
                        expr.as_ref(),
                        &state.first_row,
                        &col_map,
                    )?);
                }
                ProjKind::Aggregate(idx) => {
                    out_row.push(state.accumulators[*idx].finish());
                }
            }
        }
        result_rows.push(out_row);
    }

    // 6. HAVING (Post-Aggregation Filtering)
    if let Some(having) = &select.having {
        let aliases: Vec<String> = projection_plan.iter().map(|(a, _)| a.clone()).collect();
        let out_map: HashMap<String, usize> = aliases
            .iter()
            .enumerate()
            .map(|(i, name)| (name.to_ascii_lowercase(), i))
            .collect();

        let mut filtered_rows = Vec::new();
        for row in result_rows {
            // Create a temporary Row wrapper for evaluation
            let r = Row {
                values: row.clone(),
            };
            if eval_condition(session, having, &r, &out_map)? {
                filtered_rows.push(row);
            }
        }
        result_rows = filtered_rows;
    }

    if select.distinct.is_some() {
        result_rows = apply_distinct_rows(result_rows);
    }

    let aliases: Vec<String> = projection_plan.into_iter().map(|(a, _)| a).collect();
    finish_select(defs, result_rows, aliases, query, false)
}

fn finish_select(
    defs: &[&TableDef],
    mut rows: Vec<Vec<Cell>>,
    aliases: Vec<String>,
    query: &ast::Query,
    order_applied_pre_projection: bool,
) -> Result<ExecOutput, MiniError> {
    // 6. Order By
    if !order_applied_pre_projection {
        if let Some(order_by) = &query.order_by {
            let exprs = match &order_by.kind {
                ast::OrderByKind::Expressions(e) => e,
                _ => return Err(MiniError::NotSupported("Order By ALL not supported".into())),
            };

            // Simplified: sort by alias or column index if possible
            // For now, strict limitation: ORDER BY must match output column alias OR index (1-based)

            let mut sort_keys = Vec::new();
            for e in exprs {
                let (idx, desc) = match &e.expr {
                    ast::Expr::Identifier(ident) => {
                        // Check aliases
                        if let Some(pos) = aliases
                            .iter()
                            .position(|a| a.eq_ignore_ascii_case(&ident.value))
                        {
                            (pos, e.options.asc == Some(false))
                        } else {
                            // Fallback? Error?
                            // Maybe it's a column name in the original TableDef?
                            // If so, we need to locate it in the Output if it passed through.
                            // For GROUP BY, we lose non-projected columns.
                            return Err(MiniError::NotSupported(
                                "Order By must match output column".into(),
                            ));
                        }
                    }
                    ast::Expr::Value(v) => {
                        match &v.value {
                            ast::Value::Number(n, _) => {
                                // 1-based index
                                let pos = n.parse::<usize>().map_err(|_| {
                                    MiniError::Invalid("Order By index must be an integer".into())
                                })?;
                                if (1..=aliases.len()).contains(&pos) {
                                    (pos - 1, e.options.asc == Some(false))
                                } else {
                                    return Err(MiniError::Invalid("Order By index OOB".into()));
                                }
                            }
                            _ => {
                                return Err(MiniError::NotSupported(
                                    "Complex Order By not implemented".into(),
                                ))
                            }
                        }
                    }
                    _ => {
                        return Err(MiniError::NotSupported(
                            "Complex Order By not implemented".into(),
                        ))
                    }
                };
                sort_keys.push((idx, desc));
            }

            rows.sort_by(|a, b| {
                for (idx, desc) in &sort_keys {
                    let cmp = compare_cell_for_order(&a[*idx], &b[*idx]);
                    let cmp = if *desc { cmp.reverse() } else { cmp };
                    if cmp != std::cmp::Ordering::Equal {
                        return cmp;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }
    }

    // 7. Output Schema
    let mut columns = Vec::new();
    for (idx, alias) in aliases.into_iter().enumerate() {
        let mut inferred = None::<ColumnType>;
        for row in &rows {
            let Some(cell) = row.get(idx) else { continue };
            match cell {
                Cell::Null => {}
                Cell::Int(_) => {
                    inferred = Some(ColumnType::MYSQL_TYPE_LONGLONG);
                    break;
                }
                Cell::Float(_) => {
                    inferred = Some(ColumnType::MYSQL_TYPE_DOUBLE);
                    break;
                }
                Cell::Text(_) | Cell::Date(_) | Cell::DateTime(_) => {
                    inferred = Some(ColumnType::MYSQL_TYPE_VAR_STRING);
                    break;
                }
            }
        }

        let coltype = inferred.unwrap_or_else(|| {
            // Check all tables
            let mut found_type = None;
            for def in defs {
                if let Some(c) = def
                    .columns
                    .iter()
                    .find(|c| c.name.eq_ignore_ascii_case(&alias))
                {
                    found_type = Some(match c.ty {
                        SqlType::Int => ColumnType::MYSQL_TYPE_LONGLONG,
                        SqlType::Float => ColumnType::MYSQL_TYPE_DOUBLE,
                        SqlType::Text | SqlType::Date | SqlType::DateTime => {
                            ColumnType::MYSQL_TYPE_VAR_STRING
                        }
                    });
                    break;
                }
            }
            found_type.unwrap_or(ColumnType::MYSQL_TYPE_VAR_STRING)
        });

        columns.push(Column {
            table: "".into(),
            column: alias,
            coltype,
            colflags: ColumnFlags::empty(),
        });
    }

    // 8. Limit/Offset
    let eval_nonneg_usize = |expr: &ast::Expr, what: &str| -> Result<usize, MiniError> {
        let v = eval_expr(expr)?
            .as_i64()
            .ok_or_else(|| MiniError::Invalid(format!("{what} must be an integer")))?;
        if v < 0 {
            return Err(MiniError::Invalid(format!("{what} cannot be negative")));
        }
        usize::try_from(v).map_err(|_| MiniError::Invalid(format!("{what} is too large")))
    };

    let mut offset = 0usize;
    let mut limit = None::<usize>;
    if let Some(limit_clause) = &query.limit_clause {
        match limit_clause {
            ast::LimitClause::LimitOffset {
                limit: lim,
                offset: off,
                ..
            } => {
                if let Some(lim_expr) = lim {
                    limit = Some(eval_nonneg_usize(lim_expr, "LIMIT")?);
                }
                if let Some(off) = off {
                    offset = eval_nonneg_usize(&off.value, "OFFSET")?;
                }
            }
            ast::LimitClause::OffsetCommaLimit {
                offset: off,
                limit: lim,
            } => {
                offset = eval_nonneg_usize(off, "OFFSET")?;
                limit = Some(eval_nonneg_usize(lim, "LIMIT")?);
            }
        }
    }

    if offset > 0 {
        if offset >= rows.len() {
            rows.clear();
        } else {
            rows.drain(0..offset);
        }
    }
    if let Some(limit) = limit {
        if limit < rows.len() {
            rows.truncate(limit);
        }
    }

    Ok(ExecOutput::ResultSet { columns, rows })
}

fn parse_sql_number_literal(n: &str) -> Result<Cell, MiniError> {
    let is_float = n.contains('.') || n.contains('e') || n.contains('E');
    if is_float {
        let v = n
            .parse::<f64>()
            .map_err(|_| MiniError::Invalid(format!("Invalid number literal: {n}")))?;
        Ok(Cell::Float(v))
    } else {
        let v = n
            .parse::<i64>()
            .map_err(|_| MiniError::Invalid(format!("Invalid integer literal: {n}")))?;
        Ok(Cell::Int(v))
    }
}

fn eval_row_expr(
    session: &SessionState,
    expr: &ast::Expr,
    row: &Row,
    col_map: &std::collections::HashMap<String, usize>,
) -> Result<Cell, MiniError> {
    match expr {
        ast::Expr::Nested(inner) => eval_row_expr(session, inner, row, col_map),
        ast::Expr::Function(f) => {
            let name = f.name.to_string().to_ascii_lowercase();
            match name.as_str() {
                "database" | "schema" => {
                    Ok(Cell::Text(session.current_db.clone().unwrap_or_default()))
                }
                "version" => Ok(Cell::Text(SERVER_VERSION.to_string())),
                "connection_id" => Ok(Cell::Int(i64::from(session.conn_id))),
                "user" | "current_user" => Ok(Cell::Text(session.username.clone())),
                _ => Err(MiniError::NotSupported(format!(
                    "Function not supported in expressions: {}",
                    f.name
                ))),
            }
        }
        ast::Expr::Value(v) => match &v.value {
            ast::Value::Number(n, _) => parse_sql_number_literal(n),
            ast::Value::SingleQuotedString(s) => Ok(Cell::Text(s.clone())),
            ast::Value::Null => Ok(Cell::Null),
            _ => Err(MiniError::NotSupported(format!(
                "Value type not supported: {}",
                v.value
            ))),
        },
        ast::Expr::Identifier(ident) => {
            let name = ident.value.to_ascii_lowercase();
            if let Some(&idx) = col_map.get(&name) {
                if idx == usize::MAX {
                    return Err(MiniError::Invalid(format!(
                        "Ambiguous column reference: {}",
                        ident.value
                    )));
                }
                Ok(row.values.get(idx).cloned().unwrap_or(Cell::Null))
            } else {
                Err(MiniError::Invalid(format!(
                    "Column not found: {}",
                    ident.value
                )))
            }
        }
        ast::Expr::CompoundIdentifier(ids) => {
            // Try fully qualified match first (e.g. table.col)
            // We assume ids are [table, col] or [db, table, col].
            // Our col_map stores "table.col".
            let full_name = ids
                .iter()
                .map(|i| i.value.clone())
                .collect::<Vec<_>>()
                .join(".")
                .to_ascii_lowercase();
            if let Some(&idx) = col_map.get(&full_name) {
                if idx == usize::MAX {
                    return Err(MiniError::Invalid(format!(
                        "Ambiguous column reference: {}",
                        full_name
                    )));
                }
                return Ok(row.values.get(idx).cloned().unwrap_or(Cell::Null));
            }

            // Try last 2 parts if len > 2 (handle db.table.col -> table.col)
            if ids.len() > 2 {
                let last_two = format!("{}.{}", ids[ids.len() - 2].value, ids[ids.len() - 1].value)
                    .to_ascii_lowercase();
                if let Some(&idx) = col_map.get(&last_two) {
                    if idx == usize::MAX {
                        return Err(MiniError::Invalid(format!(
                            "Ambiguous column reference: {}",
                            last_two
                        )));
                    }
                    return Ok(row.values.get(idx).cloned().unwrap_or(Cell::Null));
                }
            }

            // Fallback to strict column name (last part)
            // This is risky if ambiguous, but matches current permissive behavior
            let dim_name = ids
                .last()
                .ok_or_else(|| MiniError::Invalid("empty identifier".into()))?
                .value
                .to_ascii_lowercase();
            if let Some(&idx) = col_map.get(&dim_name) {
                if idx == usize::MAX {
                    return Err(MiniError::Invalid(format!(
                        "Ambiguous column reference: {}",
                        dim_name
                    )));
                }
                Ok(row.values.get(idx).cloned().unwrap_or(Cell::Null))
            } else {
                Err(MiniError::Invalid(format!(
                    "Column not found: {}",
                    full_name
                )))
            }
        }
        _ => Err(MiniError::NotSupported(format!(
            "Expr not supported in WHERE: {}",
            expr
        ))),
    }
}

fn eval_condition(
    session: &SessionState,
    expr: &ast::Expr,
    row: &Row,
    col_map: &std::collections::HashMap<String, usize>,
) -> Result<bool, MiniError> {
    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    enum TriBool {
        True,
        False,
        Unknown,
    }

    impl TriBool {
        fn and(self, other: TriBool) -> TriBool {
            match (self, other) {
                (TriBool::False, _) | (_, TriBool::False) => TriBool::False,
                (TriBool::True, b) => b,
                (TriBool::Unknown, TriBool::True) => TriBool::Unknown,
                (TriBool::Unknown, TriBool::Unknown) => TriBool::Unknown,
            }
        }

        fn or(self, other: TriBool) -> TriBool {
            match (self, other) {
                (TriBool::True, _) | (_, TriBool::True) => TriBool::True,
                (TriBool::False, b) => b,
                (TriBool::Unknown, TriBool::False) => TriBool::Unknown,
                (TriBool::Unknown, TriBool::Unknown) => TriBool::Unknown,
            }
        }

        fn not(self) -> TriBool {
            match self {
                TriBool::True => TriBool::False,
                TriBool::False => TriBool::True,
                TriBool::Unknown => TriBool::Unknown,
            }
        }

        fn is_true(self) -> bool {
            matches!(self, TriBool::True)
        }
    }

    fn eval_tri(
        session: &SessionState,
        expr: &ast::Expr,
        row: &Row,
        col_map: &std::collections::HashMap<String, usize>,
    ) -> Result<TriBool, MiniError> {
        match expr {
            ast::Expr::Nested(inner) => eval_tri(session, inner, row, col_map),
            ast::Expr::BinaryOp { left, op, right } => {
                match op {
                    ast::BinaryOperator::And => {
                        return Ok(eval_tri(session, left, row, col_map)?
                            .and(eval_tri(session, right, row, col_map)?));
                    }
                    ast::BinaryOperator::Or => {
                        return Ok(eval_tri(session, left, row, col_map)?
                            .or(eval_tri(session, right, row, col_map)?));
                    }
                    _ => {}
                }

                let l_val = eval_row_expr(session, left, row, col_map)?;
                let r_val = eval_row_expr(session, right, row, col_map)?;
                if matches!(l_val, Cell::Null) || matches!(r_val, Cell::Null) {
                    return Ok(TriBool::Unknown);
                }

                // Type coercion for comparison
                let (l_final, r_final) = match (&l_val, &r_val) {
                    (Cell::Float(_), Cell::Text(s)) | (Cell::Text(s), Cell::Float(_)) => {
                        // Try to coerce text to float
                        if let Ok(f) = s.parse::<f64>() {
                            if matches!(l_val, Cell::Float(_)) {
                                (l_val.clone(), Cell::Float(f))
                            } else {
                                (Cell::Float(f), r_val.clone())
                            }
                        } else {
                            (l_val.clone(), r_val.clone()) // Fallback
                        }
                    }
                    // String compare is fine for ISO dates.
                    _ => (l_val.clone(), r_val.clone()),
                };

                let cmp = compare_cell_for_order(&l_final, &r_final);
                let ok = match op {
                    ast::BinaryOperator::Eq => cmp == std::cmp::Ordering::Equal,
                    ast::BinaryOperator::NotEq => cmp != std::cmp::Ordering::Equal,
                    ast::BinaryOperator::Gt => cmp == std::cmp::Ordering::Greater,
                    ast::BinaryOperator::Lt => cmp == std::cmp::Ordering::Less,
                    ast::BinaryOperator::GtEq => cmp != std::cmp::Ordering::Less,
                    ast::BinaryOperator::LtEq => cmp != std::cmp::Ordering::Greater,
                    _ => {
                        return Err(MiniError::NotSupported(format!(
                            "Operator not supported: {}",
                            op
                        )))
                    }
                };

                Ok(if ok { TriBool::True } else { TriBool::False })
            }
            ast::Expr::UnaryOp { op, expr } => match op {
                ast::UnaryOperator::Not => Ok(eval_tri(session, expr, row, col_map)?.not()),
                _ => Err(MiniError::NotSupported(format!(
                    "Unary operator not supported in WHERE: {}",
                    op
                ))),
            },
            ast::Expr::IsNull(expr) => {
                let v = eval_row_expr(session, expr, row, col_map)?;
                Ok(if matches!(v, Cell::Null) {
                    TriBool::True
                } else {
                    TriBool::False
                })
            }
            ast::Expr::IsNotNull(expr) => {
                let v = eval_row_expr(session, expr, row, col_map)?;
                Ok(if matches!(v, Cell::Null) {
                    TriBool::False
                } else {
                    TriBool::True
                })
            }
            ast::Expr::InList {
                expr,
                list,
                negated,
            } => {
                if list.is_empty() {
                    return Err(MiniError::Invalid("IN (...) list cannot be empty".into()));
                }

                let needle = eval_row_expr(session, expr, row, col_map)?;
                if matches!(needle, Cell::Null) {
                    return Ok(TriBool::Unknown);
                }

                let mut has_null = false;
                for item in list {
                    let v = eval_row_expr(session, item, row, col_map)?;
                    if matches!(v, Cell::Null) {
                        has_null = true;
                        continue;
                    }
                    if compare_cell_for_order(&needle, &v) == std::cmp::Ordering::Equal {
                        return Ok(if *negated {
                            TriBool::False
                        } else {
                            TriBool::True
                        });
                    }
                }

                let base = if has_null {
                    TriBool::Unknown
                } else {
                    TriBool::False
                };
                Ok(if *negated { base.not() } else { base })
            }
            ast::Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let v = eval_row_expr(session, expr, row, col_map)?;
                let lo = eval_row_expr(session, low, row, col_map)?;
                let hi = eval_row_expr(session, high, row, col_map)?;
                if matches!(v, Cell::Null) || matches!(lo, Cell::Null) || matches!(hi, Cell::Null) {
                    return Ok(TriBool::Unknown);
                }

                let ge_lo = compare_cell_for_order(&v, &lo) != std::cmp::Ordering::Less;
                let le_hi = compare_cell_for_order(&v, &hi) != std::cmp::Ordering::Greater;
                let base = if ge_lo && le_hi {
                    TriBool::True
                } else {
                    TriBool::False
                };
                Ok(if *negated { base.not() } else { base })
            }
            ast::Expr::Like {
                negated,
                any,
                expr,
                pattern,
                escape_char,
            } => {
                if *any {
                    return Err(MiniError::NotSupported(
                        "LIKE ANY(...) is not supported".into(),
                    ));
                }

                let v = eval_row_expr(session, expr, row, col_map)?;
                let pat = eval_row_expr(session, pattern, row, col_map)?;
                if matches!(v, Cell::Null) || matches!(pat, Cell::Null) {
                    return Ok(TriBool::Unknown);
                }

                let escape = like_escape_char(escape_char.as_ref())?;
                let ok = sql_like_matches(&cell_to_string(&v), &cell_to_string(&pat), escape);
                let base = if ok { TriBool::True } else { TriBool::False };
                Ok(if *negated { base.not() } else { base })
            }
            ast::Expr::ILike {
                negated,
                any,
                expr,
                pattern,
                escape_char,
            } => {
                if *any {
                    return Err(MiniError::NotSupported(
                        "ILIKE ANY(...) is not supported".into(),
                    ));
                }

                let v = eval_row_expr(session, expr, row, col_map)?;
                let pat = eval_row_expr(session, pattern, row, col_map)?;
                if matches!(v, Cell::Null) || matches!(pat, Cell::Null) {
                    return Ok(TriBool::Unknown);
                }

                let escape = like_escape_char(escape_char.as_ref())?;
                let ok = sql_like_matches(
                    &cell_to_string(&v).to_ascii_lowercase(),
                    &cell_to_string(&pat).to_ascii_lowercase(),
                    escape,
                );
                let base = if ok { TriBool::True } else { TriBool::False };
                Ok(if *negated { base.not() } else { base })
            }
            _ => Err(MiniError::NotSupported(format!(
                "Condition not supported: {}",
                expr
            ))),
        }
    }

    Ok(eval_tri(session, expr, row, col_map)?.is_true())
}

fn coerce_cell(cell: Cell, target: &SqlType) -> Result<Cell, MiniError> {
    match (target, &cell) {
        (SqlType::Float, Cell::Int(i)) => Ok(Cell::Float(*i as f64)),
        (SqlType::Float, Cell::Text(s)) => {
            let f = s
                .parse::<f64>()
                .map_err(|_| MiniError::Invalid(format!("Invalid float: {s}")))?;
            Ok(Cell::Float(f))
        }
        (SqlType::Date, Cell::Text(s)) => {
            // Try YYYY-MM-DD
            if let Ok(dt) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                let days = (dt - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days();
                return Ok(Cell::Date(days));
            }
            Err(MiniError::Invalid(format!(
                "Invalid date format: {s} (expected YYYY-MM-DD)"
            )))
        }
        (SqlType::DateTime, Cell::Text(s)) => {
            // Try YYYY-MM-DD HH:MM:SS
            if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                let millis = dt.and_utc().timestamp_millis();
                return Ok(Cell::DateTime(millis));
            }
            Err(MiniError::Invalid(format!("Invalid datetime format: {s}")))
        }
        // Passthrough if match or other types
        _ => Ok(cell),
    }
}

fn eval_expr(expr: &ast::Expr) -> Result<Cell, MiniError> {
    match expr {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::Number(n, _) => parse_sql_number_literal(n),
            ast::Value::SingleQuotedString(s) => Ok(Cell::Text(s.clone())),
            ast::Value::Null => Ok(Cell::Null),
            _ => Err(MiniError::NotSupported(format!(
                "Value type not supported: {}",
                v.value
            ))),
        },
        ast::Expr::Identifier(ident) => Ok(Cell::Text(ident.value.clone())),
        _ => Err(MiniError::NotSupported(format!(
            "Expr not supported: {}",
            expr
        ))),
    }
}

fn parse_eq_predicate(expr: &ast::Expr) -> Result<(String, Cell), MiniError> {
    match expr {
        ast::Expr::BinaryOp { left, op, right } if *op == ast::BinaryOperator::Eq => {
            let col = match left.as_ref() {
                ast::Expr::Identifier(ident) => ident.value.clone(),
                ast::Expr::CompoundIdentifier(ids) => ids
                    .last()
                    .ok_or_else(|| MiniError::Invalid("empty identifier".into()))?
                    .value
                    .clone(),
                _ => {
                    return Err(MiniError::NotSupported(
                        "WHERE left side must be a column".into(),
                    ))
                }
            };
            let val = eval_expr(right)?;
            Ok((col, val))
        }
        _ => Err(MiniError::NotSupported(
            "Only WHERE col = val supported".into(),
        )),
    }
}

fn object_name_to_parts(name: &ObjectName) -> Result<(Option<String>, String), MiniError> {
    match name.0.len() {
        1 => Ok((None, get_ident_name(&name.0[0]))),
        2 => Ok((Some(get_ident_name(&name.0[0])), get_ident_name(&name.0[1]))),
        _ => Err(MiniError::NotSupported(
            "object name with more than 2 parts is not supported".into(),
        )),
    }
}

fn like_escape_char(escape_char: Option<&ast::Value>) -> Result<char, MiniError> {
    let Some(v) = escape_char else {
        return Ok('\\');
    };

    let s = match v {
        ast::Value::SingleQuotedString(s) => s.as_str(),
        ast::Value::DoubleQuotedString(s) => s.as_str(),
        _ => {
            return Err(MiniError::NotSupported(
                "ESCAPE value must be a quoted string".into(),
            ))
        }
    };

    let mut chars = s.chars();
    let Some(ch) = chars.next() else {
        return Err(MiniError::Invalid("ESCAPE string cannot be empty".into()));
    };
    if chars.next().is_some() {
        return Err(MiniError::Invalid(
            "ESCAPE string must be a single character".into(),
        ));
    }
    Ok(ch)
}

fn sql_like_matches(text: &str, pattern: &str, escape: char) -> bool {
    let t: Vec<char> = text.chars().collect();
    let p: Vec<char> = pattern.chars().collect();

    let mut ti = 0usize;
    let mut pi = 0usize;

    let mut star_pi: Option<usize> = None;
    let mut star_ti = 0usize;

    while ti < t.len() {
        if pi < p.len() {
            let pc = p[pi];
            if pc == '%' {
                star_pi = Some(pi);
                pi += 1;
                while pi < p.len() && p[pi] == '%' {
                    pi += 1;
                }
                star_ti = ti;
                continue;
            }

            if pc == escape {
                if pi + 1 < p.len() {
                    let lit = p[pi + 1];
                    if lit == t[ti] {
                        pi += 2;
                        ti += 1;
                        continue;
                    }
                } else if pc == t[ti] {
                    pi += 1;
                    ti += 1;
                    continue;
                }
            } else if pc == '_' || pc == t[ti] {
                pi += 1;
                ti += 1;
                continue;
            }
        }

        if let Some(star_pos) = star_pi {
            star_ti += 1;
            ti = star_ti;
            pi = star_pos + 1;
            continue;
        }

        return false;
    }

    while pi < p.len() {
        if p[pi] == '%' {
            pi += 1;
            continue;
        }
        if p[pi] == escape && pi + 1 < p.len() {
            return false;
        }
        break;
    }

    pi == p.len()
}

fn table_def_has_column(def: &TableDef, col: &str) -> bool {
    def.columns.iter().any(|c| c.name.eq_ignore_ascii_case(col))
}

fn find_unique_table_for_column<'a>(
    defs: &'a [&'a TableDef],
    col: &str,
) -> Result<&'a TableDef, MiniError> {
    let mut matches = defs
        .iter()
        .copied()
        .filter(|d| table_def_has_column(d, col));
    let Some(first) = matches.next() else {
        return Err(MiniError::NotFound(format!(
            "unknown column `{col}` in JOIN constraint"
        )));
    };
    if matches.next().is_some() {
        return Err(MiniError::Invalid(format!(
            "ambiguous column `{col}` in JOIN constraint"
        )));
    }
    Ok(first)
}

fn using_column_name(name: &ObjectName) -> Result<String, MiniError> {
    if name.0.len() != 1 {
        return Err(MiniError::NotSupported(
            "qualified column names in USING(...) are not supported".into(),
        ));
    }
    let col = get_ident_name(&name.0[0]);
    if col.is_empty() {
        return Err(MiniError::NotSupported(
            "non-identifier column names in USING(...) are not supported".into(),
        ));
    }
    Ok(col)
}

fn build_eq_column_expr(left_table: &str, right_table: &str, col: &str) -> ast::Expr {
    ast::Expr::BinaryOp {
        left: Box::new(ast::Expr::CompoundIdentifier(vec![
            Ident::new(left_table),
            Ident::new(col),
        ])),
        op: ast::BinaryOperator::Eq,
        right: Box::new(ast::Expr::CompoundIdentifier(vec![
            Ident::new(right_table),
            Ident::new(col),
        ])),
    }
}

fn build_and_expr(left: ast::Expr, right: ast::Expr) -> ast::Expr {
    ast::Expr::BinaryOp {
        left: Box::new(left),
        op: ast::BinaryOperator::And,
        right: Box::new(right),
    }
}

fn build_using_join_on_expr(
    left_defs: &[&TableDef],
    right_def: &TableDef,
    cols: &[ObjectName],
) -> Result<ast::Expr, MiniError> {
    if cols.is_empty() {
        return Err(MiniError::Invalid(
            "USING(...) must specify at least one column".into(),
        ));
    }

    let right_table = right_def.name.clone();
    let mut expr_opt: Option<ast::Expr> = None;

    for col_obj in cols {
        let col = using_column_name(col_obj)?;

        if !table_def_has_column(right_def, &col) {
            return Err(MiniError::NotFound(format!(
                "unknown column `{col}` in right table for USING(...)"
            )));
        }

        let left_def = find_unique_table_for_column(left_defs, &col)?;
        let eq = build_eq_column_expr(&left_def.name, &right_table, &col);
        expr_opt = Some(match expr_opt {
            None => eq,
            Some(prev) => build_and_expr(prev, eq),
        });
    }

    Ok(expr_opt.expect("cols is non-empty"))
}

fn build_natural_join_on_expr(
    left_defs: &[&TableDef],
    right_def: &TableDef,
) -> Result<Option<ast::Expr>, MiniError> {
    let right_table = right_def.name.clone();
    let mut expr_opt: Option<ast::Expr> = None;

    for col_def in &right_def.columns {
        let col = &col_def.name;

        let mut matches = left_defs
            .iter()
            .copied()
            .filter(|d| table_def_has_column(d, col));
        let Some(left_def) = matches.next() else {
            continue;
        };
        if matches.next().is_some() {
            return Err(MiniError::Invalid(format!(
                "ambiguous NATURAL join column: {col}"
            )));
        }

        let eq = build_eq_column_expr(&left_def.name, &right_table, col);
        expr_opt = Some(match expr_opt {
            None => eq,
            Some(prev) => build_and_expr(prev, eq),
        });
    }

    Ok(expr_opt)
}

fn extract_equi_join_pairs(
    expr: &ast::Expr,
    col_map: &std::collections::HashMap<String, usize>,
    left_col_count: usize,
) -> Option<Vec<(usize, usize)>> {
    fn collect_and_terms<'a>(expr: &'a ast::Expr, out: &mut Vec<&'a ast::Expr>) {
        match expr {
            ast::Expr::BinaryOp {
                left,
                op: ast::BinaryOperator::And,
                right,
            } => {
                collect_and_terms(left, out);
                collect_and_terms(right, out);
            }
            other => out.push(other),
        }
    }

    let mut terms = Vec::new();
    collect_and_terms(expr, &mut terms);

    let mut pairs = Vec::new();
    for term in terms {
        let ast::Expr::BinaryOp { left, op, right } = term else {
            return None;
        };
        if *op != ast::BinaryOperator::Eq {
            return None;
        }

        let l_idx = order_by_expr_to_base_col_idx(left, col_map)?;
        let r_idx = order_by_expr_to_base_col_idx(right, col_map)?;

        if l_idx < left_col_count && r_idx >= left_col_count {
            pairs.push((l_idx, r_idx - left_col_count));
        } else if r_idx < left_col_count && l_idx >= left_col_count {
            pairs.push((r_idx, l_idx - left_col_count));
        } else {
            return None;
        }
    }

    if pairs.is_empty() {
        None
    } else {
        Some(pairs)
    }
}

fn eval_equi_join_pairs(left: &Row, right: &Row, pairs: &[(usize, usize)]) -> bool {
    for (l_idx, r_idx) in pairs {
        let Some(l) = left.values.get(*l_idx) else {
            return false;
        };
        let Some(r) = right.values.get(*r_idx) else {
            return false;
        };
        if matches!(l, Cell::Null) || matches!(r, Cell::Null) {
            return false;
        }
        if compare_cell_for_order(l, r) != std::cmp::Ordering::Equal {
            return false;
        }
    }
    true
}

fn compare_cell_for_order(a: &Cell, b: &Cell) -> std::cmp::Ordering {
    match (a, b) {
        (Cell::Int(a_val), Cell::Int(b_val)) => a_val.cmp(b_val),
        (Cell::Float(a_val), Cell::Float(b_val)) => a_val
            .partial_cmp(b_val)
            .unwrap_or(std::cmp::Ordering::Equal),
        (Cell::Int(_), Cell::Float(_)) | (Cell::Float(_), Cell::Int(_)) => {
            let Some(a_num) = a.as_f64() else {
                return std::cmp::Ordering::Equal;
            };
            let Some(b_num) = b.as_f64() else {
                return std::cmp::Ordering::Equal;
            };
            a_num
                .partial_cmp(&b_num)
                .unwrap_or(std::cmp::Ordering::Equal)
        }
        (Cell::Text(a_val), Cell::Text(b_val)) => a_val.cmp(b_val),
        (Cell::Date(a_val), Cell::Date(b_val)) => a_val.cmp(b_val),
        (Cell::DateTime(a_val), Cell::DateTime(b_val)) => a_val.cmp(b_val),
        (Cell::Null, Cell::Null) => std::cmp::Ordering::Equal,
        // Nulls are typically sorted first or last depending on SQL dialect and specific clauses.
        // For simplicity, let's put Nulls first.
        (Cell::Null, _) => std::cmp::Ordering::Less,
        (_, Cell::Null) => std::cmp::Ordering::Greater,
        // Mixed types - arbitrary order, or error. For simplicity, let's convert to string and compare.
        _ => cell_to_string(a).cmp(&cell_to_string(b)),
    }
}

fn cell_to_string(c: &Cell) -> String {
    match c {
        Cell::Int(i) => i.to_string(),
        Cell::Float(f) => f.to_string(),
        Cell::Text(s) => s.clone(),
        Cell::Date(days) => {
            use chrono::TimeZone;
            let secs = days.saturating_mul(86_400);
            match chrono::Utc.timestamp_opt(secs, 0).single() {
                Some(dt) => dt.format("%Y-%m-%d").to_string(),
                None => secs.to_string(),
            }
        }
        Cell::DateTime(millis) => {
            use chrono::TimeZone;
            match chrono::Utc.timestamp_millis_opt(*millis).single() {
                Some(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
                None => millis.to_string(),
            }
        }
        Cell::Null => "NULL".into(),
    }
}

fn should_buffer_writes(session: &SessionState) -> bool {
    session.txn.in_txn || !session.autocommit
}

fn txn_get_row(
    store: &Store,
    session: &SessionState,
    db: &str,
    table: &str,
    pk: i64,
) -> Result<Option<Row>, MiniError> {
    // Check local writes first (Read My Own Writes)
    if !session.txn.pending_rows.is_empty() {
        let key = RowKey {
            db: db.to_string(),
            table: table.to_string(),
            pk,
        };
        if let Some(v) = session.txn.pending_rows.get(&key) {
            return Ok(v.clone());
        }
    }
    // Fallback to store
    let view = session
        .txn
        .read_view
        .as_ref()
        .ok_or_else(|| MiniError::Invalid("No active transaction view".into()))?;
    store.get_row_mvcc(db, table, pk, view)
}

fn txn_scan_rows(
    store: &Store,
    session: &SessionState,
    db: &str,
    table: &str,
) -> Result<Vec<(i64, Row)>, MiniError> {
    let view = session
        .txn
        .read_view
        .as_ref()
        .ok_or_else(|| MiniError::Invalid("No active transaction view".into()))?;
    let base = store.scan_rows_mvcc(db, table, view)?;

    if session.txn.pending_rows.is_empty() {
        return Ok(base);
    }

    let mut merged: BTreeMap<i64, Row> = base.into_iter().collect();
    for (k, v) in &session.txn.pending_rows {
        if k.db == db && k.table == table {
            match v {
                Some(row) => {
                    merged.insert(k.pk, row.clone());
                }
                None => {
                    merged.remove(&k.pk);
                }
            }
        }
    }
    Ok(merged.into_iter().collect())
}

fn ensure_txn_active(store: &Store, session: &mut SessionState) {
    if session.txn.tx_id.is_none() {
        let (tx, view) = store.txn_manager.start_txn();
        session.txn.tx_id = Some(tx);
        session.txn.read_view = Some(view);
    }
}

fn txn_commit(store: &Store, session: &mut SessionState) -> Result<(), MiniError> {
    if let Some(tx_id) = session.txn.tx_id {
        if !session.txn.pending_rows.is_empty() {
            // Convert BTreeMap iterator to what apply_row_changes_mvcc expects
            let changes = session
                .txn
                .pending_rows
                .iter()
                .map(|(k, v)| (k.db.as_str(), k.table.as_str(), k.pk, v.as_ref()));
            store.apply_row_changes_mvcc(changes, tx_id)?;
        }
        store.txn_manager.commit_txn(tx_id);
    }

    session.txn.tx_id = None;
    session.txn.read_view = None;
    session.txn.pending_rows.clear();
    session.txn.savepoints.clear();
    store.unlock_all(session.conn_id);
    Ok(())
}

fn txn_rollback(store: &Store, session: &mut SessionState) {
    if let Some(tx_id) = session.txn.tx_id {
        store.txn_manager.rollback_txn(tx_id);
    }
    session.txn.tx_id = None;
    session.txn.read_view = None;
    session.txn.pending_rows.clear();
    session.txn.savepoints.clear();
    store.unlock_all(session.conn_id);
}

fn get_ident_name(part: &ObjectNamePart) -> String {
    match part {
        ObjectNamePart::Identifier(i) => i.value.clone(),
        _ => "".to_string(),
    }
}

fn handle_create_database(
    store: &Store,
    session: &mut SessionState,
    user: &UserRecord,
    name: &ObjectName,
    if_not_exists: bool,
) -> Result<ExecOutput, MiniError> {
    require_priv(user, None, Priv::CREATE)?;
    txn_commit(store, session)?;
    let db_name = get_ident_name(name.0.last().unwrap());

    match store.create_database(&db_name) {
        Ok(_) => {}
        Err(MiniError::Invalid(msg)) if if_not_exists && msg.contains("exists") => {
            // Ignore
        }
        Err(e) => return Err(e),
    }
    Ok(ExecOutput::Ok {
        affected_rows: 1,
        last_insert_id: 0,
        info: "".into(),
    })
}

fn handle_drop_database(
    store: &Store,
    session: &mut SessionState,
    user: &UserRecord,
    name: &ObjectName,
    if_exists: bool,
) -> Result<ExecOutput, MiniError> {
    require_priv(user, None, Priv::DROP)?;
    txn_commit(store, session)?;
    let db_name = get_ident_name(name.0.last().unwrap());

    match store.drop_database(&db_name) {
        Ok(_) => {}
        Err(MiniError::NotFound(_)) if if_exists => {
            // Ignore
        }
        Err(e) => return Err(e),
    }
    Ok(ExecOutput::Ok {
        affected_rows: 1,
        last_insert_id: 0,
        info: "".into(),
    })
}

fn handle_create_index(
    store: &Store,
    session: &mut SessionState,
    user: &UserRecord,
    create_index: &ast::CreateIndex,
) -> Result<ExecOutput, MiniError> {
    require_priv(user, session.current_db.as_deref(), Priv::CREATE)?; // Create priv
    txn_commit(store, session)?; // Implicit commit

    let (db_opt, table) = object_name_to_parts(&create_index.table_name)?;
    let db = db_opt
        .or_else(|| session.current_db.clone())
        .ok_or_else(|| MiniError::Invalid("no database selected".into()))?;

    // Index Name
    let idx_name = if let Some(n) = &create_index.name {
        // ObjectName to string (last part)
        get_ident_name(n.0.last().unwrap())
    } else {
        // Auto-generate name based on column?
        if create_index.columns.is_empty() {
            return Err(MiniError::Parse("Index requires columns".into()));
        }
        let expr = &create_index.columns[0].column.expr;
        match expr {
            ast::Expr::Identifier(ident) => format!("idx_{}", ident.value),
            _ => "idx_unknown".to_string(),
        }
    };

    if create_index.unique {
        return Err(MiniError::NotSupported(
            "UNIQUE index not supported in MVP".into(),
        ));
    }

    let mut col_names = Vec::new();
    for col in &create_index.columns {
        match &col.column.expr {
            ast::Expr::Identifier(ident) => col_names.push(ident.value.clone()),
            _ => {
                return Err(MiniError::NotSupported(
                    "Index on complex expr not supported".into(),
                ))
            }
        }
    }

    let index_def = IndexDef {
        name: idx_name,
        columns: col_names,
    };

    match store.create_index(&db, &table, index_def) {
        Ok(_) => {}
        Err(MiniError::Invalid(msg))
            if create_index.if_not_exists && msg.contains("already exists") =>
        {
            // Ignore
        }
        Err(e) => return Err(e),
    }

    Ok(ExecOutput::Ok {
        affected_rows: 0,
        last_insert_id: 0,
        info: "Index created".into(),
    })
}

fn handle_create_table(
    store: &Store,
    session: &mut SessionState,
    user: &UserRecord,
    name: &ObjectName,
    columns: &[ast::ColumnDef],
    constraints: &[ast::TableConstraint],
    if_not_exists: bool,
) -> Result<ExecOutput, MiniError> {
    require_priv(user, session.current_db.as_deref(), Priv::CREATE)?;
    txn_commit(store, session)?;

    let (db_opt, table_name) = match name.0.len() {
        1 => (None, get_ident_name(&name.0[0])),
        2 => (Some(get_ident_name(&name.0[0])), get_ident_name(&name.0[1])),
        _ => return Err(MiniError::Parse("Invalid table name".into())),
    };

    let db = db_opt
        .or_else(|| session.current_db.clone())
        .ok_or_else(|| MiniError::Invalid("no database selected".into()))?;

    let mut my_columns = Vec::new();
    let mut primary_key: Option<String> = None;
    let mut auto_inc_cols: HashSet<String> = HashSet::new();

    for col in columns {
        let col_name = col.name.value.clone();
        let sql_ty = match &col.data_type {
            ast::DataType::Int(_)
            | ast::DataType::BigInt(_)
            | ast::DataType::Integer(_)
            | ast::DataType::TinyInt(_)
            | ast::DataType::SmallInt(_) => SqlType::Int,
            ast::DataType::Float(_)
            | ast::DataType::Double(_)
            | ast::DataType::DoublePrecision
            | ast::DataType::Real => SqlType::Float,
            ast::DataType::Date => SqlType::Date,
            ast::DataType::Datetime(_) | ast::DataType::Timestamp(_, _) => SqlType::DateTime,
            _ => SqlType::Text, // Fallback
        };

        let mut nullable = true;
        let mut auto_increment = false;
        for opt in &col.options {
            match &opt.option {
                ast::ColumnOption::NotNull => nullable = false,
                ast::ColumnOption::Unique(_) => { /* Unique but not PK here? */ }
                ast::ColumnOption::PrimaryKey(_) => primary_key = Some(col_name.clone()),
                ast::ColumnOption::DialectSpecific(tokens) => {
                    let text = tokens
                        .iter()
                        .map(|t| t.to_string())
                        .collect::<Vec<_>>()
                        .join(" ");
                    if text.to_ascii_lowercase().contains("auto_increment") {
                        auto_increment = true;
                    }
                }
                _ => {}
            }
        }
        if auto_increment {
            auto_inc_cols.insert(col_name.to_ascii_lowercase());
        }

        my_columns.push(crate::model::ColumnDef {
            name: col_name,
            ty: sql_ty,
            nullable,
        });
    }

    for c in constraints {
        match c {
            ast::TableConstraint::Unique(_u) => {
                // Check if it's primary? No, PrimaryKey is separate.
            }
            ast::TableConstraint::PrimaryKey(pk) => {
                if !pk.columns.is_empty() {
                    // pk.columns is Vec<IndexColumn>.
                    // IndexColumn has column: OrderByExpr. OrderByExpr has expr: Expr.
                    let order_expr = &pk.columns[0].column;
                    if let ast::Expr::Identifier(ident) = &order_expr.expr {
                        primary_key = Some(ident.value.clone());
                    }
                }
            }
            _ => {}
        }
    }

    let pk = primary_key.ok_or_else(|| MiniError::Invalid("PRIMARY KEY required".into()))?;
    let table_auto_increment = auto_inc_cols.contains(&pk.to_ascii_lowercase());

    // Check PK type
    let pk_col = my_columns
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(&pk))
        .ok_or(MiniError::Parse("PK col missing".into()))?;
    if pk_col.ty != SqlType::Int {
        return Err(MiniError::Invalid("PRIMARY KEY must be INT".into()));
    }

    let def = TableDef {
        db,
        name: table_name,
        columns: my_columns,
        primary_key: pk,
        auto_increment: table_auto_increment,
        indexes: vec![],
    };

    match store.create_table(&def) {
        Ok(_) => {}
        Err(MiniError::Invalid(msg)) if if_not_exists && msg.contains("exists") => {}
        Err(e) => return Err(e),
    }

    Ok(ExecOutput::Ok {
        affected_rows: 1,
        last_insert_id: 0,
        info: "".into(),
    })
}

fn handle_alter_table(
    store: &Store,
    session: &mut SessionState,
    user: &UserRecord,
    alter: &ast::AlterTable,
) -> Result<ExecOutput, MiniError> {
    require_priv(user, session.current_db.as_deref(), Priv::CREATE)?;
    txn_commit(store, session)?;

    if alter.only
        || alter.location.is_some()
        || alter.on_cluster.is_some()
        || alter.table_type.is_some()
    {
        return Err(MiniError::NotSupported(
            "ALTER TABLE modifiers are not supported".into(),
        ));
    }

    let (db_opt, table_name) = object_name_to_parts(&alter.name)?;
    let db = db_opt
        .or_else(|| session.current_db.clone())
        .ok_or_else(|| MiniError::Invalid("no database selected".into()))?;
    if is_system_schema(&db) {
        return Err(MiniError::NotSupported(format!(
            "ALTER TABLE is not supported for system schema {db}"
        )));
    }

    let mut def = match store.get_table(&db, &table_name) {
        Ok(def) => def,
        Err(MiniError::NotFound(_)) if alter.if_exists => {
            return Ok(ExecOutput::Ok {
                affected_rows: 0,
                last_insert_id: 0,
                info: "".into(),
            })
        }
        Err(e) => return Err(e),
    };

    let mut new_columns: Vec<ColumnDef> = Vec::new();
    let mut fill_values: Vec<Cell> = Vec::new();

    for op in &alter.operations {
        match op {
            ast::AlterTableOperation::AddColumn {
                if_not_exists,
                column_def,
                column_position,
                ..
            } => {
                if column_position.is_some() {
                    return Err(MiniError::NotSupported(
                        "ALTER TABLE ... ADD COLUMN with FIRST/AFTER is not supported".into(),
                    ));
                }

                let col_name = column_def.name.value.clone();
                if def
                    .columns
                    .iter()
                    .any(|c| c.name.eq_ignore_ascii_case(&col_name))
                    || new_columns
                        .iter()
                        .any(|c| c.name.eq_ignore_ascii_case(&col_name))
                {
                    if *if_not_exists {
                        continue;
                    }
                    return Err(MiniError::Invalid(format!(
                        "duplicate column: {db}.{table_name}.{col_name}"
                    )));
                }

                let sql_ty = match &column_def.data_type {
                    ast::DataType::Int(_)
                    | ast::DataType::BigInt(_)
                    | ast::DataType::Integer(_)
                    | ast::DataType::TinyInt(_)
                    | ast::DataType::SmallInt(_) => SqlType::Int,
                    _ => SqlType::Text,
                };

                let mut nullable = true;
                let mut default_expr: Option<&ast::Expr> = None;
                for opt in &column_def.options {
                    match &opt.option {
                        ast::ColumnOption::NotNull => nullable = false,
                        ast::ColumnOption::Null => nullable = true,
                        ast::ColumnOption::Default(expr) => default_expr = Some(expr),
                        ast::ColumnOption::Comment(_)
                        | ast::ColumnOption::CharacterSet(_)
                        | ast::ColumnOption::Collation(_)
                        | ast::ColumnOption::DialectSpecific(_)
                        | ast::ColumnOption::Generated { .. } => {}
                        _ => {
                            return Err(MiniError::NotSupported(
                                "ALTER TABLE ADD COLUMN supports only NULL/NOT NULL/DEFAULT".into(),
                            ))
                        }
                    }
                }

                let fill = match default_expr {
                    Some(expr) => eval_expr(expr)?,
                    None => Cell::Null,
                };
                if !nullable && matches!(fill, Cell::Null) {
                    return Err(MiniError::NotSupported(format!(
                        "ADD COLUMN {col_name} NOT NULL requires DEFAULT"
                    )));
                }

                new_columns.push(ColumnDef {
                    name: col_name,
                    ty: sql_ty,
                    nullable,
                });
                fill_values.push(fill);
            }
            _ => {
                return Err(MiniError::NotSupported(
                    "Only ALTER TABLE ... ADD COLUMN is supported".into(),
                ))
            }
        }
    }

    if new_columns.is_empty() {
        return Ok(ExecOutput::Ok {
            affected_rows: 0,
            last_insert_id: 0,
            info: "".into(),
        });
    }

    let mut updated: Vec<(i64, Row)> = Vec::new();
    for (pk, mut row) in store.scan_rows(&db, &table_name)? {
        row.values.extend(fill_values.iter().cloned());
        updated.push((pk, row));
    }
    let changes = updated
        .iter()
        .map(|(pk, row)| (db.as_str(), table_name.as_str(), *pk, Some(row)));
    store.apply_row_changes(changes)?;

    def.columns.extend(new_columns);
    store.update_table(&def)?;

    Ok(ExecOutput::Ok {
        affected_rows: 0,
        last_insert_id: 0,
        info: "".into(),
    })
}

fn handle_drop_table(
    store: &Store,
    session: &mut SessionState,
    user: &UserRecord,
    name: &ObjectName,
    if_exists: bool,
) -> Result<ExecOutput, MiniError> {
    require_priv(user, session.current_db.as_deref(), Priv::DROP)?;
    txn_commit(store, session)?;

    let (db_opt, table_name) = match name.0.len() {
        1 => (None, get_ident_name(&name.0[0])),
        2 => (Some(get_ident_name(&name.0[0])), get_ident_name(&name.0[1])),
        _ => return Err(MiniError::Parse("Invalid table name".into())),
    };

    let db = db_opt
        .or_else(|| session.current_db.clone())
        .ok_or_else(|| MiniError::Invalid("no database selected".into()))?;

    match store.drop_table(&db, &table_name) {
        Ok(_) => {}
        Err(MiniError::NotFound(_)) if if_exists => {}
        Err(e) => return Err(e),
    }

    Ok(ExecOutput::Ok {
        affected_rows: 1,
        last_insert_id: 0,
        info: "".into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_secondary_index_flow() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store.ensure_root_user("").unwrap();

        let mut session = SessionState::new(1);
        session.current_db = Some("test".into());
        let user = UserRecord {
            username: "root".into(),
            host: "%".into(),
            plugin: "".into(),
            auth_stage2: None,
            global_privs: Priv::ALL.bits(),
            db_privs: Default::default(),
        };

        // 1. Create DB and Table
        let setup_sqls = vec![
            "CREATE DATABASE test",
            "CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id))",
            "INSERT INTO users VALUES (1, 'Alice', 30)",
            "INSERT INTO users VALUES (2, 'Bob', 25)",
        ];
        for sql in setup_sqls {
            match execute(sql, &store, &mut session, &user) {
                Ok(_) => {}
                Err(e) => panic!("Failed to run {}: {:?}", sql, e),
            }
        }

        // 2. Create Index
        // Should succeed and backfill
        match execute(
            "CREATE INDEX idx_age ON users (age)",
            &store,
            &mut session,
            &user,
        ) {
            Ok(_) => {}
            Err(e) => panic!("Failed to create index: {:?}", e),
        }

        // 3. Show Index
        let res = execute("SHOW INDEX FROM users", &store, &mut session, &user).unwrap();
        match res {
            ExecOutput::ResultSet { rows, .. } => {
                // Expected: PRIMARY (seq 1), idx_age (seq 1)
                assert_eq!(
                    rows.len(),
                    2,
                    "Should have 2 index rows (PRIMARY + idx_age)"
                );

                // Row 1: PRIMARY
                let row0 = &rows[0];
                assert_eq!(row0[2], Cell::Text("PRIMARY".into()));

                // Row 2: idx_age
                let row1 = &rows[1];
                // Table, Non_unique, Key_name...
                // Key_name is index 2
                assert_eq!(row1[2], Cell::Text("idx_age".into()));
                assert_eq!(row1[4], Cell::Text("age".into())); // Column_name
            }
            _ => panic!("Expected ResultSet"),
        }

        // 4. Insert more data (updates index)
        match execute(
            "INSERT INTO users VALUES (3, 'Charlie', 35)",
            &store,
            &mut session,
            &user,
        ) {
            Ok(_) => {}
            Err(e) => panic!("Failed to insert after index: {:?}", e),
        }
    }
}
