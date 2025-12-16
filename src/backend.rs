use crate::auth::verify_mysql_native_password;
use crate::error::MiniError;
use crate::sql::{execute, SessionState, SERVER_VERSION};
use crate::store::Store;
use async_trait::async_trait;
use opensrv_mysql::{
    AsyncMysqlShim, Column, ColumnFlags, ColumnType, ErrorKind, InitWriter, OkResponse,
    ParamParser, QueryResultWriter, StatementMetaWriter, StatusFlags, ValueInner,
};
use parking_lot::Mutex;
use rand::rngs::OsRng;
use rand::RngCore;
use std::collections::HashMap;
use std::iter;

pub struct Backend {
    store: Store,
    session: Mutex<SessionState>,
    salt: [u8; 20],
    conn_id: u32,
    next_stmt_id: u32,
    stmts: HashMap<u32, String>,
}

impl Backend {
    pub fn new(store: Store, conn_id: u32) -> Self {
        let mut salt = [0u8; 20];
        OsRng.fill_bytes(&mut salt);
        Self {
            store,
            session: Mutex::new(SessionState::new(conn_id)),
            salt,
            conn_id,
            next_stmt_id: 1,
            stmts: HashMap::new(),
        }
    }

    fn status_flags(autocommit: bool, in_trans: bool) -> StatusFlags {
        let mut flags = StatusFlags::empty();
        if in_trans {
            flags.insert(StatusFlags::SERVER_STATUS_IN_TRANS);
        }
        if autocommit {
            flags.insert(StatusFlags::SERVER_STATUS_AUTOCOMMIT);
        }
        flags
    }

    fn err_to_kind(err: &MiniError) -> ErrorKind {
        match err {
            MiniError::Parse(_) => ErrorKind::ER_PARSE_ERROR,
            MiniError::NotFound(msg) => {
                // best-effort: if message mentions database, use ER_BAD_DB_ERROR
                if msg.to_ascii_lowercase().contains("database") {
                    ErrorKind::ER_BAD_DB_ERROR
                } else {
                    ErrorKind::ER_BAD_TABLE_ERROR
                }
            }
            MiniError::AccessDenied(_) => ErrorKind::ER_ACCESS_DENIED_ERROR,
            MiniError::NotSupported(_) => ErrorKind::ER_NOT_SUPPORTED_YET,
            MiniError::Invalid(_) => ErrorKind::ER_WRONG_VALUE_COUNT_ON_ROW,
            MiniError::LockWaitTimeout(_) => ErrorKind::ER_LOCK_WAIT_TIMEOUT,
            MiniError::UnknownSystemVariable(_) => ErrorKind::ER_UNKNOWN_SYSTEM_VARIABLE,
            _ => ErrorKind::ER_UNKNOWN_ERROR,
        }
    }

    fn err_msg(err: &MiniError) -> String {
        err.to_string()
    }

    fn is_system_schema(name: &str) -> bool {
        matches!(
            name.trim().to_ascii_lowercase().as_str(),
            "information_schema" | "mysql" | "performance_schema" | "sys"
        )
    }
}

#[async_trait]
impl<W> AsyncMysqlShim<W> for Backend
where
    W: tokio::io::AsyncWrite + Unpin + Send,
{
    type Error = MiniError;

    fn version(&self) -> String {
        SERVER_VERSION.to_string()
    }

    fn connect_id(&self) -> u32 {
        self.conn_id
    }

    fn salt(&self) -> [u8; 20] {
        self.salt
    }

    async fn authenticate(
        &self,
        auth_plugin: &str,
        username: &[u8],
        salt: &[u8],
        auth_data: &[u8],
    ) -> bool {
        if auth_plugin != "mysql_native_password" {
            return false;
        }
        let username = match std::str::from_utf8(username) {
            Ok(u) => u,
            Err(_) => return false,
        };

        let Some(user) = self.store.get_user(username).ok().flatten() else {
            return false;
        };

        let ok = verify_mysql_native_password(salt, auth_data, user.auth_stage2);
        if ok {
            self.session.lock().username = user.username;
        }
        ok
    }

    async fn on_prepare<'a>(
        &'a mut self,
        query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> Result<(), MiniError> {
        let id = self.next_stmt_id;
        self.next_stmt_id = self.next_stmt_id.wrapping_add(1);

        let parts = split_query_template(query);
        let param_count = parts.len().saturating_sub(1);
        self.stmts.insert(id, query.to_string());

        let params: Vec<Column> = (0..param_count)
            .map(|_| Column {
                table: String::new(),
                column: String::new(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            })
            .collect();

        info.reply(id, params.iter(), iter::empty::<&Column>())
            .await?;
        Ok(())
    }

    async fn on_execute<'a>(
        &'a mut self,
        id: u32,
        params: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), MiniError> {
        let query_tpl = self
            .stmts
            .get(&id)
            .ok_or_else(|| MiniError::NotFound(format!("stmt id {id}")))?;

        let parts = split_query_template(query_tpl);
        let mut final_query = String::new();

        let mut param_iter = params.into_iter();

        for (i, part) in parts.iter().enumerate() {
            final_query.push_str(part);
            if i < parts.len() - 1 {
                let p = param_iter
                    .next()
                    .ok_or_else(|| MiniError::Parse("missing parameters".into()))?;
                let opensrv_mysql::ParamValue { value, .. } = p;
                final_query.push_str(&mysql_value_to_sql(value)?);
            }
        }

        if param_iter.next().is_some() {
            return Err(MiniError::Parse("too many parameters".into()));
        }

        self.on_query(&final_query, results).await
    }

    async fn on_close<'a>(&'a mut self, stmt: u32)
    where
        W: 'async_trait,
    {
        self.stmts.remove(&stmt);
    }

    async fn on_init<'a>(
        &'a mut self,
        db: &'a str,
        writer: InitWriter<'a, W>,
    ) -> Result<(), MiniError> {
        // Equivalent to USE <db>
        let dbs = self.store.list_databases()?;
        if !dbs.iter().any(|d| d.eq_ignore_ascii_case(db)) && !Self::is_system_schema(db) {
            writer
                .error(ErrorKind::ER_BAD_DB_ERROR, b"unknown database")
                .await?;
            return Ok(());
        }
        self.session.lock().current_db = Some(db.to_string());
        writer.ok().await?;
        Ok(())
    }

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), MiniError> {
        // Load user each time so that GRANT/REVOKE becomes effective immediately.
        let username = self.session.lock().username.clone();
        let user = self
            .store
            .get_user(&username)?
            .ok_or_else(|| MiniError::AccessDenied("unknown user".into()))?;

        let (out, autocommit, in_trans) = {
            let mut sess = self.session.lock();
            let out = execute(query, &self.store, &mut sess, &user);
            let autocommit = sess.autocommit;
            let in_trans = sess.in_transaction();
            (out, autocommit, in_trans)
        };

        match out {
            Ok(crate::sql::ExecOutput::Ok {
                affected_rows,
                last_insert_id,
                info,
            }) => {
                let ok = OkResponse {
                    affected_rows,
                    last_insert_id,
                    status_flags: Self::status_flags(autocommit, in_trans),
                    info,
                    ..Default::default()
                };
                results.completed(ok).await?;
            }
            Ok(crate::sql::ExecOutput::ResultSet { columns, rows }) => {
                let mut rw = results.start(&columns).await?;
                for row in rows {
                    for (i, cell) in row.into_iter().enumerate() {
                        let coltype = columns
                            .get(i)
                            .map(|c| c.coltype)
                            .unwrap_or(ColumnType::MYSQL_TYPE_VAR_STRING);
                        match (coltype, cell) {
                            (_, crate::model::Cell::Null) => {
                                // Any Option<T>::None encodes NULL.
                                rw.write_col(None::<u8>)?;
                            }
                            (ColumnType::MYSQL_TYPE_LONGLONG, crate::model::Cell::Int(n)) => {
                                rw.write_col(n)?;
                            }
                            (_, crate::model::Cell::Float(f)) => {
                                rw.write_col(f)?;
                            }
                            (_, crate::model::Cell::Date(d)) => {
                                // Convert days to string YYYY-MM-DD
                                let dt = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
                                    + chrono::Duration::days(d);
                                rw.write_col(dt.format("%Y-%m-%d").to_string())?;
                            }
                            (_, crate::model::Cell::DateTime(ms)) => {
                                // Convert millis to string
                                let dt =
                                    chrono::DateTime::from_timestamp_millis(ms).unwrap_or_default();
                                rw.write_col(dt.format("%Y-%m-%d %H:%M:%S").to_string())?;
                            }
                            (_, crate::model::Cell::Int(n)) => {
                                rw.write_col(n.to_string())?;
                            }
                            (_, crate::model::Cell::Text(s)) => {
                                rw.write_col(s)?;
                            }
                        }
                    }
                    rw.end_row().await?;
                }
                rw.finish().await?;
            }
            Err(err) => {
                let kind = Self::err_to_kind(&err);
                let msg = Self::err_msg(&err);
                results.error(kind, msg.as_bytes()).await?;
            }
        }

        Ok(())
    }
}

impl Drop for Backend {
    fn drop(&mut self) {
        self.store.unlock_all(self.conn_id);
    }
}

fn split_query_template(query: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut last = 0;
    let mut in_sq = false;
    let mut in_bq = false;
    let mut chars = query.char_indices().peekable();

    while let Some((i, ch)) = chars.next() {
        match ch {
            '\'' if !in_bq => {
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
                in_bq = !in_bq;
            }
            '?' if !in_sq && !in_bq => {
                out.push(&query[last..i]);
                last = i + ch.len_utf8();
            }
            _ => {}
        }
    }
    out.push(&query[last..]);
    out
}

fn mysql_value_to_sql(value: opensrv_mysql::Value<'_>) -> Result<String, MiniError> {
    match value.into_inner() {
        ValueInner::NULL => Ok("NULL".to_string()),
        ValueInner::Int(n) => Ok(n.to_string()),
        ValueInner::UInt(n) => {
            if n > i64::MAX as u64 {
                return Err(MiniError::Invalid(
                    "unsigned integer parameter is too large".into(),
                ));
            }
            Ok(n.to_string())
        }
        ValueInner::Bytes(bytes) => {
            let s = std::str::from_utf8(bytes).map_err(|_| {
                MiniError::Invalid("non-utf8 string parameter in prepared statement".into())
            })?;
            Ok(format!("'{}'", escape_sql_string(s)))
        }
        ValueInner::Double(f) => Ok(f.to_string()),
        ValueInner::Date(_) | ValueInner::Time(_) | ValueInner::Datetime(_) => Err(
            MiniError::NotSupported("date/time parameters are not supported".into()),
        ),
    }
}

fn escape_sql_string(input: &str) -> String {
    input.replace('\'', "''")
}
