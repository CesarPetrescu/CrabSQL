use crate::auth::{stage2_from_password, Priv};
use crate::error::MiniError;
use crate::model::{Row, TableDef, UserRecord};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sled::{Batch, IVec};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

#[derive(Clone)]
pub struct Store {
    db: sled::Db,
    catalog: sled::Tree,
    data: sled::Tree,
    locks: Arc<LockManager>,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, MiniError> {
        let db = sled::open(path)?;
        let catalog = db.open_tree("catalog")?;
        let data = db.open_tree("data")?;
        Ok(Self {
            db,
            catalog,
            data,
            locks: Arc::new(LockManager::default()),
        })
    }

    pub fn ensure_root_user(&self, password: &str) -> Result<(), MiniError> {
        let key = Self::user_key("root", "%");
        if self.catalog.get(&key)?.is_some() {
            return Ok(());
        }
        let record = UserRecord {
            username: "root".to_string(),
            host: "%".to_string(),
            plugin: "mysql_native_password".to_string(),
            auth_stage2: Some(stage2_from_password(password.as_bytes())),
            global_privs: Priv::ALL.bits(),
            db_privs: Default::default(),
        };
        self.put_user(&record)
    }

    pub fn get_user(&self, username: &str) -> Result<Option<UserRecord>, MiniError> {
        // Prefer exact host matches if they exist (MVP primarily uses `...@%`).
        if let Some(v) = self.catalog.get(Self::user_key(username, "localhost"))? {
            return Ok(Some(bincode::deserialize(&v)?));
        }
        if let Some(v) = self.catalog.get(Self::user_key(username, "%"))? {
            return Ok(Some(bincode::deserialize(&v)?));
        }

        // Fallback: return the first matching `username@host`.
        let prefix = Self::user_prefix(username);
        if let Some(item) = self.catalog.scan_prefix(prefix).next() {
            let (_k, v) = item?;
            return Ok(Some(bincode::deserialize(&v)?));
        }
        Ok(None)
    }

    pub fn put_user(&self, user: &UserRecord) -> Result<(), MiniError> {
        let key = Self::user_key(&user.username, &user.host);
        let val = bincode::serialize(user)?;
        self.catalog.insert(key, val)?;
        self.catalog.flush()?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn drop_user(&self, username: &str, host: &str) -> Result<(), MiniError> {
        let key = Self::user_key(username, host);
        self.catalog.remove(key)?;
        self.catalog.flush()?;
        Ok(())
    }

    pub fn list_databases(&self) -> Result<Vec<String>, MiniError> {
        let mut out = Vec::new();
        for item in self.catalog.scan_prefix(b"d\0") {
            let (k, _v) = item?;
            let name = String::from_utf8_lossy(&k[2..]).to_string();
            out.push(name);
        }
        out.sort();
        Ok(out)
    }

    pub fn create_database(&self, name: &str) -> Result<(), MiniError> {
        let k = Self::db_key(name);
        if self.catalog.get(&k)?.is_some() {
            return Err(MiniError::Invalid(format!(
                "database already exists: {name}"
            )));
        }
        self.catalog.insert(k, IVec::from(&b""[..]))?;
        self.catalog.flush()?;
        Ok(())
    }

    pub fn drop_database(&self, name: &str) -> Result<(), MiniError> {
        let k = Self::db_key(name);
        if self.catalog.get(&k)?.is_none() {
            return Err(MiniError::NotFound(format!("unknown database: {name}")));
        }
        // Drop tables + rows.
        let prefix = Self::table_prefix(name);
        let tables: Vec<Vec<u8>> = self
            .catalog
            .scan_prefix(prefix)
            .map(|r| r.map(|(k, _)| k.to_vec()))
            .collect::<Result<_, _>>()?;
        for tkey in tables {
            self.catalog.remove(tkey)?;
        }

        let ai_prefix = Self::auto_inc_prefix(name);
        let ai_keys: Vec<Vec<u8>> = self
            .catalog
            .scan_prefix(ai_prefix)
            .map(|r| r.map(|(k, _)| k.to_vec()))
            .collect::<Result<_, _>>()?;
        for akey in ai_keys {
            self.catalog.remove(akey)?;
        }

        let row_prefix = Self::row_prefix(name, "");
        let row_keys: Vec<Vec<u8>> = self
            .data
            .scan_prefix(row_prefix)
            .map(|r| r.map(|(k, _)| k.to_vec()))
            .collect::<Result<_, _>>()?;
        for rkey in row_keys {
            self.data.remove(rkey)?;
        }

        self.catalog.remove(k)?;
        self.catalog.flush()?;
        self.data.flush()?;
        Ok(())
    }

    pub fn list_tables(&self, db: &str) -> Result<Vec<String>, MiniError> {
        let mut out = Vec::new();
        let prefix = Self::table_prefix(db);
        for item in self.catalog.scan_prefix(prefix) {
            let (k, _v) = item?;
            // key: t\0<db>\0<table>
            let rest = &k[2..];
            let mut parts = rest.split(|b| *b == 0);
            let _db = parts.next();
            let table = parts.next().unwrap_or(&[]);
            out.push(String::from_utf8_lossy(table).to_string());
        }
        out.sort();
        Ok(out)
    }

    pub fn get_table(&self, db: &str, table: &str) -> Result<TableDef, MiniError> {
        let key = Self::table_key(db, table);
        let Some(v) = self.catalog.get(key)? else {
            return Err(MiniError::NotFound(format!("unknown table: {db}.{table}")));
        };
        Ok(bincode::deserialize(&v)?)
    }

    pub fn create_table(&self, def: &TableDef) -> Result<(), MiniError> {
        // Ensure db exists
        let dbk = Self::db_key(&def.db);
        if self.catalog.get(&dbk)?.is_none() {
            return Err(MiniError::NotFound(format!("unknown database: {}", def.db)));
        }
        let key = Self::table_key(&def.db, &def.name);
        if self.catalog.get(&key)?.is_some() {
            return Err(MiniError::Invalid(format!(
                "table already exists: {}.{}",
                def.db, def.name
            )));
        }
        self.catalog.insert(key, bincode::serialize(def)?)?;
        self.catalog.flush()?;
        Ok(())
    }

    pub fn update_table(&self, def: &TableDef) -> Result<(), MiniError> {
        let key = Self::table_key(&def.db, &def.name);
        if self.catalog.get(&key)?.is_none() {
            return Err(MiniError::NotFound(format!(
                "unknown table: {}.{}",
                def.db, def.name
            )));
        }
        self.catalog.insert(key, bincode::serialize(def)?)?;
        self.catalog.flush()?;
        Ok(())
    }

    pub fn drop_table(&self, db: &str, table: &str) -> Result<(), MiniError> {
        let key = Self::table_key(db, table);
        if self.catalog.get(&key)?.is_none() {
            return Err(MiniError::NotFound(format!("unknown table: {db}.{table}")));
        }
        self.catalog.remove(key)?;
        self.catalog.remove(Self::auto_inc_key(db, table))?;

        let prefix = Self::row_prefix(db, table);
        let row_keys: Vec<Vec<u8>> = self
            .data
            .scan_prefix(prefix)
            .map(|r| r.map(|(k, _)| k.to_vec()))
            .collect::<Result<_, _>>()?;
        for rkey in row_keys {
            self.data.remove(rkey)?;
        }

        self.catalog.flush()?;
        self.data.flush()?;
        Ok(())
    }

    pub fn get_row(&self, db: &str, table: &str, pk: i64) -> Result<Option<Row>, MiniError> {
        let key = Self::row_key(db, table, pk);
        Ok(self
            .data
            .get(key)?
            .map(|v| bincode::deserialize(&v))
            .transpose()?)
    }

    pub fn apply_row_changes<'a, I>(&self, changes: I) -> Result<(), MiniError>
    where
        I: IntoIterator<Item = (&'a str, &'a str, i64, Option<&'a Row>)>,
    {
        let mut batch = Batch::default();
        for (db, table, pk, row) in changes {
            let key = Self::row_key(db, table, pk);
            match row {
                Some(row) => batch.insert(key, bincode::serialize(row)?),
                None => batch.remove(key),
            }
        }
        self.data.apply_batch(batch)?;
        self.flush()?;
        Ok(())
    }

    pub fn allocate_auto_increment(&self, db: &str, table: &str) -> Result<i64, MiniError> {
        let key = Self::auto_inc_key(db, table);
        let next = self
            .catalog
            .update_and_fetch(key, |old| {
                let cur = old
                    .and_then(|bytes| {
                        if bytes.len() != 8 {
                            return None;
                        }
                        let raw: [u8; 8] = bytes.try_into().ok()?;
                        Some(i64::from_be_bytes(raw))
                    })
                    .unwrap_or(1);
                cur.checked_add(1)
                    .map(|n| n.to_be_bytes().to_vec())
                    .or_else(|| Some(i64::MAX.to_be_bytes().to_vec()))
            })?
            .ok_or_else(|| MiniError::Invalid("auto_increment update failed".into()))?;

        if next.len() != 8 {
            return Err(MiniError::Invalid("corrupt auto_increment value".into()));
        }
        let raw: [u8; 8] = next
            .as_ref()
            .try_into()
            .map_err(|_| MiniError::Invalid("corrupt auto_increment value".into()))?;
        let stored_next = i64::from_be_bytes(raw);
        let allocated = stored_next.saturating_sub(1);
        if allocated <= 0 {
            return Err(MiniError::Invalid("auto_increment exhausted".into()));
        }
        Ok(allocated)
    }

    pub fn bump_auto_increment_next(
        &self,
        db: &str,
        table: &str,
        next: i64,
    ) -> Result<(), MiniError> {
        if next <= 0 {
            return Ok(());
        }
        let key = Self::auto_inc_key(db, table);
        self.catalog.update_and_fetch(key, |old| {
            let cur = old
                .and_then(|bytes| {
                    if bytes.len() != 8 {
                        return None;
                    }
                    let raw: [u8; 8] = bytes.try_into().ok()?;
                    Some(i64::from_be_bytes(raw))
                })
                .unwrap_or(1);
            Some(cur.max(next).to_be_bytes().to_vec())
        })?;
        Ok(())
    }

    pub fn auto_increment_next(&self, db: &str, table: &str) -> Result<Option<i64>, MiniError> {
        let key = Self::auto_inc_key(db, table);
        let Some(v) = self.catalog.get(key)? else {
            return Ok(None);
        };
        if v.len() != 8 {
            return Ok(None);
        }
        let raw: [u8; 8] = v
            .as_ref()
            .try_into()
            .map_err(|_| MiniError::Invalid("corrupt auto_increment value".into()))?;
        Ok(Some(i64::from_be_bytes(raw)))
    }

    pub fn lock_row(&self, owner: u32, db: &str, table: &str, pk: i64) -> Result<bool, MiniError> {
        let key = Self::row_key(db, table, pk);
        self.locks.lock(owner, key)
    }

    pub fn unlock_row(&self, owner: u32, db: &str, table: &str, pk: i64) {
        let key = Self::row_key(db, table, pk);
        self.locks.unlock(owner, &key);
    }

    pub fn unlock_all(&self, owner: u32) {
        self.locks.unlock_all(owner);
    }

    pub fn scan_rows(&self, db: &str, table: &str) -> Result<Vec<(i64, Row)>, MiniError> {
        let prefix = Self::row_prefix(db, table);
        let mut out = Vec::new();
        for item in self.data.scan_prefix(prefix) {
            let (k, v) = item?;
            let pk = Self::parse_pk_from_row_key(&k)?;
            let row: Row = bincode::deserialize(&v)?;
            out.push((pk, row));
        }
        out.sort_by_key(|(pk, _)| *pk);
        Ok(out)
    }

    pub fn count_rows(&self, db: &str, table: &str) -> Result<u64, MiniError> {
        let prefix = Self::row_prefix(db, table);
        let mut count = 0u64;
        for item in self.data.scan_prefix(prefix) {
            item?;
            count = count.saturating_add(1);
        }
        Ok(count)
    }

    pub fn flush(&self) -> Result<(), MiniError> {
        self.data.flush()?;
        self.catalog.flush()?;
        self.db.flush()?;
        Ok(())
    }

    fn db_key(name: &str) -> Vec<u8> {
        let mut k = Vec::with_capacity(2 + name.len());
        k.extend_from_slice(b"d\0");
        k.extend_from_slice(name.as_bytes());
        k
    }

    fn table_prefix(db: &str) -> Vec<u8> {
        let mut k = Vec::new();
        k.extend_from_slice(b"t\0");
        k.extend_from_slice(db.as_bytes());
        k.push(0);
        k
    }

    fn table_key(db: &str, table: &str) -> Vec<u8> {
        let mut k = Vec::new();
        k.extend_from_slice(b"t\0");
        k.extend_from_slice(db.as_bytes());
        k.push(0);
        k.extend_from_slice(table.as_bytes());
        k
    }

    fn user_key(username: &str, host: &str) -> Vec<u8> {
        let mut k = Vec::new();
        k.extend_from_slice(b"u\0");
        k.extend_from_slice(username.as_bytes());
        k.push(0);
        k.extend_from_slice(host.as_bytes());
        k
    }

    fn user_prefix(username: &str) -> Vec<u8> {
        let mut k = Vec::new();
        k.extend_from_slice(b"u\0");
        k.extend_from_slice(username.as_bytes());
        k.push(0);
        k
    }

    fn row_prefix(db: &str, table: &str) -> Vec<u8> {
        let mut k = Vec::new();
        k.extend_from_slice(b"r\0");
        k.extend_from_slice(db.as_bytes());
        // Always end the db segment with a NUL byte.
        k.push(0);
        // If `table` is empty, the prefix will match all tables in the database.
        // Otherwise, add the table segment and a trailing NUL byte.
        if !table.is_empty() {
            k.extend_from_slice(table.as_bytes());
            k.push(0);
        }
        k
    }

    fn row_key(db: &str, table: &str, pk: i64) -> Vec<u8> {
        let mut k = Self::row_prefix(db, table);
        k.extend_from_slice(&pk.to_be_bytes());
        k
    }

    fn auto_inc_key(db: &str, table: &str) -> Vec<u8> {
        let mut k = Vec::new();
        k.extend_from_slice(b"ai\0");
        k.extend_from_slice(db.as_bytes());
        k.push(0);
        k.extend_from_slice(table.as_bytes());
        k
    }

    fn auto_inc_prefix(db: &str) -> Vec<u8> {
        let mut k = Vec::new();
        k.extend_from_slice(b"ai\0");
        k.extend_from_slice(db.as_bytes());
        k.push(0);
        k
    }

    fn parse_pk_from_row_key(key: &[u8]) -> Result<i64, MiniError> {
        if key.len() < 8 {
            return Err(MiniError::Invalid("corrupt row key".into()));
        }
        let pk_bytes: [u8; 8] = key[key.len() - 8..]
            .try_into()
            .map_err(|_| MiniError::Invalid("corrupt row key".into()))?;
        Ok(i64::from_be_bytes(pk_bytes))
    }
}

// Helpers for GRANT/REVOKE

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrantTarget {
    pub username: String,
    pub host: String,
}

#[derive(Default)]
struct LockManager {
    inner: Mutex<LockState>,
}

#[derive(Default)]
struct LockState {
    by_key: HashMap<Vec<u8>, u32>,
    by_owner: HashMap<u32, HashSet<Vec<u8>>>,
}

impl LockManager {
    fn lock(&self, owner: u32, key: Vec<u8>) -> Result<bool, MiniError> {
        let mut st = self.inner.lock();
        match st.by_key.get(&key).copied() {
            None => {
                st.by_key.insert(key.clone(), owner);
                st.by_owner.entry(owner).or_default().insert(key);
                Ok(true)
            }
            Some(current) if current == owner => Ok(false),
            Some(_) => Err(MiniError::LockWaitTimeout(
                "row is locked by another session".into(),
            )),
        }
    }

    fn unlock(&self, owner: u32, key: &[u8]) {
        let mut st = self.inner.lock();
        if st.by_key.get(key).copied() != Some(owner) {
            return;
        }
        st.by_key.remove(key);
        if let Some(keys) = st.by_owner.get_mut(&owner) {
            keys.remove(key);
            if keys.is_empty() {
                st.by_owner.remove(&owner);
            }
        }
    }

    fn unlock_all(&self, owner: u32) {
        let mut st = self.inner.lock();
        let Some(keys) = st.by_owner.remove(&owner) else {
            return;
        };
        for key in keys {
            if st.by_key.get(&key).copied() == Some(owner) {
                st.by_key.remove(&key);
            }
        }
    }
}
