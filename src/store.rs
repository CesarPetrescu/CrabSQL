use crate::auth::{stage2_from_password, Priv};
use crate::error::MiniError;
use crate::model::{Cell, ColumnDef, IndexDef, Row, TableDef, TransactionId, UserRecord};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use sled::{Batch, IVec};
use std::collections::{HashMap, HashSet, BTreeSet};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct Store {
    db: sled::Db,
    catalog: sled::Tree,
    data: sled::Tree,
    locks: Arc<LockManager>,
    pub txn_manager: Arc<TransactionManager>,
}

pub struct TransactionManager {
    // Global counter for Transction IDs.
    // We use a simple counter. Real systems use timestamps or hybrid clocks.
    next_tx_id: AtomicU64,
    // Set of currently active (uncommitted) transaction IDs.
    // Used to compute snapshots: "what was active when I started?"
    active_txns: RwLock<BTreeSet<TransactionId>>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            next_tx_id: AtomicU64::new(1),
            active_txns: RwLock::new(BTreeSet::new()),
        }
    }
    
    pub fn set_next_tx_id(&self, id: u64) {
        self.next_tx_id.store(id, Ordering::SeqCst);
    }

    /// Start a new transaction. Returns the new TxID and a "Read View".
    /// The Read View is the list of transactions that were active when this one started.
    /// In Snapshot Isolation, we should NOT see writes from these transactions (unless it's us).
    pub fn start_txn(&self) -> (TransactionId, ReadView) {
        let tx_id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
        let mut active = self.active_txns.write();
        
        // Construct the Read View: copy current active set
        let snapshot = active.clone();
        
        active.insert(tx_id);
        
        (tx_id, ReadView {
            visible_up_to: tx_id, 
            active: snapshot,
            own_tx_id: Some(tx_id),
        })
    }

    pub fn commit_txn(&self, tx_id: TransactionId) {
        let mut active = self.active_txns.write();
        active.remove(&tx_id);
    }

    pub fn rollback_txn(&self, tx_id: TransactionId) {
        let mut active = self.active_txns.write();
        active.remove(&tx_id);
    }
}

/// Defines which transaction IDs are visible to a reader.
#[derive(Debug, Clone)]
pub struct ReadView {
    // Any TxID < visible_up_to is potentially visible (unless it was active).
    // Any TxID >= visible_up_to is NOT visible (started after us).
    pub visible_up_to: TransactionId,
    // Set of TxIDs that were active when the view was created.
    // These should be INVISIBLE even if they are < visible_up_to.
    pub active: BTreeSet<TransactionId>,
    // The ID of the transaction using this view. It can always see its own writes.
    pub own_tx_id: Option<TransactionId>,
}

impl ReadView {
    pub fn is_visible(&self, tx_id: TransactionId) -> bool {
        if let Some(own) = self.own_tx_id {
            if tx_id == own {
                return true;
            }
        }
        if tx_id >= self.visible_up_to {
            return false;
        }
        if self.active.contains(&tx_id) {
            return false;
        }
        true
    }
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, MiniError> {
        let db = sled::open(path)?;
        let catalog = db.open_tree("catalog")?;
        let data = db.open_tree("data")?;
        
        let mut next_id = 1;
        if let Some(val) = data.get(b"m\0max_tx_id")? {
            let bytes: [u8; 8] = val.as_ref().try_into().unwrap_or([0; 8]);
            let last_id = u64::from_be_bytes(bytes);
            next_id = last_id + 1;
        }
        
        let txn_manager = Arc::new(TransactionManager::new());
        txn_manager.set_next_tx_id(next_id);
        
        Ok(Self {
            db,
            catalog,
            data,
            locks: Arc::new(LockManager::default()),
            txn_manager,
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

    pub fn create_index(&self, db: &str, table: &str, index: IndexDef) -> Result<(), MiniError> {
        let key = Self::table_key(db, table);

        // 1. Update Catalog
        let def_bytes = self.catalog.get(&key)?.ok_or(MiniError::NotFound(format!("Table {}.{} not found", db, table)))?;
        let mut def: TableDef = bincode::deserialize(&def_bytes)?;
        
        // Check if index exists
        if def.indexes.iter().any(|i| i.name == index.name) {
             return Err(MiniError::Invalid(format!("Index {} already exists", index.name)));
        }
        
        // Validate columns
        for col_name in &index.columns {
            if !def.columns.iter().any(|c| &c.name == col_name) {
                return Err(MiniError::Invalid(format!("Column {} not found", col_name)));
            }
        }
        
        def.indexes.push(index.clone());
        let new_def_bytes = bincode::serialize(&def)?;
        self.catalog.insert(&key, new_def_bytes)?;
        
        // 2. Backfill
        // Scan all rows (latest version) and insert index entries.
        // We use a simplified scan that ignores visibility? No, we need LATEST committed data.
        // Or we use a snapshot? 
        // Backfill usually runs in a transaction or blocks?
        // For MVP, simplistic scan.
        // Warning: This is not atomic with respect to concurrent writes if we don't lock.
        // But we are in `create_index`, maybe we should lock table?
        // `self.locks` is row-level.
        // Let's assume generic lock or just proceed.
        
        let prefix = Self::row_prefix_mvcc(db, table, 0); 
        // Note: prefix depends on PK. We need to iterate ALL PKs.
        // Structure: `r/db/table/pk/...`.
        // row_prefix_mvcc uses `db\0table\0pk`.
        // We need `r/db/table\0`.
        let mut table_prefix = Vec::new();
        table_prefix.extend_from_slice(b"r\0");
        table_prefix.extend_from_slice(db.as_bytes());
        table_prefix.push(0);
        table_prefix.extend_from_slice(table.as_bytes());
        table_prefix.push(0);
        
        // We need to group by PK to find latest version.
        // Scan gives keys sorted.
        // `r/db/table/pk1/MAX-tx1`
        // `r/db/table/pk1/MAX-tx2`
        // `r/db/table/pk2/...`
        // So we encounter LATEST version of PK1 first.
        
        let mut current_pk: Option<i64> = None;
        let mut batch = Batch::default();
        
        for item in self.data.scan_prefix(&table_prefix) {
             let (k, v) = item?;
             // Parse PK from key.
             // Key format: `r\0db\0table\0` ... then what? 
             // `row_prefix_mvcc` does: `b"r\0" + db + 0 + table + 0 + pk_bytes`.
             // So we can extract PK.
             // Header len = "r\0".len() + db.len() + 1 + table.len() + 1 = 2 + db + 1 + table + 1.
             let header_len = 2 + db.len() + 1 + table.len() + 1;
             if k.len() < header_len + 8 + 8 { // pk(8) + tx(8)
                 continue;
             }
             let pk_bytes: [u8; 8] = k[header_len..header_len+8].try_into().unwrap();
             let pk = i64::from_be_bytes(pk_bytes);
             
             if Some(pk) != current_pk {
                 current_pk = Some(pk);
                  // This is the latest version for this PK (because scan is ordered and TxID inverted).
                  // Deserialize and Add Index.
                  // Value is Option<Row>
                  let row_opt: Option<Row> = bincode::deserialize(&v)?;
                  let Some(row) = row_opt else {
                      // Tombstone (deleted row), skip index creation
                      continue;
                  };
                  
                  // Add index entry
                 // Assuming single column for MVP
                 let col_name = &index.columns[0];
                 let col_idx = def.columns.iter().position(|c| &c.name == col_name).unwrap();
                 let val = &row.values[col_idx];
                 
                 let idx_key = Self::index_key(db, table, &index.name, val, pk);
                 batch.insert(idx_key, vec![]);
             }
        }
        
        self.data.apply_batch(batch)?;
        self.flush()?;
        
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

    // MVCC: Read with snapshot isolation.
    pub fn get_row_mvcc(&self, db: &str, table: &str, pk: i64, view: &ReadView) -> Result<Option<Row>, MiniError> {
        let prefix = Self::row_prefix_mvcc(db, table, pk);
        // data.scan_prefix(prefix) will return keys sorted by raw byte value.
        // Our key format: prefix + pk + (u64::MAX - tx_id).
        // Max - TxID:
        // TxID=100 => Max-100
        // TxID=99  => Max-99
        // (Max-100) < (Max-99).
        // So newer transactions (higher TxID) have SMALLER suffixes.
        // Thus, scan_prefix returns NEWEST version first.
        
        for item in self.data.scan_prefix(&prefix) {
            let (k, v) = item?;
            // Extract TxID from key suffix (last 8 bytes).
            let tx_id = Self::parse_tx_id_from_key(&k)?;
            
            if view.is_visible(tx_id) {
                // Found the visible version!
                // Value is Option<Row> (serialized). None = Tombstone (Deleted).
                let val: Option<Row> = bincode::deserialize(&v)?;
                return Ok(val);
            }
        }
        
        Ok(None)
    }

    #[allow(deprecated)]
    pub fn get_row(&self, db: &str, table: &str, pk: i64) -> Result<Option<Row>, MiniError> {
        // Legacy path (for now). Assumes "read committed" or "dirty read" equivalent?
        // Or just read latest?
        // Let's create a temporary view that sees EVERYTHING (fake).
        let view = ReadView { visible_up_to: u64::MAX, active: BTreeSet::new(), own_tx_id: None };
        self.get_row_mvcc(db, table, pk, &view)
    }

    pub fn apply_row_changes_mvcc<'a, I>(&self, changes: I, tx_id: TransactionId) -> Result<(), MiniError>
    where
        I: IntoIterator<Item = (&'a str, &'a str, i64, Option<&'a Row>)>,
    {
        let mut batch = Batch::default();
        for (db, table, pk, new_row) in changes {
            // Write a NEW version.
            let key = Self::row_key_mvcc(db, table, pk, tx_id);
            // Value is Option<Row>.
            let val = bincode::serialize(&new_row.cloned())?;
            batch.insert(key, val);
            
            // Index Maintenance
            // 1. Get Old Row (Latest version in DB)
            // We scan prefix. First item is latest (inverted tx_id).
            let prefix = Self::row_prefix_mvcc(db, table, pk);
            let old_row: Option<Row> = if let Some(res) = self.data.scan_prefix(&prefix).next() {
                let (_, v) = res?;
                bincode::deserialize(&v)?
            } else {
                 None
            };
            
            // Index Maintenance
            // We need TableDef to know indexes.
            // We need TableDef to know indexes.
            // Lookup catalog: use table_key helper
            let cat_key = Self::table_key(db, table);
            
            let def_bytes = self.catalog.get(&cat_key)?.ok_or(MiniError::Invalid(format!("Table {}.{} not found", db, table)))?;
            let def: TableDef = bincode::deserialize(&def_bytes)?;
            
            for idx in &def.indexes {
                // Assuming single column index for MVP
                let col_name = &idx.columns[0];
                let col_idx = def.columns.iter().position(|c| &c.name == col_name).unwrap();
                
                // Remove Old
                if let Some(old) = &old_row {
                     // Check if old row was "deleted" (Option<Row> in standard storage?)
                     // Wait, in my design `val` is `Option<Row>` serialized?
                     // In `scan_rows_mvcc`: `let val: Option<Row> = bincode::deserialize(&v)?;`
                     // Yes.
                    let old_val = &old.values[col_idx];
                    let k = Self::index_key(db, table, &idx.name, old_val, pk);
                    batch.remove(k);
                }
                
                // Add New
                if let Some(new_r) = new_row {
                    let new_val = &new_r.values[col_idx];
                    let k = Self::index_key(db, table, &idx.name, new_val, pk);
                    batch.insert(k, vec![]); // Value empty
                }
            }
        }
        
        // Also persist the Max TxID to catalog so we resume correctly on restart.
        // We update 'sys_max_tx_id' to `tx_id`.
        // Since this is in the same atomic batch (applied to different trees? No, batch is tree-specific in sled usually? 
        // Wait, sled::Batch is for a single Tree? 
        // Sled documentation: db.apply_batch(batch) applies to default tree?
        // Actually batch can contain operations for multiple trees? No, verify sled API.
        // If sled::Batch is simple, we might need to put metadata in data tree or use transactions.
        // Sled `apply_batch` is on `Tree`. `db.apply_batch` applies to default tree.
        // Our data is in `self.data` (a Tree). `catalog` is another Tree.
        // Atomicity across trees requires `db.transaction(...)`.
        // But `transaction` closure is complex.
        // HACK: Store metadata in `data` tree with special prefix for MVP durability.
        // Prefix: "m\0" (metadata).
        
        let meta_key = b"m\0max_tx_id";
        batch.insert(meta_key, tx_id.to_be_bytes().to_vec());
        
        self.data.apply_batch(batch)?;
        self.flush()?;
        Ok(())
    }

    pub fn apply_row_changes<'a, I>(&self, changes: I) -> Result<(), MiniError>
    where
        I: IntoIterator<Item = (&'a str, &'a str, i64, Option<&'a Row>)>,
    {
         // Legacy: auto-assign a transaction ID? 
         // This is dangerous but good for backward compat if any calls remain.
         // We'll treat this as a "system transaction".
         let (tx, _) = self.txn_manager.start_txn();
         self.apply_row_changes_mvcc(changes, tx)?;
         self.txn_manager.commit_txn(tx);
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

    pub fn scan_rows_mvcc(&self, db: &str, table: &str, view: &ReadView) -> Result<Vec<(i64, Row)>, MiniError> {
        // row_prefix returns "r\0db\0table\0".
        // That is the correct prefix for ALL rows.
        let prefix = Self::row_prefix(db, table);
        
        let mut out = Vec::new();
        let cursor = self.data.scan_prefix(&prefix);
        
        // We need to iterate and group by PK.
        // Keys: [prefix] [pk: 8 bytes] [tx_inv: 8 bytes]
        // Sorted by pk ASC, then tx_inv ASC (Newest first).
        
        let mut current_pk: Option<i64> = None;
        let mut pk_found = false; // did we find a visible version for current_pk?
        
        for item in cursor {
            let (k, v) = item?;
            // k should be prefix.len() + 16 bytes.
            if k.len() < prefix.len() + 16 {
                continue; // Should not happen if consistent
            }
            
            // Extract PK.
            let pk_start = prefix.len();
            let pk_bytes: [u8; 8] = k[pk_start..pk_start+8].try_into().unwrap();
            let pk = i64::from_be_bytes(pk_bytes);
            
            // Extract TxID.
            let tx_id = Self::parse_tx_id_from_key(&k)?;
            
            if Some(pk) != current_pk {
                // New PK.
                current_pk = Some(pk);
                pk_found = false;
            }
            
            if pk_found {
                // We already found a visible version for this PK. Skip older versions.
                continue;
            }
            
            if view.is_visible(tx_id) {
                // Found the visible version.
                pk_found = true;
                let val: Option<Row> = bincode::deserialize(&v)?;
                if let Some(row) = val {
                    out.push((pk, row));
                }
                // If None, it's deleted. We still mark pk_found=true so we skip older versions (where it might exist).
            }
        }
        
        Ok(out)
    }

    pub fn scan_rows(&self, db: &str, table: &str) -> Result<Vec<(i64, Row)>, MiniError> {
        let view = ReadView { visible_up_to: u64::MAX, active: BTreeSet::new(), own_tx_id: None };
        self.scan_rows_mvcc(db, table, &view)
    }

    pub fn count_rows(&self, db: &str, table: &str) -> Result<u64, MiniError> {
        // Expensive legacy count.
        let rows = self.scan_rows(db, table)?;
        Ok(rows.len() as u64)
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

    // Generic prefix for all rows in a table (without PK).
    fn row_prefix(db: &str, table: &str) -> Vec<u8> {
        let mut k = Vec::new();
        k.extend_from_slice(b"r\0");
        k.extend_from_slice(db.as_bytes());
        k.push(0);
        if !table.is_empty() {
            k.extend_from_slice(table.as_bytes());
            k.push(0);
        }
        k
    }

    // Prefix including PK but NOT TxID.
    // Used for scanning versions of a specific row.
    fn row_prefix_mvcc(db: &str, table: &str, pk: i64) -> Vec<u8> {
        let mut k = Self::row_prefix(db, table); // "r\0db\0table\0"
        k.extend_from_slice(&pk.to_be_bytes());
        k
    }

    fn row_key_mvcc(db: &str, table: &str, pk: i64, tx_id: TransactionId) -> Vec<u8> {
        let mut k = Self::row_prefix_mvcc(db, table, pk);
        // Append inverted TxID for sorting (Newest First).
        let inverted = u64::MAX - tx_id;
        k.extend_from_slice(&inverted.to_be_bytes());
        k
    }

    // Legacy helper?
    #[allow(dead_code)]
    fn row_key(db: &str, table: &str, pk: i64) -> Vec<u8> {
        Self::row_prefix_mvcc(db, table, pk)
    }

    fn index_key(db: &str, table: &str, index_name: &str, val: &Cell, pk: i64) -> Vec<u8> {
        let mut k = Vec::new();
        k.extend_from_slice(b"i\0");
        k.extend_from_slice(db.as_bytes());
        k.push(0);
        k.extend_from_slice(table.as_bytes());
        k.push(0);
        k.extend_from_slice(index_name.as_bytes());
        k.push(0);
        match val {
            Cell::Int(i) => k.extend_from_slice(&i.to_be_bytes()),
            Cell::Text(s) => {
                k.extend_from_slice(s.as_bytes());
                k.push(0);
            }
            Cell::Null => k.push(0),
             _ => {
                 // Fallback
             }
        }
        k.extend_from_slice(&pk.to_be_bytes());
        k
    }

    fn parse_tx_id_from_key(key: &[u8]) -> Result<TransactionId, MiniError> {
        if key.len() < 8 {
            return Err(MiniError::Invalid("corrupt mvcc key".into()));
        }
        let inv_bytes: [u8; 8] = key[key.len() - 8..].try_into().unwrap();
        let inverted = u64::from_be_bytes(inv_bytes);
        Ok(u64::MAX - inverted)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Cell, ColumnDef, Row, SqlType};
    use tempfile::tempdir;

    #[test]
    fn test_crash_recovery() -> Result<(), MiniError> {
        let dir = tempdir().map_err(|e| MiniError::Invalid(e.to_string()))?;
        let path = dir.path().to_str().unwrap();

        // RUN 1: Init and Write
        {
            let store = Store::open(path)?;
            let cols = vec![
                ColumnDef { name: "id".into(), ty: SqlType::Int, nullable: false },
                ColumnDef { name: "val".into(), ty: SqlType::Text, nullable: false },
            ];
            store.create_database("test_db")?;
            let mut table_def = TableDef {
                db: "test_db".into(),
                name: "t1".into(),
                columns: cols,
                primary_key: "id".into(),
                auto_increment: false,
                indexes: vec![],
            };
            store.create_table(&table_def)?;

            let (tx1, _view1) = store.txn_manager.start_txn();
            let row1 = Row { values: vec![Cell::Int(1), Cell::Text("v1".into())] };
            let changes = vec![("test_db", "t1", 1i64, Some(&row1))];
            store.apply_row_changes_mvcc(changes, tx1)?;
            store.txn_manager.commit_txn(tx1);
        }

        // RUN 2: Restart
        {
            let store = Store::open(path)?;
            
            // Start new txn
            let (tx2, view2) = store.txn_manager.start_txn();
            assert!(tx2 > 1, "TxID should increase. Got {}", tx2);
            
            // Read old data
            let row = store.get_row_mvcc("test_db", "t1", 1, &view2)?;
            assert!(row.is_some());
            if let Some(r) = row {
                if let Cell::Text(s) = &r.values[1] {
                     assert_eq!(s, "v1");
                } else {
                     panic!("Wrong cell type");
                }
            }
            
            // Write more
            let row2 = Row { values: vec![Cell::Int(2), Cell::Text("v2".into())] };
             let changes = vec![("test_db", "t1", 2i64, Some(&row2))];
            store.apply_row_changes_mvcc(changes, tx2)?;
            store.txn_manager.commit_txn(tx2);
        }
        
        // RUN 3: Restart Again
        {
            let store = Store::open(path)?;
            let (tx3, view3) = store.txn_manager.start_txn();
            assert!(tx3 > 2);
            
            let rows = store.scan_rows_mvcc("test_db", "t1", &view3)?;
            assert_eq!(rows.len(), 2);
        }

        Ok(())
    }
}
