use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type TransactionId = u64;
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SqlType {
    Int,
    Float,
    Text,
    Date,
    DateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub ty: SqlType,
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDef {
    pub name: String,
    pub columns: Vec<String>,
    // Potentially Unique flag in future
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDef {
    pub db: String,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    #[serde(default)]
    pub indexes: Vec<IndexDef>,
    pub primary_key: String,
    #[serde(default)]
    pub auto_increment: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Cell {
    Null,
    Int(i64),
    Float(f64),
    Text(String),
    Date(i64),     // Days since epoch
    DateTime(i64), // Millis since epoch
}

impl PartialEq for Cell {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Cell::Null, Cell::Null) => true,
            (Cell::Int(a), Cell::Int(b)) => a == b,
            (Cell::Float(a), Cell::Float(b)) => a.to_bits() == b.to_bits(),
            (Cell::Text(a), Cell::Text(b)) => a == b,
            (Cell::Date(a), Cell::Date(b)) => a == b,
            (Cell::DateTime(a), Cell::DateTime(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Cell {}

impl std::hash::Hash for Cell {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Cell::Null => 0.hash(state),
            Cell::Int(i) => {
                1.hash(state);
                i.hash(state);
            }
            Cell::Float(f) => {
                2.hash(state);
                // Hash bytes for f64 to implement Hash
                f.to_be_bytes().hash(state);
            }
            Cell::Text(s) => {
                3.hash(state);
                s.hash(state);
            }
            Cell::Date(d) => {
                4.hash(state);
                d.hash(state);
            }
            Cell::DateTime(dt) => {
                5.hash(state);
                dt.hash(state);
            }
        }
    }
}

impl Cell {
    pub fn add(&self, other: &Cell) -> Option<Cell> {
        match (self, other) {
            (Cell::Int(a), Cell::Int(b)) => Some(Cell::Int(a + b)),
            (Cell::Float(a), Cell::Float(b)) => Some(Cell::Float(a + b)),
            (Cell::Int(a), Cell::Float(b)) => Some(Cell::Float(*a as f64 + b)),
            (Cell::Float(a), Cell::Int(b)) => Some(Cell::Float(a + *b as f64)),
            _ => None, // Or Null? For now None implies mismatched types or unsupported
        }
    }

    pub fn div_count(&self, count: usize) -> Option<Cell> {
        match self {
            Cell::Int(i) => Some(Cell::Float(*i as f64 / count as f64)),
            Cell::Float(f) => Some(Cell::Float(*f / count as f64)),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Cell::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Cell::Float(f) => Some(*f),
            Cell::Int(i) => Some(*i as f64),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    /// Values aligned to TableDef.columns.
    pub values: Vec<Cell>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserRecord {
    pub username: String,
    pub host: String,
    pub plugin: String,
    /// mysql_native_password stores SHA1(SHA1(password)) (20 bytes)
    pub auth_stage2: Option<[u8; 20]>,
    pub global_privs: u64,
    pub db_privs: BTreeMap<String, u64>,
}
