use thiserror::Error;

#[derive(Debug, Error)]
pub enum MiniError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Storage error: {0}")]
    Storage(#[from] sled::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Not supported: {0}")]
    NotSupported(String),

    #[error("Access denied: {0}")]
    AccessDenied(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Invalid: {0}")]
    Invalid(String),

    #[error("Lock wait timeout: {0}")]
    LockWaitTimeout(String),

    #[error("Unknown system variable '{0}'")]
    UnknownSystemVariable(String),
}
