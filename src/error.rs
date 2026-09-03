use thiserror::Error;

pub type Result<T> = std::result::Result<T, RustHouseError>;

#[derive(Error, Debug)]
pub enum RustHouseError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("SQL parsing error: {0}")]
    SqlParse(String),

    #[error("Query execution error: {0}")]
    QueryExecution(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Metadata error: {0}")]
    Metadata(String),

    #[error("Compression error: {0}")]
    Compression(String),

    #[error("Invalid data: {0}")]
    InvalidData(String),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Type mismatch: {0}")]
    TypeMismatch(String),

    #[error("Internal error: {0}")]
    Internal(String),

    // --- Governance (GOV-*) ---
    #[error("Access denied: {0}")]
    Denied(String),

    #[error("Unauthenticated")]
    Unauthenticated,

    #[error("Audit log unavailable: {0}")]
    AuditUnavailable(String),

    #[error("Missing classification: {0}")]
    MissingClassification(String),

    #[error("Classification lowering refused: {0}")]
    ClassificationLowering(String),

    // --- Transaction log (LAKE-TXN) ---
    #[error("Commit requires a non-empty principal")]
    MissingPrincipal,

    #[error("Commit conflict on {table}: expected version {expected}, found {found}")]
    CommitConflict {
        table: String,
        expected: u64,
        found: u64,
    },

    #[error("Conflict: {0}")]
    Conflict(String),

    #[error("Corrupt transaction log: {0}")]
    CorruptLog(String),

    #[error("Invalid action: {0}")]
    InvalidAction(String),

    #[error("Version not found: {0}")]
    VersionNotFound(u64),

    #[error("Version {version} of {table} has been vacuumed")]
    VersionVacuumed { table: String, version: u64 },

    // --- Catalog (LAKE-CAT) ---
    #[error("Namespace already exists: {0}")]
    NamespaceExists(String),

    #[error("Namespace not found: {0}")]
    NamespaceNotFound(String),

    #[error("Table already exists: {0}")]
    TableExists(String),

    #[error("Ambiguous table name: {0}")]
    AmbiguousTable(String),

    #[error("Invalid identifier: {0}")]
    InvalidIdentifier(String),

    // --- Parquet (LAKE-PQT) ---
    #[error("Parquet error: {0}")]
    Parquet(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = RustHouseError::TableNotFound("test_table".to_string());
        assert_eq!(err.to_string(), "Table not found: test_table");
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err: RustHouseError = io_err.into();
        assert!(matches!(err, RustHouseError::Io(_)));
    }
}
