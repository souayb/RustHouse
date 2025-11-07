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
