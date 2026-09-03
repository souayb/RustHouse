//! Per-file column statistics recorded in `add` actions (LAKE-PQT-003, LAKE-TXN).

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnStats {
    pub min: serde_json::Value,
    pub max: serde_json::Value,
    pub null_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileStats {
    pub num_records: u64,
    pub size_bytes: u64,
    #[serde(default)]
    pub columns: BTreeMap<String, ColumnStats>,
}

impl FileStats {
    pub fn empty() -> Self {
        Self {
            num_records: 0,
            size_bytes: 0,
            columns: BTreeMap::new(),
        }
    }
}
