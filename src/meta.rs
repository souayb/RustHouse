use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ColumnType {
    Int64,
    Float64,
    String,
    Boolean,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub column_type: ColumnType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentStats {
    pub min_values: HashMap<String, serde_json::Value>,
    pub max_values: HashMap<String, serde_json::Value>,
    pub row_count: usize,
}

impl SegmentStats {
    pub fn new() -> Self {
        Self {
            min_values: HashMap::new(),
            max_values: HashMap::new(),
            row_count: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Segment {
    pub id: String,
    pub table_name: String,
    pub path: PathBuf,
    pub stats: SegmentStats,
    pub created_at: u64,
    pub size_bytes: u64,
}

impl Segment {
    pub fn new(id: String, table_name: String, path: PathBuf) -> Self {
        Self {
            id,
            table_name,
            path,
            stats: SegmentStats::new(),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            size_bytes: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub segments: Vec<Segment>,
    pub created_at: u64,
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        Self {
            name,
            columns,
            segments: Vec::new(),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    pub fn add_segment(&mut self, segment: Segment) {
        self.segments.push(segment);
    }

    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub tables: HashMap<String, Table>,
    pub version: u64,
}

impl Metadata {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            version: 0,
        }
    }

    pub fn add_table(&mut self, table: Table) {
        self.tables.insert(table.name.clone(), table);
        self.version += 1;
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }

    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        self.version += 1;
        self.tables.get_mut(name)
    }
}

pub struct MetadataStore {
    path: PathBuf,
}

impl MetadataStore {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    /// Atomically write metadata using tmp file + rename
    pub async fn write(&self, metadata: &Metadata) -> Result<()> {
        // Ensure directory exists
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).await?;
        }

        // Serialize metadata
        let json = serde_json::to_string_pretty(metadata)?;

        // Write to temporary file
        let tmp_path = self.path.with_extension("tmp");
        let mut file = fs::File::create(&tmp_path).await?;
        file.write_all(json.as_bytes()).await?;
        file.sync_all().await?;
        drop(file);

        // Atomic rename
        fs::rename(&tmp_path, &self.path).await?;

        Ok(())
    }

    /// Read metadata from file
    pub async fn read(&self) -> Result<Metadata> {
        if !fs::try_exists(&self.path).await? {
            return Ok(Metadata::new());
        }

        let contents = fs::read_to_string(&self.path).await?;
        let metadata: Metadata = serde_json::from_str(&contents)?;
        Ok(metadata)
    }

    /// Check if metadata file exists
    pub async fn exists(&self) -> bool {
        fs::try_exists(&self.path).await.unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_column_creation() {
        let col = Column {
            name: "id".to_string(),
            column_type: ColumnType::Int64,
        };
        assert_eq!(col.name, "id");
        assert!(matches!(col.column_type, ColumnType::Int64));
    }

    #[test]
    fn test_table_creation() {
        let columns = vec![
            Column {
                name: "id".to_string(),
                column_type: ColumnType::Int64,
            },
            Column {
                name: "name".to_string(),
                column_type: ColumnType::String,
            },
        ];

        let table = Table::new("users".to_string(), columns);
        assert_eq!(table.name, "users");
        assert_eq!(table.columns.len(), 2);
        assert!(table.segments.is_empty());
        assert!(table.get_column("id").is_some());
        assert!(table.get_column("unknown").is_none());
    }

    #[test]
    fn test_segment_creation() {
        let segment = Segment::new(
            "seg_001".to_string(),
            "users".to_string(),
            PathBuf::from("/data/segments/seg_001"),
        );
        assert_eq!(segment.id, "seg_001");
        assert_eq!(segment.table_name, "users");
        assert_eq!(segment.stats.row_count, 0);
    }

    #[test]
    fn test_metadata_operations() {
        let mut metadata = Metadata::new();
        assert_eq!(metadata.version, 0);

        let columns = vec![Column {
            name: "id".to_string(),
            column_type: ColumnType::Int64,
        }];
        let table = Table::new("test".to_string(), columns);

        metadata.add_table(table);
        assert_eq!(metadata.version, 1);
        assert!(metadata.get_table("test").is_some());
        assert!(metadata.get_table("nonexistent").is_none());
    }

    #[tokio::test]
    async fn test_metadata_store_write_read() {
        let temp_dir = TempDir::new().unwrap();
        let metadata_path = temp_dir.path().join("metadata.json");
        let store = MetadataStore::new(&metadata_path);

        // Create test metadata
        let mut metadata = Metadata::new();
        let columns = vec![
            Column {
                name: "id".to_string(),
                column_type: ColumnType::Int64,
            },
            Column {
                name: "value".to_string(),
                column_type: ColumnType::Float64,
            },
        ];
        let table = Table::new("test_table".to_string(), columns);
        metadata.add_table(table);

        // Write metadata
        store.write(&metadata).await.unwrap();
        assert!(store.exists().await);

        // Read metadata back
        let loaded = store.read().await.unwrap();
        assert_eq!(loaded.tables.len(), 1);
        assert!(loaded.get_table("test_table").is_some());

        let loaded_table = loaded.get_table("test_table").unwrap();
        assert_eq!(loaded_table.columns.len(), 2);
        assert_eq!(loaded_table.columns[0].name, "id");
    }

    #[tokio::test]
    async fn test_atomic_write_behavior() {
        let temp_dir = TempDir::new().unwrap();
        let metadata_path = temp_dir.path().join("metadata.json");
        let store = MetadataStore::new(&metadata_path);

        let mut metadata = Metadata::new();
        let columns = vec![Column {
            name: "id".to_string(),
            column_type: ColumnType::Int64,
        }];
        let table = Table::new("table1".to_string(), columns);
        metadata.add_table(table);

        // First write
        store.write(&metadata).await.unwrap();

        // Verify tmp file is cleaned up after atomic rename
        let tmp_path = metadata_path.with_extension("tmp");
        assert!(!tmp_path.exists());

        // Second write (should overwrite)
        let columns2 = vec![Column {
            name: "id2".to_string(),
            column_type: ColumnType::String,
        }];
        let table2 = Table::new("table2".to_string(), columns2);
        metadata.add_table(table2);

        store.write(&metadata).await.unwrap();

        let loaded = store.read().await.unwrap();
        assert_eq!(loaded.tables.len(), 2);
        assert!(loaded.get_table("table1").is_some());
        assert!(loaded.get_table("table2").is_some());
    }

    #[tokio::test]
    async fn test_read_nonexistent_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let metadata_path = temp_dir.path().join("nonexistent.json");
        let store = MetadataStore::new(&metadata_path);

        // Should return empty metadata, not error
        let metadata = store.read().await.unwrap();
        assert_eq!(metadata.tables.len(), 0);
        assert_eq!(metadata.version, 0);
    }

    #[tokio::test]
    async fn test_segment_stats() {
        let mut stats = SegmentStats::new();
        stats
            .min_values
            .insert("id".to_string(), serde_json::json!(1));
        stats
            .max_values
            .insert("id".to_string(), serde_json::json!(1000));
        stats.row_count = 1000;

        assert_eq!(
            stats.min_values.get("id").unwrap(),
            &serde_json::json!(1)
        );
        assert_eq!(
            stats.max_values.get("id").unwrap(),
            &serde_json::json!(1000)
        );
        assert_eq!(stats.row_count, 1000);
    }
}