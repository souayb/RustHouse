use crate::error::{Result, RustHouseError};
use crate::meta::{Column, Segment, SegmentStats};
use crate::storage::column_writer::{ColumnData, ColumnWriter};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::fs;

pub struct SegmentWriter {
    segment_path: PathBuf,
    columns: Vec<Column>,
    column_writer: ColumnWriter,
}

impl SegmentWriter {
    pub fn new<P: AsRef<Path>>(
        segment_path: P,
        columns: Vec<Column>,
        compression_level: i32,
    ) -> Self {
        Self {
            segment_path: segment_path.as_ref().to_path_buf(),
            columns,
            column_writer: ColumnWriter::new(compression_level),
        }
    }

    /// Write multiple columns to a segment
    pub async fn write(&self, data: HashMap<String, ColumnData>) -> Result<Segment> {
        // Validate all columns present
        for column in &self.columns {
            if !data.contains_key(&column.name) {
                return Err(RustHouseError::Storage(format!(
                    "Missing column data: {}",
                    column.name
                )));
            }
        }

        // Validate all columns have same row count
        let row_count = data.values().next().map(|d| d.len()).unwrap_or(0);
        for (name, col_data) in &data {
            if col_data.len() != row_count {
                return Err(RustHouseError::Storage(format!(
                    "Column {} has {} rows, expected {}",
                    name,
                    col_data.len(),
                    row_count
                )));
            }
        }

        // Create segment directory
        fs::create_dir_all(&self.segment_path).await?;

        // Write each column
        let mut stats = SegmentStats::new();
        stats.row_count = row_count;
        let mut total_size = 0u64;

        for (i, column) in self.columns.iter().enumerate() {
            let col_data = data.get(&column.name).unwrap();

            // Validate column type matches
            if col_data.column_type() != column.column_type {
                return Err(RustHouseError::TypeMismatch(format!(
                    "Column {} type mismatch: expected {:?}, got {:?}",
                    column.name,
                    column.column_type,
                    col_data.column_type()
                )));
            }

            let column_path = self.segment_path.join(format!("col_{}.bin", i));
            let size = self.column_writer.write(&column_path, col_data).await?;
            total_size += size;

            // Collect stats
            let (min, max) = col_data.stats();
            stats.min_values.insert(column.name.clone(), min);
            stats.max_values.insert(column.name.clone(), max);
        }

        // Create segment metadata
        let segment_id = self
            .segment_path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        let mut segment = Segment::new(
            segment_id,
            "table".to_string(), // Will be set by caller
            self.segment_path.clone(),
        );
        segment.stats = stats;
        segment.size_bytes = total_size;

        Ok(segment)
    }
}

pub struct SegmentReader {
    segment_path: PathBuf,
    columns: Vec<Column>,
    column_writer: ColumnWriter, // Reuse for reading
}

impl SegmentReader {
    pub fn new<P: AsRef<Path>>(segment_path: P, columns: Vec<Column>) -> Self {
        Self {
            segment_path: segment_path.as_ref().to_path_buf(),
            columns,
            column_writer: ColumnWriter::new(3),
        }
    }

    /// Read a specific column from the segment
    pub async fn read_column(&self, column_name: &str, row_count: usize) -> Result<ColumnData> {
        // Find column index
        let (index, column) = self
            .columns
            .iter()
            .enumerate()
            .find(|(_, c)| c.name == column_name)
            .ok_or_else(|| RustHouseError::ColumnNotFound(column_name.to_string()))?;

        let column_path = self.segment_path.join(format!("col_{}.bin", index));
        self.column_writer
            .read(&column_path, &column.column_type, row_count)
            .await
    }

    /// Read all columns from the segment
    pub async fn read_all(&self, row_count: usize) -> Result<HashMap<String, ColumnData>> {
        let mut data = HashMap::new();

        for column in &self.columns {
            let col_data = self.read_column(&column.name, row_count).await?;
            data.insert(column.name.clone(), col_data);
        }

        Ok(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::ColumnType;
    use tempfile::TempDir;

    fn create_test_columns() -> Vec<Column> {
        vec![
            Column {
                name: "id".to_string(),
                column_type: ColumnType::Int64,
            },
            Column {
                name: "value".to_string(),
                column_type: ColumnType::Float64,
            },
            Column {
                name: "name".to_string(),
                column_type: ColumnType::String,
            },
        ]
    }

    #[tokio::test]
    async fn test_segment_write_read() {
        let temp_dir = TempDir::new().unwrap();
        let segment_path = temp_dir.path().join("segment_001");

        let columns = create_test_columns();
        let writer = SegmentWriter::new(&segment_path, columns.clone(), 3);

        // Prepare data
        let mut data = HashMap::new();
        data.insert("id".to_string(), ColumnData::Int64(vec![1, 2, 3, 4, 5]));
        data.insert(
            "value".to_string(),
            ColumnData::Float64(vec![1.1, 2.2, 3.3, 4.4, 5.5]),
        );
        data.insert(
            "name".to_string(),
            ColumnData::String(vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "d".to_string(),
                "e".to_string(),
            ]),
        );

        // Write segment
        let segment = writer.write(data).await.unwrap();
        assert_eq!(segment.stats.row_count, 5);
        assert!(segment.size_bytes > 0);

        // Verify stats
        assert_eq!(
            segment.stats.min_values.get("id").unwrap(),
            &serde_json::json!(1)
        );
        assert_eq!(
            segment.stats.max_values.get("id").unwrap(),
            &serde_json::json!(5)
        );

        // Read segment back
        let reader = SegmentReader::new(&segment_path, columns);
        let read_data = reader.read_all(5).await.unwrap();

        // Verify data
        match read_data.get("id").unwrap() {
            ColumnData::Int64(v) => assert_eq!(v, &vec![1, 2, 3, 4, 5]),
            _ => panic!("Wrong type"),
        }

        match read_data.get("name").unwrap() {
            ColumnData::String(v) => {
                assert_eq!(v.len(), 5);
                assert_eq!(v[0], "a");
                assert_eq!(v[4], "e");
            }
            _ => panic!("Wrong type"),
        }
    }

    #[tokio::test]
    async fn test_segment_write_mismatched_rows() {
        let temp_dir = TempDir::new().unwrap();
        let segment_path = temp_dir.path().join("segment_002");

        let columns = create_test_columns();
        let writer = SegmentWriter::new(&segment_path, columns, 3);

        let mut data = HashMap::new();
        data.insert("id".to_string(), ColumnData::Int64(vec![1, 2, 3]));
        data.insert("value".to_string(), ColumnData::Float64(vec![1.1, 2.2])); // Wrong count
        data.insert(
            "name".to_string(),
            ColumnData::String(vec!["a".to_string()]),
        );

        // Should fail due to mismatched row counts
        let result = writer.write(data).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_segment_write_missing_column() {
        let temp_dir = TempDir::new().unwrap();
        let segment_path = temp_dir.path().join("segment_003");

        let columns = create_test_columns();
        let writer = SegmentWriter::new(&segment_path, columns, 3);

        let mut data = HashMap::new();
        data.insert("id".to_string(), ColumnData::Int64(vec![1, 2, 3]));
        // Missing "value" and "name" columns

        let result = writer.write(data).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_segment_read_specific_column() {
        let temp_dir = TempDir::new().unwrap();
        let segment_path = temp_dir.path().join("segment_004");

        let columns = create_test_columns();
        let writer = SegmentWriter::new(&segment_path, columns.clone(), 3);

        let mut data = HashMap::new();
        data.insert("id".to_string(), ColumnData::Int64(vec![10, 20, 30]));
        data.insert(
            "value".to_string(),
            ColumnData::Float64(vec![1.0, 2.0, 3.0]),
        );
        data.insert(
            "name".to_string(),
            ColumnData::String(vec!["x".to_string(), "y".to_string(), "z".to_string()]),
        );

        writer.write(data).await.unwrap();

        // Read only one column
        let reader = SegmentReader::new(&segment_path, columns);
        let id_data = reader.read_column("id", 3).await.unwrap();

        match id_data {
            ColumnData::Int64(v) => assert_eq!(v, vec![10, 20, 30]),
            _ => panic!("Wrong type"),
        }
    }

    #[tokio::test]
    async fn test_segment_stats_collection() {
        let temp_dir = TempDir::new().unwrap();
        let segment_path = temp_dir.path().join("segment_005");

        let columns = vec![
            Column {
                name: "price".to_string(),
                column_type: ColumnType::Float64,
            },
            Column {
                name: "quantity".to_string(),
                column_type: ColumnType::Int64,
            },
        ];

        let writer = SegmentWriter::new(&segment_path, columns, 3);

        let mut data = HashMap::new();
        data.insert(
            "price".to_string(),
            ColumnData::Float64(vec![10.5, 20.3, 5.7, 99.9]),
        );
        data.insert("quantity".to_string(), ColumnData::Int64(vec![1, 5, 2, 10]));

        let segment = writer.write(data).await.unwrap();

        // Check stats
        assert_eq!(segment.stats.row_count, 4);
        assert_eq!(
            segment.stats.min_values.get("price").unwrap(),
            &serde_json::json!(5.7)
        );
        assert_eq!(
            segment.stats.max_values.get("price").unwrap(),
            &serde_json::json!(99.9)
        );
        assert_eq!(
            segment.stats.min_values.get("quantity").unwrap(),
            &serde_json::json!(1)
        );
        assert_eq!(
            segment.stats.max_values.get("quantity").unwrap(),
            &serde_json::json!(10)
        );
    }
}
