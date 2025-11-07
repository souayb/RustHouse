use crate::error::{Result, RustHouseError};
use crate::meta::ColumnType;
use std::path::Path;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

/// Represents a column of data to be written
#[derive(Debug, Clone)]
pub enum ColumnData {
    Int64(Vec<i64>),
    Float64(Vec<f64>),
    String(Vec<String>),
    Boolean(Vec<bool>),
}

impl ColumnData {
    pub fn len(&self) -> usize {
        match self {
            ColumnData::Int64(v) => v.len(),
            ColumnData::Float64(v) => v.len(),
            ColumnData::String(v) => v.len(),
            ColumnData::Boolean(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn column_type(&self) -> ColumnType {
        match self {
            ColumnData::Int64(_) => ColumnType::Int64,
            ColumnData::Float64(_) => ColumnType::Float64,
            ColumnData::String(_) => ColumnType::String,
            ColumnData::Boolean(_) => ColumnType::Boolean,
        }
    }

    /// Get min/max values for statistics
    pub fn stats(&self) -> (serde_json::Value, serde_json::Value) {
        match self {
            ColumnData::Int64(v) => {
                if v.is_empty() {
                    return (serde_json::json!(null), serde_json::json!(null));
                }
                let min = v.iter().min().unwrap();
                let max = v.iter().max().unwrap();
                (serde_json::json!(min), serde_json::json!(max))
            }
            ColumnData::Float64(v) => {
                if v.is_empty() {
                    return (serde_json::json!(null), serde_json::json!(null));
                }
                let min = v
                    .iter()
                    .filter(|x| !x.is_nan())
                    .min_by(|a, b| a.partial_cmp(b).unwrap())
                    .unwrap();
                let max = v
                    .iter()
                    .filter(|x| !x.is_nan())
                    .max_by(|a, b| a.partial_cmp(b).unwrap())
                    .unwrap();
                (serde_json::json!(min), serde_json::json!(max))
            }
            ColumnData::String(v) => {
                if v.is_empty() {
                    return (serde_json::json!(null), serde_json::json!(null));
                }
                let min = v.iter().min().unwrap();
                let max = v.iter().max().unwrap();
                (serde_json::json!(min), serde_json::json!(max))
            }
            ColumnData::Boolean(v) => {
                if v.is_empty() {
                    return (serde_json::json!(null), serde_json::json!(null));
                }
                let min = v.iter().any(|&x| !x);
                let max = v.iter().any(|&x| x);
                (serde_json::json!(!min), serde_json::json!(max))
            }
        }
    }

    /// Serialize column data to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            ColumnData::Int64(v) => {
                let mut bytes = Vec::with_capacity(v.len() * 8);
                for &val in v {
                    bytes.extend_from_slice(&val.to_le_bytes());
                }
                bytes
            }
            ColumnData::Float64(v) => {
                let mut bytes = Vec::with_capacity(v.len() * 8);
                for &val in v {
                    bytes.extend_from_slice(&val.to_le_bytes());
                }
                bytes
            }
            ColumnData::String(v) => {
                // Format: [len:u32][data][len:u32][data]...
                let mut bytes = Vec::new();
                for s in v {
                    let s_bytes = s.as_bytes();
                    bytes.extend_from_slice(&(s_bytes.len() as u32).to_le_bytes());
                    bytes.extend_from_slice(s_bytes);
                }
                bytes
            }
            ColumnData::Boolean(v) => {
                // Pack 8 booleans per byte
                let mut bytes = Vec::with_capacity((v.len() + 7) / 8);
                for chunk in v.chunks(8) {
                    let mut byte = 0u8;
                    for (i, &b) in chunk.iter().enumerate() {
                        if b {
                            byte |= 1 << i;
                        }
                    }
                    bytes.push(byte);
                }
                bytes
            }
        }
    }

    /// Deserialize column data from bytes
    pub fn from_bytes(bytes: &[u8], column_type: &ColumnType, row_count: usize) -> Result<Self> {
        match column_type {
            ColumnType::Int64 => {
                if bytes.len() != row_count * 8 {
                    return Err(RustHouseError::InvalidData(format!(
                        "Expected {} bytes for {} Int64 values, got {}",
                        row_count * 8,
                        row_count,
                        bytes.len()
                    )));
                }
                let mut values = Vec::with_capacity(row_count);
                for chunk in bytes.chunks_exact(8) {
                    let val = i64::from_le_bytes(chunk.try_into().unwrap());
                    values.push(val);
                }
                Ok(ColumnData::Int64(values))
            }
            ColumnType::Float64 => {
                if bytes.len() != row_count * 8 {
                    return Err(RustHouseError::InvalidData(format!(
                        "Expected {} bytes for {} Float64 values, got {}",
                        row_count * 8,
                        row_count,
                        bytes.len()
                    )));
                }
                let mut values = Vec::with_capacity(row_count);
                for chunk in bytes.chunks_exact(8) {
                    let val = f64::from_le_bytes(chunk.try_into().unwrap());
                    values.push(val);
                }
                Ok(ColumnData::Float64(values))
            }
            ColumnType::String => {
                let mut values = Vec::new();
                let mut offset = 0;
                while offset < bytes.len() {
                    if offset + 4 > bytes.len() {
                        return Err(RustHouseError::InvalidData(
                            "Invalid string column format".to_string(),
                        ));
                    }
                    let len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
                        as usize;
                    offset += 4;
                    if offset + len > bytes.len() {
                        return Err(RustHouseError::InvalidData(
                            "Invalid string length".to_string(),
                        ));
                    }
                    let s = String::from_utf8(bytes[offset..offset + len].to_vec())
                        .map_err(|e| RustHouseError::InvalidData(e.to_string()))?;
                    values.push(s);
                    offset += len;
                }
                Ok(ColumnData::String(values))
            }
            ColumnType::Boolean => {
                let mut values = Vec::with_capacity(row_count);
                for (i, byte) in bytes.iter().enumerate() {
                    for bit in 0..8 {
                        if i * 8 + bit >= row_count {
                            break;
                        }
                        values.push((byte & (1 << bit)) != 0);
                    }
                }
                Ok(ColumnData::Boolean(values))
            }
        }
    }
}

pub struct ColumnWriter {
    _compression_level: i32,
}

impl ColumnWriter {
    pub fn new(compression_level: i32) -> Self {
        Self { _compression_level: compression_level }
    }

    /// Write a column to disk with compression
    pub async fn write<P: AsRef<Path>>(
        &self,
        path: P,
        data: &ColumnData,
    ) -> Result<u64> {
        let bytes = data.to_bytes();
        let compressed = lz4_flex::compress_prepend_size(&bytes);

        let mut file = File::create(path).await?;
        file.write_all(&compressed).await?;
        file.sync_all().await?;

        Ok(compressed.len() as u64)
    }

    /// Read and decompress a column from disk
    pub async fn read<P: AsRef<Path>>(
        &self,
        path: P,
        column_type: &ColumnType,
        row_count: usize,
    ) -> Result<ColumnData> {
        let compressed = tokio::fs::read(path).await?;
        let bytes = lz4_flex::decompress_size_prepended(&compressed)
            .map_err(|e| RustHouseError::Compression(e.to_string()))?;

        ColumnData::from_bytes(&bytes, column_type, row_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_column_data_int64() {
        let data = ColumnData::Int64(vec![1, 2, 3, 4, 5]);
        assert_eq!(data.len(), 5);
        assert!(!data.is_empty());

        let (min, max) = data.stats();
        assert_eq!(min, serde_json::json!(1));
        assert_eq!(max, serde_json::json!(5));
    }

    #[test]
    fn test_column_data_serialization_int64() {
        let data = ColumnData::Int64(vec![1, 2, 3]);
        let bytes = data.to_bytes();
        assert_eq!(bytes.len(), 24); // 3 * 8 bytes

        let deserialized =
            ColumnData::from_bytes(&bytes, &ColumnType::Int64, 3).unwrap();
        match deserialized {
            ColumnData::Int64(v) => assert_eq!(v, vec![1, 2, 3]),
            _ => panic!("Wrong type"),
        }
    }

    #[test]
    fn test_column_data_serialization_string() {
        let data = ColumnData::String(vec![
            "hello".to_string(),
            "world".to_string(),
        ]);
        let bytes = data.to_bytes();

        let deserialized =
            ColumnData::from_bytes(&bytes, &ColumnType::String, 2).unwrap();
        match deserialized {
            ColumnData::String(v) => {
                assert_eq!(v.len(), 2);
                assert_eq!(v[0], "hello");
                assert_eq!(v[1], "world");
            }
            _ => panic!("Wrong type"),
        }
    }

    #[test]
    fn test_column_data_stats_float64() {
        let data = ColumnData::Float64(vec![1.5, 2.5, 3.5, 0.5]);
        let (min, max) = data.stats();
        assert_eq!(min, serde_json::json!(0.5));
        assert_eq!(max, serde_json::json!(3.5));
    }

    #[tokio::test]
    async fn test_column_writer_write_read() {
        let temp_dir = TempDir::new().unwrap();
        let column_path = temp_dir.path().join("col_0.bin");

        let writer = ColumnWriter::new(3);
        let data = ColumnData::Int64(vec![10, 20, 30, 40, 50]);

        // Write column
        let size = writer.write(&column_path, &data).await.unwrap();
        assert!(size > 0);
        assert!(column_path.exists());

        // Read column back
        let read_data = writer
            .read(&column_path, &ColumnType::Int64, 5)
            .await
            .unwrap();

        match read_data {
            ColumnData::Int64(v) => assert_eq!(v, vec![10, 20, 30, 40, 50]),
            _ => panic!("Wrong type"),
        }
    }

    #[tokio::test]
    async fn test_column_compression() {
        let temp_dir = TempDir::new().unwrap();
        let column_path = temp_dir.path().join("col_compressed.bin");

        let writer = ColumnWriter::new(3);
        // Create data with repetition (compresses well)
        let data = ColumnData::Int64(vec![1; 1000]);

        let size = writer.write(&column_path, &data).await.unwrap();

        // Compressed size should be much smaller than 8000 bytes (1000 * 8)
        assert!(size < 1000);
    }

    #[test]
    fn test_boolean_serialization() {
        let data = ColumnData::Boolean(vec![true, false, true, true, false, false, true, false, true]);
        let bytes = data.to_bytes();
        
        // 9 booleans = 2 bytes (8 + 1)
        assert_eq!(bytes.len(), 2);

        let deserialized = ColumnData::from_bytes(&bytes, &ColumnType::Boolean, 9).unwrap();
        match deserialized {
            ColumnData::Boolean(v) => {
                assert_eq!(v, vec![true, false, true, true, false, false, true, false, true]);
            }
            _ => panic!("Wrong type"),
        }
    }
}