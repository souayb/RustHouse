//! Parquet data files for the lake tier (spec LAKE-PQT, ADR-0003).
//!
//! Files are immutable `part-<uuid>.parquet`, zstd-compressed, with per-column statistics
//! returned as [`FileStats`] for the `add` action (LAKE-TXN). Row-group pruning reuses the
//! hot-tier predicate semantics via [`range_can_match`].

use crate::error::{Result, RustHouseError};
use crate::lake::stats::{ColumnStats, FileStats};
use crate::meta::{Column, ColumnType};
use crate::storage::pruner::SegmentPruner;
use crate::storage::{ColumnData, Predicate};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::arrow::ProjectionMask;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::file::statistics::Statistics;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Row groups skipped by [`ParquetReader::read_filtered`] since process start
/// (feeds `rusthouse_parquet_row_groups_pruned_total`).
static ROW_GROUPS_PRUNED: AtomicU64 = AtomicU64::new(0);

pub fn row_groups_pruned() -> u64 {
    ROW_GROUPS_PRUNED.load(Ordering::Relaxed)
}

/// `part-<uuid-v4>.parquet` inside `table_dir` (LAKE-PQT-002).
pub fn new_part_path(table_dir: &Path) -> PathBuf {
    table_dir.join(format!("part-{}.parquet", uuid::Uuid::new_v4()))
}

fn pq(e: impl std::fmt::Display) -> RustHouseError {
    RustHouseError::Parquet(e.to_string())
}

pub struct ParquetWriter {
    pub compression: Compression,
    /// Rows per row group (LAKE-PQT-012).
    pub max_rows_per_group: usize,
}

impl Default for ParquetWriter {
    fn default() -> Self {
        Self {
            compression: Compression::ZSTD(ZstdLevel::try_new(3).expect("valid zstd level")),
            max_rows_per_group: 65_536,
        }
    }
}

impl ParquetWriter {
    /// Write one immutable file. Refuses an existing path (LAKE-PQT-002) and mismatched
    /// types (LAKE-PQT-005) before touching the filesystem.
    pub async fn write(
        &self,
        path: &Path,
        columns: &[Column],
        data: HashMap<String, ColumnData>,
    ) -> Result<FileStats> {
        if path.exists() {
            return Err(RustHouseError::Storage(format!(
                "refusing to overwrite existing data file {}",
                path.display()
            )));
        }
        let mut stats_cols = std::collections::BTreeMap::new();
        for c in columns {
            let cd = data.get(&c.name).ok_or_else(|| {
                RustHouseError::Storage(format!("Missing column data: {}", c.name))
            })?;
            let (min, max) = cd.stats();
            stats_cols.insert(
                c.name.clone(),
                ColumnStats {
                    min,
                    max,
                    null_count: cd.null_count(),
                },
            );
        }
        let batch = to_record_batch(columns, data)?; // validates types and row counts
        let num_records = batch.num_rows() as u64;

        let props = WriterProperties::builder()
            .set_compression(self.compression)
            .set_max_row_group_row_count(Some(self.max_rows_per_group.max(1)))
            .build();
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).map_err(pq)?;
        // Feed the writer in row-group-sized slices so the group bound is honoured.
        let step = self.max_rows_per_group.max(1);
        let mut offset = 0;
        if batch.num_rows() == 0 {
            writer.write(&batch).map_err(pq)?;
        }
        while offset < batch.num_rows() {
            let len = step.min(batch.num_rows() - offset);
            writer.write(&batch.slice(offset, len)).map_err(pq)?;
            writer.flush().map_err(pq)?;
            offset += len;
        }
        writer.close().map_err(pq)?;
        let size_bytes = std::fs::metadata(path)?.len();

        Ok(FileStats {
            num_records,
            size_bytes,
            columns: stats_cols,
        })
    }
}

pub struct ParquetReader;

impl ParquetReader {
    /// Read only `projection` columns (LAKE-PQT-006).
    pub async fn read(path: &Path, projection: &[String]) -> Result<HashMap<String, ColumnData>> {
        Self::read_impl(path, projection, None).await
    }

    /// Like `read`, skipping row groups whose statistics prove no match (LAKE-PQT-007).
    /// Rows of surviving groups are returned unfiltered.
    pub async fn read_filtered(
        path: &Path,
        projection: &[String],
        predicate: &Predicate,
    ) -> Result<HashMap<String, ColumnData>> {
        Self::read_impl(path, projection, Some(predicate)).await
    }

    async fn read_impl(
        path: &Path,
        projection: &[String],
        predicate: Option<&Predicate>,
    ) -> Result<HashMap<String, ColumnData>> {
        let file = std::fs::File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(pq)?;
        let arrow_schema = builder.schema().clone();
        let pq_schema = builder.parquet_schema();

        // Resolve projection to leaf indices (flat schemas: leaf index == field index).
        let mut indices = Vec::with_capacity(projection.len());
        for name in projection {
            let idx = arrow_schema
                .index_of(name)
                .map_err(|_| RustHouseError::ColumnNotFound(name.clone()))?;
            indices.push(idx);
        }
        let mask = ProjectionMask::leaves(pq_schema, indices.iter().copied());

        // Row-group pruning on column-chunk statistics.
        let metadata = builder.metadata().clone();
        let selected: Vec<usize> = match predicate {
            None => (0..metadata.num_row_groups()).collect(),
            Some(pred) => {
                let mut keep = Vec::new();
                for (i, rg) in metadata.row_groups().iter().enumerate() {
                    let lookup = |col: &str| -> Option<(serde_json::Value, serde_json::Value)> {
                        let idx = arrow_schema.index_of(col).ok()?;
                        let stats = rg.column(idx).statistics()?;
                        stats_to_range(stats, arrow_schema.field(idx).data_type())
                    };
                    if SegmentPruner::can_match_with(pred, &lookup) {
                        keep.push(i);
                    } else {
                        ROW_GROUPS_PRUNED.fetch_add(1, Ordering::Relaxed);
                    }
                }
                keep
            }
        };

        let projected: Vec<Field> = indices
            .iter()
            .map(|&i| arrow_schema.field(i).clone())
            .collect();
        let projected_schema = Arc::new(Schema::new(projected));

        let reader = builder
            .with_projection(mask)
            .with_row_groups(selected)
            .with_batch_size(1 << 16)
            .build()
            .map_err(pq)?;
        let mut batches = Vec::new();
        for b in reader {
            batches.push(b.map_err(pq)?);
        }
        let combined = if batches.is_empty() {
            RecordBatch::new_empty(projected_schema)
        } else {
            arrow::compute::concat_batches(&projected_schema, &batches).map_err(pq)?
        };
        from_record_batch(&combined)
    }
}

/// Parquet column-chunk statistics as JSON `(min, max)` in `ColumnData::stats()` terms.
fn stats_to_range(
    stats: &Statistics,
    data_type: &DataType,
) -> Option<(serde_json::Value, serde_json::Value)> {
    match stats {
        Statistics::Int64(s) => Some((
            serde_json::json!(*s.min_opt()?),
            serde_json::json!(*s.max_opt()?),
        )),
        Statistics::Double(s) => Some((
            serde_json::json!(*s.min_opt()?),
            serde_json::json!(*s.max_opt()?),
        )),
        Statistics::Boolean(s) => Some((
            serde_json::json!(*s.min_opt()?),
            serde_json::json!(*s.max_opt()?),
        )),
        Statistics::ByteArray(s) if matches!(data_type, DataType::Utf8 | DataType::LargeUtf8) => {
            let min = std::str::from_utf8(s.min_opt()?.data()).ok()?;
            let max = std::str::from_utf8(s.max_opt()?.data()).ok()?;
            Some((serde_json::json!(min), serde_json::json!(max)))
        }
        _ => None,
    }
}

fn arrow_type(t: &ColumnType) -> DataType {
    match t {
        ColumnType::Int64 => DataType::Int64,
        ColumnType::Float64 => DataType::Float64,
        ColumnType::String => DataType::Utf8,
        ColumnType::Boolean => DataType::Boolean,
    }
}

/// `ColumnData` map → Arrow `RecordBatch` in `columns` order (LAKE-PQT-004/005).
pub fn to_record_batch(
    columns: &[Column],
    mut data: HashMap<String, ColumnData>,
) -> Result<RecordBatch> {
    let mut fields = Vec::with_capacity(columns.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    let mut row_count: Option<usize> = None;
    for c in columns {
        let cd = data
            .remove(&c.name)
            .ok_or_else(|| RustHouseError::Storage(format!("Missing column data: {}", c.name)))?;
        if cd.column_type() != c.column_type {
            return Err(RustHouseError::TypeMismatch(format!(
                "Column {} type mismatch: expected {:?}, got {:?}",
                c.name,
                c.column_type,
                cd.column_type()
            )));
        }
        match row_count {
            None => row_count = Some(cd.len()),
            Some(n) if n != cd.len() => {
                return Err(RustHouseError::Storage(format!(
                    "Column {} has {} rows, expected {}",
                    c.name,
                    cd.len(),
                    n
                )))
            }
            _ => {}
        }
        let nullable = cd.is_nullable();
        let array: ArrayRef = match cd {
            ColumnData::Int64(v) => Arc::new(Int64Array::from(v)),
            ColumnData::Float64(v) => Arc::new(Float64Array::from(v)),
            ColumnData::String(v) => Arc::new(StringArray::from(v)),
            ColumnData::Boolean(v) => Arc::new(BooleanArray::from(v)),
            ColumnData::NullableInt64(v) => Arc::new(Int64Array::from(v)),
            ColumnData::NullableFloat64(v) => Arc::new(Float64Array::from(v)),
            ColumnData::NullableString(v) => Arc::new(StringArray::from(v)),
            ColumnData::NullableBoolean(v) => Arc::new(BooleanArray::from(v)),
        };
        fields.push(Field::new(&c.name, arrow_type(&c.column_type), nullable));
        arrays.push(array);
    }
    let schema: SchemaRef = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrays).map_err(pq)
}

/// Arrow `RecordBatch` → `ColumnData` map. Fields declared nullable (which Parquet preserves as
/// `OPTIONAL`) become `Nullable*` variants even when no value is null, so the bridge is lossless
/// (LAKE-PQT-004).
pub fn from_record_batch(batch: &RecordBatch) -> Result<HashMap<String, ColumnData>> {
    let mut out = HashMap::with_capacity(batch.num_columns());
    for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
        let has_nulls = field.is_nullable() || array.null_count() > 0;
        let cd = match field.data_type() {
            DataType::Int64 => {
                let a = array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| pq("expected Int64Array"))?;
                if has_nulls {
                    ColumnData::NullableInt64(a.iter().collect())
                } else {
                    ColumnData::Int64(a.values().to_vec())
                }
            }
            DataType::Float64 => {
                let a = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| pq("expected Float64Array"))?;
                if has_nulls {
                    ColumnData::NullableFloat64(a.iter().collect())
                } else {
                    ColumnData::Float64(a.values().to_vec())
                }
            }
            DataType::Utf8 => {
                let a = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| pq("expected StringArray"))?;
                if has_nulls {
                    ColumnData::NullableString(a.iter().map(|s| s.map(str::to_string)).collect())
                } else {
                    ColumnData::String(
                        a.iter()
                            .map(|s| s.unwrap_or_default().to_string())
                            .collect(),
                    )
                }
            }
            DataType::Boolean => {
                let a = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| pq("expected BooleanArray"))?;
                if has_nulls {
                    ColumnData::NullableBoolean(a.iter().collect())
                } else {
                    ColumnData::Boolean(a.iter().map(|b| b.unwrap_or_default()).collect())
                }
            }
            other => {
                return Err(RustHouseError::TypeMismatch(format!(
                    "unsupported Arrow type {other:?} for column {}",
                    field.name()
                )))
            }
        };
        out.insert(field.name().clone(), cd);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_part_path_is_uuid_v4() {
        // REQ: LAKE-PQT-002
        let p = new_part_path(Path::new("/t"));
        let name = p.file_name().unwrap().to_str().unwrap();
        let id = name
            .strip_prefix("part-")
            .and_then(|s| s.strip_suffix(".parquet"))
            .unwrap();
        let u = uuid::Uuid::parse_str(id).unwrap();
        assert_eq!(u.get_version(), Some(uuid::Version::Random));
        assert_ne!(new_part_path(Path::new("/t")), p, "paths are unique");
    }

    #[tokio::test]
    async fn test_row_group_size_bounded() {
        // REQ: LAKE-PQT-012
        use parquet::file::reader::{FileReader, SerializedFileReader};
        let dir = tempfile::tempdir().unwrap();
        let path = new_part_path(dir.path());
        let columns = vec![Column {
            name: "id".into(),
            column_type: ColumnType::Int64,
        }];
        let mut data = HashMap::new();
        data.insert("id".to_string(), ColumnData::Int64((0..250).collect()));
        let w = ParquetWriter {
            max_rows_per_group: 100,
            ..ParquetWriter::default()
        };
        w.write(&path, &columns, data).await.unwrap();
        let r = SerializedFileReader::new(std::fs::File::open(&path).unwrap()).unwrap();
        let groups = r.metadata().row_groups();
        assert_eq!(groups.len(), 3);
        assert!(groups.iter().all(|g| g.num_rows() <= 100));
        assert_eq!(groups.iter().map(|g| g.num_rows()).sum::<i64>(), 250);
    }

    #[test]
    fn test_stats_to_range_maps_types() {
        use parquet::file::statistics::ValueStatistics;
        let s = Statistics::Int64(ValueStatistics::new(Some(3), Some(9), None, Some(0), false));
        assert_eq!(
            stats_to_range(&s, &DataType::Int64),
            Some((serde_json::json!(3), serde_json::json!(9)))
        );
        let s = Statistics::Int64(ValueStatistics::new(None, None, None, Some(4), false));
        assert_eq!(stats_to_range(&s, &DataType::Int64), None);
    }
}
