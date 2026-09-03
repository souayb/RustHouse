# LAKE-PQT: Parquet data files — open, statistics-bearing, immutable

| Field | Value |
|-------|-------|
| Status | Implemented |
| Owner | RustHouse maintainers |
| Related ADRs | ADR-0001, ADR-0003 |
| Depends on | CORE-STO (ColumnData), CORE-PRN (Predicate semantics), LAKE-TXN (add action) |
| Milestone | M1 |

## 1. Problem and motivation
The as-built `col_N.bin` format (`src/storage/column_writer.rs`) is readable only by RustHouse.
Auditors, model-validation teams, and successor systems cannot verify what was stored. BCBS 239
principle 3 asks for data that can be independently reconciled; DORA requires exit strategies
from ICT providers, which for a data store means data is portable. Apache Parquet, written via
the maintained arrow-rs crates (ADR-0003), makes every data file readable by Spark, DuckDB,
pandas, and any Parquet-aware audit tool, while carrying the per-column statistics the planner
already relies on for pruning. Banks also need exact decimals for monetary amounts; the
current `Float64` is unacceptable for ledgers, so `Decimal(38,18)` is introduced here.

## 2. Scope
### In scope
* Writing a `ColumnData` map to `part-<uuid>.parquet` with zstd compression.
* Extracting per-column `min`, `max`, `null_count` into `FileStats` for the `add` action.
* Reading with column projection; row-group pruning driven by `Predicate`.
* `ColumnData` ↔ Arrow `RecordBatch` bridge.
* Type mapping, including nullable columns and new `Timestamp`/`Decimal(38,18)` types.

### Out of scope
* Deciding which files a query reads (LAKE-TXN snapshot + planner).
* Encryption of Parquet columns (GOV-ENC). The writer exposes a hook, nothing more.
* Hot-tier segment format (unchanged, CORE-STO).

## 3. Requirements

| ID | Level | Requirement |
|----|-------|-------------|
| LAKE-PQT-001 | MUST | `ParquetWriter::write(path, columns: &[Column], data: HashMap<String, ColumnData>)` MUST produce a valid Parquet file readable by the `parquet` crate's standalone reader, with zstd compression on every column chunk. |
| LAKE-PQT-002 | MUST | Data file names MUST be `part-<uuid-v4>.parquet`; the writer MUST refuse to write to a path that already exists (`RustHouseError::Storage`), so files are never modified in place. |
| LAKE-PQT-003 | MUST | The writer MUST return `FileStats { num_records, size_bytes, columns: {name → {min, max, null_count}} }` where min/max are computed exactly as `ColumnData::stats()` does today, and these MUST be what the caller records in the `add` action. |
| LAKE-PQT-004 | MUST | `TryFrom<RecordBatch> for HashMap<String, ColumnData>` and `TryFrom<(&[Column], HashMap<String, ColumnData>)> for RecordBatch` MUST round-trip losslessly for every supported type; the hot tier keeps using `ColumnData` unchanged. |
| LAKE-PQT-005 | MUST | Type mapping MUST be: `Int64`→`Int64`, `Float64`→`Float64`, `String`→`Utf8`, `Boolean`→`Boolean`; a `ColumnData` variant whose type differs from the declared `Column.column_type` MUST be rejected with `RustHouseError::TypeMismatch`. |
| LAKE-PQT-006 | MUST | `ParquetReader::read(path, projection: &[String])` MUST return only the requested columns, MUST NOT read column chunks that are not projected, and MUST return `RustHouseError::ColumnNotFound` for an unknown name. |
| LAKE-PQT-007 | MUST | `ParquetReader::read_filtered(path, projection, predicate: &Predicate)` MUST skip any row group whose Parquet column statistics prove it cannot match, using the same `Eq/Gt/Lt/Gte/Lte/And/Or` semantics as `SegmentPruner::can_match`; when statistics are absent the row group MUST be read. |
| LAKE-PQT-008 | MUST | Columns MUST support nulls: `ColumnData` gains an optional validity bitmap (or nullable variants) and `null_count` in stats; min/max MUST ignore nulls; an all-null column MUST have `min = max = null`. |
| LAKE-PQT-009 | SHOULD | `ColumnType::Timestamp` (microseconds, UTC) and `ColumnType::Decimal { precision: 38, scale: 18 }` SHOULD be supported end-to-end (write, stats, read, bridge) with exact, non-floating arithmetic for Decimal. |
| LAKE-PQT-010 | MUST | Writing an empty batch (0 rows) MUST produce a valid Parquet file with `num_records = 0` and null stats, and reading it MUST return empty columns, not an error. |
| LAKE-PQT-011 | MUST | For any generated `ColumnData` map (property test), write → read → compare MUST be equal, and write → stats MUST equal `ColumnData::stats()` over the same input. |
| LAKE-PQT-012 | SHOULD | The writer SHOULD emit one row group per `max_rows_per_group` (default 65 536) so pruning granularity is bounded. |
| LAKE-PQT-013 | MAY | The writer MAY write Parquet bloom filters for `String` columns tagged as join keys. |

## 4. Design sketch

```rust
// src/lake/parquet.rs   (deps: arrow = "=5x.y", parquet = { version = "=5x.y", features = ["async","zstd"] })
pub struct FileStats { pub num_records: u64, pub size_bytes: u64, pub columns: BTreeMap<String, ColumnStats> }
pub struct ColumnStats { pub min: serde_json::Value, pub max: serde_json::Value, pub null_count: u64 }

pub struct ParquetWriter { compression: Compression /* Zstd(level) */, max_rows_per_group: usize }
impl ParquetWriter {
    pub async fn write(&self, path: &Path, columns: &[Column], data: HashMap<String, ColumnData>) -> Result<FileStats>;
}
pub struct ParquetReader;
impl ParquetReader {
    pub async fn read(path: &Path, projection: &[String]) -> Result<HashMap<String, ColumnData>>;
    pub async fn read_filtered(path: &Path, projection: &[String], pred: &Predicate) -> Result<HashMap<String, ColumnData>>;
}
pub fn new_part_path(table_dir: &Path) -> PathBuf; // part-<uuid>.parquet

// bridge (LAKE-PQT-004)
impl TryFrom<RecordBatch> for HashMap<String, ColumnData> { ... }
pub fn to_record_batch(columns: &[Column], data: HashMap<String, ColumnData>) -> Result<RecordBatch>;

// src/storage/pruner.rs gains a reusable core
pub fn range_can_match(min: &Value, max: &Value, pred: &Predicate) -> bool;  // shared by SegmentPruner and Parquet row-group pruning
```

* `ColumnType` in `src/meta.rs` gains `Timestamp` and `Decimal { precision: u8, scale: u8 }`
  and `Column` gains `nullable: bool` (default `false` to preserve existing segments).
* `ColumnData` gains a `validity: Option<Vec<bool>>` per variant or new `Nullable*` variants —
  decided during red phase; the requirement is behavioural (LAKE-PQT-008).
* Errors: reuse `Storage`, `TypeMismatch`, `ColumnNotFound`; add `Parquet(String)` for
  wrapped crate errors.
* Metrics: `rusthouse_parquet_bytes_written_total`, `rusthouse_parquet_row_groups_pruned_total`.

## 5. Acceptance scenarios

### LAKE-PQT-001: valid zstd Parquet
```gherkin
Given columns (id Int64, amount Float64, name String, active Boolean) and 1 000 rows
When  ParquetWriter::write is called
Then  the file opens with parquet::file::reader::SerializedFileReader
And   every column chunk's compression is ZSTD
```

### LAKE-PQT-002: never overwrite
```gherkin
Given an existing file part-<uuid>.parquet
When  write is called with that same path
Then  Err(Storage) is returned and the original bytes are unchanged
```

### LAKE-PQT-003: stats match ColumnData::stats
```gherkin
Given id = [5, 1, 9] and name = ["b","a","c"]
When  the file is written
Then  FileStats.columns["id"] = {min:1, max:9, null_count:0} and ["name"] = {min:"a", max:"c", null_count:0}
And   num_records = 3
```

### LAKE-PQT-004: RecordBatch bridge round-trips
```gherkin
Given a ColumnData map with all four base types
When  it is converted to a RecordBatch and back
Then  the result equals the input
```

### LAKE-PQT-005: type mismatch rejected
```gherkin
Given a Column "id" declared Int64 and ColumnData::String for "id"
When  write is called
Then  Err(TypeMismatch) and no file is created
```

### LAKE-PQT-006: projection reads only requested columns
```gherkin
Given a file with 4 columns
When  read(path, ["id"]) is called
Then  the map has exactly key "id"
When  read(path, ["nope"]) is called
Then  Err(ColumnNotFound("nope"))
```

### LAKE-PQT-007: row-group pruning mirrors SegmentPruner
```gherkin
Given a file with three row groups whose id ranges are [1-100], [101-200], [201-300]
When  read_filtered(path, ["id"], Eq("id", 150)) is called
Then  only one row group is decoded (metric row_groups_pruned_total increases by 2)
And   the returned ids are exactly those in [101-200] (post-filtering is the executor's job)
```

### LAKE-PQT-008: nulls are supported and excluded from min/max
```gherkin
Given id = [null, 3, null, 7]
When  written and stats computed
Then  null_count = 2, min = 3, max = 7
Given an all-null column
Then  min = null and max = null
```

### LAKE-PQT-010: empty batch
```gherkin
Given zero rows
When  written then read
Then  num_records = 0, all columns present and empty, no error
```

### LAKE-PQT-011: property round-trip
```gherkin
Given arbitrary ColumnData maps generated by proptest (0..10 000 rows, all types, with nulls)
When  write → read
Then  output == input for every case
And   FileStats equals ColumnData::stats() for every column
```

## 6. Test plan (write these first)

| Requirement | Test | Location | Kind |
|-------------|------|----------|------|
| LAKE-PQT-001 | `fn lake_pqt_001_writes_valid_zstd_parquet()` | `tests/spec_lake_pqt.rs` | acceptance |
| LAKE-PQT-002 | `fn lake_pqt_002_refuses_to_overwrite_existing_file()` | `tests/spec_lake_pqt.rs` | acceptance |
| LAKE-PQT-002 | `fn test_new_part_path_is_uuid_v4()` | `src/lake/parquet.rs` | unit |
| LAKE-PQT-003 | `fn lake_pqt_003_file_stats_match_columndata_stats()` | `tests/spec_lake_pqt.rs` | acceptance |
| LAKE-PQT-004 | `fn lake_pqt_004_record_batch_bridge_roundtrip()` | `tests/spec_lake_pqt.rs` | acceptance |
| LAKE-PQT-005 | `fn lake_pqt_005_type_mismatch_rejected()` | `tests/spec_lake_pqt.rs` | acceptance |
| LAKE-PQT-006 | `fn lake_pqt_006_projection_reads_only_requested_columns()` | `tests/spec_lake_pqt.rs` | acceptance |
| LAKE-PQT-007 | `fn lake_pqt_007_row_group_pruning_mirrors_segment_pruner()` | `tests/spec_lake_pqt.rs` | acceptance |
| LAKE-PQT-007 | `fn test_range_can_match_shared_with_segment_pruner()` | `src/storage/pruner.rs` | unit |
| LAKE-PQT-008 | `fn lake_pqt_008_nulls_supported_and_excluded_from_minmax()` | `tests/spec_lake_pqt.rs` | acceptance |
| LAKE-PQT-009 | `fn lake_pqt_009_timestamp_and_decimal_roundtrip()` | `tests/spec_lake_pqt.rs` | acceptance |
| LAKE-PQT-010 | `fn lake_pqt_010_empty_batch_is_valid()` | `tests/spec_lake_pqt.rs` | acceptance |
| LAKE-PQT-011 | `fn lake_pqt_011_property_roundtrip_and_stats()` | `tests/spec_lake_pqt.rs` | property |
| LAKE-PQT-012 | `fn test_row_group_size_bounded()` | `src/lake/parquet.rs` | unit |

## 7. Open questions
* Nullable representation in `ColumnData`: validity bitmap on existing variants (smaller diff,
  touches every match arm) vs `Nullable*` variants. Decide when writing red tests for -008.
* Decimal backing type: `i128` with fixed scale 18 vs `rust_decimal`. Lean `i128` (Arrow native).
* Should `String` columns default to dictionary encoding? Likely yes for low-cardinality codes.

## 8. Change log
| Date | Change |
|------|--------|
| 2026-09-02 | Created |
| 2026-09-02 | Implemented (M1). Decisions: nulls are new `ColumnData::Nullable*` variants (`Vec<Option<T>>`), hot-tier `to_bytes` refuses them (`try_to_bytes` → `InvalidData`) until HYB-ACC adds a validity bitmap; the RecordBatch bridge is the free functions `to_record_batch` / `from_record_batch` (a `TryFrom` for `HashMap` is blocked by the orphan rule); the reader honours Arrow field nullability so a nullable column with no nulls still round-trips as `Nullable*`; shared pruning core is `storage::pruner::range_can_match` plus `SegmentPruner::can_match_with`; pruned row groups are counted in `lake::parquet::row_groups_pruned()`. LAKE-PQT-009 (Timestamp/Decimal, SHOULD) and -013 (bloom filters, MAY) deferred to a later milestone. |
