# CORE-STO: As-built columnar segment storage (hot-tier format)

| Field | Value |
|-------|-------|
| Status | Implemented |
| Owner | RustHouse maintainers |
| Related ADRs | ADR-0001 |
| Depends on | CORE-META |
| Milestone | M0 (as-built) |

## 1. Problem and motivation
Low-latency scans need a compact, column-oriented on-disk format with cheap decompression. This
is the format that becomes the **hot tier** in the hybrid design (HYB-ACC). It is proprietary and
therefore, per Constitution 3, only ever holds data that can be rebuilt from Parquet.

## 2. Scope
### In scope
`ColumnData` encoding/decoding, `ColumnWriter` (LZ4 frame compression), `SegmentWriter`,
`SegmentReader`, statistics collection.
### Out of scope
Nullability, decimals, timestamps (LAKE-PQT), encryption (GOV-ENC).

## 3. Requirements

| ID | Level | Requirement |
|----|-------|-------------|
| CORE-STO-001 | MUST | `Int64` and `Float64` columns encode as little-endian 8-byte values; `String` as `[u32 len][bytes]` sequences; `Boolean` as 8 values per byte, LSB first. |
| CORE-STO-002 | MUST | Decoding with a byte length inconsistent with the declared row count fails with `InvalidData`. |
| CORE-STO-003 | MUST | Column files are LZ4-compressed and round-trip losslessly through write and read. |
| CORE-STO-004 | MUST | A segment write refuses a payload that omits a declared column (`Storage` error). |
| CORE-STO-005 | MUST | A segment write refuses columns with differing row counts (`Storage` error). |
| CORE-STO-006 | MUST | A segment write refuses a column whose data type differs from the declared type (`TypeMismatch`). |
| CORE-STO-007 | MUST | After a write, the returned `Segment` carries row count, total compressed size, and per-column min/max statistics. |
| CORE-STO-008 | MUST | A single column can be read from a segment without reading the others. |
| CORE-STO-009 | SHOULD | Repetitive data compresses to well under 10 % of raw size (compression effectiveness guard). |
| CORE-STO-010 | SHOULD | `SegmentWriter` should take the owning table name instead of hard-coding `"table"` (as-built defect; fix under HYB-ACC). |

## 4. Design sketch
As built: `src/storage/column_writer.rs`, `src/storage/segment.rs`. Column file `i` corresponds
to `columns[i]` by position; there is no per-file header. HYB-ACC adds a `_manifest.json` with
checksums around this format rather than changing it.

## 5. Acceptance scenarios

### CORE-STO-004: missing column refused
```gherkin
Given a segment writer declared with columns [id, name]
When  a payload containing only id is written
Then  the write fails with a Storage error and no files are produced
```

## 6. Test plan (as-built tests)

| Requirement | Test | Location | Kind |
|-------------|------|----------|------|
| CORE-STO-001 | `fn test_column_data_serialization_int64()` | `src/storage/column_writer.rs` | unit |
| CORE-STO-001 | `fn test_column_data_serialization_string()` | `src/storage/column_writer.rs` | unit |
| CORE-STO-001 | `fn test_boolean_serialization()` | `src/storage/column_writer.rs` | unit |
| CORE-STO-002 | `fn test_column_data_int64()` | `src/storage/column_writer.rs` | unit |
| CORE-STO-003 | `fn test_column_writer_write_read()` | `src/storage/column_writer.rs` | unit |
| CORE-STO-003 | `fn test_segment_write_read()` | `src/storage/segment.rs` | unit |
| CORE-STO-004 | `fn test_segment_write_missing_column()` | `src/storage/segment.rs` | unit |
| CORE-STO-005 | `fn test_segment_write_mismatched_rows()` | `src/storage/segment.rs` | unit |
| CORE-STO-006 | `fn core_sto_006_type_mismatch_refused()` | `tests/spec_core_sto.rs` | acceptance (to write) |
| CORE-STO-007 | `fn test_segment_stats_collection()` | `src/storage/segment.rs` | unit |
| CORE-STO-007 | `fn test_column_data_stats_float64()` | `src/storage/column_writer.rs` | unit |
| CORE-STO-008 | `fn test_segment_read_specific_column()` | `src/storage/segment.rs` | unit |
| CORE-STO-009 | `fn test_column_compression()` | `src/storage/column_writer.rs` | unit |

## 7. Open questions
* CORE-STO-006 has no test today; it is the first red test to write under M0 follow-up.

## 8. Change log
| Date | Change |
|------|--------|
| 2026-09-02 | Created from as-built code |
