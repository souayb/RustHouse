# CORE-META: As-built metadata model and atomic metadata store

| Field | Value |
|-------|-------|
| Status | Implemented |
| Owner | RustHouse maintainers |
| Related ADRs | ADR-0001 |
| Depends on | — |
| Milestone | M0 (as-built) |

## 1. Problem and motivation
The warehouse core needs a description of tables, columns, and segments that survives restarts
and is never left half-written. This spec documents what exists today in `src/meta.rs` so that
the lakehouse work (LAKE-CAT, LAKE-TXN) has a precise baseline to replace or wrap.

Under the hybrid architecture (ADR-0001) this store describes **hot-tier** materialisations only.
Authoritative table metadata moves to the catalog and the rhlog transaction log.

## 2. Scope
### In scope
`ColumnType`, `Column`, `SegmentStats`, `Segment`, `Table`, `Metadata`, `MetadataStore`.
### Out of scope
Versioned history, schema evolution, classification (see LAKE-TXN, LAKE-SCH, GOV-CLS).

## 3. Requirements

| ID | Level | Requirement |
|----|-------|-------------|
| CORE-META-001 | MUST | A `Column` has a name and one of the types `Int64`, `Float64`, `String`, `Boolean`. |
| CORE-META-002 | MUST | A `Table` has a name, an ordered column list, a segment list, and a creation timestamp; columns are looked up by name. |
| CORE-META-003 | MUST | A `Segment` records id, owning table, on-disk path, per-column min/max statistics, row count, creation time and size in bytes. |
| CORE-META-004 | MUST | `Metadata` holds tables by name and a monotonically increasing `version` that changes on every mutation. |
| CORE-META-005 | MUST | `MetadataStore::write` is atomic: the file is written to a temporary path, fsynced, then renamed; no `.tmp` file remains afterwards. |
| CORE-META-006 | MUST | `MetadataStore::read` on a missing file returns empty metadata rather than an error. |
| CORE-META-007 | MUST | Metadata round-trips through JSON without loss of tables, columns, or segment statistics. |
| CORE-META-008 | SHOULD | `Metadata::get_table_mut` should not bump `version` when the table does not exist (known as-built defect, tracked for LAKE-CAT). |

## 4. Design sketch
As built: `src/meta.rs`. Serialisation via `serde_json`, I/O via `tokio::fs`. Statistics are
`serde_json::Value` to stay type-agnostic; LAKE-PQT replaces this with typed Parquet statistics.

## 5. Acceptance scenarios

### CORE-META-005: atomic write leaves no temp file
```gherkin
Given an empty data directory
When  metadata with one table is written
Then  metadata.json exists, metadata.tmp does not, and a second write overwrites cleanly
```

### CORE-META-006: read of missing file yields empty metadata
```gherkin
Given a path that does not exist
When  the store is read
Then  the result is Ok with zero tables and version 0
```

## 6. Test plan (as-built tests)

| Requirement | Test | Location | Kind |
|-------------|------|----------|------|
| CORE-META-001 | `fn test_column_creation()` | `src/meta.rs` | unit |
| CORE-META-002 | `fn test_table_creation()` | `src/meta.rs` | unit |
| CORE-META-003 | `fn test_segment_creation()` | `src/meta.rs` | unit |
| CORE-META-003 | `fn test_segment_stats()` | `src/meta.rs` | unit |
| CORE-META-004 | `fn test_metadata_operations()` | `src/meta.rs` | unit |
| CORE-META-005 | `fn test_atomic_write_behavior()` | `src/meta.rs` | unit |
| CORE-META-006 | `fn test_read_nonexistent_metadata()` | `src/meta.rs` | unit |
| CORE-META-007 | `fn test_metadata_store_write_read()` | `src/meta.rs` | unit |

## 7. Open questions
* CORE-META-008 is a real bug (`version` increments on a failed lookup). Fix when LAKE-CAT lands
  or earlier with a red test first.

## 8. Change log
| Date | Change |
|------|--------|
| 2026-09-02 | Created from as-built code |
