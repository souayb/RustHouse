# LAKE-WAL: WAL-backed write buffer and memtable

| Field | Value |
|-------|-------|
| Status | Draft |
| Owner | RustHouse maintainers |
| Related ADRs | ADR-0002, ADR-0005, ADR-0007 |
| Depends on | LAKE-TXN, LAKE-PQT, API-SQL, GOV-AUD |
| Milestone | M3 |

## 1. Problem and motivation
Payment, trade and event feeds write small batches continuously. In M1 each write is one
Parquet file and one fsync'd rhlog commit, so a stream of small writes produces thousands of
tiny files and latency bounded by two fsyncs. A WAL-backed buffer keeps durability (an
acknowledged write survives a crash: Constitution 4, DORA resilience) while cutting latency to
one sequential append, and it produces well-sized Parquet files on flush. Attributability is
unchanged: every WAL frame carries the principal and request id, and every flush commit records
the WAL range it covers, so the audit trail (GOV-AUD) is continuous from request to file.

## 2. Scope
### In scope
Per-table WAL segments, memtable, visibility to reads, flush to Parquet + rhlog commit,
recovery, crash tests, configuration, metrics.
### Out of scope
Deletion vectors (LAKE-DML amendment per ADR-0007), replication.

## 3. Requirements

| ID | Level | Requirement |
|----|-------|-------------|
| LAKE-WAL-001 | MUST | A write MUST be acknowledged only after its frame is appended to `<table>/_wal/<seq:020>.wal` and `fsync`ed; a crash after acknowledgement MUST NOT lose the rows (recovered on open). |
| LAKE-WAL-002 | MUST | A WAL frame MUST be `[len u32][crc32c u32][Arrow IPC batch + header {principal, request_id, ts, schema_version}]`; a truncated or CRC-failing tail frame MUST be discarded on recovery and everything before it kept, and the discard MUST be audited. |
| LAKE-WAL-003 | MUST | Rows in the WAL MUST be visible to governed reads immediately through a memtable, with the same Governor treatment as file data; `versions_read` MUST report the table version **and** the WAL high-water mark (`{table, version, wal_seq}`) so results stay reproducible (LAKE-TT-007 extended). |
| LAKE-WAL-004 | MUST | A segment MUST be sealed when it reaches `wal_segment_bytes` (default 64 MiB) or `wal_flush_interval_ms` (default 5 000); a background flush MUST write one Parquet file per sealed segment and one rhlog `WRITE` commit with `operation_parameters.wal_range = [first_seq, last_seq]` and principal `system:flush`, then delete the segment. |
| LAKE-WAL-005 | MUST | Flush MUST be idempotent and crash-safe: a crash after the Parquet file is written but before the commit MUST NOT produce a duplicate on retry (the file is named by segment seq and the commit checks for an existing `add`), and a crash after the commit but before the segment is deleted MUST be repaired on open. |
| LAKE-WAL-006 | MUST | Recovery on open MUST replay unsealed and sealed-but-unflushed segments into the memtable in seq order and MUST complete for 1 GB of WAL in < 10 s on the reference machine. |
| LAKE-WAL-007 | MUST | A read pinned `AS OF VERSION n` MUST NOT include memtable rows (they belong to a future version); a read at head MUST include them. |
| LAKE-WAL-008 | MUST | Writes MUST be validated against the schema and data contracts (GOV-DQ) **before** the WAL append; a rejected batch MUST leave no frame. |
| LAKE-WAL-009 | MUST | Backpressure: if unflushed WAL exceeds `wal_max_bytes` (default 1 GiB), writes MUST be refused with `RustHouseError::Conflict("write buffer full")` rather than growing unbounded. |
| LAKE-WAL-010 | MUST | Crash tests MUST cover: kill during append, kill between fsync and ack, kill during flush before commit, kill after commit before segment delete; after each, open + recovery yields exactly the acknowledged rows, no more, no fewer. |
| LAKE-WAL-011 | SHOULD | The memtable SHOULD be an Arrow `RecordBatch` list so EXE-VEC scans it with the same operators as files. |
| LAKE-WAL-012 | MAY | Group commit MAY batch concurrent writers into one fsync. |

## 4. Design sketch
```rust
// src/lake/wal.rs
pub struct WalWriter { dir, seq: u64, file: File, bytes: u64 }
pub struct WalFrame { principal, request_id, ts, batch: RecordBatch }
pub struct Memtable { frames: Vec<WalFrame>, high_water: u64 }
pub struct Flusher { .. }   // background task per table, ParquetWriter + TransactionLog::commit
```
* `Session::write_rows` appends to the WAL instead of writing Parquet; `read_lake` adds the
  memtable as an extra batch when unpinned.
* `AddFile` gains optional `wal_range`; `VersionRead` gains `wal_seq: Option<u64>`.
* Config: `wal_segment_bytes`, `wal_flush_interval_ms`, `wal_max_bytes`.
* Metrics: `rusthouse_wal_bytes`, `rusthouse_wal_flushes_total{result}`, `rusthouse_wal_recovered_frames_total`.

## 5. Acceptance scenarios

### LAKE-WAL-001: Acknowledged writes survive a crash
```gherkin
Given a table and 1 000 acknowledged single-row writes
When  the process is killed (SIGKILL) and the table is reopened
Then  SELECT count(*) returns 1 000
```

### LAKE-WAL-002: Torn tail frame is discarded, earlier frames kept
```gherkin
Given a WAL segment with 10 frames and a truncated 11th
When  recovery runs
Then  10 rows are visible, the discard is audited, and the next append succeeds
```

### LAKE-WAL-005: Idempotent flush
```gherkin
Given a sealed segment whose Parquet file was written but whose commit was not
When  the flusher retries
Then  exactly one add action for that segment exists and the segment is deleted
```

### LAKE-WAL-007: Time travel excludes the buffer
```gherkin
Given head version 4 and 5 buffered rows
When  SELECT count(*) AS OF VERSION 4 and SELECT count(*) run
Then  the first excludes the 5 rows and the second includes them
```

## 6. Test plan (write these first)

| Requirement | Test | Location | Kind |
|-------------|------|----------|------|
| LAKE-WAL-008 | `fn lake_wal_008_rejected_batch_leaves_no_frame()` | `tests/spec_lake_wal.rs` | acceptance (negative-first) |
| LAKE-WAL-009 | `fn lake_wal_009_backpressure_refuses_writes()` | `tests/spec_lake_wal.rs` | acceptance (negative) |
| LAKE-WAL-001 | `fn lake_wal_001_acknowledged_writes_survive_kill()` | `tests/spec_lake_wal.rs` | acceptance (subprocess kill) |
| LAKE-WAL-002 | `fn lake_wal_002_torn_tail_discarded_and_audited()` | `tests/spec_lake_wal.rs` | acceptance |
| LAKE-WAL-002 | `fn test_frame_roundtrip_and_crc()` | `src/lake/wal.rs` | unit / property |
| LAKE-WAL-003 | `fn lake_wal_003_buffered_rows_visible_and_governed()` | `tests/spec_lake_wal.rs` | acceptance |
| LAKE-WAL-004 | `fn lake_wal_004_flush_produces_one_file_and_commit()` | `tests/spec_lake_wal.rs` | acceptance |
| LAKE-WAL-005 | `fn lake_wal_005_flush_is_idempotent_after_crash()` | `tests/spec_lake_wal.rs` | acceptance |
| LAKE-WAL-006 | `fn lake_wal_006_recovery_budget()` | `tests/spec_lake_wal.rs` | acceptance (`#[ignore]` debug) |
| LAKE-WAL-007 | `fn lake_wal_007_as_of_excludes_memtable()` | `tests/spec_lake_wal.rs` | acceptance |
| LAKE-WAL-010 | `fn lake_wal_010_crash_matrix()` | `tests/spec_lake_wal.rs` | acceptance (subprocess kill matrix) |

## 7. Open questions
* WAL per table vs one WAL per node: per table keeps recovery and flush independent; one WAL
  gives group commit across tables. Lean: per table for M3.
* Should the memtable be included in `AS OF TIMESTAMP now()`? Lean: yes, timestamp reads are
  "as of wall clock", version reads are "as of commit".

## 8. Change log
| Date | Change |
|------|--------|
| 2026-09-02 | Created |
