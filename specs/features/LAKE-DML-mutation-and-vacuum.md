# LAKE-DML: Mutation (INSERT, DELETE, MERGE) and VACUUM

| Field | Value |
|-------|-------|
| Status | Draft |
| Owner | RustHouse maintainers |
| Related ADRs | ADR-0001, ADR-0002, ADR-0005 |
| Depends on | LAKE-TXN, LAKE-PQT, LAKE-CAT, GOV-AUD, GOV-RET (vacuum gating) |
| Milestone | M3 |

## 1. Problem and motivation
An append-only lake answers "what happened" but not "correct this" or "forget this". Banks need
both:

* **Corrections.** A mis-booked trade or a wrong counterparty rating must be fixed in the Silver
  and Gold tables while the *original* value stays reproducible for BCBS 239 traceability and
  SOX audit. Mutation must therefore be copy-on-write with history retained, never in-place edits.
* **Erasure.** GDPR Art. 17 requires removing a data subject's rows on request. GOV-RET decides
  *what* must go; this spec provides the *mechanism*: a targeted rewrite that produces new data
  files without the subject and a commit that records it.
* **Storage hygiene under retention rules.** MiFID II demands multi-year retention; a `VACUUM`
  that physically deletes files must therefore be gated by the retention horizon and by legal
  holds, and be itself an audited, attributable commit (Constitution 4, 5).

The as-built engine (`src/storage/segment.rs`) can only add segments; `src/meta.rs` overwrites
`metadata.json`. Neither can express a delete with history. This spec adds mutation on top of
the `rhlog` transaction log defined in LAKE-TXN, using `remove` + `add` actions (ADR-0002).

## 2. Scope
### In scope
* `INSERT` (append) producing one new Parquet file and one commit.
* `DELETE ... WHERE <predicate>` as copy-on-write: identify affected files with statistics,
  rewrite them without matching rows, commit `remove`(old) + `add`(new) atomically.
* `MERGE INTO ... ON <key>` upsert (SHOULD).
* `VACUUM [DRY RUN]` gated by retention horizon and legal hold.
* Write/write conflict detection for concurrent commits on the same table.
* An `erase(subject_predicate)` hook used by GOV-RET.
### Out of scope
* `UPDATE ... SET` syntax (expressible as MERGE; may be added later).
* Cross-table transactions.
* Object-store backends (see roadmap "Deferred").

## 3. Requirements

| ID | Level | Requirement |
|----|-------|-------------|
| LAKE-DML-001 | MUST | `INSERT` into a lake table MUST write exactly one new Parquet data file and one rhlog commit containing one `add` action and a `commitInfo` with `operation = "WRITE"` and the calling principal. |
| LAKE-DML-002 | MUST | `DELETE ... WHERE p` MUST be copy-on-write: every data file whose statistics may contain matching rows is rewritten without those rows; the commit MUST contain a `remove` for each rewritten file and an `add` for each replacement, and data files MUST never be modified in place. |
| LAKE-DML-003 | MUST | Data files whose min/max statistics prove that no row matches `p` MUST NOT be rewritten or removed by a `DELETE`. |
| LAKE-DML-004 | MUST | Every mutation MUST be a single atomic commit: a reader observing version `n` or `n+1` MUST never observe a partially applied mutation, including after a crash between file write and commit rename. |
| LAKE-DML-005 | MUST | Rows deleted at version `n` MUST remain readable by a time-travel query `AS OF VERSION n-1` until the removed files are physically deleted by `VACUUM`. |
| LAKE-DML-006 | MUST | `VACUUM` MUST physically delete only files that (a) carry a `remove` action, (b) are not referenced by any snapshot at or after the table's retention horizon (GOV-RET), and (c) belong to a table that is not under legal hold (GOV-RET). If the table is under legal hold, `VACUUM` MUST refuse with `RustHouseError::Denied`. |
| LAKE-DML-007 | MUST | `VACUUM` MUST itself be recorded as an rhlog commit with `operation = "VACUUM"`, listing every deleted file path and byte size, and MUST emit an audit record (GOV-AUD) before any file is unlinked. |
| LAKE-DML-008 | MUST | `VACUUM DRY RUN` MUST report the exact set of files that a real run would delete and MUST NOT delete any file or write any commit. |
| LAKE-DML-009 | MUST | When two writers commit against the same base version, the second commit MUST detect the conflict and either retry (append vs. append, disjoint files) or fail with `RustHouseError::Conflict` (any commit that `remove`s a file the other commit also `remove`d or read for a rewrite). |
| LAKE-DML-010 | MUST | The module MUST expose `erase(table, subject_predicate, principal, reason)` that performs a copy-on-write rewrite like `DELETE` but records `operation = "ERASE"` and the erasure reason (e.g. GDPR request id) in `commitInfo.operationParameters`. |
| LAKE-DML-011 | MUST | A mutation whose `commitInfo` lacks a principal MUST be rejected before any data file is written (Constitution 4, 7). |
| LAKE-DML-012 | SHOULD | `MERGE INTO t USING s ON t.k = s.k WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT` SHOULD be supported as copy-on-write, producing one commit. |
| LAKE-DML-013 | SHOULD | `DELETE` and `MERGE` SHOULD report the number of rows affected and the number of files rewritten in the commit's `operationMetrics`. |
| LAKE-DML-014 | MAY | Deletion vectors (marking rows deleted without rewriting the file) MAY be added later as an optimisation; if added, LAKE-DML-005 and LAKE-DML-006 still hold. |

## 4. Design sketch

Module: `src/lake/dml.rs`.

```rust
pub struct DmlEngine { log: Arc<TableLog>, parquet: ParquetIo, audit: Arc<dyn AuditSink> }

pub enum Mutation { Insert(RecordBatchSet), Delete(Predicate), Merge { source: RecordBatchSet, on: Vec<String> }, Erase { subject: Predicate, reason: String } }

impl DmlEngine {
    pub async fn apply(&self, table: &TableRef, m: Mutation, principal: &Principal) -> Result<CommitResult>;
    pub async fn vacuum(&self, table: &TableRef, principal: &Principal, dry_run: bool) -> Result<VacuumReport>;
}

pub struct VacuumReport { pub candidate_files: Vec<FileRef>, pub deleted_bytes: u64, pub dry_run: bool, pub commit_version: Option<u64> }
```

* File selection for `DELETE` reuses the pruning logic in `src/storage/pruner.rs` (`Predicate`,
  `SegmentPruner::prune_segments`) generalised over `add` action statistics rather than
  `Segment` stats (`src/meta.rs::SegmentStats`), so hot and lake pruning share one algorithm.
* Rewrite: read affected file → filter rows → write `part-<uuid>.parquet` (LAKE-PQT) → build
  `remove` + `add`. Commit through `TableLog::commit(base_version, actions)` (LAKE-TXN); on
  `Conflict`, re-read head, recompute overlap of removed files, and retry only if disjoint.
* `VACUUM` asks `RetentionService::horizon(table)` and `RetentionService::is_on_hold(table)`
  (GOV-RET). Audit record written first; then unlink; then commit `VACUUM` listing deleted files.
  A crash after unlink but before commit leaves dangling `remove` actions; a subsequent `VACUUM`
  reconciles by tolerating `NotFound` on unlink.
* New error variants in `src/error.rs`: `Conflict(String)`, `Denied(String)`.
* Metrics: `rusthouse_dml_commits_total{operation}`, `rusthouse_dml_files_rewritten_total`,
  `rusthouse_vacuum_bytes_deleted_total`.

## 5. Acceptance scenarios

### LAKE-DML-001: Insert produces one file and one commit
```gherkin
Given a lake table "silver.trades" at version 3 with 2 data files
When  principal "etl_svc" inserts 100 rows
Then  the table is at version 4
And   exactly one new Parquet file exists under the table directory
And   commit 4 contains one add action and commitInfo{operation:"WRITE", principal:"etl_svc"}
```

### LAKE-DML-002: Delete is copy-on-write
```gherkin
Given a lake table with file A (ids 1-100) and file B (ids 101-200) at version 1
When  principal "ops" executes DELETE WHERE id = 150
Then  version 2 contains remove(B) and add(B') where B' has 99 rows and no id 150
And   file B still exists on disk with its original bytes unchanged
And   file A is untouched and not mentioned in commit 2
```

### LAKE-DML-003: Pruned files are not rewritten
```gherkin
Given files A (ids 1-100), B (101-200), C (201-300)
When  DELETE WHERE id > 250 is executed
Then  only C is removed and replaced; A and B appear in no action of the commit
```

### LAKE-DML-004: Mutation is atomic across a crash
```gherkin
Given a DELETE that has written its replacement Parquet file and the commit tmp file
When  the process is killed before the rename of <v+1>.json.tmp
And   the table is reopened
Then  the snapshot is still version v with the original files
And   the orphan replacement file is reported by VACUUM DRY RUN as unreferenced
```

### LAKE-DML-005: Deleted rows visible through time travel
```gherkin
Given DELETE WHERE id = 150 committed as version 2
When  a query runs SELECT count(*) WHERE id = 150 AS OF VERSION 1
Then  it returns 1
And   the same query without AS OF returns 0
```

### LAKE-DML-006: Vacuum respects retention and legal hold
```gherkin
Given a table with retention horizon "versions newer than 30 days must be kept"
And   file B removed at a version committed 10 days ago
When  VACUUM runs
Then  B is not deleted
Given the same table now placed under legal hold by "compliance_officer"
When  VACUUM runs
Then  it fails with RustHouseError::Denied and no file is deleted
```

### LAKE-DML-007: Vacuum is a committed, audited operation
```gherkin
Given file B removed at a version older than the retention horizon and no legal hold
When  VACUUM runs as principal "janitor"
Then  an audit record with operation VACUUM and the file list exists with a timestamp earlier than the unlink
And   the new head commit has operation "VACUUM" listing B and its byte size
```

### LAKE-DML-008: Dry run is side-effect free
```gherkin
Given a table with 3 vacuumable files
When  VACUUM DRY RUN runs
Then  the report lists exactly those 3 files
And   all 3 files still exist and the table version is unchanged
```

### LAKE-DML-009: Concurrent writers
```gherkin
Given two DmlEngine instances both at base version 5
When  writer 1 commits INSERT and writer 2 commits INSERT concurrently
Then  both succeed with versions 6 and 7 in some order and both files are visible at version 7
When  writer 1 commits DELETE WHERE id=1 (rewrites file A) and writer 2 commits DELETE WHERE id=2 (also rewrites file A) from the same base
Then  exactly one succeeds and the other fails with RustHouseError::Conflict
```

### LAKE-DML-010: Erasure hook
```gherkin
Given a table containing rows for customer_id 42 in two files
When  erase(table, customer_id = 42, principal "dpo", reason "GDPR-REQ-2026-0117") is called
Then  the new head has operation "ERASE" and operationParameters.reason = "GDPR-REQ-2026-0117"
And   no row with customer_id 42 is readable at the head version
```

### LAKE-DML-011: Missing principal is refused early
```gherkin
Given a mutation request whose principal is absent
When  apply() is called
Then  it fails with RustHouseError::Denied
And   no new file exists under the table directory and the version is unchanged
```

## 6. Test plan (write these first)

| Requirement | Test | Location | Kind |
|-------------|------|----------|------|
| LAKE-DML-001 | `fn lake_dml_001_insert_writes_one_file_one_commit()` | `tests/spec_lake_dml.rs` | acceptance |
| LAKE-DML-002 | `fn lake_dml_002_delete_is_copy_on_write()` | `tests/spec_lake_dml.rs` | acceptance |
| LAKE-DML-002 | `fn test_rewrite_filters_rows_and_keeps_original_bytes()` | `src/lake/dml.rs` | unit |
| LAKE-DML-003 | `fn lake_dml_003_delete_skips_pruned_files()` | `tests/spec_lake_dml.rs` | acceptance |
| LAKE-DML-003 | `fn test_affected_files_from_add_stats()` | `src/lake/dml.rs` | unit |
| LAKE-DML-004 | `fn lake_dml_004_crash_before_rename_leaves_old_snapshot()` | `tests/spec_lake_dml.rs` | acceptance |
| LAKE-DML-005 | `fn lake_dml_005_deleted_rows_visible_as_of_previous_version()` | `tests/spec_lake_dml.rs` | acceptance |
| LAKE-DML-006 | `fn lake_dml_006_vacuum_respects_retention_horizon()` | `tests/spec_lake_dml.rs` | acceptance |
| LAKE-DML-006 | `fn lake_dml_006_vacuum_denied_under_legal_hold()` | `tests/spec_lake_dml.rs` | acceptance (negative) |
| LAKE-DML-007 | `fn lake_dml_007_vacuum_is_audited_commit()` | `tests/spec_lake_dml.rs` | acceptance |
| LAKE-DML-008 | `fn lake_dml_008_vacuum_dry_run_has_no_side_effects()` | `tests/spec_lake_dml.rs` | acceptance |
| LAKE-DML-009 | `fn lake_dml_009_concurrent_appends_both_succeed()` | `tests/spec_lake_dml.rs` | acceptance |
| LAKE-DML-009 | `fn lake_dml_009_overlapping_deletes_conflict()` | `tests/spec_lake_dml.rs` | acceptance (negative) |
| LAKE-DML-010 | `fn lake_dml_010_erase_records_reason_and_removes_subject()` | `tests/spec_lake_dml.rs` | acceptance |
| LAKE-DML-011 | `fn lake_dml_011_missing_principal_rejected_before_write()` | `tests/spec_lake_dml.rs` | acceptance (negative) |
| LAKE-DML-012 | `fn lake_dml_012_merge_upserts_by_key()` | `tests/spec_lake_dml.rs` | acceptance |
| LAKE-DML-013 | `fn lake_dml_013_operation_metrics_reported()` | `tests/spec_lake_dml.rs` | acceptance |
| LAKE-DML-002/003 | `proptest: delete(p) ∘ read == read.filter(!p)` for random predicates | `tests/spec_lake_dml.rs` | property |

## 7. Open questions
* Should `VACUUM` require a dedicated role (`janitor`) or is `ALTER` grant on the table enough?
  Leaning: dedicated privilege `VACUUM` in GOV-ACC.
* Erasure of rows that also live in Bronze raw files: does `erase` cascade across layers via
  lineage (GOV-LIN), or does GOV-RET call `erase` per table? Leaning: GOV-RET orchestrates.
* Conflict rule for INSERT vs DELETE on the same base: append never conflicts with delete
  (disjoint files). Confirm this is acceptable for MERGE semantics.

## 8. Change log
| Date | Change |
|------|--------|
| 2026-09-02 | Created |
