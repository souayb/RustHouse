# LAKE-TXN: rhlog transaction log — atomic, versioned, attributable commits

| Field | Value |
|-------|-------|
| Status | Implemented |
| Owner | RustHouse maintainers |
| Related ADRs | ADR-0001, ADR-0002 |
| Depends on | CORE-META (atomic tmp+rename pattern), LAKE-PQT (add-action statistics) |
| Milestone | M1 |

## 1. Problem and motivation
`MetadataStore` in `src/meta.rs` overwrites `metadata.json` atomically, so the current state is
consistent but every previous state is lost. A regulator applying BCBS 239 principle 6
(adaptability) and principle 3 (integrity) asks "what did `gold.risk_exposure` contain at the
31 March close, and who changed it since?". MiFID II record-keeping requires that transaction
records are retained unaltered for 5-7 years, and SOX requires each change to financial data to
be attributable to a person or process. A transaction log, where every change is an immutable,
principal-stamped commit and the table state at any version is the fold of commits, gives all
three properties from one structure. `rhlog` is a strict subset of the Delta Lake log so a
later export path exists (ADR-0002).

## 2. Scope
### In scope
* Commit file format and naming under `<table>/_rhlog/`.
* Actions: `protocol`, `metaData`, `add`, `remove`, `commitInfo`.
* Atomic commit via tmp + rename, optimistic concurrency, mandatory principal.
* Snapshot construction by replay; crash safety; checkpoints; `_last_checkpoint`.
* Performance budgets from `specs/architecture/overview.md` §6.

### Out of scope
* Producing Parquet data files (LAKE-PQT). The log records them; it does not write them.
* Delete/merge semantics beyond the `remove` action itself (LAKE-DML).
* Object-store backends; the local filesystem is the only backend in M1.

## 3. Requirements

| ID | Level | Requirement |
|----|-------|-------------|
| LAKE-TXN-001 | MUST | Commit files MUST be named `<version>.json` where `<version>` is the `u64` version zero-padded to 20 digits, stored under `<table>/_rhlog/`; version 0 is the table's creation commit. |
| LAKE-TXN-002 | MUST | A commit file MUST be newline-delimited JSON where each line is exactly one action of type `protocol`, `metaData`, `add`, `remove`, `commitInfo`, or `catalog` (opaque catalog row, LAKE-CAT); unknown action types MUST cause replay to fail with `RustHouseError::CorruptLog`. |
| LAKE-TXN-003 | MUST | Version 0 MUST contain exactly one `protocol`, one `metaData`, and one `commitInfo` action; every later commit MUST contain exactly one `commitInfo`. |
| LAKE-TXN-004 | MUST | A commit MUST be written to a temporary file `<version>.json.<nonce>.tmp` and published to `<version>.json` with an atomic create-if-absent operation; the log MUST never write directly to the final name and MUST never modify an existing commit file. |
| LAKE-TXN-005 | MUST | `commitInfo.principal` MUST be a non-empty string; `TransactionLog::commit` MUST reject a commit without a principal with `RustHouseError::MissingPrincipal` before any file is written. |
| LAKE-TXN-006 | MUST | A `*.json.tmp` file MUST be ignored by replay, so a crash between write and rename leaves the table at its previous version with no error; the next writer MUST overwrite or remove the stale tmp. |
| LAKE-TXN-007 | SHOULD | Every `N` commits (default 10, configurable) the log SHOULD write `<version>.checkpoint.json` containing the folded snapshot and update `_last_checkpoint`; replay SHOULD start from the latest valid checkpoint at or below the requested version. |
| LAKE-TXN-008 | MUST | If rename fails because `<version>.json` already exists, `commit` MUST re-read the log head and retry with the next version up to a configurable limit (default 5), then fail with `RustHouseError::CommitConflict`. The retried commit MUST re-validate against the new snapshot. |
| LAKE-TXN-009 | MAY | The log MAY offer `export_delta(target)` producing a Delta Lake-compatible `_delta_log/` from an rhlog. |
| LAKE-TXN-010 | MUST | `TransactionLog::snapshot(version)` MUST return the set of active data files (`add` minus later `remove`), the schema, and the `metaData` in force at that version, and MUST return `RustHouseError::VersionNotFound` for a version greater than the head. |
| LAKE-TXN-011 | MUST | Replaying the same log twice MUST produce structurally identical snapshots (deterministic fold); action order within a commit MUST be preserved. |
| LAKE-TXN-012 | MUST | `commitInfo` MUST carry `timestamp` (ms since epoch, UTC), `principal`, `operation` ∈ {`CREATE`, `WRITE`, `MERGE`, `DELETE`, `ALTER`, `VACUUM`, `ERASE`, `POLICY`, `CREATE_NAMESPACE`, `CREATE_TABLE`, `DROP_TABLE`}, `operation_parameters` (JSON object), and `client_version`. |
| LAKE-TXN-013 | MUST | Committing 10 000 consecutive versions on a local disk MUST complete with p99 commit latency below 20 ms (excluding Parquet encode), and replaying those 10 000 commits without a checkpoint MUST take under 500 ms. |
| LAKE-TXN-014 | MUST | A `remove` action MUST reference a path previously added and not already removed; otherwise the commit MUST be rejected with `RustHouseError::InvalidAction`. |
| LAKE-TXN-015 | SHOULD | The log SHOULD expose `history(from, to)` returning the `commitInfo` of each version in range, for audit and lineage consumers. |

## 4. Design sketch

```rust
// src/lake/log.rs
#[derive(Copy, Clone, PartialOrd, Ord, Serialize, Deserialize)] pub struct Version(pub u64);

#[derive(Serialize, Deserialize)] #[serde(tag = "action", rename_all = "camelCase")]
pub enum Action {
    Protocol { min_reader_version: u32, min_writer_version: u32 },
    MetaData { id: Uuid, schema: SchemaJson, partition_columns: Vec<String>, classification: Classification, created_time: u64 },
    Add { path: String, size: u64, num_records: u64, stats: FileStats /* min/max/null_count per column, from LAKE-PQT */, partition_values: BTreeMap<String,String> },
    Remove { path: String, deletion_timestamp: u64 },
    CommitInfo { timestamp: u64, principal: String, operation: Operation, operation_parameters: serde_json::Value, client_version: String },
}

pub struct Commit { pub actions: Vec<Action> }         // must contain exactly one CommitInfo
pub struct Snapshot { pub version: Version, pub metadata: MetaDataAction, pub files: Vec<AddAction> }

pub struct TransactionLog { dir: PathBuf, checkpoint_interval: u32, max_retries: u32 }
impl TransactionLog {
    pub async fn create(dir, principal, metadata: MetaDataAction) -> Result<Self>;   // writes version 0
    pub async fn open(dir) -> Result<Self>;
    pub async fn commit(&self, commit: Commit) -> Result<Version>;                     // tmp+rename, retry
    pub async fn head(&self) -> Result<Version>;
    pub async fn snapshot(&self, v: Option<Version>) -> Result<Snapshot>;
    pub async fn history(&self, from: Version, to: Version) -> Result<Vec<CommitInfo>>;
    pub async fn checkpoint(&self) -> Result<()>;
}
```

* File name helper: `format!("{:020}.json", v.0)`; parsing accepts only 20-digit names.
* Fold: iterate versions ascending; `Add` inserts by path; `Remove` deletes by path (error if
  absent, LAKE-TXN-014); `MetaData` replaces; `Protocol` replaces.
* Errors added to `src/error.rs`: `CommitConflict`, `MissingPrincipal`, `CorruptLog(String)`,
  `VersionNotFound(u64)`, `InvalidAction(String)`.
* Metrics: `rusthouse_lake_commits_total{operation,result}`, `rusthouse_lake_commit_seconds`,
  `rusthouse_lake_replay_seconds`.

## 5. Acceptance scenarios

### LAKE-TXN-001: commit file naming
```gherkin
Given a new table created under a temp dir
When  two WRITE commits are made
Then  _rhlog/ contains exactly 00000000000000000000.json, 00000000000000000001.json, 00000000000000000002.json
```

### LAKE-TXN-002: unknown action type is corrupt
```gherkin
Given a valid log at version 1
When  a line {"action":"teleport"} is appended to 00000000000000000001.json and the log is reopened
Then  snapshot() returns Err(CorruptLog)
```

### LAKE-TXN-003: version 0 shape
```gherkin
Given TransactionLog::create is called by principal "svc-ingest"
When  00000000000000000000.json is parsed
Then  it has exactly one protocol, one metaData and one commitInfo line
```

### LAKE-TXN-004: tmp + rename, never in place
```gherkin
Given a log at version 3
When  a commit is made while a filesystem watcher records every create/rename
Then  the only create is 00000000000000000004.json.tmp and the only rename is tmp → 00000000000000000004.json
And   the bytes of 00000000000000000003.json are unchanged
```

### LAKE-TXN-005: principal is mandatory
```gherkin
Given a Commit whose commitInfo.principal is ""
When  commit() is called
Then  Err(MissingPrincipal) is returned
And   no new file, including tmp, exists in _rhlog/
```

### LAKE-TXN-006: crash mid-commit is invisible
```gherkin
Given a log at version 2 and a stray 00000000000000000003.json.tmp with partial content
When  the log is opened and snapshot() is called
Then  the snapshot version is 2 and no error is raised
When  a new commit is made
Then  it lands as version 3 and the stale tmp is gone
```

### LAKE-TXN-008: optimistic concurrency
```gherkin
Given two writers holding the same head (version 7)
When  both commit
Then  one becomes version 8 and the other retries and becomes version 9
And   both add actions are present in snapshot(9)
Given a writer whose retries are exhausted by a competitor committing on every attempt
Then  it returns Err(CommitConflict)
```

### LAKE-TXN-010: snapshot folds add/remove
```gherkin
Given commits: v1 add a.parquet, v2 add b.parquet, v3 remove a.parquet
When  snapshot(v2) and snapshot(v3) are requested
Then  v2 files = {a, b} and v3 files = {b}
When  snapshot(v4) is requested
Then  Err(VersionNotFound(4))
```

### LAKE-TXN-011: deterministic replay
```gherkin
Given a log with 50 commits
When  snapshot(head) is computed twice, once from a fresh open
Then  both snapshots are equal including file order
```

### LAKE-TXN-012: commitInfo fields
```gherkin
Given a WRITE commit by "alice" with parameters {"mode":"append"}
When  the commitInfo line is parsed
Then  timestamp is within 5 s of now, principal = "alice", operation = "WRITE",
      operation_parameters.mode = "append", client_version = env!("CARGO_PKG_VERSION")
```

### LAKE-TXN-013: performance budget
```gherkin
Given a log on local disk
When  10 000 commits each with one add action are made sequentially
Then  p99 commit latency < 20 ms
And   a cold snapshot(head) without checkpoint completes in < 500 ms
```

### LAKE-TXN-014: remove must reference an active file
```gherkin
Given a snapshot with files {a}
When  a commit removing "zzz.parquet" is attempted
Then  Err(InvalidAction) and the head is unchanged
```

## 6. Test plan (write these first)

| Requirement | Test | Location | Kind |
|-------------|------|----------|------|
| LAKE-TXN-001 | `fn lake_txn_001_commit_files_are_zero_padded_versions()` | `tests/spec_lake_txn.rs` | acceptance |
| LAKE-TXN-001 | `fn test_version_filename_roundtrip()` | `src/lake/log.rs` | unit |
| LAKE-TXN-002 | `fn lake_txn_002_unknown_action_is_corrupt_log()` | `tests/spec_lake_txn.rs` | acceptance |
| LAKE-TXN-003 | `fn lake_txn_003_version_zero_has_protocol_metadata_commitinfo()` | `tests/spec_lake_txn.rs` | acceptance |
| LAKE-TXN-004 | `fn lake_txn_004_commit_uses_tmp_then_rename_only()` | `tests/spec_lake_txn.rs` | acceptance |
| LAKE-TXN-005 | `fn lake_txn_005_missing_principal_rejected_before_write()` | `tests/spec_lake_txn.rs` | acceptance |
| LAKE-TXN-006 | `fn lake_txn_006_stale_tmp_ignored_on_replay()` | `tests/spec_lake_txn.rs` | acceptance |
| LAKE-TXN-007 | `fn lake_txn_007_checkpoint_written_every_n_commits()` | `tests/spec_lake_txn.rs` | acceptance |
| LAKE-TXN-008 | `fn lake_txn_008_concurrent_writers_retry_then_conflict()` | `tests/spec_lake_txn.rs` | acceptance |
| LAKE-TXN-010 | `fn lake_txn_010_snapshot_folds_add_and_remove()` | `tests/spec_lake_txn.rs` | acceptance |
| LAKE-TXN-010 | `fn test_fold_actions_property()` | `src/lake/log.rs` | property |
| LAKE-TXN-011 | `fn lake_txn_011_replay_is_deterministic()` | `tests/spec_lake_txn.rs` | acceptance |
| LAKE-TXN-012 | `fn lake_txn_012_commitinfo_carries_required_fields()` | `tests/spec_lake_txn.rs` | acceptance |
| LAKE-TXN-013 | `fn lake_txn_013_commit_and_replay_within_budget()` | `tests/spec_lake_txn.rs` | acceptance (`#[ignore]` in debug, run in CI release) |
| LAKE-TXN-014 | `fn lake_txn_014_remove_of_unknown_file_rejected()` | `tests/spec_lake_txn.rs` | acceptance |
| LAKE-TXN-015 | `fn lake_txn_015_history_returns_commitinfo_range()` | `tests/spec_lake_txn.rs` | acceptance |

## 7. Open questions
* Should `commit` fsync the directory after rename on Linux? Lean: yes, behind a config flag
  default on; measure against LAKE-TXN-013.
* Checkpoint format: Parquet (Delta-like) vs JSON. Parquet keeps the export path clean but
  makes LAKE-TXN-007 depend on LAKE-PQT; JSON is simpler for M1.

## 8. Change log
| Date | Change |
|------|--------|
| 2026-09-02 | Created |
| 2026-09-02 | Implemented in `src/lake/log.rs`. Decisions: checkpoints are JSON (open question 2 resolved: no dependency on LAKE-PQT); temp files carry a per-writer nonce so concurrent drafts never clobber each other; publication uses `hard_link` (create-if-absent) because POSIX `rename` silently replaces, with a checked-rename fallback on filesystems without hard links; `catalog` action added for LAKE-CAT; catalog operations added to `Operation`. LAKE-TXN-008 "retries exhausted" is tested with `max_retries = 0`. LAKE-TXN-013 runs `#[ignore]` in debug, in release in CI. fsync on by default (open question 1). |
| 2026-09-02 | Perf fix for LAKE-TXN-013: `commit` re-validates against the snapshot at `target-1`; the first implementation replayed the whole log for every commit (quadratic). Each `TransactionLog` handle now keeps the newest snapshot it has built and folds only the commits after it, so commit latency is flat and cold replay still starts from the checkpoint. |
