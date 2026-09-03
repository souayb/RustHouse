# LAKE-CAT: Catalog — namespaces, table registry, policy storage

| Field | Value |
|-------|-------|
| Status | Implemented |
| Owner | RustHouse maintainers |
| Related ADRs | ADR-0001, ADR-0002, ADR-0005 |
| Depends on | LAKE-TXN |
| Milestone | M1 |

## 1. Problem and motivation
The as-built engine keeps every table in one `metadata.json` (`src/meta.rs`, `Metadata`,
`MetadataStore`). It has no namespaces, no history, and no place to store anything but tables.
A lakehouse needs a **catalog**: the authoritative registry of namespaces (medallion layers),
tables, and the governance objects that hang off them (classifications, grants, policies,
lineage edges). Banks are audited on the catalog itself: BCBS 239 principle 3 (accuracy and
integrity) requires a single inventory of risk data with owners, and SOX requires that changes
to that inventory are attributable and reviewable. Storing the catalog as an rhlog table gives
it the same versioned, attributable history as any data table, so "what tables and policies
existed on date X" is a time-travel query, not archaeology.

## 2. Scope
### In scope
* Namespaces: the three medallion layers `bronze`, `silver`, `gold` plus operator-defined ones.
* Table registry: create, drop (logical), list, describe, resolve `layer.table` → `TableRef`.
* Catalog persistence as an rhlog table at `<data_dir>/catalog/_rhlog/`.
* Catalog "system tables" for governance objects: `catalog.tables`, `catalog.columns`,
  `catalog.grants`, `catalog.policies`, `catalog.lineage`, written by the GOV-* specs.
* Atomic catalog updates with a monotonically increasing catalog version.
* Fail-closed resolution: unknown namespace/table is an error, never an implicit create.

### Out of scope
* Contents and semantics of grants/policies/lineage rows (GOV-ACC, GOV-LIN own those).
* Cross-node catalog replication.
* Migration tooling from `metadata.json` (hot-tier `Metadata` stays as-is for CORE-*).

## 3. Requirements

| ID | Level | Requirement |
|----|-------|-------------|
| LAKE-CAT-001 | MUST | The catalog MUST expose the built-in namespaces `bronze`, `silver`, `gold` on first open without any DDL. |
| LAKE-CAT-002 | MUST | `Catalog::create_namespace(principal, name)` MUST create a custom namespace; names are `[a-z][a-z0-9_]{0,62}`; duplicates MUST return `RustHouseError::NamespaceExists`. |
| LAKE-CAT-003 | MUST | `Catalog::create_table(principal, TableSpec)` MUST register a table under `namespace.table`, create its rhlog directory, and write version 0 of the table's log in the same logical operation; a failure at any step MUST leave neither a catalog entry nor a partial table directory visible. |
| LAKE-CAT-004 | MUST | `Catalog::resolve("layer.table")` MUST return a `TableRef { namespace, name, table_id, path }` for an existing table and `RustHouseError::TableNotFound` for an unknown one. Resolution MUST never create a table as a side effect. |
| LAKE-CAT-005 | MUST | A bare table name without a namespace MUST be rejected with `RustHouseError::AmbiguousTable` unless the session default namespace is set. |
| LAKE-CAT-006 | MUST | The catalog MUST itself be persisted as an rhlog table (`<data_dir>/catalog/_rhlog/`), so every create/drop/alter is a commit with a mandatory principal (LAKE-TXN-005). |
| LAKE-CAT-007 | MUST | Every catalog mutation MUST increase the catalog version by exactly one and MUST be atomic: a concurrent reader observes either the old or the new catalog, never a mix. |
| LAKE-CAT-008 | MUST | `Catalog::drop_table` MUST be logical: the entry is marked dropped at catalog version `v`, remains resolvable via `AS OF VERSION v-1` (LAKE-TT), and its data files MUST NOT be deleted by the drop itself (only vacuum/erasure may, GOV-RET). |
| LAKE-CAT-009 | MUST | `Catalog::list_tables(namespace)` and `Catalog::describe(table_ref)` MUST return the table id, schema with column ids and classifications, current version, created time, and owner principal. |
| LAKE-CAT-010 | MUST | The catalog MUST expose system tables `catalog.tables`, `catalog.columns`, `catalog.grants`, `catalog.policies`, `catalog.lineage` as queryable rhlog tables so governance specs store their objects as catalog rows, not as ad-hoc files. |
| LAKE-CAT-011 | MUST | Creating a table whose spec lacks a table-level classification or a classification on any column MUST be rejected with `RustHouseError::MissingClassification` (fail closed; GOV-CLS-001). |
| LAKE-CAT-012 | MUST | Reopening a `data_dir` MUST rebuild the in-memory catalog solely by replaying `catalog/_rhlog/`; no other file is consulted for the table inventory. |
| LAKE-CAT-013 | SHOULD | The catalog SHOULD cache the current snapshot in memory keyed by catalog version and invalidate on commit, so `resolve` is O(1) on the hot path. |
| LAKE-CAT-014 | MAY | The catalog MAY expose a `catalog.namespaces` system table with owner and description. |

## 4. Design sketch

```rust
// src/catalog/mod.rs
pub struct Catalog { root: PathBuf, log: TransactionLog /* catalog/_rhlog */, cache: RwLock<CatalogSnapshot> }

pub struct TableRef { pub namespace: String, pub name: String, pub table_id: Uuid, pub path: PathBuf }

pub struct TableSpec {
    pub namespace: String,
    pub name: String,
    pub columns: Vec<ColumnSpec>,         // ColumnSpec = meta::Column + column_id + classification
    pub classification: Classification,   // table-level, GOV-CLS
    pub partition_by: Vec<String>,
}

pub struct CatalogSnapshot { version: u64, namespaces: HashMap<String, Namespace>, tables: HashMap<(String,String), TableEntry> }

impl Catalog {
    pub async fn open(root: impl AsRef<Path>) -> Result<Self>;                 // LAKE-CAT-001, -012
    pub async fn create_namespace(&self, p: &Principal, name: &str) -> Result<u64>;
    pub async fn create_table(&self, p: &Principal, spec: TableSpec) -> Result<TableRef>;
    pub async fn drop_table(&self, p: &Principal, r: &TableRef) -> Result<u64>;
    pub fn resolve(&self, qualified: &str, default_ns: Option<&str>) -> Result<TableRef>;
    pub fn list_tables(&self, namespace: &str) -> Result<Vec<TableSummary>>;
    pub fn describe(&self, r: &TableRef) -> Result<TableDescription>;
    pub fn version(&self) -> u64;
}
```

* Catalog rows are stored as actions in the catalog rhlog: `metaData` for the catalog schema,
  and data-carrying `add` files (Parquet, LAKE-PQT) for `catalog.tables` etc. For M1 a simpler
  `catalogEntry` JSON action inside the commit is acceptable as long as LAKE-CAT-012 holds.
* `create_table` sequence: (1) write table `_rhlog/00000000000000000000.json.tmp`, (2) commit
  catalog entry, (3) rename table log tmp → final. If (2) fails, delete (1). If the process dies
  between (2) and (3), `open` detects a catalog entry whose table log has only a `.tmp` and
  completes the rename (idempotent repair) — see LAKE-TXN-006 for tmp semantics.
* New error variants in `src/error.rs`: `NamespaceExists`, `NamespaceNotFound`,
  `AmbiguousTable`, `TableExists`, `MissingClassification`, `InvalidIdentifier`.
* Metrics: `rusthouse_catalog_version`, `rusthouse_catalog_resolve_total{result}`.

## 5. Acceptance scenarios

### LAKE-CAT-001: built-in layers exist
```gherkin
Given an empty data directory
When  the catalog is opened
Then  list_namespaces() returns exactly ["bronze","silver","gold"]
And   catalog version is 0
```

### LAKE-CAT-002: custom namespace, duplicate rejected
```gherkin
Given an open catalog and principal "dba"
When  create_namespace("risk") is called
Then  catalog version becomes 1 and "risk" is listed
When  create_namespace("risk") is called again
Then  the result is Err(NamespaceExists) and the version stays 1
```

### LAKE-CAT-003: create table is all-or-nothing
```gherkin
Given an open catalog and a valid TableSpec for gold.positions
When  create_table is called with a filesystem fault injected after the table log tmp is written
Then  resolve("gold.positions") returns Err(TableNotFound)
And   no directory gold/positions/ with a final _rhlog/00000000000000000000.json exists
```

### LAKE-CAT-004: resolve is read-only and fails closed
```gherkin
Given a catalog with gold.positions
When  resolve("gold.positions") is called
Then  a TableRef with namespace "gold", name "positions" and a stable table_id is returned
When  resolve("gold.missing") is called
Then  Err(TableNotFound) is returned and catalog version is unchanged
```

### LAKE-CAT-005: bare name is ambiguous
```gherkin
Given tables bronze.trades and silver.trades
When  resolve("trades", default_ns = None) is called
Then  Err(AmbiguousTable) is returned
When  resolve("trades", default_ns = Some("silver")) is called
Then  silver.trades is returned
```

### LAKE-CAT-006: catalog is an rhlog table with a principal
```gherkin
Given an open catalog
When  create_table is called by principal "alice"
Then  a new file catalog/_rhlog/<version>.json exists
And   its commitInfo.principal is "alice" and operation is "CREATE_TABLE"
```

### LAKE-CAT-007: atomic version bump
```gherkin
Given a catalog at version 5 and 8 concurrent create_table calls for distinct tables
When  all calls complete
Then  the catalog version is 13
And   every reader that ran concurrently saw a snapshot whose table count equals its version minus 5
```

### LAKE-CAT-008: drop is logical
```gherkin
Given gold.positions created at catalog version 3 with one data file
When  drop_table is called at version 4
Then  resolve("gold.positions") returns Err(TableNotFound)
And   the Parquet data file still exists on disk
And   resolve_as_of("gold.positions", version 3) returns the TableRef
```

### LAKE-CAT-009: describe returns governance-relevant metadata
```gherkin
Given gold.positions with columns (account_id Restricted/PII, notional Confidential)
When  describe(gold.positions) is called
Then  the result contains table_id, owner "alice", current version, created_at,
      and each column with its column_id, ColumnType and classification
```

### LAKE-CAT-010: system tables are queryable
```gherkin
Given an open catalog with two tables
When  the planner scans catalog.tables
Then  two rows are returned with namespace, name, table_id, classification, owner
And   catalog.grants, catalog.policies, catalog.lineage resolve without error (possibly empty)
```

### LAKE-CAT-011: missing classification refused
```gherkin
Given a TableSpec where one column has no classification
When  create_table is called
Then  Err(MissingClassification("<column>")) is returned
And   the catalog version is unchanged and no table directory was created
```

### LAKE-CAT-012: reopen rebuilds from log only
```gherkin
Given a catalog with three tables in two namespaces, then the process exits
When  Catalog::open is called on the same data_dir
Then  the same three tables resolve with the same table_ids
And   deleting any file outside catalog/_rhlog/ before reopen does not change the inventory
```

## 6. Test plan (write these first)

| Requirement | Test | Location | Kind |
|-------------|------|----------|------|
| LAKE-CAT-001 | `fn lake_cat_001_builtin_namespaces_exist()` | `tests/spec_lake_cat.rs` | acceptance |
| LAKE-CAT-002 | `fn lake_cat_002_custom_namespace_and_duplicate_rejected()` | `tests/spec_lake_cat.rs` | acceptance |
| LAKE-CAT-002 | `fn test_namespace_identifier_validation()` | `src/catalog/mod.rs` | unit |
| LAKE-CAT-003 | `fn lake_cat_003_create_table_is_atomic_under_fault()` | `tests/spec_lake_cat.rs` | acceptance |
| LAKE-CAT-004 | `fn lake_cat_004_resolve_existing_and_missing()` | `tests/spec_lake_cat.rs` | acceptance |
| LAKE-CAT-005 | `fn lake_cat_005_bare_name_ambiguous_without_default()` | `tests/spec_lake_cat.rs` | acceptance |
| LAKE-CAT-006 | `fn lake_cat_006_catalog_commit_carries_principal()` | `tests/spec_lake_cat.rs` | acceptance |
| LAKE-CAT-007 | `fn lake_cat_007_concurrent_creates_bump_version_atomically()` | `tests/spec_lake_cat.rs` | acceptance |
| LAKE-CAT-008 | `fn lake_cat_008_drop_is_logical_and_time_travelable()` | `tests/spec_lake_cat.rs` | acceptance |
| LAKE-CAT-009 | `fn lake_cat_009_describe_returns_ids_and_classifications()` | `tests/spec_lake_cat.rs` | acceptance |
| LAKE-CAT-010 | `fn lake_cat_010_system_tables_queryable()` | `tests/spec_lake_cat.rs` | acceptance |
| LAKE-CAT-011 | `fn lake_cat_011_missing_classification_refused()` | `tests/spec_lake_cat.rs` | acceptance |
| LAKE-CAT-012 | `fn lake_cat_012_reopen_rebuilds_from_log_only()` | `tests/spec_lake_cat.rs` | acceptance |
| LAKE-CAT-013 | `fn test_snapshot_cache_invalidated_on_commit()` | `src/catalog/mod.rs` | unit |

## 7. Open questions
* Should the hot-tier `Metadata` (`src/meta.rs`) be folded into the catalog in M4 (HYB-ACC), or
  remain a per-materialisation manifest? Current lean: manifest, catalog stays authoritative.
* Owner transfer of a table: DDL `ALTER TABLE ... OWNER TO` — M2 with GOV-ACC?

## 8. Change log
| Date | Change |
|------|--------|
| 2026-09-02 | Created |
| 2026-09-02 | Implemented in `src/catalog/mod.rs`. Catalog rows are typed `CatalogEntry` values carried by the rhlog `catalog` action (one entry per commit). Table creation stages the table log in `<ns>/.staging-<name>-<id>/`, commits the catalog entry, then renames the directory; `open` repairs a crash between commit and rename. LAKE-CAT-003 fault injection is a public `inject_fault` hook. System tables (LAKE-CAT-010) are columnar views over the catalog snapshot; `catalog.grants/policies/lineage` are empty until GOV-ACC / GOV-LIN add their entry kinds. `Principal` lives in `catalog` until API-SQL introduces `src/auth.rs`. |
