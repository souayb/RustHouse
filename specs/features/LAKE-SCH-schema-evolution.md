# LAKE-SCH: Schema evolution — stable column ids, safe changes only

| Field | Value |
|-------|-------|
| Status | Draft |
| Owner | RustHouse maintainers |
| Related ADRs | ADR-0001, ADR-0002 |
| Depends on | LAKE-TXN, LAKE-PQT, LAKE-TT, GOV-CLS |
| Milestone | M3 |

## 1. Problem and motivation
Today a `Table` in `src/meta.rs` is a fixed `Vec<Column>`; adding a regulatory field (an LEI, a
new Basel risk-weight bucket) means a new table and a migration. BCBS 239 principle 6 requires
risk data to be adaptable to new requirements without loss of history, and MiFID II requires
that records remain readable in their original form for years. Schema evolution therefore has to
be **additive and reversible**: columns are identified by stable ids rather than names so renames
do not break old files; drops hide rather than destroy; every change is an `ALTER` commit with a
principal; and every new column arrives with a classification (GOV-CLS) so PII can never slip in
unlabelled. Unsafe changes (narrowing a type, changing scale of a Decimal) are refused because
they silently alter historical values.

## 2. Scope
### In scope
* Column ids, schema-per-version, `ALTER TABLE` commits.
* Add column, rename column, drop (hide) column, explicitly safe type widening.
* Reading old Parquet files under a newer schema and vice versa (LAKE-TT-010).
* Rejection rules for incompatible changes.

### Out of scope
* Partition-spec evolution.
* Backfilling values into existing files (that is a DML rewrite, LAKE-DML).
* Physical removal of dropped column data (vacuum/erasure, GOV-RET).

## 3. Requirements

| ID | Level | Requirement |
|----|-------|-------------|
| LAKE-SCH-001 | MUST | Every column MUST have a `column_id: u32` assigned at creation from a per-table counter `max_column_id` stored in `metaData`; ids MUST never be reused, including after a drop. |
| LAKE-SCH-002 | MUST | Parquet files MUST record the column id in each field's metadata (`PARQUET:field_id`), and readers MUST resolve columns by id, not by name. |
| LAKE-SCH-003 | MUST | `ALTER TABLE ns.t ADD COLUMN <name> <type> CLASSIFICATION <label> [TAGS (...)]` MUST create a nullable column with a new id and MUST be persisted as a single commit with `operation = ALTER`; the column MUST read as null from files written before the change. |
| LAKE-SCH-004 | MUST | `ADD COLUMN` without a classification MUST be rejected with `RustHouseError::MissingClassification` and MUST NOT create a commit (GOV-CLS-001). |
| LAKE-SCH-005 | MUST | `ALTER TABLE ns.t RENAME COLUMN a TO b` MUST change only the name bound to the column id; files written under the old name MUST still be read correctly, and `AS OF` at an earlier version MUST show the old name. |
| LAKE-SCH-006 | MUST | The schema in force at version `n` MUST be exactly the `metaData` of the latest commit `<= n` that carries one; readers MUST apply that schema for `AS OF VERSION n` (LAKE-TT-010). |
| LAKE-SCH-007 | MUST | `ALTER TABLE ns.t DROP COLUMN c` MUST hide the column from the current schema without removing any data file or rewriting any Parquet; the column MUST remain visible via `AS OF` before the drop. |
| LAKE-SCH-008 | MUST | The only permitted type changes MUST be the safe widenings `Int64 → Decimal(38,18)`, `Float64 → Decimal(38,18)` is NOT permitted (inexact), and `Decimal(p,s) → Decimal(p',s)` with `p' > p` and equal scale; any other change MUST be rejected with `RustHouseError::IncompatibleSchemaChange`. |
| LAKE-SCH-009 | MUST | Adding a column whose name already exists in the current schema (case-insensitive) MUST be rejected with `RustHouseError::ColumnExists`; re-adding a previously dropped name MUST succeed with a **new** column id. |
| LAKE-SCH-010 | MUST | A file written under schema version `n` MUST be readable under any later schema: missing columns read as null, hidden columns are skipped, renamed columns map by id. |
| LAKE-SCH-011 | MUST | Changing a column's `nullable` from `true` to `false` MUST be rejected unless the operation is a DML rewrite that proves no nulls exist (LAKE-DML); `false → true` MUST be allowed. |
| LAKE-SCH-012 | MUST | An `ALTER` commit's `commitInfo.operation_parameters` MUST contain the full before/after column definitions so the audit log (GOV-AUD) can show exactly what changed. |
| LAKE-SCH-013 | SHOULD | The catalog SHOULD reject an `ALTER` that would leave a table with zero visible columns. |
| LAKE-SCH-014 | MAY | `ALTER TABLE ... ALTER COLUMN c SET CLASSIFICATION <label>` MAY be supported here as a schema change; otherwise it belongs to GOV-CLS. |

## 4. Design sketch

```rust
// src/lake/schema.rs
pub struct ColumnDef { pub id: u32, pub name: String, pub column_type: ColumnType, pub nullable: bool, pub classification: Classification, pub tags: Vec<Tag> }
pub struct Schema { pub columns: Vec<ColumnDef>, pub max_column_id: u32 }   // stored in metaData.schema

pub enum SchemaChange {
    AddColumn(ColumnDef /* id assigned by apply */),
    RenameColumn { id: u32, new_name: String },
    DropColumn { id: u32 },
    WidenType { id: u32, to: ColumnType },
    SetNullable { id: u32, nullable: bool },
}

impl Schema {
    pub fn apply(&self, change: &SchemaChange) -> Result<Schema>;      // pure; enforces -004, -008, -009, -011, -013
    pub fn visible(&self) -> impl Iterator<Item=&ColumnDef>;           // excludes dropped
    pub fn by_id(&self, id: u32) -> Option<&ColumnDef>;
}

// src/lake/parquet.rs: reader takes &Schema and resolves fields by PARQUET:field_id, projecting
// nulls for ids not present in the file (LAKE-SCH-010).
// src/meta.rs: `Column` gains `id: u32` and `nullable: bool`; hot-tier segments keep positional col_N.bin
// but the manifest (HYB-ACC) records id → position.
```

* Dropped columns are kept in `Schema.columns` with `dropped_at: Option<Version>` so `visible()`
  and `AS OF` both work from one structure.
* Errors added to `src/error.rs`: `IncompatibleSchemaChange(String)`, `ColumnExists(String)`.
* DDL parsing extends `src/sql/planner.rs` with `LogicalPlan::AlterTable { table, change }`.
* Metrics: `rusthouse_schema_changes_total{kind,result}`.

## 5. Acceptance scenarios

### LAKE-SCH-001: ids are monotonic and never reused
```gherkin
Given a table with columns id(1), name(2)
When  ADD COLUMN lei, then DROP COLUMN name, then ADD COLUMN name
Then  lei has id 3 and the new name has id 4; max_column_id = 4
```

### LAKE-SCH-002: field ids in Parquet
```gherkin
Given a file written for that table
When  its Parquet schema is inspected
Then  each field carries PARQUET:field_id equal to its column id
```

### LAKE-SCH-003: add column is an ALTER commit, reads null on old files
```gherkin
Given a table at version 2 with one data file
When  ADD COLUMN lei String CLASSIFICATION Confidential is executed by "dba"
Then  version 3 exists with operation ALTER and principal "dba"
And   SELECT lei FROM ns.t returns null for every existing row
```

### LAKE-SCH-004: add without classification refused
```gherkin
Given a table at version 3
When  ADD COLUMN ssn String is executed with no CLASSIFICATION clause
Then  Err(MissingClassification("ssn")) and the head is still version 3
```

### LAKE-SCH-005: rename by id
```gherkin
Given column "cpty" (id 5) with data in files
When  RENAME COLUMN cpty TO counterparty
Then  SELECT counterparty returns the same values
And   SELECT cpty AS OF VERSION <before rename> also returns them
```

### LAKE-SCH-006: schema at version
```gherkin
Given metaData changes at versions 0, 3 and 7
When  the schema for version 5 is requested
Then  it equals the metaData written at version 3
```

### LAKE-SCH-007: drop hides, keeps data
```gherkin
Given column "notes" with data
When  DROP COLUMN notes
Then  SELECT * no longer includes notes, no Parquet file changed (checksums equal)
And   SELECT notes ... AS OF VERSION <before drop> returns the data
```

### LAKE-SCH-008: only safe widenings
```gherkin
Given column amount Int64
When  ALTER COLUMN amount TYPE Decimal(38,18)
Then  the change is accepted
When  ALTER COLUMN amount TYPE Int64 (narrowing back) or Float64 → Decimal
Then  Err(IncompatibleSchemaChange)
```

### LAKE-SCH-009: duplicate name refused, re-add after drop gets new id
```gherkin
Given columns id, Name
When  ADD COLUMN name Int64 CLASSIFICATION Internal
Then  Err(ColumnExists("name"))
When  DROP COLUMN Name, then ADD COLUMN name ...
Then  success, and the new column id differs from the dropped one
```

### LAKE-SCH-010: old files under new schema
```gherkin
Given file F written under schema v0 (id, name), schema now v3 (id, counterparty[renamed], lei[new], name dropped)
When  F is read under v3
Then  columns are id, counterparty (from old name), lei = null; name is absent
```

### LAKE-SCH-011: nullable tightening refused
```gherkin
Given column lei nullable
When  ALTER COLUMN lei SET NOT NULL
Then  Err(IncompatibleSchemaChange) unless invoked through LAKE-DML rewrite
When  ALTER COLUMN id DROP NOT NULL
Then  success
```

### LAKE-SCH-012: alter parameters carry before/after
```gherkin
Given RENAME COLUMN cpty TO counterparty
When  the ALTER commitInfo is read
Then  operation_parameters.before.name = "cpty", .after.name = "counterparty", same id
```

## 6. Test plan (write these first)

| Requirement | Test | Location | Kind |
|-------------|------|----------|------|
| LAKE-SCH-001 | `fn lake_sch_001_column_ids_monotonic_never_reused()` | `tests/spec_lake_sch.rs` | acceptance |
| LAKE-SCH-001 | `fn test_schema_apply_assigns_next_id()` | `src/lake/schema.rs` | unit |
| LAKE-SCH-002 | `fn lake_sch_002_parquet_fields_carry_column_ids()` | `tests/spec_lake_sch.rs` | acceptance |
| LAKE-SCH-003 | `fn lake_sch_003_add_column_is_alter_commit_reads_null()` | `tests/spec_lake_sch.rs` | acceptance |
| LAKE-SCH-004 | `fn lake_sch_004_add_column_without_classification_refused()` | `tests/spec_lake_sch.rs` | acceptance (deny-first) |
| LAKE-SCH-005 | `fn lake_sch_005_rename_resolves_by_id_across_versions()` | `tests/spec_lake_sch.rs` | acceptance |
| LAKE-SCH-006 | `fn lake_sch_006_schema_at_version_is_latest_metadata()` | `tests/spec_lake_sch.rs` | acceptance |
| LAKE-SCH-007 | `fn lake_sch_007_drop_hides_without_touching_files()` | `tests/spec_lake_sch.rs` | acceptance |
| LAKE-SCH-008 | `fn lake_sch_008_only_safe_widenings_accepted()` | `tests/spec_lake_sch.rs` | acceptance |
| LAKE-SCH-008 | `fn test_widening_matrix()` | `src/lake/schema.rs` | unit |
| LAKE-SCH-009 | `fn lake_sch_009_duplicate_name_refused_readd_gets_new_id()` | `tests/spec_lake_sch.rs` | acceptance |
| LAKE-SCH-010 | `fn lake_sch_010_old_file_readable_under_new_schema()` | `tests/spec_lake_sch.rs` | acceptance |
| LAKE-SCH-011 | `fn lake_sch_011_nullable_tightening_refused_loosening_ok()` | `tests/spec_lake_sch.rs` | acceptance |
| LAKE-SCH-012 | `fn lake_sch_012_alter_parameters_contain_before_after()` | `tests/spec_lake_sch.rs` | acceptance |
| LAKE-SCH-013 | `fn test_reject_drop_of_last_visible_column()` | `src/lake/schema.rs` | unit |

## 7. Open questions
* Nested types (struct/list) are not in `ColumnType`; if added later, id assignment must cover
  nested fields (Iceberg-style). Out of scope until a spec asks for them.
* Should `Int64 → Float64` be allowed? Rejected here as inexact above 2^53; revisit if a
  consumer needs it and accepts the loss.

## 8. Change log
| Date | Change |
|------|--------|
| 2026-09-02 | Created |
