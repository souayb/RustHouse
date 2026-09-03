# Error model and type additions implied by the specs

Consolidated from the feature specs so `src/error.rs` and `src/meta.rs` evolve in one place.
Add a variant **only when the first red test needs it**.

## `RustHouseError` variants to add

| Variant | Introduced by | HTTP status (API-SQL) |
|---------|---------------|-----------------------|
| `Denied(String)` | GOV-ACC, GOV-CLS, GOV-ENC | 403 |
| `Unauthenticated` | API-SQL | 401 |
| `AuditUnavailable(String)` | GOV-AUD | 503 |
| `LineageUnavailable(String)` | GOV-LIN | 503 |
| `MissingClassification(String)` | GOV-CLS, LAKE-SCH | 400 |
| `ClassificationLowering(String)` | GOV-CLS | 403 |
| `MissingPrincipal` | LAKE-TXN | 400 |
| `CommitConflict { table, expected, found }` | LAKE-TXN, LAKE-DML | 409 |
| `Conflict(String)` | LAKE-DML (overlapping rewrite) | 409 |
| `CorruptLog(String)` | LAKE-TXN | 500 |
| `InvalidAction(String)` | LAKE-TXN | 500 |
| `VersionNotFound { table, version }` | LAKE-TT | 404 |
| `VersionVacuumed { table, version }` | LAKE-TT, GOV-RET | 410 |
| `NamespaceExists` / `NamespaceNotFound` / `TableExists` / `AmbiguousTable` / `InvalidIdentifier` | LAKE-CAT | 409 / 404 / 409 / 400 / 400 |
| `IncompatibleSchemaChange(String)` / `ColumnExists(String)` | LAKE-SCH | 400 / 409 |
| `Parquet(String)` | LAKE-PQT | 500 |
| `LegalHoldActive { table, hold }` | GOV-RET | 423 |
| `RetentionViolation(String)` | GOV-RET | 403 |
| `ContractViolation { rule, rows }` | GOV-DQ | 422 |
| `KeyUnavailable(String)` | GOV-ENC | 403 |

Existing variants keep their meaning: `TableNotFound` → 404, `SqlParse` → 400,
`TypeMismatch`/`InvalidData` → 400, `Io`/`Storage`/`Internal` → 500.

## Added in M2
`plan_hash: Option<String>` on `AuditRecord` (absent on pre-M2 records, so old chains still verify);
`rows: Vec<Value>` on `DdlResult`; `Operation::Grant` / `Operation::Revoke` in the rhlog.
Roles in use: `security_admin` (grants and policies), `classification_admin` (reclassification),
`metrics_reader`. The table owner holds every privilege on their own table.

## `src/meta.rs` additions

| Change | Introduced by |
|--------|---------------|
| `ColumnType::Timestamp` (UTC, microseconds) | LAKE-PQT (SHOULD) |
| `ColumnType::Decimal { precision: u8, scale: u8 }` (default 38,18) | LAKE-PQT (SHOULD) |
| `Column.id: u32` stable identifier, never reused | LAKE-SCH |
| `Column.nullable: bool` | LAKE-PQT, LAKE-SCH |
| `Column.classification: Classification { level, tags }` | GOV-CLS |
| `ColumnData::*` gain a validity bitmap or an `Option`-based nullable variant | LAKE-PQT-004, GOV-DQ-006 |

## Roles referenced by governance specs

`analyst` (example), `classification_admin`, `security_admin`, `auditor_admin`,
`legal_hold_admin`, `retention_override`. Roles are catalog data (GOV-ACC), not code.
