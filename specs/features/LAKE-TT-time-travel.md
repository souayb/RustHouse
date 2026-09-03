# LAKE-TT: Time travel — query any table as it was at a version or timestamp

| Field | Value |
|-------|-------|
| Status | Draft |
| Owner | RustHouse maintainers |
| Related ADRs | ADR-0001, ADR-0002, ADR-0005 |
| Depends on | LAKE-TXN, LAKE-CAT, CORE-SQL, GOV-AUD |
| Milestone | M1 |

## 1. Problem and motivation
Regulatory reporting is retrospective. A BCBS 239 reconciliation, a MiFID II transaction-report
resubmission, or a SOX control test all ask the same thing: "run the report exactly as it would
have run on date D". With a mutable warehouse this requires frozen copies of every table on every
reporting date. With an rhlog table it is a query clause: `AS OF VERSION n` or
`AS OF TIMESTAMP t`. Reproducibility (Constitution 6) additionally requires that every query
response states which versions it read, so a report can be pinned and re-run byte-for-byte later,
and that the audit log (GOV-AUD) records the same versions so the evidence trail is complete.

## 2. Scope
### In scope
* SQL syntax `FROM <ns>.<table> AS OF VERSION <n>` and `AS OF TIMESTAMP '<iso8601>'`.
* Planner extension: `LogicalPlan::Scan` carries `as_of: Option<AsOf>`.
* Resolution of timestamp → version; error cases; `versions_read` in every response.
* Interplay with logical drop (LAKE-CAT-008) and vacuum (GOV-RET).

### Out of scope
* Restoring a table to an old version (`RESTORE`), which is a DML operation (LAKE-DML).
* Time travel on the hot tier; hot-tier materialisations are pinned to one version (HYB-ACC).

## 3. Requirements

| ID | Level | Requirement |
|----|-------|-------------|
| LAKE-TT-001 | MUST | The parser and planner MUST accept `SELECT ... FROM ns.t AS OF VERSION <u64>` and produce `LogicalPlan::Scan { as_of: Some(AsOf::Version(n)) }`. |
| LAKE-TT-002 | MUST | The parser and planner MUST accept `AS OF TIMESTAMP '<RFC 3339>'` and produce `AsOf::Timestamp(ms_utc)`; a timestamp without a timezone offset MUST be rejected with `RustHouseError::SqlParse`. |
| LAKE-TT-003 | MUST | `AsOf::Timestamp(t)` MUST resolve to the greatest version whose `commitInfo.timestamp <= t`; if `t` precedes version 0's timestamp the query MUST fail with `RustHouseError::VersionNotFound`. |
| LAKE-TT-004 | MUST | `AsOf::Version(n)` where `n` is greater than the table head MUST fail with `RustHouseError::VersionNotFound(n)`. |
| LAKE-TT-005 | MUST | A scan without `AS OF` MUST read the table head as resolved once at plan time; all scans in one statement MUST use a single consistent catalog version. |
| LAKE-TT-006 | MUST | Every query response MUST include `versions_read: [{table: "ns.t", version: n}]` for each table scanned, whether or not `AS OF` was used. |
| LAKE-TT-007 | MUST | Re-running a query with `AS OF VERSION n` after further commits to the same table MUST return a byte-identical result body (same rows, same order under an `ORDER BY`, same encoding). |
| LAKE-TT-008 | MUST | If the snapshot at the requested version references a data file that has been physically removed by vacuum or erasure, the query MUST fail with `RustHouseError::VersionVacuumed { table, version }` and MUST NOT return partial results. |
| LAKE-TT-009 | MUST | The audit record for a query (GOV-AUD) MUST contain the same `versions_read` list that was returned to the caller. |
| LAKE-TT-010 | MUST | Time travel MUST apply the schema in force at the requested version (LAKE-SCH-006): a column added after version `n` MUST NOT appear in `AS OF VERSION n` results. |
| LAKE-TT-011 | MUST | Governance policies (GOV-ACC) MUST be evaluated at the **current** policy version regardless of the data version requested; time travel MUST NOT be usable to read data under an older, more permissive policy. |
| LAKE-TT-012 | SHOULD | A logically dropped table (LAKE-CAT-008) SHOULD remain queryable with `AS OF VERSION` at a catalog version preceding the drop. |
| LAKE-TT-013 | MAY | The planner MAY support `AS OF` on the catalog system tables themselves (e.g. `catalog.policies AS OF TIMESTAMP ...`). |

## 4. Design sketch

```rust
// src/sql/planner.rs
#[derive(Debug, Clone, PartialEq)]
pub enum AsOf { Version(u64), Timestamp(i64 /* ms since epoch UTC */) }

pub enum LogicalPlan {
    Scan { table: String, columns: Vec<String>, as_of: Option<AsOf> },   // as_of added
    ...
}

// src/lake/log.rs
impl TransactionLog {
    pub async fn resolve_as_of(&self, as_of: &AsOf) -> Result<Version>;   // LAKE-TT-003, -004
}

// query response (src/session.rs / http_api.rs)
pub struct QueryResponse { pub rows: Vec<..>, pub elapsed_ms: u64, pub versions_read: Vec<VersionRead> }
pub struct VersionRead { pub table: String, pub version: u64 }
```

* `sqlparser` 0.43 does not parse `AS OF`; implement a small pre-parser that extracts
  `AS OF VERSION n | AS OF TIMESTAMP '...'` after a table factor, or upgrade `sqlparser` to a
  version with `TableVersion`/temporal support. Decision recorded in the red phase.
* Timestamp parsing: `time` or `chrono` crate, RFC 3339 only, offset required.
* Errors added to `src/error.rs`: `VersionVacuumed { table: String, version: u64 }`; reuse
  `VersionNotFound` from LAKE-TXN.
* Metrics: `rusthouse_time_travel_queries_total{kind="version"|"timestamp"}`.

## 5. Acceptance scenarios

### LAKE-TT-001: AS OF VERSION parses
```gherkin
Given the SQL "SELECT id FROM gold.positions AS OF VERSION 3"
When  it is parsed and planned
Then  the plan is Project([id]) over Scan{table:"gold.positions", as_of: Some(Version(3))}
```

### LAKE-TT-002: AS OF TIMESTAMP requires an offset
```gherkin
Given "SELECT * FROM gold.positions AS OF TIMESTAMP '2026-03-31T23:59:59Z'"
When  planned
Then  as_of = Some(Timestamp(ms of that instant))
Given "... AS OF TIMESTAMP '2026-03-31 23:59:59'"
Then  Err(SqlParse) mentioning the missing timezone
```

### LAKE-TT-003: timestamp resolves to the latest commit at or before it
```gherkin
Given commits at t0 (v0), t0+10s (v1), t0+20s (v2)
When  AS OF TIMESTAMP t0+15s is resolved
Then  version 1 is used
When  AS OF TIMESTAMP t0-1s is resolved
Then  Err(VersionNotFound)
```

### LAKE-TT-004: future version rejected
```gherkin
Given a table at head version 2
When  "SELECT * FROM gold.positions AS OF VERSION 7" runs
Then  Err(VersionNotFound(7))
```

### LAKE-TT-005: head pinned once per statement
```gherkin
Given a table at version 4
When  a query without AS OF runs while a concurrent commit creates version 5 mid-execution
Then  versions_read reports version 4 and the result contains only version-4 files
```

### LAKE-TT-006: versions_read always present
```gherkin
Given a query over gold.positions without AS OF at head 4
When  it completes
Then  the response has versions_read = [{table:"gold.positions", version:4}]
```

### LAKE-TT-007: reproducibility
```gherkin
Given "SELECT id, notional FROM gold.positions AS OF VERSION 2 ORDER BY id" returning body B1
When  three more WRITE commits are made and the same query is run
Then  the response body B2 is byte-identical to B1
```

### LAKE-TT-008: vacuumed version refused
```gherkin
Given version 1 referenced file a.parquet, later removed at v2 and physically vacuumed
When  "SELECT * FROM gold.positions AS OF VERSION 1" runs
Then  Err(VersionVacuumed{table:"gold.positions", version:1}) and zero rows are returned
```

### LAKE-TT-009: audit records versions read
```gherkin
Given a query with AS OF VERSION 2
When  it completes
Then  the audit outcome record's versions_read equals the response's versions_read
```

### LAKE-TT-010: schema as of version
```gherkin
Given column "lei" added at version 5
When  "SELECT * FROM gold.positions AS OF VERSION 4" runs
Then  the result has no column "lei"
```

### LAKE-TT-011: policies are never time-travelled by data queries
```gherkin
Given column "account_id" was unmasked for role "analyst" at data version 2, and masked since policy version 9
When  an analyst runs "SELECT account_id FROM gold.positions AS OF VERSION 2"
Then  account_id is returned masked according to the current policy
```

## 6. Test plan (write these first)

| Requirement | Test | Location | Kind |
|-------------|------|----------|------|
| LAKE-TT-001 | `fn lake_tt_001_as_of_version_parses_into_scan()` | `tests/spec_lake_tt.rs` | acceptance |
| LAKE-TT-001 | `fn test_plan_scan_as_of_version()` | `src/sql/planner.rs` | unit |
| LAKE-TT-002 | `fn lake_tt_002_as_of_timestamp_requires_offset()` | `tests/spec_lake_tt.rs` | acceptance |
| LAKE-TT-003 | `fn lake_tt_003_timestamp_resolves_to_latest_commit_at_or_before()` | `tests/spec_lake_tt.rs` | acceptance |
| LAKE-TT-003 | `fn test_resolve_as_of_timestamp_boundaries()` | `src/lake/log.rs` | unit |
| LAKE-TT-004 | `fn lake_tt_004_future_version_is_not_found()` | `tests/spec_lake_tt.rs` | acceptance |
| LAKE-TT-005 | `fn lake_tt_005_head_pinned_once_per_statement()` | `tests/spec_lake_tt.rs` | acceptance |
| LAKE-TT-006 | `fn lake_tt_006_versions_read_present_without_as_of()` | `tests/spec_lake_tt.rs` | acceptance |
| LAKE-TT-007 | `fn lake_tt_007_same_version_is_byte_identical_after_appends()` | `tests/spec_lake_tt.rs` | acceptance |
| LAKE-TT-008 | `fn lake_tt_008_vacuumed_version_refused_no_partial_rows()` | `tests/spec_lake_tt.rs` | acceptance |
| LAKE-TT-009 | `fn lake_tt_009_audit_record_matches_versions_read()` | `tests/spec_lake_tt.rs` | acceptance |
| LAKE-TT-010 | `fn lake_tt_010_schema_as_of_version_hides_later_columns()` | `tests/spec_lake_tt.rs` | acceptance |
| LAKE-TT-011 | `fn lake_tt_011_current_policy_applies_to_old_data()` | `tests/spec_lake_tt.rs` | acceptance (deny-first) |
| LAKE-TT-012 | `fn lake_tt_012_dropped_table_queryable_before_drop_version()` | `tests/spec_lake_tt.rs` | acceptance |

## 7. Open questions
* `AS OF` on a hot-tier materialised table: route to the lake if the materialised version differs
  (HYB-ACC), or refuse? Lean: route to lake silently, latency is the only difference.
* Should `versions_read` also carry the catalog version and policy version for full pinning? Lean
  yes, as `catalog_version` and `policy_version` fields; GOV-AUD would then record them too.

## 8. Change log
| Date | Change |
|------|--------|
| 2026-09-02 | Created |
| 2026-09-02 | Implemented 001–009 (`AS OF` pre-parser in `src/sql/parser.rs::strip_as_of`, `AsOf` in `src/lake/log.rs`, `resolve_as_of`, `versions_read` in `QueryResult` and audit records, `VersionVacuumed` on missing files before any read, `PinnedSnapshot` for 005). 010 waits for LAKE-SCH, 011 for GOV-ACC, 012 for the planner to accept a catalog version. Status stays Draft until those land. Decision: open question 2 partially — `catalog_version` is returned in every response; `policy_version` arrives with GOV-ACC. |
