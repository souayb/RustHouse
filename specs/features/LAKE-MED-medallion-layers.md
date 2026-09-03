# LAKE-MED: Medallion layers (Bronze / Silver / Gold) and promotion

| Field | Value |
|-------|-------|
| Status | Draft |
| Owner | RustHouse maintainers |
| Related ADRs | ADR-0001, ADR-0002, ADR-0005 |
| Depends on | LAKE-CAT, LAKE-TXN, LAKE-PQT, LAKE-DML, GOV-CLS, GOV-DQ, GOV-LIN, GOV-RET, API-SQL |
| Milestone | M4 |

## 1. Problem and motivation
BCBS 239 asks a bank to show, for every number in a risk report, where it came from, how it was
validated, and that nobody could have edited it on the way. A flat set of tables cannot show
this. The medallion pattern makes the pipeline explicit:

* **Bronze** holds raw data exactly as received from source systems (core banking, trading,
  market data). Append-only and schema-tolerant so that a source change never loses data.
* **Silver** holds validated, conformed data. Entry requires a data contract (GOV-DQ) and full
  classification (GOV-CLS), so PII/PCI tagging is guaranteed before analysts can touch it.
* **Gold** holds business-ready aggregates that feed reports and dashboards. It is read-mostly,
  hot-tier eligible (HYB-ACC), and may be written only by promotion jobs, never directly by
  a client, which gives SOX-style segregation of duties between data producers and report consumers.

Promotions are governed queries executed through `execute_governed` (API-SQL, ADR-0005), so
they are authorised, audited, and leave a lineage edge (GOV-LIN) that pins the exact source
versions (LAKE-TXN) each output version was computed from.

## 2. Scope
### In scope
* Three fixed namespaces `bronze`, `silver`, `gold` in the catalog (LAKE-CAT) with per-layer rules.
* Layer rule enforcement at `CREATE TABLE`, `INSERT`, and promotion time.
* Promotion job definition (`CREATE PROMOTION`) and execution, with lineage and version pinning.
* Layer listing and inspection.
* Per-layer default retention (GOV-RET) applied at table creation.
### Out of scope
* Scheduling (cron) of promotions; a promotion is triggered by an API call or by the
  background worker on a fixed interval configured in `src/config.rs`.
* Additional custom layers.

## 3. Requirements

| ID | Level | Requirement |
|----|-------|-------------|
| LAKE-MED-001 | MUST | The catalog MUST provide exactly three layer namespaces `bronze`, `silver`, `gold`; a table MUST belong to exactly one, expressed as `<layer>.<table>` and stored under `<data_dir>/<layer>/<table>/`. |
| LAKE-MED-002 | MUST | Bronze tables MUST be append-only: `DELETE`, `MERGE`, and schema-narrowing changes MUST be refused with `RustHouseError::Denied`, except `erase` invoked by GOV-RET. |
| LAKE-MED-003 | MUST | Bronze tables MUST be schema-tolerant: an `INSERT` carrying a column not in the schema MUST succeed by adding the column (LAKE-SCH additive evolution) rather than failing, and the new column MUST inherit the table's source-system classification. |
| LAKE-MED-004 | MUST | Creating or promoting into a Silver table MUST be refused unless the table has an attached data contract (GOV-DQ) and every column has an explicit classification (GOV-CLS). |
| LAKE-MED-005 | MUST | Gold tables MUST NOT accept `INSERT`, `DELETE`, or `MERGE` from a client principal; only a promotion executed by the promotion service principal MAY write to Gold. Violations MUST fail with `RustHouseError::Denied` and be audited. |
| LAKE-MED-006 | MUST | A promotion MUST be defined as a governed SQL query from one layer to the next-higher layer only (bronze→silver, silver→gold); definitions skipping a layer or going downward MUST be rejected. |
| LAKE-MED-007 | MUST | Executing a promotion MUST pin the source table version(s) read, write the target as one commit (LAKE-DML), and record a lineage edge `source@version → target@version` (GOV-LIN) in the same operation. |
| LAKE-MED-008 | MUST | If a promotion's output violates the target's data contract (GOV-DQ), no commit MUST be made to the target; offending rows MUST be routed to the quarantine table and the promotion result MUST report the violation count. |
| LAKE-MED-009 | MUST | The catalog MUST expose a listing of tables per layer with head version, row count, classification summary, contract status, and hot-tier status (HYB-ACC). |
| LAKE-MED-010 | MUST | Each layer MUST have a configurable default retention policy applied to new tables (GOV-RET); a table MAY override it only with a stricter (longer) policy. |
| LAKE-MED-011 | SHOULD | Gold tables SHOULD be automatically registered as hot-tier candidates on creation (HYB-ACC). |
| LAKE-MED-012 | SHOULD | Promotions SHOULD be idempotent: re-running a promotion whose pinned source versions have not changed SHOULD produce no new target commit. |
| LAKE-MED-013 | MAY | A promotion MAY declare an incremental mode processing only source versions since the last successful run. |

## 4. Design sketch

Module: `src/lake/medallion.rs`, catalog extensions in `src/catalog/`.

```rust
#[derive(Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Layer { Bronze, Silver, Gold }

impl Layer {
    pub fn next(self) -> Option<Layer>;
    pub fn rules(self) -> LayerRules;   // append_only, schema_tolerant, requires_contract, client_writable, hot_eligible
}

pub struct Promotion { pub name: String, pub source: TableRef, pub target: TableRef, pub sql: String, pub mode: PromotionMode }

pub struct PromotionService { session: Arc<Session>, lineage: Arc<dyn LineageStore>, dq: Arc<ContractChecker> }

impl PromotionService {
    pub async fn define(&self, p: Promotion, principal: &Principal) -> Result<()>;
    pub async fn run(&self, name: &str) -> Result<PromotionResult>;
}

pub struct PromotionResult { pub target_version: Option<u64>, pub sources: Vec<(TableRef, u64)>, pub rows_written: u64, pub rows_quarantined: u64 }
```

* `Layer` is derived from the namespace in `TableRef`; layer rules are consulted by the
  `Governor` (ADR-0005) before any DML, so enforcement lives in one place.
* Promotion runs under a dedicated service principal `svc.promotion` with `INSERT` on Gold;
  clients never hold that grant (GOV-ACC).
* Promotion SQL is planned through `src/sql/planner.rs` (`LogicalPlan`); the plan's `Scan`
  nodes yield the source tables whose head versions are pinned before execution.
* Errors: `Denied` for layer-rule violations; `ContractViolation` (GOV-DQ) for LAKE-MED-008.
* Config (`src/config.rs`): `promotion_interval_secs`, `layer_default_retention: {bronze, silver, gold}`.
* Metrics: `rusthouse_promotion_runs_total{promotion,status}`, `rusthouse_promotion_rows_quarantined_total`.

## 5. Acceptance scenarios

### LAKE-MED-001: Three layers, one table each
```gherkin
Given an empty catalog
When  CREATE TABLE bronze.trades_raw, silver.trades, gold.exposure_daily are executed
Then  each table directory exists under <data_dir>/<layer>/<table>/
And   CREATE TABLE platinum.x fails with an error naming the allowed layers
```

### LAKE-MED-002: Bronze is append-only
```gherkin
Given bronze.trades_raw with 10 rows
When  client principal "etl" executes DELETE FROM bronze.trades_raw WHERE id = 1
Then  it fails with RustHouseError::Denied and the table version is unchanged
When  GOV-RET calls erase(bronze.trades_raw, customer_id = 42, "dpo", "GDPR-REQ-1")
Then  the erase commit succeeds
```

### LAKE-MED-003: Bronze tolerates new columns
```gherkin
Given bronze.trades_raw with columns (id, amount) and source classification Confidential
When  an INSERT carries rows with an extra column "venue"
Then  the insert succeeds and the head schema has column "venue" classified Confidential
```

### LAKE-MED-004: Silver requires contract and classification
```gherkin
Given a CREATE TABLE silver.trades statement with a column lacking classification
When  it is executed
Then  it fails with an error naming the unclassified column
Given the same statement with full classification but no data contract
When  it is executed
Then  it fails with an error stating a contract is required for silver
```

### LAKE-MED-005: Gold is not client-writable
```gherkin
Given gold.exposure_daily and a client principal "analyst" holding INSERT on it
When  "analyst" executes INSERT INTO gold.exposure_daily ...
Then  it fails with RustHouseError::Denied and an audit record with outcome "denied" exists
When  the promotion service principal writes via a promotion run
Then  the commit succeeds
```

### LAKE-MED-006: Promotions move exactly one layer up
```gherkin
When  CREATE PROMOTION p1 FROM bronze.trades_raw TO gold.exposure_daily is executed
Then  it fails with an error stating promotions must target the next layer
When  CREATE PROMOTION p2 FROM silver.trades TO bronze.trades_raw is executed
Then  it fails
When  CREATE PROMOTION p3 FROM bronze.trades_raw TO silver.trades is executed
Then  it succeeds
```

### LAKE-MED-007: Promotion pins versions and records lineage
```gherkin
Given bronze.trades_raw at version 7 and promotion p3 defined
When  p3 runs
Then  silver.trades gains a new version v
And   the lineage store contains an edge bronze.trades_raw@7 -> silver.trades@v
And   the target commitInfo.operationParameters lists {"bronze.trades_raw": 7}
```

### LAKE-MED-008: Contract violation quarantines, does not commit
```gherkin
Given silver.trades has contract "amount >= 0" and bronze.trades_raw contains 3 negative amounts among 100 rows
When  p3 runs
Then  silver.trades gains one version containing 97 rows
And   the quarantine table for silver.trades contains 3 rows tagged with the promotion run id
And   the result reports rows_quarantined = 3
Given a contract violation that applies to the schema itself (missing NOT NULL column)
When  p3 runs
Then  no commit is made to silver.trades and the run fails with ContractViolation
```

### LAKE-MED-009: Layer listing
```gherkin
Given tables in all three layers
When  the catalog is asked to list layer "silver"
Then  each entry shows head version, row count, classification summary, contract status, hot-tier status
```

### LAKE-MED-010: Default retention per layer
```gherkin
Given config layer_default_retention = {bronze: 7y, silver: 10y, gold: 10y}
When  CREATE TABLE silver.trades is executed without a retention clause
Then  the table's retention policy is 10y
When  CREATE TABLE silver.x WITH RETENTION 2y is executed
Then  it fails because 2y is weaker than the layer default
```

## 6. Test plan (write these first)

| Requirement | Test | Location | Kind |
|-------------|------|----------|------|
| LAKE-MED-001 | `fn lake_med_001_three_layers_and_paths()` | `tests/spec_lake_med.rs` | acceptance |
| LAKE-MED-001 | `fn lake_med_001_unknown_layer_rejected()` | `tests/spec_lake_med.rs` | acceptance (negative) |
| LAKE-MED-002 | `fn lake_med_002_bronze_delete_denied()` | `tests/spec_lake_med.rs` | acceptance (negative) |
| LAKE-MED-002 | `fn lake_med_002_bronze_erase_allowed()` | `tests/spec_lake_med.rs` | acceptance |
| LAKE-MED-003 | `fn lake_med_003_bronze_adds_unknown_column_with_inherited_classification()` | `tests/spec_lake_med.rs` | acceptance |
| LAKE-MED-004 | `fn lake_med_004_silver_requires_contract_and_classification()` | `tests/spec_lake_med.rs` | acceptance (negative) |
| LAKE-MED-005 | `fn lake_med_005_gold_client_insert_denied_and_audited()` | `tests/spec_lake_med.rs` | acceptance (negative) |
| LAKE-MED-005 | `fn lake_med_005_gold_promotion_write_allowed()` | `tests/spec_lake_med.rs` | acceptance |
| LAKE-MED-006 | `fn lake_med_006_promotion_must_target_next_layer()` | `tests/spec_lake_med.rs` | acceptance (negative) |
| LAKE-MED-006 | `fn test_layer_next()` | `src/lake/medallion.rs` | unit |
| LAKE-MED-007 | `fn lake_med_007_promotion_pins_versions_and_records_lineage()` | `tests/spec_lake_med.rs` | acceptance |
| LAKE-MED-008 | `fn lake_med_008_contract_violation_quarantines_rows()` | `tests/spec_lake_med.rs` | acceptance |
| LAKE-MED-008 | `fn lake_med_008_schema_violation_makes_no_commit()` | `tests/spec_lake_med.rs` | acceptance (negative) |
| LAKE-MED-009 | `fn lake_med_009_layer_listing_fields()` | `tests/spec_lake_med.rs` | acceptance |
| LAKE-MED-010 | `fn lake_med_010_default_retention_applied_and_weaker_override_rejected()` | `tests/spec_lake_med.rs` | acceptance |
| LAKE-MED-011 | `fn lake_med_011_gold_registered_as_hot_candidate()` | `tests/spec_lake_med.rs` | acceptance |
| LAKE-MED-012 | `fn lake_med_012_promotion_idempotent_on_unchanged_sources()` | `tests/spec_lake_med.rs` | acceptance |

## 7. Open questions
* Should Bronze inherit classification per source system (catalog-level `SOURCE` object) or
  per table? Leaning: per table with an optional source-system default.
* Quarantine table location: `silver.<table>__quarantine` or a `quarantine` namespace? GOV-DQ
  decides; this spec only requires it exists.
* Is a Silver→Silver transformation (conforming across two silver tables) a promotion or a plain
  governed write? Leaning: plain write, lineage still recorded by GOV-LIN.

## 8. Change log
| Date | Change |
|------|--------|
| 2026-09-02 | Created |
