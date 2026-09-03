# GOV-DQ: Data contracts, quality rules, and quarantine

| Field | Value |
|-------|-------|
| Status | Draft |
| Owner | |
| Related ADRs | ADR-0001, ADR-0002, ADR-0005 |
| Depends on | LAKE-TXN, LAKE-CAT, LAKE-MED, LAKE-SCH, GOV-AUD, GOV-ACC |
| Milestone | M4 |

## 1. Problem and motivation
BCBS 239 requires that risk data be **accurate** (principle 3), **complete** (principle 4), and
**timely** (principle 5), and that a bank can demonstrate the controls that make it so. In a
lakehouse the Bronze layer is deliberately tolerant; Silver and Gold are where the bank's
numbers come from, and every regulator question ("why did the exposure total move?") ends up
asking whether bad rows got in. Today RustHouse validates only that a column exists and has
the right storage type (`src/storage/segment.rs`).

A data contract is the producer's machine-checkable promise for a Silver or Gold table: the
exact schema and the rules its rows satisfy. Enforcing it inside the commit path (LAKE-TXN)
means a violated `reject` rule produces no commit at all (accuracy), a `quarantine` rule keeps
the bad rows visible and attributable instead of silently dropping them (completeness), and a
freshness rule flags a table whose feed has stalled (timeliness). Contracts are versioned in the
catalog, so a change to the rules is itself an audited, time-travelable ALTER commit (SOX
change control, ADR-0005 policy-as-data).

## 2. Scope
### In scope
* Contract definition: schema (types, nullability), rule set, severity per rule.
* Rule kinds: `not_null`, `unique`, `range`, `regex`, `exists_in` (referential), `freshness`.
* Enforcement at write time for Silver and Gold tables; Bronze is exempt by default.
* Quarantine tables `<layer>.<table>__quarantine`.
* Contract storage, versioning, `SHOW CONTRACT FOR`, breaking-change rules.
* Prometheus metrics and freshness background job.
### Out of scope
* Statistical anomaly detection (drift, distribution tests).
* Cross-table reconciliation reports; `exists_in` covers key existence only.
* Contract enforcement on hot-tier materialisation (the hot tier is derived; HYB-ACC).

## 3. Requirements

| ID | Level | Requirement |
|----|-------|-------------|
| GOV-DQ-001 | MUST | A write to a table whose contract has a violated `reject`-severity rule MUST produce no commit, no data file, and MUST return `RustHouseError::ContractViolation` listing every violated rule with row counts. |
| GOV-DQ-002 | MUST | A contract MUST be declared with `CREATE CONTRACT FOR <layer>.<table> (...)` specifying, per column, an exact `ColumnType` and nullability, and zero or more rules each with a severity in `{reject, quarantine, warn}`. |
| GOV-DQ-003 | MUST | Supported rule kinds MUST include `not_null(col)`, `unique(col, ...)`, `range(col, min, max)`, `regex(col, pattern)`, `exists_in(col, other_table.col)`, and `freshness(max_staleness)`. |
| GOV-DQ-004 | MUST | Rows that violate only `quarantine`-severity rules MUST be written to `<layer>.<table>__quarantine` with additional columns `_dq_rule`, `_dq_reason`, `_dq_source_commit_ref`, `_dq_written_at`, and the remaining rows MUST be committed to the target table in the same logical operation. |
| GOV-DQ-005 | MUST | Rows that violate only `warn`-severity rules MUST be committed to the target table, and the violation counts MUST appear in the commit's `commitInfo.operationMetrics` and in the audit record. |
| GOV-DQ-006 | MUST | A write whose schema differs from the contract's schema (missing column, extra column, type mismatch, null in a non-nullable column) MUST be treated as a `reject` violation regardless of rule severities. |
| GOV-DQ-007 | MUST | Contracts MUST be stored in the catalog (LAKE-CAT) with a monotonically increasing contract version per table; creating or altering a contract MUST be an `ALTER` commit on the target table's rhlog and MUST be audited. |
| GOV-DQ-008 | MUST | `ALTER CONTRACT` that narrows the contract (adds a column, adds a `reject` rule, tightens a range, makes a column non-nullable) MUST require a new contract version and MUST be refused unless the current table snapshot already satisfies the new rules or `ALLOW EXISTING VIOLATIONS` is stated. |
| GOV-DQ-009 | MUST | `SHOW CONTRACT FOR <layer>.<table>` MUST return the active contract version, its schema, rules, severities, and the commit version at which it became active. |
| GOV-DQ-010 | MUST | Every enforcement outcome MUST increment `rusthouse_dq_rows_total{table, rule, severity, outcome}` where outcome is `passed`, `rejected`, `quarantined`, or `warned`. |
| GOV-DQ-011 | MUST | A background freshness job MUST evaluate `freshness(max_staleness)` rules against the latest commit timestamp and set `rusthouse_dq_table_stale{table}` to 1 plus write a GOV-AUD record of kind `STALE` when the threshold is exceeded; the flag MUST be visible via `SHOW CONTRACT FOR`. |
| GOV-DQ-012 | MUST | Silver and Gold tables MUST have a contract before accepting their first write; a write to a Silver or Gold table without a contract MUST be refused with `ContractViolation("no contract")` (fail closed). Bronze tables MAY have contracts. |
| GOV-DQ-013 | SHOULD | `exists_in` SHOULD be evaluated against the referenced table's snapshot at the same catalog version used for the write, and the referenced version SHOULD be recorded in `operationMetrics`. |
| GOV-DQ-014 | SHOULD | Contract enforcement SHOULD add no more than 20% to the encode time of a 1M-row write for a contract with ten non-`exists_in` rules. |
| GOV-DQ-015 | MAY | A contract MAY be exported as a JSON document (with its version and hash) for consumers to validate against outside RustHouse. |

## 4. Design sketch

Module: `src/governance/quality.rs`.

```rust
pub enum Severity { Reject, Quarantine, Warn }

pub enum Rule {
    NotNull { col: String },
    Unique { cols: Vec<String> },
    Range { col: String, min: serde_json::Value, max: serde_json::Value },
    Regex { col: String, pattern: String },
    ExistsIn { col: String, table: TableRef, ref_col: String },
    Freshness { max_staleness_secs: u64 },
}

pub struct ContractColumn { pub name: String, pub column_type: ColumnType, pub nullable: bool }

pub struct Contract {
    pub table: TableRef,
    pub version: u32,
    pub columns: Vec<ContractColumn>,
    pub rules: Vec<(Rule, Severity)>,
    pub active_since_commit: u64,
}

pub struct Verdict {
    pub accepted: Vec<usize>,                 // row indices
    pub quarantined: Vec<(usize, RuleId, String)>,
    pub rejected_rules: Vec<(RuleId, usize)>, // rule, row count
    pub warnings: Vec<(RuleId, usize)>,
}

pub struct ContractChecker<'a> { catalog: &'a Catalog }
impl ContractChecker<'_> {
    pub fn check(&self, table: &TableRef, batch: &HashMap<String, ColumnData>) -> Result<Verdict>;
}
```

* The commit path in `src/lake/dml.rs::append` calls `ContractChecker::check` after schema
  resolution and before Parquet encoding. A non-empty `rejected_rules` short-circuits with
  `ContractViolation` before any file is created (GOV-DQ-001). Otherwise the batch is split by
  `Verdict` into the target file and the quarantine file, and both `add` actions are written in
  the target table's commit and the quarantine table's commit, in that order, with the
  quarantine commit's `commitInfo.parameters.source_commit` pointing back.
* `ColumnData` (`src/storage/column_writer.rs`) currently has no null representation;
  `not_null` and nullability checks require the nullable `ColumnData` variant introduced by
  LAKE-PQT. Until then, `nullable = false` is the only accepted value.
* Regex compiled once per contract version and cached in the catalog's contract cache.
* `exists_in` reads the referenced table's snapshot through the Governor with a system
  principal so the check is itself audited (ADR-0005).
* Contracts live in the catalog's rhlog table as rows keyed by `(table, contract_version)`;
  `SHOW CONTRACT` is a catalog query. `src/sql/planner.rs` gains `LogicalPlan::CreateContract`,
  `AlterContract`, `ShowContract`.
* `src/error.rs` gains `ContractViolation(String)`.
* Freshness job in `src/lake/medallion.rs` background workers, interval configurable in
  `src/config.rs` (`dq_freshness_interval_secs`).
* Metrics in `src/metrics.rs`: `rusthouse_dq_rows_total`, `rusthouse_dq_table_stale`,
  `rusthouse_dq_check_seconds`.

## 5. Acceptance scenarios

### GOV-DQ-001: reject rule produces no commit
```gherkin
Given silver.trades at version 3 with contract v1 having range(qty, 0, 1e9) severity reject
When  a batch of 100 rows with one qty = -5 is written
Then  the result is ContractViolation listing range(qty) with count 1
And   silver.trades is still at version 3
And   no new parquet file exists under silver/trades/
```

### GOV-DQ-002: contract declaration
```gherkin
When  CREATE CONTRACT FOR silver.trades (trade_id Int64 NOT NULL, qty Int64 NOT NULL, isin String NOT NULL,
        RULE unique(trade_id) SEVERITY reject, RULE regex(isin, '^[A-Z]{2}[A-Z0-9]{9}[0-9]$') SEVERITY quarantine) is executed
Then  SHOW CONTRACT FOR silver.trades returns contract version 1 with 3 columns and 2 rules
```

### GOV-DQ-003: all rule kinds are accepted and evaluated
```gherkin
Given a contract using not_null, unique, range, regex, exists_in, and freshness rules
When  a batch violating each of the first five once is checked
Then  the Verdict reports exactly one violation per rule kind
```

### GOV-DQ-004: quarantine splits the batch
```gherkin
Given silver.trades with regex(isin) severity quarantine
When  a batch of 10 rows with 2 invalid ISINs is written
Then  silver.trades gains 8 rows in a new commit
And   silver.trades__quarantine gains 2 rows with _dq_rule = 'regex(isin)' and _dq_source_commit_ref pointing at that commit
```

### GOV-DQ-005: warn commits and records counts
```gherkin
Given silver.trades with range(qty, 0, 1e6) severity warn
When  a batch with 3 rows of qty > 1e6 is written
Then  all rows are committed
And   commitInfo.operationMetrics.dq_warned["range(qty)"] = 3
And   the audit record for the write contains the same count
```

### GOV-DQ-006: schema mismatch is always reject
```gherkin
Given silver.trades contract with columns (trade_id, qty, isin) and only warn rules
When  a batch missing column isin is written
Then  the result is ContractViolation("schema") and no commit exists
```

### GOV-DQ-007: contract change is an audited ALTER commit
```gherkin
Given silver.trades at version 5 with contract v1
When  ALTER CONTRACT FOR silver.trades ADD RULE not_null(isin) SEVERITY warn is executed
Then  version 6 exists with commitInfo.operation = "ALTER" and parameters.contract_version = 2
And   an audit record for the ALTER exists
```

### GOV-DQ-008: narrowing requires a clean snapshot or explicit allowance
```gherkin
Given silver.trades containing a row with qty = -1 and contract v1 without a range rule
When  ALTER CONTRACT FOR silver.trades ADD RULE range(qty, 0, 1e9) SEVERITY reject is executed
Then  the result is ContractViolation reporting 1 existing violation and the contract stays at v1
When  the same statement with ALLOW EXISTING VIOLATIONS is executed
Then  contract v2 is active
```

### GOV-DQ-009: show contract
```gherkin
Given contract v2 active since commit 6
When  SHOW CONTRACT FOR silver.trades is executed
Then  the response contains version 2, active_since_commit 6, the schema, and each rule with severity
```

### GOV-DQ-010: metrics per outcome
```gherkin
Given the write from GOV-DQ-004
When  /metrics is fetched
Then  rusthouse_dq_rows_total{table="silver.trades",rule="regex(isin)",severity="quarantine",outcome="quarantined"} = 2
And   the same series with outcome="passed" = 8
```

### GOV-DQ-011: freshness job flags stale tables
```gherkin
Given silver.trades with freshness(3600) and last commit 2 hours ago
When  the freshness job runs
Then  rusthouse_dq_table_stale{table="silver.trades"} = 1
And   an audit record of kind STALE exists
And   SHOW CONTRACT FOR silver.trades reports stale = true
```

### GOV-DQ-012: silver write without a contract is refused
```gherkin
Given silver.new_table created without a contract
When  any batch is written to silver.new_table
Then  the result is ContractViolation("no contract") and the table stays at version 0
```

## 6. Test plan (write these first)

| Requirement | Test | Location | Kind |
|-------------|------|----------|------|
| GOV-DQ-001 | `fn gov_dq_001_reject_rule_produces_no_commit()` | `tests/spec_gov_dq.rs` | acceptance |
| GOV-DQ-001 | `fn test_check_short_circuits_before_encode()` | `src/governance/quality.rs` | unit |
| GOV-DQ-002 | `fn gov_dq_002_create_contract_declares_schema_and_rules()` | `tests/spec_gov_dq.rs` | acceptance |
| GOV-DQ-003 | `fn gov_dq_003_all_rule_kinds_evaluated()` | `tests/spec_gov_dq.rs` | acceptance |
| GOV-DQ-003 | `fn prop_range_rule_matches_manual_bounds()` | `src/governance/quality.rs` | property |
| GOV-DQ-004 | `fn gov_dq_004_quarantine_splits_batch()` | `tests/spec_gov_dq.rs` | acceptance |
| GOV-DQ-005 | `fn gov_dq_005_warn_commits_and_records_counts()` | `tests/spec_gov_dq.rs` | acceptance |
| GOV-DQ-006 | `fn gov_dq_006_schema_mismatch_is_reject()` | `tests/spec_gov_dq.rs` | acceptance |
| GOV-DQ-007 | `fn gov_dq_007_contract_change_is_alter_commit()` | `tests/spec_gov_dq.rs` | acceptance |
| GOV-DQ-008 | `fn gov_dq_008_narrowing_requires_clean_snapshot()` | `tests/spec_gov_dq.rs` | acceptance |
| GOV-DQ-009 | `fn gov_dq_009_show_contract()` | `tests/spec_gov_dq.rs` | acceptance |
| GOV-DQ-010 | `fn gov_dq_010_metrics_per_outcome()` | `tests/spec_gov_dq.rs` | acceptance |
| GOV-DQ-011 | `fn gov_dq_011_freshness_job_flags_stale()` | `tests/spec_gov_dq.rs` | acceptance |
| GOV-DQ-012 | `fn gov_dq_012_silver_write_without_contract_refused()` | `tests/spec_gov_dq.rs` | acceptance |
| GOV-DQ-013 | `fn gov_dq_013_exists_in_pins_referenced_version()` | `tests/spec_gov_dq.rs` | acceptance |
| GOV-DQ-014 | `fn bench_contract_check_overhead()` | `benches/bench_dq.rs` | benchmark |

## 7. Open questions
* Quarantine table retention and access: inherit the source table's classification (GOV-CLS)
  and GOV-RET policy, or a stricter default since it concentrates bad, possibly sensitive rows?
* Should `unique` be evaluated against the whole table snapshot (expensive) or the batch plus a
  key index maintained per table?
* Warn-severity rules on Bronze: useful for early signal, but Bronze writes are high volume.

## 8. Change log
| Date | Change |
|------|--------|
| 2026-09-02 | Created |
