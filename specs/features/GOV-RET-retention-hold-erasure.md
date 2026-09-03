# GOV-RET: Retention policies, legal hold, and GDPR erasure

| Field | Value |
|-------|-------|
| Status | Draft |
| Owner | |
| Related ADRs | ADR-0001, ADR-0002, ADR-0005 |
| Depends on | LAKE-TXN, LAKE-DML, LAKE-CAT, GOV-AUD, GOV-ACC, GOV-LIN, GOV-ENC |
| Milestone | M3 |

## 1. Problem and motivation
Banks live under two opposing legal pressures on the same data. MiFID II record keeping and
national bookkeeping law require order, transaction, and risk data to be kept, unmodified, for
five to ten years; SOX requires the audit trail behind financial reporting to be tamper-evident
for the same horizon. GDPR Art. 17 (right to erasure) and Art. 5(1)(e) (storage limitation)
require that a data subject's personal data be made unreadable when the lawful basis ends.
Litigation and regulator investigations add a third force: a legal hold that freezes everything
regardless of retention or erasure.

The `rhlog` transaction log (ADR-0002) makes history immutable by default, which satisfies the
retention side but, on its own, makes erasure impossible. This spec adds the controlled
exceptions: a retention horizon that VACUUM (LAKE-DML) must respect, a legal hold that suspends
both vacuum and erasure, and an ERASE operation that is the single, audited way to rewrite
history. Every conflict between these forces fails closed (Constitution 7) and is audited
(GOV-AUD), so a compliance officer can prove why data was, or was not, removed.

## 2. Scope
### In scope
* Retention policy per table, with layer-level defaults (`bronze`, `silver`, `gold`).
* Time-based (`RETENTION 7 YEARS`) and version-history (`KEEP VERSIONS 30 DAYS`) rules.
* Retention horizon computation and its enforcement inside `VACUUM`.
* Legal hold set/release with a dedicated role, listed and audited.
* `ERASE FROM t WHERE ...` with a mandatory request reference, copy-on-write rewrite across all
  snapshots, ERASE commit, downstream checklist via lineage, dry-run, erasure certificate.
* Crypto-shredding as an alternative erasure method for encrypted columns (GOV-ENC).
### Out of scope
* Subject discovery ("which tables contain subject X") beyond what classification tags and
  lineage already expose; a subject index is a later spec.
* Retention for the audit log itself (GOV-AUD owns its own retention).
* Backup media handling outside `<data_dir>`.

## 3. Requirements

| ID | Level | Requirement |
|----|-------|-------------|
| GOV-RET-001 | MUST | `VACUUM t` MUST refuse to remove any data file or log version whose retention horizon has not passed, returning `RustHouseError::RetentionViolation`, and MUST NOT partially remove files in the same run. |
| GOV-RET-002 | MUST | A retention policy MUST be settable per table with `ALTER TABLE t SET RETENTION <n> {DAYS\|MONTHS\|YEARS}` (data horizon) and `ALTER TABLE t SET KEEP VERSIONS <n> {DAYS\|MONTHS\|YEARS}` (history horizon); both are `ALTER` commits in the table's rhlog and recorded in the catalog. |
| GOV-RET-003 | MUST | Each layer MUST carry a default retention in the catalog applied to tables without an explicit policy; a table without an explicit or layer default MUST be treated as infinite retention (fail closed: nothing is vacuumable). |
| GOV-RET-004 | MUST | The retention horizon of a table MUST be computable as a pure function of (policy, commit timestamps) exposed as `RetentionPolicy::horizon(now, log)` and reported by `SHOW RETENTION FOR t`. |
| GOV-RET-005 | MUST | `ALTER TABLE t SET LEGAL HOLD '<case-ref>'` MUST suspend vacuum and erasure for `t`; while any hold is active, `VACUUM` and `ERASE` MUST return `RustHouseError::LegalHoldActive` and be audited as denied. |
| GOV-RET-006 | MUST | Setting or releasing a legal hold MUST require the role `legal_hold_officer`; any other principal MUST be `Denied`. Release MUST name the hold's case reference. |
| GOV-RET-007 | MUST | Active and released holds MUST be listed by `SHOW LEGAL HOLDS [FOR t]` with case reference, principal, set time, and release time; every set/release MUST produce a GOV-AUD record. |
| GOV-RET-008 | MUST | `ERASE FROM t WHERE <pred> REQUEST '<ref>'` MUST rewrite every data file in every snapshot of `t` that contains matching rows, producing new files without those rows, and MUST commit exactly one `ERASE` commit whose `commitInfo` lists rewritten file paths, row counts, predicate columns, and request reference, and MUST NOT record any erased values. |
| GOV-RET-009 | MUST | After an `ERASE` commit, reading `t` at any earlier version (`AS OF VERSION n`) MUST NOT return the erased rows; the log entries for earlier versions MUST remain readable and their `add` actions MUST resolve to the rewritten files. |
| GOV-RET-010 | MUST | `ERASE` MUST require the `REQUEST '<ref>'` clause; a statement without it MUST be rejected at planning time. |
| GOV-RET-011 | MUST | `ERASE` MUST consult lineage (GOV-LIN) and return, alongside the result, a checklist of downstream tables derived from `t` (with the columns involved) that may still hold the subject's data; the checklist MUST be included in the audit record. |
| GOV-RET-012 | MUST | `ERASE ... DRY RUN` MUST report affected files, row counts per snapshot, and the downstream checklist without writing any file or commit. |
| GOV-RET-013 | MUST | If the erasure predicate matches rows inside the table's retention horizon, `ERASE` MUST be refused with `RetentionViolation` naming the horizon, and the refusal MUST be audited. |
| GOV-RET-014 | SHOULD | A principal holding the role `erasure_override` SHOULD be able to add `OVERRIDE RETENTION` to `ERASE`; the override reason is mandatory and is audited. Legal hold is never overridable. |
| GOV-RET-015 | MUST | For a column encrypted with a subject-scoped DEK (GOV-ENC), `ERASE ... USING CRYPTO_SHRED` MUST destroy the subject's DEK instead of rewriting files, and MUST still produce an `ERASE` commit and certificate. |
| GOV-RET-016 | MUST | Every completed erasure MUST be exportable as an erasure certificate (JSON) containing request reference, request hash, table, predicate columns, snapshots affected, rewritten file count, executing principal, timestamps, method (`REWRITE`/`CRYPTO_SHRED`), and the audit record ids; the certificate MUST NOT contain erased values. |

## 4. Design sketch

Module: `src/governance/retention.rs`.

```rust
pub enum Horizon { Duration(chrono::Duration), Infinite }

pub struct RetentionPolicy {
    pub data: Horizon,      // rows older than this may be vacuumed
    pub history: Horizon,   // superseded versions older than this may be vacuumed
}

pub struct LegalHold { pub case_ref: String, pub set_by: PrincipalId, pub set_at: u64,
                       pub released_by: Option<PrincipalId>, pub released_at: Option<u64> }

pub struct EraseRequest { pub table: TableRef, pub predicate: Predicate, pub request_ref: String,
                          pub method: EraseMethod, pub dry_run: bool, pub override_retention: Option<String> }
pub enum EraseMethod { Rewrite, CryptoShred }

pub struct ErasureCertificate { /* see GOV-RET-016 */ }

pub struct RetentionGuard<'a> { catalog: &'a Catalog, audit: &'a AuditSink }
impl RetentionGuard<'_> {
    pub fn check_vacuum(&self, table: &TableRef, candidates: &[DataFile], now: u64) -> Result<()>;
    pub fn check_erase(&self, req: &EraseRequest, principal: &Principal, now: u64) -> Result<()>;
}
```

* Policies and holds are stored in the catalog (LAKE-CAT) as rows of the catalog's own rhlog
  table, so a policy change is itself a versioned, audited commit (ADR-0005, policy-as-data).
* `VACUUM` in `src/lake/dml.rs` calls `RetentionGuard::check_vacuum` before deleting anything.
  The guard evaluates the horizon against `commitInfo.timestamp` of the `remove` action for
  each candidate file; one violation fails the whole run (GOV-RET-001 atomicity).
* `ERASE` is planned in `src/sql/planner.rs` as `LogicalPlan::Erase`, executed in
  `src/lake/dml.rs::erase` which reuses the copy-on-write rewrite path of LAKE-DML `DELETE`
  but iterates over all `add` actions in the full log, not only the current snapshot. The
  ERASE commit contains one `remove`+`add` pair per rewritten file plus a `commitInfo` with
  `operation = "ERASE"`. Earlier versions' `add` paths are remapped via a `_rhlog/_erasures.json`
  file-alias table so that time travel resolves old paths to rewritten files (GOV-RET-009).
* Crypto-shred delegates to `src/governance/crypto.rs::destroy_dek(scope)` (GOV-ENC).
* `src/error.rs` gains `LegalHoldActive(String)` and `RetentionViolation(String)`.
* `src/meta.rs` `Table` gains `retention: Option<RetentionPolicy>` and `legal_holds: Vec<LegalHold>`
  in its catalog representation.
* Metrics: `rusthouse_vacuum_refused_total{reason}`, `rusthouse_erasures_total{method}`,
  `rusthouse_legal_holds_active`.

## 5. Acceptance scenarios

### GOV-RET-001: vacuum inside the retention horizon is refused
```gherkin
Given table silver.trades with RETENTION 7 YEARS and files removed 2 years ago
When  a principal runs VACUUM silver.trades
Then  the result is RetentionViolation
And   every data file still exists on disk
And   an audit record with decision=denied, reason=retention exists
```

### GOV-RET-002: retention policy is an ALTER commit
```gherkin
Given table silver.trades at version 4 with no retention policy
When  ALTER TABLE silver.trades SET RETENTION 7 YEARS is executed
Then  version 5 exists with commitInfo.operation = "ALTER" and parameters.retention = "7 YEARS"
And   SHOW RETENTION FOR silver.trades reports data horizon 7 years
```

### GOV-RET-003: missing policy means infinite retention
```gherkin
Given layer gold has no default retention and table gold.pnl has no explicit policy
When  VACUUM gold.pnl is executed with candidate files 20 years old
Then  the result is RetentionViolation
And   no file is removed
```

### GOV-RET-004: horizon is a pure function
```gherkin
Given a policy RETENTION 30 DAYS and now = 2026-09-02T00:00:00Z
When  horizon(now, log) is computed
Then  the data horizon is 2026-08-03T00:00:00Z regardless of catalog or disk state
```

### GOV-RET-005: legal hold blocks vacuum and erasure
```gherkin
Given table silver.trades under LEGAL HOLD 'case-123'
When  VACUUM silver.trades is executed
Then  the result is LegalHoldActive("case-123")
When  ERASE FROM silver.trades WHERE subject_id = 42 REQUEST 'dsr-9' is executed
Then  the result is LegalHoldActive("case-123")
And   both attempts are audited as denied
```

### GOV-RET-006: only legal_hold_officer may set or release a hold
```gherkin
Given principal analyst without role legal_hold_officer
When  analyst executes ALTER TABLE silver.trades SET LEGAL HOLD 'case-123'
Then  the result is Denied and no hold exists
When  principal counsel with role legal_hold_officer executes the same statement
Then  the hold exists and a release without the case reference is a parse error
```

### GOV-RET-007: holds are listed and audited
```gherkin
Given hold 'case-123' set by counsel then released by counsel
When  SHOW LEGAL HOLDS FOR silver.trades is executed
Then  one row shows case_ref=case-123, set_by=counsel, released_at not null
And   the audit log contains one SET_HOLD and one RELEASE_HOLD record
```

### GOV-RET-008: erasure rewrites every affected file and records no values
```gherkin
Given table silver.customers with subject_id 42 present in files across versions 1, 3, and 5
When  ERASE FROM silver.customers WHERE subject_id = 42 REQUEST 'dsr-9' is executed
Then  exactly one new commit with operation ERASE exists
And   its commitInfo lists three rewritten files with row counts
And   no field of the commit or audit record contains the value 42 or any column value of the erased rows
```

### GOV-RET-009: history no longer returns erased rows
```gherkin
Given the ERASE commit from GOV-RET-008
When  SELECT * FROM silver.customers AS OF VERSION 1 WHERE subject_id = 42 is executed
Then  zero rows are returned
And   SELECT count(*) FROM silver.customers AS OF VERSION 1 equals the original count minus erased rows
```

### GOV-RET-010: request reference is mandatory
```gherkin
When  ERASE FROM silver.customers WHERE subject_id = 42 is planned
Then  planning fails with SqlParse mentioning REQUEST
```

### GOV-RET-011: downstream checklist from lineage
```gherkin
Given gold.customer_360 is derived from silver.customers via lineage
When  ERASE FROM silver.customers WHERE subject_id = 42 REQUEST 'dsr-9' is executed
Then  the result includes a checklist containing gold.customer_360
And   the audit record contains the same checklist
```

### GOV-RET-012: dry run writes nothing
```gherkin
Given table silver.customers at version 5
When  ERASE FROM silver.customers WHERE subject_id = 42 REQUEST 'dsr-9' DRY RUN is executed
Then  the report lists affected files and row counts per snapshot
And   the table is still at version 5 and no new files exist
```

### GOV-RET-013: erasure inside retention is refused
```gherkin
Given silver.trades with RETENTION 7 YEARS containing subject 42 rows written 1 year ago
When  ERASE FROM silver.trades WHERE subject_id = 42 REQUEST 'dsr-9' is executed
Then  the result is RetentionViolation naming the horizon
And   the refusal is audited
```

### GOV-RET-015: crypto-shred erasure destroys the key
```gherkin
Given silver.customers.ssn encrypted with a DEK scoped to subject 42
When  ERASE FROM silver.customers WHERE subject_id = 42 REQUEST 'dsr-9' USING CRYPTO_SHRED is executed
Then  the DEK for subject 42 no longer exists in the key store
And   reading ssn for subject 42 at any version returns Denied / KeyUnavailable
And   an ERASE commit with method CRYPTO_SHRED exists
```

### GOV-RET-016: certificate export
```gherkin
Given the ERASE commit from GOV-RET-008
When  SHOW ERASURE CERTIFICATE 'dsr-9' is executed
Then  a JSON document with request_ref, request_hash, table, snapshots, file_count, principal, method, audit_ids is returned
And   it contains no erased column values
```

## 6. Test plan (write these first)

| Requirement | Test | Location | Kind |
|-------------|------|----------|------|
| GOV-RET-001 | `fn gov_ret_001_vacuum_inside_horizon_refused()` | `tests/spec_gov_ret.rs` | acceptance |
| GOV-RET-001 | `fn test_check_vacuum_is_all_or_nothing()` | `src/governance/retention.rs` | unit |
| GOV-RET-002 | `fn gov_ret_002_set_retention_is_alter_commit()` | `tests/spec_gov_ret.rs` | acceptance |
| GOV-RET-003 | `fn gov_ret_003_missing_policy_is_infinite()` | `tests/spec_gov_ret.rs` | acceptance |
| GOV-RET-004 | `fn gov_ret_004_horizon_is_pure_function()` | `tests/spec_gov_ret.rs` | acceptance |
| GOV-RET-004 | `fn prop_horizon_monotonic_in_now()` | `src/governance/retention.rs` | property |
| GOV-RET-005 | `fn gov_ret_005_legal_hold_blocks_vacuum_and_erase()` | `tests/spec_gov_ret.rs` | acceptance |
| GOV-RET-006 | `fn gov_ret_006_hold_requires_officer_role()` | `tests/spec_gov_ret.rs` | acceptance |
| GOV-RET-007 | `fn gov_ret_007_holds_listed_and_audited()` | `tests/spec_gov_ret.rs` | acceptance |
| GOV-RET-008 | `fn gov_ret_008_erase_rewrites_all_snapshots_no_values()` | `tests/spec_gov_ret.rs` | acceptance |
| GOV-RET-009 | `fn gov_ret_009_time_travel_excludes_erased_rows()` | `tests/spec_gov_ret.rs` | acceptance |
| GOV-RET-010 | `fn gov_ret_010_erase_requires_request_ref()` | `tests/spec_gov_ret.rs` | acceptance |
| GOV-RET-010 | `fn test_plan_erase_without_request_fails()` | `src/sql/planner.rs` | unit |
| GOV-RET-011 | `fn gov_ret_011_downstream_checklist_from_lineage()` | `tests/spec_gov_ret.rs` | acceptance |
| GOV-RET-012 | `fn gov_ret_012_dry_run_writes_nothing()` | `tests/spec_gov_ret.rs` | acceptance |
| GOV-RET-013 | `fn gov_ret_013_erase_inside_retention_refused()` | `tests/spec_gov_ret.rs` | acceptance |
| GOV-RET-014 | `fn gov_ret_014_override_role_with_reason()` | `tests/spec_gov_ret.rs` | acceptance |
| GOV-RET-015 | `fn gov_ret_015_crypto_shred_destroys_dek()` | `tests/spec_gov_ret.rs` | acceptance |
| GOV-RET-016 | `fn gov_ret_016_certificate_has_no_values()` | `tests/spec_gov_ret.rs` | acceptance |

## 7. Open questions
* Should `KEEP VERSIONS` default to the data horizon when unset, or to a shorter layer default?
* File-alias table (`_erasures.json`) versus rewriting historical `add` actions in place: the
  alias keeps old log files byte-identical (better for SOX), at the cost of one more lookup.
* Erasure across the hot tier (HYB-ACC): rebuild is implied; do we block reads until rebuilt?
* Certificate signing (detached signature with a KEK-derived key) for external DPOs.

## 8. Change log
| Date | Change |
|------|--------|
| 2026-09-02 | Created |
