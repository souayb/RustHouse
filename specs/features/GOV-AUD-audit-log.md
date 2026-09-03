# GOV-AUD: Tamper-evident audit log

| Field | Value |
|-------|-------|
| Status | Draft |
| Owner | RustHouse maintainers |
| Related ADRs | ADR-0002, ADR-0004, ADR-0005 |
| Depends on | LAKE-TXN, API-SQL |
| Milestone | M1 |

## 1. Problem and motivation
SOX requires an audit trail over systems that produce financial statements, and requires that
the trail itself cannot be silently altered. MiFID II requires records of transactions and the
access to them to be kept for five to seven years. DORA requires evidence for incident
investigation. GDPR Art. 30/33 require knowing who accessed personal data and when.

A log that an administrator can edit is not evidence. RustHouse therefore writes an
**append-only, hash-chained** audit log: every record carries the hash of its predecessor, so a
single altered byte breaks the chain from that point on. The log records **intent before
execution** and **outcome after**, so a crash or a kill mid-query still leaves a trace. If the log
cannot be written, the operation does not run (Constitution 7): availability never beats
accountability. There is no admin bypass; the Governor (ADR-0005) is the only path to data and
it always writes audit.

## 2. Scope
### In scope
* Record schema, canonical serialisation, SHA-256 hash chain, `_chain_head`.
* Intent / outcome pairing keyed by `request_id`.
* Fail-closed behaviour: `AuditUnavailable` → HTTP 503.
* Daily rotation; separate retention exempt from vacuum.
* `rusthouse audit verify` and `rusthouse audit export`.
* Audit of queries, writes/commits (LAKE-TXN), DDL, classification and policy changes.

### Out of scope
* Shipping to an external SIEM (MAY, later); the export command is the integration seam.
* Access control on the audit log itself beyond a read-only `auditor` role (GOV-ACC).

## 3. Requirements

| ID | Level | Requirement |
|----|-------|-------------|
| GOV-AUD-001 | MUST | If the audit sink cannot append (I/O error, disk full, permission denied), the governed operation MUST be refused with `RustHouseError::AuditUnavailable` before any data file is read or any rhlog commit is attempted; API-SQL maps this to HTTP 503. |
| GOV-AUD-002 | MUST | Each record MUST be one JSON line with fields `seq: u64`, `ts: RFC3339 UTC`, `request_id`, `principal`, `operation`, `object` (`layer.table` or `catalog.policies`), `columns: []`, `classifications: {}`, `versions_read: {}`, `decision: allow|deny|mask|override`, `row_count: Option<u64>`, `status: intent|ok|error`, `prev_hash`, `hash`; unknown fields MUST be rejected by the verifier. |
| GOV-AUD-003 | MUST | `hash` MUST be `SHA-256(canonical_json(record without hash) || prev_hash)` where canonical JSON has sorted keys and no insignificant whitespace; `seq` MUST increase by exactly 1 per record and `prev_hash` of record `n` MUST equal `hash` of record `n-1`. The genesis record has `prev_hash = "0" * 64`. |
| GOV-AUD-004 | MUST | Files MUST live under `<data_dir>/audit/<yyyy>/<mm>/<dd>/audit-<seq_start>.jsonl`, rotated at UTC midnight; the chain MUST continue across file boundaries and `<data_dir>/audit/_chain_head` MUST hold `{seq, hash}` of the last record, updated atomically (tmp + rename). |
| GOV-AUD-005 | MUST | Every governed operation MUST write an `intent` record before execution and an `ok` or `error` record after, sharing `request_id`; the outcome record MUST include `row_count`, `versions_read` and elapsed milliseconds. |
| GOV-AUD-006 | MUST | `rusthouse audit verify [--from <seq>]` MUST walk the chain from genesis (or `--from`) and exit non-zero reporting the first `seq` whose hash or `prev_hash` does not verify, or whose `seq` is not contiguous. |
| GOV-AUD-007 | MUST | Records MUST be appended with `O_APPEND` semantics and `fsync` before the operation proceeds; the writer MUST never open an audit file for writing other than append. |
| GOV-AUD-008 | MUST | rhlog commits (LAKE-TXN: WRITE, ALTER, DELETE, VACUUM, ERASE) and catalog policy changes (GOV-ACC GRANT/REVOKE/CREATE POLICY, GOV-CLS reclassification) MUST produce audit records naming the resulting version. |
| GOV-AUD-009 | MUST | Audit records MUST be excluded from LAKE-DML vacuum and GOV-RET retention; the audit log MUST have its own retention (default 7 years) and deleting within it MUST require the `auditor_admin` role and MUST itself be audited. |
| GOV-AUD-010 | MUST | `rusthouse audit export --from <ts> --to <ts> [--principal p] [--table t]` MUST emit matching records as JSONL together with a `manifest.json` containing the seq range and the chain head hash so a regulator can independently verify the extract. |
| GOV-AUD-011 | MUST | There MUST be no code path that executes a `LogicalPlan` without passing through the audit-writing Governor; `Executor` MUST be `pub(crate)` and a compile-time test (`tests/spec_gov_aud.rs` uses only the public API) MUST confirm it is not reachable. |
| GOV-AUD-012 | SHOULD | The audit writer SHOULD batch records within one request (intent + outcome) into a single fsync when they occur within the same call, while preserving order and chain integrity. |
| GOV-AUD-013 | MAY | A `rusthouse_audit_records_total{status,decision}` counter and `rusthouse_audit_write_seconds` histogram MAY be exposed. |

## 4. Design sketch

```rust
// src/governance/audit.rs
#[derive(Serialize, Deserialize)]
pub struct AuditRecord {
    pub seq: u64, pub ts: String, pub request_id: String, pub principal: String,
    pub operation: String, pub object: String, pub columns: Vec<String>,
    pub classifications: BTreeMap<String, Classification>,   // GOV-CLS-009
    pub versions_read: BTreeMap<String, u64>,
    pub decision: Decision, pub row_count: Option<u64>, pub status: Status,
    pub elapsed_ms: Option<u64>, pub prev_hash: String, pub hash: String,
}

pub trait AuditSink: Send + Sync {
    fn append(&self, draft: AuditDraft) -> Result<AuditRecord>;   // computes seq, prev_hash, hash; fsyncs
}
pub struct FileAuditSink { root: PathBuf, head: Mutex<ChainHead> }
pub fn verify(root: &Path, from: Option<u64>) -> Result<VerifyReport>;
pub fn export(root: &Path, filter: ExportFilter, out: &mut impl Write) -> Result<Manifest>;
```

* Canonical JSON: `serde_json` with `BTreeMap`-ordered structs; a dedicated `canonical_bytes()`
  helper is the single place the hash input is produced, and is property-tested.
* `src/error.rs`: add `AuditUnavailable(String)`; `src/main.rs` gains `audit verify` and
  `audit export` subcommands (clap).
* Governor (`src/governance/mod.rs`) calls `sink.append(intent)?` before `Executor`, and
  `sink.append(outcome)` in a `finally`-style block so error paths are also recorded.
* Test hook: `FailingAuditSink` implementing `AuditSink` to drive GOV-AUD-001.
* Metrics as in GOV-AUD-013.

## 5. Acceptance scenarios

### GOV-AUD-001: Unwritable audit sink refuses the operation
```gherkin
Given a Session whose AuditSink always fails with an I/O error
And   principal analyst with a valid SELECT grant on gold.accounts
When  analyst executes SELECT * FROM gold.accounts
Then  the result is AuditUnavailable
And   no data file under gold/accounts was opened
And   the HTTP API returns status 503 for the same request
```

### GOV-AUD-002: Record schema is complete and strict
```gherkin
Given one governed query has completed
When  the last audit line is parsed
Then  all fields listed in GOV-AUD-002 are present with the specified types
And   a line with an extra unknown field is rejected by the verifier
```

### GOV-AUD-003: Hash chain is well-formed
```gherkin
Given three consecutive records r1, r2, r3
When  their hashes are recomputed from canonical JSON
Then  r2.prev_hash == r1.hash and r3.prev_hash == r2.hash
And   r1.prev_hash is 64 zeros and seq values are 0, 1, 2
```

### GOV-AUD-004: Daily rotation keeps the chain
```gherkin
Given records written with a clock at 2026-03-31T23:59:59Z and then 2026-04-01T00:00:01Z
When  the audit directory is listed
Then  files exist under audit/2026/03/31/ and audit/2026/04/01/
And   the first record of the April file has prev_hash equal to the last March record's hash
And   _chain_head matches the last record
```

### GOV-AUD-005: Intent precedes outcome
```gherkin
Given a query that reads gold.accounts at version 12 and returns 40 rows
When  the audit log is read
Then  an intent record with status "intent" precedes an outcome record with status "ok", row_count 40, versions_read {gold.accounts: 12}
And   both share the same request_id
Given a query whose execution panics or errors
Then  an outcome record with status "error" still exists for its request_id
```

### GOV-AUD-006: Tampering is detected at the exact record
```gherkin
Given an audit log with 100 records passing verify
When  one byte of the principal field of record seq 57 is changed on disk
Then  rusthouse audit verify exits non-zero and reports "chain broken at seq 57"
When  record seq 57 is deleted instead
Then  verify reports a seq gap at 57
```

### GOV-AUD-007: Append-only and durable
```gherkin
Given the FileAuditSink
When  it appends a record
Then  the file descriptor was opened with append mode and fsync was called before append() returned
```

### GOV-AUD-008: Commits and policy changes are audited
```gherkin
Given a WRITE commit producing version 3 of silver.trades and a GRANT producing catalog version 9
When  the audit log is read
Then  records exist with operation "WRITE", object silver.trades, versions_read {silver.trades: 3}
And   operation "GRANT", object catalog.policies, versions_read {catalog.policies: 9}
```

### GOV-AUD-009: Vacuum and retention never touch audit
```gherkin
Given a retention policy of 1 day on all tables and audit records older than 1 day
When  vacuum and the retention job run
Then  every audit record still exists and verify passes
When  principal analyst attempts to delete audit records
Then  the result is Denied
```

### GOV-AUD-010: Export is verifiable
```gherkin
Given 500 audit records spanning two days
When  rusthouse audit export --from day1 --to day1 --principal analyst runs
Then  the output JSONL contains only analyst's day-1 records
And   manifest.json states the seq range and chain head hash, and verify --from that range passes on the extract
```

### GOV-AUD-011: No ungoverned execution path
```gherkin
Given only the public API of the rusthouse crate
When  an integration test attempts to construct and call Executor directly
Then  it does not compile (checked with a compile_fail doc-test or trybuild)
```

## 6. Test plan (write these first)

| Requirement | Test | Location | Kind |
|-------------|------|----------|------|
| GOV-AUD-001 | `fn gov_aud_001_unwritable_sink_refuses_operation()` | `tests/spec_gov_aud.rs` | acceptance (deny-first) |
| GOV-AUD-001 | `fn gov_aud_001_http_maps_to_503()` | `tests/spec_gov_aud.rs` | acceptance |
| GOV-AUD-006 | `fn gov_aud_006_tamper_detected_at_seq()` | `tests/spec_gov_aud.rs` | acceptance (negative) |
| GOV-AUD-006 | `fn gov_aud_006_deleted_record_detected_as_gap()` | `tests/spec_gov_aud.rs` | acceptance (negative) |
| GOV-AUD-002 | `fn gov_aud_002_record_schema_complete_and_strict()` | `tests/spec_gov_aud.rs` | acceptance |
| GOV-AUD-003 | `fn gov_aud_003_hash_chain_well_formed()` | `tests/spec_gov_aud.rs` | acceptance |
| GOV-AUD-003 | `fn test_canonical_json_is_deterministic()` | `src/governance/audit.rs` | unit / property |
| GOV-AUD-004 | `fn gov_aud_004_daily_rotation_keeps_chain()` | `tests/spec_gov_aud.rs` | acceptance |
| GOV-AUD-005 | `fn gov_aud_005_intent_precedes_outcome()` | `tests/spec_gov_aud.rs` | acceptance |
| GOV-AUD-005 | `fn gov_aud_005_error_path_writes_outcome()` | `tests/spec_gov_aud.rs` | acceptance |
| GOV-AUD-007 | `fn test_sink_opens_append_and_fsyncs()` | `src/governance/audit.rs` | unit |
| GOV-AUD-008 | `fn gov_aud_008_commits_and_policy_changes_audited()` | `tests/spec_gov_aud.rs` | acceptance |
| GOV-AUD-009 | `fn gov_aud_009_vacuum_and_retention_skip_audit()` | `tests/spec_gov_aud.rs` | acceptance |
| GOV-AUD-010 | `fn gov_aud_010_export_with_verifiable_manifest()` | `tests/spec_gov_aud.rs` | acceptance |
| GOV-AUD-011 | `fn gov_aud_011_executor_not_reachable_from_public_api()` | `tests/spec_gov_aud.rs` (compile_fail) | acceptance |
| GOV-AUD-012 | `fn gov_aud_012_intent_and_outcome_single_fsync()` | `tests/spec_gov_aud.rs` | acceptance |

## 7. Open questions
* Clock source: wall clock only, or also a monotonic `seq` from rhlog? `seq` is already the
  ordering; `ts` is informational. Confirm regulators accept this.
* Should the chain head be periodically anchored externally (e.g. printed to stdout / sent to a
  SIEM) so that truncating the whole log is also detectable? Proposed as M5 hardening.
* Record size for wide queries (hundreds of columns): cap `columns` at N and reference the
  plan hash instead?

## 8. Change log
| Date | Change |
|------|--------|
| 2026-09-02 | Created |
| 2026-09-02 | Session integration: 001 (fail closed for queries, writes, DDL and unauthenticated requests), 005 (intent/outcome on every path, error outcomes included), 008 (CREATE_TABLE, WRITE, ALTER audited with resulting versions), 011 (`compile_fail` doctest on the crate root; `Executor` is `pub(crate)`). CLI `rusthouse audit verify|export` wired in `src/main.rs`. 009 waits for LAKE-DML/GOV-RET; status stays Draft. |
| 2026-09-02 | Implemented sink, hash chain, verify, export in `src/governance/audit.rs` (GOV-AUD-002/003/004/006/007/010/012). Decisions: `status` gains `unauthenticated` for API-SQL-003; hash input is compact JSON with recursively sorted keys (own canonicaliser, not serde_json ordering); the chain head is repaired on open when it lags the last record by exactly one (crash between append and head update), any other mismatch refuses to open; the sink exposes `fsync_count()` and GOV-AUD-012 is asserted as one `sync_data` per `append_batch` (the `_chain_head` rename is not counted); an export filtered by principal is not a contiguous chain, so GOV-AUD-010 verifies each exported record's hash individually plus the manifest's `chain_head_hash` against `_chain_head`. Requirements 001/005/008/009/011 await the Session/Governor. |
