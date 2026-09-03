# GOV-ACC: Access control — roles, grants, row filters, masking

| Field | Value |
|-------|-------|
| Status | Implemented |
| Owner | RustHouse maintainers |
| Related ADRs | ADR-0002, ADR-0004, ADR-0005 |
| Depends on | LAKE-CAT, LAKE-TXN, GOV-CLS, GOV-AUD, API-SQL |
| Milestone | M2 |

## 1. Problem and motivation
A bank's data platform is audited on "who can see what, and prove it". SOX requires
segregation of duties and evidence that access controls operate as documented. GDPR requires
purpose limitation and data minimisation: an analyst building a churn model does not need raw
e-mail addresses. PCI DSS requires cardholder data to be masked for anyone without a business
need for the full PAN. MNPI (material non-public information) must be walled off between desks.

RustHouse implements these as **policy-as-data** evaluated at a **single enforcement point**, the
Governor (ADR-0005), which rewrites the `LogicalPlan` before execution. Access is deny-by-default:
no grant, no data. Policies live in the catalog as an rhlog table, so "what policy was in force
on the 31st of March" is a time-travel query rather than a forensic exercise. Every decision is
audited (GOV-AUD).

## 2. Scope
### In scope
* `Principal { id, roles, attributes }` model and role hierarchy (flat roles in v1).
* Table- and column-level `GRANT` / `REVOKE` for `SELECT`, `INSERT`, `ALTER`, `DELETE`.
* Row filter policies bound to roles, with attribute substitution.
* Masking policies: `hash`, `redact`, `partial(last4)`, `null`.
* Plan rewriting in the Governor; policies versioned in the catalog.
* Aggregate-only access, segregation of duties.
* Audit and metrics for every decision; performance budget.

### Out of scope
* Authentication (token/mTLS parsing is API-SQL). GOV-ACC receives an already-built `Principal`.
* Column encryption and key access (GOV-ENC).
* Retention and erasure (GOV-RET).

## 3. Requirements

| ID | Level | Requirement |
|----|-------|-------------|
| GOV-ACC-001 | MUST | Access MUST be deny-by-default: a principal with no matching grant on a table receives `RustHouseError::Denied`, no data file is opened, and a GOV-AUD record with decision `deny` is written. |
| GOV-ACC-002 | MUST | The Governor MUST accept a `Principal { id: String, roles: BTreeSet<Role>, attributes: BTreeMap<String, String> }`; a request without a principal MUST be refused before parsing. |
| GOV-ACC-003 | MUST | `GRANT <priv> ON <layer.table> TO ROLE <r>` and `REVOKE` MUST be catalog commits (LAKE-TXN) with `commitInfo.operation = "GRANT"`/`"REVOKE"`, executable only by principals holding `security_admin`. |
| GOV-ACC-004 | MUST | Column-level `GRANT SELECT(col_a, col_b) ON t TO ROLE r` MUST restrict the role to the listed columns; `SELECT *` MUST be rewritten to the permitted subset, and an explicit reference to an unpermitted column MUST be refused with `Denied` naming the column. |
| GOV-ACC-005 | MUST | `CREATE ROW POLICY <p> ON <t> FOR ROLE <r> USING (<predicate>)` MUST cause the Governor to inject the predicate as a `Filter` node (`src/sql/planner.rs`, `Predicate` from `src/storage/pruner.rs`) under every scan of `t` by a principal holding `r`; multiple applicable policies are combined with AND. |
| GOV-ACC-006 | MUST | Row policy predicates MUST support `current_attribute('<key>')`, substituted from `Principal.attributes` at plan time; a missing attribute MUST evaluate to `Denied`, never to "no filter". |
| GOV-ACC-007 | MUST | `CREATE MASKING POLICY <m> ON <t>.<col> FOR ROLE <r> AS hash|redact|partial(last4)|null` MUST cause the Governor to replace the column projection with the masking expression for principals lacking an `UNMASK` grant; masked output MUST be type-stable (STRING stays STRING). |
| GOV-ACC-008 | MUST | Policies (grants, row policies, masking policies) MUST be stored in the catalog as an rhlog table `catalog.policies` so that `SHOW POLICIES ON t AS OF VERSION n` returns the policy set in force at catalog version `n`. |
| GOV-ACC-009 | MUST | Every allow, deny and mask decision MUST produce a GOV-AUD record naming the policy IDs applied and MUST increment `rusthouse_governor_decisions_total{decision, policy}`. |
| GOV-ACC-010 | MUST | Governor evaluation MUST complete in p99 < 1 ms for a table with 100 applicable policies, measured by a criterion benchmark; policy sets MUST be cached per catalog version. |
| GOV-ACC-011 | MUST | Filters injected by row policies MUST also be applied to segment pruning on the hot tier (HYB-ACC) and row-group pruning on the lake (LAKE-PQT) so that tier selection never widens visible rows. |
| GOV-ACC-012 | SHOULD | Segregation of duties: a principal holding `security_admin` SHOULD be refused `SELECT` on `Restricted` columns unless also holding an explicit data role, and both roles on one principal SHOULD raise an audit warning. |
| GOV-ACC-013 | SHOULD | `GRANT AGGREGATE ON t TO ROLE r` SHOULD permit `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` over `t` while refusing any non-aggregated projection; `MIN`/`MAX` on masked columns are refused. |
| GOV-ACC-014 | MAY | Role hierarchy (`GRANT ROLE analyst TO ROLE senior_analyst`) MAY be supported; v1 roles are flat. |

## 4. Design sketch

```rust
// src/governance/access.rs
pub struct Principal { pub id: String, pub roles: BTreeSet<Role>, pub attributes: BTreeMap<String, String> }
pub enum Privilege { Select { columns: Option<BTreeSet<String>> }, Insert, Alter, Delete, Unmask { column: String }, Aggregate }
pub struct Grant { pub role: Role, pub object: TableRef, pub privilege: Privilege, pub granted_by: String, pub version: u64 }
pub struct RowPolicy { pub id: PolicyId, pub table: TableRef, pub role: Role, pub predicate: PredicateTemplate }
pub enum MaskKind { Hash, Redact, PartialLast4, Null }
pub struct MaskingPolicy { pub id: PolicyId, pub column: ColumnRef, pub role: Role, pub kind: MaskKind }

pub struct Governor { catalog: Arc<Catalog>, audit: Arc<dyn AuditSink>, cache: PolicyCache }
pub struct Governed { pub plan: LogicalPlan, pub applied: Vec<PolicyId>, pub masked: Vec<String> }

impl Governor {
    /// ADR-0005 entry point. Never returns a plan for a Denied principal.
    pub fn govern(&self, principal: &Principal, plan: LogicalPlan, snapshot: &Snapshot) -> Result<Governed>;
}
```

* `LogicalPlan` (`src/sql/planner.rs`) gains a `Mask { input, column, kind }` node or, simpler,
  `Project` carries `Vec<ProjectedColumn>` where `ProjectedColumn::Masked { name, kind }`.
* `Predicate` (`src/storage/pruner.rs`) gets a `PredicateTemplate::bind(&Principal) -> Result<Predicate>`
  for `current_attribute()` substitution (GOV-ACC-006).
* `src/error.rs`: add `Denied { principal: String, object: String, reason: String }`.
* `catalog.policies` is a normal rhlog table (LAKE-TXN) under `<data_dir>/catalog/`; the
  `PolicyCache` is keyed by catalog version and invalidated on commit.
* `src/session.rs::execute_governed` is the only public path; `Executor` stays `pub(crate)`.
* Metrics: `rusthouse_governor_decisions_total{decision="allow|deny|mask",policy}`,
  `rusthouse_governor_eval_seconds` histogram.

## 5. Acceptance scenarios

### GOV-ACC-001: No grant means denied, and nothing is read
```gherkin
Given table gold.accounts exists and principal intern has no grants
When  intern executes SELECT * FROM gold.accounts
Then  the result is Denied
And   no Parquet or segment file under gold/accounts was opened (observed via a counting storage shim)
And   an audit record with decision "deny" and object gold.accounts exists
```

### GOV-ACC-002: Requests without a principal are refused before parsing
```gherkin
Given a Session
When  execute_governed is called with no Principal and the SQL "this is not sql"
Then  the error is Denied (not SqlParse)
```

### GOV-ACC-003: Grants are commits by security admins only
```gherkin
Given principal analyst without security_admin
When  analyst executes GRANT SELECT ON gold.accounts TO ROLE analyst
Then  the result is Denied
When  principal secadmin (security_admin) executes the same GRANT
Then  catalog.policies advances one version with commitInfo.operation = "GRANT" and principal secadmin
```

### GOV-ACC-004: Column-level grant narrows SELECT *
```gherkin
Given GRANT SELECT(id, balance) ON gold.accounts TO ROLE analyst
When  analyst executes SELECT * FROM gold.accounts
Then  the result has exactly the columns id and balance
When  analyst executes SELECT email FROM gold.accounts
Then  the result is Denied naming column email
```

### GOV-ACC-005: Row policy injects a filter
```gherkin
Given CREATE ROW POLICY fr_only ON gold.accounts FOR ROLE analyst USING (country = 'FR')
And   gold.accounts holds 3 FR rows and 2 DE rows
When  analyst executes SELECT COUNT(*) FROM gold.accounts
Then  the result is 3
And   the governed plan contains Filter(Eq(country, "FR")) above Scan(gold.accounts)
```

### GOV-ACC-006: Attribute substitution, missing attribute denies
```gherkin
Given CREATE ROW POLICY own_country ON gold.accounts FOR ROLE analyst USING (country = current_attribute('country'))
When  analyst with attributes {country: "DE"} queries gold.accounts
Then  only DE rows are returned
When  analyst with no country attribute queries gold.accounts
Then  the result is Denied with reason "missing_attribute:country"
```

### GOV-ACC-007: Masking policy rewrites the projection
```gherkin
Given CREATE MASKING POLICY pan_mask ON gold.payments.pan FOR ROLE analyst AS partial(last4)
And   a stored pan "4111111111111111"
When  analyst executes SELECT pan FROM gold.payments
Then  the value returned is "************1111" and the column type is STRING
When  principal fraud_lead with GRANT UNMASK(pan) executes the same query
Then  the value returned is "4111111111111111"
```

### GOV-ACC-008: Policies are time-travelable
```gherkin
Given a row policy created at catalog version 5 and dropped at version 8
When  SHOW POLICIES ON gold.accounts AS OF VERSION 6 is executed
Then  the policy is listed
When  SHOW POLICIES ON gold.accounts AS OF VERSION 9 is executed
Then  the policy is absent
```

### GOV-ACC-009: Decisions are audited and counted
```gherkin
Given one allowed, one denied and one masked query
When  the audit log and /metrics are read
Then  three audit records exist with decisions allow, deny, mask and the applied policy IDs
And   rusthouse_governor_decisions_total{decision="deny"} equals 1
```

### GOV-ACC-010: Evaluation stays under budget
```gherkin
Given a table with 100 applicable row and masking policies
When  govern() is benchmarked over 10 000 iterations with a warm cache
Then  p99 latency is below 1 ms
```

### GOV-ACC-011: Injected filters reach pruning on both tiers
```gherkin
Given a row policy country = 'FR' and segments/row-groups whose stats exclude FR
When  analyst scans the table on the hot tier and on the lake
Then  the excluded segments and row groups are pruned in both cases
And   both tiers return identical results
```

## 6. Test plan (write these first)

Deny-first ordering (Constitution 1): the first five tests below MUST be written and red
before any allow-path test exists.

| Requirement | Test | Location | Kind |
|-------------|------|----------|------|
| GOV-ACC-001 | `fn gov_acc_001_no_grant_is_denied_and_nothing_read()` | `tests/spec_gov_acc.rs` | acceptance (deny-first) |
| GOV-ACC-002 | `fn gov_acc_002_missing_principal_denied_before_parse()` | `tests/spec_gov_acc.rs` | acceptance (deny) |
| GOV-ACC-004 | `fn gov_acc_004_unpermitted_column_is_denied()` | `tests/spec_gov_acc.rs` | acceptance (deny) |
| GOV-ACC-006 | `fn gov_acc_006_missing_attribute_denies()` | `tests/spec_gov_acc.rs` | acceptance (deny) |
| GOV-ACC-003 | `fn gov_acc_003_grant_by_non_admin_is_denied()` | `tests/spec_gov_acc.rs` | acceptance (deny) |
| GOV-ACC-003 | `fn gov_acc_003_grant_is_a_catalog_commit()` | `tests/spec_gov_acc.rs` | acceptance |
| GOV-ACC-004 | `fn gov_acc_004_select_star_narrowed_to_granted_columns()` | `tests/spec_gov_acc.rs` | acceptance |
| GOV-ACC-005 | `fn gov_acc_005_row_policy_injects_filter()` | `tests/spec_gov_acc.rs` | acceptance |
| GOV-ACC-005 | `fn test_multiple_row_policies_are_anded()` | `src/governance/access.rs` | unit |
| GOV-ACC-006 | `fn gov_acc_006_attribute_substitution_filters_rows()` | `tests/spec_gov_acc.rs` | acceptance |
| GOV-ACC-006 | `fn test_predicate_template_bind()` | `src/governance/access.rs` | unit / property |
| GOV-ACC-007 | `fn gov_acc_007_masking_partial_last4_and_unmask_grant()` | `tests/spec_gov_acc.rs` | acceptance |
| GOV-ACC-007 | `fn test_mask_kinds_are_type_stable()` | `src/governance/access.rs` | unit / property |
| GOV-ACC-008 | `fn gov_acc_008_policies_are_time_travelable()` | `tests/spec_gov_acc.rs` | acceptance |
| GOV-ACC-009 | `fn gov_acc_009_decisions_audited_and_counted()` | `tests/spec_gov_acc.rs` | acceptance |
| GOV-ACC-010 | `fn gov_acc_010_governor_p99_under_1ms_100_policies()` | `benches/bench_governor.rs` | benchmark (asserting) |
| GOV-ACC-011 | `fn gov_acc_011_injected_filter_prunes_both_tiers()` | `tests/spec_gov_acc.rs` | acceptance |
| GOV-ACC-012 | `fn gov_acc_012_security_admin_cannot_read_restricted()` | `tests/spec_gov_acc.rs` | acceptance (deny) |
| GOV-ACC-013 | `fn gov_acc_013_aggregate_only_refuses_raw_rows()` | `tests/spec_gov_acc.rs` | acceptance (deny) |

## 7. Open questions
* Hash masking: keyed (HMAC with a per-table key from GOV-ENC) or plain SHA-256? Keyed is
  safer against dictionary attacks on PANs; decide with GOV-ENC.
* Should `Denied` reveal the column name (GOV-ACC-004)? It aids debugging but leaks schema.
  Proposed: name it in the audit record always, in the client error only for `Internal`-level
  tables.
* Policy conflict: two masking policies on the same column for two roles held by one principal.
  Proposed: most restrictive wins (`null` > `redact` > `hash` > `partial`).

## 8. Change log
| Date | Change |
|------|--------|
| 2026-09-02 | Created |
| 2026-09-03 | Implemented (001–011; 012/013 SHOULD deferred; 014 MAY not planned). Implementation notes: implementation done on the DataFusion plan type (ADR-0006), which changes the design sketch: no `Mask` node in our `LogicalPlan`; the Governor is a DataFusion `AnalyzerRule` (`src/exec/vector.rs::GovernorRule`) placed before `TypeCoercion` that wraps every governed `TableScan` in `Filter(row policies ANDed)` and `Projection(masked columns via the `rh_mask` UDF)`. Column-level grants narrow `SELECT *` by exposing a `GovernedTableProvider` whose visible schema is the permitted columns; an explicit reference to a hidden column maps to `Denied` naming the column. Decisions are pure functions in `src/governance/access.rs` (`decide_read`, `decide_write`, `decide_alter`). Policies are `CatalogEntry` variants, so `SHOW POLICIES ... AS OF VERSION n` replays the catalog log. Decisions recorded: (1) the table **owner** has every privilege on their own table (as in SQL databases; otherwise M1 workflows would need a bootstrap admin); (2) `classification_admin` may reclassify any table (GOV-CLS-011) without an ALTER grant; (3) masking conflict resolves to the most restrictive kind (`null` > `redact` > `hash` > `partial`); (4) hash masking is plain SHA-256 until GOV-ENC provides keys; (5) **limitation**: a row policy that references a column outside the principal's column grant is refused with `Denied` rather than evaluated on a hidden column — lifted when the provider learns hidden filter columns (tracked for M3); (6) GOV-ACC-010 is an `#[ignore]` release test in `tests/spec_gov_acc.rs` rather than a criterion bench; (7) GOV-ACC-011 covers the lake tier only — the hot tier is re-scoped by ADR-0006. |
