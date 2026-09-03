# GOV-CLS: Data classification and sensitivity tags

| Field | Value |
|-------|-------|
| Status | Draft |
| Owner | RustHouse maintainers |
| Related ADRs | ADR-0002, ADR-0004, ADR-0005 |
| Depends on | LAKE-CAT, LAKE-TXN |
| Milestone | M1 |

## 1. Problem and motivation
Every downstream governance control (access, masking, retention, erasure, audit) needs to know
*how sensitive* a piece of data is. Banks are required to know where personal data lives (GDPR
Art. 30 records of processing), where cardholder data lives (PCI DSS scope), and which datasets
feed regulatory reports (BCBS 239 data ownership). A column whose sensitivity is unknown cannot
be protected, and an auditor treats "unknown" as "unprotected".

RustHouse therefore makes classification **mandatory at creation time** and **versioned**: a
table cannot exist without a level on every column, the level is stored in the table's rhlog
`metaData` action so time travel answers "how was this column classified on date X", and
derived tables inherit the sensitivity of their sources so nothing gets laundered through a
`CREATE TABLE AS SELECT`.

## 2. Scope
### In scope
* Sensitivity levels, ordered: `Public < Internal < Confidential < Restricted`.
* Tags: `PII`, `PCI`, `MNPI`, `SPI` (GDPR Art. 9 special category), `FINANCIAL`, and
  user-defined `custom:<name>`.
* DDL syntax for classification on `CREATE TABLE` and `ALTER TABLE`.
* Storage of classification in rhlog `metaData`; catalog queries over classifications.
* Propagation to derived tables (inputs come from GOV-LIN edges).
* Tag-triggered default policies (hand-off to GOV-ACC).
* Fail-closed behaviour when classification is missing.

### Out of scope
* Automatic PII detection / content scanning (may be a later `GOV-CLS` extension).
* Enforcement of access itself (GOV-ACC), masking implementations (GOV-ACC), retention (GOV-RET).

## 3. Requirements

| ID | Level | Requirement |
|----|-------|-------------|
| GOV-CLS-001 | MUST | `CREATE TABLE` without a sensitivity level on every column MUST be refused with `RustHouseError::MissingClassification`, and no rhlog commit or catalog entry MUST be produced. |
| GOV-CLS-002 | MUST | Every lake schema field (`src/lake/schema.rs::Field`) MUST carry a `classification: Classification { level: Level, tags: BTreeSet<Tag> }`; `Level` is one of `Public`, `Internal`, `Confidential`, `Restricted`, totally ordered. The hot-tier `meta::Column` is derived from a field and carries no classification of its own. |
| GOV-CLS-003 | MUST | The table-level classification MUST be computed as the maximum column level and the union of column tags; it MUST NOT be independently settable lower than that. |
| GOV-CLS-004 | MUST | Classification MUST be serialised into the rhlog `metaData` action (LAKE-TXN) so that reading a table `AS OF VERSION n` returns the classification in force at version `n`. |
| GOV-CLS-005 | MUST | Changing a column's level or tags MUST be an `ALTER TABLE ... ALTER COLUMN ... CLASSIFIED AS` commit with `commitInfo.operation = "ALTER"` and MUST produce a GOV-AUD record naming principal, column, old and new classification. |
| GOV-CLS-006 | MUST | A table created or written from a query (CTAS, `INSERT ... SELECT`, medallion promotion) MUST inherit, per output column, at least the level of the source column(s) it derives from and the union of their tags; an explicit lower level on the target MUST be refused. |
| GOV-CLS-007 | MUST | Columns tagged `PCI`, `PII`, or `SPI` MUST have a default masking policy registered in GOV-ACC at creation time unless the creating principal explicitly overrides with `WITHOUT DEFAULT MASKING`, which is itself audited. |
| GOV-CLS-008 | MUST | The catalog MUST answer `SHOW COLUMNS WHERE TAG = 'PII'` (and by level) across all layers, returning `layer.table.column`, level, tags and the version at which the classification was last changed. |
| GOV-CLS-009 | MUST | Every GOV-AUD record for a query MUST include the classification (level + tags) of every column the governed plan reads, as it was at the snapshot version read. |
| GOV-CLS-010 | MUST | A read of a table whose stored metadata lacks classification (e.g. a hand-edited log) MUST be refused with `MissingClassification` rather than defaulting to `Public` (fail closed). |
| GOV-CLS-011 | MUST | Lowering a column's level (e.g. `Restricted` → `Internal`) MUST require a principal holding the `classification_admin` role and MUST be recorded with a mandatory free-text `REASON`. |
| GOV-CLS-012 | SHOULD | Bronze-layer tables SHOULD be allowed a table-wide default `CLASSIFIED AS Restricted` shorthand applied to every column, since raw ingest is treated as maximally sensitive until conformed in Silver. |
| GOV-CLS-013 | SHOULD | Custom tags SHOULD be declared once in the catalog (`CREATE TAG custom:trade_desk`) before use; use of an undeclared tag is refused. |
| GOV-CLS-014 | MAY | The catalog MAY expose a Prometheus gauge `rusthouse_classified_columns{level,tag}` for coverage dashboards. |

## 4. Design sketch

```rust
// src/governance/classification.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Level { Public, Internal, Confidential, Restricted }

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Tag { Pii, Pci, Mnpi, Spi, Financial, Custom(String) }

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Classification { pub level: Level, pub tags: BTreeSet<Tag> }

impl Classification {
    pub fn merge(&self, other: &Self) -> Self;           // max level, union tags (GOV-CLS-003/006)
    pub fn is_lowering_of(&self, prev: &Self) -> bool;   // GOV-CLS-011
}
```

* `src/meta.rs`: `Column` gains `pub classification: Classification` (no `Default` impl, so the
  compiler forces callers to supply it). `Table::classification()` computes the aggregate.
* rhlog `metaData.schema` (LAKE-TXN) carries the JSON of `Classification` per field under
  `metadata.rusthouse.classification`.
* `src/sql/parser.rs` / `planner.rs`: extend DDL parsing for `CLASSIFIED AS <Level> [TAGS (...)]`,
  `WITH CLASSIFICATION`, `WITHOUT DEFAULT MASKING`, `REASON '<text>'`. sqlparser's generic
  dialect does not know these clauses; they are parsed from a trailing `WITH (...)` options
  block or via a pre-pass, decision recorded in the spec change log when implemented.
* `src/error.rs`: add `MissingClassification(String)` and `ClassificationLowering(String)`.
* Governor (ADR-0005) consults `Classification` for every column in the plan; GOV-CLS-009 and
  GOV-CLS-010 are implemented in the Governor, not the executor.
* Propagation (GOV-CLS-006) is computed from the source plan's resolved columns at write time;
  GOV-LIN records the same edges.
* Metrics: `rusthouse_classification_refusals_total{reason}`.

## 5. Acceptance scenarios

### GOV-CLS-001: Unclassified table creation is refused
```gherkin
Given an authenticated principal with CREATE grant on layer silver
When  they execute CREATE TABLE silver.customers (id INT64, email STRING)
Then  the statement fails with MissingClassification naming columns id and email
And   no directory silver/customers/_rhlog exists
And   the catalog does not list silver.customers
```

### GOV-CLS-002: Column carries an ordered level and tag set
```gherkin
Given a Column built with Classification { level: Confidential, tags: {PII} }
When  it is compared with a Column at level Restricted
Then  Restricted > Confidential > Internal > Public holds
And   the tag set round-trips through serde JSON unchanged
```

### GOV-CLS-003: Table level is the maximum of its columns
```gherkin
Given columns id (Internal), email (Confidential, PII), card (Restricted, PCI)
When  the table classification is computed
Then  the level is Restricted and the tags are {PII, PCI}
And   an attempt to set the table level to Internal explicitly is refused
```

### GOV-CLS-004: Classification is versioned in rhlog
```gherkin
Given table silver.customers created at version 0 with email CLASSIFIED AS Confidential TAGS (PII)
And   an ALTER at version 1 changes email to Restricted
When  the schema is read AS OF VERSION 0
Then  email reports Confidential/{PII}
When  the schema is read AS OF VERSION 1
Then  email reports Restricted/{PII}
```

### GOV-CLS-005: Reclassification is a commit and is audited
```gherkin
Given table silver.customers at version 3
When  principal dpo executes ALTER TABLE silver.customers ALTER COLUMN email CLASSIFIED AS Restricted TAGS (PII, SPI)
Then  rhlog contains version 4 with commitInfo.operation = "ALTER" and principal = dpo
And   the audit log contains a record with object silver.customers, column email, old Confidential/{PII}, new Restricted/{PII,SPI}
```

### GOV-CLS-006: Derived tables inherit sensitivity
```gherkin
Given silver.customers.email is Confidential/{PII} and silver.cards.pan is Restricted/{PCI}
When  CREATE TABLE gold.contact AS SELECT email, pan FROM silver.customers JOIN silver.cards USING (id) is executed
Then  gold.contact.email is Confidential/{PII} and gold.contact.pan is Restricted/{PCI}
And   the same statement with pan CLASSIFIED AS Internal is refused with ClassificationLowering
```

### GOV-CLS-007: Sensitive tags register default masking
```gherkin
Given a principal creates gold.payments with pan CLASSIFIED AS Restricted TAGS (PCI)
When  the create commit succeeds
Then  GOV-ACC lists a masking policy default_mask_gold_payments_pan of kind partial(last4) applying to every role without explicit unmask grant
And   creating the same table WITHOUT DEFAULT MASKING produces no policy and an audit record with decision "override"
```

### GOV-CLS-008: Catalog lists columns by tag
```gherkin
Given three tables across bronze, silver and gold with five PII-tagged columns in total
When  SHOW COLUMNS WHERE TAG = 'PII' is executed by a principal with catalog read grant
Then  exactly five rows are returned with layer.table.column, level, tags and last_changed_version
```

### GOV-CLS-009: Audit records carry column classifications
```gherkin
Given a query SELECT email, balance FROM gold.accounts executed at version 7
When  the outcome audit record is read
Then  it lists email -> Confidential/{PII} and balance -> Confidential/{FINANCIAL} as of version 7
```

### GOV-CLS-010: Missing classification on read fails closed
```gherkin
Given a table whose rhlog metaData was written without classification metadata
When  any principal executes SELECT * FROM that table
Then  the query is refused with MissingClassification
And   an audit record with decision "deny" and reason "missing_classification" is written
```

### GOV-CLS-011: Lowering a level needs the admin role and a reason
```gherkin
Given gold.accounts.balance is Confidential
When  principal analyst (no classification_admin role) executes ALTER ... CLASSIFIED AS Internal REASON 'aggregated'
Then  the statement is refused with Denied
When  principal dpo (classification_admin) executes the same statement without REASON
Then  the statement is refused with a parse/validation error requiring REASON
When  dpo executes it with REASON 'aggregated per DPO ticket 123'
Then  the commit succeeds and the audit record contains the reason text
```

## 6. Test plan (write these first)

| Requirement | Test | Location | Kind |
|-------------|------|----------|------|
| GOV-CLS-001 | `fn gov_cls_001_create_without_classification_is_refused()` | `tests/spec_gov_cls.rs` | acceptance (deny-first) |
| GOV-CLS-010 | `fn gov_cls_010_read_of_unclassified_metadata_fails_closed()` | `tests/spec_gov_cls.rs` | acceptance (deny) |
| GOV-CLS-011 | `fn gov_cls_011_lowering_requires_admin_and_reason()` | `tests/spec_gov_cls.rs` | acceptance (deny) |
| GOV-CLS-002 | `fn gov_cls_002_level_ordering_and_tag_roundtrip()` | `tests/spec_gov_cls.rs` | acceptance |
| GOV-CLS-002 | `fn test_level_is_totally_ordered()` | `src/governance/classification.rs` | unit |
| GOV-CLS-003 | `fn gov_cls_003_table_level_is_max_of_columns()` | `tests/spec_gov_cls.rs` | acceptance |
| GOV-CLS-003 | `fn test_classification_merge_max_and_union()` | `src/governance/classification.rs` | unit / property |
| GOV-CLS-004 | `fn gov_cls_004_classification_is_time_travelable()` | `tests/spec_gov_cls.rs` | acceptance |
| GOV-CLS-005 | `fn gov_cls_005_reclassification_commit_is_audited()` | `tests/spec_gov_cls.rs` | acceptance |
| GOV-CLS-006 | `fn gov_cls_006_ctas_inherits_max_level_and_tags()` | `tests/spec_gov_cls.rs` | acceptance |
| GOV-CLS-006 | `fn gov_cls_006_ctas_lowering_is_refused()` | `tests/spec_gov_cls.rs` | acceptance (deny) |
| GOV-CLS-007 | `fn gov_cls_007_pci_tag_registers_default_masking()` | `tests/spec_gov_cls.rs` | acceptance |
| GOV-CLS-008 | `fn gov_cls_008_show_columns_by_tag()` | `tests/spec_gov_cls.rs` | acceptance |
| GOV-CLS-009 | `fn gov_cls_009_audit_record_carries_classifications()` | `tests/spec_gov_cls.rs` | acceptance |
| GOV-CLS-012 | `fn gov_cls_012_bronze_table_wide_shorthand()` | `tests/spec_gov_cls.rs` | acceptance |
| GOV-CLS-013 | `fn gov_cls_013_undeclared_custom_tag_refused()` | `tests/spec_gov_cls.rs` | acceptance (deny) |

## 7. Open questions
* DDL syntax: extend sqlparser via a custom dialect, or accept classification in a
  `WITH (classification = '...')` options map? Decide before GOV-CLS-001 goes green.
* Should `Level` be extensible per institution (e.g. a fifth `TopSecret`)? Current answer: no,
  use tags for finer granularity.
* How does propagation (GOV-CLS-006) treat expressions such as `hash(email)`: does hashing lower
  PII to pseudonymous? Proposed: keep PII tag, allow level lowering only with `REASON`.

## 8. Change log
| Date | Change |
|------|--------|
| 2026-09-02 | Created |
| 2026-09-02 | Implemented 001, 004, 005, 009, 010, 011 end-to-end (`src/sql/ddl.rs`, `src/governance/governor.rs`, `src/session.rs`). DDL decision (open question 1): classification is given in a trailing `WITH CLASSIFICATION (col Level [TAG...], ..., * Level)` block; `* Level` is the GOV-CLS-012 shorthand; inline `CLASSIFIED AS` on column definitions is not supported by sqlparser and is not planned. Reclassification audit records carry the previous value under `<column>.previous` in `classifications`; the reason text lives in the rhlog `commitInfo.operation_parameters`. 006 (CTAS propagation), 007 (default masking) and 008 (`SHOW COLUMNS`) wait for GOV-LIN / GOV-ACC; status stays Draft. |
| 2026-09-02 | GOV-CLS-002 re-targeted from `meta::Column` to the lake `Field`: the hot tier is derived and never authoritative (ADR-0001), so classification lives on the lake schema. `Field.classification` is `Option` on disk so a hand-edited log yields `MissingClassification` (GOV-CLS-010) instead of a deserialisation error; every constructor sets it. Types implemented in `src/governance/classification.rs`; 002, 003, 010 green. |
