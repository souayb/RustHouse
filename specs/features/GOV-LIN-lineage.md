# GOV-LIN: Table and column lineage

| Field | Value |
|-------|-------|
| Status | Draft |
| Owner | RustHouse maintainers |
| Related ADRs | ADR-0002, ADR-0004, ADR-0005 |
| Depends on | LAKE-CAT, LAKE-TXN, LAKE-MED, GOV-CLS, GOV-AUD |
| Milestone | M4 |

## 1. Problem and motivation
BCBS 239 Principle 3 ("accuracy and integrity") and Principle 6 ("adaptability") require a bank
to trace any number in a risk report back to its sources and to assess the impact of a change
upstream before it is made. GDPR erasure (Art. 17) requires finding *every* copy of a subject's
data, including derived tables. GOV-CLS propagation requires knowing which source columns feed
which target columns.

RustHouse records **lineage edges at write time**, anchored to rhlog versions: whenever a write
to a table is produced by a query (CTAS, `INSERT ... SELECT`, medallion promotion), the Governor
already knows the source tables, their snapshot versions, and the column mapping. Persisting that
as a catalog table makes lineage itself versioned, auditable, and queryable with the same
time-travel semantics as data.

## 2. Scope
### In scope
* Edge model and persistence in `catalog.lineage` (rhlog table).
* Automatic capture for CTAS, `INSERT ... SELECT`, and LAKE-MED promotions.
* Upstream / downstream traversal, impact analysis, JSON graph export.
* Inputs to GOV-CLS propagation and GOV-RET erasure discovery.

### Out of scope
* Lineage of external producers writing through `/v1/write` with raw rows (recorded as
  `external:<principal>` source only).
* Visual lineage UI.

## 3. Requirements

| ID | Level | Requirement |
|----|-------|-------------|
| GOV-LIN-001 | MUST | Every write derived from a query MUST record one lineage edge `{ target: table@version, sources: [table@version], columns: [(target_col, [source_table.col])], request_id, principal, ts }` in the same request; if the lineage write fails, the data commit MUST be rolled back (or not performed) and the operation refused. |
| GOV-LIN-002 | MUST | Lineage MUST be stored as an rhlog table `catalog.lineage` (LAKE-TXN) so that `SHOW LINEAGE ... AS OF VERSION n` returns the graph as known at catalog version `n`. |
| GOV-LIN-003 | MUST | Column mapping MUST be derived from the resolved `LogicalPlan` (`src/sql/planner.rs`): a projected column maps to every source column referenced by its expression; aggregates map to their input columns; `*` expands to the resolved column list. |
| GOV-LIN-004 | MUST | `SHOW LINEAGE FOR <layer.table> UPSTREAM [DEPTH n]` MUST return all ancestor tables with the versions that contributed, transitively to depth `n` (default unbounded), terminating on cycles. |
| GOV-LIN-005 | MUST | `SHOW LINEAGE FOR <layer.table> DOWNSTREAM [DEPTH n]` MUST return all descendant tables, and `SHOW IMPACT OF <layer.table>.<column>` MUST return every downstream column that transitively depends on it. |
| GOV-LIN-006 | MUST | GOV-CLS propagation MUST use the column mapping from GOV-LIN-003 as its input, so that the recorded lineage and the applied classification always agree. |
| GOV-LIN-007 | MUST | For a row in a Gold table read at version `v`, `EXPLAIN LINEAGE` MUST return the chain of table versions down to the Bronze tables and the Parquet data files (`add` actions) present in those Bronze snapshots (BCBS 239 traceability). |
| GOV-LIN-008 | MUST | GOV-RET erasure MUST be able to call `downstream_tables_containing(source_table, columns)` to enumerate every table into which the given subject-identifying columns have flowed, including through renames and expressions. |
| GOV-LIN-009 | MUST | `rusthouse lineage export [--root <table>]` MUST emit a JSON graph `{ nodes: [table@version], edges: [...] }` suitable for regulator submission or import into an external catalog. |
| GOV-LIN-010 | MUST | Writes of raw rows via `/v1/write` (no source query) MUST still record an edge with a synthetic source `external:<principal>` so that Bronze tables are never lineage orphans. |
| GOV-LIN-011 | SHOULD | Lineage traversal for a graph of 10 000 edges SHOULD complete in under 100 ms using an in-memory adjacency index rebuilt on catalog version change. |
| GOV-LIN-012 | MAY | Edges MAY carry the SQL text hash so identical pipelines can be grouped in reports. |

## 4. Design sketch

```rust
// src/catalog/lineage.rs
pub struct TableVersion { pub table: TableRef, pub version: u64 }
pub struct ColumnMapping { pub target: String, pub sources: Vec<ColumnRef> }
pub struct LineageEdge {
    pub target: TableVersion, pub sources: Vec<TableVersion>,
    pub columns: Vec<ColumnMapping>, pub request_id: String, pub principal: String, pub ts: String,
    pub sql_hash: Option<String>,
}
pub struct LineageStore { catalog: Arc<Catalog>, index: RwLock<AdjacencyIndex> }
impl LineageStore {
    pub fn record(&self, edge: LineageEdge) -> Result<u64>;                       // GOV-LIN-001/002
    pub fn upstream(&self, t: &TableRef, depth: Option<u32>, as_of: Option<u64>) -> Result<Graph>;
    pub fn downstream(&self, t: &TableRef, depth: Option<u32>, as_of: Option<u64>) -> Result<Graph>;
    pub fn impact_of(&self, col: &ColumnRef) -> Result<Vec<ColumnRef>>;            // GOV-LIN-005
    pub fn downstream_tables_containing(&self, t: &TableRef, cols: &[String]) -> Result<Vec<TableRef>>; // GOV-LIN-008
    pub fn explain(&self, t: &TableRef, version: u64) -> Result<ProvenanceTree>;   // GOV-LIN-007
}
pub fn column_mapping_from_plan(plan: &LogicalPlan, resolved: &ResolvedSchema) -> Vec<ColumnMapping>; // GOV-LIN-003
```

* Capture point: `src/session.rs::execute_governed` for CTAS / `INSERT ... SELECT`;
  `src/lake/medallion.rs` for promotions; `src/http_api.rs` `/v1/write` for external edges.
* Atomicity (GOV-LIN-001): the lineage edge and the data commit are written in that order; if
  the data commit fails the edge is marked `aborted` by a follow-up commit; if the edge write
  fails the data commit is not attempted. A two-table transaction is out of scope for v1.
* Storage: `<data_dir>/catalog/lineage/_rhlog/` + Parquet, one row per edge, columns mapping
  stored as JSON string.
* `src/error.rs`: add `LineageUnavailable(String)`.
* Metrics: `rusthouse_lineage_edges_total`, `rusthouse_lineage_query_seconds`.

## 5. Acceptance scenarios

### GOV-LIN-001: Derived writes record an edge, or fail
```gherkin
Given silver.trades at version 4
When  analyst executes CREATE TABLE gold.daily_pnl AS SELECT trade_date, SUM(pnl) AS pnl FROM silver.trades GROUP BY trade_date
Then  catalog.lineage contains an edge target gold.daily_pnl@0, sources [silver.trades@4], columns [(trade_date,[silver.trades.trade_date]), (pnl,[silver.trades.pnl])]
Given a LineageStore that fails to write
When  the same CTAS is executed
Then  the operation is refused with LineageUnavailable and gold.daily_pnl does not exist
```

### GOV-LIN-002: Lineage is time-travelable
```gherkin
Given an edge recorded at catalog version 11
When  SHOW LINEAGE FOR gold.daily_pnl UPSTREAM AS OF VERSION 10 is executed
Then  no edge is returned
When  the same query is executed AS OF VERSION 11
Then  the edge is returned
```

### GOV-LIN-003: Column mapping follows the plan
```gherkin
Given a plan projecting a, b + c AS s, COUNT(*) AS n, * over table t(a, b, c)
When  column_mapping_from_plan runs
Then  a -> [t.a], s -> [t.b, t.c], n -> [t.a, t.b, t.c] (all inputs), and * expands to a, b, c each mapped to itself
```

### GOV-LIN-004: Upstream traversal with depth and cycle termination
```gherkin
Given edges bronze.raw -> silver.trades -> gold.daily_pnl -> gold.monthly_pnl
When  SHOW LINEAGE FOR gold.monthly_pnl UPSTREAM DEPTH 2 is executed
Then  gold.daily_pnl and silver.trades are returned, bronze.raw is not
Given an artificial cycle gold.a -> gold.b -> gold.a
When  UPSTREAM is executed without depth
Then  the query terminates and returns each node once
```

### GOV-LIN-005: Downstream and impact analysis
```gherkin
Given silver.customers.email feeds gold.contact.email and gold.marketing.email_hash
When  SHOW IMPACT OF silver.customers.email is executed
Then  gold.contact.email and gold.marketing.email_hash are returned
And   gold.marketing.balance is not
```

### GOV-LIN-006: Classification propagation uses the same mapping
```gherkin
Given silver.customers.email is Confidential/{PII}
When  CTAS creates gold.contact with email_hash = hash(email)
Then  the lineage edge maps email_hash -> [silver.customers.email]
And   gold.contact.email_hash carries tag PII (GOV-CLS-006) derived from that mapping
```

### GOV-LIN-007: From a Gold number back to Bronze files
```gherkin
Given gold.daily_pnl@3 derived from silver.trades@9 derived from bronze.trades_raw@21
When  EXPLAIN LINEAGE FOR gold.daily_pnl AS OF VERSION 3 is executed
Then  the result lists silver.trades@9, bronze.trades_raw@21
And   the Parquet files that were `add` actions in bronze.trades_raw's snapshot at version 21
```

### GOV-LIN-008: Erasure discovery finds every downstream copy
```gherkin
Given silver.customers(customer_id, email) feeds gold.contact(email) and gold.segments(customer_id)
When  downstream_tables_containing(silver.customers, [customer_id, email]) is called
Then  gold.contact and gold.segments are both returned
```

### GOV-LIN-009: JSON graph export
```gherkin
Given five tables with four edges
When  rusthouse lineage export --root gold.monthly_pnl runs
Then  the output is valid JSON with 5 nodes of the form layer.table@version and 4 edges each carrying request_id and principal
```

### GOV-LIN-010: External writes are not orphans
```gherkin
Given principal ingest_svc posts rows to /v1/write?table=bronze.trades_raw
When  the commit succeeds at version 22
Then  catalog.lineage has an edge target bronze.trades_raw@22 with sources [external:ingest_svc]
```

## 6. Test plan (write these first)

| Requirement | Test | Location | Kind |
|-------------|------|----------|------|
| GOV-LIN-001 | `fn gov_lin_001_lineage_write_failure_refuses_ctas()` | `tests/spec_gov_lin.rs` | acceptance (negative-first) |
| GOV-LIN-001 | `fn gov_lin_001_ctas_records_edge()` | `tests/spec_gov_lin.rs` | acceptance |
| GOV-LIN-002 | `fn gov_lin_002_lineage_is_time_travelable()` | `tests/spec_gov_lin.rs` | acceptance |
| GOV-LIN-003 | `fn gov_lin_003_column_mapping_follows_plan()` | `tests/spec_gov_lin.rs` | acceptance |
| GOV-LIN-003 | `fn test_column_mapping_star_and_aggregates()` | `src/catalog/lineage.rs` | unit |
| GOV-LIN-004 | `fn gov_lin_004_upstream_depth_and_cycles()` | `tests/spec_gov_lin.rs` | acceptance |
| GOV-LIN-005 | `fn gov_lin_005_downstream_and_impact()` | `tests/spec_gov_lin.rs` | acceptance |
| GOV-LIN-006 | `fn gov_lin_006_classification_uses_lineage_mapping()` | `tests/spec_gov_lin.rs` | acceptance |
| GOV-LIN-007 | `fn gov_lin_007_explain_reaches_bronze_files()` | `tests/spec_gov_lin.rs` | acceptance |
| GOV-LIN-008 | `fn gov_lin_008_downstream_tables_for_erasure()` | `tests/spec_gov_lin.rs` | acceptance |
| GOV-LIN-009 | `fn gov_lin_009_json_graph_export()` | `tests/spec_gov_lin.rs` | acceptance |
| GOV-LIN-010 | `fn gov_lin_010_external_write_records_synthetic_source()` | `tests/spec_gov_lin.rs` | acceptance |
| GOV-LIN-011 | `fn gov_lin_011_traversal_10k_edges_under_100ms()` | `benches/bench_lineage.rs` | benchmark (asserting) |

## 7. Open questions
* Row-level lineage (which source rows produced which target row) is what some auditors ask
  for; it is much more expensive. Proposed: version-level lineage plus reproducible queries
  (`versions_read`) is sufficient, documented as such.
* Should `hash(email)` be considered "containing" the subject's data for GOV-LIN-008? Proposed:
  yes, conservative by default.
* Two-phase commit between `catalog.lineage` and the target table would remove the `aborted`
  follow-up commit; defer to a multi-table transaction spec if needed.

## 8. Change log
| Date | Change |
|------|--------|
| 2026-09-02 | Created |
