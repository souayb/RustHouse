# EXE-VEC: Arrow-native vectorized execution

| Field | Value |
|-------|-------|
| Status | Implemented |
| Owner | RustHouse maintainers |
| Related ADRs | ADR-0005, ADR-0006 |
| Depends on | CORE-EXE, LAKE-PQT, LAKE-TXN, API-SQL |
| Milestone | M2 (engine decision), M3 (full) |

## 1. Problem and motivation
BCBS 239 principle 5 (timeliness) and the operational reality of risk and fraud desks require
interactive latency over hundreds of millions of rows. The M1 executor (CORE-EXE) materialises
every cell as a JSON value; it is a correctness baseline, not an engine. Vectorized execution
over Arrow batches gives cache-friendly kernels and is the prerequisite for joins (a single-table
engine cannot compute a risk exposure) and for parallelism (EXE-PAR).

## 2. Scope
### In scope
* Arrow `RecordBatch` as the batch type between scan and result.
* Filter, projection, hash aggregate, sort, limit as vectorized operators; inner/left hash join.
* Governor rewrite of the engine's logical plan (ADR-0005) with the same observable behaviour
  as M1: classification fail-closed, columns read, `versions_read`, audit intent/outcome.
* Result equivalence with CORE-EXE for every M1 query shape.
### Out of scope
* Window functions, subqueries beyond `IN (SELECT ...)`, UDFs — later specs.
* Parallel scheduling (EXE-PAR). Write path (LAKE-WAL).

## 3. Requirements

| ID | Level | Requirement |
|----|-------|-------------|
| EXE-VEC-001 | MUST | Operators MUST consume and produce Arrow `RecordBatch`es; `ColumnData` MUST NOT appear between scan and result assembly. |
| EXE-VEC-002 | MUST | For every query in the CORE-EXE and API-SQL acceptance suites, the vectorized engine MUST return the same rows (order-insensitive unless `ORDER BY`) and the same JSON types as CORE-EXE; an equivalence suite runs both engines on the same snapshot. |
| EXE-VEC-003 | MUST | The Governor MUST rewrite the engine's logical plan before optimisation; a test MUST show that a row-filter predicate injected on a scan survives optimisation and that no optimizer rule removes a masking projection. The test MUST include a string-typed row policy (Parquet strings arrive as `Utf8View`; see ADR-0006 spike finding 1). |
| EXE-VEC-004 | MUST | It MUST be impossible to obtain an executable query context without the Governor installed: the constructor that installs it is the only `pub(crate)` path, and a `compile_fail` doctest covers the public API (extends API-SQL-001). |
| EXE-VEC-005 | MUST | Inner and left hash joins between two governed tables MUST be supported; the Governor MUST apply each side's row filters and masking independently before the join, and audit MUST record both tables' versions and columns. |
| EXE-VEC-006 | MUST | Lake scans MUST be exposed through a table provider bound to a pinned rhlog snapshot: the engine MUST never list the table directory and MUST read exactly the snapshot's active files (time travel, LAKE-TT-005/008 semantics preserved). |
| EXE-VEC-007 | MUST | File-level pruning MUST use the `add` action statistics before any I/O, and row-group pruning MUST use Parquet statistics, so that the row-group-pruned counter of LAKE-PQT-007 still increases. |
| EXE-VEC-008 | MUST | Full scan of 10 M rows across 20 files with `COUNT(*), SUM(x)` MUST complete in < 2 s cold and < 500 ms warm on the reference machine (release, single query, EXE-PAR parallelism allowed). |
| EXE-VEC-009 | MUST | Every executed plan MUST be recorded in the audit outcome record as a stable hash of its governed logical plan text, so a regulator can prove which rewritten plan produced a result. |
| EXE-VEC-010 | SHOULD | Nullable columns, `Timestamp` and `Decimal(38,18)` (LAKE-PQT-009) SHOULD flow through unchanged; monetary sums over `Decimal` SHOULD be exact. |
| EXE-VEC-011 | SHOULD | The engine SHOULD be swappable behind a `QueryEngine` trait so the CORE-EXE row engine remains available as an oracle for equivalence tests. |
| EXE-VEC-012 | MAY | Explain output (`EXPLAIN SELECT ...`) MAY be exposed through `/v1/query`, showing the governed plan. |

## 4. Design sketch
```rust
// src/exec/engine.rs
pub(crate) trait QueryEngine: Send + Sync {
    async fn execute(&self, governed: &GovernedPlan, sources: Vec<PinnedSnapshot>) -> Result<Vec<RecordBatch>>;
}
pub(crate) struct RowEngine;            // CORE-EXE, oracle
pub(crate) struct VectorEngine { .. }   // Arrow kernels or DataFusion (ADR-0006)

// with DataFusion:
struct RhlogTableProvider { snapshot: Snapshot, table_dir: PathBuf }   // impl TableProvider: scan() -> ParquetExec over snapshot.files
struct GovernorRule { principal: Principal, policies: PolicySet }      // impl AnalyzerRule
```
* `Session` keeps intent/outcome auditing and `versions_read`; the engine only executes.
* Constraints from the ADR-0006 spike: the Governor rule runs **before** `TypeCoercion` (or
  emits expressions cast to the column's Arrow type — Parquet strings arrive as `Utf8View`);
  the audit plan hash is taken from the analyzed+optimized plan, not the parsed one.
* Audit record gains `plan_hash: Option<String>` (GOV-AUD-002 amendment, verifier updated).
* Metrics: `rusthouse_exec_batches_total`, `rusthouse_exec_rows_scanned_total`,
  `rusthouse_exec_files_pruned_total`.

## 5. Acceptance scenarios

### EXE-VEC-003: Governor rewrite survives optimisation
```gherkin
Given a row policy country = 'FR' on gold.accounts for role analyst
When  SELECT count(*) FROM gold.accounts is planned for an analyst
Then  the optimised physical plan contains the predicate country = 'FR' below the aggregate
And   the row count equals the number of FR rows
```

### EXE-VEC-004: No ungoverned context
```gherkin
Given only the public API of the crate
When  code tries to construct the engine's session context directly
Then  it does not compile
```

### EXE-VEC-006: Provider reads exactly the pinned snapshot
```gherkin
Given gold.accounts at version 3 and a stray part-zzz.parquet in the table directory
When  SELECT count(*) FROM gold.accounts AS OF VERSION 3 runs
Then  the stray file is never opened (observed via a counting object-store shim)
```

### EXE-VEC-008: Performance budget
```gherkin
Given 20 Parquet files totalling 10 M rows
When  SELECT count(*), sum(balance) FROM t runs cold, then warm
Then  cold < 2 s and warm < 500 ms
```

## 6. Test plan (write these first)

| Requirement | Test | Location | Kind |
|-------------|------|----------|------|
| EXE-VEC-001 | `fn exe_vec_001_operators_exchange_record_batches()` | `tests/spec_exe_vec.rs` | acceptance |
| EXE-VEC-002 | `fn exe_vec_002_equivalent_to_row_engine()` | `tests/spec_exe_vec.rs` | property (both engines, generated queries) |
| EXE-VEC-003 | `fn exe_vec_003_governor_rewrite_survives_optimizer()` | `tests/spec_exe_vec.rs` | acceptance (deny-first) |
| EXE-VEC-004 | `fn exe_vec_004_no_ungoverned_context()` | `src/lib.rs` doctest `compile_fail` + `tests/spec_exe_vec.rs` | acceptance |
| EXE-VEC-005 | `fn exe_vec_005_join_applies_both_sides_policies()` | `tests/spec_exe_vec.rs` | acceptance |
| EXE-VEC-006 | `fn exe_vec_006_provider_reads_only_snapshot_files()` | `tests/spec_exe_vec.rs` | acceptance |
| EXE-VEC-007 | `fn exe_vec_007_file_and_row_group_pruning()` | `tests/spec_exe_vec.rs` | acceptance |
| EXE-VEC-008 | `fn exe_vec_008_full_scan_budget()` | `tests/spec_exe_vec.rs` | acceptance (`#[ignore]` debug, release CI) |
| EXE-VEC-009 | `fn exe_vec_009_audit_carries_plan_hash()` | `tests/spec_exe_vec.rs` | acceptance |
| EXE-VEC-010 | `fn exe_vec_010_decimal_sum_is_exact()` | `tests/spec_exe_vec.rs` | acceptance |

## 7. Open questions
* DataFusion vs own kernels: ADR-0006 spike decides.
* Should `versions_read` also carry the plan hash, or only the audit record? Lean: both.

## 8. Change log
| Date | Change |
|------|--------|
| 2026-09-02 | Created |
| 2026-09-03 | Implemented on DataFusion 55 (`src/exec/vector.rs`): `GovernedTableProvider` (visible schema, pinned files, `add`-statistics pruning inside `scan`), `GovernorRule` analyzer placed before `TypeCoercion`, `rh_mask` UDF, plan hash over the analyzed+optimized logical plan, `columns_read` from scan projections. `Session` defaults to the vector engine; `Session::with_row_engine()` keeps CORE-EXE as the oracle for EXE-VEC-002 (test lives in `tests/spec_exe_vec.rs` against the public API). All 34 M1 API-SQL acceptance tests pass unchanged on the new engine. EXE-VEC-010 (Decimal/Timestamp) still waits for LAKE-PQT-009. `AS OF` applies to every table in a statement (per-table pins are a follow-up). |
