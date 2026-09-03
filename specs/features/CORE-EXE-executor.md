# CORE-EXE: Single-table vectorised executor

| Field | Value |
|-------|-------|
| Status | Implemented |
| Owner | RustHouse maintainers |
| Related ADRs | ADR-0001, ADR-0005 |
| Depends on | CORE-SQL, CORE-STO, LAKE-PQT |
| Milestone | M1 |

## 1. Problem and motivation
`TECHNICAL.md` Step 5 sketched an executor that never landed. The governed session (API-SQL)
needs a component that turns a governed `LogicalPlan` plus columnar input batches into rows.
The executor is deliberately dumb: it receives batches already chosen and pruned by the tier
router and never opens files itself, so it cannot bypass governance (ADR-0005, GOV-AUD-011).

## 2. Scope
### In scope
Filter (exact row evaluation), Project, Aggregate (COUNT/SUM/AVG/MIN/MAX with GROUP BY), Sort,
Limit over `ColumnData` batches; conversion to JSON rows.
### Out of scope
Joins, subqueries, expressions in projections, file I/O, tier selection (HYB-ACC).

## 3. Requirements

| ID | Level | Requirement |
|----|-------|-------------|
| CORE-EXE-001 | MUST | The executor MUST be `pub(crate)`: it is reachable only through `Session::execute_governed` (API-SQL-001). |
| CORE-EXE-002 | MUST | `Filter` MUST evaluate the predicate exactly per row with the same comparison semantics as `SegmentPruner` (numeric on f64, lexicographic strings, booleans); rows whose column is null MUST NOT match any comparison. |
| CORE-EXE-003 | MUST | `Project` MUST return exactly the requested columns in the requested order and MUST fail with `ColumnNotFound` for an unknown column. |
| CORE-EXE-004 | MUST | `Aggregate` MUST support `COUNT(*)`, `COUNT(col)` (non-null only), `SUM`, `AVG`, `MIN`, `MAX` with optional `GROUP BY`; groups MUST be emitted in first-seen order; aggregates over zero rows without GROUP BY MUST return one row with COUNT 0 and null for the others. |
| CORE-EXE-005 | MUST | `Sort` MUST be stable, MUST order nulls last, and MUST honour ASC/DESC per key; `Limit` MUST truncate after sorting. |
| CORE-EXE-006 | MUST | Multiple input batches (one per data file) MUST be processed as one logical table: filters and projections per batch, aggregates and sorts across all batches. |
| CORE-EXE-007 | MUST | Output rows MUST be JSON values with the column order of the final plan node; `Int64` stays an integer, `Float64` a float, nulls `null`. |
| CORE-EXE-008 | SHOULD | Filtering SHOULD short-circuit `AND` and `OR`. |

## 4. Design sketch
`src/exec/mod.rs`: `pub(crate) struct Executor; pub(crate) fn execute(plan: &LogicalPlan, input: Vec<Batch>) -> Result<ResultSet>`.
`Batch { columns: Vec<Column>, data: HashMap<String, ColumnData> }`, `ResultSet { columns: Vec<String>, rows: Vec<Vec<serde_json::Value>> }`.
Values are read through one `value_at(&ColumnData, i) -> Option<Value>` helper so nullable
variants (LAKE-PQT-008) are handled in one place.

## 5. Acceptance scenarios

### CORE-EXE-002: exact filter
```gherkin
Given a batch with id = [1, 5, 10] and name = ["a", "b", "c"]
When  the plan Filter(id > 3 AND name < "c") is executed
Then  exactly the row (5, "b") is returned
```

### CORE-EXE-004: grouped aggregates
```gherkin
Given rows (grp, v): (x,1), (y,2), (x,3)
When  SELECT grp, COUNT(*), SUM(v), AVG(v), MIN(v), MAX(v) GROUP BY grp is executed
Then  rows are [x, 2, 4, 2.0, 1, 3] then [y, 1, 2, 2.0, 2, 2]
```

## 6. Test plan (write these first)

| Requirement | Test | Location | Kind |
|-------------|------|----------|------|
| CORE-EXE-001 | `fn api_sql_001_executor_is_crate_private()` | `tests/spec_api_sql.rs` | acceptance |
| CORE-EXE-002 | `fn core_exe_002_filter_is_exact_and_null_safe()` | `src/exec/mod.rs` | unit |
| CORE-EXE-003 | `fn core_exe_003_project_order_and_unknown_column()` | `src/exec/mod.rs` | unit |
| CORE-EXE-004 | `fn core_exe_004_grouped_aggregates()` | `src/exec/mod.rs` | unit |
| CORE-EXE-004 | `fn core_exe_004_aggregate_over_empty_input()` | `src/exec/mod.rs` | unit |
| CORE-EXE-005 | `fn core_exe_005_sort_stable_nulls_last_then_limit()` | `src/exec/mod.rs` | unit |
| CORE-EXE-006 | `fn core_exe_006_multiple_batches_act_as_one_table()` | `src/exec/mod.rs` | unit |
| CORE-EXE-007 | `fn core_exe_007_json_row_types()` | `src/exec/mod.rs` | unit |

## 7. Open questions
* Vectorised (Arrow compute kernels) vs row-at-a-time: M1 is row-at-a-time over `ColumnData`;
  HYB-ACC benchmarks decide whether to move to Arrow kernels.

## 8. Change log
| Date | Change |
|------|--------|
| 2026-09-02 | Created (executor had no spec; TECHNICAL.md Step 5 superseded) |
| 2026-09-02 | Implemented in `src/exec/mod.rs` (row-at-a-time over `ColumnData`, nullable variants handled in `value_at`). `SELECT *` without projection and `COUNT(*)` are governed as "all columns" / "no columns" respectively; the session reads one column for `COUNT(*)` to know the row count. |
