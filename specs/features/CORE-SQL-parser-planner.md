# CORE-SQL: As-built SQL parsing and logical planning

| Field | Value |
|-------|-------|
| Status | Implemented |
| Owner | RustHouse maintainers |
| Related ADRs | ADR-0005 |
| Depends on | CORE-PRN |
| Milestone | M0 (as-built) |

## 1. Problem and motivation
SQL is the single query surface. The planner output (`LogicalPlan`) is also the object the
Governor rewrites (ADR-0005), so its shape is a governance contract, not just an engine detail.

## 2. Scope
`Parser` (sqlparser wrapper), `Planner` (`Scan`, `Filter`, `Project`, `Aggregate`).
### Out of scope
Execution (TECHNICAL.md Step 5 sketch, finished under M1), joins, subqueries, DDL (API-SQL),
`AS OF` (LAKE-TT).

## 3. Requirements

| ID | Level | Requirement |
|----|-------|-------------|
| CORE-SQL-001 | MUST | A single `SELECT` statement parses; empty input and multiple statements are rejected with `SqlParse`. |
| CORE-SQL-002 | MUST | Syntactically invalid SQL is rejected with `SqlParse`, never a panic. |
| CORE-SQL-003 | MUST | `SELECT cols FROM t` plans to `Project(cols) -> Scan(t)`. |
| CORE-SQL-004 | MUST | A `WHERE` clause with `=`, `>`, `<`, `>=`, `<=`, `AND`, `OR` plans to a `Filter(Predicate)` between projection and scan. |
| CORE-SQL-005 | MUST | Aggregates `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` with optional `GROUP BY` plan to an `Aggregate` node. |
| CORE-SQL-006 | MUST | Non-`SELECT` statements are rejected by the planner with `SqlParse` (until API-SQL adds DDL). |
| CORE-SQL-007 | SHOULD | The planner should expose a `LogicalPlan::rewrite` visitor so the Governor can inject predicates and masking expressions without pattern-matching every variant. |

## 4. Design sketch
As built: `src/sql/parser.rs`, `src/sql/planner.rs`. `sqlparser 0.43` with `GenericDialect`.

## 5. Acceptance scenarios

### CORE-SQL-004: WHERE becomes a Filter node
```gherkin
Given SELECT id, name FROM users WHERE id > 100
When  planned
Then  the plan is Project([id,name]) -> Filter(Gt(id,100)) -> Scan(users)
```

## 6. Test plan (as-built tests)

| Requirement | Test | Location | Kind |
|-------------|------|----------|------|
| CORE-SQL-001 | `fn test_parse_simple_select()` | `src/sql/parser.rs` | unit |
| CORE-SQL-001 | `fn test_parse_multiple_statements()` | `src/sql/parser.rs` | unit |
| CORE-SQL-002 | `fn test_parse_invalid_sql()` | `src/sql/parser.rs` | unit |
| CORE-SQL-003 | `fn test_plan_simple_select()` | `src/sql/planner.rs` | unit |
| CORE-SQL-004 | `fn test_parse_select_with_where()` | `src/sql/parser.rs` | unit |
| CORE-SQL-004 | `fn test_plan_select_with_filter()` | `src/sql/planner.rs` | unit |
| CORE-SQL-004 | `fn test_plan_complex_predicate()` | `src/sql/planner.rs` | unit |
| CORE-SQL-005 | `fn test_parse_select_with_aggregation()` | `src/sql/parser.rs` | unit |
| CORE-SQL-005 | `fn test_plan_aggregation()` | `src/sql/planner.rs` | unit |
| CORE-SQL-006 | `fn core_sql_006_non_select_rejected()` | `tests/spec_core_sql.rs` | acceptance (to write) |

## 7. Open questions
* `sqlparser 0.43` is old; `AS OF` and policy DDL may require an upgrade. Decide in LAKE-TT.

## 8. Change log
| Date | Change |
|------|--------|
| 2026-09-02 | Created from as-built code |
