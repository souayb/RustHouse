# CORE-PRN: As-built min/max segment pruning

| Field | Value |
|-------|-------|
| Status | Implemented |
| Owner | RustHouse maintainers |
| Related ADRs | ADR-0001 |
| Depends on | CORE-META, CORE-STO |
| Milestone | M0 (as-built) |

## 1. Problem and motivation
Skipping segments that cannot contain matching rows is the main lever for scan latency. The same
predicate semantics are reused by LAKE-PQT for Parquet row-group pruning, so the contract must
be explicit.

## 2. Scope
`Predicate` (Eq, Gt, Lt, Gte, Lte, And, Or) and `SegmentPruner::prune_segments`.

## 3. Requirements

| ID | Level | Requirement |
|----|-------|-------------|
| CORE-PRN-001 | MUST | Pruning is conservative: a segment is only dropped if **no** row in it can satisfy the predicate given its min/max statistics. |
| CORE-PRN-002 | MUST | `Eq(c, v)` keeps a segment iff `min(c) <= v <= max(c)`. |
| CORE-PRN-003 | MUST | `Gt(c, v)` keeps iff `max(c) > v`; `Gte` iff `max(c) >= v`; `Lt(c, v)` keeps iff `min(c) < v`; `Lte` iff `min(c) <= v`. |
| CORE-PRN-004 | MUST | `And` keeps iff both sides keep; `Or` keeps iff either side keeps. |
| CORE-PRN-005 | MUST | A segment with no statistics for the referenced column is always kept. |
| CORE-PRN-006 | MUST | String comparisons are lexicographic; numeric comparisons are performed on `f64`. |
| CORE-PRN-007 | SHOULD | Comparing values of different JSON types must not silently return "equal" (as-built: unequal types compare as 0, which is unsafe for `Eq`; tighten under LAKE-PQT). |

## 4. Design sketch
As built: `src/storage/pruner.rs`. Statistics come from `SegmentStats` (JSON values).

## 5. Acceptance scenarios

### CORE-PRN-001: no false negatives
```gherkin
Given segments [1..100], [101..200], [201..300] on column id
When  pruned with id = 150
Then  exactly the segment [101..200] remains
```

## 6. Test plan (as-built tests)

| Requirement | Test | Location | Kind |
|-------------|------|----------|------|
| CORE-PRN-001 | `fn test_prune_no_segments_match()` | `src/storage/pruner.rs` | unit |
| CORE-PRN-001 | `fn test_prune_all_segments_match()` | `src/storage/pruner.rs` | unit |
| CORE-PRN-002 | `fn test_prune_eq_predicate()` | `src/storage/pruner.rs` | unit |
| CORE-PRN-003 | `fn test_prune_gt_predicate()` | `src/storage/pruner.rs` | unit |
| CORE-PRN-003 | `fn test_prune_lt_predicate()` | `src/storage/pruner.rs` | unit |
| CORE-PRN-004 | `fn test_prune_and_predicate()` | `src/storage/pruner.rs` | unit |
| CORE-PRN-004 | `fn test_prune_or_predicate()` | `src/storage/pruner.rs` | unit |
| CORE-PRN-005 | `fn core_prn_005_missing_stats_keeps_segment()` | `tests/spec_core_prn.rs` | acceptance (to write) |
| CORE-PRN-006 | `fn test_string_comparison()` | `src/storage/pruner.rs` | unit |
| CORE-PRN-001 | `fn core_prn_001_pruning_is_conservative()` | `tests/spec_core_prn.rs` | property (to write) |

## 7. Open questions
* CORE-PRN-005 and the property test are red tests to write next.

## 8. Change log
| Date | Change |
|------|--------|
| 2026-09-02 | Created from as-built code |
