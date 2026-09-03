# EXE-PAR: Parallel scan and execution

| Field | Value |
|-------|-------|
| Status | Draft |
| Owner | RustHouse maintainers |
| Related ADRs | ADR-0005, ADR-0006 |
| Depends on | EXE-VEC, LAKE-PQT |
| Milestone | M3 |

## 1. Problem and motivation
A snapshot is a list of independent immutable files; scanning them one after another leaves
every core but one idle. Parallel scan is the cheapest large speed-up available and needs no
governance changes because it happens below the Governor (ADR-0005): the governed plan is
fixed before any file is touched.

## 2. Scope
### In scope
Parallel file/row-group scanning, partitioned hash aggregation with final merge, parallel sort
with k-way merge, bounded memory, cancellation, per-query CPU budget.
### Out of scope
Distributed execution across nodes.

## 3. Requirements

| ID | Level | Requirement |
|----|-------|-------------|
| EXE-PAR-001 | MUST | A scan over N files MUST be able to read and filter up to `max_parallelism` files concurrently (default: available cores); results MUST be identical to the sequential scan (order-insensitive unless `ORDER BY`). |
| EXE-PAR-002 | MUST | Parallelism MUST be applied after governance: the governed plan (row filters, masking, projection) is fixed before any task is spawned, and every task executes the same governed plan fragment. |
| EXE-PAR-003 | MUST | Memory MUST be bounded by a per-query budget (`max_query_memory_bytes`, default 1 GiB); exceeding it MUST fail the query with `RustHouseError::QueryExecution("memory budget exceeded")`, never OOM the process. |
| EXE-PAR-004 | MUST | Cancelling a request (client disconnect or timeout `query_timeout_ms`) MUST stop all its tasks within 100 ms and MUST still write the audit outcome record with status `error` and reason `cancelled`. |
| EXE-PAR-005 | MUST | `COUNT/SUM/AVG/MIN/MAX ... GROUP BY` over 10 M rows in 20 files MUST scale: 8 threads MUST be at least 4× faster than 1 thread on the reference machine. |
| EXE-PAR-006 | MUST | Concurrent queries from different principals MUST never share operator state or batches (no cross-principal data leakage via caches); a test runs two principals with different row policies concurrently and checks every returned row against its policy. |
| EXE-PAR-007 | SHOULD | Row groups within one large file SHOULD be scanned in parallel as well, so a single-file table also benefits. |
| EXE-PAR-008 | MAY | A scheduler MAY prioritise short queries over long scans (two-queue fairness). |

## 4. Design sketch
With DataFusion (ADR-0006): `target_partitions = max_parallelism`, `ParquetExec` partitions over
the snapshot's files, `MemoryPool` for EXE-PAR-003, `tokio` cancellation via dropping the
stream for EXE-PAR-004. With own kernels: a morsel-driven scheduler over `tokio::task::spawn_blocking`
with a bounded channel, partial aggregates merged by the coordinator. Config additions in
`src/config.rs`: `max_parallelism`, `max_query_memory_bytes`, `query_timeout_ms`.
Metrics: `rusthouse_exec_tasks_total`, `rusthouse_exec_cancelled_total`, `rusthouse_exec_memory_bytes`.

## 5. Acceptance scenarios

### EXE-PAR-003: Memory budget fails closed
```gherkin
Given max_query_memory_bytes = 64 MiB and a 10 M row sort
When  the query runs
Then  it fails with QueryExecution("memory budget exceeded") and the process RSS stays under budget + baseline
And   the audit outcome record has status error
```

### EXE-PAR-006: No cross-principal leakage under concurrency
```gherkin
Given row policies country='FR' for role fr_desk and country='DE' for role de_desk
When  50 queries per role run concurrently on the same table
Then  every row returned to fr_desk has country FR and every row to de_desk has country DE
```

## 6. Test plan (write these first)

| Requirement | Test | Location | Kind |
|-------------|------|----------|------|
| EXE-PAR-006 | `fn exe_par_006_no_cross_principal_leakage_under_concurrency()` | `tests/spec_exe_par.rs` | acceptance (deny-first) |
| EXE-PAR-003 | `fn exe_par_003_memory_budget_fails_closed()` | `tests/spec_exe_par.rs` | acceptance (negative) |
| EXE-PAR-004 | `fn exe_par_004_cancellation_stops_tasks_and_audits()` | `tests/spec_exe_par.rs` | acceptance (negative) |
| EXE-PAR-001 | `fn exe_par_001_parallel_scan_matches_sequential()` | `tests/spec_exe_par.rs` | property |
| EXE-PAR-002 | `fn exe_par_002_governed_plan_fixed_before_spawn()` | `tests/spec_exe_par.rs` | acceptance |
| EXE-PAR-005 | `fn exe_par_005_scaling_budget()` | `tests/spec_exe_par.rs` | acceptance (`#[ignore]` debug, release CI) |
| EXE-PAR-007 | `fn exe_par_007_row_groups_scanned_in_parallel()` | `tests/spec_exe_par.rs` | acceptance |

## 7. Open questions
* Thread pool: share tokio's blocking pool or a dedicated compute pool? Lean: dedicated
  (`rayon` or DataFusion's own), so I/O and CPU do not starve each other.

## 8. Change log
| Date | Change |
|------|--------|
| 2026-09-02 | Created |
