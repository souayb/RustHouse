# ADR-0006: Arrow-native vectorized execution, with DataFusion as the candidate engine

| Field | Value |
|-------|-------|
| Status | Accepted (2026-09-03, M2 kick-off) |
| Date | 2026-09-02 |
| Deciders | RustHouse maintainers |

## Context
The M1 executor (`src/exec/`, CORE-EXE) evaluates rows one at a time as JSON values. It is
correct and fully governed, but it cannot serve a risk or fraud desk: no joins, no
parallelism, and per-cell allocation. Meanwhile the Parquet layer (LAKE-PQT) already produces
Arrow `RecordBatch`es and converts them to `ColumnData` only to satisfy the old executor.

Access control (GOV-ACC, M2) rewrites logical plans: it injects row-filter predicates, replaces
projected columns with masking expressions, and strips unauthorised columns (ADR-0005). The plan
representation the Governor targets is therefore a long-lived contract. Choosing the engine
after M2 would mean writing the Governor's rewriter twice.

Two options:

1. **Build.** Keep our `LogicalPlan`, add Arrow compute kernels (filter, take, hash aggregate,
   sort), a hash join, and a morsel-driven parallel scheduler.
2. **Adopt Apache DataFusion.** Its `LogicalPlan` is Arrow-native, it ships vectorized operators,
   joins, window functions, a cost-based optimizer, parallel Parquet scans with page-index and
   bloom-filter pruning, and its `AnalyzerRule` extension point runs before optimisation, which
   is exactly where a Governor must sit (a rewrite the optimizer cannot undo).

## Decision
* **Arrow `RecordBatch` becomes the internal batch format now**, regardless of engine.
  `ColumnData` remains the hot-tier on-disk encoding and a test convenience; it is no longer what
  operators consume. (EXE-VEC-001.)
* **DataFusion is the candidate engine.** It is adopted if the spike criteria below hold;
  otherwise option 1 proceeds under EXE-VEC/EXE-PAR with our own kernels.
* **The Governor stays ours and stays mandatory.** With DataFusion it is implemented as an
  `AnalyzerRule` registered on a `SessionState` that RustHouse constructs; no `SessionContext`
  is ever created without it, and the `compile_fail` doctest guarding the executor (API-SQL-001)
  is extended to the DataFusion context. Audit intent/outcome, `versions_read`, and
  classification checks remain in `Session` (API-SQL), which is the only public entry point.
* **rhlog remains the table format.** A `TableProvider` implementation exposes a pinned rhlog
  snapshot (list of Parquet files + statistics) to the engine, so time travel and file-level
  pruning keep working and DataFusion never lists directories itself.

## Decision criteria for the spike (recorded below when measured)
| Criterion | Threshold | Why |
|-----------|-----------|-----|
| Governor as `AnalyzerRule` | a row filter injected on `TableScan` is present in the final plan and cannot be optimised away | ADR-0005 |
| Release build on the reference machine (16 GB, `-j 2`) | completes; peak RSS < 6 GB | the machine already OOMs with default parallelism |
| Clean release build time | < 15 min | developer loop |
| 10 M rows, `count/sum` full scan, 20 Parquet files | < 2 s cold | parity with hot-tier expectations in README |
| Arrow version alignment | DataFusion's `arrow` major == ours, or we move to its version | one Arrow in the tree |
| Dependency count | documented; each additional crate is a supply-chain review item for a bank | DORA / third-party risk |

## Consequences
### Positive
* Joins, window functions, and parallel scans arrive without months of kernel work.
* Governance rewrites operate on a mature plan type with visitors and a display format that
  can be logged in audit records (plan hash).
* Page-index pruning and bloom filters make HYB-ACC-005 ("is Parquet fast enough to retire the
  hot tier?") likely to resolve *yes*, which removes an entire storage path.
### Negative
* A large dependency tree; each DataFusion major (roughly quarterly) may break our
  `AnalyzerRule` and `TableProvider` code. Mitigation: pin exact versions, upgrade on a
  schedule, keep the Governor's tests behaviour-based.
* Compile time and memory on small machines. Mitigation: `default-features = false`, only
  `parquet` and `sql` features.
* Less control over operator internals. Acceptable: our differentiator is governance, not
  hash-join implementation.

## Alternatives considered
* Polars: excellent kernels, weaker SQL/plan extension story, lazy plans not designed for an
  external analyzer hook. Rejected.
* Keep the row executor and add threads: does not address vectorisation or joins. Rejected.

## Requirements affected
CORE-EXE (superseded by EXE-VEC), EXE-PAR (new), GOV-ACC (rewrite target), HYB-ACC (scope
shrinks if HYB-ACC-005 resolves yes), LAKE-PQT (bridge becomes one-directional), API-SQL-001.

## Spike evidence (2026-09-02, `specs/adr/spikes/ADR-0006-datafusion-spike.rs`)
Standalone crate: `datafusion = "55"` with `default-features = false, features = ["parquet","sql"]`,
20 Parquet files × 500 000 rows (10 M rows, 4 columns), a Governor stand-in `AnalyzerRule` that
appends `country = 'FR'` to every scan of `accounts`. Reference machine: WSL2, 16 GB, `-j 2`.

| Criterion | Threshold | Measured | Verdict |
|-----------|-----------|----------|---------|
| Governor as `AnalyzerRule` | filter present and enforced | `count(*)` = 3 333 340 (the FR third of 10 M); `GROUP BY country` returns only FR; point lookup `id BETWEEN 100 AND 110` returns only ids 102, 105, 108 | pass |
| Release build, peak memory | completes, < 6 GB | completes; peak RSS 1.08 GB | pass |
| Clean release build time | < 15 min | 21 min 25 s at `-j 2` (dependencies); incremental rebuild 1 min 56 s | **fail on this machine**; expected to pass at `-j 8` in CI — re-measure there |
| 10 M rows `count/sum`, 20 files | < 2 s cold | 116 ms first run, 80 ms warm; `GROUP BY` 75–88 ms; filtered lookup 16–22 ms | pass (≈20× margin) |
| Arrow alignment | same major | DataFusion 55 → arrow 59.3, identical to ours | pass |
| Dependency count | documented | 246 crates in the spike lock file (RustHouse alone: see `Cargo.lock`); unstripped release binary 165 MB | recorded; supply-chain review item |

Two findings that become design constraints in EXE-VEC:
1. **Rule ordering.** `SessionStateBuilder::with_analyzer_rule` appends after the default rules,
   i.e. after `TypeCoercion`. An injected `col("country") = lit("FR")` failed at execution with
   `Invalid comparison operation: Utf8View == Utf8` because DataFusion 55 reads Parquet strings
   as `Utf8View`. The Governor must either emit type-correct expressions (cast to the column's
   type, as the spike does) or be registered *before* `TypeCoercion`. EXE-VEC-003's test must
   cover a string row policy for exactly this reason.
2. **Plan hash source.** `DataFrame::logical_plan()` returns the plan *before* analysis; it did
   not show the injected filter even though execution enforced it. EXE-VEC-009 must hash the
   analyzed + optimized plan (`SessionState::optimize` output or the physical plan display),
   never the raw parsed plan.

## Change log
| Date | Change |
|------|--------|
| 2026-09-02 | Proposed |
| 2026-09-03 | Accepted at M2 kick-off. `datafusion = "55.0"` (parquet, sql features only) added to the crate. |
| 2026-09-02 | Spike run and recorded. All criteria pass except clean-build time on the 2-job reference machine; recommendation is to accept, re-measuring build time on CI hardware. Spike source kept under `specs/adr/spikes/` as evidence only (not compiled, per ADR-0004 spike rule). |
