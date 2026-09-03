# ADR-0001: Evolve RustHouse into a hybrid Lakehouse + hot-tier warehouse

| Field | Value |
|-------|-------|
| Status | Accepted |
| Date | 2026-09-02 |
| Deciders | RustHouse maintainers |

## Context

RustHouse today is a single-node, ClickHouse-style OLAP warehouse: rows are ingested into
proprietary LZ4-compressed column files (`col_N.bin`), described by a single `metadata.json`,
pruned by per-segment min/max statistics, and queried through a SQL parser and logical planner.
It is fast to write and read but has the classic warehouse weaknesses for regulated industries:

* The storage format is closed. Nothing but RustHouse can read a segment; auditors, Spark,
  DuckDB, or a successor system cannot.
* There is no transaction log. `metadata.json` is overwritten atomically, so the *current*
  state is consistent, but history is lost. A regulator asking "what did the risk table contain
  on the 31st of March?" cannot be answered.
* Schema is fixed at table creation. Adding a column means a new table.
* Deletes and updates are impossible without rewriting everything, which blocks GDPR erasure.

Banks and other heavily regulated institutions need: reproducible point-in-time reports
(BCBS 239), multi-year immutable record keeping (MiFID II, SOX), the ability to erase or
restrict a subject's data (GDPR), and low-latency analytics for risk and fraud dashboards.

Three options were considered: keep the warehouse, replace it with a pure lakehouse, or combine.

## Decision

RustHouse becomes a **hybrid**:

1. **Lakehouse tables are the system of record.** Every table is a directory of immutable
   Apache Parquet data files plus an append-only JSON transaction log (`rhlog`, see ADR-0002).
   Commits are atomic, versioned, attributable, and give schema evolution and time travel.
   Tables are organised in Bronze / Silver / Gold medallion layers.
2. **The existing segment engine becomes the hot tier.** The current `col_N.bin` format,
   `SegmentWriter/Reader`, and `SegmentPruner` are kept and repurposed as a **derived, rebuildable
   acceleration layer** materialised from a pinned lake table version. The hot tier is never
   authoritative; it can be dropped and rebuilt at any time from the lake.
3. **One catalog, one SQL surface.** The planner resolves a table name to a lake table version
   and, if a fresh hot-tier materialisation exists for that version, routes the scan to it.
   Otherwise it scans Parquet directly. Callers cannot tell the difference except in latency.
4. **Governance sits between the planner and the executor**, applied identically to both tiers
   (see ADR-0005).

## Consequences

### Positive
* Open, auditable, portable data. Any Parquet reader can verify what RustHouse stored.
* Time travel and immutable history come for free from the transaction log, which is the
  foundation for audit, reproducibility, and retention.
* Existing code (storage, pruning, planner) is preserved and gains a clear role.
* Hot/cold separation gives warehouse-class latency on the working set without giving up
  lakehouse economics on history.

### Negative
* Two storage paths to maintain and test. Mitigated by making the hot tier strictly derived and
  covered by equivalence tests (same query, both tiers, same result).
* Parquet write/read adds dependencies (`arrow`, `parquet`) and compile time.
* Compaction becomes more complex: lake-level compaction (small Parquet files) and hot-tier
  refresh are separate background jobs.

## Alternatives considered

* **Stay a warehouse, add a WAL.** Solves crash consistency, not portability, schema evolution,
  or erasure. Rejected.
* **Pure lakehouse, delete the segment engine.** Simplest architecture, but loses the sub-10 ms
  scan path the project was built for, and throws away working, tested code. Rejected for now;
  may be revisited if Parquet scans with page indexes prove fast enough (HYB-ACC-005 tracks this).
* **Adopt Delta Lake or Apache Iceberg directly.** Correct long-term answer for interoperability,
  but both specs are large. `rhlog` is deliberately a strict subset of the Delta Lake log
  structure so that a later export/compatibility layer is feasible (ADR-0002).

## Requirements affected
All `LAKE-*`, `HYB-*`, and `GOV-*` specs. `CORE-*` specs are re-scoped from "the database" to
"the hot tier".
