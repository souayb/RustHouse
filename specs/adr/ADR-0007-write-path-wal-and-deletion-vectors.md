# ADR-0007: Write path — WAL-backed write buffer and deletion vectors

| Field | Value |
|-------|-------|
| Status | Proposed |
| Date | 2026-09-02 |
| Deciders | RustHouse maintainers |

## Context
Every `INSERT` in M1 produces one Parquet file and one rhlog commit with `fsync`. That is
correct and durable, but small frequent writes (payments, ticks, events) create thousands of
tiny files and bound ingest latency by two fsyncs per row batch. Deletes, updates and GDPR
erasure are specified as copy-on-write rewrites (LAKE-DML, GOV-RET): erasing one customer from a
1 GB file rewrites 1 GB.

Terminology check against the M1 code: the rhlog **is already a write-ahead log for the lake**
(a commit is durable before it is visible) and version pinning **is already MVCC at table
granularity** (readers see one snapshot; writers append with optimistic conflict detection). What
is missing is a low-latency buffer in front of the lake, and row-level change tracking.

## Decision (proposed)
1. **WAL-backed write buffer (LAKE-WAL).** Writes append row batches to a per-table,
   row-oriented, fsync'd WAL segment (`<table>/_wal/<seq>.wal`, length-prefixed Arrow IPC
   frames with a CRC) and become visible to the query path immediately through an in-memory
   table (memtable) that the executor scans alongside Parquet files. A background flush
   converts a sealed WAL segment into one Parquet file plus one rhlog commit, then deletes the
   segment. Recovery replays unsealed segments into the memtable on open. Durability, atomicity
   and attributability are unchanged: each WAL frame carries the principal and request id, and
   the flush commit records the WAL sequence range it covers.
2. **Deletion vectors for DELETE, MERGE and ERASE (LAKE-DML amendment).** Instead of rewriting
   files, a delete writes a bitmap of deleted row positions per data file, referenced from the
   `add` action's successor (`add` with `deletion_vector: { path, cardinality }`). Readers apply
   the bitmap (merge-on-read). Compaction (LAKE-DML vacuum/optimize) rewrites files with many
   deleted rows and drops the vector. Erasure remains the exception that also purges history: it
   rewrites the affected files across all retained snapshots, as GOV-RET specifies, but can use
   deletion vectors for the *current* snapshot immediately and schedule the historical rewrite.
3. **Isolation level.** Snapshot isolation per statement stays. Write-write conflicts are
   detected at commit by overlapping `remove`/deletion-vector targets (LAKE-DML-008). No
   serializable multi-statement transactions in scope; cross-table promotions get a two-log
   commit protocol coordinated through the catalog log (LAKE-MED).

## Consequences
### Positive
* Ingest latency drops to one fsync per batch and files stay large.
* Deletes and erasure become O(deleted rows), not O(file), which makes GDPR erasure SLAs
  achievable on large history.
* The hot-tier segment format (CORE-STO) becomes unnecessary as an ingest path; HYB-ACC narrows
  to "materialised Arrow cache of a pinned version" or is withdrawn after ADR-0006's spike.
### Negative
* Two read sources per table (memtable + files) must be governed identically; the Governor
  already sits above tier selection (ADR-0005), so this is a router concern, not a policy one.
* Recovery code is a new crash-consistency surface: DORA-style crash-mid-write tests are a
  MUST in LAKE-WAL, not a nice-to-have.
* Deletion vectors diverge from "strict Delta subset" (ADR-0002) unless we adopt Delta's own
  deletion-vector encoding (RoaringBitmap, `deletionVector` field on `add`). Proposed: adopt
  Delta's encoding to keep the export path.

## Alternatives considered
* Buffer in memory without a WAL: loses acknowledged writes on crash. Rejected (Constitution 4).
* Per-row MVCC tuples (Postgres-style) inside Parquet: incompatible with immutable open files.
  Rejected.
* Rewrite-only deletes (status quo): simple but makes erasure and MERGE prohibitively expensive.

## Requirements affected
LAKE-WAL (new), LAKE-DML (deletion vectors, conflict rule), GOV-RET (erasure fast path),
HYB-ACC (scope), LAKE-TXN (`add.deletion_vector` field, `WRITE` commit `wal_range` parameter),
GOV-AUD-008 (flush commits are system commits with principal `system:flush`, still audited).

## Change log
| Date | Change |
|------|--------|
| 2026-09-02 | Proposed |
