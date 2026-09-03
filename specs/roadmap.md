# Roadmap

Milestones are vertical slices. A milestone is complete when every spec listed is `Implemented`
and `scripts/spec-check.sh` passes on `main`.

## M0 — SDD foundation (this change)
* `specs/` structure, constitution, ADR-0001..0005, templates
* As-built specs `CORE-*` mapped to existing tests
* `scripts/spec-check.sh` traceability gate + `tests/spec_traceability.rs`
* Build fixed (`arrow2` removed, `segment.rs` test import)

## M1 — Lakehouse core, append-only, governed by default (2026-09-02: core delivered)
| Spec | Title | State |
|------|-------|-------|
| LAKE-CAT | Catalog: namespaces, table registry, policy storage | Implemented |
| LAKE-TXN | rhlog transaction log: atomic versioned commits | Implemented |
| LAKE-PQT | Parquet data files, statistics, `ColumnData` bridge | Implemented (Timestamp/Decimal SHOULD deferred) |
| LAKE-TT | Time travel: `AS OF VERSION` / `AS OF TIMESTAMP` | 001–009 done; 010–012 need LAKE-SCH / GOV-ACC |
| GOV-CLS | Mandatory classification on tables and columns | 001–005, 009–012 done; 006–008 need GOV-LIN / GOV-ACC |
| GOV-AUD | Hash-chained audit log for commits and queries | all but 009 (needs GOV-RET) |
| API-SQL | Governed session entry point, principal on every request | Implemented (policy DDL with GOV-ACC) |
| CORE-EXE | Single-table executor (replaces `TECHNICAL.md` Step 5) | Implemented |

## M2 — Engine decision and access control (2026-09-03: delivered)
| Spec | Title | State |
|------|-------|-------|
| ADR-0006 | Execution engine decision (DataFusion spike → accept or build) | Accepted: DataFusion |
| EXE-VEC | Arrow-native vectorized execution (Governor as plan analyzer) | Implemented (Decimal/Timestamp pending) |
| GOV-ACC | Roles, grants, column-level access, row filters, masking | Implemented (SHOULDs 012/013 deferred) |
The engine is decided first so the Governor's plan rewriter (GOV-ACC) targets the final plan
type once. Deny-first tests; Governor as the single enforcement point (ADR-0005).

## M3 — Write path, mutation, evolution, retention
| Spec | Title |
|------|-------|
| LAKE-WAL | WAL-backed write buffer and memtable (ADR-0007) |
| EXE-PAR | Parallel scan and execution, memory budgets, cancellation |
| LAKE-DML | Delete, merge/upsert (deletion vectors per ADR-0007), vacuum |
| LAKE-SCH | Schema evolution with stable column ids |
| GOV-RET | Retention policies, legal hold, GDPR erasure |
| GOV-ENC | Column encryption, key rotation, crypto-shredding |

## M4 — Hybrid hot tier and data management
| Spec | Title |
|------|-------|
| HYB-ACC | Hot-tier materialisation, routing, equivalence, rebuild — scope re-evaluated after HYB-ACC-005 / ADR-0006 |
| LAKE-MED | Medallion layers and promotion pipelines |
| GOV-LIN | Table and column lineage, impact analysis |
| GOV-DQ | Data contracts, quality rules, quarantine |
Also: lake compaction and hot-tier refresh workers (replaces `TECHNICAL.md` Step 7 sketch).

## M5 — Hardening and interoperability
* LAKE-TXN checkpoints, Delta Lake export (`MAY`)
* Prometheus metrics for every governance decision, perf budgets as tests
* DORA-style resilience tests: crash mid-commit, corrupted audit chain, disk full
* Threat model review of every `Denied` path

## Deferred / explicitly out of scope for now
* Distributed execution, replication, object-store backends (S3) — `rhlog` is designed to allow
  them later; the local filesystem is the only backend until M5 is done.
* Joins beyond single-table scans (planner limitation inherited from CORE-SQL).
