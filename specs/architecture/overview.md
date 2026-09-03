# Target architecture: hybrid Lakehouse

See ADR-0001 for the decision, ADR-0002 for the table format, ADR-0005 for governance placement.

## 1. Component view

```
                 ┌──────────────────────────────────────────────────────────────┐
  clients        │  HTTP API (axum)  /v1/query  /v1/write  /v1/ddl  /metrics    │
  (BI, jobs,     │  Auth: principal from mTLS cert or bearer token (API-SQL)    │
   auditors)     └──────────────┬───────────────────────────────────────────────┘
                                │ Session::execute_governed(principal, sql)
                 ┌──────────────▼───────────────────────────────────────────────┐
                 │  SQL Parser (sqlparser)  →  Planner  →  LogicalPlan          │
                 │  + time-travel clauses, DDL for tables/policies (LAKE-TT)    │
                 └──────────────┬───────────────────────────────────────────────┘
                                │ LogicalPlan + Principal
                 ┌──────────────▼───────────────────────────────────────────────┐
                 │  GOVERNOR  (src/governance/)                 ADR-0005         │
                 │   classification check  → RBAC → row filters → masking       │
                 │   audit(intent)  ─────────────────────────────┐              │
                 └──────────────┬────────────────────────────────┼──────────────┘
                                │ governed LogicalPlan            │
                 ┌──────────────▼──────────────┐   ┌─────────────▼─────────────┐
                 │  Tier Router (src/exec/)    │   │  Audit Log (append-only,  │
                 │  fresh hot materialisation? │   │  hash-chained)  GOV-AUD   │
                 └──────┬───────────────┬──────┘   └───────────────────────────┘
                        │ yes           │ no
          ┌─────────────▼──────┐  ┌─────▼──────────────────────┐
          │ HOT TIER           │  │ LAKE SCAN                  │
          │ SegmentReader      │  │ Parquet reader + row-group │
          │ SegmentPruner      │  │ stats pruning   LAKE-PQT   │
          │ (as-built CORE-*)  │  │                            │
          └─────────┬──────────┘  └─────┬──────────────────────┘
                    │  derived from      │
                    └────────┐   ┌───────┘
                    ┌────────▼───▼─────────────────────────────────────────────┐
                    │  CATALOG (src/catalog/)                        LAKE-CAT  │
                    │  namespaces · tables · policies · lineage · classifications│
                    │  every table = rhlog transaction log + Parquet files       │
                    │  bronze/   silver/   gold/            LAKE-TXN, LAKE-MED   │
                    └───────────────────────────────────────────────────────────┘
   background:  compaction (lake small files)  ·  hot-tier refresh  ·  vacuum/retention  ·  contract checks
```

## 2. Data layout on disk

```
<data_dir>/
  catalog/
    _rhlog/                          # the catalog is itself an rhlog table (policies, lineage, grants)
  bronze/<table>/                    # raw ingest, append-only, schema-on-read tolerant
    _rhlog/0000...0.json
    part-<uuid>.parquet
  silver/<table>/                    # validated by data contracts, conformed types, PII classified
  gold/<table>/                      # business aggregates, most hot-tier candidates
  hot/<layer>/<table>/v<version>/    # derived segments: col_N.bin (as-built format), rebuildable
    seg_0/ seg_1/ ...
    _manifest.json                   # which lake version, which segments, checksum
  audit/
    <yyyy>/<mm>/<dd>/audit-<seq>.jsonl   # hash-chained, append-only
    _chain_head                          # last hash
  keys/                              # wrapped data-encryption keys (GOV-ENC); never plaintext
```

## 3. Request lifecycle (read)

1. HTTP layer authenticates and builds a `Principal { id, roles, attributes }`.
2. Parser → Planner produce a `LogicalPlan` with optional `AsOf { version | timestamp }`.
3. Catalog resolves `layer.table` to a `TableRef` and the requested `Snapshot`.
4. Governor:
   1. refuses if the table or any referenced column lacks a classification (fail closed);
   2. checks `SELECT` grant on table and each column for the principal's roles;
   3. appends row-filter predicates bound to the principal;
   4. wraps restricted columns in masking expressions;
   5. writes `audit(intent)` with the snapshot version(s); refuses if the write fails.
5. Tier router: if `hot/<table>/v<version>/_manifest.json` exists and is verified, execute on
   segments; else read Parquet with row-group pruning.
6. Executor returns columnar batches; response includes `versions_read` for reproducibility.
7. Governor writes `audit(outcome)`: rows returned, masked columns, elapsed, status.

## 4. Request lifecycle (write / commit)

1. Authenticate; Governor checks `INSERT`/`ALTER` grant and data-contract compliance (GOV-DQ).
2. Rows are validated against the table schema, encoded to a Parquet file `part-<uuid>.parquet`.
3. A commit `{commitInfo(principal, op), add(file, stats)}` is written to `_rhlog/<v+1>.json.tmp`
   and renamed. Conflict → retry against the new head.
4. Lineage edge recorded (source query/version → target version) if the write came from a query.
5. Hot-tier materialisation for that table is marked stale; background refresh rebuilds.
6. `audit(outcome)` written.

## 5. Module map (target)

| Path | Responsibility | Specs |
|------|----------------|-------|
| `src/catalog/` | namespaces, table registry, policy & grant storage, lineage store | LAKE-CAT, GOV-LIN |
| `src/lake/log.rs` | rhlog commit/replay/snapshot/checkpoint | LAKE-TXN, LAKE-TT |
| `src/lake/parquet.rs` | Parquet write/read, stats, `ColumnData` ↔ `RecordBatch` | LAKE-PQT |
| `src/lake/schema.rs` | schema evolution rules, column ids | LAKE-SCH |
| `src/lake/dml.rs` | append, delete, merge (copy-on-write), vacuum | LAKE-DML |
| `src/lake/medallion.rs` | layer rules, promotion jobs | LAKE-MED |
| `src/hot/` | materialise lake version → segments, manifest, router | HYB-ACC |
| `src/storage/` | as-built segment format (unchanged role: hot tier) | CORE-STO, CORE-PRN |
| `src/sql/` | parser, planner (+ AS OF, DDL) | CORE-SQL, LAKE-TT, API-SQL |
| `src/governance/classification.rs` | labels, tags, propagation | GOV-CLS |
| `src/governance/access.rs` | principals, roles, grants, row filters, masking | GOV-ACC |
| `src/governance/audit.rs` | hash-chained audit sink + verifier | GOV-AUD |
| `src/governance/retention.rs` | retention, legal hold, erasure | GOV-RET |
| `src/governance/quality.rs` | data contracts, quarantine | GOV-DQ |
| `src/governance/crypto.rs` | column keys, wrapping, shredding | GOV-ENC |
| `src/exec/` | executor (M1 row engine), tier router, vectorized engine (ADR-0006) | CORE-EXE, EXE-VEC, EXE-PAR, HYB-ACC |
| `src/lake/wal.rs` | WAL segments, memtable, flusher, recovery | LAKE-WAL |
| `src/session.rs` | `execute_governed` entry point | API-SQL, ADR-0005 |

## 6. Non-functional budgets (tested where marked)

| Concern | Budget | Test |
|---------|--------|------|
| Commit latency, single writer, local disk | p99 < 20 ms excluding Parquet encode | LAKE-TXN perf test |
| Governor overhead | p99 < 1 ms for 100 policies on one table | GOV-ACC-010 |
| Audit write | synchronous, fsync per record batch, < 5 ms p99 | GOV-AUD |
| Hot vs lake equivalence | identical results for the same snapshot | HYB-ACC equivalence suite |
| Snapshot replay | 10 000 commits < 500 ms without checkpoint | LAKE-TXN-007 motivates checkpoints |

## 7. Regulatory mapping (why each area exists)

| Regulation / principle | What it demands | RustHouse mechanism |
|------------------------|-----------------|---------------------|
| BCBS 239 (risk data) | accuracy, completeness, traceability, reproducibility | rhlog versions, lineage, `versions_read`, data contracts |
| SOX / internal control | tamper-evident audit trail, segregation of duties | hash-chained audit log, RBAC, policy-as-data |
| MiFID II / record keeping | multi-year immutable retention | retention policies, immutable Parquet, vacuum gating |
| GDPR | minimisation, purpose limitation, erasure, access requests | classification tags (PII), masking, erasure via rewrite / crypto-shredding, subject lookup |
| DORA | resilience, integrity, incident evidence | audit verify, crash-consistency tests, rebuildable hot tier |
| PCI DSS | protect cardholder data, restrict access | `PCI` tag → masking by default, column encryption |
