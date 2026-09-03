# ADR-0002: `rhlog`, a minimal transaction-log table format modelled on Delta Lake

| Field | Value |
|-------|-------|
| Status | Accepted |
| Date | 2026-09-02 |
| Deciders | RustHouse maintainers |

## Context
A lakehouse needs a table format: metadata that turns a bag of Parquet files into a versioned,
transactional table. The two industry standards are Delta Lake (ordered JSON log + checkpoints)
and Apache Iceberg (metadata files, manifest lists, manifests). Implementing either fully is
months of work; implementing neither leaves us with an unversioned file dump.

## Decision
Implement `rhlog`, a **deliberately small subset of the Delta Lake protocol**:

```
<table>/
  _rhlog/
    00000000000000000000.json      # version 0: protocol + metaData + add actions
    00000000000000000001.json      # version 1
    ...
    00000000000000000010.checkpoint.parquet   # optional, every N commits (LAKE-TXN-007)
    _last_checkpoint                          # pointer to latest checkpoint
  part-<uuid>.parquet                        # data files, never modified
```

Each commit file is newline-delimited JSON with actions:

| Action | Purpose |
|--------|---------|
| `protocol` | min reader/writer version |
| `metaData` | table id, schema (JSON), partition columns, classification tags, created time |
| `add` | data file path, size, row count, per-column min/max/null counts, partition values |
| `remove` | data file logically deleted at this version (kept for time travel until vacuum) |
| `commitInfo` | timestamp, principal, operation (`WRITE`, `MERGE`, `DELETE`, `ALTER`, `VACUUM`, `ERASE`), operation parameters, client |

Rules:
* A commit is atomic: written to `<version>.json.tmp` then renamed. Rename failure because the
  target exists means a concurrent writer won: retry with the next version (optimistic
  concurrency, single filesystem for now).
* The current snapshot is the fold of all actions up to version `n`.
* `commitInfo.principal` is mandatory. A commit without a principal is rejected (Constitution 4, 7).
* Data files are never rewritten in place. Updates and deletes produce new files plus `remove`
  actions.

## Consequences
### Positive
* Time travel, audit, lineage anchoring, and retention all hang off a single, simple structure.
* Readable with `cat` and `jq`; a compliance officer can inspect a table's history without tooling.
* Structural compatibility with Delta Lake keeps a future export path open.

### Negative
* Not byte-compatible with Delta or Iceberg; external engines cannot read `rhlog` tables
  directly until an export layer exists (LAKE-TXN-009, `MAY`).
* Log replay cost grows with version count until checkpoints are implemented.

## Alternatives considered
* Full Delta Lake via the `deltalake` crate. Heavy dependency tree, object-store oriented, and
  hides the mechanism we want to own and test. Rejected for v1.
* Apache Iceberg. More metadata indirection than we need for single-node.

## Requirements affected
`LAKE-TXN-*`, `LAKE-TT-*`, `GOV-AUD-*` (audit log references commit versions), `GOV-RET-*`.
