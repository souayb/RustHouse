# ADR-0003: Use apache `arrow` + `parquet` crates; drop `arrow2`

| Field | Value |
|-------|-------|
| Status | Accepted |
| Date | 2026-09-02 |
| Deciders | RustHouse maintainers |

## Context
`Cargo.toml` declared `arrow2 = "0.18"` (with `io_parquet`) but no source file used it.
`arrow2` is archived and unmaintained; its transitive dependency `ethnum 1.5.2` fails to compile
on rustc 1.97 (`E0512: cannot transmute between types of different sizes`), which broke
`cargo build` for the whole project on 2026-09-02.

## Decision
* Remove `arrow2` immediately (done in this change; it was unused).
* When `LAKE-PQT` is implemented, add the officially maintained
  [arrow-rs](https://github.com/apache/arrow-rs) crates: `arrow` (in-memory columnar) and
  `parquet` (with `async` feature for tokio I/O). Pin exact minor versions; arrow-rs releases
  breaking majors frequently.
* Internal `ColumnData` stays as the hot-tier in-memory representation; a `From`/`TryFrom`
  bridge to Arrow `RecordBatch` is the seam (`LAKE-PQT-004`).

## Consequences
* Build works on current stable Rust.
* Parquet page indexes, column statistics, and bloom filters become available for pruning lake
  scans, mirroring the hot-tier pruner.
* Larger dependency footprint; accepted.

## Alternatives considered
* Pin an older toolchain so `arrow2` compiles. Rejected: unmaintained crypto/format code is a
  compliance liability in itself.
* Hand-roll a Parquet writer. Rejected: Parquet is complex and the value is interoperability,
  which a home-grown writer would undermine.
