# RustHouse Spec-Driven Development (SDD)

This directory is the **source of truth** for what RustHouse does and why. Code that is not
traceable to an accepted requirement here is out of scope; a requirement without tests is not
done.

## How SDD works in this repository

```
  Idea ──► Spec (Draft) ──► Review ──► Spec (Accepted) ──► Red tests ──► Green code ──► Refactor ──► Spec (Implemented)
                                            │                                                            │
                                            └──── ADR if the decision is architectural ──────────────────┘
```

1. **Write the spec first.** Copy `templates/feature-spec.md` into `features/`. Give every
   requirement a stable ID (see *ID scheme*). Write acceptance scenarios in Given/When/Then form.
2. **Review.** A spec moves from `Draft` to `Accepted` when the acceptance scenarios are
   unambiguous enough that a test can be written for each `MUST`.
3. **Red.** Write the tests named in the spec's *Test plan* section **before** any implementation.
   Every test carries its requirement ID in its name or in a `// REQ: <ID>` comment. Tests may
   fail to compile at this stage; that is the red state. Commit red tests on a feature branch,
   never on `main`.
4. **Green.** Write the minimum code to pass. Nothing more.
5. **Refactor** with the tests as the safety net.
6. **Close the loop.** Set the spec status to `Implemented` and run `scripts/spec-check.sh`,
   which fails CI if an `Accepted`/`Implemented` requirement has no test referencing it.

## ID scheme

| Prefix | Area | Example |
|--------|------|---------|
| `CORE-` | As-built warehouse engine (storage, pruning, SQL) | `CORE-STO-003` |
| `LAKE-` | Lakehouse features (table format, Parquet, time travel, medallion layers) | `LAKE-TXN-002` |
| `HYB-`  | Hybrid tier (hot serving segments derived from lake tables) | `HYB-ACC-001` |
| `GOV-`  | Data governance (classification, access, audit, lineage, retention, quality) | `GOV-AUD-004` |
| `API-`  | HTTP/SQL surface | `API-SQL-001` |
| `ADR-`  | Architecture decision records | `ADR-0001` |

IDs are **never reused or renumbered**. A withdrawn requirement keeps its ID with status `Withdrawn`.

Requirement wording follows RFC 2119: `MUST` is tested and gating, `SHOULD` is tested when
implemented, `MAY` is optional.

## Spec status lifecycle

| Status | Meaning | Gate enforced by `spec-check.sh` |
|--------|---------|----------------------------------|
| `Draft` | Being written, may change freely | none |
| `Accepted` | Reviewed, tests may be written | every `MUST` has a test reference (may be red) |
| `Implemented` | Tests green on `main` | every `MUST` has a test reference and `cargo test` passes |
| `Withdrawn` | Dropped; kept for history | none |

## Layout

```
specs/
├── README.md                 this file: the process
├── constitution.md           non-negotiable engineering principles
├── glossary.md               shared vocabulary (lakehouse, medallion, legal hold, ...)
├── roadmap.md                milestones and ordering
├── traceability.md           generated matrix: requirement -> tests -> status
├── adr/                      architecture decision records
├── architecture/             target architecture and data model
├── features/                 one spec per feature, requirement IDs live here
└── templates/                spec and ADR templates
```

## Test conventions (TDD)

* Unit tests live next to the code (`#[cfg(test)] mod tests`).
* Acceptance tests live in `tests/spec_<area>_<topic>.rs` and are the executable form of the
  Given/When/Then scenarios. One test function per scenario.
* Test names embed the requirement: `fn gov_cls_001_column_carries_classification()`, or the
  test body contains `// REQ: GOV-CLS-001`.
* Property-based tests (`proptest`) are encouraged for encoders, pruning, and policy evaluation.
* Governance tests are **negative-first**: the first test for any access-control or masking rule
  proves that the forbidden path is denied.

## Tooling

```bash
scripts/spec-check.sh            # traceability gate: requirements -> tests
scripts/spec-check.sh --matrix   # regenerate specs/traceability.md
cargo test                       # unit + acceptance
cargo clippy -- -D warnings
cargo fmt --check
```
