# RustHouse — working agreement for AI assistants and contributors

RustHouse is a Rust OLAP engine evolving into a **hybrid Lakehouse** (open Parquet + `rhlog`
transaction log as system of record, existing LZ4 segment format as a rebuildable hot tier) with
**bank-grade data governance**. Development is **spec-driven** and **test-driven**.

## Before writing any code
1. Read `specs/README.md` (process), `specs/constitution.md` (principles), and the feature spec
   you are implementing under `specs/features/`.
2. If the spec is `Draft`, finish the spec first (requirements, scenarios, test plan) and move it
   to `Accepted`. Do not implement from a `Draft`.
3. If the change is architectural, add an ADR in `specs/adr/` using `specs/templates/adr.md`.

## TDD loop (non-negotiable)
1. Write the acceptance test named in the spec's test plan in `tests/spec_<area>_<topic>.rs`.
   The function name embeds the requirement ID (`fn gov_acc_001_...`) or the body has
   `// REQ: GOV-ACC-001`.
2. Run it, watch it fail (compile failure counts as red).
3. Write the minimum code to pass. Add unit tests beside the code.
4. Refactor. Run `cargo fmt`, `cargo clippy --all-targets -- -D warnings`, `cargo test`.
5. Run `scripts/spec-check.sh --matrix` and commit the regenerated `specs/traceability.md`.
6. Governance features: write the **deny/refuse** test first, always.

## Rules of thumb
* New error variants and metadata fields are listed in `specs/architecture/error-model.md`; add
  them there when you add them to code.
* Never modify data files in place; every change to a table is a commit (Constitution 4).
* No execution path may bypass the Governor (`ADR-0005`). The raw `Executor` stays crate-private.
* Requirement IDs are permanent; never renumber. Withdraw instead.
* Keep `README.md` build/test instructions accurate when commands change.

## Commands
```bash
cargo test                                  # unit + acceptance + traceability gate
cargo clippy --all-targets -- -D warnings
cargo fmt --check
scripts/spec-check.sh --matrix              # traceability gate + regenerate matrix
cargo run -- --bind 127.0.0.1:8123 --data-dir ./data
```

## Layout
See `specs/architecture/overview.md` section 5 for the target module map. As of M1:
`src/lake/{log,schema,stats,parquet}.rs` (rhlog + Parquet), `src/catalog/` (catalog as an rhlog
table), `src/governance/{classification,governor,audit}.rs`, `src/exec/` (crate-private
executor), `src/session.rs` (the only public execution entry), `src/auth.rs`, `src/http_api.rs`,
`src/metrics.rs`; `src/storage/` and `src/sql/` are the as-built engine (hot tier + planner).
M2 added `src/exec/vector.rs` (DataFusion engine: governed table provider, Governor analyzer
rule, `rh_mask` UDF) and `src/governance/access.rs` (grants, row policies, masks as catalog
entries). `TECHNICAL.md` Steps 5–8 are superseded by `CORE-EXE`/`EXE-VEC`, `API-SQL`, `HYB-ACC`, `GOV-AUD`.
Always build with the same target set (`cargo test -j 2 ...`): a plain `cargo build` unifies
features differently and recompiles the whole dependency tree.
Debug test binaries linking DataFusion exceed 1 GB; if one fails to start with an `ld.so`
relocation assertion or exit code 127, delete that binary under `target/debug/deps/` and relink.
Build with `cargo test -j 2` on memory-constrained machines (the Arrow crates are heavy).
