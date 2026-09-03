# ADR-0004: Test-driven development with requirement traceability as a CI gate

| Field | Value |
|-------|-------|
| Status | Accepted |
| Date | 2026-09-02 |
| Deciders | RustHouse maintainers |

## Context
In regulated environments the question is not only "does it work" but "prove that the control
you documented is the control that runs". Auditors ask for a mapping from requirement to test to
evidence. Traditional after-the-fact testing produces neither the mapping nor confidence that
edge cases (deny paths, masking, erasure) were considered before the happy path was built.

## Decision
1. **Red before green.** No production code without a failing test. Enforced socially in review
   and mechanically by requiring every `MUST` requirement to be referenced by a test.
2. **Requirement IDs in tests.** A test references a requirement by including the ID, lowercase
   with underscores, in its function name (`gov_cls_001_...`) **or** by a `// REQ: GOV-CLS-001`
   comment inside the test body.
3. **`scripts/spec-check.sh` is a CI gate.** It parses every `specs/features/*.md`, collects
   requirement IDs whose spec status is `Accepted` or `Implemented`, and fails if any `MUST` has
   no reference under `src/` or `tests/`. It also fails if a test references an ID that no spec
   defines (catches typos and orphaned tests). `--matrix` regenerates `specs/traceability.md`.
4. **Acceptance tests are executable scenarios.** Each Given/When/Then block in a spec maps to
   exactly one function in `tests/spec_<area>_<topic>.rs`.
5. **Negative-first for governance.** The first test written for any policy is the deny/mask
   case (Constitution 1, 7).
6. **Property tests for encoders and evaluators.** `proptest` is added as a dev-dependency when
   the first property test is written, not before.

## Consequences
### Positive
* Audit evidence is generated from the codebase itself, not maintained by hand.
* Scope creep is visible: code with no requirement fails review.
### Negative
* Friction for exploratory spikes. Mitigated: spikes live on branches named `spike/*` and are
  never merged; they inform a spec instead.

## Alternatives considered
* Coverage percentage thresholds. Measures lines, not requirements; rejected as the primary gate.
* BDD framework (`cucumber-rs`). Adds a DSL layer between spec and Rust; plain `#[test]`
  functions with Gherkin in the spec keep the tooling simple. May revisit.
