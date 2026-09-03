# RustHouse Constitution

These principles are binding on every spec, ADR, and pull request. Changing this file requires
an ADR.

## 1. Tests before code
No production code is written without a failing test that demands it. Every `MUST` requirement
has at least one test that names it. Coverage of governance paths is measured on the **deny**
side first: a policy is proven by showing what it refuses.

## 2. Specs are the contract
Behaviour is defined in `specs/features/`. If code and spec disagree, the spec is either fixed
through review or the code is a bug. Requirement IDs are permanent.

## 3. Open formats are the system of record
Durable data is stored in open, documented formats (Apache Parquet data files, a JSON transaction
log) readable without RustHouse. Proprietary formats are allowed only for **derived, rebuildable**
acceleration structures (the hot tier).

## 4. Immutability and append-only history
Data files are never modified in place. Every change to a table is a new commit in its
transaction log. Every commit is attributable to a principal and a timestamp. Physical deletion
happens only through explicit, audited vacuum or erasure operations.

## 5. Governance is not optional
Every table and column carries a classification. Every access is authorised against a policy
and written to an audit log before the result is returned. There is no "admin bypass" path that
skips the audit log.

## 6. Reproducibility
Any query result can be reproduced later by pinning the table version(s) it read. Regulatory
reports reference table versions, not wall-clock time alone.

## 7. Fail closed
Missing policy, missing classification, unknown principal, or an unwritable audit log means the
operation is refused, not allowed.

## 8. Small, vertical slices
Milestones deliver thin end-to-end paths (spec, red tests, green code, docs) rather than wide
horizontal layers. A slice is done when its spec is `Implemented`.

## 9. Observability by default
Every component exposes Prometheus metrics and structured tracing. Governance decisions
(allow/deny/mask) are counted and labelled by policy.

## 10. Zero unsafe without justification
`unsafe` requires a comment explaining the invariant and a test exercising the boundary.
