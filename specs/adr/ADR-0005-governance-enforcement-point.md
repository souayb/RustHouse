# ADR-0005: Single governance enforcement point between planner and executor

| Field | Value |
|-------|-------|
| Status | Accepted |
| Date | 2026-09-02 |
| Deciders | RustHouse maintainers |

## Context
Governance controls (classification, RBAC, column masking, row filters, audit logging) can be
implemented at the API edge, in the planner, in storage, or scattered across all three.
Scattered enforcement is how bypasses happen: a new endpoint or a new storage tier forgets a
check. Banks are audited on exactly this: "show me every path to the data and the control on it".

## Decision
* All reads and writes flow through one `Governor` (`src/governance/`) that receives a
  `LogicalPlan` plus a `Principal` and returns either a **rewritten plan** or a `Denied` error.
  Rewrites inject row-filter predicates, replace columns with masking expressions, and strip
  unauthorised projections.
* The `Governor` is invoked by the planner's caller (HTTP API, CLI, future gRPC) via a single
  `execute_governed(principal, sql)` entry point in `src/session.rs`. The raw `Executor` is not
  `pub` outside the crate.
* The `Governor` writes an audit record **before** execution starts (intent) and **after** it
  finishes (outcome, row counts, versions read). If the audit sink is unavailable, the query is
  refused (Constitution 7).
* Policies are data (JSON/SQL DDL), versioned in the catalog like any table, so policy changes
  themselves are audited and time-travelable.
* The same `Governor` applies to lake scans and hot-tier scans; tier selection happens after
  governance, never before.

## Consequences
### Positive
* One place to test, one place to audit, one place to add a new control.
* Policy-as-data means "what policy was in force on date X" is a query, not archaeology.
### Negative
* The `Governor` is on the hot path of every query. Policy evaluation must be O(policies for
  this table), cached per catalog version. Budget: < 1 ms p99 for 100 policies (GOV-ACC-010).

## Alternatives considered
* Enforce in storage (encrypt per column, hand out keys). Strong, but cannot express row filters
  or aggregate-only access. May complement, not replace (GOV-ENC-*).
* Enforce at HTTP edge only. Rejected: internal callers (compaction, hot-tier refresh, CLI)
  would bypass it.
