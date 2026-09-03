# API-SQL: Governed session and HTTP API

| Field | Value |
|-------|-------|
| Status | Implemented |
| Owner | RustHouse maintainers |
| Related ADRs | ADR-0001, ADR-0004, ADR-0005 |
| Depends on | CORE-SQL, LAKE-CAT, LAKE-TXN, LAKE-TT, GOV-CLS, GOV-AUD |
| Milestone | M1 |

## 1. Problem and motivation
Every regulator question about data access ("who ran this query, on which version, and what did
they see?") is only answerable if there is exactly one door into the engine and every request
through it carries an identity. Today `src/main.rs` exposes `/health` and `/` and the sketch in
`TECHNICAL.md` Step 6 (`http_api.rs`) calls `Parser`, `Planner`, and `Executor` directly with no
principal. That sketch is replaced by this spec.

`Session::execute_governed(principal, sql)` in `src/session.rs` becomes the **only** public
execution entry point (ADR-0005). The HTTP layer authenticates, builds a `Principal`, and
delegates. Every response carries a `request_id` that appears in the audit log (GOV-AUD) and the
list of table versions read (LAKE-TT), so any report can be reproduced (BCBS 239) and any access
can be proven (SOX, GDPR access requests, DORA incident evidence).

## 2. Scope
### In scope
* `Session` API and crate-private `Executor`.
* `Authenticator` trait with a static-file implementation for dev/tests; bearer token and mTLS
  identity extraction.
* Endpoints `/v1/query`, `/v1/write`, `/v1/ddl`, `/health`, `/metrics`.
* Error → HTTP status mapping, request size limits, `request_id` propagation.
### Out of scope
* OAuth/OIDC token validation against an external IdP (an `Authenticator` implementation to be
  specified later).
* Streaming result formats other than JSON rows (JSONEachRow/Arrow IPC later).

## 3. Requirements

| ID | Level | Requirement |
|----|-------|-------------|
| API-SQL-001 | MUST | `Session::execute_governed(&self, principal: &Principal, sql: &str) -> Result<QueryResult>` MUST be the only `pub` way to execute SQL; `exec::Executor` and its constructor MUST be `pub(crate)`. |
| API-SQL-002 | MUST | A `Principal { id: String, roles: Vec<String>, attributes: BTreeMap<String,String> }` MUST be resolved for every request by an `Authenticator` implementation from either an `Authorization: Bearer <token>` header or a client mTLS certificate; the crate MUST ship a `StaticFileAuthenticator` reading a JSON file for dev and tests. |
| API-SQL-003 | MUST | A request with no credentials, an unknown token, or an expired token MUST receive HTTP 401 with no table or schema information, and an audit record with `outcome = "unauthenticated"` and the `request_id` MUST be written. |
| API-SQL-004 | MUST | `POST /v1/query {"sql": "..."}` MUST return `{ request_id, rows: [...], versions_read: [{table, version}], elapsed_ms }`; `versions_read` MUST list every lake table snapshot the query touched. |
| API-SQL-005 | MUST | `POST /v1/write?table=<layer>.<name>` with `{"rows": [...]}` MUST execute as an `INSERT` commit through the governed session, returning `{ request_id, version, rows_written }`. |
| API-SQL-006 | MUST | `POST /v1/ddl {"sql": "..."}` MUST accept `CREATE TABLE ... WITH CLASSIFICATION (...)` and `ALTER TABLE ... ALTER COLUMN ... CLASSIFIED AS ...`, routed through the governed session; a `CREATE TABLE` without classification for every column MUST be rejected (GOV-CLS). `GRANT`/`REVOKE`, `CREATE ROW POLICY`, `CREATE MASKING POLICY`, `DROP POLICY` and `SHOW POLICIES` (GOV-ACC) go through the same endpoint; `SHOW POLICIES` returns its rows in the `rows` field of the DDL response. |
| API-SQL-007 | MUST | Every HTTP response, including errors, MUST carry `request_id` (UUID v4) both in the JSON body and in an `X-Request-Id` header, and the same id MUST be present on every audit record produced by that request. |
| API-SQL-008 | MUST | `RustHouseError` MUST map to HTTP status as: `Denied` → 403, `TableNotFound`/`ColumnNotFound` → 404, `SqlParse`/`InvalidData`/`TypeMismatch` → 400, `Conflict` → 409, `AuditUnavailable` → 503, everything else → 500; error bodies MUST NOT include file paths or stack traces. |
| API-SQL-009 | MUST | Request bodies larger than `max_request_bytes` (`src/config.rs`, default 16 MiB) MUST be rejected with 413 before the body is parsed. |
| API-SQL-010 | MUST | `GET /health` MUST return 200 `OK` without authentication; `GET /metrics` MUST return Prometheus text and MUST require a principal holding role `metrics_reader`. |
| API-SQL-011 | MUST | If the audit sink refuses the intent record, the query MUST NOT execute and the response MUST be 503 (Constitution 7). |
| API-SQL-012 | SHOULD | `/v1/query` SHOULD accept an optional `"as_of": {"version": n}` or `{"timestamp": "..."}` object equivalent to the SQL `AS OF` clause (LAKE-TT). |
| API-SQL-013 | SHOULD | Responses SHOULD include `Cache-Control: no-store` and the server SHOULD not log SQL text at `info` level or above (SQL text may contain PII). |
| API-SQL-014 | MAY | A `format=jsoneachrow` query parameter MAY switch the result encoding. |

## 4. Design sketch

```rust
// src/session.rs
pub struct Session { catalog: Arc<Catalog>, governor: Arc<Governor>, router: Arc<TierRouter>, executor: exec::Executor /* pub(crate) */, audit: Arc<dyn AuditSink> }

pub struct QueryResult { pub request_id: Uuid, pub columns: Vec<Column>, pub rows: Vec<Vec<serde_json::Value>>, pub versions_read: Vec<(TableRef, u64)>, pub elapsed: Duration }

impl Session {
    pub async fn execute_governed(&self, principal: &Principal, sql: &str) -> Result<QueryResult>;
}

// src/auth.rs
#[async_trait]
pub trait Authenticator: Send + Sync { async fn authenticate(&self, req: &AuthMaterial) -> Result<Principal>; }
pub enum AuthMaterial { Bearer(String), ClientCert { subject_dn: String, fingerprint: String }, None }
pub struct StaticFileAuthenticator { /* token -> Principal, fingerprint -> Principal */ }

// src/http_api.rs (replaces TECHNICAL.md Step 6 sketch)
pub fn create_router(state: AppState) -> Router;   // axum 0.7, tower-http RequestBodyLimit + TraceLayer
```

* `execute_governed` flow: parse (`src/sql/parser.rs`) → plan (`src/sql/planner.rs`,
  `LogicalPlan`) → catalog resolve snapshot → `Governor::authorise` → `audit(intent)` →
  `TierRouter` → `Executor` → `audit(outcome)`.
* `request_id` is generated in an axum middleware, stored in request extensions, and threaded
  through `tracing::Span` fields and `AuditRecord.request_id`.
* Errors: add `Denied(String)`, `Conflict(String)`, `Unauthenticated`, `AuditUnavailable(String)`
  to `src/error.rs`; implement `IntoResponse` for `RustHouseError` with the mapping table above.
* `src/main.rs` bootstrap wires `Config` → `Catalog` → `Session` → `create_router`.
* Metrics: `rusthouse_http_requests_total{route,status}`, `rusthouse_query_duration_seconds`.

## 5. Acceptance scenarios

### API-SQL-001: Single entry point
```gherkin
Given the compiled crate
When  a doc-test or compile-fail test attempts `rusthouse::exec::Executor::new(...)` from outside the crate
Then  compilation fails (the type is not public)
And   `Session::execute_governed` is public
```

### API-SQL-002: Principal from bearer token via static authenticator
```gherkin
Given a StaticFileAuthenticator mapping token "t-analyst" to {id:"alice", roles:["analyst"]}
When  POST /v1/query with Authorization: Bearer t-analyst
Then  the governed session receives Principal{id:"alice", roles:["analyst"]}
```

### API-SQL-003: Unauthenticated requests
```gherkin
When  POST /v1/query is sent without an Authorization header
Then  the status is 401 and the body contains only {request_id, error:"unauthenticated"}
And   an audit record with outcome "unauthenticated" and that request_id exists
When  POST /v1/query is sent with Authorization: Bearer unknown-token
Then  the status is 401 (same shape)
```

### API-SQL-004: Query response shape
```gherkin
Given silver.trades at version 4 with 3 rows
When  authenticated POST /v1/query {"sql":"SELECT id FROM silver.trades"}
Then  the body has request_id, rows (3 entries), versions_read = [{"table":"silver.trades","version":4}], elapsed_ms
```

### API-SQL-005: Write endpoint
```gherkin
Given silver.trades at version 4
When  authenticated POST /v1/write?table=silver.trades {"rows":[{...},{...}]}
Then  the body is {request_id, version: 5, rows_written: 2}
And   the commit's principal equals the authenticated principal
```

### API-SQL-006: DDL requires classification
```gherkin
When  POST /v1/ddl with CREATE TABLE silver.x (id INT64, iban STRING) WITH CLASSIFICATION (id Internal, iban Restricted PII)
Then  the status is 200 and the table exists with those classifications
When  POST /v1/ddl with CREATE TABLE silver.y (id INT64, iban STRING)
Then  the status is 400 and the error names the unclassified columns
```

### API-SQL-007: request_id everywhere
```gherkin
When  any request is made (success or failure)
Then  the X-Request-Id header equals body.request_id
And   every audit record written during that request carries the same request_id
```

### API-SQL-008: Error mapping
```gherkin
When  a query on a table the principal has no grant for runs
Then  status 403
When  a query names a non-existent table
Then  status 404
When  the SQL does not parse
Then  status 400 and the body contains no filesystem path
When  the audit sink is unavailable
Then  status 503
```

### API-SQL-009: Request size limit
```gherkin
Given max_request_bytes = 1024
When  POST /v1/write with a 2 KiB body
Then  status 413 and no commit is made
```

### API-SQL-010: Health and metrics
```gherkin
When  GET /health without credentials
Then  200 "OK"
When  GET /metrics without credentials
Then  401
When  GET /metrics as a principal with role metrics_reader
Then  200 with Content-Type text/plain and a line starting with "# TYPE rusthouse_"
```

### API-SQL-011: Audit intent failure blocks execution
```gherkin
Given an audit sink configured to fail on write
When  an otherwise valid authenticated query is submitted
Then  status 503
And   the executor was never invoked (asserted via a counting executor stub)
```

## 6. Test plan (write these first)

| Requirement | Test | Location | Kind |
|-------------|------|----------|------|
| API-SQL-001 | `fn api_sql_001_executor_is_crate_private()` (compile-fail via `trybuild` or doc-test `compile_fail`) | `tests/spec_api_sql.rs` | acceptance |
| API-SQL-002 | `fn api_sql_002_bearer_token_resolves_principal()` | `tests/spec_api_sql.rs` | acceptance |
| API-SQL-002 | `fn test_static_file_authenticator_parses_and_maps()` | `src/auth.rs` | unit |
| API-SQL-003 | `fn api_sql_003_missing_credentials_401_and_audited()` | `tests/spec_api_sql.rs` | acceptance (negative) |
| API-SQL-003 | `fn api_sql_003_unknown_token_401()` | `tests/spec_api_sql.rs` | acceptance (negative) |
| API-SQL-004 | `fn api_sql_004_query_response_has_versions_read()` | `tests/spec_api_sql.rs` | acceptance |
| API-SQL-005 | `fn api_sql_005_write_commits_with_principal()` | `tests/spec_api_sql.rs` | acceptance |
| API-SQL-006 | `fn api_sql_006_ddl_create_table_with_classification()` | `tests/spec_api_sql.rs` | acceptance |
| API-SQL-006 | `fn api_sql_006_ddl_unclassified_column_rejected()` | `tests/spec_api_sql.rs` | acceptance (negative) |
| API-SQL-007 | `fn api_sql_007_request_id_in_header_body_and_audit()` | `tests/spec_api_sql.rs` | acceptance |
| API-SQL-008 | `fn api_sql_008_error_status_mapping()` | `tests/spec_api_sql.rs` | acceptance |
| API-SQL-008 | `fn test_error_into_response_mapping()` | `src/http_api.rs` | unit |
| API-SQL-009 | `fn api_sql_009_oversized_body_413()` | `tests/spec_api_sql.rs` | acceptance (negative) |
| API-SQL-010 | `fn api_sql_010_health_open_metrics_protected()` | `tests/spec_api_sql.rs` | acceptance |
| API-SQL-011 | `fn api_sql_011_audit_failure_blocks_execution()` | `tests/spec_api_sql.rs` | acceptance (negative) |
| API-SQL-012 | `fn api_sql_012_as_of_json_equivalent_to_sql()` | `tests/spec_api_sql.rs` | acceptance |
| API-SQL-013 | `fn api_sql_013_no_store_and_no_sql_in_info_logs()` | `tests/spec_api_sql.rs` | acceptance |

## 7. Open questions
* Token format for the static authenticator: opaque strings only, or also HMAC-signed tokens
  with expiry so API-SQL-003's "expired" case is testable without an IdP? Leaning: opaque +
  optional `expires_at` field in the JSON file.
* Should `versions_read` include the catalog table version (policies in force)? Leaning: yes,
  as `catalog_version`, since it is needed to reproduce masking decisions.
* Result pagination for large scans; current sketch buffers all rows.

## 8. Change log
| Date | Change |
|------|--------|
| 2026-09-02 | Created |
| 2026-09-03 | M2: `/v1/ddl` accepts the GOV-ACC statements; DDL responses carry `rows`; `QueryResult`/`/v1/query` carry `plan_hash`; `Session::explain_governed` added (audited as `EXPLAIN`); joins are accepted (`versions_read` lists every table). Deny-by-default now applies: a principal needs a grant or ownership to read or write (GOV-ACC-001). |
| 2026-09-02 | Implemented: `src/session.rs` (single entry point), `src/auth.rs` (`Authenticator`, `StaticFileAuthenticator` with optional `expires_at`), `src/http_api.rs`, `src/metrics.rs`; `src/main.rs` gains `serve` (default) and `audit verify|export`. API-SQL-001 is enforced by a `compile_fail` doctest on the crate root plus `api_sql_001_executor_is_crate_private`. Rows are returned as JSON objects keyed by column. Every failure path, including pre-governance parse/resolution errors and unauthenticated requests, writes an audit record, so an unavailable sink yields 503 for any request (Constitution 7). API-SQL-013: `Cache-Control: no-store` on every response; SQL text is never logged. Policy DDL deferred to GOV-ACC (006 amended). Open question 1 resolved: opaque tokens with optional `expires_at`. |
