//! Acceptance tests for specs/features/API-SQL-governed-session.md, plus the execution-level
//! scenarios of LAKE-TT, GOV-CLS and GOV-AUD that need the governed session.

use reqwest::StatusCode;
use rusthouse::auth::StaticFileAuthenticator;
use rusthouse::catalog::{Catalog, Principal};
use rusthouse::config::Config;
use rusthouse::governance::audit::{
    AuditSink, FailingAuditSink, FileAuditSink, MemoryAuditSink, Status,
};
use rusthouse::governance::classification::{Classification, Level, Tag};
use rusthouse::http_api::{create_router, AppState};
use rusthouse::lake::log::{Action, Commit, CommitInfo, Operation, TransactionLog, Version};
use rusthouse::session::Session;
use rusthouse::RustHouseError;
use serde_json::{json, Value};
use std::sync::Arc;

const AUTH_JSON: &str = r#"{
  "tokens": {
    "t-analyst": {"id": "alice", "roles": ["analyst"]},
    "t-dpo": {"id": "dpo", "roles": ["classification_admin"]},
    "t-metrics": {"id": "prom", "roles": ["metrics_reader"]},
    "t-expired": {"id": "old", "roles": [], "expires_at": "2000-01-01T00:00:00Z"}
  }
}"#;

struct Env {
    _dir: tempfile::TempDir,
    session: Arc<Session>,
    audit: Arc<MemoryAuditSink>,
}

async fn env() -> Env {
    let dir = tempfile::tempdir().unwrap();
    let catalog = Arc::new(Catalog::open(dir.path()).await.unwrap());
    let audit = Arc::new(MemoryAuditSink::new());
    let session = Arc::new(Session::new(catalog, audit.clone()));
    Env {
        _dir: dir,
        session,
        audit,
    }
}

fn alice() -> Principal {
    Principal::new("alice", vec!["analyst".into()])
}

async fn seed(session: &Session, table: &str, rows: Vec<Value>) -> u64 {
    let p = alice();
    session
        .execute_ddl(
            &p,
            &format!(
                "CREATE TABLE {table} (id INT64, email STRING, balance FLOAT64) \
                 WITH CLASSIFICATION (id Internal, email Confidential PII, balance Confidential FINANCIAL)"
            ),
        )
        .await
        .unwrap();
    let rows: Vec<serde_json::Map<String, Value>> = rows
        .into_iter()
        .map(|r| r.as_object().unwrap().clone())
        .collect();
    session.write_rows(&p, table, rows).await.unwrap().version
}

fn three_rows() -> Vec<Value> {
    vec![
        json!({"id": 1, "email": "a@x", "balance": 10.5}),
        json!({"id": 2, "email": "b@x", "balance": 20.0}),
        json!({"id": 3, "email": "c@x", "balance": 30.0}),
    ]
}

struct Server {
    base: String,
    client: reqwest::Client,
}

async fn serve(session: Arc<Session>, max_request_bytes: usize) -> Server {
    let mut config = Config::new("unused".into(), "127.0.0.1:0".into());
    config.max_request_bytes = max_request_bytes;
    let state = AppState {
        session,
        authenticator: Arc::new(StaticFileAuthenticator::from_json(AUTH_JSON).unwrap()),
        config: Arc::new(config),
    };
    let app = create_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Server {
        base: format!("http://{addr}"),
        client: reqwest::Client::new(),
    }
}

impl Server {
    async fn post(&self, path: &str, token: Option<&str>, body: Value) -> reqwest::Response {
        let mut r = self
            .client
            .post(format!("{}{}", self.base, path))
            .json(&body);
        if let Some(t) = token {
            r = r.bearer_auth(t);
        }
        r.send().await.unwrap()
    }
    async fn get(&self, path: &str, token: Option<&str>) -> reqwest::Response {
        let mut r = self.client.get(format!("{}{}", self.base, path));
        if let Some(t) = token {
            r = r.bearer_auth(t);
        }
        r.send().await.unwrap()
    }
}

// ---------------------------------------------------------------------------------------
// API-SQL
// ---------------------------------------------------------------------------------------

/// The executor is `pub(crate)`; the proof is the `compile_fail` doctest on the crate root
/// (`src/lib.rs`), which fails to compile `rusthouse::exec::Executor` from outside.
/// This test pins the positive half: the governed entry point is public.
#[tokio::test]
async fn api_sql_001_executor_is_crate_private() {
    // REQ: API-SQL-001, CORE-EXE-001, GOV-AUD-011
    let e = env().await;
    seed(&e.session, "gold.accounts", three_rows()).await;
    let r = e
        .session
        .execute_governed(&alice(), "SELECT id FROM gold.accounts")
        .await
        .unwrap();
    assert_eq!(r.rows.len(), 3);
}

#[tokio::test]
async fn api_sql_002_bearer_token_resolves_principal() {
    // REQ: API-SQL-002
    let e = env().await;
    seed(&e.session, "gold.accounts", three_rows()).await;
    let s = serve(e.session.clone(), 1 << 20).await;
    let resp = s
        .post(
            "/v1/query",
            Some("t-analyst"),
            json!({"sql": "SELECT id FROM gold.accounts"}),
        )
        .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let intents: Vec<_> = e
        .audit
        .records()
        .into_iter()
        .filter(|r| r.status == Status::Intent && r.operation == "QUERY")
        .collect();
    assert_eq!(intents.last().unwrap().principal, "alice");
}

#[tokio::test]
async fn api_sql_003_missing_credentials_401_and_audited() {
    // REQ: API-SQL-003
    let e = env().await;
    let s = serve(e.session.clone(), 1 << 20).await;
    let resp = s.post("/v1/query", None, json!({"sql": "SELECT 1"})).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "unauthenticated");
    assert!(body["request_id"].is_string());
    assert_eq!(
        body.as_object().unwrap().len(),
        2,
        "no schema/table information leaks: {body}"
    );
    let rid = body["request_id"].as_str().unwrap();
    let rec = e
        .audit
        .records()
        .into_iter()
        .find(|r| r.request_id == rid)
        .expect("unauthenticated request is audited");
    assert_eq!(rec.status, Status::Unauthenticated);
}

#[tokio::test]
async fn api_sql_003_unknown_token_401() {
    // REQ: API-SQL-003
    let e = env().await;
    let s = serve(e.session.clone(), 1 << 20).await;
    for token in ["unknown-token", "t-expired"] {
        let resp = s
            .post("/v1/query", Some(token), json!({"sql": "SELECT 1"}))
            .await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "{token}");
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["error"], "unauthenticated");
    }
}

#[tokio::test]
async fn api_sql_004_query_response_has_versions_read() {
    // REQ: API-SQL-004, LAKE-TT-006
    let e = env().await;
    let v = seed(&e.session, "silver.trades", three_rows()).await;
    assert_eq!(v, 1);
    let s = serve(e.session.clone(), 1 << 20).await;
    let resp = s
        .post(
            "/v1/query",
            Some("t-analyst"),
            json!({"sql": "SELECT id FROM silver.trades"}),
        )
        .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert!(body["request_id"].is_string());
    assert_eq!(body["rows"].as_array().unwrap().len(), 3);
    assert_eq!(
        body["versions_read"],
        json!([{"table": "silver.trades", "version": 1}])
    );
    assert!(body["elapsed_ms"].is_number());
}

#[tokio::test]
async fn api_sql_005_write_commits_with_principal() {
    // REQ: API-SQL-005
    let e = env().await;
    seed(&e.session, "silver.trades", three_rows()).await;
    let s = serve(e.session.clone(), 1 << 20).await;
    let resp = s
        .post(
            "/v1/write?table=silver.trades",
            Some("t-analyst"),
            json!({"rows": [{"id": 4, "email": "d@x", "balance": 1.0}, {"id": 5, "email": "e@x", "balance": 2.0}]}),
        )
        .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["version"], 2);
    assert_eq!(body["rows_written"], 2);
    let table = e.session.catalog().resolve("silver.trades", None).unwrap();
    let log = TransactionLog::open(&table.path).await.unwrap();
    let h = log.history(Version(2), Version(2)).await.unwrap();
    assert_eq!(h[0].principal, "alice");
    assert_eq!(h[0].operation, Operation::Write);
    // Data files are Parquet and open formats: five rows are now visible.
    let r = e
        .session
        .execute_governed(&alice(), "SELECT COUNT(*) AS n FROM silver.trades")
        .await
        .unwrap();
    assert_eq!(r.rows[0][0], json!(5));
}

#[tokio::test]
async fn api_sql_006_ddl_create_table_with_classification() {
    // REQ: API-SQL-006, GOV-CLS-001
    let e = env().await;
    let s = serve(e.session.clone(), 1 << 20).await;
    let resp = s
        .post(
            "/v1/ddl",
            Some("t-analyst"),
            json!({"sql": "CREATE TABLE silver.x (id INT64, iban STRING) WITH CLASSIFICATION (id Internal, iban Restricted PII)"}),
        )
        .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let r = e.session.catalog().resolve("silver.x", None).unwrap();
    let d = e.session.catalog().describe(&r).await.unwrap();
    let iban = d.schema.field("iban").unwrap().classification().unwrap();
    assert_eq!(iban.level, Level::Restricted);
    assert!(iban.tags.contains(&Tag::Pii));
}

#[tokio::test]
async fn api_sql_006_ddl_unclassified_column_rejected() {
    // REQ: API-SQL-006, GOV-CLS-001
    let e = env().await;
    let s = serve(e.session.clone(), 1 << 20).await;
    let resp = s
        .post(
            "/v1/ddl",
            Some("t-analyst"),
            json!({"sql": "CREATE TABLE silver.y (id INT64, iban STRING)"}),
        )
        .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body: Value = resp.json().await.unwrap();
    let msg = body["error"].as_str().unwrap();
    assert!(msg.contains("id") && msg.contains("iban"), "{msg}");
    assert!(e.session.catalog().resolve("silver.y", None).is_err());
}

#[tokio::test]
async fn api_sql_007_request_id_in_header_body_and_audit() {
    // REQ: API-SQL-007
    let e = env().await;
    seed(&e.session, "gold.accounts", three_rows()).await;
    let s = serve(e.session.clone(), 1 << 20).await;
    for (token, sql) in [
        (Some("t-analyst"), "SELECT id FROM gold.accounts"),
        (Some("t-analyst"), "SELECT id FROM gold.missing"),
        (None, "SELECT 1"),
    ] {
        let resp = s.post("/v1/query", token, json!({"sql": sql})).await;
        let header = resp
            .headers()
            .get("x-request-id")
            .expect("header present")
            .to_str()
            .unwrap()
            .to_string();
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["request_id"], header);
        uuid::Uuid::parse_str(&header).expect("uuid");
        let recs: Vec<_> = e
            .audit
            .records()
            .into_iter()
            .filter(|r| r.request_id == header)
            .collect();
        assert!(
            !recs.is_empty(),
            "every request leaves an audit trail ({sql})"
        );
    }
}

#[tokio::test]
async fn api_sql_008_error_status_mapping() {
    // REQ: API-SQL-008
    let e = env().await;
    seed(&e.session, "gold.accounts", three_rows()).await;
    let s = serve(e.session.clone(), 1 << 20).await;
    let cases = [
        ("SELECT id FROM gold.nope", StatusCode::NOT_FOUND),
        ("SELECT nope FROM gold.accounts", StatusCode::NOT_FOUND),
        ("SELECT FROM WHERE", StatusCode::BAD_REQUEST),
        (
            "SELECT id FROM gold.accounts AS OF VERSION 99",
            StatusCode::NOT_FOUND,
        ),
    ];
    for (sql, status) in cases {
        let resp = s
            .post("/v1/query", Some("t-analyst"), json!({"sql": sql}))
            .await;
        assert_eq!(resp.status(), status, "{sql}");
        let body: Value = resp.json().await.unwrap();
        let msg = body["error"].as_str().unwrap();
        assert!(
            !msg.contains('/'),
            "no filesystem path in error body: {msg}"
        );
    }
    // Denied maps to 403: lowering a classification without the admin role.
    let resp = s
        .post(
            "/v1/ddl",
            Some("t-analyst"),
            json!({"sql": "ALTER TABLE gold.accounts ALTER COLUMN email CLASSIFIED AS Public REASON 'x'"}),
        )
        .await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn api_sql_009_oversized_body_413() {
    // REQ: API-SQL-009
    let e = env().await;
    seed(&e.session, "silver.trades", three_rows()).await;
    let s = serve(e.session.clone(), 1024).await;
    let big: Vec<Value> = (0..100)
        .map(|i| json!({"id": i, "email": "x".repeat(40), "balance": 1.0}))
        .collect();
    let resp = s
        .post(
            "/v1/write?table=silver.trades",
            Some("t-analyst"),
            json!({"rows": big}),
        )
        .await;
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let table = e.session.catalog().resolve("silver.trades", None).unwrap();
    let log = TransactionLog::open(&table.path).await.unwrap();
    assert_eq!(log.head().await.unwrap(), Version(1), "no commit was made");
}

#[tokio::test]
async fn api_sql_010_health_open_metrics_protected() {
    // REQ: API-SQL-010
    let e = env().await;
    let s = serve(e.session.clone(), 1 << 20).await;
    let resp = s.get("/health", None).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.text().await.unwrap(), "OK");
    assert_eq!(
        s.get("/metrics", None).await.status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        s.get("/metrics", Some("t-analyst")).await.status(),
        StatusCode::FORBIDDEN
    );
    let resp = s.get("/metrics", Some("t-metrics")).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp.headers()["content-type"]
        .to_str()
        .unwrap()
        .starts_with("text/plain"));
    let text = resp.text().await.unwrap();
    assert!(
        text.lines().any(|l| l.starts_with("# TYPE rusthouse_")),
        "{text}"
    );
}

#[tokio::test]
async fn api_sql_011_audit_failure_blocks_execution() {
    // REQ: API-SQL-011, GOV-AUD-001
    let dir = tempfile::tempdir().unwrap();
    let catalog = Arc::new(Catalog::open(dir.path()).await.unwrap());
    // Seed with a working sink, then swap in a failing one.
    let good = Arc::new(Session::new(
        catalog.clone(),
        Arc::new(MemoryAuditSink::new()),
    ));
    seed(&good, "gold.accounts", three_rows()).await;
    let bad = Arc::new(Session::new(catalog, Arc::new(FailingAuditSink)));
    let s = serve(bad.clone(), 1 << 20).await;
    let resp = s
        .post(
            "/v1/query",
            Some("t-analyst"),
            json!({"sql": "SELECT id FROM gold.accounts"}),
        )
        .await;
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(bad.executions(), 0, "executor never invoked");
}

#[tokio::test]
async fn api_sql_012_as_of_json_equivalent_to_sql() {
    // REQ: API-SQL-012
    let e = env().await;
    seed(&e.session, "gold.accounts", three_rows()).await;
    e.session
        .write_rows(
            &alice(),
            "gold.accounts",
            vec![json!({"id": 9, "email": "z", "balance": 0.0})
                .as_object()
                .unwrap()
                .clone()],
        )
        .await
        .unwrap();
    let s = serve(e.session.clone(), 1 << 20).await;
    let a: Value = s
        .post(
            "/v1/query",
            Some("t-analyst"),
            json!({"sql": "SELECT id FROM gold.accounts AS OF VERSION 1 ORDER BY id"}),
        )
        .await
        .json()
        .await
        .unwrap();
    let b: Value = s
        .post(
            "/v1/query",
            Some("t-analyst"),
            json!({"sql": "SELECT id FROM gold.accounts ORDER BY id", "as_of": {"version": 1}}),
        )
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(a["rows"], b["rows"]);
    assert_eq!(a["versions_read"], b["versions_read"]);
    assert_eq!(a["rows"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn api_sql_013_no_store_and_no_sql_in_info_logs() {
    // REQ: API-SQL-013 (the no-store half; log content is covered by review, tracing is not
    // enabled in tests)
    let e = env().await;
    let s = serve(e.session.clone(), 1 << 20).await;
    let resp = s.get("/health", None).await;
    assert_eq!(resp.headers()["cache-control"], "no-store");
}

// ---------------------------------------------------------------------------------------
// LAKE-TT execution scenarios
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn lake_tt_003_timestamp_resolves_to_latest_commit_at_or_before() {
    // REQ: LAKE-TT-003
    let e = env().await;
    seed(&e.session, "gold.positions", three_rows()).await; // v1
    let table = e.session.catalog().resolve("gold.positions", None).unwrap();
    let log = TransactionLog::open(&table.path).await.unwrap();
    let t1 = log.history(Version(1), Version(1)).await.unwrap()[0].timestamp as i64;
    let iso = |ms: i64| {
        chrono::DateTime::from_timestamp_millis(ms)
            .unwrap()
            .to_rfc3339()
    };
    let r = e
        .session
        .execute_governed(
            &alice(),
            &format!(
                "SELECT id FROM gold.positions AS OF TIMESTAMP '{}'",
                iso(t1 + 1)
            ),
        )
        .await
        .unwrap();
    assert_eq!(r.versions_read[0].version, 1);
    let t0 = log.history(Version(0), Version(0)).await.unwrap()[0].timestamp as i64;
    let err = e
        .session
        .execute_governed(
            &alice(),
            &format!(
                "SELECT id FROM gold.positions AS OF TIMESTAMP '{}'",
                iso(t0 - 60_000)
            ),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, RustHouseError::VersionNotFound(_)), "{err:?}");
}

#[tokio::test]
async fn lake_tt_004_future_version_is_not_found() {
    // REQ: LAKE-TT-004
    let e = env().await;
    seed(&e.session, "gold.positions", three_rows()).await;
    let err = e
        .session
        .execute_governed(&alice(), "SELECT id FROM gold.positions AS OF VERSION 7")
        .await
        .unwrap_err();
    assert!(matches!(err, RustHouseError::VersionNotFound(7)), "{err:?}");
}

#[tokio::test]
async fn lake_tt_005_head_pinned_once_per_statement() {
    // REQ: LAKE-TT-005
    let e = env().await;
    seed(&e.session, "gold.positions", three_rows()).await;
    // Pin at head=1, then commit version 2 before execution continues: the pinned snapshot
    // must still be what is read. We simulate "mid-execution" by pinning explicitly.
    let pinned = e
        .session
        .pin_snapshot(&alice(), "gold.positions")
        .await
        .unwrap();
    e.session
        .write_rows(
            &alice(),
            "gold.positions",
            vec![json!({"id": 9, "email": "z", "balance": 0.0})
                .as_object()
                .unwrap()
                .clone()],
        )
        .await
        .unwrap();
    let r = e
        .session
        .execute_pinned(&alice(), "SELECT id FROM gold.positions", &pinned)
        .await
        .unwrap();
    assert_eq!(r.versions_read[0].version, 1);
    assert_eq!(r.rows.len(), 3);
}

#[tokio::test]
async fn lake_tt_006_versions_read_present_without_as_of() {
    // REQ: LAKE-TT-006
    let e = env().await;
    seed(&e.session, "gold.positions", three_rows()).await;
    let r = e
        .session
        .execute_governed(&alice(), "SELECT COUNT(*) FROM gold.positions")
        .await
        .unwrap();
    assert_eq!(r.versions_read.len(), 1);
    assert_eq!(r.versions_read[0].table, "gold.positions");
    assert_eq!(r.versions_read[0].version, 1);
}

#[tokio::test]
async fn lake_tt_007_same_version_is_byte_identical_after_appends() {
    // REQ: LAKE-TT-007
    let e = env().await;
    seed(&e.session, "gold.positions", three_rows()).await;
    let sql = "SELECT id, balance FROM gold.positions AS OF VERSION 1 ORDER BY id";
    let b1 = e
        .session
        .execute_governed(&alice(), sql)
        .await
        .unwrap()
        .body_bytes();
    for i in 0..3 {
        e.session
            .write_rows(
                &alice(),
                "gold.positions",
                vec![json!({"id": 100 + i, "email": "n", "balance": 1.0})
                    .as_object()
                    .unwrap()
                    .clone()],
            )
            .await
            .unwrap();
    }
    let b2 = e
        .session
        .execute_governed(&alice(), sql)
        .await
        .unwrap()
        .body_bytes();
    assert_eq!(b1, b2);
}

#[tokio::test]
async fn lake_tt_008_vacuumed_version_refused_no_partial_rows() {
    // REQ: LAKE-TT-008
    let e = env().await;
    seed(&e.session, "gold.positions", three_rows()).await; // v1: file A
    e.session
        .write_rows(
            &alice(),
            "gold.positions",
            vec![json!({"id": 9, "email": "z", "balance": 0.0})
                .as_object()
                .unwrap()
                .clone()],
        )
        .await
        .unwrap(); // v2: file B
    let table = e.session.catalog().resolve("gold.positions", None).unwrap();
    let log = TransactionLog::open(&table.path).await.unwrap();
    let v1 = log.snapshot(Some(Version(1))).await.unwrap();
    let file_a = v1.files[0].path.clone();
    // Logically remove A at v3, then physically vacuum it.
    log.commit(Commit::new(
        CommitInfo::new("alice", Operation::Vacuum, json!({})),
        vec![Action::Remove {
            path: file_a.clone(),
            deletion_timestamp: 0,
        }],
    ))
    .await
    .unwrap();
    std::fs::remove_file(table.path.join(&file_a)).unwrap();

    let err = e
        .session
        .execute_governed(&alice(), "SELECT id FROM gold.positions AS OF VERSION 2")
        .await
        .unwrap_err();
    match err {
        RustHouseError::VersionVacuumed { table, version } => {
            assert_eq!(table, "gold.positions");
            assert_eq!(version, 2);
        }
        other => panic!("{other:?}"),
    }
    // Head still works (only B).
    let r = e
        .session
        .execute_governed(&alice(), "SELECT id FROM gold.positions")
        .await
        .unwrap();
    assert_eq!(r.rows.len(), 1);
}

#[tokio::test]
async fn lake_tt_009_audit_record_matches_versions_read() {
    // REQ: LAKE-TT-009, GOV-AUD-005
    let e = env().await;
    seed(&e.session, "gold.positions", three_rows()).await;
    e.session
        .write_rows(
            &alice(),
            "gold.positions",
            vec![json!({"id": 9, "email": "z", "balance": 0.0})
                .as_object()
                .unwrap()
                .clone()],
        )
        .await
        .unwrap();
    let r = e
        .session
        .execute_governed(&alice(), "SELECT id FROM gold.positions AS OF VERSION 1")
        .await
        .unwrap();
    let outcome = e
        .audit
        .records()
        .into_iter()
        .find(|a| a.request_id == r.request_id && a.status == Status::Ok)
        .unwrap();
    assert_eq!(outcome.versions_read.get("gold.positions"), Some(&1));
    assert_eq!(outcome.row_count, Some(3));
    let intent = e
        .audit
        .records()
        .into_iter()
        .find(|a| a.request_id == r.request_id && a.status == Status::Intent)
        .unwrap();
    assert!(intent.seq < outcome.seq, "intent precedes outcome");
    assert_eq!(intent.versions_read, outcome.versions_read);
}

// ---------------------------------------------------------------------------------------
// GOV-CLS execution scenarios
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn gov_cls_001_create_without_classification_is_refused() {
    // REQ: GOV-CLS-001
    let e = env().await;
    let err = e
        .session
        .execute_ddl(
            &alice(),
            "CREATE TABLE silver.customers (id INT64, email STRING)",
        )
        .await
        .unwrap_err();
    match err {
        RustHouseError::MissingClassification(m) => {
            assert!(m.contains("id") && m.contains("email"), "{m}")
        }
        other => panic!("{other:?}"),
    }
    assert!(!e._dir.path().join("silver/customers/_rhlog").exists());
    assert!(e
        .session
        .catalog()
        .resolve("silver.customers", None)
        .is_err());
    assert_eq!(e.session.catalog().version(), 0);
}

#[tokio::test]
async fn gov_cls_004_classification_is_time_travelable() {
    // REQ: GOV-CLS-004
    let e = env().await;
    seed(&e.session, "silver.customers", three_rows()).await; // v1
    let dpo = Principal::new("dpo", vec!["classification_admin".into()]);
    e.session
        .execute_ddl(
            &dpo,
            "ALTER TABLE silver.customers ALTER COLUMN email CLASSIFIED AS Restricted TAGS (PII)",
        )
        .await
        .unwrap(); // v2
    let s0 = e
        .session
        .schema_as_of(&alice(), "silver.customers", 1)
        .await
        .unwrap();
    assert_eq!(
        s0.field("email").unwrap().classification().unwrap().level,
        Level::Confidential
    );
    let s1 = e
        .session
        .schema_as_of(&alice(), "silver.customers", 2)
        .await
        .unwrap();
    assert_eq!(
        s1.field("email").unwrap().classification().unwrap().level,
        Level::Restricted
    );
}

#[tokio::test]
async fn gov_cls_005_reclassification_commit_is_audited() {
    // REQ: GOV-CLS-005, GOV-AUD-008
    let e = env().await;
    seed(&e.session, "silver.customers", three_rows()).await;
    let dpo = Principal::new("dpo", vec!["classification_admin".into()]);
    let d = e
        .session
        .execute_ddl(&dpo, "ALTER TABLE silver.customers ALTER COLUMN email CLASSIFIED AS Restricted TAGS (PII, SPI)")
        .await
        .unwrap();
    assert_eq!(d.version, Some(2));
    let table = e
        .session
        .catalog()
        .resolve("silver.customers", None)
        .unwrap();
    let log = TransactionLog::open(&table.path).await.unwrap();
    let h = log.history(Version(2), Version(2)).await.unwrap();
    assert_eq!(h[0].operation, Operation::Alter);
    assert_eq!(h[0].principal, "dpo");
    let rec = e
        .audit
        .records()
        .into_iter()
        .find(|r| r.operation == "ALTER" && r.status == Status::Ok)
        .expect("audited");
    assert_eq!(rec.object, "silver.customers");
    assert_eq!(rec.columns, vec!["email"]);
    assert_eq!(rec.classifications["email"].level, Level::Restricted);
    assert_eq!(
        rec.classifications["email.previous"].level,
        Level::Confidential
    );
    assert_eq!(rec.versions_read.get("silver.customers"), Some(&2));
}

#[tokio::test]
async fn gov_cls_009_audit_record_carries_classifications() {
    // REQ: GOV-CLS-009
    let e = env().await;
    seed(&e.session, "gold.accounts", three_rows()).await;
    let r = e
        .session
        .execute_governed(&alice(), "SELECT email, balance FROM gold.accounts")
        .await
        .unwrap();
    let rec = e
        .audit
        .records()
        .into_iter()
        .find(|a| a.request_id == r.request_id && a.status == Status::Ok)
        .unwrap();
    assert_eq!(rec.columns, vec!["email", "balance"]);
    assert_eq!(
        rec.classifications["email"],
        Classification::new(Level::Confidential).with_tags([Tag::Pii])
    );
    assert_eq!(
        rec.classifications["balance"],
        Classification::new(Level::Confidential).with_tags([Tag::Financial])
    );
}

#[tokio::test]
async fn gov_cls_010_read_of_unclassified_metadata_fails_closed_end_to_end() {
    // REQ: GOV-CLS-010
    let e = env().await;
    seed(&e.session, "gold.accounts", three_rows()).await;
    // Hand-edit the log: strip classification from the metaData of version 0 by rewriting a
    // new table log with an unclassified schema at the same path.
    let table = e.session.catalog().resolve("gold.accounts", None).unwrap();
    let v0 = table.path.join("_rhlog/00000000000000000000.json");
    let text = std::fs::read_to_string(&v0).unwrap();
    let stripped: String = text
        .lines()
        .map(|l| {
            let mut v: Value = serde_json::from_str(l).unwrap();
            if v["action"] == "metaData" {
                for f in v["schema"]["fields"].as_array_mut().unwrap() {
                    f.as_object_mut().unwrap().remove("classification");
                }
            }
            serde_json::to_string(&v).unwrap()
        })
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(&v0, stripped + "\n").unwrap();

    let err = e
        .session
        .execute_governed(&alice(), "SELECT id FROM gold.accounts")
        .await
        .unwrap_err();
    assert!(
        matches!(err, RustHouseError::MissingClassification(_)),
        "{err:?}"
    );
    let deny = e
        .audit
        .records()
        .into_iter()
        .rev()
        .find(|r| r.object == "gold.accounts" && r.status == Status::Error)
        .expect("refusal is audited");
    assert_eq!(deny.decision, rusthouse::governance::audit::Decision::Deny);
    assert!(
        deny.operation.contains("missing_classification"),
        "{}",
        deny.operation
    );
}

#[tokio::test]
async fn gov_cls_011_lowering_requires_admin_and_reason() {
    // REQ: GOV-CLS-011
    let e = env().await;
    seed(&e.session, "gold.accounts", three_rows()).await;
    let lower = "ALTER TABLE gold.accounts ALTER COLUMN balance CLASSIFIED AS Internal";
    let err = e
        .session
        .execute_ddl(&alice(), &format!("{lower} REASON 'aggregated'"))
        .await
        .unwrap_err();
    assert!(matches!(err, RustHouseError::Denied(_)), "{err:?}");
    let dpo = Principal::new("dpo", vec!["classification_admin".into()]);
    let err = e.session.execute_ddl(&dpo, lower).await.unwrap_err();
    assert!(
        matches!(&err, RustHouseError::SqlParse(m) if m.contains("REASON")),
        "{err:?}"
    );
    let d = e
        .session
        .execute_ddl(
            &dpo,
            &format!("{lower} REASON 'aggregated per DPO ticket 123'"),
        )
        .await
        .unwrap();
    assert_eq!(d.version, Some(2));
    let table = e.session.catalog().resolve("gold.accounts", None).unwrap();
    let log = TransactionLog::open(&table.path).await.unwrap();
    let h = log.history(Version(2), Version(2)).await.unwrap();
    assert_eq!(
        h[0].operation_parameters["reason"],
        "aggregated per DPO ticket 123"
    );
    // Raising needs no special role.
    e.session
        .execute_ddl(
            &alice(),
            "ALTER TABLE gold.accounts ALTER COLUMN id CLASSIFIED AS Confidential",
        )
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------------------
// GOV-AUD session scenarios
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn gov_aud_001_unwritable_sink_refuses_operation() {
    // REQ: GOV-AUD-001
    let dir = tempfile::tempdir().unwrap();
    let catalog = Arc::new(Catalog::open(dir.path()).await.unwrap());
    let good = Arc::new(Session::new(
        catalog.clone(),
        Arc::new(MemoryAuditSink::new()),
    ));
    seed(&good, "gold.accounts", three_rows()).await;
    let bad = Session::new(catalog.clone(), Arc::new(FailingAuditSink));
    let err = bad
        .execute_governed(&alice(), "SELECT id FROM gold.accounts")
        .await
        .unwrap_err();
    assert!(
        matches!(err, RustHouseError::AuditUnavailable(_)),
        "{err:?}"
    );
    assert_eq!(bad.executions(), 0);
    // Writes are refused too: no new commit.
    let err = bad
        .write_rows(
            &alice(),
            "gold.accounts",
            vec![json!({"id": 1, "email": "a", "balance": 1.0})
                .as_object()
                .unwrap()
                .clone()],
        )
        .await
        .unwrap_err();
    assert!(matches!(err, RustHouseError::AuditUnavailable(_)));
    let table = catalog.resolve("gold.accounts", None).unwrap();
    assert_eq!(
        TransactionLog::open(&table.path)
            .await
            .unwrap()
            .head()
            .await
            .unwrap(),
        Version(1)
    );
}

#[tokio::test]
async fn gov_aud_001_http_maps_to_503() {
    // REQ: GOV-AUD-001
    let dir = tempfile::tempdir().unwrap();
    let catalog = Arc::new(Catalog::open(dir.path()).await.unwrap());
    let bad = Arc::new(Session::new(catalog, Arc::new(FailingAuditSink)));
    let s = serve(bad, 1 << 20).await;
    let resp = s
        .post("/v1/query", Some("t-analyst"), json!({"sql": "SELECT 1"}))
        .await;
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn gov_aud_005_intent_precedes_outcome() {
    // REQ: GOV-AUD-005
    let e = env().await;
    seed(&e.session, "gold.accounts", three_rows()).await;
    let r = e
        .session
        .execute_governed(&alice(), "SELECT id FROM gold.accounts")
        .await
        .unwrap();
    let recs: Vec<_> = e
        .audit
        .records()
        .into_iter()
        .filter(|a| a.request_id == r.request_id)
        .collect();
    assert_eq!(recs.len(), 2);
    assert_eq!(recs[0].status, Status::Intent);
    assert_eq!(recs[1].status, Status::Ok);
    assert_eq!(recs[1].row_count, Some(3));
    assert!(recs[1].elapsed_ms.is_some());
    assert_eq!(recs[1].versions_read.get("gold.accounts"), Some(&1));
}

#[tokio::test]
async fn gov_aud_005_error_path_writes_outcome() {
    // REQ: GOV-AUD-005
    let e = env().await;
    seed(&e.session, "gold.accounts", three_rows()).await;
    // Passes governance (columns exist in the schema) but fails in execution: the data file
    // is removed underneath the query.
    let table = e.session.catalog().resolve("gold.accounts", None).unwrap();
    let snap = TransactionLog::open(&table.path)
        .await
        .unwrap()
        .snapshot(None)
        .await
        .unwrap();
    std::fs::write(table.path.join(&snap.files[0].path), b"garbage").unwrap();
    let err = e
        .session
        .execute_governed(&alice(), "SELECT id FROM gold.accounts")
        .await
        .unwrap_err();
    assert!(!matches!(err, RustHouseError::AuditUnavailable(_)));
    let last = e.audit.records().pop().unwrap();
    assert_eq!(last.status, Status::Error);
    assert_eq!(last.object, "gold.accounts");
}

#[tokio::test]
async fn gov_aud_008_commits_and_policy_changes_audited() {
    // REQ: GOV-AUD-008
    let e = env().await;
    seed(&e.session, "silver.trades", three_rows()).await; // WRITE producing version 1
    let recs = e.audit.records();
    let create = recs
        .iter()
        .find(|r| r.operation == "CREATE_TABLE" && r.status == Status::Ok)
        .expect("create audited");
    assert_eq!(create.object, "silver.trades");
    assert_eq!(create.versions_read.get("silver.trades"), Some(&0));
    assert_eq!(create.versions_read.get("catalog"), Some(&1));
    let write = recs
        .iter()
        .find(|r| r.operation == "WRITE" && r.status == Status::Ok)
        .expect("write audited");
    assert_eq!(write.object, "silver.trades");
    assert_eq!(write.versions_read.get("silver.trades"), Some(&1));
    assert_eq!(write.row_count, Some(3));
}

/// Real file sink end-to-end: the chain verifies after a governed session ran (GOV-AUD-004/006
/// integration).
#[tokio::test]
async fn gov_aud_file_sink_end_to_end_chain_verifies() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = Arc::new(Catalog::open(dir.path()).await.unwrap());
    let sink: Arc<dyn AuditSink> =
        Arc::new(FileAuditSink::open(&dir.path().join("audit")).unwrap());
    let session = Arc::new(Session::new(catalog, sink));
    seed(&session, "gold.accounts", three_rows()).await;
    session
        .execute_governed(&alice(), "SELECT id FROM gold.accounts")
        .await
        .unwrap();
    let report = rusthouse::governance::audit::verify(&dir.path().join("audit"), None).unwrap();
    assert!(report.ok, "{report:?}");
    assert!(report.records >= 6);
}
