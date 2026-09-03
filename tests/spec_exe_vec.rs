//! Acceptance tests for specs/features/EXE-VEC-vectorized-executor.md

use rusthouse::catalog::{Catalog, Principal};
use rusthouse::governance::audit::{MemoryAuditSink, Status};
use rusthouse::session::Session;
use rusthouse::RustHouseError;
use serde_json::{json, Value};
use std::sync::Arc;

async fn session(dir: &std::path::Path) -> (Arc<Session>, Arc<MemoryAuditSink>) {
    let catalog = Arc::new(Catalog::open(dir).await.unwrap());
    let audit = Arc::new(MemoryAuditSink::new());
    (Arc::new(Session::new(catalog, audit.clone())), audit)
}

fn owner() -> Principal {
    Principal::new("owner", vec![])
}

async fn seed(s: &Session, table: &str, n_files: usize, rows_per_file: usize) {
    s.execute_ddl(
        &owner(),
        &format!(
            "CREATE TABLE {table} (id INT64, country STRING, balance FLOAT64) \
             WITH CLASSIFICATION (id Internal, country Internal, balance Confidential FINANCIAL)"
        ),
    )
    .await
    .unwrap();
    for f in 0..n_files {
        let rows = (0..rows_per_file)
            .map(|i| {
                let id = (f * rows_per_file + i) as i64;
                json!({"id": id, "country": if id % 3 == 0 { "FR" } else { "DE" }, "balance": id as f64 * 1.5})
                    .as_object()
                    .unwrap()
                    .clone()
            })
            .collect();
        s.write_rows(&owner(), table, rows).await.unwrap();
    }
}

#[tokio::test]
async fn exe_vec_001_operators_exchange_record_batches() {
    // REQ: EXE-VEC-001 — observable through the engine name reported in the plan and the
    // join support that the row engine never had.
    let dir = tempfile::tempdir().unwrap();
    let (s, _) = session(dir.path()).await;
    seed(&s, "gold.a", 1, 10).await;
    let plan = s
        .explain_governed(&owner(), "SELECT id FROM gold.a")
        .await
        .unwrap();
    assert!(
        plan.contains("DataSourceExec"),
        "Arrow-native physical plan:\n{plan}"
    );
}

#[tokio::test]
async fn exe_vec_002_equivalent_to_row_engine() {
    // REQ: EXE-VEC-002 — the M1 row engine is the oracle; both read the same snapshot.
    let dir = tempfile::tempdir().unwrap();
    let (vector, _) = session(dir.path()).await;
    seed(&vector, "gold.a", 3, 40).await;
    let catalog = Arc::new(Catalog::open(dir.path()).await.unwrap());
    let row = Session::new(catalog, Arc::new(MemoryAuditSink::new())).with_row_engine();
    let queries = [
        "SELECT * FROM gold.a",
        "SELECT id, balance FROM gold.a WHERE id > 100 AND country = 'FR'",
        "SELECT country, COUNT(*) AS n, SUM(balance) AS s, MIN(id) AS lo, MAX(id) AS hi FROM gold.a GROUP BY country",
        "SELECT id FROM gold.a WHERE balance >= 30.0 ORDER BY id DESC LIMIT 7",
        "SELECT COUNT(*) AS n FROM gold.a WHERE id < 0",
        "SELECT AVG(balance) AS a FROM gold.a AS OF VERSION 1",
    ];
    for sql in queries {
        let v = vector.execute_governed(&owner(), sql).await.unwrap();
        let r = row.execute_governed(&owner(), sql).await.unwrap();
        let mut vr = v.rows.clone();
        let mut rr = r.rows.clone();
        let key = |x: &Vec<Value>| serde_json::to_string(x).unwrap();
        if !sql.contains("ORDER BY") {
            vr.sort_by_key(key);
            rr.sort_by_key(key);
        }
        assert_eq!(vr, rr, "{sql}");
        assert_eq!(v.versions_read, r.versions_read, "{sql}");
    }
}

#[tokio::test]
async fn exe_vec_003_governor_rewrite_survives_optimizer() {
    // REQ: EXE-VEC-003
    let dir = tempfile::tempdir().unwrap();
    let (s, _) = session(dir.path()).await;
    seed(&s, "gold.a", 2, 30).await;
    let sec = Principal::new("sec", vec!["security_admin".into()]);
    s.execute_ddl(&sec, "GRANT SELECT ON gold.a TO ROLE analyst")
        .await
        .unwrap();
    // String-typed policy on purpose (Parquet strings arrive as Utf8View; ADR-0006 finding 1).
    s.execute_ddl(
        &sec,
        "CREATE ROW POLICY fr ON gold.a FOR ROLE analyst USING (country = 'FR')",
    )
    .await
    .unwrap();
    s.execute_ddl(
        &sec,
        "CREATE MASKING POLICY m ON gold.a.balance FOR ROLE analyst AS null",
    )
    .await
    .unwrap();
    let analyst = Principal::new("ana", vec!["analyst".into()]);
    // Aggregates: the optimizer must not drop the filter below them.
    let r = s
        .execute_governed(&analyst, "SELECT COUNT(*), SUM(balance) FROM gold.a")
        .await
        .unwrap();
    assert_eq!(r.rows[0][0], json!(20));
    assert_eq!(
        r.rows[0][1],
        Value::Null,
        "masked column aggregates to null"
    );
    // Projection pruning must not remove the masking projection.
    let r = s
        .execute_governed(&analyst, "SELECT balance FROM gold.a WHERE id = 3")
        .await
        .unwrap();
    assert_eq!(r.rows, vec![vec![Value::Null]]);
    let plan = s
        .explain_governed(&analyst, "SELECT COUNT(*) FROM gold.a")
        .await
        .unwrap();
    assert!(plan.contains("FR"), "{plan}");
}

#[tokio::test]
async fn exe_vec_004_no_ungoverned_context() {
    // REQ: EXE-VEC-004 — the negative half is the crate-root `compile_fail` doctest
    // (`rusthouse::exec::VectorEngine` is not nameable). Here: every execution is audited.
    let dir = tempfile::tempdir().unwrap();
    let (s, audit) = session(dir.path()).await;
    seed(&s, "gold.a", 1, 5).await;
    s.execute_governed(&owner(), "SELECT id FROM gold.a")
        .await
        .unwrap();
    let recs = audit.records();
    assert!(recs
        .iter()
        .any(|r| r.operation == "QUERY" && r.status == Status::Intent));
    assert!(recs
        .iter()
        .any(|r| r.operation == "QUERY" && r.status == Status::Ok));
}

#[tokio::test]
async fn exe_vec_005_join_applies_both_sides_policies() {
    // REQ: EXE-VEC-005
    let dir = tempfile::tempdir().unwrap();
    let (s, audit) = session(dir.path()).await;
    seed(&s, "gold.a", 1, 6).await;
    s.execute_ddl(
        &owner(),
        "CREATE TABLE gold.names (id INT64, name STRING) WITH CLASSIFICATION (id Internal, name Confidential PII)",
    )
    .await
    .unwrap();
    s.write_rows(
        &owner(),
        "gold.names",
        (0..6)
            .map(|i| {
                json!({"id": i, "name": format!("n{i}")})
                    .as_object()
                    .unwrap()
                    .clone()
            })
            .collect(),
    )
    .await
    .unwrap();
    let sec = Principal::new("sec", vec!["security_admin".into()]);
    for ddl in [
        "GRANT SELECT ON gold.a TO ROLE analyst",
        "GRANT SELECT ON gold.names TO ROLE analyst",
        "CREATE ROW POLICY fr ON gold.a FOR ROLE analyst USING (country = 'FR')",
        "CREATE MASKING POLICY m ON gold.names.name FOR ROLE analyst AS redact",
    ] {
        s.execute_ddl(&sec, ddl).await.unwrap();
    }
    let analyst = Principal::new("ana", vec!["analyst".into()]);
    let r = s
        .execute_governed(
            &analyst,
            "SELECT a.id, n.name FROM gold.a a JOIN gold.names n ON a.id = n.id ORDER BY a.id",
        )
        .await
        .unwrap();
    assert_eq!(
        r.rows,
        vec![
            vec![json!(0), json!("[REDACTED]")],
            vec![json!(3), json!("[REDACTED]")]
        ]
    );
    assert_eq!(r.versions_read.len(), 2);
    let ok = audit
        .records()
        .into_iter()
        .rev()
        .find(|x| x.status == Status::Ok)
        .unwrap();
    assert_eq!(ok.versions_read.len(), 2);
    assert!(ok.columns.iter().any(|c| c.starts_with("gold.names.")));
}

#[tokio::test]
async fn exe_vec_006_provider_reads_only_snapshot_files() {
    // REQ: EXE-VEC-006
    let dir = tempfile::tempdir().unwrap();
    let (s, _) = session(dir.path()).await;
    seed(&s, "gold.a", 2, 10).await; // versions 1, 2
    let table = s.catalog().resolve("gold.a", None).unwrap();
    // A stray Parquet file that is NOT in any snapshot: copy an existing part under a new name.
    let existing = std::fs::read_dir(&table.path)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .find(|p| p.extension().map(|x| x == "parquet").unwrap_or(false))
        .unwrap();
    std::fs::copy(&existing, table.path.join("part-stray.parquet")).unwrap();
    let r = s
        .execute_governed(&owner(), "SELECT COUNT(*) FROM gold.a")
        .await
        .unwrap();
    assert_eq!(r.rows[0][0], json!(20), "stray file ignored");
    let r = s
        .execute_governed(&owner(), "SELECT COUNT(*) FROM gold.a AS OF VERSION 1")
        .await
        .unwrap();
    assert_eq!(r.rows[0][0], json!(10));
}

#[tokio::test]
async fn exe_vec_007_file_and_row_group_pruning() {
    // REQ: EXE-VEC-007
    let dir = tempfile::tempdir().unwrap();
    let (s, _) = session(dir.path()).await;
    seed(&s, "gold.a", 4, 100).await; // id ranges [0,99] [100,199] ...
    let plan = s
        .explain_governed(
            &owner(),
            "SELECT id FROM gold.a WHERE id BETWEEN 150 AND 160",
        )
        .await
        .unwrap();
    // File-level pruning by add-action statistics happens before planning: only one file is
    // handed to the scan.
    assert_eq!(
        plan.matches(".parquet").count(),
        1,
        "one file after stats pruning:\n{plan}"
    );
    assert!(
        plan.contains("predicate="),
        "row-group pruning predicate present:\n{plan}"
    );
    let r = s
        .execute_governed(
            &owner(),
            "SELECT COUNT(*) FROM gold.a WHERE id BETWEEN 150 AND 160",
        )
        .await
        .unwrap();
    assert_eq!(r.rows[0][0], json!(11));
}

#[tokio::test]
#[ignore = "performance budget; run with --release --ignored"]
async fn exe_vec_008_full_scan_budget() {
    // REQ: EXE-VEC-008
    let dir = tempfile::tempdir().unwrap();
    let (s, _) = session(dir.path()).await;
    seed(&s, "gold.big", 20, 500_000).await;
    let t = std::time::Instant::now();
    let r = s
        .execute_governed(&owner(), "SELECT COUNT(*), SUM(balance) FROM gold.big")
        .await
        .unwrap();
    let cold = t.elapsed();
    assert_eq!(r.rows[0][0], json!(10_000_000));
    let t = std::time::Instant::now();
    s.execute_governed(&owner(), "SELECT COUNT(*), SUM(balance) FROM gold.big")
        .await
        .unwrap();
    let warm = t.elapsed();
    assert!(cold.as_millis() < 2000, "cold {cold:?}");
    assert!(warm.as_millis() < 500, "warm {warm:?}");
}

#[tokio::test]
async fn exe_vec_009_audit_carries_plan_hash() {
    // REQ: EXE-VEC-009
    let dir = tempfile::tempdir().unwrap();
    let (s, audit) = session(dir.path()).await;
    seed(&s, "gold.a", 1, 5).await;
    let r = s
        .execute_governed(&owner(), "SELECT id FROM gold.a")
        .await
        .unwrap();
    assert_eq!(r.plan_hash.as_ref().map(|h| h.len()), Some(64));
    let ok = audit
        .records()
        .into_iter()
        .rev()
        .find(|x| x.status == Status::Ok)
        .unwrap();
    assert_eq!(ok.plan_hash, r.plan_hash);
    // Same governed plan, same hash; a different principal with a policy changes it.
    let r2 = s
        .execute_governed(&owner(), "SELECT id FROM gold.a")
        .await
        .unwrap();
    assert_eq!(r.plan_hash, r2.plan_hash);
}

#[tokio::test]
async fn exe_vec_errors_map_like_the_row_engine() {
    let dir = tempfile::tempdir().unwrap();
    let (s, _) = session(dir.path()).await;
    seed(&s, "gold.a", 1, 5).await;
    assert!(matches!(
        s.execute_governed(&owner(), "SELECT nope FROM gold.a")
            .await,
        Err(RustHouseError::ColumnNotFound(_))
    ));
    assert!(matches!(
        s.execute_governed(&owner(), "SELECT id FROM gold.missing")
            .await,
        Err(RustHouseError::TableNotFound(_))
    ));
    assert!(matches!(
        s.execute_governed(&owner(), "SELECT FROM").await,
        Err(RustHouseError::SqlParse(_))
    ));
}
