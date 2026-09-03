//! Acceptance tests for specs/features/GOV-ACC-access-control.md. Deny-first (Constitution 1).

use rusthouse::catalog::{Catalog, Principal};
use rusthouse::governance::audit::{Decision, MemoryAuditSink, Status};
use rusthouse::lake::log::{TransactionLog, Version};
use rusthouse::session::Session;
use rusthouse::RustHouseError;
use serde_json::{json, Value};
use std::sync::Arc;

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

fn owner() -> Principal {
    Principal::new("owner", vec![])
}
fn secadmin() -> Principal {
    Principal::new("secadmin", vec!["security_admin".into()])
}
fn analyst() -> Principal {
    Principal::new("alice", vec!["analyst".into()])
}
fn analyst_with(key: &str, value: &str) -> Principal {
    let mut p = analyst();
    p.attributes.insert(key.into(), value.into());
    p
}

/// gold.accounts: 3 FR rows, 2 DE rows, owned by `owner`.
async fn seed_accounts(s: &Session) {
    s.execute_ddl(
        &owner(),
        "CREATE TABLE gold.accounts (id INT64, country STRING, email STRING, balance FLOAT64) \
         WITH CLASSIFICATION (id Internal, country Internal, email Confidential PII, balance Confidential FINANCIAL)",
    )
    .await
    .unwrap();
    let rows: Vec<serde_json::Map<String, Value>> = [
        json!({"id": 1, "country": "FR", "email": "a@x", "balance": 10.0}),
        json!({"id": 2, "country": "FR", "email": "b@x", "balance": 20.0}),
        json!({"id": 3, "country": "FR", "email": "c@x", "balance": 30.0}),
        json!({"id": 4, "country": "DE", "email": "d@x", "balance": 40.0}),
        json!({"id": 5, "country": "DE", "email": "e@x", "balance": 50.0}),
    ]
    .iter()
    .map(|r| r.as_object().unwrap().clone())
    .collect();
    s.write_rows(&owner(), "gold.accounts", rows).await.unwrap();
}

async fn ddl(s: &Session, p: &Principal, sql: &str) {
    s.execute_ddl(p, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
}

// ---------------------------------------------------------------------------- deny-first

#[tokio::test]
async fn gov_acc_001_no_grant_is_denied_and_nothing_read() {
    // REQ: GOV-ACC-001
    let e = env().await;
    seed_accounts(&e.session).await;
    let intern = Principal::new("intern", vec![]);
    let before = e.session.executions();
    let err = e
        .session
        .execute_governed(&intern, "SELECT * FROM gold.accounts")
        .await
        .unwrap_err();
    assert!(matches!(err, RustHouseError::Denied(_)), "{err:?}");
    assert_eq!(
        e.session.executions(),
        before,
        "no execution, no file opened"
    );
    let rec = e
        .audit
        .records()
        .into_iter()
        .rev()
        .find(|r| r.principal == "intern")
        .expect("denial audited");
    assert_eq!(rec.decision, Decision::Deny);
    assert_eq!(rec.object, "gold.accounts");
}

#[tokio::test]
async fn gov_acc_002_missing_principal_denied_before_parse() {
    // REQ: GOV-ACC-002
    let e = env().await;
    let nobody = Principal::new("", vec![]);
    let err = e
        .session
        .execute_governed(&nobody, "this is not sql")
        .await
        .unwrap_err();
    assert!(
        matches!(err, RustHouseError::Denied(_)),
        "expected Denied, got {err:?}"
    );
}

#[tokio::test]
async fn gov_acc_004_unpermitted_column_is_denied() {
    // REQ: GOV-ACC-004
    let e = env().await;
    seed_accounts(&e.session).await;
    ddl(
        &e.session,
        &secadmin(),
        "GRANT SELECT(id, balance) ON gold.accounts TO ROLE analyst",
    )
    .await;
    let err = e
        .session
        .execute_governed(&analyst(), "SELECT email FROM gold.accounts")
        .await
        .unwrap_err();
    match err {
        RustHouseError::Denied(m) => assert!(m.contains("email"), "{m}"),
        other => panic!("{other:?}"),
    }
    // Filtering on a hidden column is a read too.
    let err = e
        .session
        .execute_governed(
            &analyst(),
            "SELECT id FROM gold.accounts WHERE email = 'a@x'",
        )
        .await
        .unwrap_err();
    assert!(matches!(err, RustHouseError::Denied(_)), "{err:?}");
}

#[tokio::test]
async fn gov_acc_006_missing_attribute_denies() {
    // REQ: GOV-ACC-006
    let e = env().await;
    seed_accounts(&e.session).await;
    ddl(
        &e.session,
        &secadmin(),
        "GRANT SELECT ON gold.accounts TO ROLE analyst",
    )
    .await;
    ddl(
        &e.session,
        &secadmin(),
        "CREATE ROW POLICY own_country ON gold.accounts FOR ROLE analyst USING (country = current_attribute('country'))",
    )
    .await;
    let err = e
        .session
        .execute_governed(&analyst(), "SELECT id FROM gold.accounts")
        .await
        .unwrap_err();
    match err {
        RustHouseError::Denied(m) => assert!(m.contains("missing_attribute:country"), "{m}"),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn gov_acc_003_grant_by_non_admin_is_denied() {
    // REQ: GOV-ACC-003
    let e = env().await;
    seed_accounts(&e.session).await;
    let v = e.session.catalog().version();
    let err = e
        .session
        .execute_ddl(&analyst(), "GRANT SELECT ON gold.accounts TO ROLE analyst")
        .await
        .unwrap_err();
    assert!(matches!(err, RustHouseError::Denied(_)), "{err:?}");
    // The owner is not a security admin either.
    let err = e
        .session
        .execute_ddl(&owner(), "GRANT SELECT ON gold.accounts TO ROLE analyst")
        .await
        .unwrap_err();
    assert!(matches!(err, RustHouseError::Denied(_)));
    assert_eq!(e.session.catalog().version(), v, "no catalog commit");
}

// ---------------------------------------------------------------------------- allow paths

#[tokio::test]
async fn gov_acc_003_grant_is_a_catalog_commit() {
    // REQ: GOV-ACC-003
    let e = env().await;
    seed_accounts(&e.session).await;
    let v = e.session.catalog().version();
    ddl(
        &e.session,
        &secadmin(),
        "GRANT SELECT ON gold.accounts TO ROLE analyst",
    )
    .await;
    assert_eq!(e.session.catalog().version(), v + 1);
    let log = TransactionLog::open(e._dir.path().join("catalog"))
        .await
        .unwrap();
    let ci = log
        .history(Version(v + 1), Version(v + 1))
        .await
        .unwrap()
        .remove(0);
    assert_eq!(ci.principal, "secadmin");
    assert_eq!(format!("{:?}", ci.operation), "Grant");
    // catalog.policies lists it.
    let r = e
        .session
        .execute_governed(&secadmin(), "SELECT * FROM catalog.policies")
        .await
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    // Revoke is a commit too and restores denial.
    ddl(
        &e.session,
        &secadmin(),
        "REVOKE SELECT ON gold.accounts FROM ROLE analyst",
    )
    .await;
    assert!(matches!(
        e.session
            .execute_governed(&analyst(), "SELECT id FROM gold.accounts")
            .await,
        Err(RustHouseError::Denied(_))
    ));
}

#[tokio::test]
async fn gov_acc_004_select_star_narrowed_to_granted_columns() {
    // REQ: GOV-ACC-004
    let e = env().await;
    seed_accounts(&e.session).await;
    ddl(
        &e.session,
        &secadmin(),
        "GRANT SELECT(id, balance) ON gold.accounts TO ROLE analyst",
    )
    .await;
    let r = e
        .session
        .execute_governed(&analyst(), "SELECT * FROM gold.accounts ORDER BY id")
        .await
        .unwrap();
    assert_eq!(r.columns, vec!["id", "balance"]);
    assert_eq!(r.rows.len(), 5);
    // Owner still sees everything.
    let r = e
        .session
        .execute_governed(&owner(), "SELECT * FROM gold.accounts")
        .await
        .unwrap();
    assert_eq!(r.columns, vec!["id", "country", "email", "balance"]);
}

#[tokio::test]
async fn gov_acc_005_row_policy_injects_filter() {
    // REQ: GOV-ACC-005
    let e = env().await;
    seed_accounts(&e.session).await;
    ddl(
        &e.session,
        &secadmin(),
        "GRANT SELECT ON gold.accounts TO ROLE analyst",
    )
    .await;
    ddl(
        &e.session,
        &secadmin(),
        "CREATE ROW POLICY fr_only ON gold.accounts FOR ROLE analyst USING (country = 'FR')",
    )
    .await;
    let r = e
        .session
        .execute_governed(&analyst(), "SELECT COUNT(*) AS n FROM gold.accounts")
        .await
        .unwrap();
    assert_eq!(r.rows[0][0], json!(3));
    let plan = e
        .session
        .explain_governed(&analyst(), "SELECT COUNT(*) FROM gold.accounts")
        .await
        .unwrap();
    assert!(
        plan.contains("country") && plan.contains("FR"),
        "governed plan carries the filter:\n{plan}"
    );
    // Two policies for two roles held by one principal are ANDed.
    ddl(
        &e.session,
        &secadmin(),
        "CREATE ROW POLICY small ON gold.accounts FOR ROLE junior USING (balance < 25)",
    )
    .await;
    let both = Principal::new("bob", vec!["analyst".into(), "junior".into()]);
    let r = e
        .session
        .execute_governed(&both, "SELECT id FROM gold.accounts ORDER BY id")
        .await
        .unwrap();
    assert_eq!(r.rows, vec![vec![json!(1)], vec![json!(2)]]);
    // Owner without the role is unfiltered.
    let r = e
        .session
        .execute_governed(&owner(), "SELECT COUNT(*) FROM gold.accounts")
        .await
        .unwrap();
    assert_eq!(r.rows[0][0], json!(5));
}

#[tokio::test]
async fn gov_acc_006_attribute_substitution_filters_rows() {
    // REQ: GOV-ACC-006
    let e = env().await;
    seed_accounts(&e.session).await;
    ddl(
        &e.session,
        &secadmin(),
        "GRANT SELECT ON gold.accounts TO ROLE analyst",
    )
    .await;
    ddl(
        &e.session,
        &secadmin(),
        "CREATE ROW POLICY own_country ON gold.accounts FOR ROLE analyst USING (country = current_attribute('country'))",
    )
    .await;
    let r = e
        .session
        .execute_governed(
            &analyst_with("country", "DE"),
            "SELECT id FROM gold.accounts ORDER BY id",
        )
        .await
        .unwrap();
    assert_eq!(r.rows, vec![vec![json!(4)], vec![json!(5)]]);
    // Attribute values are literals, never SQL: an injection attempt matches nothing.
    let r = e
        .session
        .execute_governed(
            &analyst_with("country", "DE' OR '1'='1"),
            "SELECT id FROM gold.accounts",
        )
        .await
        .unwrap();
    assert!(r.rows.is_empty());
}

#[tokio::test]
async fn gov_acc_007_masking_partial_last4_and_unmask_grant() {
    // REQ: GOV-ACC-007
    let e = env().await;
    e.session
        .execute_ddl(
            &owner(),
            "CREATE TABLE gold.payments (id INT64, pan STRING, amount FLOAT64) \
             WITH CLASSIFICATION (id Internal, pan Restricted PCI, amount Confidential FINANCIAL)",
        )
        .await
        .unwrap();
    e.session
        .write_rows(
            &owner(),
            "gold.payments",
            vec![json!({"id": 1, "pan": "4111111111111111", "amount": 9.5})
                .as_object()
                .unwrap()
                .clone()],
        )
        .await
        .unwrap();
    ddl(
        &e.session,
        &secadmin(),
        "GRANT SELECT ON gold.payments TO ROLE analyst",
    )
    .await;
    ddl(
        &e.session,
        &secadmin(),
        "GRANT SELECT ON gold.payments TO ROLE fraud_lead",
    )
    .await;
    ddl(
        &e.session,
        &secadmin(),
        "GRANT UNMASK(pan) ON gold.payments TO ROLE fraud_lead",
    )
    .await;
    for (role, kind, expected) in [
        ("analyst", "partial(last4)", json!("************1111")),
        ("analyst", "redact", json!("[REDACTED]")),
        ("analyst", "null", Value::Null),
    ] {
        ddl(
            &e.session,
            &secadmin(),
            &format!(
                "CREATE MASKING POLICY pan_mask ON gold.payments.pan FOR ROLE {role} AS {kind}"
            ),
        )
        .await;
        let r = e
            .session
            .execute_governed(&analyst(), "SELECT pan, amount FROM gold.payments")
            .await
            .unwrap();
        assert_eq!(r.rows[0][0], expected, "{kind}");
        assert_eq!(r.rows[0][1], json!(9.5), "unmasked columns untouched");
        // Masked columns are still filterable, over the masked value (no oracle attack).
        let r = e
            .session
            .execute_governed(
                &analyst(),
                "SELECT id FROM gold.payments WHERE pan = '4111111111111111'",
            )
            .await
            .unwrap();
        assert!(
            r.rows.is_empty(),
            "{kind}: filter must see the masked value"
        );
        ddl(
            &e.session,
            &secadmin(),
            "DROP POLICY pan_mask ON gold.payments",
        )
        .await;
    }
    ddl(
        &e.session,
        &secadmin(),
        "CREATE MASKING POLICY pan_hash ON gold.payments.pan FOR ROLE analyst AS hash",
    )
    .await;
    let r = e
        .session
        .execute_governed(&analyst(), "SELECT pan FROM gold.payments")
        .await
        .unwrap();
    let hashed = r.rows[0][0].as_str().unwrap();
    assert_eq!(hashed.len(), 64, "sha-256 hex, type stays string");
    assert_ne!(hashed, "4111111111111111");
    let fraud = Principal::new("fl", vec!["fraud_lead".into()]);
    let r = e
        .session
        .execute_governed(&fraud, "SELECT pan FROM gold.payments")
        .await
        .unwrap();
    assert_eq!(r.rows[0][0], json!("4111111111111111"));
    // Masking is audited as decision "mask" naming the policy.
    let rec = e
        .audit
        .records()
        .into_iter()
        .rev()
        .find(|r| r.principal == "alice" && r.status == Status::Ok)
        .unwrap();
    assert_eq!(rec.decision, Decision::Mask);
    assert!(rec.operation.contains("pan_hash"), "{}", rec.operation);
}

#[tokio::test]
async fn gov_acc_008_policies_are_time_travelable() {
    // REQ: GOV-ACC-008
    let e = env().await;
    seed_accounts(&e.session).await;
    ddl(
        &e.session,
        &secadmin(),
        "GRANT SELECT ON gold.accounts TO ROLE analyst",
    )
    .await;
    let v_before = e.session.catalog().version();
    ddl(
        &e.session,
        &secadmin(),
        "CREATE ROW POLICY fr_only ON gold.accounts FOR ROLE analyst USING (country = 'FR')",
    )
    .await;
    let v_created = e.session.catalog().version();
    ddl(
        &e.session,
        &secadmin(),
        "DROP POLICY fr_only ON gold.accounts",
    )
    .await;
    let v_dropped = e.session.catalog().version();

    let at = |v: u64| format!("SHOW POLICIES ON gold.accounts AS OF VERSION {v}");
    let listed = e
        .session
        .execute_ddl(&secadmin(), &at(v_created))
        .await
        .unwrap();
    assert!(
        listed.rows.iter().any(|r| r["name"] == "fr_only"),
        "{:?}",
        listed.rows
    );
    let before = e
        .session
        .execute_ddl(&secadmin(), &at(v_before))
        .await
        .unwrap();
    assert!(!before.rows.iter().any(|r| r["name"] == "fr_only"));
    let after = e
        .session
        .execute_ddl(&secadmin(), &at(v_dropped))
        .await
        .unwrap();
    assert!(!after.rows.iter().any(|r| r["name"] == "fr_only"));
    let now = e
        .session
        .execute_ddl(&secadmin(), "SHOW POLICIES ON gold.accounts")
        .await
        .unwrap();
    assert_eq!(now.rows.len(), 1, "only the grant remains: {:?}", now.rows);
}

#[tokio::test]
async fn gov_acc_009_decisions_audited_and_counted() {
    // REQ: GOV-ACC-009
    let e = env().await;
    seed_accounts(&e.session).await;
    ddl(
        &e.session,
        &secadmin(),
        "GRANT SELECT ON gold.accounts TO ROLE analyst",
    )
    .await;
    ddl(
        &e.session,
        &secadmin(),
        "CREATE MASKING POLICY m ON gold.accounts.email FOR ROLE analyst AS redact",
    )
    .await;
    let denied = Principal::new("nobody", vec![]);
    let _ = e
        .session
        .execute_governed(&denied, "SELECT id FROM gold.accounts")
        .await;
    e.session
        .execute_governed(&owner(), "SELECT id FROM gold.accounts")
        .await
        .unwrap();
    e.session
        .execute_governed(&analyst(), "SELECT email FROM gold.accounts")
        .await
        .unwrap();
    let recs = e.audit.records();
    let last_for = |p: &str| {
        recs.iter()
            .rev()
            .find(|r| r.principal == p)
            .unwrap()
            .clone()
    };
    assert_eq!(last_for("nobody").decision, Decision::Deny);
    assert_eq!(last_for("owner").decision, Decision::Allow);
    let masked = last_for("alice");
    assert_eq!(masked.decision, Decision::Mask);
    assert!(
        masked.operation.contains("m"),
        "applied policy ids in record: {}",
        masked.operation
    );
    let metrics = rusthouse::metrics::render();
    assert!(
        metrics
            .lines()
            .any(|l| l.starts_with("rusthouse_governor_decisions_total{decision=\"deny\"}")),
        "{metrics}"
    );
}

#[tokio::test]
#[ignore = "performance budget; run with --release --ignored"]
async fn gov_acc_010_governor_p99_under_1ms_100_policies() {
    // REQ: GOV-ACC-010
    let e = env().await;
    seed_accounts(&e.session).await;
    ddl(
        &e.session,
        &secadmin(),
        "GRANT SELECT ON gold.accounts TO ROLE analyst",
    )
    .await;
    for i in 0..100 {
        ddl(
            &e.session,
            &secadmin(),
            &format!(
                "CREATE ROW POLICY p{i} ON gold.accounts FOR ROLE analyst USING (balance >= {i})"
            ),
        )
        .await;
    }
    let mut lat = Vec::new();
    for _ in 0..10_000 {
        let t = std::time::Instant::now();
        e.session.decide_read(&analyst(), "gold.accounts").unwrap();
        lat.push(t.elapsed());
    }
    lat.sort();
    let p99 = lat[lat.len() * 99 / 100];
    assert!(p99.as_micros() < 1000, "p99 {p99:?}");
}

#[tokio::test]
async fn gov_acc_011_injected_filter_prunes_both_tiers() {
    // REQ: GOV-ACC-011 (lake tier; the hot tier is re-scoped by ADR-0006)
    let e = env().await;
    seed_accounts(&e.session).await;
    ddl(
        &e.session,
        &secadmin(),
        "GRANT SELECT ON gold.accounts TO ROLE analyst",
    )
    .await;
    ddl(
        &e.session,
        &secadmin(),
        "CREATE ROW POLICY fr_only ON gold.accounts FOR ROLE analyst USING (country = 'FR')",
    )
    .await;
    let plan = e
        .session
        .explain_governed(&analyst(), "SELECT id FROM gold.accounts")
        .await
        .unwrap();
    // The injected predicate is pushed into the Parquet scan, where row-group pruning happens.
    assert!(
        plan.contains("DataSourceExec") && plan.contains("predicate=") && plan.contains("FR"),
        "filter must reach the scan for pruning:\n{plan}"
    );
    let r = e
        .session
        .execute_governed(&analyst(), "SELECT COUNT(*) FROM gold.accounts")
        .await
        .unwrap();
    assert_eq!(r.rows[0][0], json!(3));
}

#[tokio::test]
async fn gov_acc_write_requires_insert_grant_or_ownership() {
    // Companion to GOV-ACC-001: deny-by-default applies to writes as well.
    let e = env().await;
    seed_accounts(&e.session).await;
    let row = vec![
        json!({"id": 9, "country": "FR", "email": "z", "balance": 1.0})
            .as_object()
            .unwrap()
            .clone(),
    ];
    let err = e
        .session
        .write_rows(&analyst(), "gold.accounts", row.clone())
        .await
        .unwrap_err();
    assert!(matches!(err, RustHouseError::Denied(_)), "{err:?}");
    ddl(
        &e.session,
        &secadmin(),
        "GRANT INSERT ON gold.accounts TO ROLE analyst",
    )
    .await;
    e.session
        .write_rows(&analyst(), "gold.accounts", row)
        .await
        .unwrap();
}
