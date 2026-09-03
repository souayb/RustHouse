//! Acceptance tests for specs/features/LAKE-CAT-catalog.md

use rusthouse::catalog::{Catalog, FaultPoint, Principal, TableSpec};
use rusthouse::governance::classification::{Classification, Level, Tag};
use rusthouse::lake::log::{TransactionLog, Version};
use rusthouse::lake::schema::{Field, Schema};
use rusthouse::meta::ColumnType;
use rusthouse::RustHouseError;
use std::path::Path;

fn principal(id: &str) -> Principal {
    Principal::new(id, vec![])
}

fn classified_schema() -> Schema {
    Schema::new(vec![
        Field::new(
            "account_id",
            ColumnType::String,
            Classification::new(Level::Restricted).with_tags([Tag::Pii]),
        ),
        Field::new(
            "notional",
            ColumnType::Float64,
            Classification::new(Level::Confidential),
        ),
    ])
}

fn spec(ns: &str, name: &str) -> TableSpec {
    TableSpec::new(ns, name, classified_schema())
}

async fn catalog(dir: &Path) -> Catalog {
    Catalog::open(dir).await.unwrap()
}

#[tokio::test]
async fn lake_cat_001_builtin_namespaces_exist() {
    // REQ: LAKE-CAT-001
    let dir = tempfile::tempdir().unwrap();
    let cat = catalog(dir.path()).await;
    assert_eq!(cat.list_namespaces(), vec!["bronze", "silver", "gold"]);
    assert_eq!(cat.version(), 0);
}

#[tokio::test]
async fn lake_cat_002_custom_namespace_and_duplicate_rejected() {
    // REQ: LAKE-CAT-002
    let dir = tempfile::tempdir().unwrap();
    let cat = catalog(dir.path()).await;
    let v = cat
        .create_namespace(&principal("dba"), "risk")
        .await
        .unwrap();
    assert_eq!(v, 1);
    assert!(cat.list_namespaces().contains(&"risk".to_string()));
    let err = cat
        .create_namespace(&principal("dba"), "risk")
        .await
        .unwrap_err();
    assert!(matches!(err, RustHouseError::NamespaceExists(_)), "{err:?}");
    assert_eq!(cat.version(), 1);
    let err = cat
        .create_namespace(&principal("dba"), "gold")
        .await
        .unwrap_err();
    assert!(matches!(err, RustHouseError::NamespaceExists(_)));
    for bad in ["Risk", "1abc", "a-b", "", &"x".repeat(64)] {
        let err = cat
            .create_namespace(&principal("dba"), bad)
            .await
            .unwrap_err();
        assert!(
            matches!(err, RustHouseError::InvalidIdentifier(_)),
            "{bad}: {err:?}"
        );
    }
}

#[tokio::test]
async fn lake_cat_003_create_table_is_atomic_under_fault() {
    // REQ: LAKE-CAT-003
    let dir = tempfile::tempdir().unwrap();
    let cat = catalog(dir.path()).await;
    cat.inject_fault(Some(FaultPoint::BeforeCatalogCommit));
    let err = cat
        .create_table(&principal("alice"), spec("gold", "positions"))
        .await
        .unwrap_err();
    assert!(matches!(err, RustHouseError::Internal(_)), "{err:?}");
    cat.inject_fault(None);

    assert!(matches!(
        cat.resolve("gold.positions", None).unwrap_err(),
        RustHouseError::TableNotFound(_)
    ));
    let final_log = dir
        .path()
        .join("gold/positions/_rhlog/00000000000000000000.json");
    assert!(!final_log.exists(), "no published table log may exist");
    assert!(
        !dir.path().join("gold").exists()
            || std::fs::read_dir(dir.path().join("gold")).unwrap().count() == 0,
        "no staging leftovers"
    );
    assert_eq!(cat.version(), 0);

    // Fault after the catalog commit but before publishing the table directory: the reopen
    // must repair it (the catalog entry is authoritative).
    cat.inject_fault(Some(FaultPoint::AfterCatalogCommit));
    let _ = cat
        .create_table(&principal("alice"), spec("gold", "positions"))
        .await;
    cat.inject_fault(None);
    let reopened = Catalog::open(dir.path()).await.unwrap();
    let r = reopened.resolve("gold.positions", None).unwrap();
    assert!(
        r.path.join("_rhlog/00000000000000000000.json").exists(),
        "repaired on open"
    );
}

#[tokio::test]
async fn lake_cat_004_resolve_existing_and_missing() {
    // REQ: LAKE-CAT-004
    let dir = tempfile::tempdir().unwrap();
    let cat = catalog(dir.path()).await;
    let created = cat
        .create_table(&principal("alice"), spec("gold", "positions"))
        .await
        .unwrap();
    let v = cat.version();
    let r = cat.resolve("gold.positions", None).unwrap();
    assert_eq!(r.namespace, "gold");
    assert_eq!(r.name, "positions");
    assert_eq!(r.table_id, created.table_id);
    assert_eq!(r.path, dir.path().join("gold").join("positions"));
    let err = cat.resolve("gold.missing", None).unwrap_err();
    assert!(matches!(err, RustHouseError::TableNotFound(_)), "{err:?}");
    let err = cat.resolve("nope.positions", None).unwrap_err();
    assert!(
        matches!(err, RustHouseError::NamespaceNotFound(_)),
        "{err:?}"
    );
    assert_eq!(cat.version(), v, "resolve never mutates");
}

#[tokio::test]
async fn lake_cat_005_bare_name_ambiguous_without_default() {
    // REQ: LAKE-CAT-005
    let dir = tempfile::tempdir().unwrap();
    let cat = catalog(dir.path()).await;
    cat.create_table(&principal("a"), spec("bronze", "trades"))
        .await
        .unwrap();
    cat.create_table(&principal("a"), spec("silver", "trades"))
        .await
        .unwrap();
    let err = cat.resolve("trades", None).unwrap_err();
    assert!(matches!(err, RustHouseError::AmbiguousTable(_)), "{err:?}");
    let r = cat.resolve("trades", Some("silver")).unwrap();
    assert_eq!(r.namespace, "silver");
    let err = cat.resolve("a.b.c", None).unwrap_err();
    assert!(matches!(err, RustHouseError::InvalidIdentifier(_)));
}

#[tokio::test]
async fn lake_cat_006_catalog_commit_carries_principal() {
    // REQ: LAKE-CAT-006
    let dir = tempfile::tempdir().unwrap();
    let cat = catalog(dir.path()).await;
    cat.create_table(&principal("alice"), spec("gold", "positions"))
        .await
        .unwrap();
    let file = dir.path().join("catalog/_rhlog/00000000000000000001.json");
    let text = std::fs::read_to_string(file).unwrap();
    let ci = text
        .lines()
        .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap())
        .find(|v| v["action"] == "commitInfo")
        .unwrap();
    assert_eq!(ci["principal"], "alice");
    assert_eq!(ci["operation"], "CREATE_TABLE");
    let err = cat
        .create_table(&principal(""), spec("gold", "other"))
        .await
        .unwrap_err();
    assert!(matches!(err, RustHouseError::MissingPrincipal));
}

#[tokio::test]
async fn lake_cat_007_concurrent_creates_bump_version_atomically() {
    // REQ: LAKE-CAT-007
    let dir = tempfile::tempdir().unwrap();
    let cat = std::sync::Arc::new(catalog(dir.path()).await);
    for i in 0..5 {
        cat.create_table(&principal("a"), spec("bronze", &format!("seed{i}")))
            .await
            .unwrap();
    }
    assert_eq!(cat.version(), 5);
    let mut handles = Vec::new();
    for i in 0..8 {
        let c = cat.clone();
        handles.push(tokio::spawn(async move {
            c.create_table(&principal("a"), spec("gold", &format!("t{i}")))
                .await
                .unwrap()
        }));
    }
    // Concurrent readers: each snapshot must be internally consistent.
    let reader = {
        let c = cat.clone();
        tokio::spawn(async move {
            for _ in 0..50 {
                let snap = c.snapshot();
                assert_eq!(
                    snap.tables.len() as u64,
                    snap.version,
                    "every version so far added exactly one table"
                );
                tokio::task::yield_now().await;
            }
        })
    };
    for h in handles {
        h.await.unwrap();
    }
    reader.await.unwrap();
    assert_eq!(cat.version(), 13);
    assert_eq!(cat.list_tables("gold").unwrap().len(), 8);
}

#[tokio::test]
async fn lake_cat_008_drop_is_logical_and_time_travelable() {
    // REQ: LAKE-CAT-008
    let dir = tempfile::tempdir().unwrap();
    let cat = catalog(dir.path()).await;
    cat.create_namespace(&principal("a"), "x1").await.unwrap();
    cat.create_namespace(&principal("a"), "x2").await.unwrap();
    let r = cat
        .create_table(&principal("a"), spec("gold", "positions"))
        .await
        .unwrap();
    assert_eq!(cat.version(), 3);
    let data_file = r.path.join("part-000.parquet");
    std::fs::write(&data_file, b"not really parquet").unwrap();

    let v = cat.drop_table(&principal("a"), &r).await.unwrap();
    assert_eq!(v, 4);
    assert!(matches!(
        cat.resolve("gold.positions", None).unwrap_err(),
        RustHouseError::TableNotFound(_)
    ));
    assert!(data_file.exists(), "drop must not delete data files");
    let old = cat.resolve_as_of("gold.positions", None, 3).await.unwrap();
    assert_eq!(old.table_id, r.table_id);
    assert!(cat.resolve_as_of("gold.positions", None, 4).await.is_err());
}

#[tokio::test]
async fn lake_cat_009_describe_returns_ids_and_classifications() {
    // REQ: LAKE-CAT-009
    let dir = tempfile::tempdir().unwrap();
    let cat = catalog(dir.path()).await;
    let r = cat
        .create_table(&principal("alice"), spec("gold", "positions"))
        .await
        .unwrap();
    let d = cat.describe(&r).await.unwrap();
    assert_eq!(d.table_id, r.table_id);
    assert_eq!(d.owner, "alice");
    assert_eq!(d.current_version, Version(0));
    assert!(d.created_at > 0);
    assert_eq!(d.schema.fields.len(), 2);
    let acct = d.schema.field("account_id").unwrap();
    assert_eq!(acct.id, 1);
    assert_eq!(acct.column_type, ColumnType::String);
    assert_eq!(acct.classification().unwrap().level, Level::Restricted);
    assert!(acct.classification().unwrap().tags.contains(&Tag::Pii));
    assert_eq!(d.classification.level, Level::Restricted);

    let summaries = cat.list_tables("gold").unwrap();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].name, "positions");
    assert_eq!(summaries[0].owner, "alice");
}

#[tokio::test]
async fn lake_cat_010_system_tables_queryable() {
    // REQ: LAKE-CAT-010
    let dir = tempfile::tempdir().unwrap();
    let cat = catalog(dir.path()).await;
    cat.create_table(&principal("alice"), spec("gold", "positions"))
        .await
        .unwrap();
    cat.create_table(&principal("bob"), spec("silver", "trades"))
        .await
        .unwrap();

    let tables = cat.system_table("catalog.tables").unwrap();
    assert_eq!(tables.num_rows(), 2);
    let names: Vec<&str> = tables.columns.iter().map(|c| c.name.as_str()).collect();
    for expected in ["namespace", "name", "table_id", "classification", "owner"] {
        assert!(
            names.contains(&expected),
            "missing column {expected} in {names:?}"
        );
    }
    let columns = cat.system_table("catalog.columns").unwrap();
    assert_eq!(columns.num_rows(), 4);
    for t in ["catalog.grants", "catalog.policies", "catalog.lineage"] {
        let rows = cat.system_table(t).unwrap();
        assert_eq!(rows.num_rows(), 0, "{t} resolves (empty)");
    }
    assert!(matches!(
        cat.system_table("catalog.nope").unwrap_err(),
        RustHouseError::TableNotFound(_)
    ));
}

#[tokio::test]
async fn lake_cat_011_missing_classification_refused() {
    // REQ: LAKE-CAT-011
    let dir = tempfile::tempdir().unwrap();
    let cat = catalog(dir.path()).await;
    let raw = serde_json::json!({
        "fields": [
            {"id": 1, "name": "id", "column_type": "Int64", "nullable": false,
             "classification": {"level": "Internal", "tags": []}},
            {"id": 2, "name": "email", "column_type": "String", "nullable": false}
        ],
        "next_column_id": 3
    });
    let schema: Schema = serde_json::from_value(raw).unwrap();
    let err = cat
        .create_table(
            &principal("a"),
            TableSpec::new("silver", "customers", schema),
        )
        .await
        .unwrap_err();
    match err {
        RustHouseError::MissingClassification(m) => assert!(m.contains("email"), "{m}"),
        other => panic!("{other:?}"),
    }
    assert_eq!(cat.version(), 0);
    assert!(!dir.path().join("silver/customers").exists());
}

#[tokio::test]
async fn lake_cat_012_reopen_rebuilds_from_log_only() {
    // REQ: LAKE-CAT-012
    let dir = tempfile::tempdir().unwrap();
    let ids = {
        let cat = catalog(dir.path()).await;
        cat.create_namespace(&principal("a"), "risk").await.unwrap();
        let a = cat
            .create_table(&principal("a"), spec("gold", "positions"))
            .await
            .unwrap();
        let b = cat
            .create_table(&principal("a"), spec("risk", "var"))
            .await
            .unwrap();
        let c = cat
            .create_table(&principal("a"), spec("risk", "es"))
            .await
            .unwrap();
        vec![a, b, c]
    };
    // Delete everything outside catalog/_rhlog: the inventory must not change.
    std::fs::remove_dir_all(dir.path().join("gold")).unwrap();
    std::fs::write(dir.path().join("stray.json"), b"{}").unwrap();

    let cat = Catalog::open(dir.path()).await.unwrap();
    assert_eq!(cat.version(), 4);
    for r in &ids {
        let got = cat
            .resolve(&format!("{}.{}", r.namespace, r.name), None)
            .unwrap();
        assert_eq!(got.table_id, r.table_id);
    }
    assert!(cat.list_namespaces().contains(&"risk".to_string()));
    // The table log of the deleted directory is gone, but the catalog still knows the table.
    let err = TransactionLog::open(&ids[0].path).await.unwrap_err();
    assert!(matches!(err, RustHouseError::TableNotFound(_)));
}
