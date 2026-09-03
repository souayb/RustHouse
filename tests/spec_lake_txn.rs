//! Acceptance tests for specs/features/LAKE-TXN-transaction-log.md

use rusthouse::governance::classification::{Classification, Level};
use rusthouse::lake::log::{
    Action, AddFile, Commit, CommitInfo, LogOptions, MetaData, Operation, TransactionLog, Version,
};
use rusthouse::lake::schema::{Field, Schema};
use rusthouse::lake::stats::FileStats;
use rusthouse::meta::ColumnType;
use rusthouse::RustHouseError;
use std::path::Path;
use std::time::Instant;

fn schema() -> Schema {
    Schema::new(vec![Field::new(
        "id",
        ColumnType::Int64,
        Classification::new(Level::Internal),
    )])
}

fn metadata() -> MetaData {
    MetaData::new(schema(), vec![])
}

fn add(path: &str) -> Action {
    Action::Add(AddFile::new(path, 10, 1, FileStats::empty()))
}

fn write_commit(principal: &str, actions: Vec<Action>) -> Commit {
    Commit::new(
        CommitInfo::new(
            principal,
            Operation::Write,
            serde_json::json!({"mode": "append"}),
        ),
        actions,
    )
}

async fn new_log(dir: &Path) -> TransactionLog {
    TransactionLog::create(dir, "svc-ingest", metadata())
        .await
        .unwrap()
}

fn rhlog_files(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(dir.join("_rhlog"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    v.sort();
    v
}

#[tokio::test]
async fn lake_txn_001_commit_files_are_zero_padded_versions() {
    // REQ: LAKE-TXN-001
    let dir = tempfile::tempdir().unwrap();
    let log = new_log(dir.path()).await;
    log.commit(write_commit("a", vec![add("part-1.parquet")]))
        .await
        .unwrap();
    log.commit(write_commit("a", vec![add("part-2.parquet")]))
        .await
        .unwrap();
    let files: Vec<String> = rhlog_files(dir.path())
        .into_iter()
        .filter(|f| f.ends_with(".json"))
        .collect();
    assert_eq!(
        files,
        vec![
            "00000000000000000000.json",
            "00000000000000000001.json",
            "00000000000000000002.json"
        ]
    );
    assert_eq!(log.head().await.unwrap(), Version(2));
}

#[tokio::test]
async fn lake_txn_002_unknown_action_is_corrupt_log() {
    // REQ: LAKE-TXN-002
    let dir = tempfile::tempdir().unwrap();
    let log = new_log(dir.path()).await;
    log.commit(write_commit("a", vec![add("part-1.parquet")]))
        .await
        .unwrap();
    let p = dir.path().join("_rhlog").join("00000000000000000001.json");
    let mut s = std::fs::read_to_string(&p).unwrap();
    s.push_str("{\"action\":\"teleport\"}\n");
    std::fs::write(&p, s).unwrap();

    let reopened = TransactionLog::open(dir.path()).await.unwrap();
    let err = reopened.snapshot(None).await.unwrap_err();
    assert!(matches!(err, RustHouseError::CorruptLog(_)), "{err:?}");
}

#[tokio::test]
async fn lake_txn_003_version_zero_has_protocol_metadata_commitinfo() {
    // REQ: LAKE-TXN-003
    let dir = tempfile::tempdir().unwrap();
    let _log = new_log(dir.path()).await;
    let text =
        std::fs::read_to_string(dir.path().join("_rhlog/00000000000000000000.json")).unwrap();
    let kinds: Vec<String> = text
        .lines()
        .map(|l| {
            serde_json::from_str::<serde_json::Value>(l).unwrap()["action"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    assert_eq!(kinds.iter().filter(|k| *k == "protocol").count(), 1);
    assert_eq!(kinds.iter().filter(|k| *k == "metaData").count(), 1);
    assert_eq!(kinds.iter().filter(|k| *k == "commitInfo").count(), 1);
    assert_eq!(kinds.len(), 3);
}

#[tokio::test]
async fn lake_txn_004_commit_uses_tmp_then_rename_only() {
    // REQ: LAKE-TXN-004
    let dir = tempfile::tempdir().unwrap();
    let log = new_log(dir.path()).await;
    for i in 0..3 {
        log.commit(write_commit("a", vec![add(&format!("p{i}.parquet"))]))
            .await
            .unwrap();
    }
    let v3 = dir.path().join("_rhlog/00000000000000000003.json");
    let before = std::fs::read(&v3).unwrap();
    log.commit(write_commit("a", vec![add("p4.parquet")]))
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(&v3).unwrap(),
        before,
        "existing commit file must not change"
    );
    let files = rhlog_files(dir.path());
    assert!(
        files.iter().all(|f| !f.ends_with(".tmp")),
        "no tmp left: {files:?}"
    );
    assert!(files.contains(&"00000000000000000004.json".to_string()));
    // The final file is complete JSON lines (never a partially written final name).
    let text =
        std::fs::read_to_string(dir.path().join("_rhlog/00000000000000000004.json")).unwrap();
    for line in text.lines() {
        serde_json::from_str::<serde_json::Value>(line).unwrap();
    }
}

#[tokio::test]
async fn lake_txn_005_missing_principal_rejected_before_write() {
    // REQ: LAKE-TXN-005
    let dir = tempfile::tempdir().unwrap();
    let log = new_log(dir.path()).await;
    let before = rhlog_files(dir.path());
    let err = log
        .commit(write_commit("", vec![add("p.parquet")]))
        .await
        .unwrap_err();
    assert!(matches!(err, RustHouseError::MissingPrincipal), "{err:?}");
    assert_eq!(
        rhlog_files(dir.path()),
        before,
        "no file, including tmp, may be written"
    );
    // Creating a table without a principal is refused too.
    let dir2 = tempfile::tempdir().unwrap();
    let err = TransactionLog::create(dir2.path(), "", metadata())
        .await
        .unwrap_err();
    assert!(matches!(err, RustHouseError::MissingPrincipal));
}

#[tokio::test]
async fn lake_txn_006_stale_tmp_ignored_on_replay() {
    // REQ: LAKE-TXN-006
    let dir = tempfile::tempdir().unwrap();
    let log = new_log(dir.path()).await;
    log.commit(write_commit("a", vec![add("p1.parquet")]))
        .await
        .unwrap();
    log.commit(write_commit("a", vec![add("p2.parquet")]))
        .await
        .unwrap();
    std::fs::write(
        dir.path().join("_rhlog/00000000000000000003.json.tmp"),
        "{\"action\":\"add\",\"pa",
    )
    .unwrap();

    let reopened = TransactionLog::open(dir.path()).await.unwrap();
    let snap = reopened.snapshot(None).await.unwrap();
    assert_eq!(snap.version, Version(2));

    let v = reopened
        .commit(write_commit("a", vec![add("p3.parquet")]))
        .await
        .unwrap();
    assert_eq!(v, Version(3));
    let files = rhlog_files(dir.path());
    assert!(
        files.iter().all(|f| !f.ends_with(".tmp")),
        "stale tmp must be gone: {files:?}"
    );
}

#[tokio::test]
async fn lake_txn_007_checkpoint_written_every_n_commits() {
    // REQ: LAKE-TXN-007
    let dir = tempfile::tempdir().unwrap();
    let opts = LogOptions {
        checkpoint_interval: 3,
        ..LogOptions::default()
    };
    let log = TransactionLog::create_with(dir.path(), opts.clone(), "a", metadata())
        .await
        .unwrap();
    for i in 1..=6 {
        log.commit(write_commit("a", vec![add(&format!("p{i}.parquet"))]))
            .await
            .unwrap();
    }
    let files = rhlog_files(dir.path());
    assert!(
        files.contains(&"00000000000000000003.checkpoint.json".to_string()),
        "{files:?}"
    );
    assert!(
        files.contains(&"00000000000000000006.checkpoint.json".to_string()),
        "{files:?}"
    );
    let last: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(dir.path().join("_rhlog/_last_checkpoint")).unwrap(),
    )
    .unwrap();
    assert_eq!(last["version"], 6);
    // Replay from checkpoint gives the same snapshot as a full replay.
    let from_ckpt = TransactionLog::open_with(dir.path(), opts)
        .await
        .unwrap()
        .snapshot(None)
        .await
        .unwrap();
    assert_eq!(from_ckpt.version, Version(6));
    assert_eq!(from_ckpt.files.len(), 6);
}

#[tokio::test]
async fn lake_txn_008_concurrent_writers_retry_then_conflict() {
    // REQ: LAKE-TXN-008
    let dir = tempfile::tempdir().unwrap();
    let log = new_log(dir.path()).await;
    for i in 1..=7 {
        log.commit(write_commit("a", vec![add(&format!("p{i}.parquet"))]))
            .await
            .unwrap();
    }
    // Two writers on the same head.
    let w1 = TransactionLog::open(dir.path()).await.unwrap();
    let w2 = TransactionLog::open(dir.path()).await.unwrap();
    assert_eq!(w1.head().await.unwrap(), Version(7));
    assert_eq!(w2.head().await.unwrap(), Version(7));
    let (r1, r2) = tokio::join!(
        w1.commit(write_commit("w1", vec![add("w1.parquet")])),
        w2.commit(write_commit("w2", vec![add("w2.parquet")]))
    );
    let mut versions = vec![r1.unwrap(), r2.unwrap()];
    versions.sort();
    assert_eq!(versions, vec![Version(8), Version(9)]);
    let snap = log.snapshot(Some(Version(9))).await.unwrap();
    let paths: Vec<&str> = snap.files.iter().map(|f| f.path.as_str()).collect();
    assert!(paths.contains(&"w1.parquet") && paths.contains(&"w2.parquet"));

    // Retries exhausted: with a zero retry budget a single collision is fatal, which is the
    // same path a writer takes when a competitor wins every one of its `max_retries` attempts.
    let opts = LogOptions {
        max_retries: 0,
        ..LogOptions::default()
    };
    let loser = TransactionLog::open_with(dir.path(), opts).await.unwrap();
    let competitor = TransactionLog::open(dir.path()).await.unwrap();
    competitor
        .commit(write_commit("c", vec![add("c0.parquet")]))
        .await
        .unwrap(); // takes v10
    let err = loser
        .commit_at(Version(10), write_commit("loser", vec![add("l.parquet")]))
        .await
        .unwrap_err();
    match err {
        RustHouseError::CommitConflict {
            expected, found, ..
        } => {
            assert_eq!(expected, 10);
            assert_eq!(found, 10);
        }
        other => panic!("expected CommitConflict, got {other:?}"),
    }
    assert_eq!(
        log.head().await.unwrap(),
        Version(10),
        "loser must not have committed"
    );
}

#[tokio::test]
async fn lake_txn_010_snapshot_folds_add_and_remove() {
    // REQ: LAKE-TXN-010
    let dir = tempfile::tempdir().unwrap();
    let log = new_log(dir.path()).await;
    log.commit(write_commit("a", vec![add("a.parquet")]))
        .await
        .unwrap();
    log.commit(write_commit("a", vec![add("b.parquet")]))
        .await
        .unwrap();
    log.commit(Commit::new(
        CommitInfo::new("a", Operation::Delete, serde_json::json!({})),
        vec![Action::Remove {
            path: "a.parquet".into(),
            deletion_timestamp: 1,
        }],
    ))
    .await
    .unwrap();

    let v2 = log.snapshot(Some(Version(2))).await.unwrap();
    let v3 = log.snapshot(Some(Version(3))).await.unwrap();
    let names = |s: &rusthouse::lake::log::Snapshot| {
        let mut v: Vec<String> = s.files.iter().map(|f| f.path.clone()).collect();
        v.sort();
        v
    };
    assert_eq!(names(&v2), vec!["a.parquet", "b.parquet"]);
    assert_eq!(names(&v3), vec!["b.parquet"]);
    assert_eq!(v3.metadata.schema.fields.len(), 1);

    let err = log.snapshot(Some(Version(4))).await.unwrap_err();
    assert!(matches!(err, RustHouseError::VersionNotFound(4)), "{err:?}");
}

#[tokio::test]
async fn lake_txn_011_replay_is_deterministic() {
    // REQ: LAKE-TXN-011
    let dir = tempfile::tempdir().unwrap();
    let log = new_log(dir.path()).await;
    for i in 0..50 {
        let mut actions = vec![add(&format!("p{i}.parquet"))];
        if i % 7 == 6 {
            actions.push(Action::Remove {
                path: format!("p{}.parquet", i - 3),
                deletion_timestamp: i as u64,
            });
        }
        log.commit(write_commit("a", actions)).await.unwrap();
    }
    let s1 = log.snapshot(None).await.unwrap();
    let s2 = TransactionLog::open(dir.path())
        .await
        .unwrap()
        .snapshot(None)
        .await
        .unwrap();
    assert_eq!(s1, s2);
    let p1: Vec<&str> = s1.files.iter().map(|f| f.path.as_str()).collect();
    let p2: Vec<&str> = s2.files.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(p1, p2, "file order preserved");
}

#[tokio::test]
async fn lake_txn_012_commitinfo_carries_required_fields() {
    // REQ: LAKE-TXN-012
    let dir = tempfile::tempdir().unwrap();
    let log = new_log(dir.path()).await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    log.commit(write_commit("alice", vec![add("p.parquet")]))
        .await
        .unwrap();
    let text =
        std::fs::read_to_string(dir.path().join("_rhlog/00000000000000000001.json")).unwrap();
    let ci = text
        .lines()
        .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap())
        .find(|v| v["action"] == "commitInfo")
        .unwrap();
    let ts = ci["timestamp"].as_u64().unwrap();
    assert!(
        ts.abs_diff(now) < 5_000,
        "timestamp within 5s: {ts} vs {now}"
    );
    assert_eq!(ci["principal"], "alice");
    assert_eq!(ci["operation"], "WRITE");
    assert_eq!(ci["operation_parameters"]["mode"], "append");
    assert_eq!(ci["client_version"], env!("CARGO_PKG_VERSION"));
}

#[tokio::test]
#[ignore = "performance budget; run with --release --ignored"]
async fn lake_txn_013_commit_and_replay_within_budget() {
    // REQ: LAKE-TXN-013
    let dir = tempfile::tempdir().unwrap();
    let opts = LogOptions {
        checkpoint_interval: 0, // disabled: measure raw replay
        ..LogOptions::default()
    };
    let log = TransactionLog::create_with(dir.path(), opts.clone(), "a", metadata())
        .await
        .unwrap();
    let mut lat = Vec::with_capacity(10_000);
    for i in 0..10_000 {
        let t = Instant::now();
        log.commit(write_commit("a", vec![add(&format!("p{i}.parquet"))]))
            .await
            .unwrap();
        lat.push(t.elapsed());
    }
    lat.sort();
    let p99 = lat[lat.len() * 99 / 100];
    assert!(p99.as_millis() < 20, "p99 commit latency {p99:?}");

    let t = Instant::now();
    let snap = TransactionLog::open_with(dir.path(), opts)
        .await
        .unwrap()
        .snapshot(None)
        .await
        .unwrap();
    let replay = t.elapsed();
    assert_eq!(snap.files.len(), 10_000);
    assert!(replay.as_millis() < 500, "cold replay took {replay:?}");
}

#[tokio::test]
async fn lake_txn_014_remove_of_unknown_file_rejected() {
    // REQ: LAKE-TXN-014
    let dir = tempfile::tempdir().unwrap();
    let log = new_log(dir.path()).await;
    log.commit(write_commit("a", vec![add("a.parquet")]))
        .await
        .unwrap();
    let err = log
        .commit(Commit::new(
            CommitInfo::new("a", Operation::Delete, serde_json::json!({})),
            vec![Action::Remove {
                path: "zzz.parquet".into(),
                deletion_timestamp: 1,
            }],
        ))
        .await
        .unwrap_err();
    assert!(matches!(err, RustHouseError::InvalidAction(_)), "{err:?}");
    assert_eq!(log.head().await.unwrap(), Version(1));
    // Removing twice is also invalid.
    log.commit(Commit::new(
        CommitInfo::new("a", Operation::Delete, serde_json::json!({})),
        vec![Action::Remove {
            path: "a.parquet".into(),
            deletion_timestamp: 1,
        }],
    ))
    .await
    .unwrap();
    let err = log
        .commit(Commit::new(
            CommitInfo::new("a", Operation::Delete, serde_json::json!({})),
            vec![Action::Remove {
                path: "a.parquet".into(),
                deletion_timestamp: 2,
            }],
        ))
        .await
        .unwrap_err();
    assert!(matches!(err, RustHouseError::InvalidAction(_)));
}

#[tokio::test]
async fn lake_txn_015_history_returns_commitinfo_range() {
    // REQ: LAKE-TXN-015
    let dir = tempfile::tempdir().unwrap();
    let log = new_log(dir.path()).await;
    for p in ["alice", "bob", "carol"] {
        log.commit(write_commit(p, vec![add(&format!("{p}.parquet"))]))
            .await
            .unwrap();
    }
    let h = log.history(Version(1), Version(2)).await.unwrap();
    assert_eq!(h.len(), 2);
    assert_eq!(h[0].principal, "alice");
    assert_eq!(h[1].principal, "bob");
    let all = log
        .history(Version(0), log.head().await.unwrap())
        .await
        .unwrap();
    assert_eq!(all.len(), 4);
    assert_eq!(all[0].operation, Operation::Create);
}
