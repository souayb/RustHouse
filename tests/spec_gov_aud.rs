//! Acceptance tests for specs/features/GOV-AUD-audit-log.md (sink, chain, verify, export).
//! Requirements needing the Governor/Session (001, 005, 008, 009, 011) are tested elsewhere.

use chrono::{DateTime, TimeZone, Utc};
use rusthouse::governance::audit::{
    compute_hash, export, verify, AuditDraft, AuditRecord, AuditSink, Decision, ExportFilter,
    FileAuditSink, Status, GENESIS_HASH,
};
use rusthouse::governance::classification::{Classification, Level, Tag};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

fn draft(principal: &str, i: u64) -> AuditDraft {
    let mut cls = BTreeMap::new();
    cls.insert(
        "email".to_string(),
        Classification::new(Level::Confidential).with_tags([Tag::Pii]),
    );
    let mut versions = BTreeMap::new();
    versions.insert("gold.accounts".to_string(), 7u64);
    AuditDraft::new(format!("req-{i}"), principal, "SELECT", "gold.accounts")
        .columns(vec!["email".into(), "balance".into()])
        .classifications(cls)
        .versions_read(versions)
        .decision(Decision::Allow)
        .status(Status::Ok)
        .row_count(Some(40))
        .elapsed_ms(Some(3))
}

/// All record lines across the audit directory, in chain order.
fn records(root: &Path) -> Vec<AuditRecord> {
    let mut files = Vec::new();
    fn walk(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
        for e in std::fs::read_dir(dir).unwrap().flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, out);
            } else if p.extension().and_then(|s| s.to_str()) == Some("jsonl") {
                out.push(p);
            }
        }
    }
    walk(root, &mut files);
    files.sort();
    let mut out = Vec::new();
    for f in files {
        for line in std::fs::read_to_string(&f).unwrap().lines() {
            out.push(serde_json::from_str::<AuditRecord>(line).unwrap());
        }
    }
    out.sort_by_key(|r| r.seq);
    out
}

type SharedClock = Arc<Mutex<DateTime<Utc>>>;

fn fixed_clock(t: DateTime<Utc>) -> (SharedClock, rusthouse::governance::audit::Clock) {
    let cell = Arc::new(Mutex::new(t));
    let c = cell.clone();
    (cell, Arc::new(move || *c.lock().unwrap()))
}

#[test]
fn gov_aud_002_record_schema_complete_and_strict() {
    // REQ: GOV-AUD-002
    let dir = tempfile::tempdir().unwrap();
    let sink = FileAuditSink::open(dir.path()).unwrap();
    sink.append(draft("alice", 1)).unwrap();

    let recs = records(dir.path());
    assert_eq!(recs.len(), 1);
    let line = serde_json::to_value(&recs[0]).unwrap();
    let obj = line.as_object().unwrap();
    let expected = [
        "seq",
        "ts",
        "request_id",
        "principal",
        "operation",
        "object",
        "columns",
        "classifications",
        "versions_read",
        "decision",
        "row_count",
        "status",
        "prev_hash",
        "hash",
        "elapsed_ms",
    ];
    for k in expected {
        assert!(obj.contains_key(k), "missing field {k}: {line}");
    }
    assert!(obj["seq"].is_u64());
    DateTime::parse_from_rfc3339(obj["ts"].as_str().unwrap()).expect("ts is RFC3339");
    assert!(
        obj["ts"].as_str().unwrap().ends_with('Z')
            || obj["ts"].as_str().unwrap().contains("+00:00")
    );
    assert!(obj["columns"].is_array());
    assert!(obj["classifications"].is_object());
    assert!(obj["versions_read"].is_object());
    assert_eq!(obj["decision"], "allow");
    assert_eq!(obj["status"], "ok");
    assert_eq!(obj["row_count"], 40);
    assert_eq!(obj["prev_hash"].as_str().unwrap().len(), 64);
    assert_eq!(obj["hash"].as_str().unwrap().len(), 64);

    // Unknown fields are rejected by the record type and by the verifier.
    let mut extra = line.clone();
    extra["sneaky"] = serde_json::json!(true);
    assert!(serde_json::from_value::<AuditRecord>(extra.clone()).is_err());

    let file = find_single_file(dir.path());
    std::fs::write(
        &file,
        format!("{}\n", serde_json::to_string(&extra).unwrap()),
    )
    .unwrap();
    let report = verify(dir.path(), None).unwrap();
    assert!(!report.ok);
    assert_eq!(report.first_bad_seq, Some(0));
}

fn find_single_file(root: &Path) -> std::path::PathBuf {
    let mut files = Vec::new();
    fn walk(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
        for e in std::fs::read_dir(dir).unwrap().flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, out);
            } else if p.extension().and_then(|s| s.to_str()) == Some("jsonl") {
                out.push(p);
            }
        }
    }
    walk(root, &mut files);
    assert_eq!(files.len(), 1, "{files:?}");
    files.pop().unwrap()
}

#[test]
fn gov_aud_003_hash_chain_well_formed() {
    // REQ: GOV-AUD-003
    let dir = tempfile::tempdir().unwrap();
    let sink = FileAuditSink::open(dir.path()).unwrap();
    for i in 0..3 {
        sink.append(draft("alice", i)).unwrap();
    }
    let r = records(dir.path());
    assert_eq!(r.len(), 3);
    assert_eq!(r[0].seq, 0);
    assert_eq!(r[1].seq, 1);
    assert_eq!(r[2].seq, 2);
    assert_eq!(r[0].prev_hash, GENESIS_HASH);
    assert_eq!(r[0].prev_hash, "0".repeat(64));
    assert_eq!(r[1].prev_hash, r[0].hash);
    assert_eq!(r[2].prev_hash, r[1].hash);
    for rec in &r {
        assert_eq!(
            compute_hash(rec),
            rec.hash,
            "hash recomputes from canonical JSON"
        );
    }
    assert!(verify(dir.path(), None).unwrap().ok);
}

#[test]
fn gov_aud_004_daily_rotation_keeps_chain() {
    // REQ: GOV-AUD-004
    let dir = tempfile::tempdir().unwrap();
    let (cell, clock) = fixed_clock(Utc.with_ymd_and_hms(2026, 3, 31, 23, 59, 59).unwrap());
    let sink = FileAuditSink::with_clock(dir.path(), clock).unwrap();
    sink.append(draft("alice", 1)).unwrap();
    sink.append(draft("alice", 2)).unwrap();
    *cell.lock().unwrap() = Utc.with_ymd_and_hms(2026, 4, 1, 0, 0, 1).unwrap();
    sink.append(draft("alice", 3)).unwrap();

    assert!(dir.path().join("2026/03/31").is_dir());
    assert!(dir.path().join("2026/04/01").is_dir());
    let march: Vec<_> = std::fs::read_dir(dir.path().join("2026/03/31"))
        .unwrap()
        .flatten()
        .collect();
    let april: Vec<_> = std::fs::read_dir(dir.path().join("2026/04/01"))
        .unwrap()
        .flatten()
        .collect();
    assert_eq!(march.len(), 1);
    assert_eq!(april.len(), 1);
    assert!(april[0].file_name().to_string_lossy().starts_with("audit-"));
    assert_eq!(
        april[0].file_name().to_string_lossy(),
        "audit-2.jsonl",
        "named by first seq"
    );

    let r = records(dir.path());
    assert_eq!(r.len(), 3);
    assert_eq!(r[2].prev_hash, r[1].hash);
    let head: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(dir.path().join("_chain_head")).unwrap())
            .unwrap();
    assert_eq!(head["seq"], 2);
    assert_eq!(head["hash"], r[2].hash);
    assert!(verify(dir.path(), None).unwrap().ok);
}

#[test]
fn gov_aud_006_tamper_detected_at_seq() {
    // REQ: GOV-AUD-006
    let dir = tempfile::tempdir().unwrap();
    let sink = FileAuditSink::open(dir.path()).unwrap();
    for i in 0..100 {
        sink.append(draft("alice", i)).unwrap();
    }
    let report = verify(dir.path(), None).unwrap();
    assert!(report.ok);
    assert_eq!(report.records, 100);

    let file = find_single_file(dir.path());
    let text = std::fs::read_to_string(&file).unwrap();
    let tampered: Vec<String> = text
        .lines()
        .map(|l| {
            let v: serde_json::Value = serde_json::from_str(l).unwrap();
            if v["seq"] == 57 {
                l.replacen("\"principal\":\"alice\"", "\"principal\":\"alicf\"", 1)
            } else {
                l.to_string()
            }
        })
        .collect();
    assert_ne!(
        tampered.join("\n"),
        text.trim_end(),
        "tamper must have changed a byte"
    );
    std::fs::write(&file, tampered.join("\n") + "\n").unwrap();

    let report = verify(dir.path(), None).unwrap();
    assert!(!report.ok);
    assert_eq!(report.first_bad_seq, Some(57));
    assert!(report.reason.as_deref().unwrap_or("").contains("57"));
    // Records after the break are not vouched for either.
    let from_60 = verify(dir.path(), Some(60)).unwrap();
    assert!(from_60.ok, "chain from 60 onwards is internally consistent");
}

#[test]
fn gov_aud_006_deleted_record_detected_as_gap() {
    // REQ: GOV-AUD-006
    let dir = tempfile::tempdir().unwrap();
    let sink = FileAuditSink::open(dir.path()).unwrap();
    for i in 0..100 {
        sink.append(draft("alice", i)).unwrap();
    }
    let file = find_single_file(dir.path());
    let text = std::fs::read_to_string(&file).unwrap();
    let kept: Vec<&str> = text
        .lines()
        .filter(|l| serde_json::from_str::<serde_json::Value>(l).unwrap()["seq"] != 57)
        .collect();
    std::fs::write(&file, kept.join("\n") + "\n").unwrap();

    let report = verify(dir.path(), None).unwrap();
    assert!(!report.ok);
    assert_eq!(report.first_bad_seq, Some(57));
    assert!(
        report
            .reason
            .as_deref()
            .unwrap()
            .to_lowercase()
            .contains("gap"),
        "{:?}",
        report.reason
    );
}

#[test]
fn gov_aud_010_export_with_verifiable_manifest() {
    // REQ: GOV-AUD-010
    let dir = tempfile::tempdir().unwrap();
    let day1 = Utc.with_ymd_and_hms(2026, 5, 1, 9, 0, 0).unwrap();
    let day2 = Utc.with_ymd_and_hms(2026, 5, 2, 9, 0, 0).unwrap();
    let (cell, clock) = fixed_clock(day1);
    let sink = FileAuditSink::with_clock(dir.path(), clock).unwrap();
    for i in 0..500u64 {
        if i == 250 {
            *cell.lock().unwrap() = day2;
        }
        let p = if i % 2 == 0 { "analyst" } else { "bob" };
        sink.append(draft(p, i)).unwrap();
    }
    let mut out = Vec::new();
    let manifest = export(
        dir.path(),
        ExportFilter {
            from_ts: Some(Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap()),
            to_ts: Some(Utc.with_ymd_and_hms(2026, 5, 1, 23, 59, 59).unwrap()),
            principal: Some("analyst".into()),
            table: None,
        },
        &mut out,
    )
    .unwrap();
    let text = String::from_utf8(out).unwrap();
    let exported: Vec<AuditRecord> = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(exported.len(), 125);
    assert!(exported.iter().all(|r| r.principal == "analyst"));
    assert!(exported.iter().all(|r| r.ts.starts_with("2026-05-01")));
    assert_eq!(manifest.count, 125);
    assert_eq!(manifest.seq_from, Some(0));
    assert_eq!(manifest.seq_to, Some(248));
    // Every exported record is individually verifiable and the manifest pins the chain head.
    for r in &exported {
        assert_eq!(compute_hash(r), r.hash);
    }
    let head: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(dir.path().join("_chain_head")).unwrap())
            .unwrap();
    assert_eq!(manifest.chain_head_hash, head["hash"].as_str().unwrap());
    let full = verify(dir.path(), None).unwrap();
    assert!(full.ok);
    assert_eq!(full.records, 500);
    // Exported seqs all lie inside the verified range.
    assert!(exported.iter().all(|r| r.seq <= 499));
}

#[test]
fn gov_aud_012_intent_and_outcome_single_fsync() {
    // REQ: GOV-AUD-012
    let dir = tempfile::tempdir().unwrap();
    let sink = FileAuditSink::open(dir.path()).unwrap();
    let before = sink.fsync_count();
    let intent =
        AuditDraft::new("req-9", "alice", "SELECT", "gold.accounts").status(Status::Intent);
    let outcome = draft("alice", 9);
    let recs = sink.append_batch(vec![intent, outcome]).unwrap();
    assert_eq!(recs.len(), 2);
    assert_eq!(recs[0].seq + 1, recs[1].seq);
    assert_eq!(recs[1].prev_hash, recs[0].hash);
    assert_eq!(recs[0].status, Status::Intent);
    assert_eq!(recs[1].status, Status::Ok);
    // One data-file fsync for the whole batch (the sink counts every sync_data call it makes).
    assert_eq!(sink.fsync_count() - before, 1);
    // Two single appends cost two fsyncs, for contrast.
    let before = sink.fsync_count();
    sink.append(draft("alice", 10)).unwrap();
    sink.append(draft("alice", 11)).unwrap();
    assert_eq!(sink.fsync_count() - before, 2);
    assert!(verify(dir.path(), None).unwrap().ok);
}
