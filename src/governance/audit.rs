//! Tamper-evident, append-only audit log (spec GOV-AUD).
//!
//! Every record carries the SHA-256 of its predecessor, so a single altered byte breaks the
//! chain from that point on. Records are appended (never rewritten) to
//! `<root>/<yyyy>/<mm>/<dd>/audit-<seq_start>.jsonl`; `<root>/_chain_head` holds the last
//! `{seq, hash}` and is replaced atomically. If the sink cannot write, callers must refuse the
//! governed operation (Constitution 7); every I/O failure surfaces as `AuditUnavailable`.

use crate::error::{Result, RustHouseError};
use crate::governance::classification::Classification;
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// `prev_hash` of the very first record (GOV-AUD-003).
pub const GENESIS_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const CHAIN_HEAD: &str = "_chain_head";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Decision {
    Allow,
    Deny,
    Mask,
    Override,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Intent,
    Ok,
    Error,
    Unauthenticated,
}

/// One line of the audit log (GOV-AUD-002). Unknown fields are rejected on read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditRecord {
    pub seq: u64,
    /// RFC 3339, UTC.
    pub ts: String,
    pub request_id: String,
    pub principal: String,
    pub operation: String,
    pub object: String,
    pub columns: Vec<String>,
    pub classifications: BTreeMap<String, Classification>,
    pub versions_read: BTreeMap<String, u64>,
    pub decision: Decision,
    pub row_count: Option<u64>,
    pub status: Status,
    pub elapsed_ms: Option<u64>,
    /// Hash of the governed, optimized plan (EXE-VEC-009). Absent for pre-M2 records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_hash: Option<String>,
    pub prev_hash: String,
    pub hash: String,
}

/// What a caller supplies; the sink adds `seq`, `ts`, `prev_hash`, `hash`.
#[derive(Debug, Clone, PartialEq)]
pub struct AuditDraft {
    pub request_id: String,
    pub principal: String,
    pub operation: String,
    pub object: String,
    pub columns: Vec<String>,
    pub classifications: BTreeMap<String, Classification>,
    pub versions_read: BTreeMap<String, u64>,
    pub decision: Decision,
    pub row_count: Option<u64>,
    pub status: Status,
    pub elapsed_ms: Option<u64>,
    pub plan_hash: Option<String>,
}

impl AuditDraft {
    pub fn new(
        request_id: impl Into<String>,
        principal: impl Into<String>,
        operation: impl Into<String>,
        object: impl Into<String>,
    ) -> Self {
        Self {
            request_id: request_id.into(),
            principal: principal.into(),
            operation: operation.into(),
            object: object.into(),
            columns: Vec::new(),
            classifications: BTreeMap::new(),
            versions_read: BTreeMap::new(),
            decision: Decision::Allow,
            row_count: None,
            status: Status::Intent,
            elapsed_ms: None,
            plan_hash: None,
        }
    }
    pub fn columns(mut self, columns: Vec<String>) -> Self {
        self.columns = columns;
        self
    }
    pub fn classifications(mut self, c: BTreeMap<String, Classification>) -> Self {
        self.classifications = c;
        self
    }
    pub fn versions_read(mut self, v: BTreeMap<String, u64>) -> Self {
        self.versions_read = v;
        self
    }
    pub fn decision(mut self, d: Decision) -> Self {
        self.decision = d;
        self
    }
    pub fn status(mut self, s: Status) -> Self {
        self.status = s;
        self
    }
    pub fn row_count(mut self, n: Option<u64>) -> Self {
        self.row_count = n;
        self
    }
    pub fn elapsed_ms(mut self, ms: Option<u64>) -> Self {
        self.elapsed_ms = ms;
        self
    }

    pub fn plan_hash(mut self, h: Option<String>) -> Self {
        self.plan_hash = h;
        self
    }

    fn into_record(self, seq: u64, ts: String, prev_hash: String) -> AuditRecord {
        let mut rec = AuditRecord {
            seq,
            ts,
            request_id: self.request_id,
            principal: self.principal,
            operation: self.operation,
            object: self.object,
            columns: self.columns,
            classifications: self.classifications,
            versions_read: self.versions_read,
            decision: self.decision,
            row_count: self.row_count,
            status: self.status,
            elapsed_ms: self.elapsed_ms,
            plan_hash: self.plan_hash.clone(),
            prev_hash,
            hash: String::new(),
        };
        rec.hash = compute_hash(&rec);
        rec
    }
}

// ---------------------------------------------------------------------------------------------
// Canonical JSON and hashing (GOV-AUD-003)
// ---------------------------------------------------------------------------------------------

/// Compact JSON with recursively sorted object keys. The single place hash input is produced.
fn write_canonical(v: &serde_json::Value, out: &mut Vec<u8>) {
    match v {
        serde_json::Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push(b'{');
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                out.extend_from_slice(serde_json::to_string(k).unwrap().as_bytes());
                out.push(b':');
                write_canonical(&map[*k], out);
            }
            out.push(b'}');
        }
        serde_json::Value::Array(items) => {
            out.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_canonical(item, out);
            }
            out.push(b']');
        }
        scalar => out.extend_from_slice(serde_json::to_string(scalar).unwrap().as_bytes()),
    }
}

/// Canonical bytes of a record **without** its `hash` field.
pub fn canonical_bytes(record: &AuditRecord) -> Vec<u8> {
    let mut v = serde_json::to_value(record).expect("AuditRecord serialises");
    if let serde_json::Value::Object(map) = &mut v {
        map.remove("hash");
    }
    let mut out = Vec::new();
    write_canonical(&v, &mut out);
    out
}

/// `hex(SHA-256(canonical_json(record without hash) || prev_hash))`.
pub fn compute_hash(record: &AuditRecord) -> String {
    let mut hasher = Sha256::new();
    hasher.update(canonical_bytes(record));
    hasher.update(record.prev_hash.as_bytes());
    hex(&hasher.finalize())
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

// ---------------------------------------------------------------------------------------------
// Sinks
// ---------------------------------------------------------------------------------------------

pub trait AuditSink: Send + Sync {
    /// Append one record, durably, before returning.
    fn append(&self, draft: AuditDraft) -> Result<AuditRecord> {
        let mut v = self.append_batch(vec![draft])?;
        v.pop()
            .ok_or_else(|| RustHouseError::AuditUnavailable("empty batch".into()))
    }
    /// Append several records in order with one durability barrier (GOV-AUD-012).
    fn append_batch(&self, drafts: Vec<AuditDraft>) -> Result<Vec<AuditRecord>>;
}

pub type Clock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChainHead {
    seq: u64,
    hash: String,
}

struct ChainState {
    next_seq: u64,
    prev_hash: String,
    /// Open file for the current day, if any.
    current: Option<(String, File)>, // (yyyy/mm/dd key, handle)
}

/// Append-only, hash-chained file sink.
pub struct FileAuditSink {
    root: PathBuf,
    clock: Clock,
    state: Mutex<ChainState>,
    fsyncs: AtomicU64,
}

fn io_err(context: &str, e: impl std::fmt::Display) -> RustHouseError {
    RustHouseError::AuditUnavailable(format!("{context}: {e}"))
}

impl std::fmt::Debug for FileAuditSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileAuditSink")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl FileAuditSink {
    pub fn open(root: &Path) -> Result<Self> {
        Self::with_clock(root, Arc::new(Utc::now))
    }

    /// Open with an injectable clock (used for rotation tests).
    pub fn with_clock(root: &Path, clock: Clock) -> Result<Self> {
        std::fs::create_dir_all(root).map_err(|e| io_err("create audit root", e))?;
        let (next_seq, prev_hash) = Self::resume(root)?;
        Ok(Self {
            root: root.to_path_buf(),
            clock,
            state: Mutex::new(ChainState {
                next_seq,
                prev_hash,
                current: None,
            }),
            fsyncs: AtomicU64::new(0),
        })
    }

    /// Number of `sync_data` calls made on data files since this sink was opened.
    pub fn fsync_count(&self) -> u64 {
        self.fsyncs.load(Ordering::Acquire)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Resume the chain from `_chain_head`, cross-checked against the last stored record.
    /// A head that lags the last record by exactly one (crash between append and head update)
    /// is repaired; anything else refuses to open.
    fn resume(root: &Path) -> Result<(u64, String)> {
        let head_path = root.join(CHAIN_HEAD);
        let head: Option<ChainHead> = match std::fs::read_to_string(&head_path) {
            Ok(text) => {
                Some(serde_json::from_str(&text).map_err(|e| io_err("parse _chain_head", e))?)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(io_err("read _chain_head", e)),
        };
        let last = last_record(root)?;
        match (head, last) {
            (None, None) => Ok((0, GENESIS_HASH.to_string())),
            (Some(h), Some(l)) if h.seq == l.seq && h.hash == l.hash => Ok((l.seq + 1, l.hash)),
            (Some(h), Some(l)) if l.seq == h.seq + 1 && l.prev_hash == h.hash => {
                write_chain_head(root, l.seq, &l.hash)?;
                Ok((l.seq + 1, l.hash))
            }
            (None, Some(_)) => Err(RustHouseError::AuditUnavailable(
                "audit records exist but _chain_head is missing".into(),
            )),
            (Some(_), None) => Err(RustHouseError::AuditUnavailable(
                "_chain_head exists but no audit records were found".into(),
            )),
            (Some(h), Some(l)) => Err(RustHouseError::AuditUnavailable(format!(
                "_chain_head (seq {}) does not match last record (seq {})",
                h.seq, l.seq
            ))),
        }
    }

    fn day_key(ts: &DateTime<Utc>) -> String {
        ts.format("%Y/%m/%d").to_string()
    }

    /// Ensure the file for `key` is open in append mode; a new day gets `audit-<seq>.jsonl`.
    fn file_for<'a>(&self, state: &'a mut ChainState, key: &str, seq: u64) -> Result<&'a mut File> {
        let reopen = match &state.current {
            Some((k, _)) => k != key,
            None => true,
        };
        if reopen {
            let dir = self.root.join(key);
            std::fs::create_dir_all(&dir).map_err(|e| io_err("create audit day dir", e))?;
            // Continue the newest file of this day if one exists, else start a new one.
            let path =
                newest_file_in(&dir)?.unwrap_or_else(|| dir.join(format!("audit-{seq}.jsonl")));
            let file = OpenOptions::new()
                .append(true)
                .create(true)
                .open(&path)
                .map_err(|e| io_err("open audit file", e))?;
            state.current = Some((key.to_string(), file));
        }
        Ok(&mut state.current.as_mut().expect("just set").1)
    }
}

impl AuditSink for FileAuditSink {
    fn append_batch(&self, drafts: Vec<AuditDraft>) -> Result<Vec<AuditRecord>> {
        if drafts.is_empty() {
            return Ok(Vec::new());
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| RustHouseError::AuditUnavailable("audit sink lock poisoned".into()))?;
        let mut records = Vec::with_capacity(drafts.len());
        let mut touched: Vec<String> = Vec::new(); // day keys written in this batch
        for draft in drafts {
            let now = (self.clock)();
            let ts = now.to_rfc3339_opts(SecondsFormat::Millis, true);
            let seq = state.next_seq;
            let rec = draft.into_record(seq, ts, state.prev_hash.clone());
            let mut line = serde_json::to_vec(&rec).map_err(|e| io_err("serialise", e))?;
            line.push(b'\n');
            let key = Self::day_key(&now);
            let file = self.file_for(&mut state, &key, seq)?;
            file.write_all(&line).map_err(|e| io_err("append", e))?;
            if !touched.contains(&key) {
                touched.push(key);
            }
            state.next_seq = seq + 1;
            state.prev_hash = rec.hash.clone();
            records.push(rec);
        }
        // One durability barrier per batch on the file currently open. If the batch crossed
        // midnight, the earlier day's handle was replaced; sync it too.
        if let Some((_, file)) = state.current.as_ref() {
            file.sync_data().map_err(|e| io_err("fsync", e))?;
            self.fsyncs.fetch_add(1, Ordering::AcqRel);
        }
        if touched.len() > 1 {
            for key in &touched[..touched.len() - 1] {
                if let Some(path) = newest_file_in(&self.root.join(key))? {
                    let f = File::open(path).map_err(|e| io_err("reopen for fsync", e))?;
                    f.sync_data().map_err(|e| io_err("fsync", e))?;
                    self.fsyncs.fetch_add(1, Ordering::AcqRel);
                }
            }
        }
        let last = records.last().expect("non-empty");
        write_chain_head(&self.root, last.seq, &last.hash)?;
        Ok(records)
    }
}

fn write_chain_head(root: &Path, seq: u64, hash: &str) -> Result<()> {
    let tmp = root.join(format!("{CHAIN_HEAD}.tmp"));
    let body = serde_json::to_vec(&ChainHead {
        seq,
        hash: hash.to_string(),
    })
    .map_err(|e| io_err("serialise _chain_head", e))?;
    std::fs::write(&tmp, body).map_err(|e| io_err("write _chain_head", e))?;
    std::fs::rename(&tmp, root.join(CHAIN_HEAD)).map_err(|e| io_err("publish _chain_head", e))
}

/// In-memory sink for tests.
#[derive(Default)]
pub struct MemoryAuditSink {
    records: Mutex<Vec<AuditRecord>>,
}

impl MemoryAuditSink {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn records(&self) -> Vec<AuditRecord> {
        self.records.lock().unwrap().clone()
    }
}

impl AuditSink for MemoryAuditSink {
    fn append_batch(&self, drafts: Vec<AuditDraft>) -> Result<Vec<AuditRecord>> {
        let mut store = self.records.lock().unwrap();
        let mut out = Vec::new();
        for d in drafts {
            let (seq, prev) = match store.last() {
                Some(r) => (r.seq + 1, r.hash.clone()),
                None => (0, GENESIS_HASH.to_string()),
            };
            let rec = d.into_record(
                seq,
                Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
                prev,
            );
            store.push(rec.clone());
            out.push(rec);
        }
        Ok(out)
    }
}

/// Sink that always fails; drives the fail-closed tests (GOV-AUD-001).
pub struct FailingAuditSink;

impl AuditSink for FailingAuditSink {
    fn append_batch(&self, _drafts: Vec<AuditDraft>) -> Result<Vec<AuditRecord>> {
        Err(RustHouseError::AuditUnavailable(
            "audit sink unavailable (injected failure)".into(),
        ))
    }
}

// ---------------------------------------------------------------------------------------------
// File discovery, verify, export
// ---------------------------------------------------------------------------------------------

/// All `audit-<seq>.jsonl` files under `root`, in chain order (by day, then by starting seq).
pub fn audit_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut out: Vec<(String, u64, PathBuf)> = Vec::new();
    fn walk(dir: &Path, out: &mut Vec<(String, u64, PathBuf)>) -> Result<()> {
        for entry in std::fs::read_dir(dir).map_err(|e| io_err("read audit dir", e))? {
            let entry = entry.map_err(|e| io_err("read audit dir entry", e))?;
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out)?;
            } else if let Some(seq) = seq_start_of(&path) {
                let day = path
                    .parent()
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_default();
                out.push((day, seq, path));
            }
        }
        Ok(())
    }
    if root.exists() {
        walk(root, &mut out)?;
    }
    out.sort();
    Ok(out.into_iter().map(|(_, _, p)| p).collect())
}

fn seq_start_of(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    name.strip_prefix("audit-")?
        .strip_suffix(".jsonl")?
        .parse()
        .ok()
}

fn newest_file_in(dir: &Path) -> Result<Option<PathBuf>> {
    let mut best: Option<(u64, PathBuf)> = None;
    for entry in std::fs::read_dir(dir).map_err(|e| io_err("read audit day dir", e))? {
        let path = entry.map_err(|e| io_err("read entry", e))?.path();
        if let Some(seq) = seq_start_of(&path) {
            if best.as_ref().is_none_or(|(s, _)| seq > *s) {
                best = Some((seq, path));
            }
        }
    }
    Ok(best.map(|(_, p)| p))
}

fn last_record(root: &Path) -> Result<Option<AuditRecord>> {
    let files = audit_files(root)?;
    let Some(last) = files.last() else {
        return Ok(None);
    };
    let text = std::fs::read_to_string(last).map_err(|e| io_err("read last audit file", e))?;
    match text.lines().rfind(|l| !l.trim().is_empty()) {
        None => Ok(None),
        Some(line) => serde_json::from_str(line)
            .map(Some)
            .map_err(|e| io_err("parse last audit record", e)),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyReport {
    pub records: u64,
    pub ok: bool,
    pub first_bad_seq: Option<u64>,
    pub reason: Option<String>,
}

/// Walk the chain from genesis (or `from`) and report the first broken link (GOV-AUD-006).
pub fn verify(root: &Path, from: Option<u64>) -> Result<VerifyReport> {
    let mut report = VerifyReport {
        records: 0,
        ok: true,
        first_bad_seq: None,
        reason: None,
    };
    let mut expected_seq = from.unwrap_or(0);
    let mut expected_prev: Option<String> = if from.is_none() {
        Some(GENESIS_HASH.to_string())
    } else {
        None // when starting mid-chain the first prev_hash is taken as given
    };
    let fail = |report: &mut VerifyReport, seq: u64, reason: String| {
        report.ok = false;
        report.first_bad_seq = Some(seq);
        report.reason = Some(reason);
    };
    'files: for path in audit_files(root)? {
        let text = std::fs::read_to_string(&path).map_err(|e| io_err("read audit file", e))?;
        for (lineno, line) in text.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            // Peek at seq to skip records before `from`.
            let peek: serde_json::Value = match serde_json::from_str(line) {
                Ok(v) => v,
                Err(e) => {
                    fail(
                        &mut report,
                        expected_seq,
                        format!("{}:{}: unparsable line: {e}", path.display(), lineno + 1),
                    );
                    break 'files;
                }
            };
            let seq = peek.get("seq").and_then(|s| s.as_u64());
            if let (Some(s), Some(f)) = (seq, from) {
                if s < f {
                    continue;
                }
            }
            let rec: AuditRecord = match serde_json::from_value(peek) {
                Ok(r) => r,
                Err(e) => {
                    fail(
                        &mut report,
                        seq.unwrap_or(expected_seq),
                        format!("seq {}: invalid record: {e}", seq.unwrap_or(expected_seq)),
                    );
                    break 'files;
                }
            };
            if rec.seq != expected_seq {
                fail(
                    &mut report,
                    expected_seq,
                    format!("gap: expected seq {expected_seq}, found {}", rec.seq),
                );
                break 'files;
            }
            if let Some(prev) = &expected_prev {
                if &rec.prev_hash != prev {
                    fail(
                        &mut report,
                        rec.seq,
                        format!("chain broken at seq {}: prev_hash mismatch", rec.seq),
                    );
                    break 'files;
                }
            }
            if compute_hash(&rec) != rec.hash {
                fail(
                    &mut report,
                    rec.seq,
                    format!("chain broken at seq {}: hash mismatch", rec.seq),
                );
                break 'files;
            }
            report.records += 1;
            expected_prev = Some(rec.hash);
            expected_seq += 1;
        }
    }
    Ok(report)
}

#[derive(Debug, Clone, Default)]
pub struct ExportFilter {
    pub from_ts: Option<DateTime<Utc>>,
    pub to_ts: Option<DateTime<Utc>>,
    pub principal: Option<String>,
    pub table: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportManifest {
    pub seq_from: Option<u64>,
    pub seq_to: Option<u64>,
    pub count: u64,
    /// Hash of the chain head at export time, so the extract can be tied to the full log.
    pub chain_head_hash: String,
}

/// Emit matching records as JSONL and return a manifest (GOV-AUD-010).
pub fn export(root: &Path, filter: ExportFilter, out: &mut dyn Write) -> Result<ExportManifest> {
    let head_text = std::fs::read_to_string(root.join(CHAIN_HEAD))
        .map_err(|e| io_err("read _chain_head", e))?;
    let head: ChainHead =
        serde_json::from_str(&head_text).map_err(|e| io_err("parse _chain_head", e))?;
    let mut manifest = ExportManifest {
        seq_from: None,
        seq_to: None,
        count: 0,
        chain_head_hash: head.hash,
    };
    for path in audit_files(root)? {
        let text = std::fs::read_to_string(&path).map_err(|e| io_err("read audit file", e))?;
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            let rec: AuditRecord =
                serde_json::from_str(line).map_err(|e| io_err("parse record", e))?;
            let ts = DateTime::parse_from_rfc3339(&rec.ts)
                .map_err(|e| io_err("parse ts", e))?
                .with_timezone(&Utc);
            if filter.from_ts.is_some_and(|f| ts < f) || filter.to_ts.is_some_and(|t| ts > t) {
                continue;
            }
            if filter
                .principal
                .as_ref()
                .is_some_and(|p| p != &rec.principal)
            {
                continue;
            }
            if filter.table.as_ref().is_some_and(|t| t != &rec.object) {
                continue;
            }
            out.write_all(line.as_bytes())
                .map_err(|e| io_err("write export", e))?;
            out.write_all(b"\n")
                .map_err(|e| io_err("write export", e))?;
            manifest.seq_from.get_or_insert(rec.seq);
            manifest.seq_to = Some(rec.seq);
            manifest.count += 1;
        }
    }
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn rec(principal: &str, prev: &str) -> AuditRecord {
        AuditDraft::new("r", principal, "SELECT", "gold.t").into_record(
            0,
            "2026-01-01T00:00:00.000Z".into(),
            prev.into(),
        )
    }

    proptest! {
        /// REQ: GOV-AUD-003 — canonical bytes are stable across a JSON round trip and sorted.
        #[test]
        fn test_canonical_json_is_deterministic(
            principal in "[a-z]{1,8}",
            cols in prop::collection::vec("[a-z_]{1,6}", 0..5),
            versions in prop::collection::btree_map("[a-z]{1,4}\\.[a-z]{1,4}", 0u64..100, 0..4),
        ) {
            let r = AuditDraft::new("req", principal, "SELECT", "gold.t")
                .columns(cols)
                .versions_read(versions)
                .into_record(3, "2026-01-01T00:00:00.000Z".into(), GENESIS_HASH.into());
            let a = canonical_bytes(&r);
            let back: AuditRecord = serde_json::from_str(&serde_json::to_string(&r).unwrap()).unwrap();
            prop_assert_eq!(&a, &canonical_bytes(&back));
            let s = String::from_utf8(a).unwrap();
            prop_assert!(!s.contains('\n') && !s.contains(": ") && !s.contains(", "));
            prop_assert!(!s.contains("\"hash\""));
            // top-level keys appear in sorted order
            let keys: Vec<&str> = ["\"classifications\"", "\"columns\"", "\"decision\"", "\"seq\"", "\"ts\""].to_vec();
            let pos: Vec<usize> = keys.iter().map(|k| s.find(k).unwrap()).collect();
            prop_assert!(pos.windows(2).all(|w| w[0] < w[1]));
            prop_assert_eq!(compute_hash(&r), compute_hash(&back));
        }
    }

    #[test]
    fn test_hash_changes_with_content_and_prev() {
        let a = rec("alice", GENESIS_HASH);
        let b = rec("bob", GENESIS_HASH);
        let c = rec("alice", &"1".repeat(64));
        assert_ne!(a.hash, b.hash);
        assert_ne!(a.hash, c.hash);
        assert_eq!(a.hash.len(), 64);
    }

    #[test]
    fn test_sink_opens_append_and_fsyncs() {
        // REQ: GOV-AUD-007
        let dir = tempfile::tempdir().unwrap();
        let sink = FileAuditSink::open(dir.path()).unwrap();
        sink.append(AuditDraft::new("r1", "a", "SELECT", "gold.t"))
            .unwrap();
        assert_eq!(sink.fsync_count(), 1, "fsync before append() returns");
        drop(sink);
        // A fresh handle must continue the same file in append mode: both lines survive.
        let sink = FileAuditSink::open(dir.path()).unwrap();
        sink.append(AuditDraft::new("r2", "a", "SELECT", "gold.t"))
            .unwrap();
        assert_eq!(sink.fsync_count(), 1);
        let files = audit_files(dir.path()).unwrap();
        assert_eq!(files.len(), 1);
        let text = std::fs::read_to_string(&files[0]).unwrap();
        assert_eq!(text.lines().count(), 2);
        let r = verify(dir.path(), None).unwrap();
        assert!(r.ok && r.records == 2);
    }

    #[test]
    fn test_resume_refuses_inconsistent_head() {
        let dir = tempfile::tempdir().unwrap();
        let sink = FileAuditSink::open(dir.path()).unwrap();
        sink.append(AuditDraft::new("r1", "a", "SELECT", "gold.t"))
            .unwrap();
        sink.append(AuditDraft::new("r2", "a", "SELECT", "gold.t"))
            .unwrap();
        drop(sink);
        std::fs::write(
            dir.path().join(CHAIN_HEAD),
            serde_json::json!({"seq": 0, "hash": "deadbeef"}).to_string(),
        )
        .unwrap();
        let err = FileAuditSink::open(dir.path()).unwrap_err();
        assert!(
            matches!(err, RustHouseError::AuditUnavailable(_)),
            "{err:?}"
        );
    }

    #[test]
    fn test_resume_repairs_head_one_behind() {
        let dir = tempfile::tempdir().unwrap();
        let sink = FileAuditSink::open(dir.path()).unwrap();
        let r1 = sink
            .append(AuditDraft::new("r1", "a", "SELECT", "gold.t"))
            .unwrap();
        sink.append(AuditDraft::new("r2", "a", "SELECT", "gold.t"))
            .unwrap();
        drop(sink);
        // Simulate a crash after the append but before the head was replaced.
        write_chain_head(dir.path(), r1.seq, &r1.hash).unwrap();
        let sink = FileAuditSink::open(dir.path()).unwrap();
        let r3 = sink
            .append(AuditDraft::new("r3", "a", "SELECT", "gold.t"))
            .unwrap();
        assert_eq!(r3.seq, 2);
        assert!(verify(dir.path(), None).unwrap().ok);
    }

    #[test]
    fn test_memory_and_failing_sinks() {
        let m = MemoryAuditSink::new();
        m.append(AuditDraft::new("r", "a", "SELECT", "t")).unwrap();
        m.append(AuditDraft::new("r", "a", "SELECT", "t")).unwrap();
        let recs = m.records();
        assert_eq!(recs[1].prev_hash, recs[0].hash);
        assert!(matches!(
            FailingAuditSink.append(AuditDraft::new("r", "a", "SELECT", "t")),
            Err(RustHouseError::AuditUnavailable(_))
        ));
    }
}
