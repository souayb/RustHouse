//! `rhlog`: an append-only, versioned transaction log per table (spec LAKE-TXN, ADR-0002).
//!
//! Layout: `<table>/_rhlog/<version:020>.json` (newline-delimited actions),
//! optional `<version:020>.checkpoint.json` plus `_last_checkpoint`.
//! Commits are written to a temporary file and published with an atomic
//! create-if-absent link, so a version file is never written in place and two
//! writers can never both own the same version.

use crate::error::{Result, RustHouseError};
use crate::lake::schema::Schema;
use crate::lake::stats::FileStats;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use uuid::Uuid;

pub const LOG_DIR: &str = "_rhlog";
const LAST_CHECKPOINT: &str = "_last_checkpoint";
const PROTOCOL_READER: u32 = 1;
const PROTOCOL_WRITER: u32 = 1;

/// Monotonically increasing table version; 0 is the creation commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Version(pub u64);

impl Version {
    pub fn filename(self) -> String {
        format!("{:020}.json", self.0)
    }
    fn checkpoint_filename(self) -> String {
        format!("{:020}.checkpoint.json", self.0)
    }
    /// Accepts only `<20 digits>.json`.
    pub fn parse_filename(name: &str) -> Option<Version> {
        let digits = name.strip_suffix(".json")?;
        if digits.len() != 20 || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        digits.parse().ok().map(Version)
    }
    pub fn next(self) -> Version {
        Version(self.0 + 1)
    }
}

/// Operation recorded in `commitInfo` (LAKE-TXN-012; catalog operations from LAKE-CAT-006).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Operation {
    Create,
    Write,
    Merge,
    Delete,
    Alter,
    Vacuum,
    Erase,
    Policy,
    CreateNamespace,
    CreateTable,
    DropTable,
    Grant,
    Revoke,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommitInfo {
    /// Milliseconds since the Unix epoch, UTC.
    pub timestamp: u64,
    pub principal: String,
    pub operation: Operation,
    pub operation_parameters: serde_json::Value,
    pub client_version: String,
}

impl CommitInfo {
    pub fn new(
        principal: impl Into<String>,
        operation: Operation,
        operation_parameters: serde_json::Value,
    ) -> Self {
        Self {
            timestamp: now_ms(),
            principal: principal.into(),
            operation,
            operation_parameters,
            client_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetaData {
    pub id: Uuid,
    pub schema: Schema,
    #[serde(default)]
    pub partition_columns: Vec<String>,
    pub created_time: u64,
}

impl MetaData {
    pub fn new(schema: Schema, partition_columns: Vec<String>) -> Self {
        Self {
            id: Uuid::new_v4(),
            schema,
            partition_columns,
            created_time: now_ms(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AddFile {
    pub path: String,
    pub size: u64,
    pub num_records: u64,
    pub stats: FileStats,
    #[serde(default)]
    pub partition_values: BTreeMap<String, String>,
}

impl AddFile {
    pub fn new(path: impl Into<String>, size: u64, num_records: u64, stats: FileStats) -> Self {
        Self {
            path: path.into(),
            size,
            num_records,
            stats,
            partition_values: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Protocol {
    pub min_reader_version: u32,
    pub min_writer_version: u32,
}

/// One line of a commit file (LAKE-TXN-002). `catalog` carries opaque catalog rows (LAKE-CAT).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "camelCase")]
pub enum Action {
    Protocol(Protocol),
    MetaData(MetaData),
    Add(AddFile),
    Remove {
        path: String,
        deletion_timestamp: u64,
    },
    CommitInfo(CommitInfo),
    Catalog {
        entry: serde_json::Value,
    },
}

/// Actions to publish as one version. Exactly one `CommitInfo`, supplied separately.
#[derive(Debug, Clone)]
pub struct Commit {
    pub info: CommitInfo,
    pub actions: Vec<Action>,
}

impl Commit {
    pub fn new(info: CommitInfo, actions: Vec<Action>) -> Self {
        Self { info, actions }
    }
}

/// Folded state of a table at a version (LAKE-TXN-010).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    pub version: Version,
    pub protocol: Protocol,
    pub metadata: MetaData,
    /// Active data files in first-added order.
    pub files: Vec<AddFile>,
    /// Catalog rows in commit order (only used by the catalog log).
    #[serde(default)]
    pub catalog_entries: Vec<serde_json::Value>,
    /// `commitInfo.timestamp` of this version, for `AS OF TIMESTAMP` resolution.
    pub timestamp: u64,
}

impl Snapshot {
    fn apply(&mut self, version: Version, actions: Vec<Action>) -> Result<()> {
        let mut saw_info = false;
        for a in actions {
            match a {
                Action::Protocol(p) => self.protocol = p,
                Action::MetaData(m) => self.metadata = m,
                Action::Add(f) => {
                    self.files.retain(|x| x.path != f.path);
                    self.files.push(f);
                }
                Action::Remove { path, .. } => {
                    let before = self.files.len();
                    self.files.retain(|x| x.path != path);
                    if self.files.len() == before {
                        return Err(RustHouseError::InvalidAction(format!(
                            "remove of '{path}' at version {}: file is not active",
                            version.0
                        )));
                    }
                }
                Action::CommitInfo(ci) => {
                    if saw_info {
                        return Err(RustHouseError::CorruptLog(format!(
                            "version {} has more than one commitInfo",
                            version.0
                        )));
                    }
                    saw_info = true;
                    self.timestamp = ci.timestamp;
                }
                Action::Catalog { entry } => self.catalog_entries.push(entry),
            }
        }
        if !saw_info {
            return Err(RustHouseError::CorruptLog(format!(
                "version {} has no commitInfo",
                version.0
            )));
        }
        self.version = version;
        Ok(())
    }
}

/// Time-travel target (LAKE-TT).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsOf {
    Version(u64),
    /// Milliseconds since the Unix epoch, UTC.
    Timestamp(i64),
}

#[derive(Debug, Clone)]
pub struct LogOptions {
    /// Write a checkpoint every N commits; 0 disables (LAKE-TXN-007).
    pub checkpoint_interval: u32,
    /// Retries on version collision before `CommitConflict` (LAKE-TXN-008).
    pub max_retries: u32,
    /// fsync commit files before publishing.
    pub fsync: bool,
}

impl Default for LogOptions {
    fn default() -> Self {
        Self {
            checkpoint_interval: 10,
            max_retries: 5,
            fsync: true,
        }
    }
}

#[derive(Debug)]
pub struct TransactionLog {
    table_dir: PathBuf,
    log_dir: PathBuf,
    options: LogOptions,
    /// Last version this handle knows about; probing starts here (LAKE-TXN-013).
    head_hint: AtomicU64,
    /// Most recent snapshot this handle built; later snapshots fold only the new commits.
    cache: Mutex<Option<Snapshot>>,
}

impl TransactionLog {
    /// Create a table log by writing version 0 (LAKE-TXN-003).
    pub async fn create(
        table_dir: impl AsRef<Path>,
        principal: &str,
        metadata: MetaData,
    ) -> Result<Self> {
        Self::create_with(table_dir, LogOptions::default(), principal, metadata).await
    }

    pub async fn create_with(
        table_dir: impl AsRef<Path>,
        options: LogOptions,
        principal: &str,
        metadata: MetaData,
    ) -> Result<Self> {
        if principal.is_empty() {
            return Err(RustHouseError::MissingPrincipal);
        }
        let table_dir = table_dir.as_ref().to_path_buf();
        let log_dir = table_dir.join(LOG_DIR);
        std::fs::create_dir_all(&log_dir)?;
        if Self::scan_head(&log_dir)?.is_some() {
            return Err(RustHouseError::TableExists(table_dir.display().to_string()));
        }
        let log = Self {
            table_dir,
            log_dir,
            options,
            head_hint: AtomicU64::new(0),
            cache: Mutex::new(None),
        };
        let actions = vec![
            Action::Protocol(Protocol {
                min_reader_version: PROTOCOL_READER,
                min_writer_version: PROTOCOL_WRITER,
            }),
            Action::MetaData(metadata),
        ];
        let info = CommitInfo::new(principal, Operation::Create, serde_json::json!({}));
        log.publish(Version(0), &info, &actions)?;
        Ok(log)
    }

    pub async fn open(table_dir: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(table_dir, LogOptions::default()).await
    }

    pub async fn open_with(table_dir: impl AsRef<Path>, options: LogOptions) -> Result<Self> {
        let table_dir = table_dir.as_ref().to_path_buf();
        let log_dir = table_dir.join(LOG_DIR);
        let head = Self::scan_head(&log_dir)?
            .ok_or_else(|| RustHouseError::TableNotFound(table_dir.display().to_string()))?;
        Ok(Self {
            table_dir,
            log_dir,
            options,
            head_hint: AtomicU64::new(head.0),
            cache: Mutex::new(None),
        })
    }

    pub fn table_dir(&self) -> &Path {
        &self.table_dir
    }

    pub fn options(&self) -> &LogOptions {
        &self.options
    }

    /// Current head version (LAKE-TXN-010).
    pub async fn head(&self) -> Result<Version> {
        let mut v = Version(self.head_hint.load(Ordering::Acquire));
        // Probe forward from the hint; O(1) in the common case.
        while self.log_dir.join(v.next().filename()).exists() {
            v = v.next();
        }
        if !self.log_dir.join(v.filename()).exists() {
            // Hint is ahead of reality (should not happen) — fall back to a scan.
            v = Self::scan_head(&self.log_dir)?
                .ok_or_else(|| RustHouseError::CorruptLog("log directory has no commits".into()))?;
        }
        self.head_hint.fetch_max(v.0, Ordering::AcqRel);
        Ok(v)
    }

    /// Append a commit at head+1 with optimistic retry (LAKE-TXN-004/005/008/014).
    pub async fn commit(&self, commit: Commit) -> Result<Version> {
        let base = self.head().await?;
        self.commit_at(base.next(), commit).await
    }

    /// Attempt to publish at `expected`; on collision retry at the new head up to the limit.
    pub async fn commit_at(&self, expected: Version, commit: Commit) -> Result<Version> {
        if commit.info.principal.is_empty() {
            return Err(RustHouseError::MissingPrincipal);
        }
        if commit
            .actions
            .iter()
            .any(|a| matches!(a, Action::CommitInfo(_)))
        {
            return Err(RustHouseError::InvalidAction(
                "commitInfo is supplied via Commit::info, not as an action".into(),
            ));
        }
        let mut target = expected;
        let mut attempts = 0u32;
        loop {
            // Re-validate against the snapshot the commit will be applied to.
            let base = self.snapshot(Some(Version(target.0 - 1))).await?;
            let mut probe = base.clone();
            let mut with_info = commit.actions.clone();
            with_info.push(Action::CommitInfo(commit.info.clone()));
            probe.apply(target, with_info)?; // checks remove semantics (LAKE-TXN-014)
            match self.publish(target, &commit.info, &commit.actions) {
                Ok(()) => {
                    self.head_hint.fetch_max(target.0, Ordering::AcqRel);
                    self.maybe_checkpoint(&probe, &commit.info);
                    self.remember(&probe);
                    return Ok(target);
                }
                Err(RustHouseError::Conflict(_)) => {
                    attempts += 1;
                    if attempts > self.options.max_retries {
                        let found = self.head().await?;
                        return Err(RustHouseError::CommitConflict {
                            table: self.table_dir.display().to_string(),
                            expected: target.0,
                            found: found.0,
                        });
                    }
                    target = self.head().await?.next();
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Fold the log up to `version` (default head) (LAKE-TXN-010/011).
    pub async fn snapshot(&self, version: Option<Version>) -> Result<Snapshot> {
        let head = self.head().await?;
        let target = version.unwrap_or(head);
        if target > head {
            return Err(RustHouseError::VersionNotFound(target.0));
        }
        // Start from the closest known state at or below the target: the in-memory cache
        // (incremental fold, keeps commit latency flat) or the latest checkpoint.
        let cached = self
            .cache
            .lock()
            .map(|g| g.clone())
            .unwrap_or(None)
            .filter(|s| s.version <= target);
        let (mut snap, mut next) = match cached {
            Some(s) => {
                let v = s.version.next();
                (Some(s), v)
            }
            None => match self.load_checkpoint(target)? {
                Some(s) => {
                    let v = s.version.next();
                    (Some(s), v)
                }
                None => (None, Version(0)),
            },
        };
        while next <= target {
            let actions = self.read_commit(next)?;
            match snap.as_mut() {
                None => snap = Some(Self::genesis(next, actions)?),
                Some(s) => s.apply(next, actions)?,
            }
            next = next.next();
        }
        let snap = snap.ok_or_else(|| RustHouseError::CorruptLog("empty log".into()))?;
        self.remember(&snap);
        Ok(snap)
    }

    /// Keep the newest snapshot seen by this handle.
    fn remember(&self, snap: &Snapshot) {
        if let Ok(mut g) = self.cache.lock() {
            if g.as_ref().map(|c| c.version < snap.version).unwrap_or(true) {
                *g = Some(snap.clone());
            }
        }
    }

    /// `commitInfo` for each version in `[from, to]` (LAKE-TXN-015).
    pub async fn history(&self, from: Version, to: Version) -> Result<Vec<CommitInfo>> {
        let head = self.head().await?;
        if to > head {
            return Err(RustHouseError::VersionNotFound(to.0));
        }
        let mut out = Vec::new();
        let mut v = from;
        while v <= to {
            let info = self
                .read_commit(v)?
                .into_iter()
                .find_map(|a| match a {
                    Action::CommitInfo(ci) => Some(ci),
                    _ => None,
                })
                .ok_or_else(|| {
                    RustHouseError::CorruptLog(format!("version {} has no commitInfo", v.0))
                })?;
            out.push(info);
            v = v.next();
        }
        Ok(out)
    }

    /// Resolve a time-travel target to a concrete version (LAKE-TT-003/004).
    pub async fn resolve_as_of(&self, as_of: &AsOf) -> Result<Version> {
        let head = self.head().await?;
        match *as_of {
            AsOf::Version(v) => {
                if v > head.0 {
                    Err(RustHouseError::VersionNotFound(v))
                } else {
                    Ok(Version(v))
                }
            }
            AsOf::Timestamp(t) => {
                // Greatest version whose commit timestamp <= t. Commit timestamps are
                // non-decreasing in practice; scan from head backwards.
                let mut v = head;
                loop {
                    let ts = self.history(v, v).await?[0].timestamp as i64;
                    if ts <= t {
                        return Ok(v);
                    }
                    if v.0 == 0 {
                        return Err(RustHouseError::VersionNotFound(0));
                    }
                    v = Version(v.0 - 1);
                }
            }
        }
    }

    /// Write a checkpoint of the current head (LAKE-TXN-007).
    pub async fn checkpoint(&self) -> Result<()> {
        let snap = self.snapshot(None).await?;
        self.write_checkpoint(&snap)
    }

    // ----- internals -------------------------------------------------------------------

    fn genesis(version: Version, actions: Vec<Action>) -> Result<Snapshot> {
        if version != Version(0) {
            return Err(RustHouseError::CorruptLog(format!(
                "log does not start at version 0 (first version {})",
                version.0
            )));
        }
        let mut protocol = None;
        let mut metadata = None;
        let mut info = None;
        let mut extra = Vec::new();
        for a in actions {
            match a {
                Action::Protocol(p) if protocol.is_none() => protocol = Some(p),
                Action::MetaData(m) if metadata.is_none() => metadata = Some(m),
                Action::CommitInfo(ci) if info.is_none() => info = Some(ci),
                Action::Protocol(_) | Action::MetaData(_) | Action::CommitInfo(_) => {
                    return Err(RustHouseError::CorruptLog(
                        "version 0 must have exactly one protocol, metaData and commitInfo".into(),
                    ))
                }
                other => extra.push(other),
            }
        }
        let (Some(protocol), Some(metadata), Some(info)) = (protocol, metadata, info) else {
            return Err(RustHouseError::CorruptLog(
                "version 0 must have exactly one protocol, metaData and commitInfo".into(),
            ));
        };
        if protocol.min_reader_version > PROTOCOL_READER {
            return Err(RustHouseError::CorruptLog(format!(
                "log requires reader version {}, this build supports {}",
                protocol.min_reader_version, PROTOCOL_READER
            )));
        }
        let mut snap = Snapshot {
            version: Version(0),
            protocol,
            metadata,
            files: Vec::new(),
            catalog_entries: Vec::new(),
            timestamp: info.timestamp,
        };
        // Extra actions in version 0 (e.g. catalog rows) still fold normally.
        if !extra.is_empty() {
            extra.push(Action::CommitInfo(info));
            snap.apply(Version(0), extra)?;
        }
        Ok(snap)
    }

    fn scan_head(log_dir: &Path) -> Result<Option<Version>> {
        if !log_dir.exists() {
            return Ok(None);
        }
        let mut head = None;
        for entry in std::fs::read_dir(log_dir)? {
            let name = entry?.file_name();
            if let Some(v) = Version::parse_filename(&name.to_string_lossy()) {
                head = Some(head.map_or(v, |h: Version| h.max(v)));
            }
        }
        Ok(head)
    }

    fn read_commit(&self, version: Version) -> Result<Vec<Action>> {
        let path = self.log_dir.join(version.filename());
        let text = std::fs::read_to_string(&path).map_err(|e| {
            if e.kind() == ErrorKind::NotFound {
                RustHouseError::VersionNotFound(version.0)
            } else {
                RustHouseError::Io(e)
            }
        })?;
        let mut actions = Vec::new();
        for (i, line) in text.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let action: Action = serde_json::from_str(line).map_err(|e| {
                RustHouseError::CorruptLog(format!("{}:{}: {}", path.display(), i + 1, e))
            })?;
            actions.push(action);
        }
        Ok(actions)
    }

    /// Serialise and publish `version`. Returns `Conflict` if the version already exists.
    fn publish(&self, version: Version, info: &CommitInfo, actions: &[Action]) -> Result<()> {
        let final_path = self.log_dir.join(version.filename());
        if final_path.exists() {
            return Err(RustHouseError::Conflict(format!(
                "version {} exists",
                version.0
            )));
        }
        self.remove_stale_tmps(version);

        let mut body = Vec::new();
        for a in actions {
            serde_json::to_writer(&mut body, a)?;
            body.push(b'\n');
        }
        serde_json::to_writer(&mut body, &Action::CommitInfo(info.clone()))?;
        body.push(b'\n');

        // Unique tmp name per writer so concurrent writers never clobber each other's draft.
        let tmp_path = self.log_dir.join(format!(
            "{}.{}.tmp",
            version.filename(),
            Uuid::new_v4().simple()
        ));
        {
            let mut f = std::fs::File::create(&tmp_path)?;
            f.write_all(&body)?;
            if self.options.fsync {
                f.sync_all()?;
            }
        }
        let published = publish_atomically(&tmp_path, &final_path);
        let _ = std::fs::remove_file(&tmp_path);
        published
    }

    /// LAKE-TXN-006: drafts left by a crashed writer for versions at or below this one are stale.
    fn remove_stale_tmps(&self, upto: Version) {
        if let Ok(rd) = std::fs::read_dir(&self.log_dir) {
            for entry in rd.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if !name.ends_with(".tmp") {
                    continue;
                }
                let is_stale = name
                    .get(..20)
                    .and_then(|d| d.parse::<u64>().ok())
                    .map(|v| v <= upto.0)
                    .unwrap_or(true);
                if is_stale {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
    }

    fn maybe_checkpoint(&self, snap: &Snapshot, _info: &CommitInfo) {
        let n = self.options.checkpoint_interval as u64;
        if n > 0 && snap.version.0 > 0 && snap.version.0.is_multiple_of(n) {
            // Checkpoints are an optimisation; a failure must not fail the commit.
            let _ = self.write_checkpoint(snap);
        }
    }

    fn write_checkpoint(&self, snap: &Snapshot) -> Result<()> {
        let path = self.log_dir.join(snap.version.checkpoint_filename());
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec(snap)?)?;
        std::fs::rename(&tmp, &path)?;
        let head_tmp = self.log_dir.join(format!("{LAST_CHECKPOINT}.tmp"));
        std::fs::write(
            &head_tmp,
            serde_json::to_vec(&serde_json::json!({ "version": snap.version.0 }))?,
        )?;
        std::fs::rename(&head_tmp, self.log_dir.join(LAST_CHECKPOINT))?;
        Ok(())
    }

    /// Latest checkpoint at or below `target`, if valid.
    fn load_checkpoint(&self, target: Version) -> Result<Option<Snapshot>> {
        if self.options.checkpoint_interval == 0 {
            return Ok(None);
        }
        let head_path = self.log_dir.join(LAST_CHECKPOINT);
        let Ok(text) = std::fs::read_to_string(&head_path) else {
            return Ok(None);
        };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) else {
            return Ok(None);
        };
        let Some(mut cv) = v["version"].as_u64() else {
            return Ok(None);
        };
        // Walk back to a checkpoint at or below the target.
        let n = self.options.checkpoint_interval as u64;
        while cv > target.0 {
            if cv < n {
                return Ok(None);
            }
            cv -= n;
        }
        let path = self.log_dir.join(Version(cv).checkpoint_filename());
        match std::fs::read(&path) {
            Ok(bytes) => Ok(serde_json::from_slice::<Snapshot>(&bytes).ok()),
            Err(_) => Ok(None),
        }
    }
}

/// Make `tmp` visible as `final_path` only if `final_path` does not exist yet.
/// Hard-linking is create-if-absent on every POSIX filesystem; rename would silently replace.
fn publish_atomically(tmp: &Path, final_path: &Path) -> Result<()> {
    match std::fs::hard_link(tmp, final_path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == ErrorKind::AlreadyExists => Err(RustHouseError::Conflict(format!(
            "{} exists",
            final_path.display()
        ))),
        Err(e) if e.kind() == ErrorKind::Unsupported || e.raw_os_error() == Some(libc_eperm()) => {
            // Filesystem without hard links (some network / DrvFs mounts): fall back to an
            // existence check plus rename. Not race-free, but never overwrites silently.
            if final_path.exists() {
                return Err(RustHouseError::Conflict(format!(
                    "{} exists",
                    final_path.display()
                )));
            }
            std::fs::rename(tmp, final_path)?;
            Ok(())
        }
        Err(e) => Err(RustHouseError::Io(e)),
    }
}

#[cfg(unix)]
fn libc_eperm() -> i32 {
    1
}
#[cfg(not(unix))]
fn libc_eperm() -> i32 {
    -1
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Convenience for tests and callers: an in-memory index of active files by path.
pub fn files_by_path(snapshot: &Snapshot) -> HashMap<&str, &AddFile> {
    snapshot
        .files
        .iter()
        .map(|f| (f.path.as_str(), f))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::governance::classification::{Classification, Level};
    use crate::lake::schema::{Field, Schema};
    use crate::meta::ColumnType;
    use proptest::prelude::*;

    fn md() -> MetaData {
        MetaData::new(
            Schema::new(vec![Field::new(
                "id",
                ColumnType::Int64,
                Classification::new(Level::Internal),
            )]),
            vec![],
        )
    }

    #[test]
    fn test_version_filename_roundtrip() {
        // REQ: LAKE-TXN-001
        for v in [0u64, 1, 42, 10_000, u64::MAX] {
            let name = Version(v).filename();
            assert_eq!(name.len(), 25);
            assert_eq!(Version::parse_filename(&name), Some(Version(v)));
        }
        assert_eq!(Version::parse_filename("1.json"), None);
        assert_eq!(
            Version::parse_filename("00000000000000000001.checkpoint.json"),
            None
        );
        assert_eq!(
            Version::parse_filename("00000000000000000001.json.tmp"),
            None
        );
    }

    #[test]
    fn test_action_serialisation_is_internally_tagged() {
        let a = Action::Remove {
            path: "p".into(),
            deletion_timestamp: 5,
        };
        let v: serde_json::Value = serde_json::to_value(&a).unwrap();
        assert_eq!(v["action"], "remove");
        assert_eq!(v["path"], "p");
        let ci = Action::CommitInfo(CommitInfo::new(
            "a",
            Operation::Write,
            serde_json::json!({}),
        ));
        let v = serde_json::to_value(&ci).unwrap();
        assert_eq!(v["action"], "commitInfo");
        assert_eq!(v["operation"], "WRITE");
    }

    fn base_snapshot() -> Snapshot {
        Snapshot {
            version: Version(0),
            protocol: Protocol {
                min_reader_version: 1,
                min_writer_version: 1,
            },
            metadata: md(),
            files: vec![],
            catalog_entries: vec![],
            timestamp: 0,
        }
    }

    proptest! {
        /// REQ: LAKE-TXN-010 — the fold equals a set-based model of add/remove.
        #[test]
        fn test_fold_actions_property(ops in prop::collection::vec((0u8..2, 0u8..6), 0..40)) {
            let mut snap = base_snapshot();
            let mut model: Vec<String> = Vec::new();
            let mut version = 0u64;
            for (kind, id) in ops {
                let path = format!("p{id}.parquet");
                version += 1;
                let action = if kind == 0 {
                    model.retain(|p| p != &path);
                    model.push(path.clone());
                    Action::Add(AddFile::new(path, 1, 1, FileStats::empty()))
                } else {
                    let present = model.iter().position(|p| p == &path);
                    match present {
                        Some(i) => { model.remove(i); }
                        None => {
                            // Model says invalid: fold must reject and leave state unchanged.
                            let before = snap.clone();
                            let res = snap.apply(Version(version), vec![
                                Action::Remove { path: path.clone(), deletion_timestamp: 0 },
                                Action::CommitInfo(CommitInfo::new("a", Operation::Delete, serde_json::json!({}))),
                            ]);
                            prop_assert!(res.is_err());
                            snap = before;
                            version -= 1;
                            continue;
                        }
                    }
                    Action::Remove { path, deletion_timestamp: 0 }
                };
                snap.apply(Version(version), vec![
                    action,
                    Action::CommitInfo(CommitInfo::new("a", Operation::Write, serde_json::json!({}))),
                ]).unwrap();
                let got: Vec<String> = snap.files.iter().map(|f| f.path.clone()).collect();
                prop_assert_eq!(got, model.clone());
            }
        }
    }

    #[tokio::test]
    async fn test_resolve_as_of_timestamp_boundaries() {
        // REQ: LAKE-TT-003
        let dir = tempfile::tempdir().unwrap();
        let log = TransactionLog::create(dir.path(), "a", md()).await.unwrap();
        let t0 = log.history(Version(0), Version(0)).await.unwrap()[0].timestamp as i64;
        // Rewrite timestamps deterministically by committing with explicit CommitInfo.
        let mut info = CommitInfo::new("a", Operation::Write, serde_json::json!({}));
        info.timestamp = (t0 + 10_000) as u64;
        log.commit(Commit::new(
            info,
            vec![Action::Add(AddFile::new("a", 1, 1, FileStats::empty()))],
        ))
        .await
        .unwrap();
        let mut info = CommitInfo::new("a", Operation::Write, serde_json::json!({}));
        info.timestamp = (t0 + 20_000) as u64;
        log.commit(Commit::new(
            info,
            vec![Action::Add(AddFile::new("b", 1, 1, FileStats::empty()))],
        ))
        .await
        .unwrap();

        assert_eq!(
            log.resolve_as_of(&AsOf::Timestamp(t0 + 15_000))
                .await
                .unwrap(),
            Version(1)
        );
        assert_eq!(
            log.resolve_as_of(&AsOf::Timestamp(t0 + 20_000))
                .await
                .unwrap(),
            Version(2)
        );
        assert_eq!(
            log.resolve_as_of(&AsOf::Timestamp(t0)).await.unwrap(),
            Version(0)
        );
        assert!(matches!(
            log.resolve_as_of(&AsOf::Timestamp(t0 - 1)).await,
            Err(RustHouseError::VersionNotFound(_))
        ));
        assert!(matches!(
            log.resolve_as_of(&AsOf::Version(7)).await,
            Err(RustHouseError::VersionNotFound(7))
        ));
    }
}
