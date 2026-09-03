//! Catalog: namespaces, table registry, and governance objects, persisted as an rhlog table
//! at `<data_dir>/catalog/_rhlog/` (spec LAKE-CAT, ADR-0002).
//!
//! Built-in namespaces `bronze`, `silver`, `gold` exist implicitly. Every mutation is one
//! catalog commit with a mandatory principal. Table creation stages the table's own log in a
//! temporary directory, commits the catalog entry, then publishes the directory; `open`
//! repairs a crash between those two steps.

use crate::error::{Result, RustHouseError};
use crate::governance::access::{Grant, MaskKind, MaskingPolicy, PolicySet, Privilege, RowPolicy};
use crate::governance::classification::{Classification, Level};
use crate::lake::log::{
    Action, Commit, CommitInfo, MetaData, Operation, Snapshot, TransactionLog, Version,
};
use crate::lake::schema::{Field, Schema};
use crate::meta::{Column, ColumnType};
use crate::storage::ColumnData;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use uuid::Uuid;

pub const CATALOG_DIR: &str = "catalog";
pub const BUILTIN_NAMESPACES: [&str; 3] = ["bronze", "silver", "gold"];
const CATALOG_PRINCIPAL: &str = "system";

/// Authenticated identity (API-SQL-002). Roles and attributes are consumed by GOV-ACC.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Principal {
    pub id: String,
    #[serde(default)]
    pub roles: Vec<String>,
    #[serde(default)]
    pub attributes: BTreeMap<String, String>,
}

impl Principal {
    pub fn new(id: impl Into<String>, roles: Vec<String>) -> Self {
        Self {
            id: id.into(),
            roles,
            attributes: BTreeMap::new(),
        }
    }

    pub fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|r| r == role)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRef {
    pub namespace: String,
    pub name: String,
    pub table_id: Uuid,
    pub path: PathBuf,
}

impl TableRef {
    pub fn qualified(&self) -> String {
        format!("{}.{}", self.namespace, self.name)
    }
}

#[derive(Debug, Clone)]
pub struct TableSpec {
    pub namespace: String,
    pub name: String,
    pub schema: Schema,
    pub partition_by: Vec<String>,
}

impl TableSpec {
    pub fn new(namespace: impl Into<String>, name: impl Into<String>, schema: Schema) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
            schema,
            partition_by: Vec::new(),
        }
    }
}

/// One row of the catalog log (`Action::Catalog { entry }`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CatalogEntry {
    CreateNamespace {
        name: String,
        owner: String,
        created_at: u64,
    },
    CreateTable {
        namespace: String,
        name: String,
        table_id: Uuid,
        owner: String,
        created_at: u64,
        schema: Schema,
        #[serde(default)]
        partition_by: Vec<String>,
    },
    DropTable {
        namespace: String,
        name: String,
        table_id: Uuid,
        dropped_at: u64,
    },
    /// Schema/classification change mirrored from the table's own log (GOV-CLS-005, LAKE-SCH).
    AlterTableSchema {
        namespace: String,
        name: String,
        table_id: Uuid,
        schema: Schema,
    },
    // --- GOV-ACC policy objects (versioned like tables) ---
    Grant {
        role: String,
        namespace: String,
        name: String,
        privilege: Privilege,
        granted_by: String,
    },
    Revoke {
        role: String,
        namespace: String,
        name: String,
        privilege: Privilege,
    },
    RowPolicy {
        policy: String,
        namespace: String,
        name: String,
        role: String,
        predicate_sql: String,
        created_by: String,
    },
    MaskingPolicy {
        policy: String,
        namespace: String,
        name: String,
        column: String,
        role: String,
        mask_kind: MaskKind,
        created_by: String,
    },
    DropPolicy {
        policy: String,
        namespace: String,
        name: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct NamespaceEntry {
    pub name: String,
    pub owner: String,
    pub created_at: u64,
    pub builtin: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableEntry {
    pub namespace: String,
    pub name: String,
    pub table_id: Uuid,
    pub owner: String,
    pub created_at: u64,
    pub schema: Schema,
    pub partition_by: Vec<String>,
    /// Catalog version at which the table was created.
    pub created_version: u64,
    /// Catalog version at which the classification/schema last changed.
    pub last_changed_version: u64,
    pub dropped_at_version: Option<u64>,
}

impl TableEntry {
    pub fn is_live(&self) -> bool {
        self.dropped_at_version.is_none()
    }
}

/// Folded catalog state at a catalog version (LAKE-CAT-007: readers see one whole snapshot).
#[derive(Debug, Clone, PartialEq)]
pub struct CatalogSnapshot {
    pub version: u64,
    pub namespaces: BTreeMap<String, NamespaceEntry>,
    /// Live tables keyed by `(namespace, name)`.
    pub tables: BTreeMap<(String, String), TableEntry>,
    /// Every table ever created (including dropped), for time travel.
    pub history: Vec<TableEntry>,
    /// Live grants, row policies and masking policies (GOV-ACC).
    pub grants: Vec<Grant>,
    pub row_policies: Vec<RowPolicy>,
    pub masking_policies: Vec<MaskingPolicy>,
}

impl CatalogSnapshot {
    fn from_log(snap: &Snapshot) -> Result<Self> {
        let mut namespaces = BTreeMap::new();
        for ns in BUILTIN_NAMESPACES {
            namespaces.insert(
                ns.to_string(),
                NamespaceEntry {
                    name: ns.to_string(),
                    owner: CATALOG_PRINCIPAL.to_string(),
                    created_at: 0,
                    builtin: true,
                },
            );
        }
        let mut s = Self {
            version: snap.version.0,
            namespaces,
            tables: BTreeMap::new(),
            history: Vec::new(),
            grants: Vec::new(),
            row_policies: Vec::new(),
            masking_policies: Vec::new(),
        };
        // Entries carry the version they were committed at via their position; the log fold
        // keeps commit order, and each catalog commit carries exactly one entry.
        for (i, raw) in snap.catalog_entries.iter().enumerate() {
            let version = (i + 1) as u64;
            let entry: CatalogEntry = serde_json::from_value(raw.clone())
                .map_err(|e| RustHouseError::CorruptLog(format!("catalog entry {version}: {e}")))?;
            s.apply(entry, version);
        }
        Ok(s)
    }

    fn apply(&mut self, entry: CatalogEntry, version: u64) {
        match entry {
            CatalogEntry::CreateNamespace {
                name,
                owner,
                created_at,
            } => {
                self.namespaces.insert(
                    name.clone(),
                    NamespaceEntry {
                        name,
                        owner,
                        created_at,
                        builtin: false,
                    },
                );
            }
            CatalogEntry::CreateTable {
                namespace,
                name,
                table_id,
                owner,
                created_at,
                schema,
                partition_by,
            } => {
                let e = TableEntry {
                    namespace: namespace.clone(),
                    name: name.clone(),
                    table_id,
                    owner,
                    created_at,
                    schema,
                    partition_by,
                    created_version: version,
                    last_changed_version: version,
                    dropped_at_version: None,
                };
                self.history.push(e.clone());
                self.tables.insert((namespace, name), e);
            }
            CatalogEntry::AlterTableSchema {
                namespace,
                name,
                table_id,
                schema,
            } => {
                if let Some(t) = self.tables.get_mut(&(namespace, name)) {
                    if t.table_id == table_id {
                        t.schema = schema;
                        t.last_changed_version = version;
                    }
                }
            }
            CatalogEntry::Grant {
                role,
                namespace,
                name,
                privilege,
                granted_by,
            } => {
                self.grants.retain(|g| {
                    !(g.role == role
                        && g.namespace == namespace
                        && g.table == name
                        && g.privilege == privilege)
                });
                self.grants.push(Grant {
                    role,
                    namespace,
                    table: name,
                    privilege,
                    granted_by,
                    version,
                });
            }
            CatalogEntry::Revoke {
                role,
                namespace,
                name,
                privilege,
            } => {
                self.grants.retain(|g| {
                    !(g.role == role
                        && g.namespace == namespace
                        && g.table == name
                        && g.privilege == privilege)
                });
            }
            CatalogEntry::RowPolicy {
                policy,
                namespace,
                name,
                role,
                predicate_sql,
                created_by,
            } => {
                self.row_policies
                    .retain(|p| !(p.name == policy && p.namespace == namespace && p.table == name));
                self.row_policies.push(RowPolicy {
                    name: policy,
                    namespace,
                    table: name,
                    role,
                    predicate_sql,
                    created_by,
                    version,
                });
            }
            CatalogEntry::MaskingPolicy {
                policy,
                namespace,
                name,
                column,
                role,
                mask_kind,
                created_by,
            } => {
                self.masking_policies
                    .retain(|p| !(p.name == policy && p.namespace == namespace && p.table == name));
                self.masking_policies.push(MaskingPolicy {
                    name: policy,
                    namespace,
                    table: name,
                    column,
                    role,
                    kind: mask_kind,
                    created_by,
                    version,
                });
            }
            CatalogEntry::DropPolicy {
                policy,
                namespace,
                name,
            } => {
                self.row_policies
                    .retain(|p| !(p.name == policy && p.namespace == namespace && p.table == name));
                self.masking_policies
                    .retain(|p| !(p.name == policy && p.namespace == namespace && p.table == name));
            }
            CatalogEntry::DropTable {
                namespace,
                name,
                table_id,
                ..
            } => {
                self.tables.remove(&(namespace, name));
                if let Some(h) = self
                    .history
                    .iter_mut()
                    .rev()
                    .find(|t| t.table_id == table_id)
                {
                    h.dropped_at_version = Some(version);
                }
            }
        }
    }

    /// Table visible at exactly this snapshot's version.
    fn table(&self, namespace: &str, name: &str) -> Option<&TableEntry> {
        self.tables.get(&(namespace.to_string(), name.to_string()))
    }

    /// Policies attached to one table at this snapshot's version (GOV-ACC-008).
    pub fn policies_on(&self, namespace: &str, name: &str) -> PolicySet {
        PolicySet {
            grants: self
                .grants
                .iter()
                .filter(|g| g.namespace == namespace && g.table == name)
                .cloned()
                .collect(),
            row_policies: self
                .row_policies
                .iter()
                .filter(|p| p.namespace == namespace && p.table == name)
                .cloned()
                .collect(),
            masking_policies: self
                .masking_policies
                .iter()
                .filter(|p| p.namespace == namespace && p.table == name)
                .cloned()
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableSummary {
    pub namespace: String,
    pub name: String,
    pub table_id: Uuid,
    pub owner: String,
    pub created_at: u64,
    pub classification: Classification,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableDescription {
    pub table_id: Uuid,
    pub namespace: String,
    pub name: String,
    pub owner: String,
    pub created_at: u64,
    pub current_version: Version,
    pub schema: Schema,
    pub classification: Classification,
    pub partition_by: Vec<String>,
}

/// Columnar rows returned by a system table (LAKE-CAT-010).
#[derive(Debug, Clone)]
pub struct SystemRows {
    pub columns: Vec<Column>,
    pub data: HashMap<String, ColumnData>,
}

impl SystemRows {
    pub fn num_rows(&self) -> usize {
        self.data.values().next().map(|c| c.len()).unwrap_or(0)
    }
}

pub const SYSTEM_TABLES: [&str; 5] = [
    "catalog.tables",
    "catalog.columns",
    "catalog.grants",
    "catalog.policies",
    "catalog.lineage",
];

/// Test hook for LAKE-CAT-003 (all-or-nothing table creation).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultPoint {
    BeforeCatalogCommit = 1,
    AfterCatalogCommit = 2,
}

pub struct Catalog {
    root: PathBuf,
    log: TransactionLog,
    cache: RwLock<Arc<CatalogSnapshot>>,
    fault: AtomicU8,
}

impl std::fmt::Debug for Catalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Catalog")
            .field("root", &self.root)
            .field("version", &self.version())
            .finish()
    }
}

fn catalog_schema() -> Schema {
    let internal = || Classification::new(Level::Internal);
    Schema::new(vec![
        Field::new("kind", ColumnType::String, internal()),
        Field::new("namespace", ColumnType::String, internal()),
        Field::new("name", ColumnType::String, internal()),
        Field::new("table_id", ColumnType::String, internal()),
        Field::new("owner", ColumnType::String, internal()),
        Field::new("created_at", ColumnType::Int64, internal()),
    ])
}

/// `[a-z][a-z0-9_]{0,62}` (LAKE-CAT-002).
pub fn validate_identifier(s: &str) -> Result<()> {
    let mut chars = s.chars();
    let ok_first = matches!(chars.next(), Some(c) if c.is_ascii_lowercase());
    let ok_rest = chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if ok_first && ok_rest && s.len() <= 63 {
        Ok(())
    } else {
        Err(RustHouseError::InvalidIdentifier(format!(
            "'{s}' must match [a-z][a-z0-9_]{{0,62}}"
        )))
    }
}

/// Split `ns.table` / bare `table` (LAKE-CAT-005).
pub fn parse_qualified(qualified: &str, default_ns: Option<&str>) -> Result<(String, String)> {
    let parts: Vec<&str> = qualified.split('.').collect();
    match parts.as_slice() {
        [ns, name] => Ok((ns.to_string(), name.to_string())),
        [name] => match default_ns {
            Some(ns) => Ok((ns.to_string(), name.to_string())),
            None => Err(RustHouseError::AmbiguousTable(format!(
                "'{name}' has no namespace and no default namespace is set"
            ))),
        },
        _ => Err(RustHouseError::InvalidIdentifier(format!(
            "'{qualified}' is not of the form namespace.table"
        ))),
    }
}

impl Catalog {
    /// Open (or initialise) the catalog under `root` and rebuild state from its log only
    /// (LAKE-CAT-001, LAKE-CAT-012).
    pub async fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let dir = root.join(CATALOG_DIR);
        std::fs::create_dir_all(&dir)?;
        let log = match TransactionLog::open(&dir).await {
            Ok(l) => l,
            Err(RustHouseError::TableNotFound(_)) => {
                TransactionLog::create(
                    &dir,
                    CATALOG_PRINCIPAL,
                    MetaData::new(catalog_schema(), vec![]),
                )
                .await?
            }
            Err(e) => return Err(e),
        };
        let snap = CatalogSnapshot::from_log(&log.snapshot(None).await?)?;
        let cat = Self {
            root,
            log,
            cache: RwLock::new(Arc::new(snap)),
            fault: AtomicU8::new(0),
        };
        cat.repair_staged_tables()?;
        Ok(cat)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn version(&self) -> u64 {
        self.cache.read().version
    }

    /// Consistent view of the whole catalog (LAKE-CAT-007, LAKE-CAT-013).
    pub fn snapshot(&self) -> Arc<CatalogSnapshot> {
        self.cache.read().clone()
    }

    pub fn list_namespaces(&self) -> Vec<String> {
        let snap = self.snapshot();
        let mut out: Vec<String> = BUILTIN_NAMESPACES.iter().map(|s| s.to_string()).collect();
        out.extend(
            snap.namespaces
                .values()
                .filter(|n| !n.builtin)
                .map(|n| n.name.clone()),
        );
        out
    }

    pub async fn create_namespace(&self, principal: &Principal, name: &str) -> Result<u64> {
        validate_identifier(name)?;
        if self.snapshot().namespaces.contains_key(name) {
            return Err(RustHouseError::NamespaceExists(name.to_string()));
        }
        let entry = CatalogEntry::CreateNamespace {
            name: name.to_string(),
            owner: principal.id.clone(),
            created_at: crate::lake::log::now_ms(),
        };
        self.commit_entry(principal, Operation::CreateNamespace, entry, |snap| {
            if snap.namespaces.contains_key(name) {
                Err(RustHouseError::NamespaceExists(name.to_string()))
            } else {
                Ok(())
            }
        })
        .await
    }

    /// Register a table and create its log, all-or-nothing (LAKE-CAT-003, -006, -011).
    pub async fn create_table(&self, principal: &Principal, spec: TableSpec) -> Result<TableRef> {
        if principal.id.is_empty() {
            return Err(RustHouseError::MissingPrincipal);
        }
        validate_identifier(&spec.namespace)?;
        validate_identifier(&spec.name)?;
        spec.schema.require_classified()?;
        {
            let snap = self.snapshot();
            if !snap.namespaces.contains_key(&spec.namespace) {
                return Err(RustHouseError::NamespaceNotFound(spec.namespace.clone()));
            }
            if snap.table(&spec.namespace, &spec.name).is_some() {
                return Err(RustHouseError::TableExists(format!(
                    "{}.{}",
                    spec.namespace, spec.name
                )));
            }
        }

        let table_id = Uuid::new_v4();
        let final_dir = self.table_dir(&spec.namespace, &spec.name);
        let staging_dir = self.staging_dir(&spec.namespace, &spec.name, table_id);
        if final_dir.exists() {
            return Err(RustHouseError::TableExists(format!(
                "directory {} already exists",
                final_dir.display()
            )));
        }

        // (1) Stage the table's own log (version 0) where no reader looks.
        std::fs::create_dir_all(&staging_dir)?;
        let metadata = MetaData {
            id: table_id,
            ..MetaData::new(spec.schema.clone(), spec.partition_by.clone())
        };
        let staged = TransactionLog::create(&staging_dir, &principal.id, metadata).await;
        if let Err(e) = staged {
            let _ = std::fs::remove_dir_all(&staging_dir);
            return Err(e);
        }

        if self.fault.load(Ordering::Relaxed) == FaultPoint::BeforeCatalogCommit as u8 {
            let _ = std::fs::remove_dir_all(&staging_dir);
            return Err(RustHouseError::Internal(
                "injected fault before catalog commit".into(),
            ));
        }

        // (2) Commit the catalog entry; re-check uniqueness against the snapshot we land on.
        let entry = CatalogEntry::CreateTable {
            namespace: spec.namespace.clone(),
            name: spec.name.clone(),
            table_id,
            owner: principal.id.clone(),
            created_at: crate::lake::log::now_ms(),
            schema: spec.schema.clone(),
            partition_by: spec.partition_by.clone(),
        };
        let ns = spec.namespace.clone();
        let name = spec.name.clone();
        let committed = self
            .commit_entry(principal, Operation::CreateTable, entry, move |snap| {
                if snap.table(&ns, &name).is_some() {
                    Err(RustHouseError::TableExists(format!("{ns}.{name}")))
                } else {
                    Ok(())
                }
            })
            .await;
        if let Err(e) = committed {
            let _ = std::fs::remove_dir_all(&staging_dir);
            return Err(e);
        }

        if self.fault.load(Ordering::Relaxed) == FaultPoint::AfterCatalogCommit as u8 {
            return Err(RustHouseError::Internal(
                "injected fault after catalog commit".into(),
            ));
        }

        // (3) Publish. A crash before this point is repaired by `open` from the catalog entry.
        std::fs::create_dir_all(final_dir.parent().unwrap())?;
        std::fs::rename(&staging_dir, &final_dir)?;

        Ok(TableRef {
            namespace: spec.namespace,
            name: spec.name,
            table_id,
            path: final_dir,
        })
    }

    /// Logical drop (LAKE-CAT-008): data files stay; the entry is hidden from this version on.
    pub async fn drop_table(&self, principal: &Principal, table: &TableRef) -> Result<u64> {
        let entry = CatalogEntry::DropTable {
            namespace: table.namespace.clone(),
            name: table.name.clone(),
            table_id: table.table_id,
            dropped_at: crate::lake::log::now_ms(),
        };
        let (ns, name, id) = (table.namespace.clone(), table.name.clone(), table.table_id);
        self.commit_entry(
            principal,
            Operation::DropTable,
            entry,
            move |snap| match snap.table(&ns, &name) {
                Some(t) if t.table_id == id => Ok(()),
                _ => Err(RustHouseError::TableNotFound(format!("{ns}.{name}"))),
            },
        )
        .await
    }

    /// Mirror a table's new schema into the catalog after its own log committed it.
    pub async fn update_schema(
        &self,
        principal: &Principal,
        table: &TableRef,
        schema: Schema,
    ) -> Result<u64> {
        schema.require_classified()?;
        let entry = CatalogEntry::AlterTableSchema {
            namespace: table.namespace.clone(),
            name: table.name.clone(),
            table_id: table.table_id,
            schema,
        };
        let (ns, name, id) = (table.namespace.clone(), table.name.clone(), table.table_id);
        self.commit_entry(principal, Operation::Alter, entry, move |snap| {
            match snap.table(&ns, &name) {
                Some(t) if t.table_id == id => Ok(()),
                _ => Err(RustHouseError::TableNotFound(format!("{ns}.{name}"))),
            }
        })
        .await
    }

    // ----- GOV-ACC policy objects --------------------------------------------------------

    pub fn policies_on(&self, table: &TableRef) -> PolicySet {
        self.snapshot().policies_on(&table.namespace, &table.name)
    }

    pub async fn policies_on_as_of(
        &self,
        table: &TableRef,
        catalog_version: u64,
    ) -> Result<PolicySet> {
        let log_snap = self.log.snapshot(Some(Version(catalog_version))).await?;
        Ok(CatalogSnapshot::from_log(&log_snap)?.policies_on(&table.namespace, &table.name))
    }

    pub async fn grant(
        &self,
        principal: &Principal,
        table: &TableRef,
        role: &str,
        privilege: Privilege,
    ) -> Result<u64> {
        validate_identifier(role)?;
        let entry = CatalogEntry::Grant {
            role: role.to_string(),
            namespace: table.namespace.clone(),
            name: table.name.clone(),
            privilege,
            granted_by: principal.id.clone(),
        };
        self.commit_policy_entry(principal, Operation::Grant, table, entry)
            .await
    }

    pub async fn revoke(
        &self,
        principal: &Principal,
        table: &TableRef,
        role: &str,
        privilege: Privilege,
    ) -> Result<u64> {
        let entry = CatalogEntry::Revoke {
            role: role.to_string(),
            namespace: table.namespace.clone(),
            name: table.name.clone(),
            privilege,
        };
        self.commit_policy_entry(principal, Operation::Revoke, table, entry)
            .await
    }

    pub async fn create_row_policy(
        &self,
        principal: &Principal,
        table: &TableRef,
        policy: &str,
        role: &str,
        predicate_sql: &str,
    ) -> Result<u64> {
        validate_identifier(policy)?;
        validate_identifier(role)?;
        let entry = CatalogEntry::RowPolicy {
            policy: policy.to_string(),
            namespace: table.namespace.clone(),
            name: table.name.clone(),
            role: role.to_string(),
            predicate_sql: predicate_sql.to_string(),
            created_by: principal.id.clone(),
        };
        self.commit_policy_entry(principal, Operation::Policy, table, entry)
            .await
    }

    pub async fn create_masking_policy(
        &self,
        principal: &Principal,
        table: &TableRef,
        policy: &str,
        column: &str,
        role: &str,
        kind: MaskKind,
    ) -> Result<u64> {
        validate_identifier(policy)?;
        validate_identifier(role)?;
        let entry = self.entry(table)?;
        if entry.schema.field(column).is_none() {
            return Err(RustHouseError::ColumnNotFound(column.to_string()));
        }
        let e = CatalogEntry::MaskingPolicy {
            policy: policy.to_string(),
            namespace: table.namespace.clone(),
            name: table.name.clone(),
            column: column.to_string(),
            role: role.to_string(),
            mask_kind: kind,
            created_by: principal.id.clone(),
        };
        self.commit_policy_entry(principal, Operation::Policy, table, e)
            .await
    }

    pub async fn drop_policy(
        &self,
        principal: &Principal,
        table: &TableRef,
        policy: &str,
    ) -> Result<u64> {
        let ps = self.policies_on(table);
        let exists = ps.row_policies.iter().any(|p| p.name == policy)
            || ps.masking_policies.iter().any(|p| p.name == policy);
        if !exists {
            return Err(RustHouseError::InvalidData(format!(
                "no policy '{policy}' on {}",
                table.qualified()
            )));
        }
        let entry = CatalogEntry::DropPolicy {
            policy: policy.to_string(),
            namespace: table.namespace.clone(),
            name: table.name.clone(),
        };
        self.commit_policy_entry(principal, Operation::Policy, table, entry)
            .await
    }

    async fn commit_policy_entry(
        &self,
        principal: &Principal,
        operation: Operation,
        table: &TableRef,
        entry: CatalogEntry,
    ) -> Result<u64> {
        let (ns, name, id) = (table.namespace.clone(), table.name.clone(), table.table_id);
        self.commit_entry(principal, operation, entry, move |snap| {
            match snap.table(&ns, &name) {
                Some(t) if t.table_id == id => Ok(()),
                _ => Err(RustHouseError::TableNotFound(format!("{ns}.{name}"))),
            }
        })
        .await
    }

    /// Read-only resolution at the current catalog version (LAKE-CAT-004, -005).
    pub fn resolve(&self, qualified: &str, default_ns: Option<&str>) -> Result<TableRef> {
        let (ns, name) = parse_qualified(qualified, default_ns)?;
        let snap = self.snapshot();
        self.resolve_in(&snap, &ns, &name)
    }

    /// Resolution against the catalog as it was at `catalog_version` (LAKE-CAT-008, LAKE-TT-012).
    pub async fn resolve_as_of(
        &self,
        qualified: &str,
        default_ns: Option<&str>,
        catalog_version: u64,
    ) -> Result<TableRef> {
        let (ns, name) = parse_qualified(qualified, default_ns)?;
        let log_snap = self.log.snapshot(Some(Version(catalog_version))).await?;
        let snap = CatalogSnapshot::from_log(&log_snap)?;
        self.resolve_in(&snap, &ns, &name)
    }

    fn resolve_in(&self, snap: &CatalogSnapshot, ns: &str, name: &str) -> Result<TableRef> {
        if !snap.namespaces.contains_key(ns) {
            return Err(RustHouseError::NamespaceNotFound(ns.to_string()));
        }
        let t = snap
            .table(ns, name)
            .ok_or_else(|| RustHouseError::TableNotFound(format!("{ns}.{name}")))?;
        Ok(TableRef {
            namespace: t.namespace.clone(),
            name: t.name.clone(),
            table_id: t.table_id,
            path: self.table_dir(&t.namespace, &t.name),
        })
    }

    pub fn list_tables(&self, namespace: &str) -> Result<Vec<TableSummary>> {
        let snap = self.snapshot();
        if !snap.namespaces.contains_key(namespace) {
            return Err(RustHouseError::NamespaceNotFound(namespace.to_string()));
        }
        snap.tables
            .values()
            .filter(|t| t.namespace == namespace)
            .map(|t| {
                Ok(TableSummary {
                    namespace: t.namespace.clone(),
                    name: t.name.clone(),
                    table_id: t.table_id,
                    owner: t.owner.clone(),
                    created_at: t.created_at,
                    classification: t.schema.classification()?,
                })
            })
            .collect()
    }

    pub fn entry(&self, table: &TableRef) -> Result<TableEntry> {
        self.snapshot()
            .table(&table.namespace, &table.name)
            .filter(|t| t.table_id == table.table_id)
            .cloned()
            .ok_or_else(|| RustHouseError::TableNotFound(table.qualified()))
    }

    pub async fn describe(&self, table: &TableRef) -> Result<TableDescription> {
        let e = self.entry(table)?;
        let log = self.open_table_log(table).await?;
        Ok(TableDescription {
            table_id: e.table_id,
            namespace: e.namespace,
            name: e.name,
            owner: e.owner,
            created_at: e.created_at,
            current_version: log.head().await?,
            classification: e.schema.classification()?,
            schema: e.schema,
            partition_by: e.partition_by,
        })
    }

    pub async fn open_table_log(&self, table: &TableRef) -> Result<TransactionLog> {
        TransactionLog::open(&table.path).await
    }

    /// System tables as columnar rows (LAKE-CAT-010). Governance tables are empty until GOV-ACC/GOV-LIN.
    pub fn system_table(&self, qualified: &str) -> Result<SystemRows> {
        let snap = self.snapshot();
        let str_col = |name: &str| Column {
            name: name.to_string(),
            column_type: ColumnType::String,
        };
        let int_col = |name: &str| Column {
            name: name.to_string(),
            column_type: ColumnType::Int64,
        };
        match qualified {
            "catalog.tables" => {
                let columns = vec![
                    str_col("namespace"),
                    str_col("name"),
                    str_col("table_id"),
                    str_col("classification"),
                    str_col("owner"),
                    int_col("created_at"),
                ];
                let mut data: HashMap<String, ColumnData> = HashMap::new();
                let mut ns = vec![];
                let mut name = vec![];
                let mut id = vec![];
                let mut cls = vec![];
                let mut owner = vec![];
                let mut created = vec![];
                for t in snap.tables.values() {
                    ns.push(t.namespace.clone());
                    name.push(t.name.clone());
                    id.push(t.table_id.to_string());
                    cls.push(t.schema.classification()?.to_string());
                    owner.push(t.owner.clone());
                    created.push(t.created_at as i64);
                }
                data.insert("namespace".into(), ColumnData::String(ns));
                data.insert("name".into(), ColumnData::String(name));
                data.insert("table_id".into(), ColumnData::String(id));
                data.insert("classification".into(), ColumnData::String(cls));
                data.insert("owner".into(), ColumnData::String(owner));
                data.insert("created_at".into(), ColumnData::Int64(created));
                Ok(SystemRows { columns, data })
            }
            "catalog.columns" => {
                let columns = vec![
                    str_col("namespace"),
                    str_col("table"),
                    str_col("column"),
                    int_col("column_id"),
                    str_col("column_type"),
                    str_col("level"),
                    str_col("tags"),
                    int_col("last_changed_version"),
                ];
                let mut ns = vec![];
                let mut table = vec![];
                let mut col = vec![];
                let mut cid = vec![];
                let mut ty = vec![];
                let mut level = vec![];
                let mut tags = vec![];
                let mut changed = vec![];
                for t in snap.tables.values() {
                    for f in &t.schema.fields {
                        let c = f.classification()?;
                        ns.push(t.namespace.clone());
                        table.push(t.name.clone());
                        col.push(f.name.clone());
                        cid.push(f.id as i64);
                        ty.push(format!("{:?}", f.column_type));
                        level.push(c.level.to_string());
                        tags.push(
                            c.tags
                                .iter()
                                .map(|t| t.to_string())
                                .collect::<Vec<_>>()
                                .join(","),
                        );
                        changed.push(t.last_changed_version as i64);
                    }
                }
                let mut data: HashMap<String, ColumnData> = HashMap::new();
                data.insert("namespace".into(), ColumnData::String(ns));
                data.insert("table".into(), ColumnData::String(table));
                data.insert("column".into(), ColumnData::String(col));
                data.insert("column_id".into(), ColumnData::Int64(cid));
                data.insert("column_type".into(), ColumnData::String(ty));
                data.insert("level".into(), ColumnData::String(level));
                data.insert("tags".into(), ColumnData::String(tags));
                data.insert("last_changed_version".into(), ColumnData::Int64(changed));
                Ok(SystemRows { columns, data })
            }
            "catalog.grants" | "catalog.policies" => {
                // `catalog.policies` is the union view (grants + row + masking policies);
                // `catalog.grants` lists grants only.
                let columns = vec![
                    str_col("kind"),
                    str_col("name"),
                    str_col("object"),
                    str_col("role"),
                    str_col("detail"),
                    str_col("created_by"),
                    int_col("version"),
                ];
                let mut kind = vec![];
                let mut name = vec![];
                let mut object = vec![];
                let mut role = vec![];
                let mut detail = vec![];
                let mut by = vec![];
                let mut ver = vec![];
                for g in &snap.grants {
                    kind.push("grant".to_string());
                    name.push(String::new());
                    object.push(format!("{}.{}", g.namespace, g.table));
                    role.push(g.role.clone());
                    detail.push(g.privilege.to_string());
                    by.push(g.granted_by.clone());
                    ver.push(g.version as i64);
                }
                if qualified == "catalog.policies" {
                    for p in &snap.row_policies {
                        kind.push("row_policy".to_string());
                        name.push(p.name.clone());
                        object.push(format!("{}.{}", p.namespace, p.table));
                        role.push(p.role.clone());
                        detail.push(p.predicate_sql.clone());
                        by.push(p.created_by.clone());
                        ver.push(p.version as i64);
                    }
                    for p in &snap.masking_policies {
                        kind.push("masking_policy".to_string());
                        name.push(p.name.clone());
                        object.push(format!("{}.{}.{}", p.namespace, p.table, p.column));
                        role.push(p.role.clone());
                        detail.push(p.kind.as_str().to_string());
                        by.push(p.created_by.clone());
                        ver.push(p.version as i64);
                    }
                }
                let mut data: HashMap<String, ColumnData> = HashMap::new();
                data.insert("kind".into(), ColumnData::String(kind));
                data.insert("name".into(), ColumnData::String(name));
                data.insert("object".into(), ColumnData::String(object));
                data.insert("role".into(), ColumnData::String(role));
                data.insert("detail".into(), ColumnData::String(detail));
                data.insert("created_by".into(), ColumnData::String(by));
                data.insert("version".into(), ColumnData::Int64(ver));
                Ok(SystemRows { columns, data })
            }
            "catalog.lineage" => {
                let columns = vec![str_col("object"), str_col("principal"), str_col("detail")];
                let mut data: HashMap<String, ColumnData> = HashMap::new();
                for c in &columns {
                    data.insert(c.name.clone(), ColumnData::String(vec![]));
                }
                Ok(SystemRows { columns, data })
            }
            other => Err(RustHouseError::TableNotFound(other.to_string())),
        }
    }

    /// Test hook (LAKE-CAT-003).
    pub fn inject_fault(&self, point: Option<FaultPoint>) {
        self.fault
            .store(point.map(|p| p as u8).unwrap_or(0), Ordering::Relaxed);
    }

    // ----- internals -------------------------------------------------------------------

    fn table_dir(&self, ns: &str, name: &str) -> PathBuf {
        self.root.join(ns).join(name)
    }

    fn staging_dir(&self, ns: &str, name: &str, id: Uuid) -> PathBuf {
        self.root
            .join(ns)
            .join(format!(".staging-{name}-{}", id.simple()))
    }

    /// Commit one catalog entry. `check` re-validates against the snapshot the commit lands on
    /// (optimistic concurrency, LAKE-CAT-007), then the cache is refreshed.
    async fn commit_entry<F>(
        &self,
        principal: &Principal,
        operation: Operation,
        entry: CatalogEntry,
        check: F,
    ) -> Result<u64>
    where
        F: Fn(&CatalogSnapshot) -> Result<()>,
    {
        if principal.id.is_empty() {
            return Err(RustHouseError::MissingPrincipal);
        }
        let value = serde_json::to_value(&entry)?;
        let mut attempts = 0u32;
        loop {
            let base = self.log.head().await?;
            let base_snap = CatalogSnapshot::from_log(&self.log.snapshot(Some(base)).await?)?;
            check(&base_snap)?;
            let commit = Commit::new(
                CommitInfo::new(&principal.id, operation, serde_json::json!({})),
                vec![Action::Catalog {
                    entry: value.clone(),
                }],
            );
            // Publish only at base+1 so `check` stays valid for the snapshot we extend.
            let mut single = self.log.options().clone();
            single.max_retries = 0;
            let one_shot = TransactionLog::open_with(self.log.table_dir(), single).await?;
            match one_shot.commit_at(base.next(), commit).await {
                Ok(v) => {
                    self.refresh().await?;
                    return Ok(v.0);
                }
                Err(RustHouseError::CommitConflict { .. }) => {
                    attempts += 1;
                    if attempts > 50 {
                        return Err(RustHouseError::Conflict(
                            "catalog commit retries exhausted".into(),
                        ));
                    }
                    tokio::task::yield_now().await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn refresh(&self) -> Result<()> {
        let snap = CatalogSnapshot::from_log(&self.log.snapshot(None).await?)?;
        let mut guard = self.cache.write();
        if snap.version >= guard.version {
            *guard = Arc::new(snap);
        }
        Ok(())
    }

    /// Complete table creations that committed to the catalog but crashed before publishing
    /// their staged directory (LAKE-CAT-003).
    fn repair_staged_tables(&self) -> Result<()> {
        let snap = self.snapshot();
        for t in snap.tables.values() {
            let final_dir = self.table_dir(&t.namespace, &t.name);
            if final_dir.join(crate::lake::log::LOG_DIR).exists() {
                continue;
            }
            let staging = self.staging_dir(&t.namespace, &t.name, t.table_id);
            if staging.join(crate::lake::log::LOG_DIR).exists() {
                std::fs::create_dir_all(final_dir.parent().unwrap())?;
                std::fs::rename(&staging, &final_dir)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_namespace_identifier_validation() {
        // REQ: LAKE-CAT-002
        for ok in ["a", "risk", "gold_2026", &"a".repeat(63)] {
            validate_identifier(ok).unwrap();
        }
        for bad in ["", "Risk", "9x", "a-b", "a.b", &"a".repeat(64)] {
            assert!(validate_identifier(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn test_parse_qualified() {
        assert_eq!(
            parse_qualified("gold.t", None).unwrap(),
            ("gold".into(), "t".into())
        );
        assert_eq!(
            parse_qualified("t", Some("silver")).unwrap(),
            ("silver".into(), "t".into())
        );
        assert!(matches!(
            parse_qualified("t", None),
            Err(RustHouseError::AmbiguousTable(_))
        ));
        assert!(matches!(
            parse_qualified("a.b.c", None),
            Err(RustHouseError::InvalidIdentifier(_))
        ));
    }

    #[tokio::test]
    async fn test_snapshot_cache_invalidated_on_commit() {
        // REQ: LAKE-CAT-013
        let dir = tempfile::tempdir().unwrap();
        let cat = Catalog::open(dir.path()).await.unwrap();
        let s0 = cat.snapshot();
        assert_eq!(s0.version, 0);
        cat.create_namespace(&Principal::new("a", vec![]), "risk")
            .await
            .unwrap();
        let s1 = cat.snapshot();
        assert_eq!(s1.version, 1);
        assert!(!Arc::ptr_eq(&s0, &s1));
        assert!(
            Arc::ptr_eq(&cat.snapshot(), &s1),
            "no rebuild without a commit"
        );
    }
}
