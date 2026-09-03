//! The governed session: the only public path from SQL to data (ADR-0005, API-SQL-001).
//!
//! Every operation: parse → resolve snapshots → Governor (classification, grants, row
//! policies, masks) → audit(intent) → execute → audit(outcome). If the audit sink refuses the
//! intent, nothing executes (GOV-AUD-001). The default engine is the Arrow-native vectorized
//! engine (ADR-0006); the M1 row engine remains as an oracle for equivalence tests.

use crate::catalog::{parse_qualified, Catalog, Principal, TableRef, SYSTEM_TABLES};
use crate::error::{Result, RustHouseError};
use crate::exec::vector::{
    arrow_schema, referenced_tables, GovernedTable, SystemBatch, VectorEngine,
};
use crate::exec::{Batch, Executor, ResultSet};
use crate::governance::access::{
    self, decide_alter, decide_read, decide_write, require_security_admin, PolicySet, ReadDecision,
};
use crate::governance::audit::{AuditDraft, AuditSink, Decision, Status};
use crate::governance::classification::{Classification, Level};
use crate::governance::governor::{Governor, ROLE_CLASSIFICATION_ADMIN};
use crate::lake::log::{
    Action, AddFile, AsOf, Commit, CommitInfo, MetaData, Operation, Snapshot, Version,
};
use crate::lake::parquet::{new_part_path, to_record_batch, ParquetReader, ParquetWriter};
use crate::lake::schema::{Field, Schema};
use crate::meta::{Column, ColumnType};
use crate::metrics::metrics;
use crate::sql::parser::strip_as_of;
use crate::sql::{parse_ddl, DdlStatement, LogicalPlan, Planner};
use crate::storage::pruner::SegmentPruner;
use crate::storage::{ColumnData, Predicate};
use serde::Serialize;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VersionRead {
    pub table: String,
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct QueryResult {
    pub request_id: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub versions_read: Vec<VersionRead>,
    pub catalog_version: u64,
    pub elapsed_ms: u64,
    /// Hash of the governed, optimized plan (EXE-VEC-009); `None` on the row engine.
    pub plan_hash: Option<String>,
}

impl QueryResult {
    /// Deterministic encoding of the data part of the result (LAKE-TT-007).
    pub fn body_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "columns": self.columns,
            "rows": self.rows,
            "versions_read": self.versions_read,
        }))
        .unwrap_or_default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WriteResult {
    pub request_id: String,
    pub table: String,
    pub version: u64,
    pub rows_written: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DdlResult {
    pub request_id: String,
    pub table: String,
    /// Table version produced, if the statement touched a table log.
    pub version: Option<u64>,
    pub catalog_version: u64,
    pub message: String,
    /// Rows for statements that return data (`SHOW POLICIES`).
    #[serde(default)]
    pub rows: Vec<Value>,
}

/// A table snapshot resolved once, so a statement reads one consistent version (LAKE-TT-005).
#[derive(Debug, Clone)]
pub struct PinnedSnapshot {
    pub table: TableRef,
    pub snapshot: Snapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Engine {
    Vector,
    Row,
}

pub struct Session {
    catalog: Arc<Catalog>,
    audit: Arc<dyn AuditSink>,
    default_namespace: Option<String>,
    engine: Engine,
    executions: AtomicU64,
}

fn ms(start: Instant) -> u64 {
    start.elapsed().as_millis() as u64
}

fn error_kind(e: &RustHouseError) -> String {
    let dbg = format!("{e:?}");
    dbg.split(['(', ' ', '{'])
        .next()
        .unwrap_or("error")
        .to_string()
}

impl Session {
    pub fn new(catalog: Arc<Catalog>, audit: Arc<dyn AuditSink>) -> Self {
        Self {
            catalog,
            audit,
            default_namespace: None,
            engine: Engine::Vector,
            executions: AtomicU64::new(0),
        }
    }

    /// Use the M1 single-table row engine (CORE-EXE). Test oracle for EXE-VEC-002.
    pub fn with_row_engine(mut self) -> Self {
        self.engine = Engine::Row;
        self
    }

    pub fn with_default_namespace(mut self, ns: impl Into<String>) -> Self {
        self.default_namespace = Some(ns.into());
        self
    }

    pub fn catalog(&self) -> &Arc<Catalog> {
        &self.catalog
    }

    /// Number of times an engine ran; test hook for GOV-AUD-001 / API-SQL-011 / GOV-ACC-001.
    pub fn executions(&self) -> u64 {
        self.executions.load(Ordering::Relaxed)
    }

    // ----- queries ---------------------------------------------------------------------

    pub async fn execute_governed(&self, principal: &Principal, sql: &str) -> Result<QueryResult> {
        self.execute_query_with_id(principal, sql, Uuid::new_v4().to_string(), None)
            .await
    }

    /// `as_of_override` applies when the SQL has no `AS OF` clause (API-SQL-012).
    pub async fn execute_query_with_id(
        &self,
        principal: &Principal,
        sql: &str,
        request_id: String,
        as_of_override: Option<AsOf>,
    ) -> Result<QueryResult> {
        // GOV-ACC-002: no principal, no parsing.
        if principal.id.is_empty() {
            return Err(RustHouseError::Denied("no principal".into()));
        }
        match self.engine {
            Engine::Vector => self
                .execute_vector(principal, sql, request_id, as_of_override, false)
                .await
                .map(|(r, _)| r),
            Engine::Row => {
                self.execute_row(principal, sql, request_id, as_of_override)
                    .await
            }
        }
    }

    /// Governed plan text (logical + physical) without executing; audited as `EXPLAIN`
    /// (EXE-VEC-012, GOV-ACC-005/011).
    pub async fn explain_governed(&self, principal: &Principal, sql: &str) -> Result<String> {
        if principal.id.is_empty() {
            return Err(RustHouseError::Denied("no principal".into()));
        }
        self.execute_vector(principal, sql, Uuid::new_v4().to_string(), None, true)
            .await
            .map(|(_, explain)| explain)
    }

    /// The read decision the Governor would make for `principal` on `table` right now
    /// (GOV-ACC-010 benchmark hook).
    pub fn decide_read(&self, principal: &Principal, table: &str) -> Result<ReadDecision> {
        let table_ref = self
            .catalog
            .resolve(table, self.default_namespace.as_deref())?;
        let entry = self.catalog.entry(&table_ref)?;
        let policies = self.catalog.policies_on(&table_ref);
        decide_read(principal, &entry.owner, &entry.schema.names(), &policies)
    }

    /// Resolve a table's head once, for use with [`Self::execute_pinned`] (LAKE-TT-005).
    pub async fn pin_snapshot(
        &self,
        _principal: &Principal,
        table: &str,
    ) -> Result<PinnedSnapshot> {
        let table = self
            .catalog
            .resolve(table, self.default_namespace.as_deref())?;
        let log = self.catalog.open_table_log(&table).await?;
        let snapshot = log.snapshot(None).await?;
        Ok(PinnedSnapshot { table, snapshot })
    }

    pub async fn execute_pinned(
        &self,
        principal: &Principal,
        sql: &str,
        pinned: &PinnedSnapshot,
    ) -> Result<QueryResult> {
        if principal.id.is_empty() {
            return Err(RustHouseError::Denied("no principal".into()));
        }
        self.execute_vector_pinned(principal, sql, Uuid::new_v4().to_string(), Some(pinned))
            .await
    }

    /// Schema in force at a table version (GOV-CLS-004); fails closed on unclassified metadata.
    pub async fn schema_as_of(
        &self,
        _principal: &Principal,
        table: &str,
        version: u64,
    ) -> Result<Schema> {
        let table = self
            .catalog
            .resolve(table, self.default_namespace.as_deref())?;
        let log = self.catalog.open_table_log(&table).await?;
        let snap = log.snapshot(Some(Version(version))).await?;
        snap.metadata.schema.require_classified()?;
        Ok(snap.metadata.schema)
    }

    // ----- vector engine path ----------------------------------------------------------

    async fn execute_vector(
        &self,
        principal: &Principal,
        sql: &str,
        request_id: String,
        as_of_override: Option<AsOf>,
        explain_only: bool,
    ) -> Result<(QueryResult, String)> {
        let start = Instant::now();
        let outcome = self
            .govern_and_run(
                principal,
                sql,
                &request_id,
                as_of_override,
                None,
                explain_only,
                start,
            )
            .await;
        match outcome {
            Ok(x) => Ok(x),
            Err(e) => {
                // Failures before the intent record still leave a trail (API-SQL-007);
                // an unavailable sink turns any failure into AuditUnavailable.
                if !matches!(e, RustHouseError::AuditUnavailable(_)) {
                    let object = strip_as_of(sql)
                        .ok()
                        .and_then(|(s, _)| referenced_tables(&s).ok())
                        .and_then(|t| {
                            t.first().map(|(ns, n)| match ns {
                                Some(ns) => format!("{ns}.{n}"),
                                None => n.clone(),
                            })
                        })
                        .unwrap_or_default();
                    let (decision, op) = if matches!(
                        e,
                        RustHouseError::Denied(_) | RustHouseError::MissingClassification(_)
                    ) {
                        metrics()
                            .governor_decisions_total
                            .with_label_values(&["deny"])
                            .inc();
                        let reason = if matches!(e, RustHouseError::MissingClassification(_)) {
                            "missing_classification"
                        } else {
                            "denied"
                        };
                        (Decision::Deny, format!("QUERY_DENIED:{reason}"))
                    } else {
                        (Decision::Deny, format!("QUERY_FAILED:{}", error_kind(&e)))
                    };
                    self.audit.append(
                        AuditDraft::new(&request_id, &principal.id, op, object)
                            .decision(decision)
                            .status(Status::Error)
                            .elapsed_ms(Some(ms(start))),
                    )?;
                }
                Err(e)
            }
        }
    }

    async fn execute_vector_pinned(
        &self,
        principal: &Principal,
        sql: &str,
        request_id: String,
        pinned: Option<&PinnedSnapshot>,
    ) -> Result<QueryResult> {
        let start = Instant::now();
        self.govern_and_run(principal, sql, &request_id, None, pinned, false, start)
            .await
            .map(|(r, _)| r)
    }

    #[allow(clippy::too_many_arguments)]
    async fn govern_and_run(
        &self,
        principal: &Principal,
        sql: &str,
        request_id: &str,
        as_of_override: Option<AsOf>,
        pinned: Option<&PinnedSnapshot>,
        explain_only: bool,
        start: Instant,
    ) -> Result<(QueryResult, String)> {
        let (stripped, as_of) = strip_as_of(sql)?;
        let as_of = as_of.or(as_of_override);
        let refs = referenced_tables(&stripped)?;
        let catalog_version = self.catalog.version();

        let mut tables: Vec<Arc<GovernedTable>> = Vec::new();
        let mut system: Vec<SystemBatch> = Vec::new();
        let mut versions_read: Vec<VersionRead> = Vec::new();
        let mut classifications_by_table: Vec<(String, BTreeMap<String, Classification>)> =
            Vec::new();
        let mut applied: Vec<String> = Vec::new();
        let mut masked = false;

        for (ns, name) in &refs {
            let qualified = match ns {
                Some(ns) => format!("{ns}.{name}"),
                None => {
                    let (ns, n) = parse_qualified(name, self.default_namespace.as_deref())?;
                    format!("{ns}.{n}")
                }
            };
            if qualified.starts_with("catalog.") {
                if !SYSTEM_TABLES.contains(&qualified.as_str()) {
                    return Err(RustHouseError::TableNotFound(qualified));
                }
                let rows = self.catalog.system_table(&qualified)?;
                let batch = to_record_batch(&rows.columns, rows.data)?;
                system.push(SystemBatch {
                    namespace: "catalog".into(),
                    name: qualified.trim_start_matches("catalog.").to_string(),
                    batch,
                });
                versions_read.push(VersionRead {
                    table: qualified,
                    version: catalog_version,
                });
                continue;
            }
            let table_ref = self.catalog.resolve(&qualified, None)?;
            let snapshot = match pinned {
                Some(p) if p.table.table_id == table_ref.table_id => p.snapshot.clone(),
                _ => {
                    let log = self.catalog.open_table_log(&table_ref).await?;
                    let version = match as_of {
                        Some(a) => Some(log.resolve_as_of(&a).await?),
                        None => None,
                    };
                    log.snapshot(version).await?
                }
            };
            let schema = &snapshot.metadata.schema;
            // Fail closed on unclassified metadata (GOV-CLS-010).
            schema.require_classified()?;
            let entry = self.catalog.entry(&table_ref)?;
            let policies = self.catalog.policies_on(&table_ref);
            let decision = decide_read(principal, &entry.owner, &schema.names(), &policies)
                .map_err(|e| match e {
                    RustHouseError::Denied(m) => {
                        RustHouseError::Denied(format!("{qualified}: {m}"))
                    }
                    other => other,
                })?;
            masked |= decision.is_masked();
            applied.extend(decision.applied.iter().cloned());
            let mut cls = BTreeMap::new();
            for c in &decision.visible_columns {
                if let Some(f) = schema.field(c) {
                    cls.insert(c.clone(), f.classification()?.clone());
                }
            }
            classifications_by_table.push((qualified.clone(), cls));

            // No partial results: every file of the snapshot must still exist (LAKE-TT-008).
            let mut files = Vec::with_capacity(snapshot.files.len());
            for f in &snapshot.files {
                let path = table_ref.path.join(&f.path);
                if !path.exists() {
                    return Err(RustHouseError::VersionVacuumed {
                        table: qualified.clone(),
                        version: snapshot.version.0,
                    });
                }
                files.push((path, f.clone()));
            }
            versions_read.push(VersionRead {
                table: qualified,
                version: snapshot.version.0,
            });
            tables.push(Arc::new(GovernedTable {
                namespace: table_ref.namespace.clone(),
                name: table_ref.name.clone(),
                full_schema: arrow_schema(schema),
                files,
                decision,
            }));
        }

        let planned = VectorEngine::plan(&stripped, &tables, &system).await?;

        // Audit columns: unqualified for a single table, qualified when several are joined.
        let multi = tables.len() + system.len() > 1;
        let mut columns: Vec<String> = Vec::new();
        let mut classifications: BTreeMap<String, Classification> = BTreeMap::new();
        for (table, cls) in &classifications_by_table {
            let read = planned.columns_read.get(table).cloned().unwrap_or_default();
            for c in read {
                let key = if multi {
                    format!("{table}.{c}")
                } else {
                    c.clone()
                };
                if let Some(k) = cls.get(&c) {
                    classifications.insert(key.clone(), k.clone());
                }
                columns.push(key);
            }
        }
        let versions_map: BTreeMap<String, u64> = versions_read
            .iter()
            .map(|v| (v.table.clone(), v.version))
            .collect();
        let object = versions_read
            .first()
            .map(|v| v.table.clone())
            .unwrap_or_default();
        let decision = if masked {
            Decision::Mask
        } else {
            Decision::Allow
        };
        metrics()
            .governor_decisions_total
            .with_label_values(&[if masked { "mask" } else { "allow" }])
            .inc();
        applied.sort();
        applied.dedup();
        let base_op = if explain_only { "EXPLAIN" } else { "QUERY" };
        let operation = if applied.is_empty() {
            base_op.to_string()
        } else {
            format!("{base_op}:policies={}", applied.join(","))
        };

        // Intent before any data file is opened (GOV-AUD-005, GOV-AUD-001).
        let draft = || {
            AuditDraft::new(request_id, &principal.id, &operation, &object)
                .columns(columns.clone())
                .classifications(classifications.clone())
                .versions_read(versions_map.clone())
                .decision(decision)
                .plan_hash(Some(planned.plan_hash.clone()))
        };
        self.audit.append(draft())?;

        let explain = format!("{}\n{}", planned.logical_text, planned.physical_text);
        let outcome = if explain_only {
            Ok(ResultSet {
                columns: vec![],
                rows: vec![],
            })
        } else {
            self.executions.fetch_add(1, Ordering::Relaxed);
            VectorEngine::execute(&planned).await.map(|e| ResultSet {
                columns: e.columns,
                rows: e.rows,
            })
        };
        let (status, row_count) = match &outcome {
            Ok(rs) => (Status::Ok, Some(rs.num_rows() as u64)),
            Err(_) => (Status::Error, None),
        };
        if !explain_only {
            metrics()
                .queries_total
                .with_label_values(&[if outcome.is_ok() { "ok" } else { "error" }])
                .inc();
        }
        self.audit.append(
            draft()
                .status(status)
                .row_count(row_count)
                .elapsed_ms(Some(ms(start))),
        )?;
        let rs = outcome?;
        Ok((
            QueryResult {
                request_id: request_id.to_string(),
                columns: rs.columns,
                rows: rs.rows,
                versions_read,
                catalog_version,
                elapsed_ms: ms(start),
                plan_hash: Some(planned.plan_hash),
            },
            explain,
        ))
    }

    // ----- row engine path (M1 oracle) ---------------------------------------------------

    async fn execute_row(
        &self,
        principal: &Principal,
        sql: &str,
        request_id: String,
        as_of_override: Option<AsOf>,
    ) -> Result<QueryResult> {
        let prepared = async {
            let plan = Planner::new().plan_sql(sql)?;
            let as_of = plan.as_of().or(as_of_override);
            let source = self.resolve_row_source(&plan, as_of).await?;
            Ok::<_, RustHouseError>((plan, source))
        }
        .await;
        match prepared {
            Ok((plan, source)) => self.run_row(principal, &plan, source, request_id).await,
            Err(e) => {
                let object = Planner::new()
                    .plan_sql(sql)
                    .ok()
                    .and_then(|p| p.table_name())
                    .unwrap_or_default();
                self.audit.append(
                    AuditDraft::new(
                        &request_id,
                        &principal.id,
                        format!("QUERY_FAILED:{}", error_kind(&e)),
                        object,
                    )
                    .decision(Decision::Deny)
                    .status(Status::Error),
                )?;
                Err(e)
            }
        }
    }

    async fn resolve_row_source(
        &self,
        plan: &LogicalPlan,
        as_of: Option<AsOf>,
    ) -> Result<RowSource> {
        let name = plan
            .table_name()
            .ok_or_else(|| RustHouseError::SqlParse("no table in query".into()))?;
        if name.starts_with("catalog.") {
            if !SYSTEM_TABLES.contains(&name.as_str()) {
                return Err(RustHouseError::TableNotFound(name));
            }
            let rows = self.catalog.system_table(&name)?;
            return Ok(RowSource::System {
                name,
                columns: rows.columns,
                data: rows.data,
            });
        }
        let table = self
            .catalog
            .resolve(&name, self.default_namespace.as_deref())?;
        let log = self.catalog.open_table_log(&table).await?;
        let version = match as_of {
            Some(a) => Some(log.resolve_as_of(&a).await?),
            None => None,
        };
        let snapshot = log.snapshot(version).await?;
        Ok(RowSource::Lake(PinnedSnapshot { table, snapshot }))
    }

    async fn run_row(
        &self,
        principal: &Principal,
        plan: &LogicalPlan,
        source: RowSource,
        request_id: String,
    ) -> Result<QueryResult> {
        let start = Instant::now();
        let catalog_version = self.catalog.version();
        let object = source.object();
        let version = source.version(catalog_version);
        let versions_read: BTreeMap<String, u64> =
            [(object.clone(), version)].into_iter().collect();
        let schema = source.schema();

        let governed = match Governor::authorise_read(principal, plan, &schema) {
            Ok(g) => g,
            Err(e) => {
                metrics()
                    .governor_decisions_total
                    .with_label_values(&["deny"])
                    .inc();
                let reason = match &e {
                    RustHouseError::MissingClassification(_) => "missing_classification",
                    RustHouseError::Denied(_) => "denied",
                    _ => "invalid",
                };
                self.audit.append(
                    AuditDraft::new(
                        &request_id,
                        &principal.id,
                        format!("QUERY_DENIED:{reason}"),
                        &object,
                    )
                    .decision(Decision::Deny)
                    .status(Status::Error)
                    .versions_read(versions_read.clone())
                    .elapsed_ms(Some(ms(start))),
                )?;
                return Err(e);
            }
        };
        if let RowSource::Lake(p) = &source {
            let entry = self.catalog.entry(&p.table)?;
            let policies = self.catalog.policies_on(&p.table);
            decide_read(principal, &entry.owner, &schema.names(), &policies)?;
        }
        metrics()
            .governor_decisions_total
            .with_label_values(&["allow"])
            .inc();
        self.audit.append(
            AuditDraft::new(&request_id, &principal.id, "QUERY", &object)
                .columns(governed.columns.clone())
                .classifications(governed.classifications.clone())
                .versions_read(versions_read.clone()),
        )?;

        let outcome = self
            .execute_row_source(plan, &source, &governed.columns)
            .await;
        let (status, row_count) = match &outcome {
            Ok(rs) => (Status::Ok, Some(rs.num_rows() as u64)),
            Err(_) => (Status::Error, None),
        };
        metrics()
            .queries_total
            .with_label_values(&[if outcome.is_ok() { "ok" } else { "error" }])
            .inc();
        self.audit.append(
            AuditDraft::new(&request_id, &principal.id, "QUERY", &object)
                .columns(governed.columns.clone())
                .classifications(governed.classifications.clone())
                .versions_read(versions_read)
                .status(status)
                .row_count(row_count)
                .elapsed_ms(Some(ms(start))),
        )?;
        let rs = outcome?;
        Ok(QueryResult {
            request_id,
            columns: rs.columns,
            rows: rs.rows,
            versions_read: vec![VersionRead {
                table: object,
                version,
            }],
            catalog_version,
            elapsed_ms: ms(start),
            plan_hash: None,
        })
    }

    async fn execute_row_source(
        &self,
        plan: &LogicalPlan,
        source: &RowSource,
        columns_read: &[String],
    ) -> Result<ResultSet> {
        let batches = match source {
            RowSource::System { columns, data, .. } => vec![Batch {
                columns: columns.clone(),
                data: data.clone(),
            }],
            RowSource::Lake(p) => self.read_lake_rows(plan, p, columns_read).await?,
        };
        self.executions.fetch_add(1, Ordering::Relaxed);
        Executor::execute(plan, batches)
    }

    async fn read_lake_rows(
        &self,
        plan: &LogicalPlan,
        p: &PinnedSnapshot,
        columns_read: &[String],
    ) -> Result<Vec<Batch>> {
        let schema = &p.snapshot.metadata.schema;
        let columns: Vec<Column> = schema
            .fields
            .iter()
            .filter(|f| columns_read.contains(&f.name))
            .map(Field::to_column)
            .collect();
        let mut projection: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
        if projection.is_empty() {
            if let Some(f) = schema.fields.first() {
                projection.push(f.name.clone());
            }
        }
        let predicate = first_predicate(plan);
        for f in &p.snapshot.files {
            if !p.table.path.join(&f.path).exists() {
                return Err(RustHouseError::VersionVacuumed {
                    table: p.table.qualified(),
                    version: p.snapshot.version.0,
                });
            }
        }
        let mut batches = Vec::new();
        for f in &p.snapshot.files {
            if let Some(pred) = predicate {
                let stats = |col: &str| {
                    f.stats
                        .columns
                        .get(col)
                        .map(|s| (s.min.clone(), s.max.clone()))
                };
                if !SegmentPruner::can_match_with(pred, &stats) {
                    continue;
                }
            }
            let path = p.table.path.join(&f.path);
            let data = match predicate {
                Some(pred) => ParquetReader::read_filtered(&path, &projection, pred).await?,
                None => ParquetReader::read(&path, &projection).await?,
            };
            batches.push(Batch {
                columns: columns.clone(),
                data,
            });
        }
        if batches.is_empty() {
            let mut data = HashMap::new();
            for c in &columns {
                data.insert(c.name.clone(), empty_column(&c.column_type));
            }
            batches.push(Batch { columns, data });
        }
        Ok(batches)
    }

    // ----- writes ------------------------------------------------------------------------

    pub async fn write_rows(
        &self,
        principal: &Principal,
        table: &str,
        rows: Vec<Map<String, Value>>,
    ) -> Result<WriteResult> {
        self.write_rows_with_id(principal, table, rows, Uuid::new_v4().to_string())
            .await
    }

    pub async fn write_rows_with_id(
        &self,
        principal: &Principal,
        table: &str,
        rows: Vec<Map<String, Value>>,
        request_id: String,
    ) -> Result<WriteResult> {
        let start = Instant::now();
        if principal.id.is_empty() {
            return Err(RustHouseError::MissingPrincipal);
        }
        let table_ref = self
            .catalog
            .resolve(table, self.default_namespace.as_deref())?;
        let object = table_ref.qualified();
        let log = self.catalog.open_table_log(&table_ref).await?;
        let head = log.snapshot(None).await?;
        let schema = &head.metadata.schema;
        Governor::authorise_write(principal, schema)?;
        let entry = self.catalog.entry(&table_ref)?;
        if let Err(e) = decide_write(
            principal,
            &entry.owner,
            &self.catalog.policies_on(&table_ref),
        ) {
            metrics()
                .governor_decisions_total
                .with_label_values(&["deny"])
                .inc();
            self.audit.append(
                AuditDraft::new(&request_id, &principal.id, "WRITE_DENIED", &object)
                    .decision(Decision::Deny)
                    .status(Status::Error)
                    .elapsed_ms(Some(ms(start))),
            )?;
            return Err(e);
        }
        let columns = schema.to_columns();
        let data = rows_to_columns(schema, &rows)?;
        let classifications: BTreeMap<String, Classification> = schema
            .fields
            .iter()
            .map(|f| Ok((f.name.clone(), f.classification()?.clone())))
            .collect::<Result<_>>()?;

        self.audit.append(
            AuditDraft::new(&request_id, &principal.id, "WRITE", &object)
                .columns(schema.names())
                .classifications(classifications.clone())
                .versions_read([(object.clone(), head.version.0)].into_iter().collect()),
        )?;

        let outcome = async {
            let part = new_part_path(&table_ref.path);
            let stats = ParquetWriter::default()
                .write(&part, &columns, data)
                .await?;
            let file_name = part.file_name().unwrap().to_string_lossy().into_owned();
            let add = AddFile::new(
                file_name,
                stats.size_bytes,
                stats.num_records,
                stats.clone(),
            );
            let commit = Commit::new(
                CommitInfo::new(&principal.id, Operation::Write, json!({"mode": "append"})),
                vec![Action::Add(add)],
            );
            match log.commit(commit).await {
                Ok(v) => Ok((v, stats.num_records)),
                Err(e) => {
                    let _ = std::fs::remove_file(&part);
                    Err(e)
                }
            }
        }
        .await;

        let (status, versions, row_count) = match &outcome {
            Ok((v, n)) => (
                Status::Ok,
                [(object.clone(), v.0)].into_iter().collect(),
                Some(*n),
            ),
            Err(_) => (
                Status::Error,
                [(object.clone(), head.version.0)].into_iter().collect(),
                None,
            ),
        };
        self.audit.append(
            AuditDraft::new(&request_id, &principal.id, "WRITE", &object)
                .columns(schema.names())
                .classifications(classifications)
                .versions_read(versions)
                .status(status)
                .row_count(row_count)
                .elapsed_ms(Some(ms(start))),
        )?;
        let (version, rows_written) = outcome?;
        Ok(WriteResult {
            request_id,
            table: object,
            version: version.0,
            rows_written,
        })
    }

    // ----- DDL ---------------------------------------------------------------------------

    pub async fn execute_ddl(&self, principal: &Principal, sql: &str) -> Result<DdlResult> {
        self.execute_ddl_with_id(principal, sql, Uuid::new_v4().to_string())
            .await
    }

    pub async fn execute_ddl_with_id(
        &self,
        principal: &Principal,
        sql: &str,
        request_id: String,
    ) -> Result<DdlResult> {
        let start = Instant::now();
        if principal.id.is_empty() {
            return Err(RustHouseError::MissingPrincipal);
        }
        let stmt = match parse_ddl(sql) {
            Ok(s) => s,
            Err(e @ RustHouseError::MissingClassification(_)) => {
                metrics()
                    .governor_decisions_total
                    .with_label_values(&["deny"])
                    .inc();
                self.audit.append(
                    AuditDraft::new(
                        &request_id,
                        &principal.id,
                        "CREATE_TABLE_DENIED:missing_classification",
                        "",
                    )
                    .decision(Decision::Deny)
                    .status(Status::Error)
                    .elapsed_ms(Some(ms(start))),
                )?;
                return Err(e);
            }
            Err(e) => return Err(e),
        };
        match stmt {
            DdlStatement::CreateTable { table, schema } => {
                self.ddl_create_table(principal, table, schema, request_id, start)
                    .await
            }
            DdlStatement::AlterColumnClassification {
                table,
                column,
                classification,
                reason,
            } => {
                self.ddl_reclassify(
                    principal,
                    table,
                    column,
                    classification,
                    reason,
                    request_id,
                    start,
                )
                .await
            }
            DdlStatement::Grant {
                table,
                role,
                privilege,
            } => {
                let table_ref = self
                    .policy_admin(principal, &table, "GRANT", &request_id, start)
                    .await?;
                let v = self
                    .catalog
                    .grant(principal, &table_ref, &role, privilege.clone())
                    .await?;
                self.audit_policy_change(
                    principal,
                    "GRANT",
                    &table_ref,
                    v,
                    &request_id,
                    start,
                    format!("{privilege} TO ROLE {role}"),
                )?;
                Ok(self.ddl_ok(
                    request_id,
                    table_ref.qualified(),
                    v,
                    format!("granted {privilege} to role {role}"),
                ))
            }
            DdlStatement::Revoke {
                table,
                role,
                privilege,
            } => {
                let table_ref = self
                    .policy_admin(principal, &table, "REVOKE", &request_id, start)
                    .await?;
                let v = self
                    .catalog
                    .revoke(principal, &table_ref, &role, privilege.clone())
                    .await?;
                self.audit_policy_change(
                    principal,
                    "REVOKE",
                    &table_ref,
                    v,
                    &request_id,
                    start,
                    format!("{privilege} FROM ROLE {role}"),
                )?;
                Ok(self.ddl_ok(
                    request_id,
                    table_ref.qualified(),
                    v,
                    format!("revoked {privilege} from role {role}"),
                ))
            }
            DdlStatement::CreateRowPolicy {
                table,
                policy,
                role,
                predicate_sql,
            } => {
                let table_ref = self
                    .policy_admin(principal, &table, "POLICY", &request_id, start)
                    .await?;
                let entry = self.catalog.entry(&table_ref)?;
                validate_row_policy(&entry.schema, &predicate_sql)?;
                let v = self
                    .catalog
                    .create_row_policy(principal, &table_ref, &policy, &role, &predicate_sql)
                    .await?;
                self.audit_policy_change(
                    principal,
                    "POLICY",
                    &table_ref,
                    v,
                    &request_id,
                    start,
                    format!("ROW POLICY {policy} FOR ROLE {role}"),
                )?;
                Ok(self.ddl_ok(
                    request_id,
                    table_ref.qualified(),
                    v,
                    format!("row policy {policy} created"),
                ))
            }
            DdlStatement::CreateMaskingPolicy {
                table,
                column,
                policy,
                role,
                kind,
            } => {
                let table_ref = self
                    .policy_admin(principal, &table, "POLICY", &request_id, start)
                    .await?;
                let v = self
                    .catalog
                    .create_masking_policy(principal, &table_ref, &policy, &column, &role, kind)
                    .await?;
                self.audit_policy_change(
                    principal,
                    "POLICY",
                    &table_ref,
                    v,
                    &request_id,
                    start,
                    format!(
                        "MASKING POLICY {policy} ON {column} FOR ROLE {role} AS {}",
                        kind.as_str()
                    ),
                )?;
                Ok(self.ddl_ok(
                    request_id,
                    table_ref.qualified(),
                    v,
                    format!("masking policy {policy} created"),
                ))
            }
            DdlStatement::DropPolicy { table, policy } => {
                let table_ref = self
                    .policy_admin(principal, &table, "POLICY", &request_id, start)
                    .await?;
                let v = self
                    .catalog
                    .drop_policy(principal, &table_ref, &policy)
                    .await?;
                self.audit_policy_change(
                    principal,
                    "POLICY",
                    &table_ref,
                    v,
                    &request_id,
                    start,
                    format!("DROP POLICY {policy}"),
                )?;
                Ok(self.ddl_ok(
                    request_id,
                    table_ref.qualified(),
                    v,
                    format!("policy {policy} dropped"),
                ))
            }
            DdlStatement::ShowPolicies {
                table,
                as_of_version,
            } => {
                let table_ref = self
                    .catalog
                    .resolve(&table, self.default_namespace.as_deref())?;
                let entry = self.catalog.entry(&table_ref)?;
                if principal.id != entry.owner {
                    require_security_admin(principal)?;
                }
                let policies = match as_of_version {
                    Some(v) => self.catalog.policies_on_as_of(&table_ref, v).await?,
                    None => self.catalog.policies_on(&table_ref),
                };
                let rows = policy_rows(&policies);
                self.audit.append(
                    AuditDraft::new(
                        &request_id,
                        &principal.id,
                        "SHOW_POLICIES",
                        table_ref.qualified(),
                    )
                    .versions_read(
                        [(
                            "catalog".to_string(),
                            as_of_version.unwrap_or(self.catalog.version()),
                        )]
                        .into_iter()
                        .collect(),
                    )
                    .status(Status::Ok)
                    .row_count(Some(rows.len() as u64))
                    .elapsed_ms(Some(ms(start))),
                )?;
                Ok(DdlResult {
                    request_id,
                    table: table_ref.qualified(),
                    version: None,
                    catalog_version: self.catalog.version(),
                    message: format!("{} policies", rows.len()),
                    rows,
                })
            }
        }
    }

    fn ddl_ok(
        &self,
        request_id: String,
        table: String,
        catalog_version: u64,
        message: String,
    ) -> DdlResult {
        DdlResult {
            request_id,
            table,
            version: None,
            catalog_version,
            message,
            rows: vec![],
        }
    }

    /// GOV-ACC-003: grants and policies are managed by security admins only; refusals audited.
    async fn policy_admin(
        &self,
        principal: &Principal,
        table: &str,
        op: &str,
        request_id: &str,
        start: Instant,
    ) -> Result<TableRef> {
        let table_ref = self
            .catalog
            .resolve(table, self.default_namespace.as_deref())?;
        if let Err(e) = require_security_admin(principal) {
            metrics()
                .governor_decisions_total
                .with_label_values(&["deny"])
                .inc();
            self.audit.append(
                AuditDraft::new(
                    request_id,
                    &principal.id,
                    format!("{op}_DENIED"),
                    table_ref.qualified(),
                )
                .decision(Decision::Deny)
                .status(Status::Error)
                .elapsed_ms(Some(ms(start))),
            )?;
            return Err(e);
        }
        Ok(table_ref)
    }

    #[allow(clippy::too_many_arguments)]
    fn audit_policy_change(
        &self,
        principal: &Principal,
        op: &str,
        table: &TableRef,
        catalog_version: u64,
        request_id: &str,
        start: Instant,
        detail: String,
    ) -> Result<()> {
        // GOV-AUD-008: policy changes name the resulting catalog version.
        self.audit.append(
            AuditDraft::new(
                request_id,
                &principal.id,
                format!("{op}:{detail}"),
                table.qualified(),
            )
            .versions_read(
                [("catalog.policies".to_string(), catalog_version)]
                    .into_iter()
                    .collect(),
            )
            .status(Status::Ok)
            .elapsed_ms(Some(ms(start))),
        )?;
        Ok(())
    }

    async fn ddl_create_table(
        &self,
        principal: &Principal,
        table: String,
        schema: Schema,
        request_id: String,
        start: Instant,
    ) -> Result<DdlResult> {
        let (ns, name) = parse_qualified(&table, self.default_namespace.as_deref())?;
        let object = format!("{ns}.{name}");
        let classifications: BTreeMap<String, Classification> = schema
            .fields
            .iter()
            .map(|f| Ok((f.name.clone(), f.classification()?.clone())))
            .collect::<Result<_>>()?;
        self.audit.append(
            AuditDraft::new(&request_id, &principal.id, "CREATE_TABLE", &object)
                .columns(schema.names())
                .classifications(classifications.clone()),
        )?;
        let outcome = self
            .catalog
            .create_table(
                principal,
                crate::catalog::TableSpec::new(ns, name, schema.clone()),
            )
            .await;
        let catalog_version = self.catalog.version();
        let mut versions: BTreeMap<String, u64> = BTreeMap::new();
        versions.insert("catalog".into(), catalog_version);
        if outcome.is_ok() {
            versions.insert(object.clone(), 0);
        }
        self.audit.append(
            AuditDraft::new(&request_id, &principal.id, "CREATE_TABLE", &object)
                .columns(schema.names())
                .classifications(classifications)
                .versions_read(versions)
                .status(if outcome.is_ok() {
                    Status::Ok
                } else {
                    Status::Error
                })
                .elapsed_ms(Some(ms(start))),
        )?;
        outcome?;
        Ok(DdlResult {
            request_id,
            table: object,
            version: Some(0),
            catalog_version,
            message: "table created".into(),
            rows: vec![],
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn ddl_reclassify(
        &self,
        principal: &Principal,
        table: String,
        column: String,
        classification: Classification,
        reason: Option<String>,
        request_id: String,
        start: Instant,
    ) -> Result<DdlResult> {
        let table_ref = self
            .catalog
            .resolve(&table, self.default_namespace.as_deref())?;
        let object = table_ref.qualified();
        let entry = self.catalog.entry(&table_ref)?;
        // Owner, ALTER grant, or the classification admin role may reclassify.
        if !principal.has_role(ROLE_CLASSIFICATION_ADMIN) {
            decide_alter(
                principal,
                &entry.owner,
                &self.catalog.policies_on(&table_ref),
            )?;
        }
        let log = self.catalog.open_table_log(&table_ref).await?;
        let head = log.snapshot(None).await?;
        let mut schema = head.metadata.schema.clone();
        let field = schema
            .fields
            .iter_mut()
            .find(|f| f.name == column)
            .ok_or_else(|| RustHouseError::ColumnNotFound(column.clone()))?;
        let previous = field.classification()?.clone();
        if let Err(e) =
            Governor::authorise_reclassify(principal, &previous, &classification, reason.as_deref())
        {
            if matches!(e, RustHouseError::Denied(_)) {
                metrics()
                    .governor_decisions_total
                    .with_label_values(&["deny"])
                    .inc();
                self.audit.append(
                    AuditDraft::new(&request_id, &principal.id, "ALTER_DENIED", &object)
                        .columns(vec![column.clone()])
                        .decision(Decision::Deny)
                        .status(Status::Error)
                        .elapsed_ms(Some(ms(start))),
                )?;
            }
            return Err(e);
        }
        field.classification = Some(classification.clone());

        let mut audited: BTreeMap<String, Classification> = BTreeMap::new();
        audited.insert(column.clone(), classification.clone());
        audited.insert(format!("{column}.previous"), previous.clone());
        self.audit.append(
            AuditDraft::new(&request_id, &principal.id, "ALTER", &object)
                .columns(vec![column.clone()])
                .classifications(audited.clone())
                .versions_read([(object.clone(), head.version.0)].into_iter().collect()),
        )?;

        let metadata = MetaData {
            schema: schema.clone(),
            ..head.metadata.clone()
        };
        let params = json!({
            "column": column,
            "previous": previous,
            "new": classification,
            "reason": reason,
        });
        let outcome = log
            .commit(Commit::new(
                CommitInfo::new(&principal.id, Operation::Alter, params),
                vec![Action::MetaData(metadata)],
            ))
            .await;
        let catalog_version = match &outcome {
            Ok(_) => {
                self.catalog
                    .update_schema(principal, &table_ref, schema)
                    .await?
            }
            Err(_) => self.catalog.version(),
        };
        let version = outcome.as_ref().map(|v| v.0).unwrap_or(head.version.0);
        self.audit.append(
            AuditDraft::new(&request_id, &principal.id, "ALTER", &object)
                .columns(vec![column.clone()])
                .classifications(audited)
                .versions_read([(object.clone(), version)].into_iter().collect())
                .status(if outcome.is_ok() {
                    Status::Ok
                } else {
                    Status::Error
                })
                .elapsed_ms(Some(ms(start))),
        )?;
        let v = outcome?;
        Ok(DdlResult {
            request_id,
            table: object,
            version: Some(v.0),
            catalog_version,
            message: format!("column {column} reclassified"),
            rows: vec![],
        })
    }

    // ----- unauthenticated requests (API-SQL-003) ---------------------------------------

    pub fn audit_unauthenticated(&self, request_id: &str, route: &str) -> Result<()> {
        self.audit.append(
            AuditDraft::new(request_id, "", "UNAUTHENTICATED", route)
                .decision(Decision::Deny)
                .status(Status::Unauthenticated),
        )?;
        Ok(())
    }
}

enum RowSource {
    Lake(PinnedSnapshot),
    System {
        name: String,
        columns: Vec<Column>,
        data: HashMap<String, ColumnData>,
    },
}

impl RowSource {
    fn object(&self) -> String {
        match self {
            RowSource::Lake(p) => p.table.qualified(),
            RowSource::System { name, .. } => name.clone(),
        }
    }
    fn version(&self, catalog_version: u64) -> u64 {
        match self {
            RowSource::Lake(p) => p.snapshot.version.0,
            RowSource::System { .. } => catalog_version,
        }
    }
    fn schema(&self) -> Schema {
        match self {
            RowSource::Lake(p) => p.snapshot.metadata.schema.clone(),
            RowSource::System { columns, .. } => Schema::new(
                columns
                    .iter()
                    .map(|c| {
                        Field::new(
                            &c.name,
                            c.column_type.clone(),
                            Classification::new(Level::Internal),
                        )
                    })
                    .collect(),
            ),
        }
    }
}

fn first_predicate(plan: &LogicalPlan) -> Option<&Predicate> {
    match plan {
        LogicalPlan::Filter { predicate, .. } => Some(predicate),
        LogicalPlan::Scan { .. } => None,
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => first_predicate(input),
    }
}

fn empty_column(t: &ColumnType) -> ColumnData {
    match t {
        ColumnType::Int64 => ColumnData::Int64(vec![]),
        ColumnType::Float64 => ColumnData::Float64(vec![]),
        ColumnType::String => ColumnData::String(vec![]),
        ColumnType::Boolean => ColumnData::Boolean(vec![]),
    }
}

fn policy_rows(p: &PolicySet) -> Vec<Value> {
    let mut rows = Vec::new();
    for g in &p.grants {
        rows.push(json!({"kind": "grant", "name": "", "role": g.role, "detail": g.privilege.to_string(), "created_by": g.granted_by, "version": g.version}));
    }
    for r in &p.row_policies {
        rows.push(json!({"kind": "row_policy", "name": r.name, "role": r.role, "detail": r.predicate_sql, "created_by": r.created_by, "version": r.version}));
    }
    for m in &p.masking_policies {
        rows.push(json!({"kind": "masking_policy", "name": m.name, "role": m.role, "column": m.column, "detail": m.kind.as_str(), "created_by": m.created_by, "version": m.version}));
    }
    rows
}

/// A row policy must parse against the table's schema (with attributes bound to placeholders),
/// so a typo is caught at CREATE time rather than at every query.
fn validate_row_policy(schema: &Schema, predicate_sql: &str) -> Result<()> {
    let mut probe = Principal::new("probe", vec![]);
    let bound = loop {
        match access::bind_attributes(predicate_sql, &probe) {
            Ok(b) => break b,
            Err(RustHouseError::Denied(m)) if m.starts_with("missing_attribute:") => {
                probe
                    .attributes
                    .insert(m["missing_attribute:".len()..].to_string(), "x".into());
            }
            Err(e) => return Err(e),
        }
    };
    let arrow = arrow_schema(schema);
    let df_schema = datafusion::common::DFSchema::try_from(arrow.as_ref().clone())
        .map_err(|e| RustHouseError::SqlParse(e.to_string()))?;
    datafusion::prelude::SessionContext::new()
        .state()
        .create_logical_expr(&bound, &df_schema)
        .map_err(|e| {
            RustHouseError::SqlParse(format!(
                "row policy predicate: {}",
                e.to_string().lines().next().unwrap_or("")
            ))
        })?;
    Ok(())
}

/// JSON rows → columnar data following the schema. Missing keys and JSON null become NULL;
/// unknown keys are refused; type mismatches are refused (no silent coercion of money).
fn rows_to_columns(
    schema: &Schema,
    rows: &[Map<String, Value>],
) -> Result<HashMap<String, ColumnData>> {
    let known: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
    for (i, r) in rows.iter().enumerate() {
        if let Some(k) = r.keys().find(|k| !known.contains(&k.as_str())) {
            return Err(RustHouseError::InvalidData(format!(
                "row {i}: unknown column '{k}'"
            )));
        }
    }
    let mut out = HashMap::new();
    for f in &schema.fields {
        let cells: Vec<Option<&Value>> = rows
            .iter()
            .map(|r| r.get(&f.name).filter(|v| !v.is_null()))
            .collect();
        let has_null = cells.iter().any(|c| c.is_none());
        let bad = |i: usize, v: &Value| {
            RustHouseError::TypeMismatch(format!(
                "row {i}: column '{}' expects {:?}, got {v}",
                f.name, f.column_type
            ))
        };
        let col = match f.column_type {
            ColumnType::Int64 => {
                let vals = cells
                    .iter()
                    .enumerate()
                    .map(|(i, c)| match c {
                        None => Ok(None),
                        Some(v) => v
                            .as_i64()
                            .or_else(|| v.as_f64().filter(|x| x.fract() == 0.0).map(|x| x as i64))
                            .map(Some)
                            .ok_or_else(|| bad(i, v)),
                    })
                    .collect::<Result<Vec<Option<i64>>>>()?;
                if has_null {
                    ColumnData::NullableInt64(vals)
                } else {
                    ColumnData::Int64(vals.into_iter().flatten().collect())
                }
            }
            ColumnType::Float64 => {
                let vals = cells
                    .iter()
                    .enumerate()
                    .map(|(i, c)| match c {
                        None => Ok(None),
                        Some(v) => v.as_f64().map(Some).ok_or_else(|| bad(i, v)),
                    })
                    .collect::<Result<Vec<Option<f64>>>>()?;
                if has_null {
                    ColumnData::NullableFloat64(vals)
                } else {
                    ColumnData::Float64(vals.into_iter().flatten().collect())
                }
            }
            ColumnType::String => {
                let vals = cells
                    .iter()
                    .enumerate()
                    .map(|(i, c)| match c {
                        None => Ok(None),
                        Some(v) => v
                            .as_str()
                            .map(|s| Some(s.to_string()))
                            .ok_or_else(|| bad(i, v)),
                    })
                    .collect::<Result<Vec<Option<String>>>>()?;
                if has_null {
                    ColumnData::NullableString(vals)
                } else {
                    ColumnData::String(vals.into_iter().flatten().collect())
                }
            }
            ColumnType::Boolean => {
                let vals = cells
                    .iter()
                    .enumerate()
                    .map(|(i, c)| match c {
                        None => Ok(None),
                        Some(v) => v.as_bool().map(Some).ok_or_else(|| bad(i, v)),
                    })
                    .collect::<Result<Vec<Option<bool>>>>()?;
                if has_null {
                    ColumnData::NullableBoolean(vals)
                } else {
                    ColumnData::Boolean(vals.into_iter().flatten().collect())
                }
            }
        };
        out.insert(f.name.clone(), col);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rows_to_columns_types_and_nulls() {
        let schema = Schema::new(vec![
            Field::new(
                "id",
                ColumnType::Int64,
                Classification::new(Level::Internal),
            ),
            Field::new(
                "name",
                ColumnType::String,
                Classification::new(Level::Internal),
            ),
        ]);
        let rows: Vec<Map<String, Value>> = vec![
            json!({"id": 1, "name": "a"}).as_object().unwrap().clone(),
            json!({"id": 2.0}).as_object().unwrap().clone(),
        ];
        let cols = rows_to_columns(&schema, &rows).unwrap();
        assert_eq!(cols["id"], ColumnData::Int64(vec![1, 2]));
        assert_eq!(
            cols["name"],
            ColumnData::NullableString(vec![Some("a".into()), None])
        );
        let bad = vec![json!({"id": "x"}).as_object().unwrap().clone()];
        assert!(matches!(
            rows_to_columns(&schema, &bad),
            Err(RustHouseError::TypeMismatch(_))
        ));
        let unknown = vec![json!({"zzz": 1}).as_object().unwrap().clone()];
        assert!(matches!(
            rows_to_columns(&schema, &unknown),
            Err(RustHouseError::InvalidData(_))
        ));
    }

    #[test]
    fn test_validate_row_policy() {
        let schema = Schema::new(vec![
            Field::new(
                "country",
                ColumnType::String,
                Classification::new(Level::Internal),
            ),
            Field::new(
                "balance",
                ColumnType::Float64,
                Classification::new(Level::Internal),
            ),
        ]);
        validate_row_policy(
            &schema,
            "country = current_attribute('country') AND balance > 3",
        )
        .unwrap();
        assert!(validate_row_policy(&schema, "nope = 1").is_err());
        assert!(validate_row_policy(&schema, "country = = 1").is_err());
    }
}
