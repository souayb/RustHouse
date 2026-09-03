//! Arrow-native vectorized engine on Apache DataFusion (spec EXE-VEC, ADR-0006).
//!
//! Crate-private: only `Session` can build a context, and every context carries the Governor
//! analyzer rule (EXE-VEC-004). Tables are exposed through [`GovernedTableProvider`], which is
//! bound to one pinned rhlog snapshot (EXE-VEC-006) and shows only the columns the principal
//! may see (GOV-ACC-004). Row filters and masks are injected by [`GovernorRule`], which runs
//! before type coercion (ADR-0006 spike finding 1).

use crate::error::{Result, RustHouseError};
use crate::governance::access::{mask_string, MaskKind, ReadDecision};
use crate::lake::log::AddFile;
use crate::storage::pruner::SegmentPruner;
use crate::storage::Predicate;
use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::memory::{MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::catalog::{CatalogProvider, SchemaProvider, Session as DfSession, TableProvider};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, ScalarValue, TableReference};
use datafusion::config::ConfigOptions;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::{MemTable, TableType};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{
    lit, BinaryExpr, ColumnarValue, Expr, LogicalPlan, LogicalPlanBuilder, Operator,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TableProviderFilterPushDown,
    Volatility,
};
use datafusion::optimizer::analyzer::{Analyzer, AnalyzerRule};
use datafusion::physical_plan::{collect, displayable, ExecutionPlan};
use datafusion::prelude::{SessionConfig, SessionContext};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

/// One lake table as the engine sees it for one query.
#[derive(Debug, Clone)]
pub(crate) struct GovernedTable {
    pub namespace: String,
    pub name: String,
    /// Full Arrow schema of the snapshot (all fields nullable).
    pub full_schema: SchemaRef,
    /// Active data files of the pinned snapshot, with their `add` statistics.
    pub files: Vec<(PathBuf, AddFile)>,
    pub decision: ReadDecision,
}

impl GovernedTable {
    pub fn qualified(&self) -> String {
        format!("{}.{}", self.namespace, self.name)
    }
    fn visible_schema(&self) -> SchemaRef {
        let fields: Vec<Field> = self
            .decision
            .visible_columns
            .iter()
            .filter_map(|c| self.full_schema.field_with_name(c).ok().cloned())
            .collect();
        Arc::new(Schema::new(fields))
    }
}

/// A system table materialised as Arrow batches (catalog.*).
#[derive(Debug, Clone)]
pub(crate) struct SystemBatch {
    pub namespace: String,
    pub name: String,
    pub batch: RecordBatch,
}

pub(crate) struct PlannedQuery {
    ctx: SessionContext,
    physical: Arc<dyn ExecutionPlan>,
    /// Governed + optimized logical plan text.
    pub logical_text: String,
    pub physical_text: String,
    pub plan_hash: String,
    /// Columns actually read per table (`ns.table` -> visible column names).
    pub columns_read: BTreeMap<String, Vec<String>>,
    pub output_columns: Vec<String>,
}

pub(crate) struct ExecutedQuery {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

pub(crate) struct VectorEngine;

impl VectorEngine {
    /// Parse, govern, analyze and optimize. No data file is opened here; only Parquet footers
    /// would be, and the provider supplies the schema so even that is avoided.
    pub(crate) async fn plan(
        sql: &str,
        tables: &[Arc<GovernedTable>],
        system: &[SystemBatch],
    ) -> Result<PlannedQuery> {
        // Parse the bound row-filter predicates first, against each table's *visible* schema,
        // so a policy on a hidden column is a clean refusal, not an engine error.
        let parse_ctx = SessionContext::new();
        let mut rewrites: HashMap<String, Rewrite> = HashMap::new();
        for t in tables {
            let visible = t.visible_schema();
            let df_schema = DFSchema::try_from_qualified_schema(
                TableReference::partial(t.namespace.clone(), t.name.clone()),
                &visible,
            )
            .map_err(df_err)?;
            let mut filter: Option<Expr> = None;
            for pred in &t.decision.row_filters {
                let e = parse_ctx
                    .state()
                    .create_logical_expr(pred, &df_schema)
                    .map_err(|e| {
                        RustHouseError::Denied(format!(
                            "row policy on {} cannot be evaluated for this principal (it may reference a column outside your grant): {}",
                            t.qualified(),
                            short(&e.to_string())
                        ))
                    })?;
                filter = Some(match filter {
                    None => e,
                    Some(acc) => acc.and(e),
                });
            }
            rewrites.insert(
                t.qualified(),
                Rewrite {
                    filter,
                    masks: t.decision.masks.clone(),
                    visible: t.decision.visible_columns.clone(),
                },
            );
        }

        let mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> =
            vec![Arc::new(GovernorRule { rewrites })];
        rules.extend(Analyzer::new().rules);
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_information_schema(false))
            .with_default_features()
            .with_analyzer_rules(rules)
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_udf(ScalarUDF::from(MaskUdf::new()));

        let catalog = Arc::new(MemoryCatalogProvider::new());
        let mut schemas: HashMap<String, Arc<MemorySchemaProvider>> = HashMap::new();
        for t in tables {
            let schema = schemas
                .entry(t.namespace.clone())
                .or_insert_with(|| Arc::new(MemorySchemaProvider::new()));
            schema
                .register_table(
                    t.name.clone(),
                    Arc::new(GovernedTableProvider::new(t.clone())),
                )
                .map_err(df_err)?;
        }
        for s in system {
            let schema = schemas
                .entry(s.namespace.clone())
                .or_insert_with(|| Arc::new(MemorySchemaProvider::new()));
            let mem =
                MemTable::try_new(s.batch.schema(), vec![vec![s.batch.clone()]]).map_err(df_err)?;
            schema
                .register_table(s.name.clone(), Arc::new(mem))
                .map_err(df_err)?;
        }
        for (name, schema) in schemas {
            catalog.register_schema(&name, schema).map_err(df_err)?;
        }
        ctx.register_catalog("datafusion", catalog);

        let raw = ctx
            .state()
            .create_logical_plan(sql)
            .await
            .map_err(|e| map_plan_error(e, tables))?;
        let optimized = ctx
            .state()
            .optimize(&raw)
            .map_err(|e| map_plan_error(e, tables))?;
        let physical = ctx
            .state()
            .create_physical_plan(&raw)
            .await
            .map_err(|e| map_plan_error(e, tables))?;

        let logical_text = optimized.display_indent().to_string();
        let physical_text = displayable(physical.as_ref()).indent(true).to_string();
        let plan_hash = hex(Sha256::digest(logical_text.as_bytes()));
        let columns_read = columns_read(&optimized, tables);
        let output_columns = physical
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        Ok(PlannedQuery {
            ctx,
            physical,
            logical_text,
            physical_text,
            plan_hash,
            columns_read,
            output_columns,
        })
    }

    pub(crate) async fn execute(planned: &PlannedQuery) -> Result<ExecutedQuery> {
        let batches = collect(planned.physical.clone(), planned.ctx.task_ctx())
            .await
            .map_err(df_err)?;
        let rows = batches_to_rows(&batches, &planned.output_columns)?;
        Ok(ExecutedQuery {
            columns: planned.output_columns.clone(),
            rows,
        })
    }
}

fn short(s: &str) -> String {
    s.lines().next().unwrap_or("").chars().take(200).collect()
}

fn df_err(e: datafusion::error::DataFusionError) -> RustHouseError {
    RustHouseError::QueryExecution(short(&e.to_string()))
}

/// Planning errors → our error model. A missing column that exists in some table's full schema
/// but not in its visible schema is a governance refusal, not "not found" (GOV-ACC-004).
fn map_plan_error(
    e: datafusion::error::DataFusionError,
    tables: &[Arc<GovernedTable>],
) -> RustHouseError {
    use datafusion::error::DataFusionError as E;
    let msg = e.to_string();
    if m_has_no_field(&msg) {
        let name = field_name_from_error(&msg);
        let hidden = tables.iter().any(|t| {
            t.full_schema.field_with_name(&name).is_ok()
                && !t.decision.visible_columns.contains(&name)
        });
        return if hidden {
            RustHouseError::Denied(format!("column '{name}' is outside your SELECT grant"))
        } else {
            RustHouseError::ColumnNotFound(name)
        };
    }
    match &e {
        E::SQL(..) => RustHouseError::SqlParse(short(&msg)),
        E::SchemaError(..) => {
            let name = field_name_from_error(&msg);
            let hidden = tables.iter().any(|t| {
                t.full_schema.field_with_name(&name).is_ok()
                    && !t.decision.visible_columns.contains(&name)
            });
            if hidden {
                RustHouseError::Denied(format!("column '{name}' is outside your SELECT grant"))
            } else {
                RustHouseError::ColumnNotFound(name)
            }
        }
        E::Plan(m) if m.contains("table") && m.contains("not found") => {
            RustHouseError::TableNotFound(short(m))
        }
        E::Plan(m) => RustHouseError::SqlParse(short(m)),
        _ => RustHouseError::QueryExecution(short(&msg)),
    }
}

fn m_has_no_field(msg: &str) -> bool {
    msg.contains("No field named")
}

fn field_name_from_error(msg: &str) -> String {
    // "No field named gold.accounts.email. Did you mean 'gold.accounts.balance'?" or
    // "No field named email.\nValid fields are ...": the name ends at the first period that
    // is followed by whitespace (or ends the text), or at a '?'.
    let after = msg.split("No field named ").nth(1).unwrap_or("");
    let bytes = after.as_bytes();
    let mut end = after.len();
    for (i, b) in bytes.iter().enumerate() {
        let next_ws = bytes
            .get(i + 1)
            .map(|c| c.is_ascii_whitespace())
            .unwrap_or(true);
        if (*b == b'.' && next_ws) || *b == b'?' {
            end = i;
            break;
        }
    }
    let head = after[..end].trim().trim_matches(|c| c == '"' || c == '\'');
    head.rsplit('.').next().unwrap_or(head).to_string()
}

fn hex(bytes: impl AsRef<[u8]>) -> String {
    bytes.as_ref().iter().map(|b| format!("{b:02x}")).collect()
}

/// Columns each governed table actually reads, from the optimized scan projections.
fn columns_read(
    plan: &LogicalPlan,
    tables: &[Arc<GovernedTable>],
) -> BTreeMap<String, Vec<String>> {
    let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let _ = plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            let key = match scan.table_name.schema() {
                Some(s) => format!("{s}.{}", scan.table_name.table()),
                None => scan.table_name.table().to_string(),
            };
            if let Some(t) = tables.iter().find(|t| t.qualified() == key) {
                let visible = &t.decision.visible_columns;
                let cols: Vec<String> = match &scan.projection {
                    Some(idx) => idx
                        .iter()
                        .filter_map(|i| visible.get(*i).cloned())
                        .collect(),
                    None => visible.clone(),
                };
                let entry = out.entry(key).or_default();
                for c in cols {
                    if !entry.contains(&c) {
                        entry.push(c);
                    }
                }
            }
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    });
    out
}

// ------------------------------------------------------------------------------------------
// Governor analyzer rule
// ------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct Rewrite {
    filter: Option<Expr>,
    masks: BTreeMap<String, MaskKind>,
    visible: Vec<String>,
}

/// Injects row filters and masking projections directly above every governed scan. Runs
/// before `TypeCoercion` so injected literals are coerced like user-written ones.
#[derive(Debug)]
struct GovernorRule {
    rewrites: HashMap<String, Rewrite>,
}

impl AnalyzerRule for GovernorRule {
    fn name(&self) -> &str {
        "rusthouse_governor"
    }

    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &ConfigOptions,
    ) -> datafusion::error::Result<LogicalPlan> {
        plan.transform_up(|node| {
            let LogicalPlan::TableScan(scan) = &node else {
                return Ok(Transformed::no(node));
            };
            let key = match scan.table_name.schema() {
                Some(s) => format!("{s}.{}", scan.table_name.table()),
                None => scan.table_name.table().to_string(),
            };
            let Some(rw) = self.rewrites.get(&key) else {
                return Ok(Transformed::no(node));
            };
            if rw.filter.is_none() && rw.masks.is_empty() {
                return Ok(Transformed::no(node));
            }
            let table_ref = scan.table_name.clone();
            let mut builder = LogicalPlanBuilder::from(node);
            if let Some(f) = &rw.filter {
                builder = builder.filter(f.clone())?;
            }
            if !rw.masks.is_empty() {
                let exprs: Vec<Expr> = rw
                    .visible
                    .iter()
                    .map(|c| {
                        let column = Expr::Column(datafusion::common::Column::new(
                            Some(table_ref.clone()),
                            c,
                        ));
                        match rw.masks.get(c) {
                            Some(kind) => Expr::ScalarFunction(
                                datafusion::logical_expr::expr::ScalarFunction::new_udf(
                                    Arc::new(ScalarUDF::from(MaskUdf::new())),
                                    vec![lit(kind.as_str()), column],
                                ),
                            )
                            .alias_qualified(Some(table_ref.clone()), c),
                            None => column,
                        }
                    })
                    .collect();
                builder = builder.project(exprs)?;
            }
            Ok(Transformed::yes(builder.build()?))
        })
        .map(|t| t.data)
    }
}

// ------------------------------------------------------------------------------------------
// Masking UDF: rh_mask(kind, value) — type-stable, deterministic (GOV-ACC-007)
// ------------------------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct MaskUdf {
    signature: Signature,
}

impl MaskUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

fn is_string(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

impl ScalarUDFImpl for MaskUdf {
    fn name(&self) -> &str {
        "rh_mask"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> datafusion::error::Result<DataType> {
        let dt = args.get(1).cloned().unwrap_or(DataType::Utf8);
        Ok(if is_string(&dt) { DataType::Utf8 } else { dt })
    }
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let kind = match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(k)))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(k)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(k))) => MaskKind::parse(k)
                .map_err(|e| datafusion::error::DataFusionError::Plan(e.to_string()))?,
            other => {
                return Err(datafusion::error::DataFusionError::Plan(format!(
                    "rh_mask: first argument must be a string literal, got {other:?}"
                )))
            }
        };
        let value: ArrayRef = match &args.args[1] {
            ColumnarValue::Array(a) => a.clone(),
            ColumnarValue::Scalar(s) => s.to_array_of_size(args.number_rows)?,
        };
        if !is_string(value.data_type()) {
            // Non-string columns can only be nulled; every mask degrades to NULL (type-stable).
            return Ok(ColumnarValue::Array(
                datafusion::arrow::array::new_null_array(value.data_type(), value.len()),
            ));
        }
        let as_utf8 = cast(&value, &DataType::Utf8)?;
        let strings = as_utf8
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("cast to Utf8");
        let masked: StringArray = strings
            .iter()
            .map(|v| v.and_then(|s| mask_string(kind, s)))
            .collect();
        Ok(ColumnarValue::Array(Arc::new(masked)))
    }
}

// ------------------------------------------------------------------------------------------
// Governed table provider: visible schema, pinned files, statistics pruning
// ------------------------------------------------------------------------------------------

#[derive(Debug)]
struct GovernedTableProvider {
    table: Arc<GovernedTable>,
    visible: SchemaRef,
}

impl GovernedTableProvider {
    fn new(table: Arc<GovernedTable>) -> Self {
        let visible = table.visible_schema();
        Self { table, visible }
    }
}

#[async_trait]
impl TableProvider for GovernedTableProvider {
    fn schema(&self) -> SchemaRef {
        self.visible.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        // Inexact: the scan prunes with statistics; DataFusion keeps the exact filter above.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn DfSession,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // 1. File-level pruning with `add` statistics before any I/O (EXE-VEC-007).
        let predicate = filters
            .iter()
            .filter_map(expr_to_predicate)
            .reduce(|a, b| Predicate::And(Box::new(a), Box::new(b)));
        let files: Vec<&(PathBuf, AddFile)> = self
            .table
            .files
            .iter()
            .filter(|(_, add)| match &predicate {
                Some(p) => {
                    let stats = |c: &str| {
                        add.stats
                            .columns
                            .get(c)
                            .map(|s| (s.min.clone(), s.max.clone()))
                    };
                    SegmentPruner::can_match_with(p, &stats)
                }
                None => true,
            })
            .collect();

        // 2. Map the projection from the visible schema to the full schema.
        let full = &self.table.full_schema;
        let projected_full: Option<Vec<usize>> = projection.map(|idx| {
            idx.iter()
                .filter_map(|i| {
                    self.visible
                        .field(*i)
                        .name()
                        .as_str()
                        .pipe(|n| full.index_of(n).ok())
                })
                .collect()
        });

        if files.is_empty() {
            // Nothing can match: an empty relation with the projected visible schema.
            let schema = match projection {
                Some(idx) => Arc::new(self.visible.project(idx)?),
                None => self.visible.clone(),
            };
            return Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                schema,
            )));
        }

        // 3. Delegate to a ListingTable over exactly the surviving files, with the full schema.
        let urls: Vec<ListingTableUrl> = files
            .iter()
            .map(|(p, _)| ListingTableUrl::parse(p.to_string_lossy()))
            .collect::<datafusion::error::Result<_>>()?;
        let options =
            ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet");
        let config = ListingTableConfig::new_with_multi_paths(urls)
            .with_listing_options(options)
            .with_schema(full.clone());
        let listing = ListingTable::try_new(config)?;
        // Filters are expressed against the visible schema's column names, which are the same
        // names in the full schema; ListingTable resolves them by name.
        let plan = listing
            .scan(state, projected_full.as_ref(), filters, limit)
            .await?;
        // The listing scan outputs full-schema field order for the projected columns; reorder
        // to the visible order DataFusion expects.
        reorder_to_visible(plan, &self.visible, projection)
    }
}

/// Small pipe helper for readability.
trait Pipe: Sized {
    fn pipe<R>(self, f: impl FnOnce(Self) -> R) -> R {
        f(self)
    }
}
impl<T> Pipe for T {}

fn reorder_to_visible(
    plan: Arc<dyn ExecutionPlan>,
    visible: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
    let wanted: Vec<String> = match projection {
        Some(idx) => idx
            .iter()
            .map(|i| visible.field(*i).name().clone())
            .collect(),
        None => visible.fields().iter().map(|f| f.name().clone()).collect(),
    };
    let actual = plan.schema();
    let already = actual.fields().len() == wanted.len()
        && actual
            .fields()
            .iter()
            .zip(&wanted)
            .all(|(f, w)| f.name() == w);
    if already {
        return Ok(plan);
    }
    let exprs = wanted
        .iter()
        .map(|name| {
            let idx = actual.index_of(name)?;
            Ok((
                Arc::new(datafusion::physical_expr::expressions::Column::new(
                    name, idx,
                )) as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                name.clone(),
            ))
        })
        .collect::<datafusion::error::Result<Vec<_>>>()?;
    Ok(Arc::new(
        datafusion::physical_plan::projection::ProjectionExec::try_new(exprs, plan)?,
    ))
}

/// Translate the simple comparisons DataFusion pushes down into the pruner's `Predicate`.
/// Anything not understood yields `None` (conservative: no pruning on it).
fn expr_to_predicate(e: &Expr) -> Option<Predicate> {
    fn unwrap_cast(e: &Expr) -> &Expr {
        match e {
            Expr::Cast(c) => unwrap_cast(&c.expr),
            Expr::TryCast(c) => unwrap_cast(&c.expr),
            other => other,
        }
    }
    fn scalar_json(v: &ScalarValue) -> Option<Value> {
        Some(match v {
            ScalarValue::Int8(Some(x)) => Value::from(*x),
            ScalarValue::Int16(Some(x)) => Value::from(*x),
            ScalarValue::Int32(Some(x)) => Value::from(*x),
            ScalarValue::Int64(Some(x)) => Value::from(*x),
            ScalarValue::UInt32(Some(x)) => Value::from(*x),
            ScalarValue::UInt64(Some(x)) => Value::from(*x),
            ScalarValue::Float32(Some(x)) => {
                serde_json::Number::from_f64(*x as f64).map(Value::Number)?
            }
            ScalarValue::Float64(Some(x)) => serde_json::Number::from_f64(*x).map(Value::Number)?,
            ScalarValue::Utf8(Some(s))
            | ScalarValue::LargeUtf8(Some(s))
            | ScalarValue::Utf8View(Some(s)) => Value::String(s.clone()),
            ScalarValue::Boolean(Some(b)) => Value::Bool(*b),
            _ => return None,
        })
    }
    match e {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::And => {
                let (l, r) = (expr_to_predicate(left), expr_to_predicate(right));
                match (l, r) {
                    (Some(l), Some(r)) => Some(Predicate::And(Box::new(l), Box::new(r))),
                    (Some(x), None) | (None, Some(x)) => Some(x),
                    (None, None) => None,
                }
            }
            Operator::Or => Some(Predicate::Or(
                Box::new(expr_to_predicate(left)?),
                Box::new(expr_to_predicate(right)?),
            )),
            Operator::Eq | Operator::Gt | Operator::Lt | Operator::GtEq | Operator::LtEq => {
                let (l, r) = (unwrap_cast(left), unwrap_cast(right));
                let (column, value, flipped) = match (l, r) {
                    (Expr::Column(c), Expr::Literal(v, _)) => {
                        (c.name.clone(), scalar_json(v)?, false)
                    }
                    (Expr::Literal(v, _), Expr::Column(c)) => {
                        (c.name.clone(), scalar_json(v)?, true)
                    }
                    _ => return None,
                };
                let op = if flipped {
                    match op {
                        Operator::Gt => Operator::Lt,
                        Operator::Lt => Operator::Gt,
                        Operator::GtEq => Operator::LtEq,
                        Operator::LtEq => Operator::GtEq,
                        o => *o,
                    }
                } else {
                    *op
                };
                Some(match op {
                    Operator::Eq => Predicate::Eq(column, value),
                    Operator::Gt => Predicate::Gt(column, value),
                    Operator::Lt => Predicate::Lt(column, value),
                    Operator::GtEq => Predicate::Gte(column, value),
                    Operator::LtEq => Predicate::Lte(column, value),
                    _ => return None,
                })
            }
            _ => None,
        },
        Expr::Between(b) if !b.negated => {
            let c = match unwrap_cast(&b.expr) {
                Expr::Column(c) => c.name.clone(),
                _ => return None,
            };
            let lo = match unwrap_cast(&b.low) {
                Expr::Literal(v, _) => scalar_json(v)?,
                _ => return None,
            };
            let hi = match unwrap_cast(&b.high) {
                Expr::Literal(v, _) => scalar_json(v)?,
                _ => return None,
            };
            Some(Predicate::And(
                Box::new(Predicate::Gte(c.clone(), lo)),
                Box::new(Predicate::Lte(c, hi)),
            ))
        }
        _ => None,
    }
}

// ------------------------------------------------------------------------------------------
// Result conversion
// ------------------------------------------------------------------------------------------

fn batches_to_rows(batches: &[RecordBatch], columns: &[String]) -> Result<Vec<Vec<Value>>> {
    let mut rows = Vec::new();
    for b in batches {
        if b.num_rows() == 0 {
            continue;
        }
        let mut writer = datafusion::arrow::json::ArrayWriter::new(Vec::new());
        writer
            .write(b)
            .map_err(|e| RustHouseError::QueryExecution(e.to_string()))?;
        writer
            .finish()
            .map_err(|e| RustHouseError::QueryExecution(e.to_string()))?;
        let objs: Vec<serde_json::Map<String, Value>> =
            serde_json::from_slice(&writer.into_inner())?;
        for mut o in objs {
            rows.push(
                columns
                    .iter()
                    .map(|c| o.remove(c).unwrap_or(Value::Null))
                    .collect(),
            );
        }
    }
    Ok(rows)
}

/// Table references mentioned by a statement (before any planning).
pub(crate) fn referenced_tables(sql: &str) -> Result<Vec<(Option<String>, String)>> {
    use datafusion::sql::parser::DFParser;
    let mut statements =
        DFParser::parse_sql(sql).map_err(|e| RustHouseError::SqlParse(short(&e.to_string())))?;
    if statements.len() != 1 {
        return Err(RustHouseError::SqlParse(
            "exactly one statement expected".into(),
        ));
    }
    let stmt = statements.pop_front().unwrap();
    let (tables, ctes) = datafusion::sql::resolve::resolve_table_references(&stmt, true)
        .map_err(|e| RustHouseError::SqlParse(short(&e.to_string())))?;
    Ok(tables
        .into_iter()
        .filter(|t| !ctes.iter().any(|c| c.table() == t.table()))
        .map(|t| (t.schema().map(str::to_string), t.table().to_string()))
        .collect())
}

/// Convert a lake schema into the Arrow schema the engine plans against (all fields nullable:
/// Parquet files written from nullable batches are OPTIONAL, see LAKE-PQT change log).
pub(crate) fn arrow_schema(schema: &crate::lake::schema::Schema) -> SchemaRef {
    Arc::new(Schema::new(
        schema
            .fields
            .iter()
            .map(|f| {
                let dt = match f.column_type {
                    crate::meta::ColumnType::Int64 => DataType::Int64,
                    crate::meta::ColumnType::Float64 => DataType::Float64,
                    crate::meta::ColumnType::String => DataType::Utf8,
                    crate::meta::ColumnType::Boolean => DataType::Boolean,
                };
                Field::new(&f.name, dt, true)
            })
            .collect::<Vec<_>>(),
    ))
}

#[allow(dead_code)]
fn _unused(_: &dyn CatalogProvider, _: &dyn SchemaProvider) {}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::col;

    #[test]
    fn test_expr_to_predicate_handles_casts_and_between() {
        let e = col("id").gt(lit(5i64));
        assert!(matches!(expr_to_predicate(&e), Some(Predicate::Gt(c, v)) if c == "id" && v == 5));
        let e = lit(5i64).lt(col("id")); // flipped
        assert!(matches!(expr_to_predicate(&e), Some(Predicate::Gt(..))));
        let e = col("country").eq(Expr::Cast(datafusion::logical_expr::Cast::new(
            Box::new(lit("FR")),
            DataType::Utf8View,
        )));
        assert!(
            matches!(expr_to_predicate(&e), Some(Predicate::Eq(c, v)) if c == "country" && v == "FR")
        );
        let e = col("id").between(lit(1i64), lit(3i64));
        assert!(matches!(expr_to_predicate(&e), Some(Predicate::And(..))));
        assert!(expr_to_predicate(&col("a").eq(col("b"))).is_none());
    }

    #[test]
    fn test_field_name_from_error() {
        // REQ: GOV-ACC-004
        assert_eq!(
            field_name_from_error("Schema error: No field named gold.accounts.email. Did you mean 'gold.accounts.balance'?."),
            "email"
        );
        assert_eq!(
            field_name_from_error(
                "No field named email. Valid fields are gold.accounts.id, gold.accounts.balance."
            ),
            "email"
        );
        assert_eq!(field_name_from_error("No field named nope."), "nope");
        assert_eq!(
            field_name_from_error(
                "Schema error: No field named email.\nValid fields are gold.t.id."
            ),
            "email"
        );
    }

    #[tokio::test]
    async fn test_hidden_column_reference_is_denied() {
        // REQ: GOV-ACC-004
        use crate::governance::access::ReadDecision;
        let full = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("email", DataType::Utf8, true),
        ]));
        let table = Arc::new(GovernedTable {
            namespace: "gold".into(),
            name: "t".into(),
            full_schema: full,
            files: vec![],
            decision: ReadDecision {
                visible_columns: vec!["id".into()],
                row_filters: vec![],
                masks: BTreeMap::new(),
                applied: vec![],
            },
        });
        async fn plan_err(sql: &str, t: &Arc<GovernedTable>) -> RustHouseError {
            match VectorEngine::plan(sql, std::slice::from_ref(t), &[]).await {
                Ok(_) => panic!("{sql}: expected an error"),
                Err(e) => e,
            }
        }
        let err = plan_err("SELECT email FROM gold.t", &table).await;
        assert!(
            matches!(&err, RustHouseError::Denied(m) if m.contains("email")),
            "{err:?}"
        );
        let err = plan_err("SELECT id FROM gold.t WHERE email = 'x'", &table).await;
        assert!(
            matches!(&err, RustHouseError::Denied(m) if m.contains("email")),
            "{err:?}"
        );
        let err = plan_err("SELECT nope FROM gold.t", &table).await;
        assert!(
            matches!(&err, RustHouseError::ColumnNotFound(c) if c == "nope"),
            "{err:?}"
        );
    }

    #[test]
    fn test_referenced_tables() {
        let t =
            referenced_tables("SELECT a.id FROM gold.a a JOIN gold.b b ON a.id = b.id").unwrap();
        assert_eq!(t.len(), 2);
        assert!(t.contains(&(Some("gold".into()), "a".into())));
        assert!(referenced_tables("SELECT FROM").is_err());
    }

    #[test]
    fn test_mask_udf_is_type_stable() {
        // REQ: GOV-ACC-007
        let udf = MaskUdf::new();
        assert_eq!(
            udf.return_type(&[DataType::Utf8, DataType::Utf8View])
                .unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            udf.return_type(&[DataType::Utf8, DataType::Float64])
                .unwrap(),
            DataType::Float64
        );
    }
}
