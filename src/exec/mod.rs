//! Single-table executor (spec CORE-EXE). Crate-private by design: the only caller is
//! `Session::execute_governed` (ADR-0005, API-SQL-001).

pub(crate) mod vector;

use crate::error::{Result, RustHouseError};
use crate::meta::Column;
use crate::sql::planner::{AggregateExpr, AggregateFunction, LogicalPlan, SortKey};
use crate::storage::{ColumnData, Predicate};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::HashMap;

/// One columnar input batch (typically one data file).
#[derive(Debug, Clone)]
pub struct Batch {
    pub columns: Vec<Column>,
    pub data: HashMap<String, ColumnData>,
}

impl Batch {
    pub fn num_rows(&self) -> usize {
        self.data.values().next().map(|c| c.len()).unwrap_or(0)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResultSet {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

impl ResultSet {
    pub fn num_rows(&self) -> usize {
        self.rows.len()
    }
}

/// Read one cell as JSON. `None` means SQL NULL. Nullable variants added by LAKE-PQT are
/// handled through their JSON representation so this stays the single access point.
pub(crate) fn value_at(col: &ColumnData, i: usize) -> Option<Value> {
    fn float(x: f64) -> Option<Value> {
        serde_json::Number::from_f64(x).map(Value::Number)
    }
    match col {
        ColumnData::Int64(v) => v.get(i).map(|x| Value::from(*x)),
        ColumnData::Float64(v) => v.get(i).and_then(|x| float(*x)),
        ColumnData::String(v) => v.get(i).map(|x| Value::String(x.clone())),
        ColumnData::Boolean(v) => v.get(i).map(|x| Value::Bool(*x)),
        ColumnData::NullableInt64(v) => v.get(i).copied().flatten().map(Value::from),
        ColumnData::NullableFloat64(v) => v.get(i).copied().flatten().and_then(float),
        ColumnData::NullableString(v) => v.get(i).cloned().flatten().map(Value::String),
        ColumnData::NullableBoolean(v) => v.get(i).copied().flatten().map(Value::Bool),
    }
}

/// Comparison with the same semantics as the pruner: numbers on f64, strings lexicographic.
fn compare(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64()?.partial_cmp(&y.as_f64()?),
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        (Value::Bool(x), Value::Bool(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

fn row_matches(pred: &Predicate, batch: &Batch, i: usize) -> Result<bool> {
    let cell = |c: &str| -> Result<Option<Value>> {
        let col = batch
            .data
            .get(c)
            .ok_or_else(|| RustHouseError::ColumnNotFound(c.to_string()))?;
        Ok(value_at(col, i))
    };
    let cmp = |c: &str, v: &Value, f: fn(Ordering) -> bool| -> Result<bool> {
        Ok(match cell(c)? {
            None => false, // NULL never matches (CORE-EXE-002)
            Some(x) => compare(&x, v).map(f).unwrap_or(false),
        })
    };
    match pred {
        Predicate::Eq(c, v) => cmp(c, v, |o| o == Ordering::Equal),
        Predicate::Gt(c, v) => cmp(c, v, |o| o == Ordering::Greater),
        Predicate::Lt(c, v) => cmp(c, v, |o| o == Ordering::Less),
        Predicate::Gte(c, v) => cmp(c, v, |o| o != Ordering::Less),
        Predicate::Lte(c, v) => cmp(c, v, |o| o != Ordering::Greater),
        Predicate::And(a, b) => Ok(row_matches(a, batch, i)? && row_matches(b, batch, i)?),
        Predicate::Or(a, b) => Ok(row_matches(a, batch, i)? || row_matches(b, batch, i)?),
    }
}

fn batch_to_rows(batch: &Batch) -> Result<ResultSet> {
    let columns: Vec<String> = batch.columns.iter().map(|c| c.name.clone()).collect();
    let cols: Vec<&ColumnData> = columns
        .iter()
        .map(|c| {
            batch
                .data
                .get(c)
                .ok_or_else(|| RustHouseError::ColumnNotFound(c.clone()))
        })
        .collect::<Result<_>>()?;
    let n = batch.num_rows();
    let rows = (0..n)
        .map(|i| {
            cols.iter()
                .map(|c| value_at(c, i).unwrap_or(Value::Null))
                .collect()
        })
        .collect();
    Ok(ResultSet { columns, rows })
}

pub(crate) struct Executor;

impl Executor {
    /// Execute `plan` over `input`. Batches are the pruned data files of one table.
    pub(crate) fn execute(plan: &LogicalPlan, input: Vec<Batch>) -> Result<ResultSet> {
        match plan {
            LogicalPlan::Scan { .. } => {
                let mut out: Option<ResultSet> = None;
                for b in &input {
                    let rs = batch_to_rows(b)?;
                    match out.as_mut() {
                        None => out = Some(rs),
                        Some(acc) => acc.rows.extend(rs.rows),
                    }
                }
                Ok(out.unwrap_or_else(|| ResultSet {
                    columns: input
                        .first()
                        .map(|b| b.columns.iter().map(|c| c.name.clone()).collect())
                        .unwrap_or_default(),
                    rows: vec![],
                }))
            }
            LogicalPlan::Filter {
                input: child,
                predicate,
            } => {
                // Filter pushes into batches before materialising rows (CORE-EXE-006).
                let filtered = input
                    .into_iter()
                    .map(|b| filter_batch(&b, predicate))
                    .collect::<Result<Vec<_>>>()?;
                Self::execute(child, filtered)
            }
            LogicalPlan::Project {
                input: child,
                columns,
            } => {
                let rs = Self::execute(child, input)?;
                project(rs, columns)
            }
            LogicalPlan::Aggregate {
                input: child,
                group_by,
                aggregates,
            } => {
                let rs = Self::execute(child, input)?;
                aggregate(rs, group_by, aggregates)
            }
            LogicalPlan::Sort { input: child, keys } => {
                let mut rs = Self::execute(child, input)?;
                sort(&mut rs, keys)?;
                Ok(rs)
            }
            LogicalPlan::Limit {
                input: child,
                limit,
            } => {
                let mut rs = Self::execute(child, input)?;
                rs.rows.truncate(*limit);
                Ok(rs)
            }
        }
    }
}

fn filter_batch(batch: &Batch, pred: &Predicate) -> Result<Batch> {
    let n = batch.num_rows();
    let mut keep = Vec::with_capacity(n);
    for i in 0..n {
        keep.push(row_matches(pred, batch, i)?);
    }
    let mut data = HashMap::with_capacity(batch.data.len());
    for (name, col) in &batch.data {
        data.insert(name.clone(), take_rows(col, &keep));
    }
    Ok(Batch {
        columns: batch.columns.clone(),
        data,
    })
}

fn take_rows(col: &ColumnData, keep: &[bool]) -> ColumnData {
    fn pick<T: Clone>(v: &[T], keep: &[bool]) -> Vec<T> {
        v.iter()
            .zip(keep)
            .filter(|(_, k)| **k)
            .map(|(x, _)| x.clone())
            .collect()
    }
    match col {
        ColumnData::Int64(v) => ColumnData::Int64(pick(v, keep)),
        ColumnData::Float64(v) => ColumnData::Float64(pick(v, keep)),
        ColumnData::String(v) => ColumnData::String(pick(v, keep)),
        ColumnData::Boolean(v) => ColumnData::Boolean(pick(v, keep)),
        ColumnData::NullableInt64(v) => ColumnData::NullableInt64(pick(v, keep)),
        ColumnData::NullableFloat64(v) => ColumnData::NullableFloat64(pick(v, keep)),
        ColumnData::NullableString(v) => ColumnData::NullableString(pick(v, keep)),
        ColumnData::NullableBoolean(v) => ColumnData::NullableBoolean(pick(v, keep)),
    }
}

fn project(rs: ResultSet, columns: &[String]) -> Result<ResultSet> {
    if columns.iter().any(|c| c == "*") {
        return Ok(rs);
    }
    let idx: Vec<usize> = columns
        .iter()
        .map(|c| {
            rs.columns
                .iter()
                .position(|x| x == c)
                .ok_or_else(|| RustHouseError::ColumnNotFound(c.clone()))
        })
        .collect::<Result<_>>()?;
    let rows = rs
        .rows
        .into_iter()
        .map(|r| idx.iter().map(|&i| r[i].clone()).collect())
        .collect();
    Ok(ResultSet {
        columns: columns.to_vec(),
        rows,
    })
}

#[derive(Default)]
struct AggState {
    count: u64,
    sum: f64,
    all_int: bool,
    min: Option<Value>,
    max: Option<Value>,
    seen: bool,
}

impl AggState {
    fn new() -> Self {
        Self {
            all_int: true,
            ..Default::default()
        }
    }
    fn push(&mut self, v: Option<&Value>) {
        let Some(v) = v else { return };
        self.seen = true;
        self.count += 1;
        if let Some(n) = v.as_f64() {
            self.sum += n;
            if !v.is_i64() {
                self.all_int = false;
            }
        }
        if self
            .min
            .as_ref()
            .map(|m| compare(v, m) == Some(Ordering::Less))
            .unwrap_or(true)
        {
            self.min = Some(v.clone());
        }
        if self
            .max
            .as_ref()
            .map(|m| compare(v, m) == Some(Ordering::Greater))
            .unwrap_or(true)
        {
            self.max = Some(v.clone());
        }
    }
    fn finish(&self, f: &AggregateFunction, star_count: u64) -> Value {
        match f {
            AggregateFunction::Count => Value::from(star_count),
            AggregateFunction::Sum if !self.seen => Value::Null,
            AggregateFunction::Sum if self.all_int => Value::from(self.sum as i64),
            AggregateFunction::Sum => serde_json::Number::from_f64(self.sum)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            AggregateFunction::Avg if !self.seen => Value::Null,
            AggregateFunction::Avg => serde_json::Number::from_f64(self.sum / self.count as f64)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            AggregateFunction::Min => self.min.clone().unwrap_or(Value::Null),
            AggregateFunction::Max => self.max.clone().unwrap_or(Value::Null),
        }
    }
}

fn agg_name(a: &AggregateExpr) -> String {
    if let Some(alias) = &a.alias {
        return alias.clone();
    }
    let f = match a.function {
        AggregateFunction::Count => "count",
        AggregateFunction::Sum => "sum",
        AggregateFunction::Avg => "avg",
        AggregateFunction::Min => "min",
        AggregateFunction::Max => "max",
    };
    match &a.column {
        Some(c) => format!("{f}({c})"),
        None => format!("{f}(*)"),
    }
}

fn aggregate(
    rs: ResultSet,
    group_by: &[String],
    aggregates: &[AggregateExpr],
) -> Result<ResultSet> {
    let col_idx = |c: &str| -> Result<usize> {
        rs.columns
            .iter()
            .position(|x| x == c)
            .ok_or_else(|| RustHouseError::ColumnNotFound(c.to_string()))
    };
    let group_idx: Vec<usize> = group_by.iter().map(|c| col_idx(c)).collect::<Result<_>>()?;
    let agg_idx: Vec<Option<usize>> = aggregates
        .iter()
        .map(|a| a.column.as_deref().map(col_idx).transpose())
        .collect::<Result<_>>()?;

    // Groups in first-seen order (CORE-EXE-004).
    let mut order: Vec<Vec<Value>> = Vec::new();
    let mut states: HashMap<String, (usize, Vec<AggState>, u64)> = HashMap::new();
    for row in &rs.rows {
        let key_vals: Vec<Value> = group_idx.iter().map(|&i| row[i].clone()).collect();
        let key = serde_json::to_string(&key_vals)?;
        let entry = states.entry(key).or_insert_with(|| {
            order.push(key_vals.clone());
            (
                order.len() - 1,
                aggregates.iter().map(|_| AggState::new()).collect(),
                0,
            )
        });
        entry.2 += 1;
        for (state, idx) in entry.1.iter_mut().zip(&agg_idx) {
            match idx {
                Some(i) => state.push(if row[*i].is_null() {
                    None
                } else {
                    Some(&row[*i])
                }),
                None => state.push(Some(&Value::Bool(true))), // COUNT(*)
            }
        }
    }
    if group_by.is_empty() && order.is_empty() {
        // Aggregates over zero rows yield one row (CORE-EXE-004).
        order.push(vec![]);
        states.insert(
            "[]".into(),
            (0, aggregates.iter().map(|_| AggState::new()).collect(), 0),
        );
    }

    let mut rows: Vec<Vec<Value>> = vec![Vec::new(); order.len()];
    for (_, (pos, sts, star)) in states {
        let mut r = order[pos].clone();
        for (a, st) in aggregates.iter().zip(&sts) {
            let star_count = if a.column.is_some() { st.count } else { star };
            r.push(st.finish(&a.function, star_count));
        }
        rows[pos] = r;
    }
    let mut columns = group_by.to_vec();
    columns.extend(aggregates.iter().map(agg_name));
    Ok(ResultSet { columns, rows })
}

fn sort(rs: &mut ResultSet, keys: &[SortKey]) -> Result<()> {
    let idx: Vec<(usize, bool)> = keys
        .iter()
        .map(|k| {
            rs.columns
                .iter()
                .position(|c| *c == k.column)
                .map(|i| (i, k.ascending))
                .ok_or_else(|| RustHouseError::ColumnNotFound(k.column.clone()))
        })
        .collect::<Result<_>>()?;
    rs.rows.sort_by(|a, b| {
        for &(i, asc) in &idx {
            let ord = match (&a[i], &b[i]) {
                (Value::Null, Value::Null) => Ordering::Equal,
                (Value::Null, _) => Ordering::Greater, // nulls last regardless of direction
                (_, Value::Null) => Ordering::Less,
                (x, y) => {
                    let o = compare(x, y).unwrap_or(Ordering::Equal);
                    if asc {
                        o
                    } else {
                        o.reverse()
                    }
                }
            };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::ColumnType;
    use crate::sql::Planner;
    use serde_json::json;

    fn col(name: &str, t: ColumnType) -> Column {
        Column {
            name: name.into(),
            column_type: t,
        }
    }

    fn batch(ids: Vec<i64>, names: Vec<&str>) -> Batch {
        let mut data = HashMap::new();
        data.insert("id".to_string(), ColumnData::Int64(ids));
        data.insert(
            "name".to_string(),
            ColumnData::String(names.into_iter().map(String::from).collect()),
        );
        Batch {
            columns: vec![
                col("id", ColumnType::Int64),
                col("name", ColumnType::String),
            ],
            data,
        }
    }

    fn run(sql: &str, input: Vec<Batch>) -> ResultSet {
        let plan = Planner::new().plan_sql(sql).unwrap();
        Executor::execute(&plan, input).unwrap()
    }

    #[test]
    fn core_exe_002_filter_is_exact_and_null_safe() {
        // REQ: CORE-EXE-002
        let rs = run(
            "SELECT * FROM t WHERE id > 3 AND name < 'c'",
            vec![batch(vec![1, 5, 10], vec!["a", "b", "c"])],
        );
        assert_eq!(rs.rows, vec![vec![json!(5), json!("b")]]);
        // Numeric comparison on f64 semantics: 5 >= 5.0
        let rs = run(
            "SELECT id FROM t WHERE id >= 5.0",
            vec![batch(vec![1, 5, 10], vec!["a", "b", "c"])],
        );
        assert_eq!(rs.rows.len(), 2);
        // NULL never matches a comparison (CORE-EXE-002).
        let mut data = HashMap::new();
        data.insert(
            "id".to_string(),
            ColumnData::NullableInt64(vec![Some(1), None, Some(10)]),
        );
        let b = Batch {
            columns: vec![col("id", ColumnType::Int64)],
            data,
        };
        let rs = run("SELECT id FROM t WHERE id < 100", vec![b]);
        assert_eq!(rs.rows, vec![vec![json!(1)], vec![json!(10)]]);
        // Type-incompatible comparison never matches.
        let rs = run(
            "SELECT id FROM t WHERE name > 3",
            vec![batch(vec![1, 5], vec!["a", "b"])],
        );
        assert!(rs.rows.is_empty());
    }

    #[test]
    fn core_exe_003_project_order_and_unknown_column() {
        // REQ: CORE-EXE-003
        let rs = run("SELECT name, id FROM t", vec![batch(vec![1], vec!["a"])]);
        assert_eq!(rs.columns, vec!["name", "id"]);
        assert_eq!(rs.rows, vec![vec![json!("a"), json!(1)]]);
        let plan = Planner::new().plan_sql("SELECT nope FROM t").unwrap();
        let err = Executor::execute(&plan, vec![batch(vec![1], vec!["a"])]).unwrap_err();
        assert!(matches!(err, RustHouseError::ColumnNotFound(_)));
    }

    #[test]
    fn core_exe_004_grouped_aggregates() {
        // REQ: CORE-EXE-004
        let mut data = HashMap::new();
        data.insert(
            "grp".to_string(),
            ColumnData::String(vec!["x".into(), "y".into(), "x".into()]),
        );
        data.insert("v".to_string(), ColumnData::Int64(vec![1, 2, 3]));
        let b = Batch {
            columns: vec![col("grp", ColumnType::String), col("v", ColumnType::Int64)],
            data,
        };
        let rs = run(
            "SELECT grp, COUNT(*), SUM(v), AVG(v), MIN(v), MAX(v) FROM t GROUP BY grp",
            vec![b],
        );
        assert_eq!(
            rs.columns,
            vec!["grp", "count(*)", "sum(v)", "avg(v)", "min(v)", "max(v)"]
        );
        assert_eq!(
            rs.rows,
            vec![
                vec![
                    json!("x"),
                    json!(2),
                    json!(4),
                    json!(2.0),
                    json!(1),
                    json!(3)
                ],
                vec![
                    json!("y"),
                    json!(1),
                    json!(2),
                    json!(2.0),
                    json!(2),
                    json!(2)
                ],
            ]
        );
    }

    #[test]
    fn core_exe_004_aggregate_over_empty_input() {
        // REQ: CORE-EXE-004
        let rs = run(
            "SELECT COUNT(*), SUM(id) FROM t",
            vec![batch(vec![], vec![])],
        );
        assert_eq!(rs.rows, vec![vec![json!(0), Value::Null]]);
        let rs = run("SELECT COUNT(*) AS n FROM t", vec![]);
        assert_eq!(rs.columns, vec!["n"]);
        assert_eq!(rs.rows, vec![vec![json!(0)]]);
    }

    #[test]
    fn core_exe_005_sort_stable_nulls_last_then_limit() {
        // REQ: CORE-EXE-005
        let mut rs = ResultSet {
            columns: vec!["k".into(), "tag".into()],
            rows: vec![
                vec![json!(2), json!("a")],
                vec![Value::Null, json!("n")],
                vec![json!(1), json!("b")],
                vec![json!(2), json!("c")],
            ],
        };
        sort(
            &mut rs,
            &[SortKey {
                column: "k".into(),
                ascending: false,
            }],
        )
        .unwrap();
        let tags: Vec<&Value> = rs.rows.iter().map(|r| &r[1]).collect();
        assert_eq!(
            tags,
            vec![&json!("a"), &json!("c"), &json!("b"), &json!("n")]
        );
        let rs = run(
            "SELECT id FROM t ORDER BY id LIMIT 2",
            vec![batch(vec![9, 3, 7], vec!["a", "b", "c"])],
        );
        assert_eq!(rs.rows, vec![vec![json!(3)], vec![json!(7)]]);
    }

    #[test]
    fn core_exe_006_multiple_batches_act_as_one_table() {
        // REQ: CORE-EXE-006
        let rs = run(
            "SELECT COUNT(*), MAX(id) FROM t WHERE id > 1",
            vec![
                batch(vec![1, 2], vec!["a", "b"]),
                batch(vec![3, 4], vec!["c", "d"]),
            ],
        );
        assert_eq!(rs.rows, vec![vec![json!(3), json!(4)]]);
        let rs = run(
            "SELECT id FROM t ORDER BY id DESC",
            vec![
                batch(vec![1, 4], vec!["a", "b"]),
                batch(vec![3, 2], vec!["c", "d"]),
            ],
        );
        assert_eq!(
            rs.rows,
            vec![
                vec![json!(4)],
                vec![json!(3)],
                vec![json!(2)],
                vec![json!(1)]
            ]
        );
    }

    #[test]
    fn core_exe_007_json_row_types() {
        // REQ: CORE-EXE-007
        let mut data = HashMap::new();
        data.insert("i".to_string(), ColumnData::Int64(vec![7]));
        data.insert("f".to_string(), ColumnData::Float64(vec![1.5]));
        data.insert("b".to_string(), ColumnData::Boolean(vec![true]));
        let b = Batch {
            columns: vec![
                col("i", ColumnType::Int64),
                col("f", ColumnType::Float64),
                col("b", ColumnType::Boolean),
            ],
            data,
        };
        let rs = run("SELECT * FROM t", vec![b]);
        assert_eq!(rs.columns, vec!["i", "f", "b"]);
        assert!(rs.rows[0][0].is_i64());
        assert!(rs.rows[0][1].is_f64());
        assert!(rs.rows[0][2].is_boolean());
    }
}
