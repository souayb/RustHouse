use crate::error::{Result, RustHouseError};
use crate::lake::log::AsOf;
use crate::sql::Parser;
use crate::storage::Predicate;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, GroupByExpr, Query, Select, SelectItem,
    SetExpr, Statement, TableFactor,
};

#[derive(Debug, Clone)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone)]
pub struct AggregateExpr {
    pub function: AggregateFunction,
    pub column: Option<String>, // None for COUNT(*)
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortKey {
    pub column: String,
    pub ascending: bool,
}

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan {
        table: String,
        columns: Vec<String>,
        /// Time-travel pin (LAKE-TT-001/002); `None` reads the head resolved at plan time.
        as_of: Option<AsOf>,
    },
    Sort {
        input: Box<LogicalPlan>,
        keys: Vec<SortKey>,
    },
    Limit {
        input: Box<LogicalPlan>,
        limit: usize,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: Predicate,
    },
    Project {
        input: Box<LogicalPlan>,
        columns: Vec<String>,
    },
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<String>,
        aggregates: Vec<AggregateExpr>,
    },
}

impl LogicalPlan {
    /// The single table this plan scans (M1 plans have exactly one scan).
    pub fn table_name(&self) -> Option<String> {
        match self {
            LogicalPlan::Scan { table, .. } => Some(table.clone()),
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => input.table_name(),
        }
    }

    pub fn as_of(&self) -> Option<AsOf> {
        match self {
            LogicalPlan::Scan { as_of, .. } => *as_of,
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => input.as_of(),
        }
    }

    /// Column names the plan reads. Contains `"*"` when the whole table is needed.
    /// Used by the Governor to check classifications and grants (GOV-CLS-009, GOV-ACC).
    pub fn referenced_columns(&self) -> Vec<String> {
        let mut out = Vec::new();
        self.collect_columns(&mut out);
        if !self.narrows_output() {
            // `SELECT *`: every column of the table reaches the caller.
            out.insert(0, "*".to_string());
        }
        out
    }

    fn narrows_output(&self) -> bool {
        match self {
            LogicalPlan::Project { .. } | LogicalPlan::Aggregate { .. } => true,
            LogicalPlan::Scan { .. } => false,
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => input.narrows_output(),
        }
    }

    fn collect_columns(&self, out: &mut Vec<String>) {
        fn push(out: &mut Vec<String>, c: &str) {
            if !out.iter().any(|x| x == c) {
                out.push(c.to_string());
            }
        }
        match self {
            LogicalPlan::Scan { .. } => {}
            LogicalPlan::Filter { input, predicate } => {
                input.collect_columns(out);
                for c in predicate_columns(predicate) {
                    push(out, &c);
                }
            }
            LogicalPlan::Project { input, columns } => {
                input.collect_columns(out);
                for c in columns {
                    push(out, c);
                }
            }
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => {
                input.collect_columns(out);
                for c in group_by {
                    push(out, c);
                }
                for a in aggregates {
                    if let Some(c) = &a.column {
                        push(out, c);
                    }
                }
            }
            LogicalPlan::Sort { input, keys } => {
                input.collect_columns(out);
                for k in keys {
                    push(out, &k.column);
                }
            }
            LogicalPlan::Limit { input, .. } => input.collect_columns(out),
        }
    }
}

/// Columns referenced by a predicate, in first-seen order.
pub fn predicate_columns(p: &Predicate) -> Vec<String> {
    fn walk(p: &Predicate, out: &mut Vec<String>) {
        match p {
            Predicate::Eq(c, _)
            | Predicate::Gt(c, _)
            | Predicate::Lt(c, _)
            | Predicate::Gte(c, _)
            | Predicate::Lte(c, _) => {
                if !out.iter().any(|x| x == c) {
                    out.push(c.clone());
                }
            }
            Predicate::And(a, b) | Predicate::Or(a, b) => {
                walk(a, out);
                walk(b, out);
            }
        }
    }
    let mut out = Vec::new();
    walk(p, &mut out);
    out
}

pub struct Planner;

impl Planner {
    pub fn new() -> Self {
        Self
    }

    /// Parse and plan in one step, including the `AS OF` clause (LAKE-TT-001/002).
    pub fn plan_sql(&self, sql: &str) -> Result<LogicalPlan> {
        let (stmt, as_of) = Parser::new().parse_with_as_of(sql)?;
        self.plan_with_as_of(stmt, as_of)
    }

    pub fn plan(&self, stmt: Statement) -> Result<LogicalPlan> {
        self.plan_with_as_of(stmt, None)
    }

    pub fn plan_with_as_of(&self, stmt: Statement, as_of: Option<AsOf>) -> Result<LogicalPlan> {
        match stmt {
            Statement::Query(query) => self.plan_query(*query, as_of),
            _ => Err(RustHouseError::SqlParse(
                "Only SELECT queries supported".to_string(),
            )),
        }
    }

    fn plan_query(&self, query: Query, as_of: Option<AsOf>) -> Result<LogicalPlan> {
        let SetExpr::Select(select) = *query.body else {
            return Err(RustHouseError::SqlParse(
                "Only simple SELECT supported".to_string(),
            ));
        };
        let mut plan = self.plan_select(*select, as_of)?;
        if !query.order_by.is_empty() {
            let keys = query
                .order_by
                .iter()
                .map(|o| {
                    Ok(SortKey {
                        column: self.expr_to_column_name(&o.expr)?,
                        ascending: o.asc.unwrap_or(true),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                keys,
            };
        }
        if let Some(limit) = query.limit {
            let v = self.expr_to_value(&limit)?;
            let n = v.as_u64().ok_or_else(|| {
                RustHouseError::SqlParse("LIMIT must be a non-negative integer".into())
            })?;
            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                limit: n as usize,
            };
        }
        Ok(plan)
    }

    fn plan_select(&self, select: Select, as_of: Option<AsOf>) -> Result<LogicalPlan> {
        // Extract table name
        if select.from.is_empty() {
            return Err(RustHouseError::SqlParse("No table specified".to_string()));
        }
        if select.from.len() > 1 || !select.from[0].joins.is_empty() {
            return Err(RustHouseError::SqlParse(
                "Joins are not supported yet".to_string(),
            ));
        }

        let table_name = self.extract_table_name(&select.from[0].relation)?;

        // Start with scan
        let mut plan = LogicalPlan::Scan {
            table: table_name,
            columns: vec!["*".to_string()],
            as_of,
        };

        // Apply WHERE clause
        if let Some(selection) = select.selection {
            let predicate = self.expr_to_predicate(selection)?;
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate,
            };
        }

        // Check for aggregation
        let has_aggregates = select
            .projection
            .iter()
            .any(|item| self.is_aggregate_item(item));

        let has_group_by =
            matches!(select.group_by, GroupByExpr::Expressions(ref exprs) if !exprs.is_empty());

        if has_aggregates || has_group_by {
            // Build aggregation plan
            let group_by = match select.group_by {
                GroupByExpr::Expressions(exprs) => exprs
                    .iter()
                    .map(|expr| self.expr_to_column_name(expr))
                    .collect::<Result<Vec<_>>>()?,
                _ => Vec::new(),
            };

            let mut aggregates = Vec::new();
            for item in &select.projection {
                if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item
                {
                    if let Some(agg) = self.expr_to_aggregate(expr, item)? {
                        aggregates.push(agg);
                    }
                }
            }

            plan = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_by,
                aggregates,
            };
        } else {
            // Build projection
            let columns = self.extract_projection_columns(&select.projection)?;
            if columns != vec!["*"] {
                plan = LogicalPlan::Project {
                    input: Box::new(plan),
                    columns,
                };
            }
        }

        Ok(plan)
    }

    fn extract_table_name(&self, relation: &TableFactor) -> Result<String> {
        match relation {
            TableFactor::Table { name, .. } => Ok(name
                .0
                .iter()
                .map(|i| i.value.clone())
                .collect::<Vec<_>>()
                .join(".")),
            _ => Err(RustHouseError::SqlParse(
                "Complex table expressions not supported".to_string(),
            )),
        }
    }

    fn expr_to_predicate(&self, expr: Expr) -> Result<Predicate> {
        match expr {
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::And => {
                    let left_pred = self.expr_to_predicate(*left)?;
                    let right_pred = self.expr_to_predicate(*right)?;
                    Ok(Predicate::And(Box::new(left_pred), Box::new(right_pred)))
                }
                BinaryOperator::Or => {
                    let left_pred = self.expr_to_predicate(*left)?;
                    let right_pred = self.expr_to_predicate(*right)?;
                    Ok(Predicate::Or(Box::new(left_pred), Box::new(right_pred)))
                }
                _ => {
                    let column = self.expr_to_column_name(&left)?;
                    let value = self.expr_to_value(&right)?;

                    match op {
                        BinaryOperator::Eq => Ok(Predicate::Eq(column, value)),
                        BinaryOperator::Gt => Ok(Predicate::Gt(column, value)),
                        BinaryOperator::Lt => Ok(Predicate::Lt(column, value)),
                        BinaryOperator::GtEq => Ok(Predicate::Gte(column, value)),
                        BinaryOperator::LtEq => Ok(Predicate::Lte(column, value)),
                        _ => Err(RustHouseError::SqlParse(format!(
                            "Unsupported operator: {:?}",
                            op
                        ))),
                    }
                }
            },
            _ => Err(RustHouseError::SqlParse(
                "Complex predicates not yet supported".to_string(),
            )),
        }
    }

    fn expr_to_column_name(&self, expr: &Expr) -> Result<String> {
        match expr {
            Expr::Identifier(ident) => Ok(ident.value.clone()),
            Expr::CompoundIdentifier(idents) => Ok(idents
                .iter()
                .map(|i| i.value.clone())
                .collect::<Vec<_>>()
                .join(".")),
            _ => Err(RustHouseError::SqlParse(format!(
                "Expected column name, got: {:?}",
                expr
            ))),
        }
    }

    fn expr_to_value(&self, expr: &Expr) -> Result<serde_json::Value> {
        match expr {
            Expr::Value(val) => match val {
                sqlparser::ast::Value::Number(n, _) => {
                    if let Ok(i) = n.parse::<i64>() {
                        Ok(serde_json::json!(i))
                    } else if let Ok(f) = n.parse::<f64>() {
                        Ok(serde_json::json!(f))
                    } else {
                        Err(RustHouseError::SqlParse(format!("Invalid number: {}", n)))
                    }
                }
                sqlparser::ast::Value::SingleQuotedString(s)
                | sqlparser::ast::Value::DoubleQuotedString(s) => Ok(serde_json::json!(s)),
                sqlparser::ast::Value::Boolean(b) => Ok(serde_json::json!(b)),
                _ => Err(RustHouseError::SqlParse(
                    "Unsupported value type".to_string(),
                )),
            },
            _ => Err(RustHouseError::SqlParse(format!(
                "Expected value, got: {:?}",
                expr
            ))),
        }
    }

    fn is_aggregate_item(&self, item: &SelectItem) -> bool {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                self.is_aggregate_expr(expr)
            }
            _ => false,
        }
    }

    fn is_aggregate_expr(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();
                matches!(name.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX")
            }
            _ => false,
        }
    }

    fn expr_to_aggregate(&self, expr: &Expr, item: &SelectItem) -> Result<Option<AggregateExpr>> {
        if !self.is_aggregate_expr(expr) {
            return Ok(None);
        }

        if let Expr::Function(func) = expr {
            let name = func.name.to_string().to_uppercase();
            let function = match name.as_str() {
                "COUNT" => AggregateFunction::Count,
                "SUM" => AggregateFunction::Sum,
                "AVG" => AggregateFunction::Avg,
                "MIN" => AggregateFunction::Min,
                "MAX" => AggregateFunction::Max,
                _ => {
                    return Err(RustHouseError::SqlParse(format!(
                        "Unknown aggregate function: {}",
                        name
                    )))
                }
            };

            let column = if func.args.is_empty() {
                None
            } else {
                match &func.args[0] {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                        Some(self.expr_to_column_name(e)?)
                    }
                    FunctionArg::Named {
                        arg: FunctionArgExpr::Expr(e),
                        ..
                    } => Some(self.expr_to_column_name(e)?),
                    _ => None,
                }
            };

            let alias = if let SelectItem::ExprWithAlias { alias, .. } = item {
                Some(alias.value.clone())
            } else {
                None
            };

            Ok(Some(AggregateExpr {
                function,
                column,
                alias,
            }))
        } else {
            Ok(None)
        }
    }

    fn extract_projection_columns(&self, projection: &[SelectItem]) -> Result<Vec<String>> {
        let mut columns = Vec::new();
        for item in projection {
            match item {
                SelectItem::Wildcard(_) => {
                    columns.push("*".to_string());
                    break;
                }
                SelectItem::UnnamedExpr(expr) => {
                    columns.push(self.expr_to_column_name(expr)?);
                }
                SelectItem::ExprWithAlias { alias, .. } => {
                    columns.push(alias.value.clone());
                }
                _ => {
                    return Err(RustHouseError::SqlParse(
                        "Unsupported projection item".to_string(),
                    ))
                }
            }
        }
        Ok(columns)
    }
}

impl Default for Planner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::Parser;

    #[test]
    fn test_plan_simple_select() {
        let parser = Parser::new();
        let planner = Planner::new();

        let stmt = parser.parse("SELECT id, name FROM users").unwrap();
        let plan = planner.plan(stmt).unwrap();

        match plan {
            LogicalPlan::Project { columns, .. } => {
                assert_eq!(columns, vec!["id", "name"]);
            }
            _ => panic!("Expected project plan"),
        }
    }

    #[test]
    fn test_plan_select_with_filter() {
        let parser = Parser::new();
        let planner = Planner::new();

        let stmt = parser.parse("SELECT * FROM users WHERE id > 100").unwrap();
        let plan = planner.plan(stmt).unwrap();

        match plan {
            LogicalPlan::Filter { predicate, .. } => match predicate {
                Predicate::Gt(col, val) => {
                    assert_eq!(col, "id");
                    assert_eq!(val, serde_json::json!(100));
                }
                _ => panic!("Expected Gt predicate"),
            },
            _ => panic!("Expected filter plan"),
        }
    }

    #[test]
    fn test_plan_aggregation() {
        let parser = Parser::new();
        let planner = Planner::new();

        let stmt = parser
            .parse("SELECT COUNT(*), SUM(price) FROM orders GROUP BY user_id")
            .unwrap();
        let plan = planner.plan(stmt).unwrap();

        match plan {
            LogicalPlan::Aggregate {
                group_by,
                aggregates,
                ..
            } => {
                assert_eq!(group_by, vec!["user_id"]);
                assert_eq!(aggregates.len(), 2);
            }
            _ => panic!("Expected aggregate plan"),
        }
    }

    #[test]
    fn test_plan_scan_as_of_version() {
        // REQ: LAKE-TT-001
        let plan = Planner::new()
            .plan_sql("SELECT * FROM gold.t AS OF VERSION 12 WHERE id > 1 ORDER BY id DESC LIMIT 5")
            .unwrap();
        assert_eq!(plan.as_of(), Some(AsOf::Version(12)));
        assert_eq!(plan.table_name().as_deref(), Some("gold.t"));
        let LogicalPlan::Limit { input, limit } = plan else {
            panic!("expected Limit")
        };
        assert_eq!(limit, 5);
        let LogicalPlan::Sort { keys, .. } = *input else {
            panic!("expected Sort")
        };
        assert_eq!(
            keys,
            vec![SortKey {
                column: "id".into(),
                ascending: false
            }]
        );
    }

    #[test]
    fn test_referenced_columns() {
        let plan = Planner::new()
            .plan_sql("SELECT id, name FROM t WHERE age > 3 ORDER BY city")
            .unwrap();
        let mut cols = plan.referenced_columns();
        cols.sort();
        assert_eq!(cols, vec!["age", "city", "id", "name"]);
        assert_eq!(
            Planner::new()
                .plan_sql("SELECT * FROM t")
                .unwrap()
                .referenced_columns(),
            vec!["*"]
        );
    }

    #[test]
    fn test_plan_complex_predicate() {
        let parser = Parser::new();
        let planner = Planner::new();

        let stmt = parser
            .parse("SELECT * FROM users WHERE id > 10 AND name = 'test'")
            .unwrap();
        let plan = planner.plan(stmt).unwrap();

        match plan {
            LogicalPlan::Filter { predicate, .. } => {
                match predicate {
                    Predicate::And(_, _) => {
                        // Success
                    }
                    _ => panic!("Expected And predicate"),
                }
            }
            _ => panic!("Expected filter plan"),
        }
    }
}
