use crate::error::{Result, RustHouseError};
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

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan {
        table: String,
        columns: Vec<String>,
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

pub struct Planner;

impl Planner {
    pub fn new() -> Self {
        Self
    }

    pub fn plan(&self, stmt: Statement) -> Result<LogicalPlan> {
        match stmt {
            Statement::Query(query) => self.plan_query(*query),
            _ => Err(RustHouseError::SqlParse(
                "Only SELECT queries supported".to_string(),
            )),
        }
    }

    fn plan_query(&self, query: Query) -> Result<LogicalPlan> {
        if let SetExpr::Select(select) = *query.body {
            self.plan_select(*select)
        } else {
            Err(RustHouseError::SqlParse(
                "Only simple SELECT supported".to_string(),
            ))
        }
    }

    fn plan_select(&self, select: Select) -> Result<LogicalPlan> {
        // Extract table name
        if select.from.is_empty() {
            return Err(RustHouseError::SqlParse("No table specified".to_string()));
        }

        let table_name = self.extract_table_name(&select.from[0].relation)?;

        // Start with scan
        let mut plan = LogicalPlan::Scan {
            table: table_name,
            columns: vec!["*".to_string()],
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
