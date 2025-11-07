use crate::error::{Result, RustHouseError};
use sqlparser::ast::{Query, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser as SqlParser;

pub struct Parser {
    dialect: GenericDialect,
}

impl Parser {
    pub fn new() -> Self {
        Self {
            dialect: GenericDialect {},
        }
    }

    pub fn parse(&self, sql: &str) -> Result<Statement> {
        let statements = SqlParser::parse_sql(&self.dialect, sql)
            .map_err(|e| RustHouseError::SqlParse(e.to_string()))?;

        if statements.is_empty() {
            return Err(RustHouseError::SqlParse(
                "No SQL statement found".to_string(),
            ));
        }

        if statements.len() > 1 {
            return Err(RustHouseError::SqlParse(
                "Multiple statements not supported".to_string(),
            ));
        }

        Ok(statements[0].clone())
    }

    pub fn parse_query(&self, sql: &str) -> Result<Box<Query>> {
        let stmt = self.parse(sql)?;
        match stmt {
            Statement::Query(query) => Ok(query),
            _ => Err(RustHouseError::SqlParse(
                "Expected SELECT query".to_string(),
            )),
        }
    }
}

impl Default for Parser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::SetExpr;

    #[test]
    fn test_parse_simple_select() {
        let parser = Parser::new();
        let sql = "SELECT id, name FROM users";
        let stmt = parser.parse(sql).unwrap();

        match stmt {
            Statement::Query(_) => {
                // Success
            }
            _ => panic!("Expected query statement"),
        }
    }

    #[test]
    fn test_parse_select_with_where() {
        let parser = Parser::new();
        let sql = "SELECT id, name FROM users WHERE id > 100";
        let query = parser.parse_query(sql).unwrap();

        if let SetExpr::Select(select) = *query.body {
            assert!(select.selection.is_some());
        } else {
            panic!("Expected SELECT");
        }
    }

    #[test]
    fn test_parse_select_with_aggregation() {
        let parser = Parser::new();
        let sql = "SELECT COUNT(*), SUM(price) FROM orders GROUP BY user_id";
        let query = parser.parse_query(sql).unwrap();

        if let SetExpr::Select(select) = *query.body {
            assert!(matches!(
                select.group_by,
                sqlparser::ast::GroupByExpr::Expressions(_)
            ));
        } else {
            panic!("Expected SELECT");
        }
    }

    #[test]
    fn test_parse_invalid_sql() {
        let parser = Parser::new();
        let sql = "SELECT FROM WHERE";
        let result = parser.parse(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_multiple_statements() {
        let parser = Parser::new();
        let sql = "SELECT * FROM users; SELECT * FROM orders;";
        let result = parser.parse(sql);
        // Should fail - multiple statements not supported
        assert!(result.is_err());
    }
}
