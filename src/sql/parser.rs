use crate::error::{Result, RustHouseError};
use crate::lake::log::AsOf;
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

    /// Parse a statement after extracting a trailing table `AS OF VERSION n` /
    /// `AS OF TIMESTAMP '<rfc3339>'` clause (LAKE-TT-001/002). `sqlparser` 0.43 has no
    /// temporal-table syntax, so the clause is removed textually before parsing.
    pub fn parse_with_as_of(&self, sql: &str) -> Result<(Statement, Option<AsOf>)> {
        let (stripped, as_of) = strip_as_of(sql)?;
        Ok((self.parse(&stripped)?, as_of))
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

/// Remove the first `AS OF ...` clause from `sql` and return the parsed target.
pub fn strip_as_of(sql: &str) -> Result<(String, Option<AsOf>)> {
    let upper = sql.to_ascii_uppercase();
    let words: Vec<(usize, &str)> = upper
        .match_indices(|c: char| !c.is_whitespace())
        .fold(Vec::new(), |mut acc: Vec<(usize, usize)>, (i, _)| {
            match acc.last_mut() {
                Some(last) if last.1 == i => last.1 = i + 1,
                _ => acc.push((i, i + 1)),
            }
            acc
        })
        .into_iter()
        .map(|(a, b)| (a, &upper[a..b]))
        .collect();

    let Some(pos) = words
        .windows(2)
        .position(|w| w[0].1 == "AS" && w[1].1 == "OF")
    else {
        return Ok((sql.to_string(), None));
    };
    let start = words[pos].0;
    let kind = words
        .get(pos + 2)
        .map(|w| w.1)
        .ok_or_else(|| RustHouseError::SqlParse("AS OF requires VERSION or TIMESTAMP".into()))?;
    let (end, as_of) = match kind {
        "VERSION" => {
            let (vstart, vtext) = words.get(pos + 3).ok_or_else(|| {
                RustHouseError::SqlParse("AS OF VERSION requires a number".into())
            })?;
            let v: u64 = vtext.parse().map_err(|_| {
                RustHouseError::SqlParse(format!("AS OF VERSION expects an integer, got '{vtext}'"))
            })?;
            (vstart + vtext.len(), AsOf::Version(v))
        }
        "TIMESTAMP" => {
            let lit_start = words.get(pos + 3).map(|w| w.0).ok_or_else(|| {
                RustHouseError::SqlParse("AS OF TIMESTAMP requires a quoted string".into())
            })?;
            let rest = &sql[lit_start..];
            let inner_end = rest[1..]
                .find('\'')
                .filter(|_| rest.starts_with('\''))
                .ok_or_else(|| {
                    RustHouseError::SqlParse("AS OF TIMESTAMP requires a quoted string".into())
                })?;
            let literal = &rest[1..1 + inner_end];
            let ts = chrono::DateTime::parse_from_rfc3339(literal).map_err(|e| {
                RustHouseError::SqlParse(format!(
                    "AS OF TIMESTAMP '{literal}' must be RFC 3339 with a timezone offset (e.g. 2026-03-31T23:59:59Z): {e}"
                ))
            })?;
            (
                lit_start + inner_end + 2,
                AsOf::Timestamp(ts.timestamp_millis()),
            )
        }
        other => {
            return Err(RustHouseError::SqlParse(format!(
                "AS OF requires VERSION or TIMESTAMP, got '{other}'"
            )))
        }
    };
    let stripped = format!("{} {}", sql[..start].trim_end(), sql[end..].trim_start());
    Ok((stripped.trim().to_string(), Some(as_of)))
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
