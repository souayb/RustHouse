//! DDL statements that carry governance metadata (GOV-CLS, API-SQL-006).
//!
//! `sqlparser`'s generic dialect does not know the classification clauses, so the trailing
//! `WITH CLASSIFICATION (...)` block is split off before the `CREATE TABLE` head is parsed,
//! and `ALTER TABLE ... ALTER COLUMN ... CLASSIFIED AS ...` is parsed by hand.
//!
//! Grammar:
//! ```text
//! CREATE TABLE ns.t (col TYPE, ...) WITH CLASSIFICATION (col LEVEL [TAG ...], ..., [* LEVEL [TAG ...]])
//! ALTER TABLE ns.t ALTER COLUMN col CLASSIFIED AS LEVEL [TAGS (TAG, ...)] [REASON '<text>']
//! ```
//! `* LEVEL` is the table-wide shorthand (GOV-CLS-012). Types: INT64|BIGINT|INT, FLOAT64|DOUBLE,
//! STRING|VARCHAR|TEXT, BOOLEAN|BOOL.

use crate::error::{Result, RustHouseError};
use crate::governance::access::{MaskKind, Privilege};
use crate::governance::classification::{Classification, Level, Tag};
use crate::lake::schema::{Field, Schema};
use crate::meta::ColumnType;
use sqlparser::ast::{DataType, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser as SqlParser;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq)]
pub enum DdlStatement {
    CreateTable {
        table: String,
        schema: Schema,
    },
    AlterColumnClassification {
        table: String,
        column: String,
        classification: Classification,
        reason: Option<String>,
    },
    // --- GOV-ACC ---
    Grant {
        table: String,
        role: String,
        privilege: Privilege,
    },
    Revoke {
        table: String,
        role: String,
        privilege: Privilege,
    },
    CreateRowPolicy {
        table: String,
        policy: String,
        role: String,
        predicate_sql: String,
    },
    CreateMaskingPolicy {
        table: String,
        column: String,
        policy: String,
        role: String,
        kind: MaskKind,
    },
    DropPolicy {
        table: String,
        policy: String,
    },
    ShowPolicies {
        table: String,
        as_of_version: Option<u64>,
    },
}

/// True if the statement is DDL this module handles (as opposed to a query).
pub fn is_ddl(sql: &str) -> bool {
    let up = sql.trim_start().to_ascii_uppercase();
    [
        "CREATE TABLE",
        "ALTER TABLE",
        "GRANT ",
        "REVOKE ",
        "CREATE ROW POLICY",
        "CREATE MASKING POLICY",
        "DROP POLICY",
        "SHOW POLICIES",
    ]
    .iter()
    .any(|p| up.starts_with(p))
}

pub fn parse_ddl(sql: &str) -> Result<DdlStatement> {
    let up = sql.trim().to_ascii_uppercase();
    if up.starts_with("CREATE TABLE") {
        parse_create_table(sql.trim())
    } else if up.starts_with("ALTER TABLE") {
        parse_alter_classification(sql.trim())
    } else if up.starts_with("GRANT ") || up.starts_with("REVOKE ") {
        parse_grant_revoke(sql.trim())
    } else if up.starts_with("CREATE ROW POLICY") {
        parse_row_policy(sql.trim())
    } else if up.starts_with("CREATE MASKING POLICY") {
        parse_masking_policy(sql.trim())
    } else if up.starts_with("DROP POLICY") {
        parse_drop_policy(sql.trim())
    } else if up.starts_with("SHOW POLICIES") {
        parse_show_policies(sql.trim())
    } else {
        Err(RustHouseError::SqlParse(
            "unsupported DDL; expected CREATE TABLE or ALTER TABLE".into(),
        ))
    }
}

fn find_ci(hay: &str, needle: &str) -> Option<usize> {
    hay.to_ascii_uppercase().find(&needle.to_ascii_uppercase())
}

fn parse_create_table(sql: &str) -> Result<DdlStatement> {
    let sql = sql.trim_end_matches(';').trim();
    let (head, cls_block) = match find_ci(sql, "WITH CLASSIFICATION") {
        Some(i) => (
            sql[..i].trim(),
            Some(sql[i + "WITH CLASSIFICATION".len()..].trim()),
        ),
        None => (sql, None),
    };
    let stmts = SqlParser::parse_sql(&GenericDialect {}, head)
        .map_err(|e| RustHouseError::SqlParse(e.to_string()))?;
    let Some(Statement::CreateTable { name, columns, .. }) = stmts.into_iter().next() else {
        return Err(RustHouseError::SqlParse("expected CREATE TABLE".into()));
    };
    let table = name
        .0
        .iter()
        .map(|i| i.value.clone())
        .collect::<Vec<_>>()
        .join(".");

    let mut spec: BTreeMap<String, Classification> = BTreeMap::new();
    let mut default: Option<Classification> = None;
    if let Some(block) = cls_block {
        let inner = block
            .strip_prefix('(')
            .and_then(|s| s.strip_suffix(')'))
            .ok_or_else(|| {
                RustHouseError::SqlParse("WITH CLASSIFICATION requires a parenthesised list".into())
            })?;
        for item in inner.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let mut toks = item.split_whitespace();
            let col = toks.next().unwrap();
            let level: Level = toks
                .next()
                .ok_or_else(|| RustHouseError::SqlParse(format!("missing level for column {col}")))?
                .parse()
                .map_err(|_| {
                    RustHouseError::SqlParse(format!(
                        "invalid classification level for column {col}"
                    ))
                })?;
            let mut c = Classification::new(level);
            for t in toks {
                let tag: Tag = t.parse().map_err(|_| {
                    RustHouseError::SqlParse(format!("unknown tag '{t}' for column {col}"))
                })?;
                c.tags.insert(tag);
            }
            if col == "*" {
                default = Some(c);
            } else {
                spec.insert(col.to_string(), c);
            }
        }
    }

    let mut fields = Vec::new();
    let mut missing = Vec::new();
    for col in columns {
        let ty = map_type(&col.data_type)?;
        let name = col.name.value.clone();
        match spec.remove(&name).or_else(|| default.clone()) {
            Some(c) => fields.push(Field::new(name, ty, c)),
            None => {
                missing.push(name.clone());
                fields.push(Field {
                    id: 0,
                    name,
                    column_type: ty,
                    nullable: false,
                    classification: None,
                });
            }
        }
    }
    if let Some(unknown) = spec.keys().next() {
        return Err(RustHouseError::SqlParse(format!(
            "classification names unknown column '{unknown}'"
        )));
    }
    if !missing.is_empty() {
        return Err(RustHouseError::MissingClassification(format!(
            "columns without classification: {}",
            missing.join(", ")
        )));
    }
    Ok(DdlStatement::CreateTable {
        table,
        schema: Schema::new(fields),
    })
}

fn map_type(dt: &DataType) -> Result<ColumnType> {
    Ok(match dt {
        DataType::Int64
        | DataType::BigInt(_)
        | DataType::Int(_)
        | DataType::Integer(_)
        | DataType::Int8(_)
        | DataType::UnsignedBigInt(_) => ColumnType::Int64,
        DataType::Float64
        | DataType::Double
        | DataType::DoublePrecision
        | DataType::Float(_)
        | DataType::Real => ColumnType::Float64,
        DataType::String(_) | DataType::Varchar(_) | DataType::Text | DataType::Char(_) => {
            ColumnType::String
        }
        DataType::Boolean | DataType::Bool => ColumnType::Boolean,
        other => {
            return Err(RustHouseError::SqlParse(format!(
                "unsupported column type {other}"
            )))
        }
    })
}

/// Case-insensitive tokenizer that keeps single-quoted strings and parenthesised groups whole.
fn tokens(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' => {
                if !cur.is_empty() {
                    out.push(std::mem::take(&mut cur));
                }
                let mut lit = String::from("'");
                for c2 in chars.by_ref() {
                    lit.push(c2);
                    if c2 == '\'' {
                        break;
                    }
                }
                out.push(lit);
            }
            '(' => {
                if !cur.is_empty() {
                    out.push(std::mem::take(&mut cur));
                }
                let mut grp = String::from("(");
                let mut depth = 1;
                for c2 in chars.by_ref() {
                    grp.push(c2);
                    if c2 == '(' {
                        depth += 1;
                    } else if c2 == ')' {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                }
                out.push(grp);
            }
            c if c.is_whitespace() || c == ';' => {
                if !cur.is_empty() {
                    out.push(std::mem::take(&mut cur));
                }
            }
            c => cur.push(c),
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

fn parse_alter_classification(sql: &str) -> Result<DdlStatement> {
    let t = tokens(sql);
    let up: Vec<String> = t.iter().map(|s| s.to_ascii_uppercase()).collect();
    let bad = |m: &str| RustHouseError::SqlParse(format!("ALTER TABLE: {m}"));
    // ALTER TABLE <t> ALTER COLUMN <c> CLASSIFIED AS <Level> [TAGS (..)] [REASON '..']
    if up.len() < 8
        || up[0] != "ALTER"
        || up[1] != "TABLE"
        || up[3] != "ALTER"
        || up[4] != "COLUMN"
        || up[6] != "CLASSIFIED"
        || up[7] != "AS"
    {
        return Err(bad(
            "expected ALTER TABLE <table> ALTER COLUMN <column> CLASSIFIED AS <level> [TAGS (...)] [REASON '...']",
        ));
    }
    let table = t[2].clone();
    let column = t[5].clone();
    let level: Level = t
        .get(8)
        .ok_or_else(|| bad("missing level"))?
        .parse()
        .map_err(|_| bad("invalid level"))?;
    let mut classification = Classification::new(level);
    let mut reason = None;
    let mut i = 9;
    while i < t.len() {
        match up[i].as_str() {
            "TAGS" => {
                let grp = t.get(i + 1).ok_or_else(|| bad("TAGS requires a list"))?;
                let inner = grp
                    .strip_prefix('(')
                    .and_then(|s| s.strip_suffix(')'))
                    .ok_or_else(|| bad("TAGS requires a parenthesised list"))?;
                for tag in inner.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                    classification.tags.insert(
                        tag.parse()
                            .map_err(|_| bad(&format!("unknown tag '{tag}'")))?,
                    );
                }
                i += 2;
            }
            "REASON" => {
                let lit = t
                    .get(i + 1)
                    .ok_or_else(|| bad("REASON requires a quoted string"))?;
                let text = lit
                    .strip_prefix('\'')
                    .and_then(|s| s.strip_suffix('\''))
                    .ok_or_else(|| bad("REASON requires a quoted string"))?;
                if text.trim().is_empty() {
                    return Err(bad("REASON must not be empty"));
                }
                reason = Some(text.to_string());
                i += 2;
            }
            other => return Err(bad(&format!("unexpected token '{other}'"))),
        }
    }
    Ok(DdlStatement::AlterColumnClassification {
        table,
        column,
        classification,
        reason,
    })
}

fn group_inner<'a>(tok: &'a str, what: &str) -> Result<&'a str> {
    tok.strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .ok_or_else(|| RustHouseError::SqlParse(format!("{what} requires a parenthesised list")))
}

fn parse_privilege(t: &[String], i: &mut usize) -> Result<Privilege> {
    let kw = t
        .get(*i)
        .map(|s| s.to_ascii_uppercase())
        .unwrap_or_default();
    *i += 1;
    let group = t.get(*i).filter(|g| g.starts_with('(')).cloned();
    if group.is_some() {
        *i += 1;
    }
    match (kw.as_str(), group) {
        ("SELECT", None) => Ok(Privilege::Select),
        ("SELECT", Some(g)) => {
            let columns: BTreeSet<String> = group_inner(&g, "SELECT(...)")?
                .split(',')
                .map(|c| c.trim().to_string())
                .filter(|c| !c.is_empty())
                .collect();
            if columns.is_empty() {
                return Err(RustHouseError::SqlParse("SELECT(...) needs at least one column".into()));
            }
            Ok(Privilege::SelectColumns { columns })
        }
        ("INSERT", None) => Ok(Privilege::Insert),
        ("ALTER", None) => Ok(Privilege::Alter),
        ("DELETE", None) => Ok(Privilege::Delete),
        ("UNMASK", Some(g)) => Ok(Privilege::Unmask {
            column: group_inner(&g, "UNMASK(...)")?.trim().to_string(),
        }),
        (other, _) => Err(RustHouseError::SqlParse(format!(
            "unknown privilege '{other}'; expected SELECT[(cols)] | INSERT | ALTER | DELETE | UNMASK(col)"
        ))),
    }
}

/// `GRANT <priv> ON <ns.table> TO ROLE <role>` / `REVOKE <priv> ON <ns.table> FROM ROLE <role>`
fn parse_grant_revoke(sql: &str) -> Result<DdlStatement> {
    let t = tokens(sql);
    let up: Vec<String> = t.iter().map(|s| s.to_ascii_uppercase()).collect();
    let is_grant = up[0] == "GRANT";
    let bad = |m: &str| {
        RustHouseError::SqlParse(format!(
            "{}: {m}",
            if is_grant { "GRANT" } else { "REVOKE" }
        ))
    };
    let mut i = 1;
    let privilege = parse_privilege(&t, &mut i)?;
    if up.get(i).map(String::as_str) != Some("ON") {
        return Err(bad("expected ON <namespace.table>"));
    }
    let table = t.get(i + 1).ok_or_else(|| bad("missing table"))?.clone();
    let link = if is_grant { "TO" } else { "FROM" };
    if up.get(i + 2).map(String::as_str) != Some(link)
        || up.get(i + 3).map(String::as_str) != Some("ROLE")
    {
        return Err(bad(&format!("expected {link} ROLE <role>")));
    }
    let role = t.get(i + 4).ok_or_else(|| bad("missing role"))?.clone();
    if t.len() > i + 5 {
        return Err(bad("unexpected trailing tokens"));
    }
    Ok(if is_grant {
        DdlStatement::Grant {
            table,
            role,
            privilege,
        }
    } else {
        DdlStatement::Revoke {
            table,
            role,
            privilege,
        }
    })
}

/// `CREATE ROW POLICY <p> ON <ns.table> FOR ROLE <r> USING (<predicate>)`
fn parse_row_policy(sql: &str) -> Result<DdlStatement> {
    let t = tokens(sql);
    let up: Vec<String> = t.iter().map(|s| s.to_ascii_uppercase()).collect();
    let bad = |m: &str| RustHouseError::SqlParse(format!("CREATE ROW POLICY: {m}"));
    if up.len() != 11
        || up[3..]
            .iter()
            .zip(["", "ON", "", "FOR", "ROLE", "", "USING", ""])
            .any(|(a, b)| !b.is_empty() && a != b)
    {
        return Err(bad("expected CREATE ROW POLICY <name> ON <namespace.table> FOR ROLE <role> USING (<predicate>)"));
    }
    let predicate_sql = group_inner(&t[10], "USING")?.trim().to_string();
    if predicate_sql.is_empty() {
        return Err(bad("empty predicate"));
    }
    Ok(DdlStatement::CreateRowPolicy {
        policy: t[3].clone(),
        table: t[5].clone(),
        role: t[8].clone(),
        predicate_sql,
    })
}

/// `CREATE MASKING POLICY <p> ON <ns.table.column> FOR ROLE <r> AS hash|redact|partial(last4)|null`
fn parse_masking_policy(sql: &str) -> Result<DdlStatement> {
    let t = tokens(sql);
    let up: Vec<String> = t.iter().map(|s| s.to_ascii_uppercase()).collect();
    let bad = |m: &str| RustHouseError::SqlParse(format!("CREATE MASKING POLICY: {m}"));
    if up.len() < 10 || up[4] != "ON" || up[6] != "FOR" || up[7] != "ROLE" || up[9] != "AS" {
        return Err(bad("expected CREATE MASKING POLICY <name> ON <namespace.table.column> FOR ROLE <role> AS <kind>"));
    }
    let kind = MaskKind::parse(&t[10..].join(""))?;
    let target = &t[5];
    let (table, column) = target
        .rsplit_once('.')
        .filter(|(tbl, _)| tbl.contains('.'))
        .ok_or_else(|| bad("target must be namespace.table.column"))?;
    Ok(DdlStatement::CreateMaskingPolicy {
        table: table.to_string(),
        column: column.to_string(),
        policy: t[3].clone(),
        role: t[8].clone(),
        kind,
    })
}

/// `DROP POLICY <p> ON <ns.table>`
fn parse_drop_policy(sql: &str) -> Result<DdlStatement> {
    let t = tokens(sql);
    if t.len() != 5 || !t[3].eq_ignore_ascii_case("ON") {
        return Err(RustHouseError::SqlParse(
            "DROP POLICY: expected DROP POLICY <name> ON <namespace.table>".into(),
        ));
    }
    Ok(DdlStatement::DropPolicy {
        policy: t[2].clone(),
        table: t[4].clone(),
    })
}

/// `SHOW POLICIES ON <ns.table> [AS OF VERSION <n>]`
fn parse_show_policies(sql: &str) -> Result<DdlStatement> {
    let t = tokens(sql);
    let up: Vec<String> = t.iter().map(|s| s.to_ascii_uppercase()).collect();
    let bad = |m: &str| RustHouseError::SqlParse(format!("SHOW POLICIES: {m}"));
    if t.len() < 4 || up[2] != "ON" {
        return Err(bad(
            "expected SHOW POLICIES ON <namespace.table> [AS OF VERSION <n>]",
        ));
    }
    let as_of_version = match t.len() {
        4 => None,
        8 if up[4] == "AS" && up[5] == "OF" && up[6] == "VERSION" => Some(
            t[7].parse::<u64>()
                .map_err(|_| bad("AS OF VERSION expects an integer"))?,
        ),
        _ => return Err(bad("expected [AS OF VERSION <n>]")),
    };
    Ok(DdlStatement::ShowPolicies {
        table: t[3].clone(),
        as_of_version,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_policy_ddl_parsing() {
        // REQ: GOV-ACC-003, GOV-ACC-004, GOV-ACC-005, GOV-ACC-007
        assert_eq!(
            parse_ddl("GRANT SELECT(id, balance) ON gold.accounts TO ROLE analyst").unwrap(),
            DdlStatement::Grant {
                table: "gold.accounts".into(),
                role: "analyst".into(),
                privilege: Privilege::SelectColumns {
                    columns: ["id".to_string(), "balance".to_string()].into()
                },
            }
        );
        assert!(matches!(
            parse_ddl("REVOKE UNMASK(pan) ON gold.payments FROM ROLE fraud_lead").unwrap(),
            DdlStatement::Revoke {
                privilege: Privilege::Unmask { .. },
                ..
            }
        ));
        assert_eq!(
            parse_ddl("CREATE ROW POLICY fr ON gold.accounts FOR ROLE analyst USING (country = current_attribute('country'))").unwrap(),
            DdlStatement::CreateRowPolicy {
                table: "gold.accounts".into(),
                policy: "fr".into(),
                role: "analyst".into(),
                predicate_sql: "country = current_attribute('country')".into(),
            }
        );
        assert_eq!(
            parse_ddl(
                "CREATE MASKING POLICY m ON gold.payments.pan FOR ROLE analyst AS partial(last4)"
            )
            .unwrap(),
            DdlStatement::CreateMaskingPolicy {
                table: "gold.payments".into(),
                column: "pan".into(),
                policy: "m".into(),
                role: "analyst".into(),
                kind: MaskKind::PartialLast4,
            }
        );
        assert!(
            parse_ddl("CREATE MASKING POLICY m ON payments.pan FOR ROLE r AS hash").is_err(),
            "needs namespace"
        );
        assert_eq!(
            parse_ddl("SHOW POLICIES ON gold.accounts AS OF VERSION 6").unwrap(),
            DdlStatement::ShowPolicies {
                table: "gold.accounts".into(),
                as_of_version: Some(6)
            }
        );
        assert!(matches!(
            parse_ddl("DROP POLICY m ON gold.accounts").unwrap(),
            DdlStatement::DropPolicy { .. }
        ));
        assert!(parse_ddl("GRANT FLY ON gold.accounts TO ROLE r").is_err());
    }

    #[test]
    fn test_create_table_with_classification_block() {
        // REQ: GOV-CLS-001, API-SQL-006
        let d = parse_ddl(
            "CREATE TABLE silver.x (id INT64, iban STRING, ok BOOLEAN, amt FLOAT64) \
             WITH CLASSIFICATION (id Internal, iban Restricted PII, ok Public, amt Confidential FINANCIAL)",
        )
        .unwrap();
        let DdlStatement::CreateTable { table, schema } = d else {
            panic!()
        };
        assert_eq!(table, "silver.x");
        assert_eq!(schema.fields.len(), 4);
        let iban = schema.field("iban").unwrap();
        assert_eq!(iban.column_type, ColumnType::String);
        assert_eq!(iban.classification().unwrap().level, Level::Restricted);
        assert!(iban.classification().unwrap().tags.contains(&Tag::Pii));
        assert_eq!(
            schema.field("amt").unwrap().column_type,
            ColumnType::Float64
        );
    }

    #[test]
    fn test_create_table_without_classification_is_refused() {
        // REQ: GOV-CLS-001
        let err = parse_ddl("CREATE TABLE silver.y (id INT64, iban STRING)").unwrap_err();
        match err {
            RustHouseError::MissingClassification(m) => {
                assert!(m.contains("id") && m.contains("iban"), "{m}")
            }
            o => panic!("{o:?}"),
        }
        let err = parse_ddl(
            "CREATE TABLE silver.y (id INT64, iban STRING) WITH CLASSIFICATION (id Internal)",
        )
        .unwrap_err();
        assert!(matches!(err, RustHouseError::MissingClassification(_)));
    }

    #[test]
    fn test_create_table_wildcard_default() {
        // REQ: GOV-CLS-012
        let d = parse_ddl(
            "CREATE TABLE bronze.raw (a INT64, b STRING) WITH CLASSIFICATION (* Restricted)",
        )
        .unwrap();
        let DdlStatement::CreateTable { schema, .. } = d else {
            panic!()
        };
        assert!(schema
            .fields
            .iter()
            .all(|f| f.classification().unwrap().level == Level::Restricted));
    }

    #[test]
    fn test_alter_column_classification() {
        // REQ: GOV-CLS-005, GOV-CLS-011
        let d = parse_ddl(
            "ALTER TABLE silver.customers ALTER COLUMN email CLASSIFIED AS Restricted TAGS (PII, SPI) REASON 'DPO ticket 123'",
        )
        .unwrap();
        assert_eq!(
            d,
            DdlStatement::AlterColumnClassification {
                table: "silver.customers".into(),
                column: "email".into(),
                classification: Classification::new(Level::Restricted)
                    .with_tags([Tag::Pii, Tag::Spi]),
                reason: Some("DPO ticket 123".into()),
            }
        );
        let d = parse_ddl("ALTER TABLE s.t ALTER COLUMN c CLASSIFIED AS Internal").unwrap();
        assert!(matches!(
            d,
            DdlStatement::AlterColumnClassification { reason: None, .. }
        ));
        assert!(
            parse_ddl("ALTER TABLE s.t ALTER COLUMN c CLASSIFIED AS Internal REASON ''").is_err()
        );
        assert!(parse_ddl("ALTER TABLE s.t DROP COLUMN c").is_err());
    }
}
