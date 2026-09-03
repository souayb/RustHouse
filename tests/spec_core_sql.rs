//! Acceptance tests for specs/features/CORE-SQL-parser-planner.md

use rusthouse::sql::{Parser, Planner};
use rusthouse::RustHouseError;

#[test]
fn core_sql_006_non_select_rejected() {
    // REQ: CORE-SQL-006
    let parser = Parser::new();
    let planner = Planner::new();
    for sql in [
        "INSERT INTO t (id) VALUES (1)",
        "CREATE TABLE t (id INT)",
        "DROP TABLE t",
    ] {
        let stmt = parser.parse(sql).expect("valid SQL parses");
        let err = planner.plan(stmt).unwrap_err();
        assert!(matches!(err, RustHouseError::SqlParse(_)), "{sql}: {err:?}");
    }
}
