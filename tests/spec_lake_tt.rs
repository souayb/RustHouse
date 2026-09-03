//! Acceptance tests for specs/features/LAKE-TT-time-travel.md (planner-level subset; the
//! execution scenarios live in tests/spec_api_sql.rs once the governed session exists).

use rusthouse::lake::log::AsOf;
use rusthouse::sql::{LogicalPlan, Planner};
use rusthouse::RustHouseError;

#[test]
fn lake_tt_001_as_of_version_parses_into_scan() {
    // REQ: LAKE-TT-001
    let plan = Planner::new()
        .plan_sql("SELECT id FROM gold.positions AS OF VERSION 3")
        .unwrap();
    match plan {
        LogicalPlan::Project { columns, input } => {
            assert_eq!(columns, vec!["id"]);
            match *input {
                LogicalPlan::Scan { table, as_of, .. } => {
                    assert_eq!(table, "gold.positions");
                    assert_eq!(as_of, Some(AsOf::Version(3)));
                }
                other => panic!("expected Scan, got {other:?}"),
            }
        }
        other => panic!("expected Project, got {other:?}"),
    }
    // Without the clause the scan is unpinned.
    let plan = Planner::new()
        .plan_sql("SELECT id FROM gold.positions")
        .unwrap();
    assert_eq!(plan.as_of(), None);
    assert_eq!(plan.table_name(), Some("gold.positions".to_string()));
}

#[test]
fn lake_tt_002_as_of_timestamp_requires_offset() {
    // REQ: LAKE-TT-002
    let plan = Planner::new()
        .plan_sql("SELECT * FROM gold.positions AS OF TIMESTAMP '2026-03-31T23:59:59Z'")
        .unwrap();
    let expected_ms = chrono::DateTime::parse_from_rfc3339("2026-03-31T23:59:59Z")
        .unwrap()
        .timestamp_millis();
    assert_eq!(plan.as_of(), Some(AsOf::Timestamp(expected_ms)));

    let plan = Planner::new()
        .plan_sql(
            "SELECT * FROM gold.positions AS OF TIMESTAMP '2026-03-31T23:59:59+02:00' WHERE id > 1",
        )
        .unwrap();
    assert_eq!(
        plan.as_of(),
        Some(AsOf::Timestamp(expected_ms - 2 * 3600 * 1000))
    );
    assert!(
        matches!(plan, LogicalPlan::Filter { .. }),
        "clause may precede WHERE"
    );

    let err = Planner::new()
        .plan_sql("SELECT * FROM gold.positions AS OF TIMESTAMP '2026-03-31 23:59:59'")
        .unwrap_err();
    match err {
        RustHouseError::SqlParse(msg) => {
            assert!(
                msg.to_lowercase().contains("timezone") || msg.to_lowercase().contains("offset"),
                "{msg}"
            )
        }
        other => panic!("{other:?}"),
    }
}
