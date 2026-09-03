//! Acceptance tests for specs/features/GOV-CLS-classification.md (M1 subset).

use rusthouse::governance::classification::{Classification, Level, Tag};
use rusthouse::lake::schema::{Field, Schema};
use rusthouse::meta::ColumnType;
use rusthouse::RustHouseError;
use std::collections::BTreeSet;

fn cls(level: Level, tags: &[Tag]) -> Classification {
    Classification {
        level,
        tags: tags.iter().cloned().collect(),
    }
}

#[test]
fn gov_cls_002_level_ordering_and_tag_roundtrip() {
    // REQ: GOV-CLS-002
    assert!(Level::Restricted > Level::Confidential);
    assert!(Level::Confidential > Level::Internal);
    assert!(Level::Internal > Level::Public);

    let c = cls(
        Level::Confidential,
        &[Tag::Pii, Tag::Custom("trade_desk".into())],
    );
    let json = serde_json::to_string(&c).unwrap();
    assert!(
        json.contains("\"PII\""),
        "tags serialise as stable strings: {json}"
    );
    assert!(json.contains("\"custom:trade_desk\""), "{json}");
    let back: Classification = serde_json::from_str(&json).unwrap();
    assert_eq!(back, c);
}

#[test]
fn gov_cls_003_table_level_is_max_of_columns() {
    // REQ: GOV-CLS-003
    let schema = Schema::new(vec![
        Field::new("id", ColumnType::Int64, cls(Level::Internal, &[])),
        Field::new(
            "email",
            ColumnType::String,
            cls(Level::Confidential, &[Tag::Pii]),
        ),
        Field::new(
            "card",
            ColumnType::String,
            cls(Level::Restricted, &[Tag::Pci]),
        ),
    ]);
    let table = schema.classification().unwrap();
    assert_eq!(table.level, Level::Restricted);
    let expected: BTreeSet<Tag> = [Tag::Pii, Tag::Pci].into_iter().collect();
    assert_eq!(table.tags, expected);

    // An explicit table level lower than the computed one is refused.
    let err = schema.validate_table_level(Level::Internal).unwrap_err();
    assert!(
        matches!(err, RustHouseError::ClassificationLowering(_)),
        "{err:?}"
    );
}

#[test]
fn gov_cls_010_read_of_unclassified_metadata_fails_closed() {
    // REQ: GOV-CLS-010
    // A schema deserialised from a hand-edited log without classification metadata.
    let raw = serde_json::json!({
        "fields": [
            {"id": 1, "name": "id", "column_type": "Int64", "nullable": false},
            {"id": 2, "name": "email", "column_type": "String", "nullable": false}
        ],
        "next_column_id": 3
    });
    let schema: Schema = serde_json::from_value(raw).unwrap();
    let err = schema.require_classified().unwrap_err();
    match err {
        RustHouseError::MissingClassification(msg) => {
            assert!(msg.contains("id") && msg.contains("email"), "{msg}");
        }
        other => panic!("expected MissingClassification, got {other:?}"),
    }
    assert!(schema.classification().is_err());
}
