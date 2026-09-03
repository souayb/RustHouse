//! Acceptance tests for specs/features/CORE-STO-segment-storage.md

use rusthouse::meta::{Column, ColumnType};
use rusthouse::storage::{ColumnData, SegmentWriter};
use rusthouse::RustHouseError;
use std::collections::HashMap;

#[tokio::test]
async fn core_sto_006_type_mismatch_refused() {
    // REQ: CORE-STO-006
    let dir = tempfile::tempdir().unwrap();
    let columns = vec![Column {
        name: "id".to_string(),
        column_type: ColumnType::Int64,
    }];
    let writer = SegmentWriter::new(dir.path().join("seg_0"), columns, 3);

    let mut data = HashMap::new();
    data.insert("id".to_string(), ColumnData::String(vec!["a".to_string()]));

    let err = writer.write(data).await.unwrap_err();
    assert!(
        matches!(err, RustHouseError::TypeMismatch(_)),
        "got {err:?}"
    );
    assert!(
        !dir.path().join("seg_0").join("col_0.bin").exists(),
        "no column file may be produced on refusal"
    );
}
