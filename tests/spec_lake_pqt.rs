//! Acceptance tests for specs/features/LAKE-PQT-parquet-storage.md

use proptest::prelude::*;
use rusthouse::lake::parquet::{
    from_record_batch, new_part_path, row_groups_pruned, to_record_batch, ParquetReader,
    ParquetWriter,
};
use rusthouse::meta::{Column, ColumnType};
use rusthouse::storage::{ColumnData, Predicate};
use rusthouse::RustHouseError;
use serde_json::json;
use std::collections::HashMap;
use std::path::Path;

fn col(name: &str, t: ColumnType) -> Column {
    Column {
        name: name.to_string(),
        column_type: t,
    }
}

fn four_columns() -> Vec<Column> {
    vec![
        col("id", ColumnType::Int64),
        col("amount", ColumnType::Float64),
        col("name", ColumnType::String),
        col("active", ColumnType::Boolean),
    ]
}

fn four_column_data(n: usize) -> HashMap<String, ColumnData> {
    let mut d = HashMap::new();
    d.insert("id".into(), ColumnData::Int64((0..n as i64).collect()));
    d.insert(
        "amount".into(),
        ColumnData::Float64((0..n).map(|i| i as f64 * 1.5).collect()),
    );
    d.insert(
        "name".into(),
        ColumnData::String((0..n).map(|i| format!("n{i:05}")).collect()),
    );
    d.insert(
        "active".into(),
        ColumnData::Boolean((0..n).map(|i| i % 3 == 0).collect()),
    );
    d
}

async fn write(
    dir: &Path,
    columns: &[Column],
    data: HashMap<String, ColumnData>,
) -> std::path::PathBuf {
    let path = new_part_path(dir);
    ParquetWriter::default()
        .write(&path, columns, data)
        .await
        .unwrap();
    path
}

#[tokio::test]
async fn lake_pqt_001_writes_valid_zstd_parquet() {
    // REQ: LAKE-PQT-001
    use parquet::basic::Compression;
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let dir = tempfile::tempdir().unwrap();
    let path = write(dir.path(), &four_columns(), four_column_data(1000)).await;

    let reader = SerializedFileReader::new(std::fs::File::open(&path).unwrap()).unwrap();
    let meta = reader.metadata();
    assert_eq!(meta.file_metadata().num_rows(), 1000);
    assert!(meta.num_row_groups() >= 1);
    for rg in meta.row_groups() {
        for c in rg.columns() {
            assert!(
                matches!(c.compression(), Compression::ZSTD(_)),
                "column {} compression {:?}",
                c.column_path(),
                c.compression()
            );
        }
    }
}

#[tokio::test]
async fn lake_pqt_002_refuses_to_overwrite_existing_file() {
    // REQ: LAKE-PQT-002
    let dir = tempfile::tempdir().unwrap();
    let path = write(dir.path(), &four_columns(), four_column_data(10)).await;
    let before = std::fs::read(&path).unwrap();
    let err = ParquetWriter::default()
        .write(&path, &four_columns(), four_column_data(5))
        .await
        .unwrap_err();
    assert!(matches!(err, RustHouseError::Storage(_)), "{err:?}");
    assert_eq!(
        std::fs::read(&path).unwrap(),
        before,
        "original bytes unchanged"
    );
    let name = path.file_name().unwrap().to_str().unwrap();
    assert!(
        name.starts_with("part-") && name.ends_with(".parquet"),
        "{name}"
    );
}

#[tokio::test]
async fn lake_pqt_003_file_stats_match_columndata_stats() {
    // REQ: LAKE-PQT-003
    let dir = tempfile::tempdir().unwrap();
    let columns = vec![
        col("id", ColumnType::Int64),
        col("name", ColumnType::String),
    ];
    let mut data = HashMap::new();
    data.insert("id".to_string(), ColumnData::Int64(vec![5, 1, 9]));
    data.insert(
        "name".to_string(),
        ColumnData::String(vec!["b".into(), "a".into(), "c".into()]),
    );
    let expected_id = data["id"].stats();
    let expected_name = data["name"].stats();
    let path = new_part_path(dir.path());
    let stats = ParquetWriter::default()
        .write(&path, &columns, data)
        .await
        .unwrap();
    assert_eq!(stats.num_records, 3);
    assert_eq!(stats.size_bytes, std::fs::metadata(&path).unwrap().len());
    let id = &stats.columns["id"];
    assert_eq!((id.min.clone(), id.max.clone()), (json!(1), json!(9)));
    assert_eq!((id.min.clone(), id.max.clone()), expected_id);
    assert_eq!(id.null_count, 0);
    let name = &stats.columns["name"];
    assert_eq!(
        (name.min.clone(), name.max.clone()),
        (json!("a"), json!("c"))
    );
    assert_eq!((name.min.clone(), name.max.clone()), expected_name);
    assert_eq!(name.null_count, 0);
}

#[test]
fn lake_pqt_004_record_batch_bridge_roundtrip() {
    // REQ: LAKE-PQT-004
    let columns = four_columns();
    let data = four_column_data(17);
    let batch = to_record_batch(&columns, data.clone()).unwrap();
    assert_eq!(batch.num_rows(), 17);
    assert_eq!(batch.num_columns(), 4);
    let back = from_record_batch(&batch).unwrap();
    assert_eq!(back.len(), 4);
    for (k, v) in &data {
        assert_eq!(format!("{:?}", back[k]), format!("{:?}", v), "column {k}");
    }
    // Nullable columns also round-trip through the bridge.
    let ncols = vec![col("v", ColumnType::Int64)];
    let mut nd = HashMap::new();
    nd.insert(
        "v".to_string(),
        ColumnData::NullableInt64(vec![Some(1), None, Some(3)]),
    );
    let batch = to_record_batch(&ncols, nd.clone()).unwrap();
    let back = from_record_batch(&batch).unwrap();
    assert_eq!(format!("{:?}", back["v"]), format!("{:?}", nd["v"]));
}

#[tokio::test]
async fn lake_pqt_005_type_mismatch_rejected() {
    // REQ: LAKE-PQT-005
    let dir = tempfile::tempdir().unwrap();
    let columns = vec![col("id", ColumnType::Int64)];
    let mut data = HashMap::new();
    data.insert("id".to_string(), ColumnData::String(vec!["a".into()]));
    let path = new_part_path(dir.path());
    let err = ParquetWriter::default()
        .write(&path, &columns, data)
        .await
        .unwrap_err();
    assert!(matches!(err, RustHouseError::TypeMismatch(_)), "{err:?}");
    assert!(!path.exists(), "no file may be created on refusal");
}

#[tokio::test]
async fn lake_pqt_006_projection_reads_only_requested_columns() {
    // REQ: LAKE-PQT-006
    let dir = tempfile::tempdir().unwrap();
    let path = write(dir.path(), &four_columns(), four_column_data(100)).await;
    let out = ParquetReader::read(&path, &["id".to_string()])
        .await
        .unwrap();
    assert_eq!(out.keys().collect::<Vec<_>>(), vec!["id"]);
    assert_eq!(out["id"].len(), 100);
    let err = ParquetReader::read(&path, &["nope".to_string()])
        .await
        .unwrap_err();
    assert!(
        matches!(err, RustHouseError::ColumnNotFound(ref c) if c == "nope"),
        "{err:?}"
    );
}

#[tokio::test]
async fn lake_pqt_007_row_group_pruning_mirrors_segment_pruner() {
    // REQ: LAKE-PQT-007
    let dir = tempfile::tempdir().unwrap();
    let columns = vec![col("id", ColumnType::Int64)];
    let mut data = HashMap::new();
    data.insert("id".to_string(), ColumnData::Int64((1..=300).collect()));
    let path = new_part_path(dir.path());
    let writer = ParquetWriter {
        max_rows_per_group: 100,
        ..ParquetWriter::default()
    };
    writer.write(&path, &columns, data).await.unwrap();

    let before = row_groups_pruned();
    let out = ParquetReader::read_filtered(
        &path,
        &["id".to_string()],
        &Predicate::Eq("id".into(), json!(150)),
    )
    .await
    .unwrap();
    assert_eq!(
        row_groups_pruned() - before,
        2,
        "two of three row groups pruned"
    );
    match &out["id"] {
        ColumnData::Int64(v) => {
            assert_eq!(v.first(), Some(&101));
            assert_eq!(v.last(), Some(&200));
            assert_eq!(v.len(), 100);
        }
        other => panic!("{other:?}"),
    }

    // No match at all: every group pruned, empty column returned.
    let before = row_groups_pruned();
    let out = ParquetReader::read_filtered(
        &path,
        &["id".to_string()],
        &Predicate::Gt("id".into(), json!(1000)),
    )
    .await
    .unwrap();
    assert_eq!(row_groups_pruned() - before, 3);
    assert_eq!(out["id"].len(), 0);

    // OR spanning two groups keeps exactly two.
    let before = row_groups_pruned();
    let out = ParquetReader::read_filtered(
        &path,
        &["id".to_string()],
        &Predicate::Or(
            Box::new(Predicate::Lt("id".into(), json!(50))),
            Box::new(Predicate::Gte("id".into(), json!(250))),
        ),
    )
    .await
    .unwrap();
    assert_eq!(row_groups_pruned() - before, 1);
    assert_eq!(out["id"].len(), 200);
}

#[tokio::test]
async fn lake_pqt_008_nulls_supported_and_excluded_from_minmax() {
    // REQ: LAKE-PQT-008
    let dir = tempfile::tempdir().unwrap();
    let columns = vec![
        col("id", ColumnType::Int64),
        col("all_null", ColumnType::String),
    ];
    let mut data = HashMap::new();
    data.insert(
        "id".to_string(),
        ColumnData::NullableInt64(vec![None, Some(3), None, Some(7)]),
    );
    data.insert(
        "all_null".to_string(),
        ColumnData::NullableString(vec![None, None, None, None]),
    );
    assert_eq!(data["id"].null_count(), 2);
    let path = new_part_path(dir.path());
    let stats = ParquetWriter::default()
        .write(&path, &columns, data.clone())
        .await
        .unwrap();
    let id = &stats.columns["id"];
    assert_eq!(id.null_count, 2);
    assert_eq!(id.min, json!(3));
    assert_eq!(id.max, json!(7));
    let an = &stats.columns["all_null"];
    assert_eq!(an.null_count, 4);
    assert_eq!(an.min, json!(null));
    assert_eq!(an.max, json!(null));

    let back = ParquetReader::read(&path, &["id".into(), "all_null".into()])
        .await
        .unwrap();
    assert_eq!(format!("{:?}", back["id"]), format!("{:?}", data["id"]));
    assert_eq!(
        format!("{:?}", back["all_null"]),
        format!("{:?}", data["all_null"])
    );
}

#[tokio::test]
async fn lake_pqt_010_empty_batch_is_valid() {
    // REQ: LAKE-PQT-010
    let dir = tempfile::tempdir().unwrap();
    let path = new_part_path(dir.path());
    let stats = ParquetWriter::default()
        .write(&path, &four_columns(), four_column_data(0))
        .await
        .unwrap();
    assert_eq!(stats.num_records, 0);
    for c in stats.columns.values() {
        assert_eq!(c.min, json!(null));
        assert_eq!(c.max, json!(null));
        assert_eq!(c.null_count, 0);
    }
    let names: Vec<String> = four_columns().iter().map(|c| c.name.clone()).collect();
    let back = ParquetReader::read(&path, &names).await.unwrap();
    assert_eq!(back.len(), 4);
    for c in back.values() {
        assert_eq!(c.len(), 0);
    }
}

fn arb_column(t: ColumnType, n: usize) -> BoxedStrategy<ColumnData> {
    match t {
        ColumnType::Int64 => prop_oneof![
            prop::collection::vec(any::<i64>(), n).prop_map(ColumnData::Int64),
            prop::collection::vec(prop::option::of(any::<i64>()), n)
                .prop_map(ColumnData::NullableInt64),
        ]
        .boxed(),
        ColumnType::Float64 => prop_oneof![
            prop::collection::vec(-1e12f64..1e12, n).prop_map(ColumnData::Float64),
            prop::collection::vec(prop::option::of(-1e12f64..1e12), n)
                .prop_map(ColumnData::NullableFloat64),
        ]
        .boxed(),
        ColumnType::String => prop_oneof![
            prop::collection::vec("[a-z]{0,8}", n).prop_map(ColumnData::String),
            prop::collection::vec(prop::option::of("[a-z]{0,8}"), n)
                .prop_map(ColumnData::NullableString),
        ]
        .boxed(),
        ColumnType::Boolean => prop_oneof![
            prop::collection::vec(any::<bool>(), n).prop_map(ColumnData::Boolean),
            prop::collection::vec(prop::option::of(any::<bool>()), n)
                .prop_map(ColumnData::NullableBoolean),
        ]
        .boxed(),
    }
}

fn arb_batch() -> impl Strategy<Value = HashMap<String, ColumnData>> {
    (0usize..2000).prop_flat_map(|n| {
        (
            arb_column(ColumnType::Int64, n),
            arb_column(ColumnType::Float64, n),
            arb_column(ColumnType::String, n),
            arb_column(ColumnType::Boolean, n),
        )
            .prop_map(|(a, b, c, d)| {
                let mut m = HashMap::new();
                m.insert("id".to_string(), a);
                m.insert("amount".to_string(), b);
                m.insert("name".to_string(), c);
                m.insert("active".to_string(), d);
                m
            })
    })
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 32, .. ProptestConfig::default() })]
    #[test]
    fn lake_pqt_011_property_roundtrip_and_stats(data in arb_batch()) {
        // REQ: LAKE-PQT-011
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let columns = four_columns();
        let path = new_part_path(dir.path());
        let stats = rt.block_on(ParquetWriter::default().write(&path, &columns, data.clone())).unwrap();
        prop_assert_eq!(stats.num_records as usize, data["id"].len());
        for (name, cd) in &data {
            let (min, max) = cd.stats();
            let s = &stats.columns[name];
            prop_assert_eq!(&s.min, &min, "min of {}", name);
            prop_assert_eq!(&s.max, &max, "max of {}", name);
            prop_assert_eq!(s.null_count, cd.null_count());
        }
        let names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
        let back = rt.block_on(ParquetReader::read(&path, &names)).unwrap();
        for (name, cd) in &data {
            prop_assert_eq!(format!("{:?}", &back[name]), format!("{:?}", cd), "column {}", name);
        }
    }
}
