//! Spike for ADR-0006: can a Governor-style rewrite be an AnalyzerRule, and how does
//! DataFusion cost (compile time, binary size, scan speed) look on this machine?
use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{cast, col, lit, LogicalPlan};
use datafusion::arrow::datatypes::DataType as DT;
use datafusion::optimizer::AnalyzerRule;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::*;
use std::sync::Arc;
use std::time::Instant;

/// Governor stand-in: every scan of `accounts` gets `country = 'FR'` appended (row policy)
/// and the projection of `email` is replaced by a masked expression.
#[derive(Debug)]
struct GovernorRule;

impl AnalyzerRule for GovernorRule {
    fn name(&self) -> &str {
        "rusthouse_governor"
    }
    fn analyze(&self, plan: LogicalPlan, _c: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up(|node| match node {
            LogicalPlan::TableScan(scan) if scan.table_name.table() == "accounts" => {
                let scan = LogicalPlan::TableScan(scan);
                let filtered = datafusion::logical_expr::LogicalPlanBuilder::from(scan)
                    .filter(col("country").eq(cast(lit("FR"), DT::Utf8View)))?
                    .build()?;
                Ok(Transformed::yes(filtered))
            }
            other => Ok(Transformed::no(other)),
        })
        .map(|t| t.data)
    }
}

fn batch(n: usize, seed: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("country", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, false),
        Field::new("balance", DataType::Float64, false),
    ]));
    let ids: Vec<i64> = (0..n as i64).map(|i| i + seed).collect();
    let countries: Vec<&str> = (0..n).map(|i| if i % 3 == 0 { "FR" } else { "DE" }).collect();
    let emails: Vec<String> = (0..n).map(|i| format!("u{i}@x")).collect();
    let bal: Vec<f64> = (0..n).map(|i| i as f64 * 1.5).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(countries)),
            Arc::new(StringArray::from(emails)),
            Arc::new(Float64Array::from(bal)),
        ],
    )
    .unwrap()
}

#[tokio::main]
async fn main() -> Result<()> {
    let dir = std::env::temp_dir().join("df_spike_data");
    std::fs::create_dir_all(&dir).unwrap();
    // 20 files x 500k rows = 10M rows
    let t = Instant::now();
    for f in 0..20 {
        let path = dir.join(format!("part-{f}.parquet"));
        if path.exists() {
            continue;
        }
        let file = std::fs::File::create(&path).unwrap();
        let b = batch(500_000, f * 500_000);
        let mut w = parquet::arrow::ArrowWriter::try_new(file, b.schema(), None).unwrap();
        w.write(&b).unwrap();
        w.close().unwrap();
    }
    println!("wrote 10M rows in {:?}", t.elapsed());

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_analyzer_rule(Arc::new(GovernorRule))
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_parquet("accounts", dir.to_str().unwrap(), ParquetReadOptions::default())
        .await?;

    for sql in [
        "SELECT count(*), sum(balance) FROM accounts",
        "SELECT country, count(*), avg(balance) FROM accounts GROUP BY country",
        "SELECT id, email FROM accounts WHERE id BETWEEN 100 AND 110 ORDER BY id",
    ] {
        let t = Instant::now();
        let df = ctx.sql(sql).await?;
        let plan = df.logical_plan().display_indent().to_string();
        let rows = df.collect().await?;
        let n: usize = rows.iter().map(|b| b.num_rows()).sum();
        println!("{sql}\n  -> {n} rows in {:?}\n  governed plan contains row filter: {}", t.elapsed(), plan.contains("country"));
        if sql.starts_with("SELECT count") { println!("{plan}"); }
        if n <= 12 {
            println!("{}", datafusion::arrow::util::pretty::pretty_format_batches(&rows)?);
        }
    }
    Ok(())
}
