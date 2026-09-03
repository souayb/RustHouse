use rusthouse::config::Config;
use rusthouse::meta::{Column, ColumnType, Metadata, MetadataStore, Table};
use rusthouse::storage::column_writer::ColumnData;
use rusthouse::storage::{Predicate, SegmentPruner, SegmentReader, SegmentWriter};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== RustHouse Dataset Creation ===\n");

    // Setup
    let config = Config::new("./demo_data".to_string(), "127.0.0.1:8123".to_string());
    tokio::fs::create_dir_all(&config.data_dir).await?;
    tokio::fs::create_dir_all(config.metadata_path()).await?;
    tokio::fs::create_dir_all(config.segments_path()).await?;

    let metadata_store = MetadataStore::new(format!("{}/metadata.json", config.metadata_path()));
    let mut metadata = Metadata::new();

    // Create "events" table
    println!("📋 Creating 'events' table...");
    let events_table = create_events_table();
    metadata.add_table(events_table.clone());

    // Create "users" table
    println!("📋 Creating 'users' table...");
    let users_table = create_users_table();
    metadata.add_table(users_table.clone());

    // Save metadata
    metadata_store.write(&metadata).await?;
    println!("✓ Metadata saved\n");

    // Populate events table with 3 segments
    println!("📊 Populating 'events' table with dummy data...");
    populate_events_table(&config, &mut metadata).await?;

    // Populate users table with 1 segment
    println!("📊 Populating 'users' table with dummy data...");
    populate_users_table(&config, &mut metadata).await?;

    // Save updated metadata with segments
    metadata_store.write(&metadata).await?;
    println!("✓ All data created\n");

    // Display summary
    display_summary(&metadata);

    // Demonstrate querying
    println!("\n=== Query Examples ===\n");
    demonstrate_queries(&metadata).await?;

    println!("\n✅ Dataset creation complete!");
    println!("Data location: {}", config.data_dir);

    Ok(())
}

fn create_events_table() -> Table {
    let columns = vec![
        Column {
            name: "event_id".to_string(),
            column_type: ColumnType::Int64,
        },
        Column {
            name: "user_id".to_string(),
            column_type: ColumnType::Int64,
        },
        Column {
            name: "event_type".to_string(),
            column_type: ColumnType::String,
        },
        Column {
            name: "value".to_string(),
            column_type: ColumnType::Float64,
        },
        Column {
            name: "is_processed".to_string(),
            column_type: ColumnType::Boolean,
        },
    ];

    Table::new("events".to_string(), columns)
}

fn create_users_table() -> Table {
    let columns = vec![
        Column {
            name: "user_id".to_string(),
            column_type: ColumnType::Int64,
        },
        Column {
            name: "username".to_string(),
            column_type: ColumnType::String,
        },
        Column {
            name: "age".to_string(),
            column_type: ColumnType::Int64,
        },
        Column {
            name: "balance".to_string(),
            column_type: ColumnType::Float64,
        },
    ];

    Table::new("users".to_string(), columns)
}

async fn populate_events_table(
    config: &Config,
    metadata: &mut Metadata,
) -> Result<(), Box<dyn std::error::Error>> {
    let event_types = ["click", "view", "purchase", "signup", "login"];

    // Clone columns once to avoid borrow issues
    let columns = metadata.get_table("events").unwrap().columns.clone();

    // Create 3 segments with different ranges
    for segment_idx in 0..3 {
        let start_id = segment_idx * 1000;
        let end_id = start_id + 1000;

        println!(
            "  Creating segment {} (event_id: {}-{})...",
            segment_idx,
            start_id,
            end_id - 1
        );

        let mut data = HashMap::new();

        // Generate event IDs
        let event_ids: Vec<i64> = (start_id..end_id).collect();
        data.insert("event_id".to_string(), ColumnData::Int64(event_ids.clone()));

        // Generate user IDs (cycling through 1-100)
        let user_ids: Vec<i64> = event_ids.iter().map(|&id| (id % 100) + 1).collect();
        data.insert("user_id".to_string(), ColumnData::Int64(user_ids));

        // Generate event types
        let events: Vec<String> = event_ids
            .iter()
            .map(|&id| event_types[(id % 5) as usize].to_string())
            .collect();
        data.insert("event_type".to_string(), ColumnData::String(events));

        // Generate values
        let values: Vec<f64> = event_ids
            .iter()
            .map(|&id| (id as f64 * 1.5) % 1000.0)
            .collect();
        data.insert("value".to_string(), ColumnData::Float64(values));

        // Generate boolean flags
        let flags: Vec<bool> = event_ids.iter().map(|&id| id % 2 == 0).collect();
        data.insert("is_processed".to_string(), ColumnData::Boolean(flags));

        // Write segment
        let segment_path = format!("{}/events_seg_{}", config.segments_path(), segment_idx);
        let writer = SegmentWriter::new(&segment_path, columns.clone(), 3);
        let mut segment = writer.write(data).await?;
        segment.table_name = "events".to_string();

        // Add segment to table
        let table = metadata.get_table_mut("events").unwrap();
        table.add_segment(segment);

        println!("    ✓ Segment {} created (1000 rows)", segment_idx);
    }

    Ok(())
}

async fn populate_users_table(
    config: &Config,
    metadata: &mut Metadata,
) -> Result<(), Box<dyn std::error::Error>> {
    // Clone columns once to avoid borrow issues
    let columns = metadata.get_table("users").unwrap().columns.clone();

    println!("  Creating segment 0 (100 users)...");

    let mut data = HashMap::new();

    // Generate user IDs
    let user_ids: Vec<i64> = (1..=100).collect();
    data.insert("user_id".to_string(), ColumnData::Int64(user_ids.clone()));

    // Generate usernames
    let usernames: Vec<String> = user_ids.iter().map(|&id| format!("user_{}", id)).collect();
    data.insert("username".to_string(), ColumnData::String(usernames));

    // Generate ages (18-80)
    let ages: Vec<i64> = user_ids.iter().map(|&id| 18 + (id % 62)).collect();
    data.insert("age".to_string(), ColumnData::Int64(ages));

    // Generate balances
    let balances: Vec<f64> = user_ids
        .iter()
        .map(|&id| (id as f64 * 123.45) % 10000.0)
        .collect();
    data.insert("balance".to_string(), ColumnData::Float64(balances));

    // Write segment
    let segment_path = format!("{}/users_seg_0", config.segments_path());
    let writer = SegmentWriter::new(&segment_path, columns, 3);
    let mut segment = writer.write(data).await?;
    segment.table_name = "users".to_string();

    // Add segment to table
    let table = metadata.get_table_mut("users").unwrap();
    table.add_segment(segment);

    println!("    ✓ Segment 0 created (100 rows)");

    Ok(())
}

fn display_summary(metadata: &Metadata) {
    println!("=== Dataset Summary ===\n");

    for (table_name, table) in &metadata.tables {
        println!("📊 Table: {}", table_name);
        println!("   Columns: {}", table.columns.len());
        for col in &table.columns {
            println!("     - {} ({:?})", col.name, col.column_type);
        }
        println!("   Segments: {}", table.segments.len());

        let total_rows: usize = table.segments.iter().map(|s| s.stats.row_count).sum();
        let total_size: u64 = table.segments.iter().map(|s| s.size_bytes).sum();

        println!("   Total rows: {}", total_rows);
        println!(
            "   Total size: {} bytes ({:.2} KB)",
            total_size,
            total_size as f64 / 1024.0
        );

        for (idx, segment) in table.segments.iter().enumerate() {
            println!(
                "     Segment {}: {} rows, {} bytes",
                idx, segment.stats.row_count, segment.size_bytes
            );

            // Show min/max stats for first column
            if let Some(first_col) = table.columns.first() {
                if let (Some(min), Some(max)) = (
                    segment.stats.min_values.get(&first_col.name),
                    segment.stats.max_values.get(&first_col.name),
                ) {
                    println!("       {} range: {} - {}", first_col.name, min, max);
                }
            }
        }
        println!();
    }
}

async fn demonstrate_queries(metadata: &Metadata) -> Result<(), Box<dyn std::error::Error>> {
    let events_table = metadata.get_table("events").unwrap();

    // Query 1: Exact match - should prune 2 out of 3 segments
    println!("Query 1: SELECT * FROM events WHERE event_id = 1500");
    let predicate1 = Predicate::Eq("event_id".to_string(), serde_json::json!(1500));
    let pruned1 = SegmentPruner::prune_segments(&events_table.segments, &predicate1);
    println!("  Segments before pruning: {}", events_table.segments.len());
    println!("  Segments after pruning: {}", pruned1.len());
    println!(
        "  Pruning efficiency: {:.1}%\n",
        (1.0 - pruned1.len() as f64 / events_table.segments.len() as f64) * 100.0
    );

    // Query 2: Range query - should prune some segments
    println!("Query 2: SELECT * FROM events WHERE event_id > 2500");
    let predicate2 = Predicate::Gt("event_id".to_string(), serde_json::json!(2500));
    let pruned2 = SegmentPruner::prune_segments(&events_table.segments, &predicate2);
    println!("  Segments before pruning: {}", events_table.segments.len());
    println!("  Segments after pruning: {}", pruned2.len());
    println!(
        "  Pruning efficiency: {:.1}%\n",
        (1.0 - pruned2.len() as f64 / events_table.segments.len() as f64) * 100.0
    );

    // Query 3: Complex predicate
    println!("Query 3: SELECT * FROM events WHERE event_id > 500 AND event_id < 1500");
    let predicate3 = Predicate::And(
        Box::new(Predicate::Gt(
            "event_id".to_string(),
            serde_json::json!(500),
        )),
        Box::new(Predicate::Lt(
            "event_id".to_string(),
            serde_json::json!(1500),
        )),
    );
    let pruned3 = SegmentPruner::prune_segments(&events_table.segments, &predicate3);
    println!("  Segments before pruning: {}", events_table.segments.len());
    println!("  Segments after pruning: {}", pruned3.len());
    println!(
        "  Pruning efficiency: {:.1}%\n",
        (1.0 - pruned3.len() as f64 / events_table.segments.len() as f64) * 100.0
    );

    // Actually read some data from a segment
    println!("Query 4: Reading actual data from events segment 0...");
    let segment = &events_table.segments[0];
    let reader = SegmentReader::new(&segment.path, events_table.columns.clone());
    let data = reader
        .read_column("event_id", segment.stats.row_count)
        .await?;

    match data {
        ColumnData::Int64(ids) => {
            println!("  First 10 event IDs: {:?}", &ids[0..10.min(ids.len())]);
            println!("  Total rows read: {}", ids.len());
        }
        _ => println!("  Unexpected data type"),
    }

    Ok(())
}
