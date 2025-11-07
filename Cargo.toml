[package]
name = "rusthouse"
version = "0.1.0"
edition = "2021"
authors = ["RustHouse Team"]
description = "A ClickHouse-like OLAP database in Rust"

[dependencies]
# Async runtime
tokio = { version = "1.35", features = ["full"] }

# HTTP server
axum = { version = "0.7", features = ["macros"] }
tower = "0.4"
tower-http = { version = "0.5", features = ["trace", "cors"] }

# SQL parsing
sqlparser = "0.43"

# Columnar data structures (using arrow2 for zero-copy efficiency)
arrow2 = { version = "0.18", features = ["io_parquet", "io_json"] }

# CLI
clap = { version = "4.4", features = ["derive"] }

# Logging and tracing
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }

# Error handling
thiserror = "1.0"
anyhow = "1.0"

# Serialization
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"

# Compression
lz4_flex = "0.11"
zstd = "0.13"

# Metrics
prometheus = { version = "0.13", features = ["process"] }

# File operations
tempfile = "3.8"

# Concurrency
parking_lot = "0.12"
dashmap = "5.5"

# Hashing for aggregations
ahash = "0.8"

[dev-dependencies]
criterion = { version = "0.5", features = ["html_reports"] }
once_cell = "1.19"
reqwest = { version = "0.11", default-features = false, features = ["json", "rustls-tls"] }
tempfile = "3.8"

[[bench]]
name = "bench_import"
harness = false

[[example]]
name = "create_dataset"
path = "examples/create_dataset.rs"

[profile.release]
opt-level = 3
lto = true
codegen-units = 1

[profile.bench]
inherits = "release"