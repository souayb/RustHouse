# RustHouse: Complete Build & Test Guide

## Quick Start

### Step 0: Initial Setup
```bash
# Create project structure
cargo new rusthouse
cd rusthouse

# Copy all provided files to appropriate locations

# Build project
cargo build

# Run all tests
cargo test

# Run benchmarks
cargo bench

# Start server
cargo run -- --bind 0.0.0.0:8123 --data-dir ./data
```

## Step-by-Step Testing

### Step 0: Project Scaffold ✅

**Files Created:**
- `Cargo.toml` - Dependencies
- `src/main.rs` - Server entry point
- `src/lib.rs` - Library exports
- `src/config.rs` - Configuration
- `src/error.rs` - Error types
- `tests/integration_test.rs` - Integration tests
- `benches/bench_import.rs` - Benchmarks

**Test Commands:**
```bash
# Build
cargo build

# Unit tests
cargo test --lib

# Integration tests  
cargo test --test integration_test

# Benchmarks (quick mode)
cargo bench -- --quick

# Check code quality
cargo clippy -- -D warnings

# Format code
cargo fmt

# Start server (3 seconds test)
timeout 3s cargo run -- --bind 127.0.0.1:18123 --data-dir ./test_data || true
```

**Expected Output:**
- All tests pass
- Server starts successfully
- Health endpoint responds with "OK"

---

### Step 1: Table Metadata & Atomic Metadata Store ✅

**Files Created:**
- `src/meta.rs` - Metadata structures

**Test Commands:**
```bash
# Run metadata tests
cargo test meta:: -- --nocapture

# Test atomic writes specifically
cargo test test_atomic_write_behavior -- --nocapture

# Test serialization
cargo test test_metadata_store_write_read -- --nocapture

# All tests
cargo test
```

**Key Tests:**
- `test_metadata_operations` - Table/Column creation
- `test_metadata_store_write_read` - File I/O
- `test_atomic_write_behavior` - Atomic rename
- `test_segment_stats` - Statistics collection

**Verification:**
```bash
# Should see atomic behavior: tmp file → rename
# No .tmp files should remain after writes
ls ./target/tmp/
```

---

### Step 2: Columnar Storage Write Path ✅

**Files Created:**
- `src/storage/mod.rs` - Storage module
- `src/storage/column_writer.rs` - Column serialization
- `src/storage/segment.rs` - Segment I/O

**Test Commands:**
```bash
# Column writer tests
cargo test column_writer:: -- --nocapture

# Segment writer tests
cargo test segment:: -- --nocapture

# Compression tests
cargo test test_column_compression -- --nocapture

# Stats collection
cargo test test_segment_stats_collection -- --nocapture

# All storage tests
cargo test storage::
```

**Key Tests:**
- `test_column_data_serialization_int64` - Binary format
- `test_column_writer_write_read` - File I/O + compression
- `test_segment_write_read` - Multi-column segments
- `test_column_compression` - LZ4 effectiveness

**Verification:**
```bash
# Compressed files should be smaller than raw data
# Example: 1000 i64 values = 8000 bytes raw → ~100 bytes compressed
```

---

### Step 3: Columnar Storage Read Path + Segment Pruning ✅

**Files Created:**
- `src/storage/pruner.rs` - Segment pruning logic

**Test Commands:**
```bash
# Pruner tests
cargo test pruner:: -- --nocapture

# Predicate tests
cargo test test_prune_eq_predicate -- --nocapture
cargo test test_prune_gt_predicate -- --nocapture
cargo test test_prune_lt_predicate -- --nocapture

# Complex predicates
cargo test test_prune_and_predicate -- --nocapture
cargo test test_prune_or_predicate -- --nocapture

# Effectiveness tests
cargo test test_prune_no_segments_match -- --nocapture
cargo test test_prune_all_segments_match -- --nocapture

# All storage tests
cargo test storage::
```

**Key Tests:**
- `test_prune_eq_predicate` - Exact match pruning
- `test_prune_and_predicate` - Combined predicates
- `test_string_comparison` - Non-numeric pruning

**Expected Behavior:**
```
Segments: [1-100], [101-200], [201-300]
Query: WHERE id = 150
Result: Only segment [101-200] scanned (66% pruned)
```

---

### Step 4: SQL Parsing & Planner ✅

**Files Created:**
- `src/sql/mod.rs` - SQL module
- `src/sql/parser.rs` - SQL parser wrapper
- `src/sql/planner.rs` - Logical planner

**Test Commands:**
```bash
# Parser tests
cargo test parser:: -- --nocapture

# Planner tests
cargo test planner:: -- --nocapture

# Specific query types
cargo test test_plan_simple_select -- --nocapture
cargo test test_plan_select_with_filter -- --nocapture
cargo test test_plan_aggregation -- --nocapture
cargo test test_plan_complex_predicate -- --nocapture

# All SQL tests
cargo test sql::
```

**Key Tests:**
- `test_parse_simple_select` - Basic SELECT
- `test_plan_select_with_filter` - WHERE clause
- `test_plan_aggregation` - GROUP BY + aggregates
- `test_plan_complex_predicate` - AND/OR predicates

**Example Plans:**
```sql
SELECT id, name FROM users WHERE id > 100
→ Project(columns: [id, name])
  └── Filter(predicate: id > 100)
      └── Scan(table: users)
```

---

### Step 5-8: Executor, HTTP API, Compaction, Metrics

**Additional Files:**
- `src/exec/mod.rs`, `src/exec/executor.rs` - Query execution
- `src/http_api.rs` - HTTP endpoints
- `src/compaction.rs` - Background compaction
- `src/metrics.rs` - Prometheus metrics

**Test Commands:**
```bash
# All tests
cargo test

# Integration test
cargo test --test integration_test

# End-to-end test
cargo test end_to_end

# Performance benchmarks
cargo bench

# Start server
cargo run --release -- --bind 0.0.0.0:8123 --data-dir ./data
```

**HTTP API Tests:**
```bash
# Health check
curl http://localhost:8123/health

# Query (manual test)
curl -X POST http://localhost:8123/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT * FROM test_table WHERE id > 100"}'

# Metrics
curl http://localhost:8123/metrics
```

---

## Complete Test Script

Create `test_all.sh`:
```bash
#!/bin/bash
set -e

echo "=== RustHouse Complete Test Suite ==="

echo "Step 0: Building..."
cargo build
cargo build --release

echo "Step 1-4: Unit tests..."
cargo test --lib

echo "Step 5: Integration tests..."
cargo test --test integration_test

echo "Step 6: Benchmarks (quick)..."
cargo bench -- --quick

echo "Step 7: Code quality..."
cargo clippy -- -D warnings
cargo fmt --check

echo "Step 8: Server smoke test..."
cargo run --release -- --bind 127.0.0.1:18124 --data-dir ./test_data &
SERVER_PID=$!
sleep 2
curl -f http://127.0.0.1:18124/health || exit 1
kill $SERVER_PID

echo "✅ All tests passed!"
```

## Performance Expectations

**Synthetic Benchmark Results:**

```
Compression Ratio:
- Int64 array (1000 values): ~98% reduction
- String array (repetitive): ~95% reduction
- Random data: ~30-50% reduction

Write Performance:
- 10,000 rows (3 columns): ~5ms
- 100,000 rows (3 columns): ~50ms

Read Performance (with pruning):
- Query matching 1/10 segments: 90% I/O reduction
- Query matching 0 segments: No I/O

Memory Usage:
- Server baseline: ~10MB
- Per-query overhead: ~5-20MB
```

## Troubleshooting

### Build Errors

```bash
# Missing dependencies
cargo update

# Clean rebuild
cargo clean
cargo build

# Check Rust version (need 1.70+)
rustc --version
```

### Test Failures

```bash
# Run specific test with output
cargo test test_name -- --nocapture

# Show all test output
RUST_LOG=debug cargo test -- --nocapture

# Single-threaded tests (for debugging)
cargo test -- --test-threads=1
```

### Runtime Issues

```bash
# Enable debug logging
RUST_LOG=debug cargo run

# Check data directory permissions
ls -la ./data

# Verify port availability
lsof -i :8123
```

## Project Structure Summary

```
rusthouse/
├── Cargo.toml
├── src/
│   ├── main.rs              # Server entry point
│   ├── lib.rs               # Library root
│   ├── config.rs            # Configuration
│   ├── error.rs             # Error types
│   ├── meta.rs              # Metadata structures
│   ├── storage/
│   │   ├── mod.rs
│   │   ├── column_writer.rs # Column I/O
│   │   ├── segment.rs       # Segment I/O
│   │   └── pruner.rs        # Segment pruning
│   ├── sql/
│   │   ├── mod.rs
│   │   ├── parser.rs        # SQL parsing
│   │   └── planner.rs       # Logical planning
│   ├── exec/
│   │   ├── mod.rs
│   │   └── executor.rs      # Query execution
│   ├── http_api.rs          # HTTP endpoints
│   ├── compaction.rs        # Compaction
│   └── metrics.rs           # Metrics
├── tests/
│   ├── integration_test.rs
│   └── end_to_end.rs
├── benches/
│   └── bench_import.rs
└── README.md
```

## Next Steps

1. **Run all tests**: `./test_all.sh`
2. **Start server**: `cargo run --release`
3. **Try queries**: Use curl or HTTP client
4. **Monitor metrics**: Check `/metrics` endpoint
5. **Run benchmarks**: `cargo bench`

## Contributing

Each commit should:
- Pass all tests (`cargo test`)
- Pass clippy (`cargo clippy`)
- Be formatted (`cargo fmt`)
- Include test coverage for new code

## License

MIT