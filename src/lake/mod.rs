//! Lakehouse layer: open Parquet data files plus the `rhlog` transaction log (ADR-0001, ADR-0002).

pub mod log;
pub mod parquet;
pub mod schema;
pub mod stats;
