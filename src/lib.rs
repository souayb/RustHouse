//! RustHouse: a hybrid Lakehouse with bank-grade governance (see `specs/`).
//!
//! The only way to execute SQL is [`session::Session::execute_governed`] (ADR-0005). The
//! executor is crate-private; this does not compile (API-SQL-001, GOV-AUD-011, CORE-EXE-001):
//!
//! ```compile_fail
//! let _ = rusthouse::exec::Executor;
//! ```
//!
//! Nor can the vectorized engine or its DataFusion context be reached (EXE-VEC-004):
//!
//! ```compile_fail
//! let _ = rusthouse::exec::vector::VectorEngine;
//! ```

pub mod auth;
pub mod catalog;
pub mod config;
pub mod error;
pub(crate) mod exec;
pub mod governance;
pub mod http_api;
pub mod lake;
pub mod meta;
pub mod metrics;
pub mod session;
pub mod sql;
pub mod storage;

// Re-export commonly used types
pub use config::Config;
pub use error::{Result, RustHouseError};
