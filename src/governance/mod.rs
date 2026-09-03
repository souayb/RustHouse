//! Data governance: classification (GOV-CLS), access control (GOV-ACC), audit (GOV-AUD),
//! lineage (GOV-LIN), retention (GOV-RET), data quality (GOV-DQ), encryption (GOV-ENC).
//! See ADR-0005: all enforcement flows through a single Governor.

pub mod access;
pub mod audit;
pub mod classification;
pub mod governor;
