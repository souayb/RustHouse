//! Prometheus metrics (API-SQL-010, GOV-AUD-013, GOV-ACC-009).

use prometheus::{Encoder, IntCounterVec, Opts, Registry, TextEncoder};
use std::sync::OnceLock;

pub struct Metrics {
    pub registry: Registry,
    pub http_requests_total: IntCounterVec,
    pub queries_total: IntCounterVec,
    pub governor_decisions_total: IntCounterVec,
    pub audit_records_total: IntCounterVec,
}

fn counter(registry: &Registry, name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
    let c = IntCounterVec::new(Opts::new(name, help), labels).expect("valid metric");
    registry
        .register(Box::new(c.clone()))
        .expect("register once");
    c
}

pub fn metrics() -> &'static Metrics {
    static M: OnceLock<Metrics> = OnceLock::new();
    M.get_or_init(|| {
        let registry = Registry::new();
        Metrics {
            http_requests_total: counter(
                &registry,
                "rusthouse_http_requests_total",
                "HTTP requests by route and status",
                &["route", "status"],
            ),
            queries_total: counter(
                &registry,
                "rusthouse_queries_total",
                "Governed queries by outcome",
                &["result"],
            ),
            governor_decisions_total: counter(
                &registry,
                "rusthouse_governor_decisions_total",
                "Governor decisions",
                &["decision"],
            ),
            audit_records_total: counter(
                &registry,
                "rusthouse_audit_records_total",
                "Audit records written",
                &["status"],
            ),
            registry,
        }
    })
}

pub fn render() -> String {
    let m = metrics();
    // Touch every family so `# TYPE` lines exist even before the first event.
    m.queries_total.with_label_values(&["ok"]).inc_by(0);
    m.http_requests_total
        .with_label_values(&["/health", "200"])
        .inc_by(0);
    m.governor_decisions_total
        .with_label_values(&["allow"])
        .inc_by(0);
    m.audit_records_total.with_label_values(&["ok"]).inc_by(0);
    let mut buf = Vec::new();
    TextEncoder::new()
        .encode(&m.registry.gather(), &mut buf)
        .expect("encode");
    String::from_utf8(buf).unwrap_or_default()
}
