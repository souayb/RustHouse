//! HTTP surface (spec API-SQL). Authenticates, builds a `Principal`, delegates to `Session`.
//! Replaces the `TECHNICAL.md` Step 6 sketch.

use crate::auth::{AuthMaterial, Authenticator};
use crate::catalog::Principal;
use crate::config::Config;
use crate::error::RustHouseError;
use crate::governance::governor::ROLE_METRICS_READER;
use crate::lake::log::AsOf;
use crate::metrics::{metrics, render};
use crate::session::{QueryResult, Session};
use axum::{
    body::Body,
    extract::{DefaultBodyLimit, Extension, Query, State},
    http::{header, HeaderMap, HeaderValue, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone)]
pub struct AppState {
    pub session: Arc<Session>,
    pub authenticator: Arc<dyn Authenticator>,
    pub config: Arc<Config>,
}

#[derive(Clone, Debug)]
pub struct RequestId(pub String);

pub fn create_router(state: AppState) -> Router {
    let limit = state.config.max_request_bytes;
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics_handler))
        .route("/v1/query", post(query_handler))
        .route("/v1/write", post(write_handler))
        .route("/v1/ddl", post(ddl_handler))
        .layer(DefaultBodyLimit::max(limit))
        .layer(middleware::from_fn(request_id_middleware))
        .with_state(state)
}

/// API-SQL-007 / API-SQL-013: request id on every response, plus `Cache-Control: no-store`.
async fn request_id_middleware(mut req: Request<Body>, next: Next) -> Response {
    let id = Uuid::new_v4().to_string();
    req.extensions_mut().insert(RequestId(id.clone()));
    let route = req.uri().path().to_string();
    let mut resp = next.run(req).await;
    let headers = resp.headers_mut();
    if let Ok(v) = HeaderValue::from_str(&id) {
        headers.insert("x-request-id", v);
    }
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    metrics()
        .http_requests_total
        .with_label_values(&[&route, resp.status().as_str()])
        .inc();
    resp
}

/// HTTP status for an engine error (API-SQL-008).
pub fn status_for(err: &RustHouseError) -> StatusCode {
    use RustHouseError::*;
    match err {
        Denied(_) => StatusCode::FORBIDDEN,
        Unauthenticated => StatusCode::UNAUTHORIZED,
        TableNotFound(_) | ColumnNotFound(_) | NamespaceNotFound(_) | VersionNotFound(_) => {
            StatusCode::NOT_FOUND
        }
        SqlParse(_)
        | InvalidData(_)
        | TypeMismatch(_)
        | InvalidIdentifier(_)
        | MissingClassification(_)
        | AmbiguousTable(_)
        | ClassificationLowering(_)
        | MissingPrincipal
        | InvalidAction(_) => StatusCode::BAD_REQUEST,
        Conflict(_) | CommitConflict { .. } | TableExists(_) | NamespaceExists(_) => {
            StatusCode::CONFLICT
        }
        VersionVacuumed { .. } => StatusCode::GONE,
        AuditUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
        Io(_) | Serialization(_) | QueryExecution(_) | Storage(_) | Metadata(_)
        | Compression(_) | Internal(_) | CorruptLog(_) | Parquet(_) => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// Error text safe for clients: no paths, no internals (API-SQL-008).
fn public_message(err: &RustHouseError, status: StatusCode) -> String {
    if status.is_server_error() {
        return "internal error".to_string();
    }
    let msg = err.to_string();
    if msg.contains('/') || msg.contains('\\') {
        // Fall back to the variant name only.
        let dbg = format!("{err:?}");
        dbg.split(['(', ' ', '{'])
            .next()
            .unwrap_or("error")
            .to_string()
    } else {
        msg
    }
}

struct ApiError {
    request_id: String,
    err: RustHouseError,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = status_for(&self.err);
        let body = json!({
            "request_id": self.request_id,
            "error": if status == StatusCode::UNAUTHORIZED { "unauthenticated".to_string() } else { public_message(&self.err, status) },
        });
        (status, Json(body)).into_response()
    }
}

async fn health() -> &'static str {
    "OK"
}

fn authenticate(
    state: &AppState,
    headers: &HeaderMap,
    rid: &str,
    route: &str,
) -> Result<Principal, ApiError> {
    let material = AuthMaterial::from_authorization_header(
        headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok()),
    );
    match state.authenticator.authenticate(&material) {
        Ok(p) => Ok(p),
        Err(_) => {
            // Audit the refusal; if the sink is down that is a 503, not a 401.
            if let Err(e) = state.session.audit_unauthenticated(rid, route) {
                return Err(ApiError {
                    request_id: rid.to_string(),
                    err: e,
                });
            }
            Err(ApiError {
                request_id: rid.to_string(),
                err: RustHouseError::Unauthenticated,
            })
        }
    }
}

async fn metrics_handler(
    State(state): State<AppState>,
    Extension(RequestId(rid)): Extension<RequestId>,
    headers: HeaderMap,
) -> Response {
    let principal = match authenticate(&state, &headers, &rid, "/metrics") {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    if !principal.has_role(ROLE_METRICS_READER) {
        return ApiError {
            request_id: rid,
            err: RustHouseError::Denied(format!("role {ROLE_METRICS_READER} required")),
        }
        .into_response();
    }
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        render(),
    )
        .into_response()
}

#[derive(Deserialize)]
struct AsOfBody {
    version: Option<u64>,
    timestamp: Option<String>,
}

#[derive(Deserialize)]
struct QueryBody {
    sql: String,
    as_of: Option<AsOfBody>,
}

fn rows_as_objects(r: &QueryResult) -> Vec<Value> {
    r.rows
        .iter()
        .map(|row| {
            let mut m = Map::new();
            for (c, v) in r.columns.iter().zip(row) {
                m.insert(c.clone(), v.clone());
            }
            Value::Object(m)
        })
        .collect()
}

async fn query_handler(
    State(state): State<AppState>,
    Extension(RequestId(rid)): Extension<RequestId>,
    headers: HeaderMap,
    Json(body): Json<QueryBody>,
) -> Response {
    let principal = match authenticate(&state, &headers, &rid, "/v1/query") {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    let as_of = match body.as_of {
        Some(AsOfBody {
            version: Some(v), ..
        }) => Some(AsOf::Version(v)),
        Some(AsOfBody {
            timestamp: Some(t), ..
        }) => match chrono::DateTime::parse_from_rfc3339(&t) {
            Ok(dt) => Some(AsOf::Timestamp(dt.timestamp_millis())),
            Err(_) => {
                return ApiError {
                    request_id: rid,
                    err: RustHouseError::SqlParse(
                        "as_of.timestamp must be RFC 3339 with an offset".into(),
                    ),
                }
                .into_response()
            }
        },
        _ => None,
    };
    match state
        .session
        .execute_query_with_id(&principal, &body.sql, rid.clone(), as_of)
        .await
    {
        Ok(r) => Json(json!({
            "request_id": r.request_id,
            "columns": r.columns,
            "rows": rows_as_objects(&r),
            "versions_read": r.versions_read,
            "catalog_version": r.catalog_version,
            "elapsed_ms": r.elapsed_ms,
        }))
        .into_response(),
        Err(err) => ApiError {
            request_id: rid,
            err,
        }
        .into_response(),
    }
}

#[derive(Deserialize)]
struct WriteParams {
    table: String,
}

#[derive(Deserialize)]
struct WriteBody {
    rows: Vec<Map<String, Value>>,
}

async fn write_handler(
    State(state): State<AppState>,
    Extension(RequestId(rid)): Extension<RequestId>,
    headers: HeaderMap,
    Query(params): Query<WriteParams>,
    Json(body): Json<WriteBody>,
) -> Response {
    let principal = match authenticate(&state, &headers, &rid, "/v1/write") {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    match state
        .session
        .write_rows_with_id(&principal, &params.table, body.rows, rid.clone())
        .await
    {
        Ok(r) => Json(json!({
            "request_id": r.request_id,
            "table": r.table,
            "version": r.version,
            "rows_written": r.rows_written,
        }))
        .into_response(),
        Err(err) => ApiError {
            request_id: rid,
            err,
        }
        .into_response(),
    }
}

#[derive(Deserialize)]
struct DdlBody {
    sql: String,
}

async fn ddl_handler(
    State(state): State<AppState>,
    Extension(RequestId(rid)): Extension<RequestId>,
    headers: HeaderMap,
    Json(body): Json<DdlBody>,
) -> Response {
    let principal = match authenticate(&state, &headers, &rid, "/v1/ddl") {
        Ok(p) => p,
        Err(e) => return e.into_response(),
    };
    match state
        .session
        .execute_ddl_with_id(&principal, &body.sql, rid.clone())
        .await
    {
        Ok(r) => Json(json!({
            "request_id": r.request_id,
            "table": r.table,
            "version": r.version,
            "catalog_version": r.catalog_version,
            "message": r.message,
            "rows": r.rows,
        }))
        .into_response(),
        Err(err) => ApiError {
            request_id: rid,
            err,
        }
        .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_into_response_mapping() {
        // REQ: API-SQL-008
        assert_eq!(
            status_for(&RustHouseError::Denied("x".into())),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            status_for(&RustHouseError::TableNotFound("t".into())),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            status_for(&RustHouseError::SqlParse("x".into())),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            status_for(&RustHouseError::Conflict("x".into())),
            StatusCode::CONFLICT
        );
        assert_eq!(
            status_for(&RustHouseError::AuditUnavailable("x".into())),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            status_for(&RustHouseError::Io(std::io::Error::other("boom"))),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        let e = RustHouseError::Storage("/data/secret/path".into());
        assert_eq!(public_message(&e, status_for(&e)), "internal error");
        let e = RustHouseError::TableExists("/data/gold/x".into());
        assert!(!public_message(&e, status_for(&e)).contains('/'));
    }
}
