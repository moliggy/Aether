use axum::body::{Body, Bytes};
use axum::http::Response;

use crate::control::GatewayControlDecision;
use crate::{AppState, GatewayError};

pub(crate) mod adapters;
pub(crate) mod provider_types;

pub(crate) async fn maybe_execute_sync_request(
    state: &AppState,
    parts: &http::request::Parts,
    body_bytes: &Bytes,
    trace_id: &str,
    decision: Option<&GatewayControlDecision>,
) -> Result<Option<Response<Body>>, GatewayError> {
    super::super::execution_runtime::maybe_execute_via_execution_runtime_sync(
        state, parts, body_bytes, trace_id, decision,
    )
    .await
}

pub(crate) async fn maybe_execute_stream_request(
    state: &AppState,
    parts: &http::request::Parts,
    body_bytes: &Bytes,
    trace_id: &str,
    decision: Option<&GatewayControlDecision>,
) -> Result<Option<Response<Body>>, GatewayError> {
    super::super::execution_runtime::maybe_execute_via_execution_runtime_stream(
        state, parts, body_bytes, trace_id, decision,
    )
    .await
}
