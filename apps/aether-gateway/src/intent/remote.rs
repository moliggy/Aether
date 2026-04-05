use axum::body::Body;
use axum::http::Response;

use crate::control::GatewayControlDecision;
use crate::{AppState, GatewayError};

pub(crate) async fn maybe_execute_sync_via_remote_decision(
    _state: &AppState,
    _parts: &http::request::Parts,
    _trace_id: &str,
    _decision: &GatewayControlDecision,
    _body_json: &serde_json::Value,
    _plan_kind: &str,
) -> Result<Option<Response<Body>>, GatewayError> {
    Ok(None)
}

pub(crate) async fn maybe_execute_stream_via_remote_decision(
    _state: &AppState,
    _parts: &http::request::Parts,
    _trace_id: &str,
    _decision: &GatewayControlDecision,
    _body_json: &serde_json::Value,
    _plan_kind: &str,
) -> Result<Option<Response<Body>>, GatewayError> {
    Ok(None)
}
