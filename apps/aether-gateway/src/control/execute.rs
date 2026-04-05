use axum::body::{Body, Bytes};
use axum::http::{HeaderName, HeaderValue, Response};

use crate::constants::CONTROL_EXECUTED_HEADER;
use crate::control::GatewayControlDecision;
use crate::{AppState, GatewayError};

use super::resolve_execution_runtime_auth_context;

pub(crate) fn allows_control_execute_emergency(decision: &GatewayControlDecision) -> bool {
    decision.is_execution_runtime_candidate()
}

pub(crate) async fn maybe_execute_via_control(
    state: &AppState,
    parts: &http::request::Parts,
    body_bytes: Bytes,
    trace_id: &str,
    decision: Option<&GatewayControlDecision>,
    require_stream: bool,
) -> Result<Option<Response<Body>>, GatewayError> {
    let Some(decision) = decision else {
        return Ok(None);
    };

    let mut local_decision = decision.clone();
    if let Some(auth_context) = resolve_execution_runtime_auth_context(
        state,
        &local_decision,
        &parts.headers,
        &parts.uri,
        trace_id,
    )
    .await?
    {
        local_decision.auth_context = Some(auth_context);
        local_decision.local_auth_rejection = None;
    }

    let response = if require_stream {
        crate::execution_runtime::maybe_execute_via_execution_runtime_stream(
            state,
            parts,
            &body_bytes,
            trace_id,
            Some(&local_decision),
        )
        .await?
    } else {
        crate::execution_runtime::maybe_execute_via_execution_runtime_sync(
            state,
            parts,
            &body_bytes,
            trace_id,
            Some(&local_decision),
        )
        .await?
    };

    Ok(response.map(mark_control_executed))
}

fn mark_control_executed(mut response: Response<Body>) -> Response<Body> {
    response.headers_mut().insert(
        HeaderName::from_static(CONTROL_EXECUTED_HEADER),
        HeaderValue::from_static("true"),
    );
    response
}
