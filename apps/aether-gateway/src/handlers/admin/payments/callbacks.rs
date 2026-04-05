use super::{
    build_admin_payment_callback_payload, build_admin_payment_callback_payload_from_record,
    build_admin_payments_bad_request_response, parse_admin_payments_limit,
    parse_admin_payments_offset,
};
use crate::control::GatewayPublicRequestContext;
use crate::handlers::query_param_value;
use crate::{AppState, GatewayError};
use axum::{
    body::Body,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

pub(super) async fn maybe_build_local_admin_payment_callbacks_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
    route_kind: Option<&str>,
) -> Result<Option<Response<Body>>, GatewayError> {
    match route_kind {
        Some("list_callbacks") => Ok(Some(
            build_admin_payment_callbacks_response(state, request_context).await?,
        )),
        _ => Ok(None),
    }
}

async fn build_admin_payment_callbacks_response(
    state: &AppState,
    request_context: &GatewayPublicRequestContext,
) -> Result<Response<Body>, GatewayError> {
    let query = request_context.request_query_string.as_deref();
    let limit = match parse_admin_payments_limit(query) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_payments_bad_request_response(detail)),
    };
    let offset = match parse_admin_payments_offset(query) {
        Ok(value) => value,
        Err(detail) => return Ok(build_admin_payments_bad_request_response(detail)),
    };
    let payment_method = query_param_value(query, "payment_method");

    let (items, total) = state
        .list_admin_payment_callbacks(payment_method.as_deref(), limit, offset)
        .await?
        .unwrap_or_default();

    Ok(Json(json!({
        "items": items
            .iter()
            .map(build_admin_payment_callback_payload_from_record)
            .collect::<Vec<_>>(),
        "total": total,
        "limit": limit,
        "offset": offset,
    }))
    .into_response())
}
