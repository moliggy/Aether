use axum::body::Bytes;
use axum::http::Uri;

use super::super::GatewayControlDecision;
use super::credentials::{contains_string, extract_requested_model};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum GatewayLocalAuthRejection {
    InvalidApiKey,
    LockedApiKey,
    WalletUnavailable,
    BalanceDenied { remaining: Option<f64> },
    ProviderNotAllowed { provider: String },
    ApiFormatNotAllowed { api_format: String },
    ModelNotAllowed { model: String },
}

pub(crate) fn trusted_auth_local_rejection(
    decision: Option<&GatewayControlDecision>,
    _headers: &http::HeaderMap,
) -> Option<GatewayLocalAuthRejection> {
    let decision = decision?;
    if decision.route_class.as_deref() != Some("ai_public") {
        return None;
    }

    decision
        .local_auth_rejection
        .clone()
        .or_else(|| decision.auth_context.as_ref()?.local_rejection.clone())
}

pub(crate) fn should_buffer_request_for_local_auth(
    decision: Option<&GatewayControlDecision>,
    headers: &http::HeaderMap,
) -> bool {
    let Some(decision) = decision else {
        return false;
    };
    decision.route_class.as_deref() == Some("ai_public")
        && decision.route_kind.as_deref() != Some("files")
        && crate::headers::is_json_request(headers)
}

pub(crate) fn request_model_local_rejection(
    decision: Option<&GatewayControlDecision>,
    uri: &Uri,
    headers: &http::HeaderMap,
    body: &Bytes,
) -> Option<GatewayLocalAuthRejection> {
    let decision = decision?;
    if decision.route_class.as_deref() != Some("ai_public") {
        return None;
    }
    let auth_context = decision.auth_context.as_ref()?;
    let allowed_models = auth_context.allowed_models.as_deref()?;
    let requested_model = extract_requested_model(decision, uri, headers, body)?;
    if contains_string(allowed_models, &requested_model) {
        return None;
    }

    Some(GatewayLocalAuthRejection::ModelNotAllowed {
        model: requested_model,
    })
}
