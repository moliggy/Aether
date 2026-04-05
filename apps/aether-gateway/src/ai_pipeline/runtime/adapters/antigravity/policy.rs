use serde_json::Value;

use super::auth::{
    resolve_local_antigravity_request_auth, AntigravityRequestAuth, AntigravityRequestAuthSupport,
    AntigravityRequestAuthUnsupportedReason, ANTIGRAVITY_PROVIDER_TYPE,
};
use super::request::{
    classify_antigravity_safe_request_body, AntigravityEnvelopeRequestType,
    AntigravityRequestEnvelopeUnsupportedReason,
};
use crate::provider_transport::snapshot::GatewayProviderTransportSnapshot;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AntigravityRequestSideSpec {
    pub(crate) auth: AntigravityRequestAuth,
    pub(crate) request_type: AntigravityEnvelopeRequestType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AntigravityRequestSideSupport {
    Supported(AntigravityRequestSideSpec),
    Unsupported(AntigravityRequestSideUnsupportedReason),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AntigravityRequestSideUnsupportedReason {
    InactiveTransport,
    WrongProviderType,
    UnsupportedApiFormat,
    UnsupportedCustomPath,
    UnsupportedHeaderRules,
    UnsupportedBodyRules,
    UnsupportedNetworkConfig,
    UnsupportedAuth(AntigravityRequestAuthUnsupportedReason),
    UnsupportedEnvelope(AntigravityRequestEnvelopeUnsupportedReason),
}

pub(crate) fn classify_local_antigravity_request_support(
    transport: &GatewayProviderTransportSnapshot,
    request_body: &Value,
    request_type: AntigravityEnvelopeRequestType,
) -> AntigravityRequestSideSupport {
    if !transport.provider.is_active || !transport.endpoint.is_active || !transport.key.is_active {
        return AntigravityRequestSideSupport::Unsupported(
            AntigravityRequestSideUnsupportedReason::InactiveTransport,
        );
    }
    if !transport
        .provider
        .provider_type
        .trim()
        .eq_ignore_ascii_case(ANTIGRAVITY_PROVIDER_TYPE)
    {
        return AntigravityRequestSideSupport::Unsupported(
            AntigravityRequestSideUnsupportedReason::WrongProviderType,
        );
    }

    let endpoint_format = transport.endpoint.api_format.trim();
    if !endpoint_format.eq_ignore_ascii_case("gemini:chat")
        && !endpoint_format.eq_ignore_ascii_case("gemini:cli")
    {
        return AntigravityRequestSideSupport::Unsupported(
            AntigravityRequestSideUnsupportedReason::UnsupportedApiFormat,
        );
    }
    if transport
        .endpoint
        .custom_path
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
    {
        return AntigravityRequestSideSupport::Unsupported(
            AntigravityRequestSideUnsupportedReason::UnsupportedCustomPath,
        );
    }
    if transport.endpoint.header_rules.is_some() {
        return AntigravityRequestSideSupport::Unsupported(
            AntigravityRequestSideUnsupportedReason::UnsupportedHeaderRules,
        );
    }
    if transport.endpoint.body_rules.is_some() {
        return AntigravityRequestSideSupport::Unsupported(
            AntigravityRequestSideUnsupportedReason::UnsupportedBodyRules,
        );
    }
    if transport.provider.proxy.is_some()
        || transport.endpoint.proxy.is_some()
        || transport.key.proxy.is_some()
        || transport.key.fingerprint.is_some()
    {
        return AntigravityRequestSideSupport::Unsupported(
            AntigravityRequestSideUnsupportedReason::UnsupportedNetworkConfig,
        );
    }

    let auth = match resolve_local_antigravity_request_auth(transport) {
        AntigravityRequestAuthSupport::Supported(auth) => auth,
        AntigravityRequestAuthSupport::Unsupported(reason) => {
            return AntigravityRequestSideSupport::Unsupported(
                AntigravityRequestSideUnsupportedReason::UnsupportedAuth(reason),
            );
        }
    };

    if let Err(reason) = classify_antigravity_safe_request_body(request_body) {
        return AntigravityRequestSideSupport::Unsupported(
            AntigravityRequestSideUnsupportedReason::UnsupportedEnvelope(reason),
        );
    }

    AntigravityRequestSideSupport::Supported(AntigravityRequestSideSpec { auth, request_type })
}
