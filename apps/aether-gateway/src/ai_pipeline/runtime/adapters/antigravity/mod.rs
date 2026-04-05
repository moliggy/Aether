#![allow(dead_code, unused_imports)]

pub(crate) use crate::provider_transport::antigravity::{
    build_antigravity_safe_v1internal_request, build_antigravity_static_identity_headers,
    build_antigravity_v1internal_url, classify_antigravity_safe_request_body,
    classify_local_antigravity_request_support, AntigravityEnvelopeRequestType,
    AntigravityRequestAuth, AntigravityRequestAuthSupport, AntigravityRequestAuthUnsupportedReason,
    AntigravityRequestEnvelopeSupport, AntigravityRequestEnvelopeUnsupportedReason,
    AntigravityRequestSideSpec, AntigravityRequestSideSupport,
    AntigravityRequestSideUnsupportedReason, AntigravityRequestUrlAction,
    ANTIGRAVITY_PROVIDER_TYPE, ANTIGRAVITY_REQUEST_USER_AGENT,
    ANTIGRAVITY_V1INTERNAL_PATH_TEMPLATE,
};
