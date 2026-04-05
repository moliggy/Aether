use axum::body::Body;
use axum::http::Response;
use std::collections::BTreeMap;
use url::form_urlencoded;

use aether_data::repository::candidates::{RequestCandidateStatus, UpsertRequestCandidateRecord};
use serde_json::{json, Value};
use tracing::warn;
use uuid::Uuid;

use crate::ai_pipeline::planner::candidate_affinity::prefer_local_tunnel_owner_candidates;
use crate::ai_pipeline::planner::common::{
    EXECUTION_RUNTIME_STREAM_DECISION_ACTION, EXECUTION_RUNTIME_SYNC_DECISION_ACTION,
};
use crate::ai_pipeline::planner::plan_builders::{
    LocalStreamPlanAndReport, LocalSyncPlanAndReport,
};
use crate::control::GatewayControlDecision;
use crate::execution_runtime::{ConversionMode, ExecutionStrategy};
use crate::headers::collect_control_headers;
use crate::provider_transport::antigravity::{
    build_antigravity_safe_v1internal_request, build_antigravity_static_identity_headers,
    build_antigravity_v1internal_url, classify_local_antigravity_request_support,
    AntigravityEnvelopeRequestType, AntigravityRequestEnvelopeSupport,
    AntigravityRequestSideSupport, AntigravityRequestUrlAction,
};
use crate::provider_transport::auth::{
    build_openai_passthrough_headers, resolve_local_gemini_auth, resolve_local_standard_auth,
};
use crate::provider_transport::claude_code::{
    build_claude_code_messages_url, build_claude_code_passthrough_headers,
    sanitize_claude_code_request_body, supports_local_claude_code_transport_with_network,
};
use crate::provider_transport::kiro::{
    build_kiro_generate_assistant_response_url, build_kiro_provider_headers,
    build_kiro_provider_request_body, supports_local_kiro_request_transport_with_network,
    KIRO_ENVELOPE_NAME,
};
use crate::provider_transport::policy::{
    supports_local_gemini_transport_with_network, supports_local_standard_transport_with_network,
};
use crate::provider_transport::url::{
    build_claude_messages_url, build_gemini_content_url, build_passthrough_path_url,
};
use crate::provider_transport::vertex::{
    build_vertex_api_key_gemini_content_url, resolve_local_vertex_api_key_query_auth,
    supports_local_vertex_api_key_gemini_transport_with_network,
};
use crate::provider_transport::{
    apply_local_body_rules, apply_local_header_rules, build_passthrough_headers,
    ensure_upstream_auth_header, resolve_transport_execution_timeouts,
    resolve_transport_proxy_snapshot_with_tunnel_affinity, resolve_transport_tls_profile,
    LocalResolvedOAuthRequestAuth,
};
use crate::scheduler::{
    current_unix_secs, list_selectable_candidates, record_local_request_candidate_status,
    GatewayMinimalCandidateSelectionCandidate,
};
use crate::{
    append_execution_contract_fields_to_value, AppState, GatewayControlSyncDecisionResponse,
    GatewayError,
};

pub(crate) mod family;
pub(crate) mod plans;
mod request;

pub(super) use self::family::{
    materialize_local_same_format_provider_candidate_attempts,
    maybe_build_local_same_format_provider_decision_payload_for_candidate,
    resolve_local_same_format_provider_decision_input, LocalSameFormatProviderFamily,
    LocalSameFormatProviderSpec,
};
pub(crate) use self::family::{
    maybe_build_stream_local_same_format_provider_decision_payload,
    maybe_build_sync_local_same_format_provider_decision_payload,
};
use self::request::{
    build_same_format_provider_request_body, build_same_format_upstream_url,
    extract_gemini_model_from_path,
};

const ANTIGRAVITY_ENVELOPE_NAME: &str = "antigravity:v1internal";
