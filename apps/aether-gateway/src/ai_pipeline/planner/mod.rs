use crate::ai_pipeline::contracts::{
    GatewayControlPlanResponse, GatewayControlSyncDecisionResponse,
};
use crate::control::GatewayControlDecision;
use crate::{AppState, GatewayError};

pub(crate) mod candidate_affinity;
pub(crate) mod common;
mod decision;
pub(crate) mod passthrough;
pub(crate) mod plan_builders;
pub(crate) mod specialized;
pub(crate) mod standard;

pub(crate) async fn maybe_build_sync_decision_payload(
    state: &AppState,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
    body_json: &serde_json::Value,
    body_base64: Option<&str>,
    body_is_empty: bool,
) -> Result<Option<GatewayControlSyncDecisionResponse>, GatewayError> {
    decision::maybe_build_sync_decision_payload(
        state,
        parts,
        trace_id,
        decision,
        body_json,
        body_base64,
        body_is_empty,
    )
    .await
}

pub(crate) async fn maybe_build_stream_decision_payload(
    state: &AppState,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
    body_json: &serde_json::Value,
) -> Result<Option<GatewayControlSyncDecisionResponse>, GatewayError> {
    decision::maybe_build_stream_decision_payload(state, parts, trace_id, decision, body_json).await
}

pub(crate) async fn maybe_build_sync_plan_payload(
    state: &AppState,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
    body_json: &serde_json::Value,
    body_base64: Option<&str>,
    body_is_empty: bool,
) -> Result<Option<GatewayControlPlanResponse>, GatewayError> {
    decision::maybe_build_sync_plan_payload_impl(
        state,
        parts,
        trace_id,
        decision,
        body_json,
        body_base64,
        body_is_empty,
    )
    .await
}

pub(crate) async fn maybe_build_stream_plan_payload(
    state: &AppState,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
    body_json: &serde_json::Value,
) -> Result<Option<GatewayControlPlanResponse>, GatewayError> {
    decision::maybe_build_stream_plan_payload_impl(state, parts, trace_id, decision, body_json)
        .await
}
