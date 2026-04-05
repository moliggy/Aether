mod constants;
mod plan_fallback;
mod policy;
mod remote;
mod stream_path;
mod sync_path;

use aether_contracts::ExecutionPlan;
use axum::body::{Body, Bytes};
use axum::http::Response;

pub(crate) use self::constants::{DIRECT_PLAN_BYPASS_MAX_ENTRIES, DIRECT_PLAN_BYPASS_TTL};
pub(crate) use self::plan_fallback::{
    maybe_execute_stream_via_plan_fallback, maybe_execute_sync_via_plan_fallback,
};
use self::policy::{
    build_direct_plan_bypass_cache_key, mark_direct_plan_bypass,
    should_bypass_execution_runtime_decision, should_bypass_execution_runtime_plan,
    should_skip_direct_plan,
};
pub(crate) use self::remote::{
    maybe_execute_stream_via_remote_decision, maybe_execute_sync_via_remote_decision,
};
pub(crate) use self::stream_path::maybe_build_stream_decision_payload_via_local_path;
pub(crate) use self::sync_path::maybe_build_sync_decision_payload_via_local_path;
use crate::control::GatewayControlDecision;
use crate::{AppState, GatewayControlSyncDecisionResponse, GatewayError};

pub(crate) fn build_intent_plan_bypass_cache_key(
    plan_kind: &str,
    parts: &http::request::Parts,
    body_bytes: &Bytes,
    decision: &GatewayControlDecision,
) -> String {
    build_direct_plan_bypass_cache_key(plan_kind, parts, body_bytes, decision)
}

pub(crate) fn mark_intent_plan_bypass(state: &AppState, cache_key: String) {
    mark_direct_plan_bypass(state, cache_key);
}

pub(crate) async fn maybe_execute_via_stream_intent_path(
    state: &AppState,
    parts: &http::request::Parts,
    body_bytes: &Bytes,
    trace_id: &str,
    decision: &GatewayControlDecision,
) -> Result<Option<Response<Body>>, GatewayError> {
    stream_path::maybe_execute_via_stream_decision_path(
        state, parts, body_bytes, trace_id, decision,
    )
    .await
}

pub(crate) async fn maybe_execute_via_sync_intent_path(
    state: &AppState,
    parts: &http::request::Parts,
    body_bytes: &Bytes,
    trace_id: &str,
    decision: &GatewayControlDecision,
) -> Result<Option<Response<Body>>, GatewayError> {
    sync_path::maybe_execute_via_sync_decision_path(state, parts, body_bytes, trace_id, decision)
        .await
}

pub(crate) fn should_bypass_intent_decision(payload: &GatewayControlSyncDecisionResponse) -> bool {
    should_bypass_execution_runtime_decision(payload)
}

pub(crate) fn should_bypass_intent_plan(plan: &ExecutionPlan) -> bool {
    should_bypass_execution_runtime_plan(plan)
}

pub(crate) fn should_skip_intent_plan(state: &AppState, cache_key: &str) -> bool {
    should_skip_direct_plan(state, cache_key)
}
