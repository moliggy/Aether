use base64::Engine as _;

use super::plan_builders::{
    build_direct_plan_bypass_cache_key, is_matching_stream_request, mark_direct_plan_bypass,
    resolve_direct_executor_stream_plan_kind, resolve_direct_executor_sync_plan_kind,
    should_skip_direct_plan,
};
use super::*;

#[path = "sync/decision.rs"]
mod decision;
#[path = "sync/execution.rs"]
mod execution;

use decision::maybe_execute_sync_via_decision;
use execution::execute_executor_sync;

#[allow(unused_imports)]
pub(crate) use execution::{
    resolve_local_sync_error_background_report_kind,
    resolve_local_sync_success_background_report_kind,
};

pub(crate) async fn maybe_execute_via_executor_sync(
    state: &AppState,
    parts: &http::request::Parts,
    body_bytes: &Bytes,
    trace_id: &str,
    decision: Option<&GatewayControlDecision>,
) -> Result<Option<Response<Body>>, GatewayError> {
    let Some(control_base_url) = state.control_base_url.as_deref() else {
        return Ok(None);
    };
    let Some(executor_base_url) = state.executor_base_url.as_deref() else {
        return Ok(None);
    };
    let Some(decision) = decision else {
        return Ok(None);
    };

    if let Some(response) = maybe_build_local_video_task_read_response(
        state,
        executor_base_url,
        parts,
        trace_id,
        decision,
    )
    .await?
    {
        return Ok(Some(response));
    }

    let Some(plan_kind) = resolve_direct_executor_sync_plan_kind(parts, decision) else {
        return Ok(None);
    };

    let (body_json, body_base64) = if is_json_request(&parts.headers) {
        if body_bytes.is_empty() {
            (json!({}), None)
        } else {
            match serde_json::from_slice::<serde_json::Value>(body_bytes) {
                Ok(value) => (value, None),
                Err(_) => return Ok(None),
            }
        }
    } else {
        (
            json!({}),
            (!body_bytes.is_empty())
                .then(|| base64::engine::general_purpose::STANDARD.encode(body_bytes)),
        )
    };

    if let Some(stream_plan_kind) = resolve_direct_executor_stream_plan_kind(parts, decision) {
        if is_matching_stream_request(stream_plan_kind, parts, &body_json) {
            return Ok(None);
        }
    }

    let bypass_cache_key =
        build_direct_plan_bypass_cache_key(plan_kind, parts, body_bytes, decision);
    if should_skip_direct_plan(state, &bypass_cache_key) {
        return Ok(None);
    }

    if let Some(response) = maybe_execute_local_video_task_follow_up_sync(
        state,
        control_base_url,
        executor_base_url,
        parts,
        trace_id,
        decision,
        plan_kind,
    )
    .await?
    {
        return Ok(Some(response));
    }

    if matches!(
        plan_kind,
        OPENAI_CHAT_SYNC_PLAN_KIND
            | OPENAI_CLI_SYNC_PLAN_KIND
            | OPENAI_COMPACT_SYNC_PLAN_KIND
            | CLAUDE_CHAT_SYNC_PLAN_KIND
            | CLAUDE_CLI_SYNC_PLAN_KIND
            | GEMINI_CHAT_SYNC_PLAN_KIND
            | GEMINI_CLI_SYNC_PLAN_KIND
            | OPENAI_VIDEO_CREATE_SYNC_PLAN_KIND
            | OPENAI_VIDEO_REMIX_SYNC_PLAN_KIND
            | OPENAI_VIDEO_CANCEL_SYNC_PLAN_KIND
            | OPENAI_VIDEO_DELETE_SYNC_PLAN_KIND
            | GEMINI_VIDEO_CREATE_SYNC_PLAN_KIND
            | GEMINI_VIDEO_CANCEL_SYNC_PLAN_KIND
            | GEMINI_FILES_GET_PLAN_KIND
            | GEMINI_FILES_LIST_PLAN_KIND
            | GEMINI_FILES_DELETE_PLAN_KIND
    ) {
        if let Some(response) = maybe_execute_sync_via_decision(
            state,
            control_base_url,
            executor_base_url,
            parts,
            trace_id,
            decision,
            &body_json,
            plan_kind,
        )
        .await?
        {
            return Ok(Some(response));
        }
    }

    let auth_context =
        resolve_executor_auth_context(state, decision, &parts.headers, &parts.uri, trace_id)
            .await?;
    let request_payload = GatewayControlPlanRequest {
        trace_id: trace_id.to_string(),
        method: parts.method.to_string(),
        path: parts.uri.path().to_string(),
        query_string: parts.uri.query().map(ToOwned::to_owned),
        headers: collect_control_headers(&parts.headers),
        body_json,
        body_base64,
        auth_context,
    };

    let response = state
        .client
        .post(format!("{control_base_url}/api/internal/gateway/plan-sync"))
        .header(TRACE_ID_HEADER, trace_id)
        .json(&request_payload)
        .send()
        .await
        .map_err(|err| GatewayError::ControlUnavailable {
            trace_id: trace_id.to_string(),
            message: err.to_string(),
        })?;

    if response.status() == http::StatusCode::CONFLICT
        && header_equals(
            response.headers(),
            CONTROL_ACTION_HEADER,
            CONTROL_ACTION_PROXY_PUBLIC,
        )
    {
        mark_direct_plan_bypass(state, bypass_cache_key);
        return Ok(None);
    }

    if header_equals(response.headers(), CONTROL_EXECUTED_HEADER, "true") {
        if !allow_control_execute_fallback(state, parts) {
            mark_direct_plan_bypass(state, bypass_cache_key);
            return Ok(None);
        }
        mark_direct_plan_bypass(state, bypass_cache_key);
        return Ok(Some(build_client_response(
            response,
            trace_id,
            Some(decision),
        )?));
    }

    let response = response
        .error_for_status()
        .map_err(|err| GatewayError::ControlUnavailable {
            trace_id: trace_id.to_string(),
            message: err.to_string(),
        })?;

    let payload: GatewayControlPlanResponse = response
        .json()
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;

    if let Some(auth_context) = payload.auth_context.clone() {
        cache_executor_auth_context(state, decision, &parts.headers, &parts.uri, auth_context);
    }

    if payload.action != EXECUTOR_SYNC_ACTION {
        return Ok(None);
    }

    if payload.plan_kind.as_deref() != Some(plan_kind) {
        return Ok(None);
    }

    let Some(plan) = payload.plan else {
        return Err(GatewayError::Internal(
            "gateway sync plan response missing execution plan".to_string(),
        ));
    };

    execute_executor_sync(
        state,
        control_base_url,
        executor_base_url,
        parts.uri.path(),
        plan,
        trace_id,
        decision,
        plan_kind,
        payload.report_kind,
        payload.report_context,
    )
    .await
}

async fn maybe_build_local_video_task_read_response(
    state: &AppState,
    executor_base_url: &str,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
) -> Result<Option<Response<Body>>, GatewayError> {
    if parts.method != http::Method::GET || decision.route_kind.as_deref() != Some("video") {
        return Ok(None);
    }

    if let Some(refresh_plan) = state.video_tasks.prepare_read_refresh_sync_plan(
        decision.route_family.as_deref(),
        parts.uri.path(),
        trace_id,
    ) {
        state
            .execute_video_task_refresh_plan(executor_base_url, &refresh_plan)
            .await?;
    }

    let read_response = state
        .video_tasks
        .read_response(decision.route_family.as_deref(), parts.uri.path());
    let Some(read_response) = read_response else {
        return Ok(None);
    };

    let body_bytes = serde_json::to_vec(&read_response.body_json)
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
    let mut headers = BTreeMap::new();
    headers.insert("content-type".to_string(), "application/json".to_string());
    headers.insert("content-length".to_string(), body_bytes.len().to_string());

    Ok(Some(build_client_response_from_parts(
        read_response.status_code,
        &headers,
        Body::from(body_bytes),
        trace_id,
        Some(decision),
    )?))
}

async fn maybe_execute_local_video_task_follow_up_sync(
    state: &AppState,
    control_base_url: &str,
    executor_base_url: &str,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
    plan_kind: &str,
) -> Result<Option<Response<Body>>, GatewayError> {
    if !matches!(
        plan_kind,
        OPENAI_VIDEO_CANCEL_SYNC_PLAN_KIND
            | OPENAI_VIDEO_DELETE_SYNC_PLAN_KIND
            | GEMINI_VIDEO_CANCEL_SYNC_PLAN_KIND
    ) {
        return Ok(None);
    }

    let auth_context =
        resolve_executor_auth_context(state, decision, &parts.headers, &parts.uri, trace_id)
            .await?;
    let Some(follow_up) = state.video_tasks.prepare_follow_up_sync_plan(
        plan_kind,
        parts.uri.path(),
        auth_context.as_ref(),
        trace_id,
    ) else {
        return Ok(None);
    };

    execute_executor_sync(
        state,
        control_base_url,
        executor_base_url,
        parts.uri.path(),
        follow_up.plan,
        trace_id,
        decision,
        plan_kind,
        follow_up.report_kind,
        follow_up.report_context,
    )
    .await
}
