use base64::Engine as _;

use super::plan_builders::{
    build_direct_plan_bypass_cache_key, is_matching_stream_request, mark_direct_plan_bypass,
    resolve_direct_executor_stream_plan_kind, should_skip_direct_plan,
};
use super::*;

#[path = "stream/decision.rs"]
mod decision;
#[path = "stream/error.rs"]
mod error;
#[path = "stream/execution.rs"]
mod execution;

use decision::maybe_execute_stream_via_decision;
use execution::execute_executor_stream;

pub(crate) async fn maybe_execute_via_executor_stream(
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

    let Some(plan_kind) = resolve_direct_executor_stream_plan_kind(parts, decision) else {
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

    if !is_matching_stream_request(plan_kind, parts, &body_json) {
        return Ok(None);
    }

    let bypass_cache_key =
        build_direct_plan_bypass_cache_key(plan_kind, parts, body_bytes, decision);
    if should_skip_direct_plan(state, &bypass_cache_key) {
        return Ok(None);
    }

    if let Some(response) = maybe_execute_local_video_task_content_stream(
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
        OPENAI_CHAT_STREAM_PLAN_KIND
            | CLAUDE_CHAT_STREAM_PLAN_KIND
            | GEMINI_CHAT_STREAM_PLAN_KIND
            | OPENAI_CLI_STREAM_PLAN_KIND
            | OPENAI_COMPACT_STREAM_PLAN_KIND
            | CLAUDE_CLI_STREAM_PLAN_KIND
            | GEMINI_CLI_STREAM_PLAN_KIND
            | GEMINI_FILES_DOWNLOAD_PLAN_KIND
            | OPENAI_VIDEO_CONTENT_PLAN_KIND
    ) {
        if let Some(response) = maybe_execute_stream_via_decision(
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
        .post(format!(
            "{control_base_url}/api/internal/gateway/plan-stream"
        ))
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

    if payload.action != EXECUTOR_STREAM_ACTION {
        return Ok(None);
    }

    if payload.plan_kind.as_deref() != Some(plan_kind) {
        return Ok(None);
    }

    let Some(plan) = payload.plan else {
        return Err(GatewayError::Internal(
            "gateway plan response missing execution plan".to_string(),
        ));
    };

    execute_executor_stream(
        state,
        control_base_url,
        executor_base_url,
        plan,
        trace_id,
        decision,
        plan_kind,
        payload.report_kind,
        payload.report_context,
    )
    .await
}

async fn maybe_execute_local_video_task_content_stream(
    state: &AppState,
    control_base_url: &str,
    executor_base_url: &str,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
    plan_kind: &str,
) -> Result<Option<Response<Body>>, GatewayError> {
    if plan_kind != OPENAI_VIDEO_CONTENT_PLAN_KIND
        || decision.route_family.as_deref() != Some("openai")
    {
        return Ok(None);
    }

    if let Some(task_id) =
        crate::gateway::video_tasks::extract_openai_task_id_from_content_path(parts.uri.path())
    {
        let refresh_path = format!("/v1/videos/{task_id}");
        if let Some(refresh_plan) = state.video_tasks.prepare_read_refresh_sync_plan(
            Some("openai"),
            &refresh_path,
            trace_id,
        ) {
            state
                .execute_video_task_refresh_plan(executor_base_url, &refresh_plan)
                .await?;
        }
    }

    let Some(action) = state.video_tasks.prepare_openai_content_stream_action(
        parts.uri.path(),
        parts.uri.query(),
        trace_id,
    ) else {
        return Ok(None);
    };

    match action {
        crate::gateway::video_tasks::LocalVideoTaskContentAction::Immediate {
            status_code,
            body_json,
        } => Ok(Some(build_json_response(
            trace_id,
            decision,
            status_code,
            &body_json,
        )?)),
        crate::gateway::video_tasks::LocalVideoTaskContentAction::StreamPlan(plan) => {
            execute_executor_stream(
                state,
                control_base_url,
                executor_base_url,
                plan,
                trace_id,
                decision,
                plan_kind,
                None,
                None,
            )
            .await
        }
    }
}

fn build_json_response(
    trace_id: &str,
    decision: &GatewayControlDecision,
    status_code: u16,
    body_json: &serde_json::Value,
) -> Result<Response<Body>, GatewayError> {
    let body_bytes =
        serde_json::to_vec(body_json).map_err(|err| GatewayError::Internal(err.to_string()))?;
    let mut headers = BTreeMap::new();
    headers.insert("content-type".to_string(), "application/json".to_string());
    headers.insert("content-length".to_string(), body_bytes.len().to_string());
    build_client_response_from_parts(
        status_code,
        &headers,
        Body::from(body_bytes),
        trace_id,
        Some(decision),
    )
}
