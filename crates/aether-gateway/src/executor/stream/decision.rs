use super::super::plan_builders::{
    build_gemini_stream_plan_from_decision, build_openai_chat_stream_plan_from_decision,
    build_openai_cli_stream_plan_from_decision, build_passthrough_stream_plan_from_decision,
    build_standard_stream_plan_from_decision,
};
use super::super::*;
use super::execution::execute_executor_stream;

pub(super) async fn maybe_execute_stream_via_decision(
    state: &AppState,
    control_base_url: &str,
    executor_base_url: &str,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
    body_json: &serde_json::Value,
    plan_kind: &str,
) -> Result<Option<Response<Body>>, GatewayError> {
    let request_payload = GatewayControlPlanRequest {
        trace_id: trace_id.to_string(),
        method: parts.method.to_string(),
        path: parts.uri.path().to_string(),
        query_string: parts.uri.query().map(ToOwned::to_owned),
        headers: collect_control_headers(&parts.headers),
        body_json: body_json.clone(),
        body_base64: None,
        auth_context: resolve_executor_auth_context(
            state,
            decision,
            &parts.headers,
            &parts.uri,
            trace_id,
        )
        .await?,
    };

    let response = state
        .client
        .post(format!(
            "{control_base_url}/api/internal/gateway/decision-stream"
        ))
        .header(TRACE_ID_HEADER, trace_id)
        .json(&request_payload)
        .send()
        .await
        .map_err(|err| GatewayError::ControlUnavailable {
            trace_id: trace_id.to_string(),
            message: err.to_string(),
        })?;

    if header_equals(response.headers(), CONTROL_EXECUTED_HEADER, "true") {
        if !allow_control_execute_fallback(state, parts) {
            return Ok(None);
        }
        return Ok(Some(build_client_response(
            response,
            trace_id,
            Some(decision),
        )?));
    }

    let response = match response.error_for_status() {
        Ok(response) => response,
        Err(err) => {
            warn!(trace_id = %trace_id, error = %err, "gateway decision-stream returned error status");
            return Ok(None);
        }
    };

    let payload: GatewayControlSyncDecisionResponse = match response.json().await {
        Ok(payload) => payload,
        Err(err) => {
            warn!(trace_id = %trace_id, error = %err, "gateway decision-stream response deserialization failed");
            return Ok(None);
        }
    };

    if let Some(auth_context) = payload.auth_context.clone() {
        cache_executor_auth_context(state, decision, &parts.headers, &parts.uri, auth_context);
    }

    if payload.action != EXECUTOR_STREAM_DECISION_ACTION {
        return Ok(None);
    }
    if payload.decision_kind.as_deref() != Some(plan_kind) {
        return Ok(None);
    }

    let plan_and_report = match plan_kind {
        OPENAI_CHAT_STREAM_PLAN_KIND => {
            build_openai_chat_stream_plan_from_decision(parts, body_json, payload)?
        }
        CLAUDE_CHAT_STREAM_PLAN_KIND => {
            build_standard_stream_plan_from_decision(parts, body_json, payload, true)?
        }
        GEMINI_CHAT_STREAM_PLAN_KIND => {
            build_gemini_stream_plan_from_decision(parts, body_json, payload)?
        }
        OPENAI_CLI_STREAM_PLAN_KIND => {
            build_openai_cli_stream_plan_from_decision(parts, body_json, payload, false)?
        }
        OPENAI_COMPACT_STREAM_PLAN_KIND => {
            build_openai_cli_stream_plan_from_decision(parts, body_json, payload, true)?
        }
        CLAUDE_CLI_STREAM_PLAN_KIND => {
            build_standard_stream_plan_from_decision(parts, body_json, payload, true)?
        }
        GEMINI_CLI_STREAM_PLAN_KIND => {
            build_gemini_stream_plan_from_decision(parts, body_json, payload)?
        }
        GEMINI_FILES_DOWNLOAD_PLAN_KIND | OPENAI_VIDEO_CONTENT_PLAN_KIND => {
            build_passthrough_stream_plan_from_decision(parts, payload)?
        }
        _ => None,
    };

    let Some(plan_and_report) = plan_and_report else {
        return Ok(None);
    };

    execute_executor_stream(
        state,
        control_base_url,
        executor_base_url,
        plan_and_report.plan,
        trace_id,
        decision,
        plan_kind,
        plan_and_report.report_kind,
        plan_and_report.report_context,
    )
    .await
}
