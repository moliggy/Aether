use std::collections::BTreeMap;
use std::io::Error as IoError;

use aether_contracts::{
    ExecutionPlan, ExecutionResult, ExecutionTelemetry, StreamFrame, StreamFramePayload,
};
use async_stream::stream;
use axum::body::{Body, Bytes};
use axum::http::Response;
use base64::Engine as _;
use futures_util::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::mpsc;
use tokio_util::codec::{FramedRead, LinesCodec};
use tokio_util::io::StreamReader;
use tracing::warn;

use crate::gateway::constants::*;
use crate::gateway::headers::{collect_control_headers, header_equals, is_json_request};
use crate::gateway::{
    build_client_response, build_client_response_from_parts, AppState, GatewayControlAuthContext,
    GatewayControlDecision, GatewayError,
};

const GEMINI_FILES_GET_PLAN_KIND: &str = "gemini_files_get";
const GEMINI_FILES_LIST_PLAN_KIND: &str = "gemini_files_list";
const GEMINI_FILES_UPLOAD_PLAN_KIND: &str = "gemini_files_upload";
const GEMINI_FILES_DELETE_PLAN_KIND: &str = "gemini_files_delete";
const GEMINI_FILES_DOWNLOAD_PLAN_KIND: &str = "gemini_files_download";
const OPENAI_VIDEO_CONTENT_PLAN_KIND: &str = "openai_video_content";
const OPENAI_VIDEO_CANCEL_SYNC_PLAN_KIND: &str = "openai_video_cancel_sync";
const OPENAI_VIDEO_REMIX_SYNC_PLAN_KIND: &str = "openai_video_remix_sync";
const OPENAI_VIDEO_DELETE_SYNC_PLAN_KIND: &str = "openai_video_delete_sync";
const GEMINI_VIDEO_CREATE_SYNC_PLAN_KIND: &str = "gemini_video_create_sync";
const GEMINI_VIDEO_CANCEL_SYNC_PLAN_KIND: &str = "gemini_video_cancel_sync";
const OPENAI_CHAT_STREAM_PLAN_KIND: &str = "openai_chat_stream";
const CLAUDE_CHAT_STREAM_PLAN_KIND: &str = "claude_chat_stream";
const GEMINI_CHAT_STREAM_PLAN_KIND: &str = "gemini_chat_stream";
const OPENAI_CLI_STREAM_PLAN_KIND: &str = "openai_cli_stream";
const CLAUDE_CLI_STREAM_PLAN_KIND: &str = "claude_cli_stream";
const GEMINI_CLI_STREAM_PLAN_KIND: &str = "gemini_cli_stream";
const OPENAI_VIDEO_CREATE_SYNC_PLAN_KIND: &str = "openai_video_create_sync";
const OPENAI_CHAT_SYNC_PLAN_KIND: &str = "openai_chat_sync";
const OPENAI_CLI_SYNC_PLAN_KIND: &str = "openai_cli_sync";
const OPENAI_COMPACT_SYNC_PLAN_KIND: &str = "openai_compact_sync";
const CLAUDE_CHAT_SYNC_PLAN_KIND: &str = "claude_chat_sync";
const GEMINI_CHAT_SYNC_PLAN_KIND: &str = "gemini_chat_sync";
const CLAUDE_CLI_SYNC_PLAN_KIND: &str = "claude_cli_sync";
const GEMINI_CLI_SYNC_PLAN_KIND: &str = "gemini_cli_sync";
const EXECUTOR_SYNC_ACTION: &str = "executor_sync";
const EXECUTOR_STREAM_ACTION: &str = "executor_stream";
const MAX_ERROR_BODY_BYTES: usize = 16_384;

#[derive(Debug, Serialize)]
struct GatewayControlPlanRequest {
    trace_id: String,
    method: String,
    path: String,
    query_string: Option<String>,
    headers: BTreeMap<String, String>,
    body_json: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    body_base64: Option<String>,
    auth_context: Option<GatewayControlAuthContext>,
}

#[derive(Debug, Deserialize)]
struct GatewayControlPlanResponse {
    action: String,
    #[serde(default)]
    plan_kind: Option<String>,
    #[serde(default)]
    plan: Option<ExecutionPlan>,
    #[serde(default)]
    report_kind: Option<String>,
    #[serde(default)]
    report_context: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct GatewaySyncReportRequest {
    trace_id: String,
    report_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    report_context: Option<serde_json::Value>,
    status_code: u16,
    headers: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    body_json: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    body_base64: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    telemetry: Option<ExecutionTelemetry>,
}

#[derive(Debug, Serialize)]
struct GatewayStreamReportRequest {
    trace_id: String,
    report_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    report_context: Option<serde_json::Value>,
    status_code: u16,
    headers: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    body_base64: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    telemetry: Option<ExecutionTelemetry>,
}

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

    let request_payload = GatewayControlPlanRequest {
        trace_id: trace_id.to_string(),
        method: parts.method.to_string(),
        path: parts.uri.path().to_string(),
        query_string: parts.uri.query().map(ToOwned::to_owned),
        headers: collect_control_headers(&parts.headers),
        body_json,
        body_base64,
        auth_context: decision.auth_context.clone(),
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
        return Ok(None);
    }

    if header_equals(response.headers(), CONTROL_EXECUTED_HEADER, "true")
        && response.status() != http::StatusCode::OK
    {
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
        plan,
        trace_id,
        decision,
        plan_kind,
        payload.report_kind,
        payload.report_context,
    )
    .await
}

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

    let request_payload = GatewayControlPlanRequest {
        trace_id: trace_id.to_string(),
        method: parts.method.to_string(),
        path: parts.uri.path().to_string(),
        query_string: parts.uri.query().map(ToOwned::to_owned),
        headers: collect_control_headers(&parts.headers),
        body_json,
        body_base64,
        auth_context: decision.auth_context.clone(),
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
        return Ok(None);
    }

    if header_equals(response.headers(), CONTROL_EXECUTED_HEADER, "true")
        && response.status() != http::StatusCode::OK
    {
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

fn resolve_direct_executor_stream_plan_kind(
    parts: &http::request::Parts,
    decision: &GatewayControlDecision,
) -> Option<&'static str> {
    if decision.route_class.as_deref() != Some("ai_public") {
        return None;
    }

    if decision.route_family.as_deref() == Some("gemini")
        && decision.route_kind.as_deref() == Some("files")
        && parts.method == http::Method::GET
        && parts.uri.path().ends_with(":download")
    {
        return Some(GEMINI_FILES_DOWNLOAD_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("chat")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/chat/completions"
    {
        return Some(OPENAI_CHAT_STREAM_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("claude")
        && decision.route_kind.as_deref() == Some("chat")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/messages"
    {
        return Some(CLAUDE_CHAT_STREAM_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("claude")
        && decision.route_kind.as_deref() == Some("cli")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/messages"
    {
        return Some(CLAUDE_CLI_STREAM_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("gemini")
        && decision.route_kind.as_deref() == Some("chat")
        && parts.method == http::Method::POST
        && parts.uri.path().ends_with(":streamGenerateContent")
    {
        return Some(GEMINI_CHAT_STREAM_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("gemini")
        && decision.route_kind.as_deref() == Some("cli")
        && parts.method == http::Method::POST
        && parts.uri.path().ends_with(":streamGenerateContent")
    {
        return Some(GEMINI_CLI_STREAM_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("cli")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/responses"
    {
        return Some(OPENAI_CLI_STREAM_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("video")
        && parts.method == http::Method::GET
        && parts.uri.path().ends_with("/content")
    {
        return Some(OPENAI_VIDEO_CONTENT_PLAN_KIND);
    }

    None
}

fn resolve_direct_executor_sync_plan_kind(
    parts: &http::request::Parts,
    decision: &GatewayControlDecision,
) -> Option<&'static str> {
    if decision.route_class.as_deref() != Some("ai_public") {
        return None;
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("video")
        && parts.method == http::Method::POST
        && parts.uri.path().starts_with("/v1/videos/")
        && parts.uri.path().ends_with("/cancel")
    {
        return Some(OPENAI_VIDEO_CANCEL_SYNC_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("video")
        && parts.method == http::Method::POST
        && parts.uri.path().starts_with("/v1/videos/")
        && parts.uri.path().ends_with("/remix")
    {
        return Some(OPENAI_VIDEO_REMIX_SYNC_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("video")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/videos"
    {
        return Some(OPENAI_VIDEO_CREATE_SYNC_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("video")
        && parts.method == http::Method::DELETE
        && parts.uri.path().starts_with("/v1/videos/")
    {
        return Some(OPENAI_VIDEO_DELETE_SYNC_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("gemini")
        && decision.route_kind.as_deref() == Some("video")
        && parts.method == http::Method::POST
        && parts.uri.path().ends_with(":cancel")
    {
        return Some(GEMINI_VIDEO_CANCEL_SYNC_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("gemini")
        && decision.route_kind.as_deref() == Some("video")
        && parts.method == http::Method::POST
        && parts.uri.path().ends_with(":predictLongRunning")
    {
        return Some(GEMINI_VIDEO_CREATE_SYNC_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("chat")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/chat/completions"
    {
        return Some(OPENAI_CHAT_SYNC_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("cli")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/responses"
    {
        return Some(OPENAI_CLI_SYNC_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("compact")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/responses/compact"
    {
        return Some(OPENAI_COMPACT_SYNC_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("claude")
        && decision.route_kind.as_deref() == Some("chat")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/messages"
    {
        return Some(CLAUDE_CHAT_SYNC_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("claude")
        && decision.route_kind.as_deref() == Some("cli")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/messages"
    {
        return Some(CLAUDE_CLI_SYNC_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("gemini")
        && decision.route_kind.as_deref() == Some("chat")
        && parts.method == http::Method::POST
        && parts.uri.path().ends_with(":generateContent")
    {
        return Some(GEMINI_CHAT_SYNC_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("gemini")
        && decision.route_kind.as_deref() == Some("cli")
        && parts.method == http::Method::POST
        && parts.uri.path().ends_with(":generateContent")
    {
        return Some(GEMINI_CLI_SYNC_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("gemini")
        && decision.route_kind.as_deref() == Some("files")
    {
        if parts.method == http::Method::POST && parts.uri.path() == "/upload/v1beta/files" {
            return Some(GEMINI_FILES_UPLOAD_PLAN_KIND);
        }
        if parts.method == http::Method::GET && parts.uri.path() == "/v1beta/files" {
            return Some(GEMINI_FILES_LIST_PLAN_KIND);
        }
        if parts.method == http::Method::GET
            && parts.uri.path().starts_with("/v1beta/files/")
            && !parts.uri.path().ends_with(":download")
        {
            return Some(GEMINI_FILES_GET_PLAN_KIND);
        }
        if parts.method == http::Method::DELETE
            && parts.uri.path().starts_with("/v1beta/files/")
            && !parts.uri.path().ends_with(":download")
        {
            return Some(GEMINI_FILES_DELETE_PLAN_KIND);
        }
    }

    None
}

#[allow(clippy::too_many_arguments)] // internal function, grouping would add unnecessary indirection
async fn execute_executor_stream(
    state: &AppState,
    control_base_url: &str,
    executor_base_url: &str,
    plan: ExecutionPlan,
    trace_id: &str,
    decision: &GatewayControlDecision,
    plan_kind: &str,
    report_kind: Option<String>,
    report_context: Option<serde_json::Value>,
) -> Result<Option<Response<Body>>, GatewayError> {
    let response = match state
        .client
        .post(format!("{executor_base_url}/v1/execute/stream"))
        .header(TRACE_ID_HEADER, trace_id)
        .json(&plan)
        .send()
        .await
    {
        Ok(response) => response,
        Err(err) => {
            warn!(trace_id = %trace_id, error = %err, "gateway direct executor stream unavailable");
            return Ok(None);
        }
    };

    if response.status() != http::StatusCode::OK {
        return Ok(Some(build_client_response(
            response,
            trace_id,
            Some(decision),
        )?));
    }

    let stream = response
        .bytes_stream()
        .map_err(|err| IoError::other(err.to_string()));
    let reader = StreamReader::new(stream);
    let mut lines = FramedRead::new(reader, LinesCodec::new());

    let first_frame = read_next_frame(&mut lines).await?.ok_or_else(|| {
        GatewayError::Internal("executor stream ended before headers frame".to_string())
    })?;
    let StreamFramePayload::Headers {
        status_code,
        headers,
    } = first_frame.payload
    else {
        return Err(GatewayError::Internal(
            "executor stream must start with headers frame".to_string(),
        ));
    };

    if should_fallback_to_control_stream(plan_kind, status_code) {
        return Ok(None);
    }

    if status_code >= 400 {
        let error_body = collect_error_body(&mut lines).await?;
        return Ok(Some(build_executor_error_response(
            trace_id,
            decision,
            plan_kind,
            status_code,
            headers,
            error_body,
        )?));
    }

    let (tx, mut rx) = mpsc::channel::<Result<Bytes, IoError>>(16);
    let state_for_report = state.clone();
    let trace_id_owned = trace_id.to_string();
    let control_base_url_owned = control_base_url.to_string();
    let headers_for_report = headers.clone();
    let report_kind_owned = report_kind.clone();
    let report_context_owned = report_context.clone();
    tokio::spawn(async move {
        let mut buffered_body = Vec::new();
        let mut telemetry: Option<ExecutionTelemetry> = None;
        let mut reached_eof = false;

        loop {
            let next_frame = match read_next_frame(&mut lines).await {
                Ok(frame) => frame,
                Err(err) => {
                    warn!(trace_id = %trace_id_owned, error = ?err, "gateway failed to decode executor stream frame");
                    break;
                }
            };
            let Some(frame) = next_frame else {
                break;
            };
            match frame.payload {
                StreamFramePayload::Data { chunk_b64, text } => {
                    let chunk = if let Some(chunk_b64) = chunk_b64 {
                        match base64::engine::general_purpose::STANDARD.decode(chunk_b64) {
                            Ok(decoded) => decoded,
                            Err(err) => {
                                warn!(trace_id = %trace_id_owned, error = %err, "gateway failed to decode executor chunk");
                                break;
                            }
                        }
                    } else if let Some(text) = text {
                        text.into_bytes()
                    } else {
                        Vec::new()
                    };

                    if chunk.is_empty() {
                        continue;
                    }
                    buffered_body.extend_from_slice(&chunk);
                    let _ = tx.send(Ok(Bytes::from(chunk))).await;
                }
                StreamFramePayload::Telemetry {
                    telemetry: frame_telemetry,
                } => {
                    telemetry = Some(frame_telemetry);
                }
                StreamFramePayload::Eof { .. } => {
                    reached_eof = true;
                    break;
                }
                StreamFramePayload::Error { error } => {
                    warn!(trace_id = %trace_id_owned, error = %error.message, "executor stream emitted error frame");
                    break;
                }
                StreamFramePayload::Headers { .. } => {}
            }
        }

        drop(tx);

        if reached_eof {
            if let Some(report_kind) = report_kind_owned {
                let report = GatewayStreamReportRequest {
                    trace_id: trace_id_owned.clone(),
                    report_kind,
                    report_context: report_context_owned,
                    status_code,
                    headers: headers_for_report,
                    body_base64: (!buffered_body.is_empty())
                        .then(|| base64::engine::general_purpose::STANDARD.encode(&buffered_body)),
                    telemetry,
                };
                if let Err(err) = submit_stream_report(
                    &state_for_report,
                    &control_base_url_owned,
                    &trace_id_owned,
                    report,
                )
                .await
                {
                    warn!(trace_id = %trace_id_owned, error = ?err, "gateway failed to submit stream execution report");
                }
            }
        }
    });

    let body_stream = stream! {
        while let Some(item) = rx.recv().await {
            yield item;
        }
    };

    Ok(Some(build_client_response_from_parts(
        status_code,
        &headers,
        Body::from_stream(body_stream),
        trace_id,
        Some(decision),
    )?))
}

#[allow(clippy::too_many_arguments)] // internal function, grouping would add unnecessary indirection
async fn execute_executor_sync(
    state: &AppState,
    control_base_url: &str,
    executor_base_url: &str,
    plan: ExecutionPlan,
    trace_id: &str,
    decision: &GatewayControlDecision,
    plan_kind: &str,
    report_kind: Option<String>,
    report_context: Option<serde_json::Value>,
) -> Result<Option<Response<Body>>, GatewayError> {
    let response = match state
        .client
        .post(format!("{executor_base_url}/v1/execute/sync"))
        .header(TRACE_ID_HEADER, trace_id)
        .json(&plan)
        .send()
        .await
    {
        Ok(response) => response,
        Err(err) => {
            warn!(trace_id = %trace_id, error = %err, "gateway direct executor sync unavailable");
            return Ok(None);
        }
    };

    if response.status() != http::StatusCode::OK {
        return Ok(Some(build_client_response(
            response,
            trace_id,
            Some(decision),
        )?));
    }

    let result: ExecutionResult = response
        .json()
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
    let mut headers = result.headers.clone();
    let (body_bytes, body_json, body_base64) = decode_execution_result_body(&result, &mut headers)?;

    if should_fallback_to_control_sync(plan_kind, &result, body_json.as_ref()) {
        return Ok(None);
    }

    if should_finalize_sync_response(plan_kind) {
        let payload = GatewaySyncReportRequest {
            trace_id: trace_id.to_string(),
            report_kind: report_kind.unwrap_or_default(),
            report_context,
            status_code: result.status_code,
            headers: headers.clone(),
            body_json: body_json.clone(),
            body_base64: body_base64.clone(),
            telemetry: result.telemetry.clone(),
        };
        let response = submit_sync_finalize(state, control_base_url, trace_id, payload).await?;
        return Ok(Some(build_client_response(
            response,
            trace_id,
            Some(decision),
        )?));
    }

    if let Some(report_kind) = report_kind {
        let report = GatewaySyncReportRequest {
            trace_id: trace_id.to_string(),
            report_kind,
            report_context,
            status_code: result.status_code,
            headers: headers.clone(),
            body_json: body_json.clone(),
            body_base64: body_base64.clone(),
            telemetry: result.telemetry.clone(),
        };
        if let Err(err) = submit_sync_report(state, control_base_url, trace_id, report).await {
            warn!(trace_id = %trace_id, error = ?err, "gateway failed to submit sync execution report");
        }
    }

    Ok(Some(build_client_response_from_parts(
        result.status_code,
        &headers,
        Body::from(body_bytes),
        trace_id,
        Some(decision),
    )?))
}

fn should_fallback_to_control_sync(
    plan_kind: &str,
    result: &ExecutionResult,
    body_json: Option<&serde_json::Value>,
) -> bool {
    if matches!(
        plan_kind,
        OPENAI_VIDEO_CANCEL_SYNC_PLAN_KIND | GEMINI_VIDEO_CANCEL_SYNC_PLAN_KIND
    ) {
        return result.status_code >= 400;
    }

    if plan_kind == OPENAI_VIDEO_DELETE_SYNC_PLAN_KIND {
        return result.status_code >= 400 && result.status_code != 404;
    }

    if !matches!(
        plan_kind,
        OPENAI_VIDEO_CREATE_SYNC_PLAN_KIND
            | OPENAI_VIDEO_REMIX_SYNC_PLAN_KIND
            | GEMINI_VIDEO_CREATE_SYNC_PLAN_KIND
            | OPENAI_CHAT_SYNC_PLAN_KIND
            | OPENAI_CLI_SYNC_PLAN_KIND
            | OPENAI_COMPACT_SYNC_PLAN_KIND
            | CLAUDE_CHAT_SYNC_PLAN_KIND
            | GEMINI_CHAT_SYNC_PLAN_KIND
            | CLAUDE_CLI_SYNC_PLAN_KIND
            | GEMINI_CLI_SYNC_PLAN_KIND
    ) {
        return false;
    }

    if result.status_code >= 400 {
        return true;
    }

    let Some(body_json) = body_json else {
        return true;
    };

    body_json.get("error").is_some()
}

fn should_finalize_sync_response(plan_kind: &str) -> bool {
    matches!(
        plan_kind,
        OPENAI_VIDEO_CANCEL_SYNC_PLAN_KIND
            | OPENAI_VIDEO_CREATE_SYNC_PLAN_KIND
            | OPENAI_VIDEO_REMIX_SYNC_PLAN_KIND
            | OPENAI_VIDEO_DELETE_SYNC_PLAN_KIND
            | GEMINI_VIDEO_CREATE_SYNC_PLAN_KIND
            | GEMINI_VIDEO_CANCEL_SYNC_PLAN_KIND
    )
}

fn should_fallback_to_control_stream(plan_kind: &str, status_code: u16) -> bool {
    matches!(
        plan_kind,
        OPENAI_CHAT_STREAM_PLAN_KIND
            | CLAUDE_CHAT_STREAM_PLAN_KIND
            | GEMINI_CHAT_STREAM_PLAN_KIND
            | OPENAI_CLI_STREAM_PLAN_KIND
            | CLAUDE_CLI_STREAM_PLAN_KIND
            | GEMINI_CLI_STREAM_PLAN_KIND
    ) && status_code >= 400
}

fn is_matching_stream_request(
    plan_kind: &str,
    parts: &http::request::Parts,
    body_json: &serde_json::Value,
) -> bool {
    match plan_kind {
        OPENAI_CHAT_STREAM_PLAN_KIND
        | CLAUDE_CHAT_STREAM_PLAN_KIND
        | OPENAI_CLI_STREAM_PLAN_KIND
        | CLAUDE_CLI_STREAM_PLAN_KIND => body_json
            .get("stream")
            .and_then(|value| value.as_bool())
            .unwrap_or(false),
        GEMINI_CHAT_STREAM_PLAN_KIND | GEMINI_CLI_STREAM_PLAN_KIND => {
            parts.uri.path().ends_with(":streamGenerateContent")
        }
        _ => true,
    }
}

type DecodedBody = (Vec<u8>, Option<serde_json::Value>, Option<String>);

fn decode_execution_result_body(
    result: &ExecutionResult,
    headers: &mut BTreeMap<String, String>,
) -> Result<DecodedBody, GatewayError> {
    let Some(body) = result.body.as_ref() else {
        return Ok((Vec::new(), None, None));
    };

    if let Some(json_body) = body.json_body.clone() {
        headers
            .entry("content-type".to_string())
            .or_insert_with(|| "application/json".to_string());
        let bytes = serde_json::to_vec(&json_body)
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        headers.insert("content-length".to_string(), bytes.len().to_string());
        return Ok((bytes, Some(json_body), None));
    }

    if let Some(body_bytes_b64) = body.body_bytes_b64.clone() {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(&body_bytes_b64)
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        return Ok((bytes, None, Some(body_bytes_b64)));
    }

    Ok((Vec::new(), None, None))
}

async fn submit_sync_report(
    state: &AppState,
    control_base_url: &str,
    trace_id: &str,
    payload: GatewaySyncReportRequest,
) -> Result<(), GatewayError> {
    let response = state
        .client
        .post(format!(
            "{control_base_url}/api/internal/gateway/report-sync"
        ))
        .header(TRACE_ID_HEADER, trace_id)
        .json(&payload)
        .send()
        .await
        .map_err(|err| GatewayError::ControlUnavailable {
            trace_id: trace_id.to_string(),
            message: err.to_string(),
        })?;

    response
        .error_for_status()
        .map_err(|err| GatewayError::ControlUnavailable {
            trace_id: trace_id.to_string(),
            message: err.to_string(),
        })?;
    Ok(())
}

async fn submit_sync_finalize(
    state: &AppState,
    control_base_url: &str,
    trace_id: &str,
    payload: GatewaySyncReportRequest,
) -> Result<reqwest::Response, GatewayError> {
    state
        .client
        .post(format!(
            "{control_base_url}/api/internal/gateway/finalize-sync"
        ))
        .header(TRACE_ID_HEADER, trace_id)
        .json(&payload)
        .send()
        .await
        .map_err(|err| GatewayError::ControlUnavailable {
            trace_id: trace_id.to_string(),
            message: err.to_string(),
        })
}

async fn submit_stream_report(
    state: &AppState,
    control_base_url: &str,
    trace_id: &str,
    payload: GatewayStreamReportRequest,
) -> Result<(), GatewayError> {
    let response = state
        .client
        .post(format!(
            "{control_base_url}/api/internal/gateway/report-stream"
        ))
        .header(TRACE_ID_HEADER, trace_id)
        .json(&payload)
        .send()
        .await
        .map_err(|err| GatewayError::ControlUnavailable {
            trace_id: trace_id.to_string(),
            message: err.to_string(),
        })?;

    response
        .error_for_status()
        .map_err(|err| GatewayError::ControlUnavailable {
            trace_id: trace_id.to_string(),
            message: err.to_string(),
        })?;
    Ok(())
}

async fn collect_error_body<R>(
    lines: &mut FramedRead<R, LinesCodec>,
) -> Result<Vec<u8>, GatewayError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut body = Vec::new();
    while let Some(frame) = read_next_frame(lines).await? {
        match frame.payload {
            StreamFramePayload::Data { chunk_b64, text } => {
                let chunk = if let Some(chunk_b64) = chunk_b64 {
                    base64::engine::general_purpose::STANDARD
                        .decode(chunk_b64)
                        .map_err(|err| GatewayError::Internal(err.to_string()))?
                } else {
                    text.unwrap_or_default().into_bytes()
                };
                body.extend_from_slice(&chunk);
                if body.len() >= MAX_ERROR_BODY_BYTES {
                    body.truncate(MAX_ERROR_BODY_BYTES);
                    break;
                }
            }
            StreamFramePayload::Telemetry { .. } => {}
            StreamFramePayload::Eof { .. } => break,
            StreamFramePayload::Error { error } => {
                warn!(error = %error.message, "executor stream emitted error frame while collecting error body");
                break;
            }
            StreamFramePayload::Headers { .. } => {}
        }
    }
    Ok(body)
}

async fn read_next_frame<R>(
    lines: &mut FramedRead<R, LinesCodec>,
) -> Result<Option<StreamFrame>, GatewayError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    while let Some(line) = lines.next().await {
        let line = line.map_err(|err| GatewayError::Internal(err.to_string()))?;
        if line.trim().is_empty() {
            continue;
        }
        let frame: StreamFrame =
            serde_json::from_str(&line).map_err(|err| GatewayError::Internal(err.to_string()))?;
        return Ok(Some(frame));
    }
    Ok(None)
}

fn build_executor_error_response(
    trace_id: &str,
    decision: &GatewayControlDecision,
    plan_kind: &str,
    status_code: u16,
    headers: BTreeMap<String, String>,
    error_body: Vec<u8>,
) -> Result<Response<Body>, GatewayError> {
    let content_type = headers
        .get("content-type")
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();

    if plan_kind == GEMINI_FILES_DOWNLOAD_PLAN_KIND && !content_type.starts_with("application/json")
    {
        let wrapped = serde_json::to_vec(&json!({
            "error": String::from_utf8_lossy(&error_body).to_string(),
        }))
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let wrapped_headers =
            BTreeMap::from([("content-type".to_string(), "application/json".to_string())]);
        return build_client_response_from_parts(
            status_code,
            &wrapped_headers,
            Body::from(wrapped),
            trace_id,
            Some(decision),
        );
    }

    if plan_kind == OPENAI_VIDEO_CONTENT_PLAN_KIND && !content_type.starts_with("application/json")
    {
        let wrapped = serde_json::to_vec(&json!({
            "error": {
                "type": "upstream_error",
                "message": "Video not available",
            }
        }))
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
        let wrapped_headers =
            BTreeMap::from([("content-type".to_string(), "application/json".to_string())]);
        return build_client_response_from_parts(
            status_code,
            &wrapped_headers,
            Body::from(wrapped),
            trace_id,
            Some(decision),
        );
    }

    build_client_response_from_parts(
        status_code,
        &headers,
        Body::from(error_body),
        trace_id,
        Some(decision),
    )
}
