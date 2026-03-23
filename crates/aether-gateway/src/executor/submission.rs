use super::*;

pub(super) fn maybe_build_local_core_error_response(
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &GatewaySyncReportRequest,
) -> Result<Option<Response<Body>>, GatewayError> {
    if !is_core_error_finalize_kind(payload.report_kind.as_str()) {
        return Ok(None);
    }

    let Some(body_json) = payload.body_json.as_ref() else {
        return Ok(None);
    };
    let Some(body_object) = body_json.as_object() else {
        return Ok(None);
    };
    if !body_object.contains_key("error") {
        return Ok(None);
    }

    let default_api_format = infer_default_api_format(payload.report_kind.as_str())
        .unwrap_or_default()
        .to_string();
    let client_api_format = payload
        .report_context
        .as_ref()
        .and_then(|value| value.get("client_api_format"))
        .and_then(|value| value.as_str())
        .unwrap_or(default_api_format.as_str())
        .trim()
        .to_ascii_lowercase();
    let provider_api_format = payload
        .report_context
        .as_ref()
        .and_then(|value| value.get("provider_api_format"))
        .and_then(|value| value.as_str())
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_else(|| client_api_format.clone());

    if client_api_format.is_empty() || client_api_format != provider_api_format {
        return Ok(None);
    }

    let mut response_headers = payload.headers.clone();
    response_headers.remove("content-encoding");
    response_headers.remove("content-length");
    response_headers.insert("content-type".to_string(), "application/json".to_string());

    let body_bytes =
        serde_json::to_vec(body_json).map_err(|err| GatewayError::Internal(err.to_string()))?;
    response_headers.insert("content-length".to_string(), body_bytes.len().to_string());

    Ok(Some(build_client_response_from_parts(
        resolve_local_sync_error_status_code(payload.status_code, body_json),
        &response_headers,
        Body::from(body_bytes),
        trace_id,
        Some(decision),
    )?))
}

fn is_core_error_finalize_kind(report_kind: &str) -> bool {
    matches!(
        report_kind,
        "openai_chat_sync_finalize"
            | "claude_chat_sync_finalize"
            | "gemini_chat_sync_finalize"
            | "openai_cli_sync_finalize"
            | "openai_compact_sync_finalize"
            | "claude_cli_sync_finalize"
            | "gemini_cli_sync_finalize"
    )
}

fn infer_default_api_format(report_kind: &str) -> Option<&'static str> {
    match report_kind {
        "openai_chat_sync_finalize" => Some("openai:chat"),
        "claude_chat_sync_finalize" => Some("claude:chat"),
        "gemini_chat_sync_finalize" => Some("gemini:chat"),
        "openai_cli_sync_finalize" => Some("openai:cli"),
        "openai_compact_sync_finalize" => Some("openai:compact"),
        "claude_cli_sync_finalize" => Some("claude:cli"),
        "gemini_cli_sync_finalize" => Some("gemini:cli"),
        _ => None,
    }
}

pub(crate) fn resolve_core_error_background_report_kind(report_kind: &str) -> Option<String> {
    let mapped = match report_kind {
        "openai_chat_sync_finalize" => "openai_chat_sync_error",
        "claude_chat_sync_finalize" => "claude_chat_sync_error",
        "gemini_chat_sync_finalize" => "gemini_chat_sync_error",
        "openai_cli_sync_finalize" => "openai_cli_sync_error",
        "openai_compact_sync_finalize" => "openai_compact_sync_error",
        "claude_cli_sync_finalize" => "claude_cli_sync_error",
        "gemini_cli_sync_finalize" => "gemini_cli_sync_error",
        _ => return None,
    };

    Some(mapped.to_string())
}

fn resolve_local_sync_error_status_code(status_code: u16, body_json: &serde_json::Value) -> u16 {
    if (400..600).contains(&status_code) {
        return status_code;
    }

    let Some(error_object) = body_json.get("error").and_then(|value| value.as_object()) else {
        return 400;
    };

    for key in ["code", "status"] {
        let Some(value) = error_object.get(key) else {
            continue;
        };
        if let Some(number) = value.as_u64() {
            if (400..600).contains(&number) {
                return number as u16;
            }
        }
        if let Some(text) = value.as_str() {
            if let Ok(number) = text.parse::<u16>() {
                if (400..600).contains(&number) {
                    return number;
                }
            }
        }
    }

    400
}

pub(super) fn strip_utf8_bom_and_ws(mut body: &[u8]) -> &[u8] {
    loop {
        while let Some(first) = body.first() {
            if first.is_ascii_whitespace() {
                body = &body[1..];
            } else {
                break;
            }
        }
        if body.starts_with(&[0xEF, 0xBB, 0xBF]) {
            body = &body[3..];
        } else {
            break;
        }
    }
    body
}

pub(super) fn has_nested_error(value: &serde_json::Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };

    if object.contains_key("error") {
        return true;
    }
    if object
        .get("type")
        .and_then(|value| value.as_str())
        .is_some_and(|value| value == "error")
    {
        return true;
    }

    object
        .get("chunks")
        .and_then(|value| value.as_array())
        .is_some_and(|chunks| {
            chunks.iter().any(|chunk| {
                chunk.as_object().is_some_and(|chunk_object| {
                    chunk_object.contains_key("error")
                        || chunk_object
                            .get("type")
                            .and_then(|value| value.as_str())
                            .is_some_and(|value| value == "error")
                })
            })
        })
}

pub(super) async fn submit_sync_report(
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

pub(super) fn spawn_sync_report(
    state: AppState,
    control_base_url: String,
    trace_id: String,
    payload: GatewaySyncReportRequest,
) {
    tokio::spawn(async move {
        if let Err(err) = submit_sync_report(&state, &control_base_url, &trace_id, payload).await {
            warn!(trace_id = %trace_id, error = ?err, "gateway failed to submit sync execution report");
        }
    });
}

pub(super) fn spawn_sync_finalize(
    state: AppState,
    control_base_url: String,
    trace_id: String,
    payload: GatewaySyncReportRequest,
) {
    tokio::spawn(async move {
        if let Err(err) = submit_sync_finalize(&state, &control_base_url, &trace_id, payload).await
        {
            warn!(trace_id = %trace_id, error = ?err, "gateway failed to submit sync finalize");
        }
    });
}

pub(super) async fn submit_sync_finalize(
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

pub(super) async fn submit_stream_report(
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
