use base64::Engine as _;

use super::super::chat::{
    aggregate_claude_stream_sync_response, aggregate_gemini_stream_sync_response,
};
use super::super::common::{
    build_local_success_outcome, build_local_success_outcome_with_conversion_report,
    local_finalize_allows_envelope, unwrap_local_finalize_response_value,
    LocalCoreSyncFinalizeOutcome,
};
use super::super::*;
use super::{convert_claude_cli_response_to_openai_cli, convert_gemini_cli_response_to_openai_cli};

pub(crate) fn maybe_build_local_openai_cli_stream_sync_response(
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &GatewaySyncReportRequest,
) -> Result<Option<LocalCoreSyncFinalizeOutcome>, GatewayError> {
    if !matches!(
        payload.report_kind.as_str(),
        "openai_cli_sync_finalize" | "openai_compact_sync_finalize"
    ) || payload.status_code >= 400
    {
        return Ok(None);
    }

    let Some(report_context) = payload.report_context.as_ref() else {
        return Ok(None);
    };
    let provider_api_format = report_context
        .get("provider_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let client_api_format = report_context
        .get("client_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let needs_conversion = report_context
        .get("needs_conversion")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if !local_finalize_allows_envelope(report_context) {
        return Ok(None);
    }
    if !matches!(
        provider_api_format.as_str(),
        "openai:cli" | "openai:compact"
    ) || provider_api_format != client_api_format
        || needs_conversion
    {
        return Ok(None);
    }

    let Some(body_base64) = payload.body_base64.as_deref() else {
        return Ok(None);
    };
    let body_bytes = base64::engine::general_purpose::STANDARD
        .decode(body_base64)
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
    let body_json = match aggregate_openai_cli_stream_sync_response(&body_bytes) {
        Some(body_json) => body_json,
        None => return Ok(None),
    };
    let Some(body_json) = unwrap_local_finalize_response_value(body_json, report_context)? else {
        return Ok(None);
    };

    Ok(Some(build_local_success_outcome(
        trace_id, decision, payload, body_json,
    )?))
}

pub(crate) fn maybe_build_local_openai_cli_cross_format_stream_sync_response(
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &GatewaySyncReportRequest,
) -> Result<Option<LocalCoreSyncFinalizeOutcome>, GatewayError> {
    if !matches!(
        payload.report_kind.as_str(),
        "openai_cli_sync_finalize" | "openai_compact_sync_finalize"
    ) || payload.status_code >= 400
    {
        return Ok(None);
    }

    let Some(report_context) = payload.report_context.as_ref() else {
        return Ok(None);
    };
    let provider_api_format = report_context
        .get("provider_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let client_api_format = report_context
        .get("client_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let _has_envelope = report_context
        .get("has_envelope")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if !matches!(client_api_format.as_str(), "openai:cli" | "openai:compact")
        || !local_finalize_allows_envelope(report_context)
    {
        return Ok(None);
    }

    let Some(body_base64) = payload.body_base64.as_deref() else {
        return Ok(None);
    };
    let body_bytes = base64::engine::general_purpose::STANDARD
        .decode(body_base64)
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
    let aggregated = match provider_api_format.as_str() {
        "claude:cli" => aggregate_claude_stream_sync_response(&body_bytes),
        "gemini:cli" => aggregate_gemini_stream_sync_response(&body_bytes),
        _ => None,
    };
    let Some(aggregated) = aggregated else {
        return Ok(None);
    };
    let Some(aggregated) = unwrap_local_finalize_response_value(aggregated, report_context)? else {
        return Ok(None);
    };
    let converted = match provider_api_format.as_str() {
        "claude:cli" => convert_claude_cli_response_to_openai_cli(&aggregated, report_context),
        "gemini:cli" => convert_gemini_cli_response_to_openai_cli(&aggregated, report_context),
        _ => None,
    };
    let Some(converted) = converted else {
        return Ok(None);
    };

    Ok(Some(build_local_success_outcome_with_conversion_report(
        trace_id, decision, payload, converted, aggregated,
    )?))
}

pub(crate) fn maybe_build_local_openai_cli_cross_format_sync_response(
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &GatewaySyncReportRequest,
) -> Result<Option<LocalCoreSyncFinalizeOutcome>, GatewayError> {
    if !matches!(
        payload.report_kind.as_str(),
        "openai_cli_sync_finalize" | "openai_compact_sync_finalize"
    ) || payload.status_code >= 400
    {
        return Ok(None);
    }

    let Some(report_context) = payload.report_context.as_ref() else {
        return Ok(None);
    };
    let provider_api_format = report_context
        .get("provider_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let client_api_format = report_context
        .get("client_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if !matches!(client_api_format.as_str(), "openai:cli" | "openai:compact")
        || !local_finalize_allows_envelope(report_context)
    {
        return Ok(None);
    }

    let Some(body_json) = payload.body_json.as_ref() else {
        return Ok(None);
    };
    let Some(body_json) = unwrap_local_finalize_response_value(body_json.clone(), report_context)?
    else {
        return Ok(None);
    };
    let converted = match provider_api_format.as_str() {
        "claude:cli" => convert_claude_cli_response_to_openai_cli(&body_json, report_context),
        "gemini:cli" => convert_gemini_cli_response_to_openai_cli(&body_json, report_context),
        _ => None,
    };
    let Some(converted) = converted else {
        return Ok(None);
    };

    Ok(Some(build_local_success_outcome_with_conversion_report(
        trace_id, decision, payload, converted, body_json,
    )?))
}

pub(super) fn build_openai_cli_response(
    response_id: &str,
    model: &str,
    text: &str,
    function_calls: Vec<Value>,
    prompt_tokens: u64,
    output_tokens: u64,
    total_tokens: u64,
) -> Value {
    let mut output = Vec::new();
    if !text.is_empty() {
        output.push(json!({
            "type": "message",
            "id": format!("{response_id}_msg"),
            "role": "assistant",
            "status": "completed",
            "content": [{
                "type": "output_text",
                "text": text,
                "annotations": []
            }]
        }));
    }
    output.extend(function_calls);
    json!({
        "id": response_id,
        "object": "response",
        "status": "completed",
        "model": model,
        "output": output,
        "usage": {
            "input_tokens": prompt_tokens,
            "output_tokens": output_tokens,
            "total_tokens": total_tokens,
        }
    })
}

pub(crate) fn aggregate_openai_cli_stream_sync_response(body: &[u8]) -> Option<Value> {
    let text = std::str::from_utf8(body).ok()?;

    for raw_line in text.lines() {
        let line = raw_line.trim_matches('\r').trim();
        if line.is_empty() || line.starts_with(':') || line.starts_with("event:") {
            continue;
        }
        let Some(data_line) = line.strip_prefix("data:") else {
            continue;
        };
        let data_line = data_line.trim();
        if data_line.is_empty() || data_line == "[DONE]" {
            continue;
        }

        let event: Value = serde_json::from_str(data_line).ok()?;
        let event_type = event
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if event_type == "response.completed" {
            let response = event.get("response")?.as_object()?.clone();
            return Some(Value::Object(response));
        }
    }

    None
}
