use base64::Engine as _;
use std::collections::BTreeMap;

use serde_json::{json, Map, Value};

use super::PipelineFinalizeError;
use crate::conversion::response::{
    convert_claude_chat_response_to_openai_chat, convert_claude_cli_response_to_openai_cli,
    convert_gemini_chat_response_to_openai_chat, convert_gemini_cli_response_to_openai_cli,
    convert_openai_chat_response_to_claude_chat, convert_openai_chat_response_to_gemini_chat,
    convert_openai_chat_response_to_openai_cli, convert_openai_cli_response_to_openai_chat,
};
use crate::conversion::{sync_chat_response_conversion_kind, sync_cli_response_conversion_kind};

#[derive(Clone, Debug, PartialEq)]
pub struct StandardCrossFormatSyncProduct {
    pub client_body_json: Value,
    pub provider_body_json: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub enum StandardSyncFinalizeNormalizedProduct {
    SuccessBody(Value),
    CrossFormat(StandardCrossFormatSyncProduct),
}

pub fn maybe_build_standard_cross_format_sync_product_from_normalized_payload(
    report_kind: &str,
    status_code: u16,
    report_context: Option<&Value>,
    body_json: Option<&Value>,
    body_base64: Option<&str>,
) -> Result<Option<StandardCrossFormatSyncProduct>, PipelineFinalizeError> {
    if status_code >= 400 {
        return Ok(None);
    }

    let Some(report_context) = report_context else {
        return Ok(None);
    };
    let provider_api_format = report_context
        .get("provider_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let client_api_format = report_context
        .get("client_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default();

    let aggregated_stream_body = match body_base64 {
        Some(body_base64) => {
            let body_bytes = base64::engine::general_purpose::STANDARD.decode(body_base64)?;
            if is_standard_chat_finalize_kind(report_kind) {
                aggregate_standard_chat_stream_sync_response(&body_bytes, provider_api_format)
            } else if is_standard_cli_finalize_kind(report_kind) {
                aggregate_standard_cli_stream_sync_response(&body_bytes, provider_api_format)
            } else {
                return Ok(None);
            }
        }
        None => None,
    };

    let Some(provider_body_json) = aggregated_stream_body.or_else(|| body_json.cloned()) else {
        return Ok(None);
    };

    Ok(maybe_build_standard_cross_format_sync_product(
        report_kind,
        provider_api_format,
        client_api_format,
        report_context,
        provider_body_json,
    ))
}

pub fn maybe_build_standard_same_format_sync_body_from_normalized_payload(
    report_kind: &str,
    status_code: u16,
    report_context: Option<&Value>,
    body_json: Option<&Value>,
    body_base64: Option<&str>,
) -> Result<Option<Value>, PipelineFinalizeError> {
    let stream_body = maybe_build_standard_same_format_stream_sync_body(
        report_kind,
        status_code,
        report_context,
        body_base64,
    )?;
    Ok(stream_body.or_else(|| {
        maybe_build_standard_same_format_sync_body(
            report_kind,
            status_code,
            report_context,
            body_json,
        )
    }))
}

pub fn maybe_build_openai_cli_same_family_sync_body_from_normalized_payload(
    report_kind: &str,
    status_code: u16,
    report_context: Option<&Value>,
    body_json: Option<&Value>,
    body_base64: Option<&str>,
) -> Result<Option<Value>, PipelineFinalizeError> {
    let stream_body = maybe_build_openai_cli_same_family_stream_sync_body(
        report_kind,
        status_code,
        report_context,
        body_base64,
    )?;
    Ok(stream_body.or_else(|| {
        maybe_build_openai_cli_same_family_sync_body(
            report_kind,
            status_code,
            report_context,
            body_json,
        )
    }))
}

pub fn maybe_build_openai_chat_cross_format_sync_product_from_normalized_payload(
    report_kind: &str,
    status_code: u16,
    report_context: Option<&Value>,
    body_json: Option<&Value>,
    body_base64: Option<&str>,
) -> Result<Option<StandardCrossFormatSyncProduct>, PipelineFinalizeError> {
    if report_kind != "openai_chat_sync_finalize" || status_code >= 400 {
        return Ok(None);
    }

    let Some(report_context) = report_context else {
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

    if client_api_format != "openai:chat"
        || sync_chat_response_conversion_kind(&provider_api_format, &client_api_format).is_none()
    {
        return Ok(None);
    }

    let Some(provider_body_json) =
        maybe_build_openai_cross_format_provider_body_from_normalized_payload(
            body_json,
            body_base64,
            &provider_api_format,
        )?
    else {
        return Ok(None);
    };

    let Some(client_body_json) = (match provider_api_format.as_str() {
        "claude:chat" | "claude:cli" => {
            convert_claude_chat_response_to_openai_chat(&provider_body_json, report_context)
        }
        "gemini:chat" | "gemini:cli" => {
            convert_gemini_chat_response_to_openai_chat(&provider_body_json, report_context)
        }
        "openai:cli" => {
            convert_openai_cli_response_to_openai_chat(&provider_body_json, report_context)
        }
        _ => None,
    }) else {
        return Ok(None);
    };

    Ok(Some(StandardCrossFormatSyncProduct {
        client_body_json,
        provider_body_json,
    }))
}

pub fn maybe_build_openai_cli_cross_format_sync_product_from_normalized_payload(
    report_kind: &str,
    status_code: u16,
    report_context: Option<&Value>,
    body_json: Option<&Value>,
    body_base64: Option<&str>,
) -> Result<Option<StandardCrossFormatSyncProduct>, PipelineFinalizeError> {
    if !is_openai_cli_finalize_kind(report_kind) || status_code >= 400 {
        return Ok(None);
    }

    let Some(report_context) = report_context else {
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
        || sync_cli_response_conversion_kind(&provider_api_format, &client_api_format).is_none()
    {
        return Ok(None);
    }

    let Some(provider_body_json) =
        maybe_build_openai_cross_format_provider_body_from_normalized_payload(
            body_json,
            body_base64,
            &provider_api_format,
        )?
    else {
        return Ok(None);
    };

    let Some(client_body_json) = (match provider_api_format.as_str() {
        "openai:cli" => Some(provider_body_json.clone()),
        "claude:chat" | "claude:cli" => {
            convert_claude_cli_response_to_openai_cli(&provider_body_json, report_context)
        }
        "gemini:chat" | "gemini:cli" => {
            convert_gemini_cli_response_to_openai_cli(&provider_body_json, report_context)
        }
        _ => None,
    }) else {
        return Ok(None);
    };

    Ok(Some(StandardCrossFormatSyncProduct {
        client_body_json,
        provider_body_json,
    }))
}

pub fn maybe_build_standard_sync_finalize_product_from_normalized_payload(
    report_kind: &str,
    status_code: u16,
    report_context: Option<&Value>,
    body_json: Option<&Value>,
    body_base64: Option<&str>,
) -> Result<Option<StandardSyncFinalizeNormalizedProduct>, PipelineFinalizeError> {
    if let Some(body_json) = maybe_build_standard_same_format_sync_body_from_normalized_payload(
        report_kind,
        status_code,
        report_context,
        body_json,
        body_base64,
    )? {
        return Ok(Some(StandardSyncFinalizeNormalizedProduct::SuccessBody(
            body_json,
        )));
    }

    if let Some(body_json) = maybe_build_openai_cli_same_family_sync_body_from_normalized_payload(
        report_kind,
        status_code,
        report_context,
        body_json,
        body_base64,
    )? {
        return Ok(Some(StandardSyncFinalizeNormalizedProduct::SuccessBody(
            body_json,
        )));
    }

    if let Some(product) =
        maybe_build_openai_chat_cross_format_sync_product_from_normalized_payload(
            report_kind,
            status_code,
            report_context,
            body_json,
            body_base64,
        )?
    {
        return Ok(Some(StandardSyncFinalizeNormalizedProduct::CrossFormat(
            product,
        )));
    }

    if let Some(product) = maybe_build_openai_cli_cross_format_sync_product_from_normalized_payload(
        report_kind,
        status_code,
        report_context,
        body_json,
        body_base64,
    )? {
        return Ok(Some(StandardSyncFinalizeNormalizedProduct::CrossFormat(
            product,
        )));
    }

    Ok(
        maybe_build_standard_cross_format_sync_product_from_normalized_payload(
            report_kind,
            status_code,
            report_context,
            body_json,
            body_base64,
        )?
        .map(StandardSyncFinalizeNormalizedProduct::CrossFormat),
    )
}

fn maybe_build_standard_same_format_sync_body(
    report_kind: &str,
    status_code: u16,
    report_context: Option<&Value>,
    body_json: Option<&Value>,
) -> Option<Value> {
    if status_code >= 400 {
        return None;
    }

    let report_context = report_context?;
    let expected_api_format = standard_same_format_api_format(report_kind)?;
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

    if provider_api_format != expected_api_format
        || client_api_format != expected_api_format
        || needs_conversion
    {
        return None;
    }

    let body_json = body_json?;
    if is_error_like_sync_body(body_json) {
        return None;
    }

    Some(body_json.clone())
}

fn maybe_build_standard_same_format_stream_sync_body(
    report_kind: &str,
    status_code: u16,
    report_context: Option<&Value>,
    body_base64: Option<&str>,
) -> Result<Option<Value>, PipelineFinalizeError> {
    if status_code >= 400 {
        return Ok(None);
    }

    let report_context = match report_context {
        Some(report_context) => report_context,
        None => return Ok(None),
    };
    let expected_api_format = match standard_same_format_api_format(report_kind) {
        Some(expected_api_format) => expected_api_format,
        None => return Ok(None),
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

    if provider_api_format != expected_api_format
        || client_api_format != expected_api_format
        || needs_conversion
    {
        return Ok(None);
    }

    let Some(body_base64) = body_base64 else {
        return Ok(None);
    };
    let body_bytes = base64::engine::general_purpose::STANDARD.decode(body_base64)?;
    Ok(aggregate_same_format_stream_sync_response(
        expected_api_format,
        &body_bytes,
    ))
}

fn maybe_build_openai_cli_same_family_sync_body(
    report_kind: &str,
    status_code: u16,
    report_context: Option<&Value>,
    body_json: Option<&Value>,
) -> Option<Value> {
    if status_code >= 400 || !is_openai_cli_finalize_kind(report_kind) {
        return None;
    }

    let report_context = report_context?;
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

    if !is_openai_cli_family_api_format(&provider_api_format)
        || !is_openai_cli_family_api_format(&client_api_format)
        || (provider_api_format == client_api_format && needs_conversion)
    {
        return None;
    }

    let body_json = body_json?;
    if is_error_like_sync_body(body_json) {
        return None;
    }

    Some(body_json.clone())
}

fn maybe_build_openai_cli_same_family_stream_sync_body(
    report_kind: &str,
    status_code: u16,
    report_context: Option<&Value>,
    body_base64: Option<&str>,
) -> Result<Option<Value>, PipelineFinalizeError> {
    if status_code >= 400 || !is_openai_cli_finalize_kind(report_kind) {
        return Ok(None);
    }

    let report_context = match report_context {
        Some(report_context) => report_context,
        None => return Ok(None),
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

    if !is_openai_cli_family_api_format(&provider_api_format)
        || !is_openai_cli_family_api_format(&client_api_format)
        || (provider_api_format == client_api_format && needs_conversion)
    {
        return Ok(None);
    }

    let Some(body_base64) = body_base64 else {
        return Ok(None);
    };
    let body_bytes = base64::engine::general_purpose::STANDARD.decode(body_base64)?;
    Ok(aggregate_openai_cli_stream_sync_response(&body_bytes))
}

fn maybe_build_openai_cross_format_provider_body_from_normalized_payload(
    body_json: Option<&Value>,
    body_base64: Option<&str>,
    provider_api_format: &str,
) -> Result<Option<Value>, PipelineFinalizeError> {
    let aggregated_stream_body = match body_base64 {
        Some(body_base64) => {
            let body_bytes = base64::engine::general_purpose::STANDARD.decode(body_base64)?;
            match provider_api_format.trim().to_ascii_lowercase().as_str() {
                "claude:chat" | "claude:cli" => aggregate_claude_stream_sync_response(&body_bytes),
                "gemini:chat" | "gemini:cli" => aggregate_gemini_stream_sync_response(&body_bytes),
                "openai:cli" | "openai:compact" => {
                    aggregate_openai_cli_stream_sync_response(&body_bytes)
                }
                _ => None,
            }
        }
        None => None,
    };

    Ok(aggregated_stream_body.or_else(|| body_json.cloned()))
}

fn is_error_like_sync_body(value: &Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };

    object.contains_key("error")
        || object
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(|value| value == "error")
        || object
            .get("chunks")
            .and_then(Value::as_array)
            .is_some_and(|chunks| {
                chunks.iter().any(|chunk| {
                    chunk.as_object().is_some_and(|chunk_object| {
                        chunk_object.contains_key("error")
                            || chunk_object
                                .get("type")
                                .and_then(Value::as_str)
                                .is_some_and(|value| value == "error")
                    })
                })
            })
}

pub fn maybe_build_standard_cross_format_sync_product(
    report_kind: &str,
    provider_api_format: &str,
    client_api_format: &str,
    report_context: &Value,
    provider_body_json: Value,
) -> Option<StandardCrossFormatSyncProduct> {
    let provider_api_format = provider_api_format.trim().to_ascii_lowercase();
    let client_api_format = client_api_format.trim().to_ascii_lowercase();

    let client_body_json = if is_standard_chat_finalize_kind(report_kind) {
        sync_chat_response_conversion_kind(&provider_api_format, &client_api_format)?;
        convert_standard_chat_response(
            &provider_body_json,
            &provider_api_format,
            &client_api_format,
            report_context,
        )?
    } else if is_standard_cli_finalize_kind(report_kind) {
        sync_cli_response_conversion_kind(&provider_api_format, &client_api_format)?;
        convert_standard_cli_response(
            &provider_body_json,
            &provider_api_format,
            &client_api_format,
            report_context,
        )?
    } else {
        return None;
    };

    Some(StandardCrossFormatSyncProduct {
        client_body_json,
        provider_body_json,
    })
}

pub fn aggregate_standard_chat_stream_sync_response(
    body: &[u8],
    provider_api_format: &str,
) -> Option<Value> {
    match provider_api_format.trim().to_ascii_lowercase().as_str() {
        "openai:chat" => aggregate_openai_chat_stream_sync_response(body),
        "openai:cli" | "openai:compact" => aggregate_openai_cli_stream_sync_response(body),
        "claude:chat" | "claude:cli" => aggregate_claude_stream_sync_response(body),
        "gemini:chat" | "gemini:cli" => aggregate_gemini_stream_sync_response(body),
        _ => None,
    }
}

pub fn convert_standard_chat_response(
    body_json: &Value,
    provider_api_format: &str,
    client_api_format: &str,
    report_context: &Value,
) -> Option<Value> {
    let canonical = match provider_api_format.trim().to_ascii_lowercase().as_str() {
        "openai:chat" => body_json.clone(),
        "openai:cli" | "openai:compact" => {
            convert_openai_cli_response_to_openai_chat(body_json, report_context)?
        }
        "claude:chat" | "claude:cli" => {
            convert_claude_chat_response_to_openai_chat(body_json, report_context)?
        }
        "gemini:chat" | "gemini:cli" => {
            convert_gemini_chat_response_to_openai_chat(body_json, report_context)?
        }
        _ => return None,
    };

    match client_api_format.trim().to_ascii_lowercase().as_str() {
        "openai:chat" => Some(canonical),
        "claude:chat" => convert_openai_chat_response_to_claude_chat(&canonical, report_context),
        "gemini:chat" => convert_openai_chat_response_to_gemini_chat(&canonical, report_context),
        _ => None,
    }
}

pub fn aggregate_standard_cli_stream_sync_response(
    body: &[u8],
    provider_api_format: &str,
) -> Option<Value> {
    aggregate_standard_chat_stream_sync_response(body, provider_api_format)
}

pub fn convert_standard_cli_response(
    body_json: &Value,
    provider_api_format: &str,
    client_api_format: &str,
    report_context: &Value,
) -> Option<Value> {
    let canonical = match provider_api_format.trim().to_ascii_lowercase().as_str() {
        "openai:cli" | "openai:compact" => {
            convert_openai_cli_response_to_openai_chat(body_json, report_context)?
        }
        _ => convert_standard_chat_response(
            body_json,
            provider_api_format,
            "openai:chat",
            report_context,
        )?,
    };

    match client_api_format.trim().to_ascii_lowercase().as_str() {
        "openai:cli" => {
            convert_openai_chat_response_to_openai_cli(&canonical, report_context, false)
        }
        "openai:compact" => {
            convert_openai_chat_response_to_openai_cli(&canonical, report_context, true)
        }
        "claude:cli" => convert_openai_chat_response_to_claude_chat(&canonical, report_context),
        "gemini:cli" => convert_openai_chat_response_to_gemini_chat(&canonical, report_context),
        _ => None,
    }
}

#[derive(Debug, Default)]
struct OpenAIChatChoiceState {
    role: Option<String>,
    content: String,
    finish_reason: Option<String>,
    tool_calls: BTreeMap<usize, OpenAIChatToolCallState>,
}

#[derive(Debug, Default)]
struct OpenAIChatToolCallState {
    id: Option<String>,
    tool_type: Option<String>,
    function_name: Option<String>,
    function_arguments: String,
}

#[derive(Debug, Default)]
struct ClaudeContentBlockState {
    object: Map<String, Value>,
    text: String,
    partial_json: String,
}

fn is_standard_chat_finalize_kind(report_kind: &str) -> bool {
    matches!(
        report_kind,
        "openai_chat_sync_finalize" | "claude_chat_sync_finalize" | "gemini_chat_sync_finalize"
    )
}

fn is_standard_cli_finalize_kind(report_kind: &str) -> bool {
    matches!(
        report_kind,
        "openai_cli_sync_finalize"
            | "openai_compact_sync_finalize"
            | "claude_cli_sync_finalize"
            | "gemini_cli_sync_finalize"
    )
}

fn is_openai_cli_finalize_kind(report_kind: &str) -> bool {
    matches!(
        report_kind,
        "openai_cli_sync_finalize" | "openai_compact_sync_finalize"
    )
}

fn standard_same_format_api_format(report_kind: &str) -> Option<&'static str> {
    match report_kind {
        "openai_chat_sync_finalize" => Some("openai:chat"),
        "claude_chat_sync_finalize" => Some("claude:chat"),
        "gemini_chat_sync_finalize" => Some("gemini:chat"),
        "claude_cli_sync_finalize" => Some("claude:cli"),
        "gemini_cli_sync_finalize" => Some("gemini:cli"),
        _ => None,
    }
}

fn aggregate_same_format_stream_sync_response(api_format: &str, body: &[u8]) -> Option<Value> {
    match api_format {
        "openai:chat" => aggregate_openai_chat_stream_sync_response(body),
        "claude:chat" | "claude:cli" => aggregate_claude_stream_sync_response(body),
        "gemini:chat" | "gemini:cli" => aggregate_gemini_stream_sync_response(body),
        _ => None,
    }
}

fn is_openai_cli_family_api_format(api_format: &str) -> bool {
    matches!(api_format, "openai:cli" | "openai:compact")
}

fn parse_stream_json_events(body: &[u8]) -> Option<Vec<Value>> {
    let text = std::str::from_utf8(body).ok()?;
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Some(Vec::new());
    }

    if trimmed.starts_with('[') {
        let array_value: Value = serde_json::from_str(trimmed).ok()?;
        let array = array_value.as_array()?;
        return Some(
            array
                .iter()
                .filter(|value| value.is_object())
                .cloned()
                .collect(),
        );
    }

    let mut events = Vec::new();
    let mut current_event_type: Option<String> = None;

    for raw_line in text.lines() {
        let line = raw_line.trim_matches('\r').trim();
        if line.is_empty() || line.starts_with(':') {
            continue;
        }
        if let Some(event_name) = line.strip_prefix("event:") {
            current_event_type = Some(event_name.trim().to_string());
            continue;
        }
        let data_line = if let Some(rest) = line.strip_prefix("data:") {
            rest.trim()
        } else {
            line
        };
        if data_line.is_empty() || data_line == "[DONE]" {
            continue;
        }

        let mut event: Value = serde_json::from_str(data_line).ok()?;
        if let Some(event_object) = event.as_object_mut() {
            if !event_object.contains_key("type") {
                if let Some(event_name) = current_event_type.take() {
                    event_object.insert("type".to_string(), Value::String(event_name));
                }
            }
        }
        events.push(event);
        current_event_type = None;
    }

    Some(events)
}

pub fn aggregate_openai_chat_stream_sync_response(body: &[u8]) -> Option<Value> {
    let text = std::str::from_utf8(body).ok()?;
    let mut response_id: Option<String> = None;
    let mut model: Option<String> = None;
    let mut created: Option<u64> = None;
    let mut usage: Option<Value> = None;
    let mut choices: BTreeMap<usize, OpenAIChatChoiceState> = BTreeMap::new();
    let mut saw_chunk = false;

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

        let chunk: Value = serde_json::from_str(data_line).ok()?;
        let chunk_object = chunk.as_object()?;
        saw_chunk = true;

        if response_id.is_none() {
            response_id = chunk_object
                .get("id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
        }
        if model.is_none() {
            model = chunk_object
                .get("model")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
        }
        if created.is_none() {
            created = chunk_object.get("created").and_then(Value::as_u64);
        }
        if let Some(u) = chunk_object.get("usage") {
            usage = Some(u.clone());
        }

        let Some(chunk_choices) = chunk_object.get("choices").and_then(Value::as_array) else {
            continue;
        };
        for chunk_choice in chunk_choices {
            let Some(choice_object) = chunk_choice.as_object() else {
                continue;
            };
            let Some(index) = choice_object
                .get("index")
                .and_then(Value::as_u64)
                .map(|value| value as usize)
            else {
                continue;
            };
            let state = choices.entry(index).or_default();
            if let Some(finish_reason) = choice_object.get("finish_reason").and_then(Value::as_str)
            {
                state.finish_reason = Some(finish_reason.to_string());
            }

            let Some(delta) = choice_object.get("delta").and_then(Value::as_object) else {
                continue;
            };
            if let Some(role) = delta.get("role").and_then(Value::as_str) {
                state.role = Some(role.to_string());
            }
            if let Some(content) = delta.get("content").and_then(Value::as_str) {
                state.content.push_str(content);
            }
            if let Some(tool_calls) = delta.get("tool_calls").and_then(Value::as_array) {
                for tool_call in tool_calls {
                    let Some(tool_call_object) = tool_call.as_object() else {
                        continue;
                    };
                    let tool_index = tool_call_object
                        .get("index")
                        .and_then(Value::as_u64)
                        .map(|value| value as usize)
                        .unwrap_or(0);
                    let tool_state = state.tool_calls.entry(tool_index).or_default();
                    if let Some(id) = tool_call_object.get("id").and_then(Value::as_str) {
                        tool_state.id = Some(id.to_string());
                    }
                    if let Some(tool_type) = tool_call_object.get("type").and_then(Value::as_str) {
                        tool_state.tool_type = Some(tool_type.to_string());
                    }
                    if let Some(function) =
                        tool_call_object.get("function").and_then(Value::as_object)
                    {
                        if let Some(name) = function.get("name").and_then(Value::as_str) {
                            tool_state.function_name = Some(name.to_string());
                        }
                        if let Some(arguments) = function.get("arguments").and_then(Value::as_str) {
                            tool_state.function_arguments.push_str(arguments);
                        }
                    }
                }
            }
        }
    }

    if !saw_chunk {
        return None;
    }

    let mut response_object = Map::new();
    response_object.insert(
        "id".to_string(),
        Value::String(response_id.unwrap_or_else(|| "chatcmpl-local-finalize".to_string())),
    );
    response_object.insert(
        "object".to_string(),
        Value::String("chat.completion".to_string()),
    );
    if let Some(created) = created {
        response_object.insert("created".to_string(), Value::Number(created.into()));
    }
    if let Some(model) = model {
        response_object.insert("model".to_string(), Value::String(model));
    }

    let mut response_choices = Vec::with_capacity(choices.len());
    for (index, state) in choices {
        let mut message = Map::new();
        message.insert(
            "role".to_string(),
            Value::String(state.role.unwrap_or_else(|| "assistant".to_string())),
        );
        if state.tool_calls.is_empty() {
            message.insert("content".to_string(), Value::String(state.content));
        } else {
            if state.content.is_empty() {
                message.insert("content".to_string(), Value::Null);
            } else {
                message.insert("content".to_string(), Value::String(state.content));
            }
            let tool_calls = state
                .tool_calls
                .into_iter()
                .map(|(tool_index, tool_state)| {
                    json!({
                        "index": tool_index,
                        "id": tool_state.id,
                        "type": tool_state.tool_type.unwrap_or_else(|| "function".to_string()),
                        "function": {
                            "name": tool_state.function_name,
                            "arguments": tool_state.function_arguments,
                        },
                    })
                })
                .collect::<Vec<_>>();
            message.insert("tool_calls".to_string(), Value::Array(tool_calls));
        }

        response_choices.push(json!({
            "index": index,
            "message": Value::Object(message),
            "finish_reason": state.finish_reason,
        }));
    }
    response_object.insert("choices".to_string(), Value::Array(response_choices));
    if let Some(usage) = usage {
        response_object.insert("usage".to_string(), usage);
    }

    Some(Value::Object(response_object))
}

pub fn aggregate_openai_cli_stream_sync_response(body: &[u8]) -> Option<Value> {
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

pub fn aggregate_claude_stream_sync_response(body: &[u8]) -> Option<Value> {
    let events = parse_stream_json_events(body)?;
    if events.is_empty() {
        return None;
    }

    let mut message_object: Option<Map<String, Value>> = None;
    let mut content_blocks: BTreeMap<usize, ClaudeContentBlockState> = BTreeMap::new();
    let mut usage: Option<Value> = None;
    let mut saw_message_start = false;

    for event in events {
        let event_object = event.as_object()?;
        let event_type = event_object
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();

        match event_type {
            "message_start" => {
                let mut message = event_object.get("message")?.as_object()?.clone();
                usage = message.remove("usage");
                message_object = Some(message);
                saw_message_start = true;
            }
            "content_block_start" => {
                let index = event_object
                    .get("index")
                    .and_then(Value::as_u64)
                    .map(|value| value as usize)
                    .unwrap_or(0);
                let object = event_object
                    .get("content_block")
                    .and_then(Value::as_object)
                    .cloned()
                    .unwrap_or_default();
                content_blocks.insert(
                    index,
                    ClaudeContentBlockState {
                        object,
                        ..Default::default()
                    },
                );
            }
            "content_block_delta" => {
                let index = event_object
                    .get("index")
                    .and_then(Value::as_u64)
                    .map(|value| value as usize)
                    .unwrap_or(0);
                let state = content_blocks.entry(index).or_default();
                let Some(delta) = event_object.get("delta").and_then(Value::as_object) else {
                    continue;
                };
                match delta
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                {
                    "text_delta" => {
                        if let Some(text) = delta.get("text").and_then(Value::as_str) {
                            state.text.push_str(text);
                        }
                    }
                    "input_json_delta" => {
                        if let Some(partial_json) =
                            delta.get("partial_json").and_then(Value::as_str)
                        {
                            state.partial_json.push_str(partial_json);
                        }
                    }
                    _ => {}
                }
            }
            "message_delta" => {
                if let Some(message) = message_object.as_mut() {
                    if let Some(delta) = event_object.get("delta").and_then(Value::as_object) {
                        if let Some(stop_reason) = delta.get("stop_reason") {
                            message.insert("stop_reason".to_string(), stop_reason.clone());
                        }
                        if let Some(stop_sequence) = delta.get("stop_sequence") {
                            message.insert("stop_sequence".to_string(), stop_sequence.clone());
                        }
                    }
                }
                if let Some(delta_usage) = event_object.get("usage") {
                    usage = Some(delta_usage.clone());
                }
            }
            "message_stop" => {}
            _ => {}
        }
    }

    if !saw_message_start {
        return None;
    }

    let mut message = message_object?;
    let mut content = Vec::with_capacity(content_blocks.len());
    for (_index, state) in content_blocks {
        let mut block = state.object;
        let block_type = block
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("text")
            .to_string();
        match block_type.as_str() {
            "text" => {
                block.insert(
                    "text".to_string(),
                    Value::String(if state.text.is_empty() {
                        block
                            .get("text")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string()
                    } else {
                        state.text
                    }),
                );
            }
            "tool_use" => {
                if !state.partial_json.is_empty() {
                    let input = serde_json::from_str::<Value>(&state.partial_json)
                        .unwrap_or(Value::String(state.partial_json));
                    block.insert("input".to_string(), input);
                }
            }
            _ => {
                if !state.text.is_empty() {
                    block.insert("text".to_string(), Value::String(state.text));
                }
            }
        }
        content.push(Value::Object(block));
    }
    message.insert("content".to_string(), Value::Array(content));
    if let Some(usage_value) = usage {
        message.insert("usage".to_string(), usage_value);
    }

    Some(Value::Object(message))
}

pub fn aggregate_gemini_stream_sync_response(body: &[u8]) -> Option<Value> {
    let events = parse_stream_json_events(body)?;
    if events.is_empty() {
        return None;
    }

    let mut candidates: BTreeMap<usize, Value> = BTreeMap::new();
    let mut response_id: Option<Value> = None;
    let mut private_response_id: Option<Value> = None;
    let mut model_version: Option<Value> = None;
    let mut usage_metadata: Option<Value> = None;
    let mut prompt_feedback: Option<Value> = None;
    let mut saw_candidate = false;

    for event in events {
        let raw_event_object = event.as_object()?;
        if let Some(id) = raw_event_object.get("responseId") {
            response_id = Some(id.clone());
        }
        if let Some(id) = raw_event_object.get("_v1internal_response_id") {
            private_response_id = Some(id.clone());
        }
        let event_object = if let Some(response) = raw_event_object
            .get("response")
            .and_then(Value::as_object)
            .filter(|response| response.contains_key("candidates"))
        {
            response
        } else {
            raw_event_object
        };
        if let Some(id) = event_object.get("responseId") {
            response_id = Some(id.clone());
        }
        if let Some(id) = event_object.get("_v1internal_response_id") {
            private_response_id = Some(id.clone());
        }
        if let Some(version) = event_object.get("modelVersion") {
            model_version = Some(version.clone());
        }
        if let Some(usage) = event_object.get("usageMetadata") {
            usage_metadata = Some(usage.clone());
        }
        if let Some(prompt) = event_object.get("promptFeedback") {
            prompt_feedback = Some(prompt.clone());
        }
        let Some(event_candidates) = event_object.get("candidates").and_then(Value::as_array)
        else {
            continue;
        };
        for candidate in event_candidates {
            let Some(candidate_object) = candidate.as_object() else {
                continue;
            };
            let index = candidate_object
                .get("index")
                .and_then(Value::as_u64)
                .map(|value| value as usize)
                .unwrap_or(0);
            candidates.insert(index, Value::Object(candidate_object.clone()));
            saw_candidate = true;
        }
    }

    if !saw_candidate {
        return None;
    }

    let mut response = Map::new();
    if let Some(response_id) = response_id {
        response.insert("responseId".to_string(), response_id);
    }
    if let Some(private_response_id) = private_response_id {
        response.insert("_v1internal_response_id".to_string(), private_response_id);
    }
    response.insert(
        "candidates".to_string(),
        Value::Array(candidates.into_values().collect()),
    );
    if let Some(version) = model_version {
        response.insert("modelVersion".to_string(), version);
    }
    if let Some(usage) = usage_metadata {
        response.insert("usageMetadata".to_string(), usage);
    }
    if let Some(prompt) = prompt_feedback {
        response.insert("promptFeedback".to_string(), prompt);
    }
    Some(Value::Object(response))
}

#[cfg(test)]
mod tests {
    use super::{
        maybe_build_openai_chat_cross_format_sync_product_from_normalized_payload,
        maybe_build_openai_cli_cross_format_sync_product_from_normalized_payload,
        maybe_build_openai_cli_same_family_sync_body_from_normalized_payload,
        maybe_build_standard_cross_format_sync_product_from_normalized_payload,
        maybe_build_standard_same_format_sync_body_from_normalized_payload,
        maybe_build_standard_sync_finalize_product_from_normalized_payload,
        StandardSyncFinalizeNormalizedProduct,
    };
    use base64::Engine as _;
    use serde_json::json;

    #[test]
    fn builds_standard_cross_format_sync_product_from_normalized_stream_payload() {
        let body = concat!(
            "data: {\"id\":\"chatcmpl_123\",\"object\":\"chat.completion.chunk\",\"created\":1,",
            "\"model\":\"gpt-5\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Hel\"},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"chatcmpl_123\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-5\",",
            "\"choices\":[{\"index\":0,\"delta\":{\"content\":\"lo\"},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"chatcmpl_123\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-5\",",
            "\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],",
            "\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2,\"total_tokens\":3}}\n\n",
            "data: [DONE]\n\n",
        );
        let report_context = json!({
            "provider_api_format": "openai:chat",
            "client_api_format": "claude:chat",
        });

        let product = maybe_build_standard_cross_format_sync_product_from_normalized_payload(
            "openai_chat_sync_finalize",
            200,
            Some(&report_context),
            None,
            Some(&base64::engine::general_purpose::STANDARD.encode(body)),
        )
        .expect("product build should succeed")
        .expect("product should exist");

        assert_eq!(
            product.provider_body_json,
            json!({
                "id": "chatcmpl_123",
                "object": "chat.completion",
                "created": 1,
                "model": "gpt-5",
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": "Hello",
                    },
                    "finish_reason": "stop",
                }],
                "usage": {
                    "prompt_tokens": 1,
                    "completion_tokens": 2,
                    "total_tokens": 3,
                },
            })
        );
        assert_eq!(
            product.client_body_json.get("type"),
            Some(&json!("message"))
        );
        assert_eq!(
            product.client_body_json.get("id"),
            Some(&json!("chatcmpl_123"))
        );
        assert_eq!(
            product.client_body_json.get("content"),
            Some(&json!([{ "type": "text", "text": "Hello" }]))
        );
        assert_eq!(
            product.client_body_json.get("stop_reason"),
            Some(&json!("end_turn"))
        );
    }

    #[test]
    fn falls_back_to_body_json_when_stream_aggregation_returns_none() {
        let report_context = json!({
            "provider_api_format": "openai:chat",
            "client_api_format": "claude:chat",
        });
        let provider_body_json = json!({
            "id": "chatcmpl_123",
            "object": "chat.completion",
            "created": 1,
            "model": "gpt-5",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Hello",
                },
                "finish_reason": "stop",
            }],
            "usage": {
                "prompt_tokens": 1,
                "completion_tokens": 2,
                "total_tokens": 3,
            },
        });

        let product = maybe_build_standard_cross_format_sync_product_from_normalized_payload(
            "openai_chat_sync_finalize",
            200,
            Some(&report_context),
            Some(&provider_body_json),
            Some(&base64::engine::general_purpose::STANDARD.encode("not an sse stream")),
        )
        .expect("fallback build should succeed");

        assert_eq!(
            product.expect("product should exist").provider_body_json,
            provider_body_json
        );
    }

    #[test]
    fn builds_standard_same_format_body_from_stream_payload() {
        let body = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-3-5-sonnet-latest\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null}}\n\n",
            "event: content_block_start\n",
            "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
            "event: content_block_delta\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\n",
            "event: content_block_stop\n",
            "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
            "event: message_delta\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"input_tokens\":5,\"output_tokens\":7}}\n\n",
            "event: message_stop\n",
            "data: {\"type\":\"message_stop\"}\n\n",
        );
        let report_context = json!({
            "provider_api_format": "claude:chat",
            "client_api_format": "claude:chat",
            "needs_conversion": false,
        });

        let body_json = maybe_build_standard_same_format_sync_body_from_normalized_payload(
            "claude_chat_sync_finalize",
            200,
            Some(&report_context),
            None,
            Some(&base64::engine::general_purpose::STANDARD.encode(body)),
        )
        .expect("same-format builder should succeed")
        .expect("body should exist");

        assert_eq!(body_json.get("type"), Some(&json!("message")));
        assert_eq!(body_json.get("role"), Some(&json!("assistant")));
        assert_eq!(
            body_json.get("content"),
            Some(&json!([{ "type": "text", "text": "hello" }]))
        );
    }

    #[test]
    fn falls_back_to_body_json_for_standard_same_format_sync_payload() {
        let report_context = json!({
            "provider_api_format": "gemini:cli",
            "client_api_format": "gemini:cli",
            "needs_conversion": false,
        });
        let provider_body_json = json!({
            "candidates": [{
                "index": 0,
                "content": {
                    "parts": [{ "text": "hello" }],
                    "role": "model"
                },
                "finishReason": "STOP"
            }]
        });

        let body_json = maybe_build_standard_same_format_sync_body_from_normalized_payload(
            "gemini_cli_sync_finalize",
            200,
            Some(&report_context),
            Some(&provider_body_json),
            None,
        )
        .expect("same-format sync body should succeed")
        .expect("body should exist");

        assert_eq!(body_json, provider_body_json);
    }

    #[test]
    fn rejects_standard_same_format_when_needs_conversion_is_true() {
        let report_context = json!({
            "provider_api_format": "openai:chat",
            "client_api_format": "openai:chat",
            "needs_conversion": true,
        });
        let provider_body_json = json!({ "id": "chatcmpl_123" });

        let body_json = maybe_build_standard_same_format_sync_body_from_normalized_payload(
            "openai_chat_sync_finalize",
            200,
            Some(&report_context),
            Some(&provider_body_json),
            None,
        )
        .expect("same-format guard should not error");

        assert!(body_json.is_none());
    }

    #[test]
    fn rejects_standard_same_format_error_body_json() {
        let report_context = json!({
            "provider_api_format": "claude:chat",
            "client_api_format": "claude:chat",
            "needs_conversion": false,
        });
        let provider_body_json = json!({
            "type": "error",
            "error": {
                "type": "rate_limit_error",
                "message": "slow down"
            }
        });

        let body_json = maybe_build_standard_same_format_sync_body_from_normalized_payload(
            "claude_chat_sync_finalize",
            200,
            Some(&report_context),
            Some(&provider_body_json),
            None,
        )
        .expect("same-format error guard should not error");

        assert!(body_json.is_none());
    }

    #[test]
    fn builds_openai_cli_same_family_body_from_stream_payload() {
        let body = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_123\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"in_progress\",\"output\":[]}}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_123\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":2,\"total_tokens\":3}}}\n\n",
        );
        let report_context = json!({
            "provider_api_format": "openai:compact",
            "client_api_format": "openai:compact",
            "needs_conversion": false,
        });

        let body_json = maybe_build_openai_cli_same_family_sync_body_from_normalized_payload(
            "openai_compact_sync_finalize",
            200,
            Some(&report_context),
            None,
            Some(&base64::engine::general_purpose::STANDARD.encode(body)),
        )
        .expect("openai-cli family stream should succeed")
        .expect("body should exist");

        assert_eq!(body_json.get("id"), Some(&json!("resp_123")));
        assert_eq!(body_json.get("status"), Some(&json!("completed")));
    }

    #[test]
    fn rejects_openai_cli_same_family_exact_same_stream_when_needs_conversion_is_true() {
        let body = concat!(
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_123\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"completed\",\"output\":[]}}\n\n",
        );
        let report_context = json!({
            "provider_api_format": "openai:cli",
            "client_api_format": "openai:cli",
            "needs_conversion": true,
        });

        let body_json = maybe_build_openai_cli_same_family_sync_body_from_normalized_payload(
            "openai_cli_sync_finalize",
            200,
            Some(&report_context),
            None,
            Some(&base64::engine::general_purpose::STANDARD.encode(body)),
        )
        .expect("openai-cli same-family guard should not error");

        assert!(body_json.is_none());
    }

    #[test]
    fn falls_back_to_body_json_for_openai_cli_same_family_sync_payload() {
        let report_context = json!({
            "provider_api_format": "openai:compact",
            "client_api_format": "openai:compact",
            "needs_conversion": false,
        });
        let provider_body_json = json!({
            "id": "resp_123",
            "object": "response",
            "status": "completed",
            "output": []
        });

        let body_json = maybe_build_openai_cli_same_family_sync_body_from_normalized_payload(
            "openai_compact_sync_finalize",
            200,
            Some(&report_context),
            Some(&provider_body_json),
            None,
        )
        .expect("openai-cli same-family sync should succeed")
        .expect("body should exist");

        assert_eq!(body_json, provider_body_json);
    }

    #[test]
    fn allows_openai_cli_same_family_cross_format_sync_when_conversion_is_flagged() {
        let report_context = json!({
            "provider_api_format": "openai:compact",
            "client_api_format": "openai:cli",
            "needs_conversion": true,
        });
        let provider_body_json = json!({
            "id": "resp_family_123",
            "object": "response",
            "status": "completed",
            "output": []
        });

        let body_json = maybe_build_openai_cli_same_family_sync_body_from_normalized_payload(
            "openai_cli_sync_finalize",
            200,
            Some(&report_context),
            Some(&provider_body_json),
            None,
        )
        .expect("openai-cli cross-family sync should succeed")
        .expect("body should exist");

        assert_eq!(body_json, provider_body_json);
    }

    #[test]
    fn rejects_openai_cli_same_family_error_body_json() {
        let report_context = json!({
            "provider_api_format": "openai:cli",
            "client_api_format": "openai:cli",
            "needs_conversion": false,
        });
        let provider_body_json = json!({
            "error": {
                "message": "quota reached",
                "type": "rate_limit_error"
            }
        });

        let body_json = maybe_build_openai_cli_same_family_sync_body_from_normalized_payload(
            "openai_cli_sync_finalize",
            200,
            Some(&report_context),
            Some(&provider_body_json),
            None,
        )
        .expect("openai-cli same-family error guard should not error");

        assert!(body_json.is_none());
    }

    #[test]
    fn builds_openai_chat_cross_format_sync_product_from_claude_body_json() {
        let report_context = json!({
            "provider_api_format": "claude:chat",
            "client_api_format": "openai:chat",
            "model": "gpt-5",
            "mapped_model": "claude-sonnet-4",
        });
        let provider_body_json = json!({
            "id": "msg_claude_direct_123",
            "type": "message",
            "model": "claude-sonnet-4",
            "role": "assistant",
            "content": [{"type": "text", "text": "Hello Claude"}],
            "stop_reason": "end_turn",
            "usage": {
                "input_tokens": 2,
                "output_tokens": 3
            }
        });

        let product = maybe_build_openai_chat_cross_format_sync_product_from_normalized_payload(
            "openai_chat_sync_finalize",
            200,
            Some(&report_context),
            Some(&provider_body_json),
            None,
        )
        .expect("openai-chat cross-format should succeed")
        .expect("product should exist");

        assert_eq!(product.provider_body_json, provider_body_json);
        assert_eq!(
            product.client_body_json["choices"][0]["message"]["content"],
            "Hello Claude"
        );
    }

    #[test]
    fn builds_openai_chat_cross_format_sync_product_from_gemini_body_json() {
        let report_context = json!({
            "provider_api_format": "gemini:chat",
            "client_api_format": "openai:chat",
            "model": "gpt-5",
            "mapped_model": "gemini-2.5-pro",
        });
        let provider_body_json = json!({
            "responseId": "resp_gemini_direct_123",
            "candidates": [{
                "content": {
                    "parts": [{"text": "Hello Gemini"}],
                    "role": "model"
                },
                "finishReason": "STOP",
                "index": 0
            }],
            "modelVersion": "gemini-2.5-pro-upstream",
            "usageMetadata": {
                "promptTokenCount": 1,
                "candidatesTokenCount": 2,
                "totalTokenCount": 3
            }
        });

        let product = maybe_build_openai_chat_cross_format_sync_product_from_normalized_payload(
            "openai_chat_sync_finalize",
            200,
            Some(&report_context),
            Some(&provider_body_json),
            None,
        )
        .expect("openai-chat cross-format should succeed")
        .expect("product should exist");

        assert_eq!(product.provider_body_json, provider_body_json);
        assert_eq!(
            product.client_body_json["choices"][0]["message"]["content"],
            "Hello Gemini"
        );
        assert_eq!(product.client_body_json["usage"]["completion_tokens"], 2);
    }

    #[test]
    fn builds_openai_chat_cross_format_sync_product_from_openai_cli_body_json() {
        let report_context = json!({
            "provider_api_format": "openai:cli",
            "client_api_format": "openai:chat",
            "model": "gpt-5.4",
            "mapped_model": "gpt-5.4",
        });
        let provider_body_json = json!({
            "id": "resp_cli_direct_123",
            "object": "response",
            "status": "completed",
            "model": "gpt-5.4",
            "output": [{
                "type": "message",
                "id": "msg_cli_direct_123",
                "role": "assistant",
                "status": "completed",
                "content": [{
                    "type": "output_text",
                    "text": "Hello CLI",
                    "annotations": []
                }]
            }]
        });

        let product = maybe_build_openai_chat_cross_format_sync_product_from_normalized_payload(
            "openai_chat_sync_finalize",
            200,
            Some(&report_context),
            Some(&provider_body_json),
            None,
        )
        .expect("openai-chat cross-format should succeed")
        .expect("product should exist");

        assert_eq!(product.provider_body_json, provider_body_json);
        assert_eq!(
            product.client_body_json["choices"][0]["message"]["content"],
            "Hello CLI"
        );
    }

    #[test]
    fn builds_openai_cli_cross_format_sync_product_from_gemini_stream_payload() {
        let body = concat!(
            "data: {\"responseId\":\"resp_cli_stream_123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello \"}],\"role\":\"model\"},\"index\":0}],\"modelVersion\":\"gemini-2.5-pro-upstream\"}\n\n",
            "data: {\"responseId\":\"resp_cli_stream_123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello Gemini CLI\"}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"modelVersion\":\"gemini-2.5-pro-upstream\",\"usageMetadata\":{\"promptTokenCount\":2,\"candidatesTokenCount\":3,\"totalTokenCount\":5}}\n\n",
        );
        let report_context = json!({
            "provider_api_format": "gemini:cli",
            "client_api_format": "openai:cli",
            "model": "gpt-5",
            "mapped_model": "gemini-2.5-pro-upstream",
        });

        let product = maybe_build_openai_cli_cross_format_sync_product_from_normalized_payload(
            "openai_cli_sync_finalize",
            200,
            Some(&report_context),
            None,
            Some(&base64::engine::general_purpose::STANDARD.encode(body)),
        )
        .expect("openai-cli cross-format should succeed")
        .expect("product should exist");

        assert_eq!(
            product.provider_body_json["responseId"],
            "resp_cli_stream_123"
        );
        assert_eq!(product.client_body_json["object"], "response");
        assert_eq!(
            product.client_body_json["output"][0]["content"][0]["text"],
            "Hello Gemini CLI"
        );
    }

    #[test]
    fn rejects_openai_chat_cross_format_for_unsupported_matrix() {
        let report_context = json!({
            "provider_api_format": "openai:chat",
            "client_api_format": "openai:chat",
        });
        let provider_body_json = json!({ "id": "chatcmpl_123" });

        let product = maybe_build_openai_chat_cross_format_sync_product_from_normalized_payload(
            "openai_chat_sync_finalize",
            200,
            Some(&report_context),
            Some(&provider_body_json),
            None,
        )
        .expect("unsupported matrix should not error");

        assert!(product.is_none());
    }

    #[test]
    fn standard_sync_finalize_product_prefers_same_format_success_body() {
        let report_context = json!({
            "provider_api_format": "openai:chat",
            "client_api_format": "openai:chat",
            "needs_conversion": false,
        });
        let provider_body_json = json!({
            "id": "chatcmpl_123",
            "object": "chat.completion"
        });

        let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
            "openai_chat_sync_finalize",
            200,
            Some(&report_context),
            Some(&provider_body_json),
            None,
        )
        .expect("dispatch should succeed");

        assert_eq!(
            product,
            Some(StandardSyncFinalizeNormalizedProduct::SuccessBody(
                provider_body_json
            ))
        );
    }

    #[test]
    fn standard_sync_finalize_product_handles_openai_cli_same_family_body() {
        let report_context = json!({
            "provider_api_format": "openai:compact",
            "client_api_format": "openai:compact",
            "needs_conversion": false,
        });
        let provider_body_json = json!({
            "id": "resp_123",
            "object": "response",
            "status": "completed",
            "output": []
        });

        let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
            "openai_compact_sync_finalize",
            200,
            Some(&report_context),
            Some(&provider_body_json),
            None,
        )
        .expect("dispatch should succeed");

        assert_eq!(
            product,
            Some(StandardSyncFinalizeNormalizedProduct::SuccessBody(
                provider_body_json
            ))
        );
    }

    #[test]
    fn standard_sync_finalize_product_handles_openai_cli_same_family_cross_format_body() {
        let report_context = json!({
            "provider_api_format": "openai:compact",
            "client_api_format": "openai:cli",
            "needs_conversion": true,
        });
        let provider_body_json = json!({
            "id": "resp_family_123",
            "object": "response",
            "status": "completed",
            "output": []
        });

        let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
            "openai_cli_sync_finalize",
            200,
            Some(&report_context),
            Some(&provider_body_json),
            None,
        )
        .expect("dispatch should succeed");

        assert_eq!(
            product,
            Some(StandardSyncFinalizeNormalizedProduct::SuccessBody(
                provider_body_json
            ))
        );
    }

    #[test]
    fn standard_sync_finalize_product_handles_openai_chat_cross_format() {
        let report_context = json!({
            "provider_api_format": "claude:chat",
            "client_api_format": "openai:chat",
            "model": "gpt-5",
            "mapped_model": "claude-sonnet-4",
        });
        let provider_body_json = json!({
            "id": "msg_claude_direct_123",
            "type": "message",
            "model": "claude-sonnet-4",
            "role": "assistant",
            "content": [{"type": "text", "text": "Hello Claude"}],
            "stop_reason": "end_turn"
        });

        let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
            "openai_chat_sync_finalize",
            200,
            Some(&report_context),
            Some(&provider_body_json),
            None,
        )
        .expect("dispatch should succeed")
        .expect("dispatch should produce a product");

        assert!(matches!(
            product,
            StandardSyncFinalizeNormalizedProduct::CrossFormat(_)
        ));
    }

    #[test]
    fn standard_sync_finalize_product_handles_openai_cli_cross_format() {
        let body = concat!(
            "data: {\"responseId\":\"resp_cli_stream_123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello \"}],\"role\":\"model\"},\"index\":0}],\"modelVersion\":\"gemini-2.5-pro-upstream\"}\n\n",
            "data: {\"responseId\":\"resp_cli_stream_123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello Gemini CLI\"}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"modelVersion\":\"gemini-2.5-pro-upstream\",\"usageMetadata\":{\"promptTokenCount\":2,\"candidatesTokenCount\":3,\"totalTokenCount\":5}}\n\n",
        );
        let report_context = json!({
            "provider_api_format": "gemini:cli",
            "client_api_format": "openai:cli",
            "model": "gpt-5",
            "mapped_model": "gemini-2.5-pro-upstream",
        });

        let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
            "openai_cli_sync_finalize",
            200,
            Some(&report_context),
            None,
            Some(&base64::engine::general_purpose::STANDARD.encode(body)),
        )
        .expect("dispatch should succeed")
        .expect("dispatch should produce a product");

        assert!(matches!(
            product,
            StandardSyncFinalizeNormalizedProduct::CrossFormat(_)
        ));
    }

    #[test]
    fn standard_sync_finalize_product_falls_back_to_generic_standard_cross_format() {
        let report_context = json!({
            "provider_api_format": "openai:chat",
            "client_api_format": "claude:chat",
        });
        let provider_body_json = json!({
            "id": "chatcmpl_123",
            "object": "chat.completion",
            "created": 1,
            "model": "gpt-5",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Hello",
                },
                "finish_reason": "stop",
            }],
        });

        let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
            "claude_chat_sync_finalize",
            200,
            Some(&report_context),
            Some(&provider_body_json),
            None,
        )
        .expect("dispatch should succeed")
        .expect("dispatch should produce a product");

        assert!(matches!(
            product,
            StandardSyncFinalizeNormalizedProduct::CrossFormat(_)
        ));
    }
}
