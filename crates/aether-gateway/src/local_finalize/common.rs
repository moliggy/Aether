use super::*;

pub(crate) struct LocalCoreSyncFinalizeOutcome {
    pub(crate) response: Response<Body>,
    pub(crate) background_report: Option<GatewaySyncReportRequest>,
}

pub(super) fn local_finalize_allows_envelope(report_context: &Value) -> bool {
    let has_envelope = report_context
        .get("has_envelope")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if !has_envelope {
        return true;
    }
    matches!(
        report_context.get("envelope_name").and_then(Value::as_str),
        Some("gemini_cli:v1internal") | Some("antigravity:v1internal") | Some("claude:cli")
    )
}

fn local_finalize_response_model(report_context: &Value) -> &str {
    report_context
        .get("mapped_model")
        .and_then(Value::as_str)
        .or_else(|| report_context.get("model").and_then(Value::as_str))
        .unwrap_or_default()
}

fn inject_antigravity_claude_tool_ids_response(response: &mut Value, model: &str) {
    if !model.to_ascii_lowercase().contains("claude") {
        return;
    }

    let Some(candidates) = response.get_mut("candidates").and_then(Value::as_array_mut) else {
        return;
    };

    for candidate in candidates {
        let Some(parts) = candidate
            .get_mut("content")
            .and_then(Value::as_object_mut)
            .and_then(|content| content.get_mut("parts"))
            .and_then(Value::as_array_mut)
        else {
            continue;
        };

        let mut name_counters: BTreeMap<String, usize> = BTreeMap::new();
        for part in parts {
            let function_call = if let Some(function_call) =
                part.get_mut("functionCall").and_then(Value::as_object_mut)
            {
                function_call
            } else if let Some(function_call) =
                part.get_mut("function_call").and_then(Value::as_object_mut)
            {
                function_call
            } else {
                continue;
            };
            let has_id = function_call
                .get("id")
                .and_then(Value::as_str)
                .is_some_and(|value| !value.is_empty());
            if has_id {
                continue;
            }
            let function_name = function_call
                .get("name")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .unwrap_or("unknown")
                .to_string();
            let count = name_counters.entry(function_name.clone()).or_insert(0);
            function_call.insert(
                "id".to_string(),
                Value::String(format!("call_{function_name}_{count}")),
            );
            *count += 1;
        }
    }
}

fn postprocess_local_finalize_response_value(data: &mut Value, report_context: &Value) {
    if !matches!(
        report_context.get("envelope_name").and_then(Value::as_str),
        Some("antigravity:v1internal")
    ) {
        return;
    }
    if let Some(object) = data.as_object_mut() {
        if !object.contains_key("_v1internal_response_id") {
            if let Some(response_id) = object.remove("responseId") {
                object.insert("_v1internal_response_id".to_string(), response_id);
            }
        }
    }
    inject_antigravity_claude_tool_ids_response(
        data,
        local_finalize_response_model(report_context),
    );
}

pub(crate) fn unwrap_local_finalize_response_value(
    data: Value,
    report_context: &Value,
) -> Result<Option<Value>, GatewayError> {
    if !report_context
        .get("has_envelope")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Ok(Some(data));
    }
    let mut unwrapped = match report_context.get("envelope_name").and_then(Value::as_str) {
        Some("claude:cli") => data,
        Some("gemini_cli:v1internal") => {
            if let Some(response) = data
                .get("response")
                .and_then(Value::as_object)
                .filter(|response| !response.contains_key("response"))
            {
                Value::Object(response.clone())
            } else {
                data
            }
        }
        Some("antigravity:v1internal") => {
            if let Some(response) = data
                .get("response")
                .and_then(Value::as_object)
                .filter(|response| !response.contains_key("response"))
            {
                let mut unwrapped = response.clone();
                if let Some(response_id) = data.get("responseId").cloned() {
                    unwrapped.insert("_v1internal_response_id".to_string(), response_id);
                }
                Value::Object(unwrapped)
            } else {
                data
            }
        }
        _ => return Ok(None),
    };
    postprocess_local_finalize_response_value(&mut unwrapped, report_context);
    Ok(Some(unwrapped))
}

pub(super) fn build_local_success_outcome(
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &GatewaySyncReportRequest,
    body_json: Value,
) -> Result<LocalCoreSyncFinalizeOutcome, GatewayError> {
    let headers = payload.headers.clone();
    let background_report =
        map_local_finalize_to_success_report(payload, body_json.clone(), headers.clone());
    build_local_success_outcome_with_report(
        trace_id,
        decision,
        payload.status_code,
        body_json,
        headers,
        background_report,
    )
}

pub(super) fn build_local_success_outcome_with_report(
    trace_id: &str,
    decision: &GatewayControlDecision,
    status_code: u16,
    body_json: Value,
    mut headers: BTreeMap<String, String>,
    background_report: Option<GatewaySyncReportRequest>,
) -> Result<LocalCoreSyncFinalizeOutcome, GatewayError> {
    headers.remove("content-encoding");
    headers.remove("content-length");
    headers.insert("content-type".to_string(), "application/json".to_string());
    let body_bytes =
        serde_json::to_vec(&body_json).map_err(|err| GatewayError::Internal(err.to_string()))?;
    headers.insert("content-length".to_string(), body_bytes.len().to_string());
    let response = build_client_response_from_parts(
        status_code,
        &headers,
        Body::from(body_bytes),
        trace_id,
        Some(decision),
    )?;
    Ok(LocalCoreSyncFinalizeOutcome {
        response,
        background_report,
    })
}

pub(super) fn build_local_success_outcome_with_conversion_report(
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &GatewaySyncReportRequest,
    client_body_json: Value,
    provider_body_json: Value,
) -> Result<LocalCoreSyncFinalizeOutcome, GatewayError> {
    let Some(report_kind) =
        map_local_finalize_kind_to_success_report_kind(payload.report_kind.as_str())
    else {
        return build_local_success_outcome_with_report(
            trace_id,
            decision,
            payload.status_code,
            client_body_json,
            payload.headers.clone(),
            None,
        );
    };

    let report_payload = GatewaySyncReportRequest {
        trace_id: payload.trace_id.clone(),
        report_kind: report_kind.to_string(),
        report_context: payload.report_context.clone(),
        status_code: payload.status_code,
        headers: payload.headers.clone(),
        body_json: Some(provider_body_json),
        client_body_json: Some(client_body_json.clone()),
        body_base64: None,
        telemetry: payload.telemetry.clone(),
    };

    build_local_success_outcome_with_report(
        trace_id,
        decision,
        payload.status_code,
        client_body_json,
        payload.headers.clone(),
        Some(report_payload),
    )
}

fn map_local_finalize_to_success_report(
    payload: &GatewaySyncReportRequest,
    body_json: Value,
    headers: BTreeMap<String, String>,
) -> Option<GatewaySyncReportRequest> {
    let report_kind = map_local_finalize_kind_to_success_report_kind(payload.report_kind.as_str())?;

    Some(GatewaySyncReportRequest {
        trace_id: payload.trace_id.clone(),
        report_kind: report_kind.to_string(),
        report_context: payload.report_context.clone(),
        status_code: payload.status_code,
        headers,
        body_json: Some(body_json),
        client_body_json: None,
        body_base64: None,
        telemetry: payload.telemetry.clone(),
    })
}

fn map_local_finalize_kind_to_success_report_kind(report_kind: &str) -> Option<&'static str> {
    match report_kind {
        "openai_chat_sync_finalize" => Some("openai_chat_sync_success"),
        "claude_chat_sync_finalize" => Some("claude_chat_sync_success"),
        "gemini_chat_sync_finalize" => Some("gemini_chat_sync_success"),
        "openai_cli_sync_finalize" | "openai_compact_sync_finalize" => {
            Some("openai_cli_sync_success")
        }
        "claude_cli_sync_finalize" => Some("claude_cli_sync_success"),
        "gemini_cli_sync_finalize" => Some("gemini_cli_sync_success"),
        _ => None,
    }
}

pub(crate) fn canonicalize_tool_arguments(value: Option<Value>) -> String {
    match value {
        Some(Value::String(text)) => text,
        Some(other) => serde_json::to_string(&other).unwrap_or_else(|_| "null".to_string()),
        None => "{}".to_string(),
    }
}

pub(crate) fn build_generated_tool_call_id(index: usize) -> String {
    format!("call_auto_{index}")
}

pub(super) fn parse_stream_json_events(body: &[u8]) -> Option<Vec<Value>> {
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
