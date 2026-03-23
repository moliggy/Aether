use std::collections::BTreeMap;

use serde_json::Value;

use crate::gateway::GatewayError;

fn inject_antigravity_tool_ids(value: &mut Value) {
    let Some(candidates) = value.get_mut("candidates").and_then(Value::as_array_mut) else {
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

        let mut counters: BTreeMap<String, usize> = BTreeMap::new();
        for part in parts {
            let Some(function_call) = part.get_mut("functionCall").and_then(Value::as_object_mut)
            else {
                continue;
            };
            let has_id = function_call
                .get("id")
                .and_then(Value::as_str)
                .is_some_and(|value| !value.is_empty());
            if has_id {
                continue;
            }
            let name = function_call
                .get("name")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .unwrap_or("unknown")
                .to_string();
            let index = counters.entry(name.clone()).or_insert(0);
            function_call.insert(
                "id".to_string(),
                Value::String(format!("call_{name}_{index}")),
            );
            *index += 1;
        }
    }
}

pub(super) fn transform_envelope_line(
    report_context: &Value,
    line: Vec<u8>,
) -> Result<Vec<u8>, GatewayError> {
    let Ok(text) = std::str::from_utf8(&line) else {
        return Ok(line);
    };
    let trimmed = text.trim_matches('\r').trim();
    if trimmed.is_empty() || trimmed.starts_with(':') || trimmed.starts_with("event:") {
        return Ok(Vec::new());
    }
    let Some(data_line) = trimmed.strip_prefix("data:") else {
        return Ok(line);
    };
    let data_line = data_line.trim();
    if data_line.is_empty() || data_line == "[DONE]" {
        return Ok(line);
    }

    let body: Value = match serde_json::from_str(data_line) {
        Ok(value) => value,
        Err(_) => return Ok(line),
    };

    let envelope_name = report_context
        .get("envelope_name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let unwrapped = match envelope_name {
        "gemini_cli:v1internal" => body.get("response").cloned().unwrap_or(body),
        "antigravity:v1internal" => {
            let mut response = body.get("response").cloned().unwrap_or(body.clone());
            if let Some(response_id) = body.get("responseId").cloned() {
                if let Some(object) = response.as_object_mut() {
                    object
                        .entry("_v1internal_response_id".to_string())
                        .or_insert(response_id);
                }
            }
            inject_antigravity_tool_ids(&mut response);
            response
        }
        _ => body,
    };

    let mut out = b"data: ".to_vec();
    out.extend(
        serde_json::to_vec(&unwrapped).map_err(|err| GatewayError::Internal(err.to_string()))?,
    );
    out.extend_from_slice(b"\n\n");
    Ok(out)
}
