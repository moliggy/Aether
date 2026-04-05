use serde_json::{json, Map, Value};

#[derive(Clone, Debug, Default)]
pub(crate) struct CanonicalUsage {
    pub(crate) input_tokens: u64,
    pub(crate) output_tokens: u64,
    pub(crate) total_tokens: u64,
}

#[derive(Clone, Debug)]
pub(crate) enum CanonicalStreamEvent {
    Start,
    TextDelta(String),
    ToolCallStart {
        index: usize,
        call_id: String,
        name: String,
    },
    ToolCallArgumentsDelta {
        index: usize,
        arguments: String,
    },
    Finish {
        finish_reason: Option<String>,
        usage: Option<CanonicalUsage>,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct CanonicalStreamFrame {
    pub(crate) id: String,
    pub(crate) model: String,
    pub(crate) event: CanonicalStreamEvent,
}

pub(crate) fn decode_json_data_line(line: &[u8]) -> Option<Value> {
    let text = std::str::from_utf8(line).ok()?;
    let trimmed = text.trim_matches('\r').trim();
    if trimmed.is_empty() || trimmed.starts_with(':') || trimmed.starts_with("event:") {
        return None;
    }
    let data_line = trimmed.strip_prefix("data:")?.trim();
    if data_line.is_empty() || data_line == "[DONE]" {
        return None;
    }
    serde_json::from_str(data_line).ok()
}

pub(crate) fn resolve_identity(
    response_id: Option<&str>,
    model: Option<&str>,
    report_context: &Value,
    default_id: &str,
) -> (String, String) {
    let id = response_id
        .filter(|value| !value.is_empty())
        .unwrap_or(default_id)
        .to_string();
    let model = model
        .filter(|value| !value.is_empty())
        .or_else(|| report_context.get("mapped_model").and_then(Value::as_str))
        .or_else(|| report_context.get("model").and_then(Value::as_str))
        .unwrap_or("unknown")
        .to_string();
    (id, model)
}

pub(crate) fn canonical_usage_from_openai_usage(value: Option<&Value>) -> Option<CanonicalUsage> {
    let usage = value?.as_object()?;
    let input_tokens = usage
        .get("input_tokens")
        .or_else(|| usage.get("prompt_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let output_tokens = usage
        .get("output_tokens")
        .or_else(|| usage.get("completion_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let total_tokens = usage
        .get("total_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(input_tokens + output_tokens);
    Some(CanonicalUsage {
        input_tokens,
        output_tokens,
        total_tokens,
    })
}

pub(crate) fn canonical_usage_from_claude_usage(value: Option<&Value>) -> Option<CanonicalUsage> {
    let usage = value?.as_object()?;
    let input_tokens = usage
        .get("input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let output_tokens = usage
        .get("output_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    Some(CanonicalUsage {
        input_tokens,
        output_tokens,
        total_tokens: input_tokens + output_tokens,
    })
}

pub(crate) fn canonical_usage_from_gemini_usage(value: Option<&Value>) -> Option<CanonicalUsage> {
    let usage = value?.as_object()?;
    let input_tokens = usage
        .get("promptTokenCount")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let output_tokens = usage
        .get("candidatesTokenCount")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let total_tokens = usage
        .get("totalTokenCount")
        .and_then(Value::as_u64)
        .unwrap_or(input_tokens + output_tokens);
    Some(CanonicalUsage {
        input_tokens,
        output_tokens,
        total_tokens,
    })
}

pub(crate) fn normalize_openai_finish_reason(value: Option<&str>) -> Option<String> {
    match value {
        Some("function_call") => Some("tool_calls".to_string()),
        Some(other) if !other.trim().is_empty() => Some(other.to_string()),
        _ => None,
    }
}

pub(crate) fn map_openai_finish_reason_to_claude(value: Option<&str>) -> &'static str {
    match value {
        Some("length") => "max_tokens",
        Some("tool_calls") | Some("function_call") => "tool_use",
        Some("content_filter") => "content_filtered",
        _ => "end_turn",
    }
}

pub(crate) fn map_openai_finish_reason_to_gemini(value: Option<&str>) -> &'static str {
    match value {
        Some("length") => "MAX_TOKENS",
        Some("content_filter") => "SAFETY",
        _ => "STOP",
    }
}

pub(crate) fn parse_json_arguments_value(arguments: &str) -> Option<Value> {
    let trimmed = arguments.trim();
    if trimmed.is_empty() {
        return Some(Value::Object(Map::new()));
    }
    serde_json::from_str(trimmed).ok()
}

pub(crate) fn build_openai_chat_chunk(
    id: &str,
    model: &str,
    text: String,
    tool_calls: Option<Vec<Value>>,
    finish_reason: Option<&str>,
) -> Value {
    let mut delta = Map::new();
    delta.insert("role".to_string(), Value::String("assistant".to_string()));
    if !text.is_empty() {
        delta.insert("content".to_string(), Value::String(text));
    } else if tool_calls.is_none() {
        delta.insert("content".to_string(), Value::String(String::new()));
    }
    if let Some(tool_calls) = tool_calls {
        delta.insert("tool_calls".to_string(), Value::Array(tool_calls));
    }

    json!({
        "id": id,
        "object": "chat.completion.chunk",
        "model": model,
        "choices": [{
            "index": 0,
            "delta": Value::Object(delta),
            "finish_reason": finish_reason,
        }]
    })
}

pub(crate) fn build_openai_chat_role_chunk(id: &str, model: &str) -> Value {
    json!({
        "id": id,
        "object": "chat.completion.chunk",
        "model": model,
        "choices": [{
            "index": 0,
            "delta": {
                "role": "assistant"
            },
            "finish_reason": Value::Null
        }]
    })
}

pub(crate) fn build_openai_chat_finish_chunk(
    id: &str,
    model: &str,
    finish_reason: Option<&str>,
) -> Value {
    json!({
        "id": id,
        "object": "chat.completion.chunk",
        "model": model,
        "choices": [{
            "index": 0,
            "delta": {},
            "finish_reason": finish_reason,
        }]
    })
}
