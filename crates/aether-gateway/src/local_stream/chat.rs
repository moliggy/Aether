use serde_json::{json, Map, Value};

use crate::gateway::GatewayError;

use super::common::{encode_done_sse, encode_json_sse, map_claude_stop_reason};
use super::*;

#[derive(Default)]
pub(super) struct ClaudeToOpenAIChatStreamState {
    raw: Vec<u8>,
    message_id: Option<String>,
    model: Option<String>,
}

#[derive(Default)]
pub(super) struct GeminiToOpenAIChatStreamState {
    raw: Vec<u8>,
}

#[derive(Default)]
struct ClaudeToolCallState {
    id: String,
    name: String,
    arguments: String,
}

fn canonicalize_arguments(value: Option<Value>) -> String {
    match value {
        Some(Value::String(text)) => text,
        Some(other) => serde_json::to_string(&other).unwrap_or_else(|_| "null".to_string()),
        None => "{}".to_string(),
    }
}

fn claude_tool_calls(content: &[Value]) -> Option<Vec<Value>> {
    let mut tool_calls = Vec::new();
    for (index, block) in content.iter().enumerate() {
        let Some(block) = block.as_object() else {
            continue;
        };
        if block.get("type").and_then(Value::as_str).unwrap_or("text") != "tool_use" {
            continue;
        }
        let state = ClaudeToolCallState {
            id: block
                .get("id")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .unwrap_or("tool_call")
                .to_string(),
            name: block
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
            arguments: canonicalize_arguments(block.get("input").cloned()),
        };
        tool_calls.push(json!({
            "index": index,
            "id": state.id,
            "type": "function",
            "function": {
                "name": state.name,
                "arguments": state.arguments,
            }
        }));
    }
    if tool_calls.is_empty() {
        None
    } else {
        Some(tool_calls)
    }
}

fn gemini_tool_calls(parts: &[Value]) -> Option<Vec<Value>> {
    let mut tool_calls = Vec::new();
    for (index, part) in parts.iter().enumerate() {
        let Some(part) = part.as_object() else {
            continue;
        };
        let Some(function_call) = part.get("functionCall").and_then(Value::as_object) else {
            continue;
        };
        let name = function_call
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let id = function_call
            .get("id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| format!("call_{name}_{index}"));
        tool_calls.push(json!({
            "index": index,
            "id": id,
            "type": "function",
            "function": {
                "name": name,
                "arguments": canonicalize_arguments(function_call.get("args").cloned()),
            }
        }));
    }
    if tool_calls.is_empty() {
        None
    } else {
        Some(tool_calls)
    }
}

fn build_openai_chat_chunk(
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

fn claude_identity<'a>(
    state: &'a ClaudeToOpenAIChatStreamState,
    report_context: &'a Value,
) -> (&'a str, &'a str) {
    let id = state
        .message_id
        .as_deref()
        .unwrap_or("chatcmpl-local-stream");
    let model = state
        .model
        .as_deref()
        .or_else(|| report_context.get("mapped_model").and_then(Value::as_str))
        .or_else(|| report_context.get("model").and_then(Value::as_str))
        .unwrap_or("unknown");
    (id, model)
}

fn convert_claude_aggregated_to_openai_chunk(
    body_json: &Value,
    report_context: &Value,
) -> Option<Value> {
    let body = body_json.as_object()?;
    let content = body.get("content")?.as_array()?;
    let mut text = String::new();
    for block in content {
        let block = block.as_object()?;
        if block.get("type").and_then(Value::as_str).unwrap_or("text") == "text" {
            if let Some(piece) = block.get("text").and_then(Value::as_str) {
                text.push_str(piece);
            }
        }
    }
    let tool_calls = claude_tool_calls(content);
    let finish_reason = map_claude_stop_reason(
        body.get("stop_reason").and_then(Value::as_str),
        tool_calls.is_some(),
    );
    let model = body
        .get("model")
        .and_then(Value::as_str)
        .or_else(|| report_context.get("mapped_model").and_then(Value::as_str))
        .or_else(|| report_context.get("model").and_then(Value::as_str))
        .unwrap_or("unknown");
    let id = body
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or("chatcmpl-local-stream");

    Some(build_openai_chat_chunk(
        id,
        model,
        text,
        tool_calls,
        finish_reason,
    ))
}

fn convert_gemini_aggregated_to_openai_chunk(
    body_json: &Value,
    report_context: &Value,
) -> Option<Value> {
    let body = body_json.as_object()?;
    let candidates = body.get("candidates")?.as_array()?;
    let first_candidate = candidates.first()?.as_object()?;
    let content = first_candidate.get("content")?.as_object()?;
    let parts = content.get("parts")?.as_array()?;
    let mut text = String::new();
    for part in parts {
        let part = part.as_object()?;
        if let Some(piece) = part.get("text").and_then(Value::as_str) {
            text.push_str(piece);
        }
    }
    let tool_calls = gemini_tool_calls(parts);
    let mut finish_reason = match first_candidate.get("finishReason").and_then(Value::as_str) {
        Some("STOP") => Some("stop"),
        Some("MAX_TOKENS") => Some("length"),
        Some("SAFETY") => Some("content_filter"),
        _ => None,
    };
    if tool_calls.is_some() && finish_reason.is_none_or(|value| value == "stop") {
        finish_reason = Some("tool_calls");
    }
    let model = body
        .get("modelVersion")
        .and_then(Value::as_str)
        .or_else(|| report_context.get("mapped_model").and_then(Value::as_str))
        .or_else(|| report_context.get("model").and_then(Value::as_str))
        .unwrap_or("unknown");
    let id = body
        .get("responseId")
        .and_then(Value::as_str)
        .or_else(|| body.get("_v1internal_response_id").and_then(Value::as_str))
        .unwrap_or("chatcmpl-local-stream");

    Some(build_openai_chat_chunk(
        id,
        model,
        text,
        tool_calls,
        finish_reason,
    ))
}

impl ClaudeToOpenAIChatStreamState {
    pub(super) fn transform_line(
        &mut self,
        report_context: &Value,
        line: Vec<u8>,
    ) -> Result<Vec<u8>, GatewayError> {
        self.raw.extend_from_slice(&line);

        let Ok(text) = std::str::from_utf8(&line) else {
            return Ok(Vec::new());
        };
        let trimmed = text.trim_matches('\r').trim();
        if trimmed.is_empty() {
            if self
                .raw
                .windows(b"\"type\":\"message_stop\"".len())
                .any(|window| window == b"\"type\":\"message_stop\"")
            {
                return Ok(encode_done_sse());
            }
            return Ok(Vec::new());
        }
        let Some(data_line) = trimmed.strip_prefix("data:") else {
            return Ok(Vec::new());
        };
        let data_line = data_line.trim();
        if data_line.is_empty() || data_line == "[DONE]" {
            return Ok(Vec::new());
        }
        let value: Value = match serde_json::from_str(data_line) {
            Ok(value) => value,
            Err(_) => return Ok(Vec::new()),
        };
        match value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
        {
            "message_start" => {
                if let Some(message) = value.get("message").and_then(Value::as_object) {
                    self.message_id = message
                        .get("id")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned);
                    self.model = message
                        .get("model")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned);
                }
                let (id, model) = claude_identity(self, report_context);
                encode_json_sse(
                    None,
                    &json!({
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
                    }),
                )
            }
            "content_block_delta" => {
                let Some(delta) = value.get("delta").and_then(Value::as_object) else {
                    return Ok(Vec::new());
                };
                if delta.get("type").and_then(Value::as_str) != Some("text_delta") {
                    return Ok(Vec::new());
                }
                let Some(piece) = delta.get("text").and_then(Value::as_str) else {
                    return Ok(Vec::new());
                };
                let (id, model) = claude_identity(self, report_context);
                encode_json_sse(
                    None,
                    &build_openai_chat_chunk(id, model, piece.to_string(), None, None),
                )
            }
            "content_block_start" => {
                let Some(block) = value.get("content_block").and_then(Value::as_object) else {
                    return Ok(Vec::new());
                };
                if block.get("type").and_then(Value::as_str) != Some("tool_use") {
                    return Ok(Vec::new());
                }
                let call = json!({
                    "index": value.get("index").and_then(Value::as_u64).unwrap_or(0),
                    "id": block
                        .get("id")
                        .and_then(Value::as_str)
                        .filter(|value| !value.is_empty())
                        .unwrap_or("tool_call"),
                    "type": "function",
                    "function": {
                        "name": block.get("name").and_then(Value::as_str).unwrap_or("unknown"),
                        "arguments": canonicalize_arguments(block.get("input").cloned()),
                    }
                });
                let (id, model) = claude_identity(self, report_context);
                encode_json_sse(
                    None,
                    &build_openai_chat_chunk(id, model, String::new(), Some(vec![call]), None),
                )
            }
            "message_delta" => {
                let Some(delta) = value.get("delta").and_then(Value::as_object) else {
                    return Ok(Vec::new());
                };
                let Some(finish_reason) = map_claude_stop_reason(
                    delta.get("stop_reason").and_then(Value::as_str),
                    delta.get("stop_reason").and_then(Value::as_str) == Some("tool_use"),
                ) else {
                    return Ok(Vec::new());
                };
                let (id, model) = claude_identity(self, report_context);
                encode_json_sse(
                    None,
                    &json!({
                        "id": id,
                        "object": "chat.completion.chunk",
                        "model": model,
                        "choices": [{
                            "index": 0,
                            "delta": {},
                            "finish_reason": finish_reason
                        }]
                    }),
                )
            }
            _ => Ok(Vec::new()),
        }
    }

    pub(super) fn finish(&mut self) -> Vec<u8> {
        if self.raw.is_empty() {
            return Vec::new();
        }
        let aggregated = aggregate_claude_stream_sync_response(&self.raw);
        self.raw.clear();
        let Some(aggregated) = aggregated else {
            return Vec::new();
        };
        let Some(chunk) = convert_claude_aggregated_to_openai_chunk(&aggregated, &Value::Null)
        else {
            return Vec::new();
        };
        let mut out = encode_json_sse(None, &chunk).unwrap_or_default();
        out.extend(encode_done_sse());
        out
    }
}

impl GeminiToOpenAIChatStreamState {
    pub(super) fn transform_line(
        &mut self,
        _report_context: &Value,
        line: Vec<u8>,
    ) -> Result<Vec<u8>, GatewayError> {
        self.raw.extend_from_slice(&line);
        Ok(Vec::new())
    }

    pub(super) fn finish(&mut self, report_context: &Value) -> Result<Vec<u8>, GatewayError> {
        if self.raw.is_empty() {
            return Ok(Vec::new());
        }
        let aggregated = aggregate_gemini_stream_sync_response(&self.raw);
        self.raw.clear();
        let Some(aggregated) = aggregated else {
            return Ok(Vec::new());
        };
        let Some(chunk) = convert_gemini_aggregated_to_openai_chunk(&aggregated, report_context)
        else {
            return Ok(Vec::new());
        };
        let mut out = encode_json_sse(None, &chunk)?;
        out.extend(encode_done_sse());
        Ok(out)
    }
}
