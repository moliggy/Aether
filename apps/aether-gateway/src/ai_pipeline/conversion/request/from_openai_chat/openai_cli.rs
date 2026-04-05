use std::collections::BTreeMap;

use serde_json::{json, Map, Value};

use super::super::to_openai_chat::extract_openai_text_content;
use crate::ai_pipeline::planner::standard::copy_request_number_field;

pub(crate) fn convert_openai_chat_request_to_openai_cli_request(
    body_json: &Value,
    mapped_model: &str,
    upstream_is_stream: bool,
    compact: bool,
) -> Option<Value> {
    let request = body_json.as_object()?;
    let mut instructions = Vec::new();
    let mut input_items = Vec::new();
    let mut next_generated_tool_call_index = 0usize;
    let mut tool_call_id_aliases = BTreeMap::new();

    if let Some(message_values) = request.get("messages").and_then(Value::as_array) {
        for message in message_values {
            let message_object = message.as_object()?;
            let role = message_object
                .get("role")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase();
            match role.as_str() {
                "system" | "developer" => {
                    let text = extract_openai_text_content(message_object.get("content"))?;
                    if !text.trim().is_empty() {
                        instructions.push(text);
                    }
                }
                "user" | "assistant" => {
                    let content_items = convert_openai_content_to_openai_cli_items(
                        message_object.get("content"),
                        role.as_str(),
                    )?;
                    if !content_items.is_empty() {
                        input_items.push(json!({
                            "type": "message",
                            "role": role,
                            "content": content_items,
                        }));
                    }

                    if role == "assistant" {
                        if let Some(tool_calls) =
                            message_object.get("tool_calls").and_then(Value::as_array)
                        {
                            for tool_call in tool_calls {
                                let tool_call_object = tool_call.as_object()?;
                                let function = tool_call_object.get("function")?.as_object()?;
                                let tool_name = function
                                    .get("name")
                                    .and_then(Value::as_str)
                                    .map(str::trim)
                                    .filter(|value| !value.is_empty())?;
                                let raw_call_id = tool_call_object
                                    .get("id")
                                    .and_then(Value::as_str)
                                    .map(str::trim)
                                    .unwrap_or_default();
                                let call_id = if raw_call_id.is_empty() {
                                    let generated =
                                        format!("call_auto_{next_generated_tool_call_index}");
                                    next_generated_tool_call_index += 1;
                                    generated
                                } else {
                                    raw_call_id.to_string()
                                };
                                if !raw_call_id.is_empty() && raw_call_id != call_id {
                                    tool_call_id_aliases
                                        .insert(raw_call_id.to_string(), call_id.clone());
                                }
                                let arguments = function
                                    .get("arguments")
                                    .and_then(Value::as_str)
                                    .map(ToOwned::to_owned)
                                    .unwrap_or_else(|| "{}".to_string());
                                input_items.push(json!({
                                    "type": "function_call",
                                    "call_id": call_id,
                                    "name": tool_name,
                                    "arguments": arguments,
                                }));
                            }
                        }
                    }
                }
                "tool" => {
                    let raw_tool_call_id = message_object
                        .get("tool_call_id")
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .unwrap_or_default();
                    let tool_call_id = if raw_tool_call_id.is_empty() {
                        let generated = format!("call_auto_{next_generated_tool_call_index}");
                        next_generated_tool_call_index += 1;
                        generated
                    } else {
                        tool_call_id_aliases
                            .get(raw_tool_call_id)
                            .cloned()
                            .unwrap_or_else(|| raw_tool_call_id.to_string())
                    };
                    let output = match message_object.get("content") {
                        Some(Value::String(text)) => text.clone(),
                        Some(other) => serde_json::to_string(other).ok()?,
                        None => String::new(),
                    };
                    input_items.push(json!({
                        "type": "function_call_output",
                        "call_id": tool_call_id,
                        "output": output,
                    }));
                }
                _ => {}
            }
        }
    }

    let mut output = Map::new();
    output.insert("model".to_string(), Value::String(mapped_model.to_string()));
    if !instructions.is_empty() {
        output.insert(
            "instructions".to_string(),
            Value::String(
                instructions
                    .into_iter()
                    .filter(|value: &String| !value.trim().is_empty())
                    .collect::<Vec<_>>()
                    .join("\n\n"),
            ),
        );
    }
    output.insert("input".to_string(), Value::Array(input_items));

    if upstream_is_stream && !compact {
        output.insert("stream".to_string(), Value::Bool(true));
    }
    if let Some(max_tokens) = request.get("max_tokens").and_then(Value::as_u64) {
        output.insert("max_output_tokens".to_string(), Value::from(max_tokens));
    }
    copy_request_number_field(request, &mut output, "temperature");
    copy_request_number_field(request, &mut output, "top_p");
    copy_request_integer_field(request, &mut output, "top_logprobs");
    copy_request_bool_field(request, &mut output, "parallel_tool_calls");

    for passthrough_key in [
        "prompt_cache_key",
        "service_tier",
        "metadata",
        "store",
        "previous_response_id",
        "truncation",
        "reasoning",
        "stop",
    ] {
        if let Some(value) = request.get(passthrough_key) {
            output.insert(passthrough_key.to_string(), value.clone());
        }
    }

    if let Some(text) = build_openai_cli_text_config_from_openai_chat_request(request) {
        output.insert("text".to_string(), Value::Object(text));
    }
    if let Some(tools) = build_openai_cli_tools_from_openai_chat_request(request) {
        output.insert("tools".to_string(), Value::Array(tools));
    }
    if let Some(tool_choice) = build_openai_cli_tool_choice_from_openai_chat_request(request) {
        output.insert("tool_choice".to_string(), tool_choice);
    }

    Some(Value::Object(output))
}

fn convert_openai_content_to_openai_cli_items(
    content: Option<&Value>,
    role: &str,
) -> Option<Vec<Value>> {
    let Some(content) = content else {
        return Some(Vec::new());
    };
    match content {
        Value::String(text) => {
            if text.is_empty() {
                Some(Vec::new())
            } else {
                Some(vec![json!({
                    "type": if role == "assistant" { "output_text" } else { "input_text" },
                    "text": text,
                })])
            }
        }
        Value::Array(parts) => {
            let mut items = Vec::new();
            for part in parts {
                let part_object = part.as_object()?;
                let part_type = part_object
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("text")
                    .trim()
                    .to_ascii_lowercase();
                match part_type.as_str() {
                    "text" | "input_text" | "output_text" => {
                        if let Some(text) = part_object.get("text").and_then(Value::as_str) {
                            if !text.is_empty() {
                                items.push(json!({
                                    "type": if role == "assistant" { "output_text" } else { "input_text" },
                                    "text": text,
                                }));
                            }
                        }
                    }
                    "image_url" => {
                        let image_url = part_object
                            .get("image_url")
                            .and_then(Value::as_object)
                            .and_then(|value| value.get("url"))
                            .and_then(Value::as_str)
                            .or_else(|| part_object.get("image_url").and_then(Value::as_str))?;
                        items.push(json!({
                            "type": if role == "assistant" { "output_image" } else { "input_image" },
                            "image_url": image_url,
                        }));
                    }
                    "input_image" | "output_image" => {
                        let image_url = part_object
                            .get("image_url")
                            .and_then(Value::as_str)
                            .or_else(|| part_object.get("url").and_then(Value::as_str))?;
                        items.push(json!({
                            "type": if role == "assistant" { "output_image" } else { "input_image" },
                            "image_url": image_url,
                        }));
                    }
                    _ => {}
                }
            }
            Some(items)
        }
        _ => None,
    }
}

fn build_openai_cli_text_config_from_openai_chat_request(
    request: &Map<String, Value>,
) -> Option<Map<String, Value>> {
    let mut text = Map::new();
    if let Some(response_format) = request.get("response_format") {
        text.insert("format".to_string(), response_format.clone());
    }
    if let Some(verbosity) = request.get("verbosity") {
        text.insert("verbosity".to_string(), verbosity.clone());
    }
    (!text.is_empty()).then_some(text)
}

fn build_openai_cli_tools_from_openai_chat_request(
    request: &Map<String, Value>,
) -> Option<Vec<Value>> {
    let mut tools = Vec::new();
    if let Some(tool_values) = request.get("tools").and_then(Value::as_array) {
        for tool in tool_values {
            let tool_object = tool.as_object()?;
            let tool_type = tool_object
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("function")
                .trim()
                .to_ascii_lowercase();
            match tool_type.as_str() {
                "function" => {
                    let function = tool_object.get("function")?.as_object()?;
                    let name = function
                        .get("name")
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())?;
                    let mut rebuilt = Map::new();
                    rebuilt.insert("type".to_string(), Value::String("function".to_string()));
                    rebuilt.insert("name".to_string(), Value::String(name.to_string()));
                    if let Some(description) = function.get("description") {
                        rebuilt.insert("description".to_string(), description.clone());
                    }
                    if let Some(parameters) = function.get("parameters") {
                        rebuilt.insert("parameters".to_string(), parameters.clone());
                    }
                    tools.push(Value::Object(rebuilt));
                }
                "custom" => {
                    let custom = tool_object.get("custom").and_then(Value::as_object)?;
                    let mut rebuilt = Map::new();
                    rebuilt.insert("type".to_string(), Value::String("custom".to_string()));
                    if let Some(name) = custom.get("name") {
                        rebuilt.insert("name".to_string(), name.clone());
                    }
                    if let Some(description) = custom.get("description") {
                        rebuilt.insert("description".to_string(), description.clone());
                    }
                    if let Some(format) = custom.get("format") {
                        rebuilt.insert("format".to_string(), format.clone());
                    }
                    tools.push(Value::Object(rebuilt));
                }
                _ => tools.push(tool.clone()),
            }
        }
    }

    if let Some(web_search_options) = request.get("web_search_options").and_then(Value::as_object) {
        let mut tool = Map::new();
        tool.insert("type".to_string(), Value::String("web_search".to_string()));
        if let Some(user_location) = web_search_options
            .get("user_location")
            .and_then(Value::as_object)
        {
            if user_location.get("type").and_then(Value::as_str) == Some("approximate") {
                if let Some(approximate) =
                    user_location.get("approximate").and_then(Value::as_object)
                {
                    let mut flattened = Map::new();
                    flattened.insert("type".to_string(), Value::String("approximate".to_string()));
                    if let Some(country) = approximate.get("country") {
                        flattened.insert("country".to_string(), country.clone());
                    }
                    if let Some(city) = approximate.get("city") {
                        flattened.insert("city".to_string(), city.clone());
                    }
                    tool.insert("user_location".to_string(), Value::Object(flattened));
                }
            }
        }
        if let Some(search_context_size) = web_search_options.get("search_context_size") {
            tool.insert(
                "search_context_size".to_string(),
                search_context_size.clone(),
            );
        }
        tools.push(Value::Object(tool));
    }

    (!tools.is_empty()).then_some(tools)
}

fn build_openai_cli_tool_choice_from_openai_chat_request(
    request: &Map<String, Value>,
) -> Option<Value> {
    let tool_choice = request.get("tool_choice")?;
    match tool_choice {
        Value::String(value) => Some(Value::String(value.clone())),
        Value::Object(object) => {
            let choice_type = object
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase();
            match choice_type.as_str() {
                "function" => {
                    let function = object.get("function").and_then(Value::as_object)?;
                    let name = function.get("name")?.as_str()?;
                    Some(json!({
                        "type": "function",
                        "name": name,
                    }))
                }
                "custom" => {
                    let custom = object.get("custom").and_then(Value::as_object)?;
                    let name = custom.get("name")?.as_str()?;
                    Some(json!({
                        "type": "custom",
                        "name": name,
                    }))
                }
                "allowed_tools" => {
                    let allowed_tools = object.get("allowed_tools").and_then(Value::as_object)?;
                    Some(json!({
                        "type": "allowed_tools",
                        "mode": allowed_tools.get("mode").cloned().unwrap_or_else(|| Value::String("auto".to_string())),
                        "tools": allowed_tools.get("tools").cloned().unwrap_or_else(|| Value::Array(Vec::new())),
                    }))
                }
                _ => Some(tool_choice.clone()),
            }
        }
        _ => Some(tool_choice.clone()),
    }
}

fn copy_request_integer_field(
    request: &Map<String, Value>,
    output: &mut Map<String, Value>,
    field: &str,
) {
    if let Some(value) = request.get(field).and_then(Value::as_i64) {
        output.insert(field.to_string(), Value::from(value));
    }
}

fn copy_request_bool_field(
    request: &Map<String, Value>,
    output: &mut Map<String, Value>,
    field: &str,
) {
    if let Some(value) = request.get(field).and_then(Value::as_bool) {
        output.insert(field.to_string(), Value::Bool(value));
    }
}
