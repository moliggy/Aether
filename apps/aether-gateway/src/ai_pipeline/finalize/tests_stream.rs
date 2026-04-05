use serde_json::json;

use super::maybe_build_local_stream_rewriter;

fn utf8(bytes: Vec<u8>) -> String {
    String::from_utf8(bytes).expect("utf8 should decode")
}

#[test]
fn antigravity_stream_rewriter_unwraps_and_injects_tool_ids() {
    let report_context = json!({
        "has_envelope": true,
        "provider_api_format": "gemini:cli",
        "client_api_format": "gemini:cli",
        "envelope_name": "antigravity:v1internal",
        "needs_conversion": false,
        "mapped_model": "claude-sonnet-4-5",
    });
    let mut rewriter =
        maybe_build_local_stream_rewriter(Some(&report_context)).expect("rewriter should exist");
    let output = rewriter
        .push_chunk(
            b"data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"name\":\"get_weather\",\"args\":{\"city\":\"SF\"}}}],\"role\":\"model\"},\"index\":0}],\"modelVersion\":\"claude-sonnet-4-5\"},\"responseId\":\"resp_123\"}\n\n",
        )
        .expect("rewrite should succeed");
    let output_text = String::from_utf8(output).expect("text should be utf8");
    assert!(output_text.contains("\"_v1internal_response_id\":\"resp_123\""));
    assert!(output_text.contains("\"id\":\"call_get_weather_0\""));
    assert!(output_text.contains("\"modelVersion\":\"claude-sonnet-4-5\""));
}

#[test]
fn gemini_cli_v1internal_stream_rewriter_unwraps_response_object() {
    let report_context = json!({
        "has_envelope": true,
        "provider_api_format": "gemini:cli",
        "client_api_format": "gemini:cli",
        "envelope_name": "gemini_cli:v1internal",
        "needs_conversion": false,
    });
    let mut rewriter =
        maybe_build_local_stream_rewriter(Some(&report_context)).expect("rewriter should exist");
    let output = rewriter
        .push_chunk(
            b"data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello Gemini CLI\"}],\"role\":\"model\"},\"index\":0}],\"modelVersion\":\"gemini-cli-2.5\"}}\n\n",
        )
        .expect("rewrite should succeed");
    let output_text = String::from_utf8(output).expect("text should be utf8");
    assert_eq!(
        output_text,
        "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello Gemini CLI\"}],\"role\":\"model\"},\"index\":0}],\"modelVersion\":\"gemini-cli-2.5\"}\n\n"
    );
}

#[test]
fn claude_to_openai_chat_stream_rewriter_converts_text_deltas() {
    let report_context = json!({
        "provider_api_format": "claude:chat",
        "client_api_format": "openai:chat",
        "needs_conversion": true,
        "mapped_model": "claude-sonnet-4-5",
    });
    let mut rewriter =
        maybe_build_local_stream_rewriter(Some(&report_context)).expect("rewriter should exist");
    let output = rewriter
        .push_chunk(
            concat!(
                "event: message_start\n",
                "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_123\",\"model\":\"claude-sonnet-4-5\"}}\n\n",
                "event: content_block_delta\n",
                "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}\n\n",
                "event: message_delta\n",
                "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n",
                "event: message_stop\n",
                "data: {\"type\":\"message_stop\"}\n\n"
            )
            .as_bytes(),
        )
        .expect("rewrite should succeed");
    let output_text = String::from_utf8(output).expect("utf8 should decode");
    assert!(output_text.contains("\"object\":\"chat.completion.chunk\""));
    assert!(output_text.contains("\"role\":\"assistant\""));
    assert!(output_text.contains("\"content\":\"Hello\""));
    assert!(output_text.contains("\"finish_reason\":\"stop\""));
    assert!(output_text.contains("data: [DONE]"));
}

#[test]
fn claude_to_openai_chat_stream_rewriter_converts_tool_use_to_tool_calls() {
    let report_context = json!({
        "provider_api_format": "claude:chat",
        "client_api_format": "openai:chat",
        "needs_conversion": true,
        "mapped_model": "claude-sonnet-4-5",
    });
    let mut rewriter =
        maybe_build_local_stream_rewriter(Some(&report_context)).expect("rewriter should exist");
    let output = rewriter
        .push_chunk(
            concat!(
                "event: message_start\n",
                "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_tool_claude_chat_stream_123\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-5\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null}}\n\n",
                "event: content_block_start\n",
                "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"Need a tool.\"}}\n\n",
                "event: content_block_stop\n",
                "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
                "event: content_block_start\n",
                "data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"tool_123\",\"name\":\"get_weather\",\"input\":{\"location\":\"Tokyo\"}}}\n\n",
                "event: message_delta\n",
                "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"}}\n\n",
                "event: message_stop\n",
                "data: {\"type\":\"message_stop\"}\n\n"
            )
            .as_bytes(),
        )
        .expect("rewrite should succeed");
    let output_text = String::from_utf8(output).expect("utf8 should decode");
    assert!(output_text.contains("\"object\":\"chat.completion.chunk\""));
    assert!(output_text.contains("\"role\":\"assistant\""));
    assert!(output_text.contains("\"tool_calls\":[{"));
    assert!(output_text.contains("\"id\":\"tool_123\""));
    assert!(output_text.contains("\"name\":\"get_weather\""));
    assert!(output_text.contains("\\\"location\\\":\\\"Tokyo\\\""));
    assert!(output_text.contains("\"finish_reason\":\"tool_calls\""));
    assert!(output_text.contains("data: [DONE]"));
}

#[test]
fn gemini_to_openai_chat_stream_rewriter_buffers_and_converts_text() {
    let report_context = json!({
        "provider_api_format": "gemini:chat",
        "client_api_format": "openai:chat",
        "needs_conversion": true,
        "mapped_model": "gemini-2.5-pro",
    });
    let mut rewriter =
        maybe_build_local_stream_rewriter(Some(&report_context)).expect("rewriter should exist");
    let first = rewriter
        .push_chunk(
            b"data: {\"responseId\":\"resp_123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello \"}],\"role\":\"model\"},\"index\":0}],\"modelVersion\":\"gemini-2.5-pro\"}\n\n",
        )
        .expect("rewrite should succeed");
    let first_text = utf8(first);
    assert!(first_text.contains("\"object\":\"chat.completion.chunk\""));
    assert!(first_text.contains("\"role\":\"assistant\""));
    assert!(first_text.contains("\"content\":\"Hello \""));
    assert!(!first_text.contains("data: [DONE]"));
    let second = rewriter
        .push_chunk(
            b"data: {\"responseId\":\"resp_123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Gemini\"}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"modelVersion\":\"gemini-2.5-pro\"}\n\n",
        )
        .expect("rewrite should succeed");
    let output_text = utf8(second);
    assert!(output_text.contains("\"object\":\"chat.completion.chunk\""));
    assert!(output_text.contains("\"content\":\"Gemini\""));
    assert!(output_text.contains("\"finish_reason\":\"stop\""));
    assert!(output_text.contains("data: [DONE]"));
    assert!(rewriter.finish().expect("finish should succeed").is_empty());
}

#[test]
fn gemini_to_openai_chat_stream_rewriter_buffers_and_converts_function_call() {
    let report_context = json!({
        "provider_api_format": "gemini:chat",
        "client_api_format": "openai:chat",
        "needs_conversion": true,
        "mapped_model": "gemini-2.5-pro",
    });
    let mut rewriter =
        maybe_build_local_stream_rewriter(Some(&report_context)).expect("rewriter should exist");
    let output = rewriter
        .push_chunk(
            b"data: {\"responseId\":\"resp_tool_123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Need a tool.\"},{\"functionCall\":{\"name\":\"get_weather\",\"args\":{\"city\":\"SF\"}}}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"modelVersion\":\"gemini-2.5-pro\"}\n\n",
        )
        .expect("rewrite should succeed");
    let output_text = utf8(output);
    assert!(output_text.contains("\"object\":\"chat.completion.chunk\""));
    assert!(output_text.contains("\"role\":\"assistant\""));
    assert!(output_text.contains("\"content\":\"Need a tool.\""));
    assert!(output_text.contains("\"tool_calls\":[{"));
    assert!(output_text.contains("\"name\":\"get_weather\""));
    assert!(output_text.contains("\\\"city\\\":\\\"SF\\\""));
    assert!(output_text.contains("\"finish_reason\":\"tool_calls\""));
    assert!(output_text.contains("data: [DONE]"));
    assert!(rewriter.finish().expect("finish should succeed").is_empty());
}

#[test]
fn openai_cli_to_openai_chat_stream_rewriter_converts_text_deltas_immediately() {
    let report_context = json!({
        "provider_api_format": "openai:cli",
        "client_api_format": "openai:chat",
        "needs_conversion": true,
        "mapped_model": "gpt-5.4",
    });
    let mut rewriter =
        maybe_build_local_stream_rewriter(Some(&report_context)).expect("rewriter should exist");
    let created = rewriter
        .push_chunk(
            concat!(
                "event: response.created\n",
                "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_cli_stream_123\",\"object\":\"response\",\"model\":\"gpt-5.4\",\"status\":\"in_progress\"}}\n\n"
            )
            .as_bytes(),
        )
        .expect("rewrite should succeed");
    let created_text = String::from_utf8(created).expect("utf8 should decode");
    assert!(created_text.contains("\"object\":\"chat.completion.chunk\""));
    assert!(created_text.contains("\"role\":\"assistant\""));
    assert!(!created_text.contains("data: [DONE]"));

    let delta = rewriter
        .push_chunk(
            concat!(
                "event: response.output_text.delta\n",
                "data: {\"type\":\"response.output_text.delta\",\"delta\":\"Hello Codex\"}\n\n"
            )
            .as_bytes(),
        )
        .expect("rewrite should succeed");
    let delta_text = String::from_utf8(delta).expect("utf8 should decode");
    assert!(delta_text.contains("\"object\":\"chat.completion.chunk\""));
    assert!(delta_text.contains("\"content\":\"Hello Codex\""));
    assert!(!delta_text.contains("data: [DONE]"));

    let completed = rewriter
        .push_chunk(
            concat!(
                "event: response.completed\n",
                "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_cli_stream_123\",\"object\":\"response\",\"model\":\"gpt-5.4\",\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"id\":\"msg_cli_stream_123\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"Hello Codex\",\"annotations\":[]}]}],\"usage\":{\"input_tokens\":1,\"output_tokens\":2,\"total_tokens\":3}}}\n\n"
            )
            .as_bytes(),
        )
        .expect("rewrite should succeed");
    let completed_text = String::from_utf8(completed).expect("utf8 should decode");
    assert!(completed_text.contains("\"finish_reason\":\"stop\""));
    assert!(completed_text.contains("data: [DONE]"));
    assert!(rewriter.finish().expect("finish should succeed").is_empty());
}

#[test]
fn openai_cli_to_openai_chat_stream_rewriter_converts_completed_event_without_buffering() {
    let report_context = json!({
        "provider_api_format": "openai:cli",
        "client_api_format": "openai:chat",
        "needs_conversion": true,
        "mapped_model": "gpt-5.4",
    });
    let mut rewriter =
        maybe_build_local_stream_rewriter(Some(&report_context)).expect("rewriter should exist");
    let output = rewriter
        .push_chunk(
            concat!(
                "event: response.completed\n",
                "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_cli_stream_123\",\"object\":\"response\",\"model\":\"gpt-5.4\",\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"id\":\"msg_cli_stream_123\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"Hello Codex\",\"annotations\":[]}]}],\"usage\":{\"input_tokens\":1,\"output_tokens\":2,\"total_tokens\":3}}}\n\n"
            )
            .as_bytes(),
        )
        .expect("rewrite should succeed");
    let output_text = String::from_utf8(output).expect("utf8 should decode");
    assert!(output_text.contains("\"object\":\"chat.completion.chunk\""));
    assert!(output_text.contains("\"role\":\"assistant\""));
    assert!(output_text.contains("\"content\":\"Hello Codex\""));
    assert!(output_text.contains("\"finish_reason\":\"stop\""));
    assert!(output_text.contains("data: [DONE]"));
    assert!(rewriter.finish().expect("finish should succeed").is_empty());
}

#[test]
fn antigravity_gemini_to_openai_chat_stream_rewriter_unwraps_and_converts_function_call() {
    let report_context = json!({
        "provider_api_format": "gemini:chat",
        "client_api_format": "openai:chat",
        "needs_conversion": true,
        "has_envelope": true,
        "envelope_name": "antigravity:v1internal",
        "mapped_model": "claude-sonnet-4-5",
    });
    let mut rewriter =
        maybe_build_local_stream_rewriter(Some(&report_context)).expect("rewriter should exist");
    let output = rewriter
        .push_chunk(
            b"data: {\"response\":{\"responseId\":\"resp_antigravity_chat_tool_123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Need a tool.\"},{\"functionCall\":{\"name\":\"get_weather\",\"args\":{\"city\":\"SF\"}}}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"modelVersion\":\"claude-sonnet-4-5\"},\"responseId\":\"resp_antigravity_chat_tool_123\"}\n\n",
        )
        .expect("rewrite should succeed");
    let output_text = utf8(output);
    assert!(output_text.contains("\"object\":\"chat.completion.chunk\""));
    assert!(output_text.contains("\"content\":\"Need a tool.\""));
    assert!(output_text.contains("\"tool_calls\""));
    assert!(output_text.contains("\"name\":\"get_weather\""));
    assert!(output_text.contains("\"finish_reason\":\"tool_calls\""));
    assert!(output_text.contains("data: [DONE]"));
    assert!(rewriter.finish().expect("finish should succeed").is_empty());
}

#[test]
fn antigravity_gemini_to_openai_cli_stream_rewriter_unwraps_and_converts_function_call() {
    let report_context = json!({
        "provider_api_format": "gemini:cli",
        "client_api_format": "openai:cli",
        "needs_conversion": true,
        "envelope_name": "antigravity:v1internal",
        "mapped_model": "claude-sonnet-4-5",
    });
    let mut rewriter =
        maybe_build_local_stream_rewriter(Some(&report_context)).expect("rewriter should exist");
    let output = rewriter
        .push_chunk(
            b"data: {\"response\":{\"responseId\":\"resp_antigravity_cli_tool_123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Need a tool.\"},{\"functionCall\":{\"name\":\"get_weather\",\"args\":{\"city\":\"SF\"}}}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"modelVersion\":\"claude-sonnet-4-5\",\"usageMetadata\":{\"promptTokenCount\":2,\"candidatesTokenCount\":3,\"totalTokenCount\":5}},\"responseId\":\"resp_antigravity_cli_tool_123\"}\n\n",
        )
        .expect("rewrite should succeed");
    let output_text = utf8(output);
    assert!(output_text.contains("event: response.created"));
    assert!(output_text.contains("event: response.output_text.delta"));
    assert!(output_text.contains("event: response.output_item.added"));
    assert!(output_text.contains("event: response.function_call_arguments.delta"));
    assert!(output_text.contains("event: response.completed"));
    assert!(output_text.contains("\"type\":\"function_call\""));
    assert!(output_text.contains("\"name\":\"get_weather\""));
    assert!(rewriter.finish().expect("finish should succeed").is_empty());
}

#[test]
fn gemini_to_openai_cli_stream_rewriter_buffers_and_converts_to_completed_event() {
    let report_context = json!({
        "provider_api_format": "gemini:cli",
        "client_api_format": "openai:cli",
        "needs_conversion": true,
        "mapped_model": "gemini-2.5-pro",
    });
    let mut rewriter =
        maybe_build_local_stream_rewriter(Some(&report_context)).expect("rewriter should exist");
    let first = rewriter
        .push_chunk(
            b"data: {\"responseId\":\"resp_123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello \"}],\"role\":\"model\"},\"index\":0}],\"modelVersion\":\"gemini-2.5-pro\"}\n\n",
        )
        .expect("rewrite should succeed");
    let first_text = utf8(first);
    assert!(first_text.contains("event: response.created"));
    assert!(first_text.contains("event: response.output_text.delta"));
    assert!(first_text.contains("\"delta\":\"Hello \""));
    let second = rewriter
        .push_chunk(
            b"data: {\"responseId\":\"resp_123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Gemini CLI\"}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"modelVersion\":\"gemini-2.5-pro\",\"usageMetadata\":{\"promptTokenCount\":2,\"candidatesTokenCount\":3,\"totalTokenCount\":5}}\n\n",
        )
        .expect("rewrite should succeed");
    let output_text = utf8(second);
    assert!(output_text.contains("event: response.output_text.delta"));
    assert!(output_text.contains("event: response.completed"));
    assert!(output_text.contains("\"type\":\"response.completed\""));
    assert!(output_text.contains("\"object\":\"response\""));
    assert!(output_text.contains("\"delta\":\"Gemini CLI\""));
    assert!(output_text.contains("\"text\":\"Hello Gemini CLI\""));
    assert!(output_text.contains("\"total_tokens\":5"));
    assert!(rewriter.finish().expect("finish should succeed").is_empty());
}

#[test]
fn claude_to_openai_cli_stream_rewriter_buffers_and_converts_to_completed_event() {
    let report_context = json!({
        "provider_api_format": "claude:cli",
        "client_api_format": "openai:cli",
        "needs_conversion": true,
        "mapped_model": "claude-sonnet-4-5",
    });
    let mut rewriter =
        maybe_build_local_stream_rewriter(Some(&report_context)).expect("rewriter should exist");
    let output = rewriter
        .push_chunk(
            concat!(
                "event: message_start\n",
                "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_123\",\"model\":\"claude-sonnet-4-5\"}}\n\n",
                "event: content_block_start\n",
                "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
                "event: content_block_delta\n",
                "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello Claude CLI\"}}\n\n",
                "event: message_delta\n",
                "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"input_tokens\":2,\"output_tokens\":3}}\n\n",
                "event: message_stop\n",
                "data: {\"type\":\"message_stop\"}\n\n"
            )
            .as_bytes(),
        )
        .expect("rewrite should succeed");
    let output_text = utf8(output);
    assert!(output_text.contains("event: response.created"));
    assert!(output_text.contains("event: response.output_text.delta"));
    assert!(output_text.contains("event: response.completed"));
    assert!(output_text.contains("\"type\":\"response.completed\""));
    assert!(output_text.contains("\"object\":\"response\""));
    assert!(output_text.contains("\"text\":\"Hello Claude CLI\""));
    assert!(output_text.contains("\"total_tokens\":5"));
    assert!(rewriter.finish().expect("finish should succeed").is_empty());
}

#[test]
fn claude_to_openai_cli_stream_rewriter_converts_tool_use_to_function_call() {
    let report_context = json!({
        "provider_api_format": "claude:cli",
        "client_api_format": "openai:cli",
        "needs_conversion": true,
        "mapped_model": "claude-sonnet-4-5",
    });
    let mut rewriter =
        maybe_build_local_stream_rewriter(Some(&report_context)).expect("rewriter should exist");
    let output = rewriter
        .push_chunk(
            concat!(
                "event: message_start\n",
                "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_tool_123\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-5\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null}}\n\n",
                "event: content_block_start\n",
                "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"Running tool.\"}}\n\n",
                "event: content_block_stop\n",
                "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
                "event: content_block_start\n",
                "data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"tool_123\",\"name\":\"read_file\",\"input\":{\"path\":\"/tmp/test.txt\"}}}\n\n",
                "event: content_block_stop\n",
                "data: {\"type\":\"content_block_stop\",\"index\":1}\n\n",
                "event: message_delta\n",
                "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"input_tokens\":4,\"output_tokens\":6}}\n\n",
                "event: message_stop\n",
                "data: {\"type\":\"message_stop\"}\n\n"
            )
            .as_bytes(),
        )
        .expect("rewrite should succeed");
    let output_text = utf8(output);
    assert!(output_text.contains("event: response.created"));
    assert!(output_text.contains("event: response.output_text.delta"));
    assert!(output_text.contains("\"delta\":\"Running tool.\""));
    assert!(output_text.contains("event: response.output_item.added"));
    assert!(output_text.contains("event: response.function_call_arguments.delta"));
    assert!(output_text.contains("event: response.completed"));
    assert!(output_text.contains("\"type\":\"response.completed\""));
    assert!(output_text.contains("\"type\":\"function_call\""));
    assert!(output_text.contains("\"call_id\":\"tool_123\""));
    assert!(output_text.contains("\"name\":\"read_file\""));
    assert!(output_text.contains("\\\"path\\\":\\\"/tmp/test.txt\\\""));
    assert!(rewriter.finish().expect("finish should succeed").is_empty());
}

#[test]
fn gemini_to_openai_cli_stream_rewriter_converts_function_call_to_completed_event() {
    let report_context = json!({
        "provider_api_format": "gemini:cli",
        "client_api_format": "openai:cli",
        "needs_conversion": true,
        "mapped_model": "gemini-2.5-pro",
    });
    let mut rewriter =
        maybe_build_local_stream_rewriter(Some(&report_context)).expect("rewriter should exist");
    let output = rewriter
        .push_chunk(
            b"data: {\"responseId\":\"resp_tool_123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Need a tool.\"},{\"functionCall\":{\"name\":\"get_weather\",\"args\":{\"location\":\"Tokyo\"}}}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"modelVersion\":\"gemini-2.5-pro\",\"usageMetadata\":{\"promptTokenCount\":2,\"candidatesTokenCount\":3,\"totalTokenCount\":5}}\n\n",
        )
        .expect("rewrite should succeed");
    let output_text = utf8(output);
    assert!(output_text.contains("event: response.created"));
    assert!(output_text.contains("event: response.output_item.added"));
    assert!(output_text.contains("event: response.function_call_arguments.delta"));
    assert!(output_text.contains("event: response.completed"));
    assert!(output_text.contains("\"type\":\"response.completed\""));
    assert!(output_text.contains("\"type\":\"function_call\""));
    assert!(output_text.contains("\"name\":\"get_weather\""));
    assert!(output_text.contains("\\\"location\\\":\\\"Tokyo\\\""));
    assert!(rewriter.finish().expect("finish should succeed").is_empty());
}

#[test]
fn gemini_to_openai_compact_stream_rewriter_converts_function_call_to_completed_event() {
    let report_context = json!({
        "provider_api_format": "gemini:cli",
        "client_api_format": "openai:compact",
        "needs_conversion": true,
        "mapped_model": "gemini-2.5-pro",
    });
    let mut rewriter =
        maybe_build_local_stream_rewriter(Some(&report_context)).expect("rewriter should exist");
    let output = rewriter
        .push_chunk(
            b"data: {\"responseId\":\"resp_tool_compact_123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Need a tool.\"},{\"functionCall\":{\"name\":\"get_weather\",\"args\":{\"location\":\"Tokyo\"}}}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"modelVersion\":\"gemini-2.5-pro\",\"usageMetadata\":{\"promptTokenCount\":2,\"candidatesTokenCount\":3,\"totalTokenCount\":5}}\n\n",
        )
        .expect("rewrite should succeed");
    let output_text = utf8(output);
    assert!(output_text.contains("event: response.created"));
    assert!(output_text.contains("event: response.output_item.added"));
    assert!(output_text.contains("event: response.function_call_arguments.delta"));
    assert!(output_text.contains("event: response.completed"));
    assert!(output_text.contains("\"type\":\"response.completed\""));
    assert!(output_text.contains("\"type\":\"function_call\""));
    assert!(output_text.contains("\"name\":\"get_weather\""));
    assert!(output_text.contains("\\\"location\\\":\\\"Tokyo\\\""));
    assert!(rewriter.finish().expect("finish should succeed").is_empty());
}

#[test]
fn openai_chat_to_claude_chat_stream_rewriter_converts_via_standard_matrix() {
    let report_context = json!({
        "provider_api_format": "openai:chat",
        "client_api_format": "claude:chat",
        "needs_conversion": true,
        "mapped_model": "gpt-5",
    });
    let mut rewriter =
        maybe_build_local_stream_rewriter(Some(&report_context)).expect("rewriter should exist");
    let first = rewriter
        .push_chunk(
            "data: {\"id\":\"chatcmpl_std_claude_123\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-5\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Hello Claude\"},\"finish_reason\":null}]}\n\n"
            .as_bytes(),
        )
        .expect("rewrite should succeed");
    let first_text = utf8(first);
    assert!(first_text.contains("event: message_start"));
    assert!(first_text.contains("event: content_block_start"));
    assert!(first_text.contains("event: content_block_delta"));
    assert!(first_text.contains("\"text\":\"Hello Claude\""));

    let second = rewriter
        .push_chunk(
            concat!(
                "data: {\"id\":\"chatcmpl_std_claude_123\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-5\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2,\"total_tokens\":3}}\n\n",
                "data: [DONE]\n\n"
            )
            .as_bytes(),
        )
        .expect("rewrite should succeed");
    let output_text = utf8(second);
    assert!(output_text.contains("event: content_block_stop"));
    assert!(output_text.contains("event: message_delta"));
    assert!(output_text.contains("event: message_stop"));
    assert!(rewriter.finish().expect("finish should succeed").is_empty());
}

#[test]
fn openai_chat_to_gemini_cli_stream_rewriter_converts_via_standard_matrix() {
    let report_context = json!({
        "provider_api_format": "openai:chat",
        "client_api_format": "gemini:cli",
        "needs_conversion": true,
        "mapped_model": "gpt-5",
    });
    let mut rewriter =
        maybe_build_local_stream_rewriter(Some(&report_context)).expect("rewriter should exist");
    let first = rewriter
        .push_chunk(
            "data: {\"id\":\"chatcmpl_std_gemini_cli_123\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-5\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Hello Gemini CLI\"},\"finish_reason\":null}]}\n\n"
            .as_bytes(),
        )
        .expect("rewrite should succeed");
    let first_text = utf8(first);
    assert!(first_text.contains("\"responseId\":\"chatcmpl_std_gemini_cli_123\""));
    assert!(first_text.contains("\"candidates\""));
    assert!(first_text.contains("\"text\":\"Hello Gemini CLI\""));

    let second = rewriter
        .push_chunk(
            concat!(
                "data: {\"id\":\"chatcmpl_std_gemini_cli_123\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-5\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":3,\"total_tokens\":5}}\n\n",
                "data: [DONE]\n\n"
            )
            .as_bytes(),
        )
        .expect("rewrite should succeed");
    let output_text = utf8(second);
    assert!(output_text.contains("\"finishReason\":\"STOP\""));
    assert!(output_text.contains("\"totalTokenCount\":5"));
    assert!(rewriter.finish().expect("finish should succeed").is_empty());
}
