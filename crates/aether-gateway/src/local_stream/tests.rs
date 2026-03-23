use serde_json::json;

use super::maybe_build_local_stream_rewriter;

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
    assert!(first.is_empty());
    let second = rewriter
        .push_chunk(
            b"data: {\"responseId\":\"resp_123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Gemini\"}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"modelVersion\":\"gemini-2.5-pro\"}\n\n",
        )
        .expect("rewrite should succeed");
    assert!(second.is_empty());
    let output_text = String::from_utf8(rewriter.finish().expect("finish should succeed"))
        .expect("utf8 should decode");
    assert!(output_text.contains("\"object\":\"chat.completion.chunk\""));
    assert!(output_text.contains("\"role\":\"assistant\""));
    assert!(output_text.contains("\"content\":\"Gemini\""));
    assert!(output_text.contains("\"finish_reason\":\"stop\""));
    assert!(output_text.contains("data: [DONE]"));
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
    assert!(output.is_empty());
    let output_text = String::from_utf8(rewriter.finish().expect("finish should succeed"))
        .expect("utf8 should decode");
    assert!(output_text.contains("\"object\":\"chat.completion.chunk\""));
    assert!(output_text.contains("\"role\":\"assistant\""));
    assert!(output_text.contains("\"content\":\"Need a tool.\""));
    assert!(output_text.contains("\"tool_calls\":[{"));
    assert!(output_text.contains("\"name\":\"get_weather\""));
    assert!(output_text.contains("\\\"city\\\":\\\"SF\\\""));
    assert!(output_text.contains("\"finish_reason\":\"tool_calls\""));
    assert!(output_text.contains("data: [DONE]"));
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
    assert!(output.is_empty());
    let output_text = String::from_utf8(rewriter.finish().expect("finish should succeed"))
        .expect("utf8 should decode");
    assert!(output_text.contains("\"object\":\"chat.completion.chunk\""));
    assert!(output_text.contains("\"tool_calls\""));
    assert!(output_text.contains("\"name\":\"get_weather\""));
    assert!(output_text.contains("\"finish_reason\":\"tool_calls\""));
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
    assert!(output.is_empty());
    let output_text = String::from_utf8(rewriter.finish().expect("finish should succeed"))
        .expect("utf8 should decode");
    assert!(output_text.contains("event: response.completed"));
    assert!(output_text.contains("\"type\":\"function_call\""));
    assert!(output_text.contains("\"name\":\"get_weather\""));
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
    assert!(first.is_empty());
    let second = rewriter
        .push_chunk(
            b"data: {\"responseId\":\"resp_123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Gemini CLI\"}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"modelVersion\":\"gemini-2.5-pro\",\"usageMetadata\":{\"promptTokenCount\":2,\"candidatesTokenCount\":3,\"totalTokenCount\":5}}\n\n",
        )
        .expect("rewrite should succeed");
    assert!(second.is_empty());
    let output_text = String::from_utf8(rewriter.finish().expect("finish should succeed"))
        .expect("utf8 should decode");
    assert!(output_text.contains("event: response.completed"));
    assert!(output_text.contains("\"type\":\"response.completed\""));
    assert!(output_text.contains("\"object\":\"response\""));
    assert!(output_text.contains("\"text\":\"Gemini CLI\""));
    assert!(output_text.contains("\"total_tokens\":5"));
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
    assert!(output.is_empty());
    let output_text = String::from_utf8(rewriter.finish().expect("finish should succeed"))
        .expect("utf8 should decode");
    assert!(output_text.contains("event: response.completed"));
    assert!(output_text.contains("\"type\":\"response.completed\""));
    assert!(output_text.contains("\"object\":\"response\""));
    assert!(output_text.contains("\"text\":\"Hello Claude CLI\""));
    assert!(output_text.contains("\"total_tokens\":5"));
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
    assert!(output.is_empty());
    let output_text = String::from_utf8(rewriter.finish().expect("finish should succeed"))
        .expect("utf8 should decode");
    assert!(output_text.contains("event: response.completed"));
    assert!(output_text.contains("\"type\":\"response.completed\""));
    assert!(output_text.contains("\"type\":\"function_call\""));
    assert!(output_text.contains("\"call_id\":\"tool_123\""));
    assert!(output_text.contains("\"name\":\"read_file\""));
    assert!(output_text.contains("\\\"path\\\":\\\"/tmp/test.txt\\\""));
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
    assert!(output.is_empty());
    let output_text = String::from_utf8(rewriter.finish().expect("finish should succeed"))
        .expect("utf8 should decode");
    assert!(output_text.contains("event: response.completed"));
    assert!(output_text.contains("\"type\":\"response.completed\""));
    assert!(output_text.contains("\"type\":\"function_call\""));
    assert!(output_text.contains("\"name\":\"get_weather\""));
    assert!(output_text.contains("\\\"location\\\":\\\"Tokyo\\\""));
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
    assert!(output.is_empty());
    let output_text = String::from_utf8(rewriter.finish().expect("finish should succeed"))
        .expect("utf8 should decode");
    assert!(output_text.contains("event: response.completed"));
    assert!(output_text.contains("\"type\":\"response.completed\""));
    assert!(output_text.contains("\"type\":\"function_call\""));
    assert!(output_text.contains("\"name\":\"get_weather\""));
    assert!(output_text.contains("\\\"location\\\":\\\"Tokyo\\\""));
}
