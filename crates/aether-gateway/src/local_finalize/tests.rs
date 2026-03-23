use std::collections::BTreeMap;

use serde_json::json;

use super::{
    chat::{
        aggregate_claude_stream_sync_response, aggregate_gemini_stream_sync_response,
        aggregate_openai_chat_stream_sync_response, convert_claude_chat_response_to_openai_chat,
        convert_gemini_chat_response_to_openai_chat,
    },
    cli::{
        aggregate_openai_cli_stream_sync_response, convert_claude_cli_response_to_openai_cli,
        convert_gemini_cli_response_to_openai_cli,
    },
    maybe_build_local_core_sync_finalize_response,
};
use crate::gateway::executor::GatewaySyncReportRequest;
use crate::gateway::GatewayControlDecision;

fn test_decision() -> GatewayControlDecision {
    GatewayControlDecision {
        public_path: "/v1/responses/compact".to_string(),
        public_query_string: None,
        route_class: Some("ai_public".to_string()),
        route_family: Some("openai".to_string()),
        route_kind: Some("compact".to_string()),
        auth_endpoint_signature: Some("openai:compact".to_string()),
        executor_candidate: true,
        auth_context: None,
    }
}

#[test]
fn aggregates_openai_chat_stream_text_chunks_to_final_response() {
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

    let result =
        aggregate_openai_chat_stream_sync_response(body.as_bytes()).expect("result should exist");

    assert_eq!(
        result,
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
}

#[test]
fn aggregates_openai_cli_stream_completed_event_to_final_response() {
    let body = concat!(
        "event: response.created\n",
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_123\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"in_progress\",\"output\":[]}}\n\n",
        "event: response.output_text.delta\n",
        "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"Hello\"}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_123\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":2,\"total_tokens\":3}}}\n\n",
    );

    let result =
        aggregate_openai_cli_stream_sync_response(body.as_bytes()).expect("result should exist");

    assert_eq!(
        result,
        json!({
            "id": "resp_123",
            "object": "response",
            "model": "gpt-5",
            "status": "completed",
            "output": [],
            "usage": {
                "input_tokens": 1,
                "output_tokens": 2,
                "total_tokens": 3,
            },
        })
    );
}

#[test]
fn aggregates_claude_stream_events_to_final_response() {
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

    let result =
        aggregate_claude_stream_sync_response(body.as_bytes()).expect("result should exist");

    assert_eq!(
        result,
        json!({
            "id": "msg_1",
            "type": "message",
            "role": "assistant",
            "model": "claude-3-5-sonnet-latest",
            "content": [{
                "type": "text",
                "text": "hello"
            }],
            "stop_reason": "end_turn",
            "stop_sequence": null,
            "usage": {
                "input_tokens": 5,
                "output_tokens": 7
            }
        })
    );
}

#[test]
fn aggregates_gemini_stream_events_to_final_response() {
    let body = concat!(
        "data: {\"responseId\":\"resp_gemini_123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"he\"}],\"role\":\"model\"},\"index\":0}],\"modelVersion\":\"gemini-1.5-flash\"}\n\n",
        "data: {\"responseId\":\"resp_gemini_123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"hello\"}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"modelVersion\":\"gemini-1.5-flash\",\"usageMetadata\":{\"promptTokenCount\":5,\"candidatesTokenCount\":7,\"totalTokenCount\":12}}\n\n",
    );

    let result =
        aggregate_gemini_stream_sync_response(body.as_bytes()).expect("result should exist");

    assert_eq!(
        result,
        json!({
            "responseId": "resp_gemini_123",
            "candidates": [{
                "content": {
                    "parts": [{"text": "hello"}],
                    "role": "model"
                },
                "finishReason": "STOP",
                "index": 0
            }],
            "modelVersion": "gemini-1.5-flash",
            "usageMetadata": {
                "promptTokenCount": 5,
                "candidatesTokenCount": 7,
                "totalTokenCount": 12
            }
        })
    );
}

#[test]
fn converts_claude_chat_response_to_openai_chat_response() {
    let result = convert_claude_chat_response_to_openai_chat(
        &json!({
            "id": "msg_123",
            "type": "message",
            "role": "assistant",
            "model": "claude-sonnet-4-upstream",
            "content": [
                {"type": "text", "text": "Hello"},
                {"type": "text", "text": " Claude"}
            ],
            "stop_reason": "end_turn",
            "usage": {
                "input_tokens": 5,
                "output_tokens": 7
            }
        }),
        &json!({
            "client_api_format": "openai:chat",
            "provider_api_format": "claude:chat",
            "model": "gpt-5"
        }),
    )
    .expect("result should exist");

    assert_eq!(
        result,
        json!({
            "id": "msg_123",
            "object": "chat.completion",
            "model": "claude-sonnet-4-upstream",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Hello Claude"
                },
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 5,
                "completion_tokens": 7,
                "total_tokens": 12
            }
        })
    );
}

#[test]
fn converts_claude_chat_tool_use_to_openai_chat_tool_calls() {
    let result = convert_claude_chat_response_to_openai_chat(
        &json!({
            "id": "msg_tool_123",
            "type": "message",
            "role": "assistant",
            "model": "claude-sonnet-4-upstream",
            "content": [
                {"type": "text", "text": "Checking."},
                {
                    "type": "tool_use",
                    "id": "toolu_123",
                    "name": "get_weather",
                    "input": {"location": "Tokyo"}
                }
            ],
            "stop_reason": "tool_use",
            "usage": {
                "input_tokens": 5,
                "output_tokens": 7
            }
        }),
        &json!({
            "client_api_format": "openai:chat",
            "provider_api_format": "claude:chat",
            "model": "gpt-5"
        }),
    )
    .expect("result should exist");

    assert_eq!(
        result,
        json!({
            "id": "msg_tool_123",
            "object": "chat.completion",
            "model": "claude-sonnet-4-upstream",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Checking.",
                    "tool_calls": [{
                        "id": "toolu_123",
                        "type": "function",
                        "function": {
                            "name": "get_weather",
                            "arguments": "{\"location\":\"Tokyo\"}"
                        }
                    }]
                },
                "finish_reason": "tool_calls"
            }],
            "usage": {
                "prompt_tokens": 5,
                "completion_tokens": 7,
                "total_tokens": 12
            }
        })
    );
}

#[test]
fn converts_gemini_chat_response_to_openai_chat_response() {
    let result = convert_gemini_chat_response_to_openai_chat(
        &json!({
            "responseId": "resp_123",
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
        }),
        &json!({
            "client_api_format": "openai:chat",
            "provider_api_format": "gemini:chat",
            "model": "gpt-5"
        }),
    )
    .expect("result should exist");

    assert_eq!(
        result,
        json!({
            "id": "resp_123",
            "object": "chat.completion",
            "model": "gemini-2.5-pro-upstream",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Hello Gemini"
                },
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 1,
                "completion_tokens": 2,
                "total_tokens": 3
            }
        })
    );
}

#[test]
fn converts_gemini_chat_function_call_to_openai_chat_tool_calls() {
    let result = convert_gemini_chat_response_to_openai_chat(
        &json!({
            "responseId": "resp_tool_123",
            "candidates": [{
                "content": {
                    "parts": [
                        {"text": "Let me call a tool."},
                        {"functionCall": {"name": "get_weather", "args": {"city": "SF"}}}
                    ],
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
        }),
        &json!({
            "client_api_format": "openai:chat",
            "provider_api_format": "gemini:chat",
            "model": "gpt-5"
        }),
    )
    .expect("result should exist");

    assert_eq!(
        result,
        json!({
            "id": "resp_tool_123",
            "object": "chat.completion",
            "model": "gemini-2.5-pro-upstream",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Let me call a tool.",
                    "tool_calls": [{
                        "id": "call_auto_1",
                        "type": "function",
                        "function": {
                            "name": "get_weather",
                            "arguments": "{\"city\":\"SF\"}"
                        }
                    }]
                },
                "finish_reason": "tool_calls"
            }],
            "usage": {
                "prompt_tokens": 1,
                "completion_tokens": 2,
                "total_tokens": 3
            }
        })
    );
}

#[test]
fn converts_claude_cli_response_to_openai_cli_response() {
    let result = convert_claude_cli_response_to_openai_cli(
        &json!({
            "id": "msg_cli_123",
            "type": "message",
            "role": "assistant",
            "model": "claude-code-upstream",
            "content": [
                {"type": "text", "text": "Hello"},
                {"type": "text", "text": " Claude CLI"}
            ],
            "usage": {
                "input_tokens": 4,
                "output_tokens": 6
            }
        }),
        &json!({
            "client_api_format": "openai:cli",
            "provider_api_format": "claude:cli",
            "model": "gpt-5"
        }),
    )
    .expect("result should exist");

    assert_eq!(
        result,
        json!({
            "id": "msg_cli_123",
            "object": "response",
            "status": "completed",
            "model": "claude-code-upstream",
            "output": [{
                "type": "message",
                "id": "msg_cli_123_msg",
                "role": "assistant",
                "status": "completed",
                "content": [{
                    "type": "output_text",
                    "text": "Hello Claude CLI",
                    "annotations": []
                }]
            }],
            "usage": {
                "input_tokens": 4,
                "output_tokens": 6,
                "total_tokens": 10
            }
        })
    );
}

#[test]
fn converts_claude_cli_tool_use_to_openai_cli_function_call() {
    let result = convert_claude_cli_response_to_openai_cli(
        &json!({
            "id": "msg_cli_tool_123",
            "type": "message",
            "role": "assistant",
            "model": "claude-code-upstream",
            "content": [
                {"type": "text", "text": "Running tool."},
                {
                    "type": "tool_use",
                    "id": "tool_123",
                    "name": "read_file",
                    "input": {"path": "/tmp/test.txt"}
                }
            ],
            "usage": {
                "input_tokens": 4,
                "output_tokens": 6
            }
        }),
        &json!({
            "client_api_format": "openai:cli",
            "provider_api_format": "claude:cli",
            "model": "gpt-5"
        }),
    )
    .expect("result should exist");

    assert_eq!(
        result,
        json!({
            "id": "msg_cli_tool_123",
            "object": "response",
            "status": "completed",
            "model": "claude-code-upstream",
            "output": [
                {
                    "type": "message",
                    "id": "msg_cli_tool_123_msg",
                    "role": "assistant",
                    "status": "completed",
                    "content": [{
                        "type": "output_text",
                        "text": "Running tool.",
                        "annotations": []
                    }]
                },
                {
                    "type": "function_call",
                    "call_id": "tool_123",
                    "name": "read_file",
                    "arguments": "{\"path\":\"/tmp/test.txt\"}"
                }
            ],
            "usage": {
                "input_tokens": 4,
                "output_tokens": 6,
                "total_tokens": 10
            }
        })
    );
}

#[test]
fn converts_gemini_cli_response_to_openai_cli_response() {
    let result = convert_gemini_cli_response_to_openai_cli(
        &json!({
            "responseId": "resp_cli_123",
            "candidates": [{
                "content": {
                    "parts": [{"text": "Hello Gemini CLI"}],
                    "role": "model"
                },
                "finishReason": "STOP",
                "index": 0
            }],
            "modelVersion": "gemini-cli-upstream",
            "usageMetadata": {
                "promptTokenCount": 3,
                "candidatesTokenCount": 5,
                "thoughtsTokenCount": 2,
                "totalTokenCount": 10
            }
        }),
        &json!({
            "client_api_format": "openai:cli",
            "provider_api_format": "gemini:cli",
            "model": "gpt-5"
        }),
    )
    .expect("result should exist");

    assert_eq!(
        result,
        json!({
            "id": "resp_cli_123",
            "object": "response",
            "status": "completed",
            "model": "gemini-cli-upstream",
            "output": [{
                "type": "message",
                "id": "resp_cli_123_msg",
                "role": "assistant",
                "status": "completed",
                "content": [{
                    "type": "output_text",
                    "text": "Hello Gemini CLI",
                    "annotations": []
                }]
            }],
            "usage": {
                "input_tokens": 3,
                "output_tokens": 7,
                "total_tokens": 10
            }
        })
    );
}

#[test]
fn converts_gemini_cli_function_call_to_openai_cli_function_call() {
    let result = convert_gemini_cli_response_to_openai_cli(
        &json!({
            "responseId": "resp_cli_tool_123",
            "candidates": [{
                "content": {
                    "parts": [
                        {"text": "Need a tool."},
                        {"functionCall": {"name": "get_weather", "args": {"location": "Tokyo"}}}
                    ],
                    "role": "model"
                },
                "finishReason": "STOP",
                "index": 0
            }],
            "modelVersion": "gemini-cli-upstream",
            "usageMetadata": {
                "promptTokenCount": 3,
                "candidatesTokenCount": 5,
                "thoughtsTokenCount": 2,
                "totalTokenCount": 10
            }
        }),
        &json!({
            "client_api_format": "openai:cli",
            "provider_api_format": "gemini:cli",
            "model": "gpt-5"
        }),
    )
    .expect("result should exist");

    assert_eq!(
        result,
        json!({
            "id": "resp_cli_tool_123",
            "object": "response",
            "status": "completed",
            "model": "gemini-cli-upstream",
            "output": [
                {
                    "type": "message",
                    "id": "resp_cli_tool_123_msg",
                    "role": "assistant",
                    "status": "completed",
                    "content": [{
                        "type": "output_text",
                        "text": "Need a tool.",
                        "annotations": []
                    }]
                },
                {
                    "type": "function_call",
                    "call_id": "call_auto_1",
                    "name": "get_weather",
                    "arguments": "{\"location\":\"Tokyo\"}"
                }
            ],
            "usage": {
                "input_tokens": 3,
                "output_tokens": 7,
                "total_tokens": 10
            }
        })
    );
}

#[test]
fn local_finalize_handles_openai_compact_cross_format_sync_response() {
    let payload = GatewaySyncReportRequest {
        trace_id: "trace-compact-sync-123".to_string(),
        report_kind: "openai_compact_sync_finalize".to_string(),
        report_context: Some(json!({
            "client_api_format": "openai:compact",
            "provider_api_format": "gemini:cli",
            "model": "gpt-5",
            "needs_conversion": true,
            "has_envelope": false,
        })),
        status_code: 200,
        headers: BTreeMap::from([("content-type".to_string(), "application/json".to_string())]),
        body_json: Some(json!({
            "responseId": "resp_cli_123",
            "candidates": [{
                "content": {
                    "parts": [{"text": "Hello Gemini CLI"}],
                    "role": "model"
                },
                "finishReason": "STOP",
                "index": 0
            }],
            "modelVersion": "gemini-cli-upstream",
            "usageMetadata": {
                "promptTokenCount": 3,
                "candidatesTokenCount": 5,
                "thoughtsTokenCount": 2,
                "totalTokenCount": 10
            }
        })),
        client_body_json: None,
        body_base64: None,
        telemetry: None,
    };

    let outcome = maybe_build_local_core_sync_finalize_response(
        "trace-compact-sync-123",
        &test_decision(),
        &payload,
    )
    .expect("local finalize should succeed")
    .expect("local finalize should match");

    assert_eq!(outcome.response.status(), 200);
    let report = outcome
        .background_report
        .expect("compact cross-format should downgrade to success report");
    assert_eq!(report.report_kind, "openai_cli_sync_success");
    assert_eq!(
        report.client_body_json.expect("client body should exist")["object"],
        "response"
    );
}

#[test]
fn local_finalize_handles_openai_compact_cross_format_function_call_response() {
    let payload = GatewaySyncReportRequest {
        trace_id: "trace-compact-tool-123".to_string(),
        report_kind: "openai_compact_sync_finalize".to_string(),
        report_context: Some(json!({
            "client_api_format": "openai:compact",
            "provider_api_format": "gemini:cli",
            "model": "gpt-5",
            "needs_conversion": true,
            "has_envelope": false,
        })),
        status_code: 200,
        headers: BTreeMap::from([("content-type".to_string(), "application/json".to_string())]),
        body_json: Some(json!({
            "responseId": "resp_cli_tool_123",
            "candidates": [{
                "content": {
                    "parts": [
                        {"text": "Need a tool."},
                        {"functionCall": {"name": "get_weather", "args": {"location": "Tokyo"}}}
                    ],
                    "role": "model"
                },
                "finishReason": "STOP",
                "index": 0
            }],
            "modelVersion": "gemini-cli-upstream",
            "usageMetadata": {
                "promptTokenCount": 3,
                "candidatesTokenCount": 5,
                "thoughtsTokenCount": 2,
                "totalTokenCount": 10
            }
        })),
        client_body_json: None,
        body_base64: None,
        telemetry: None,
    };

    let outcome = maybe_build_local_core_sync_finalize_response(
        "trace-compact-tool-123",
        &test_decision(),
        &payload,
    )
    .expect("local finalize should succeed")
    .expect("local finalize should match");

    let report = outcome
        .background_report
        .expect("compact tool-call should downgrade to success report");
    let client_body = report.client_body_json.expect("client body should exist");
    assert_eq!(client_body["object"], "response");
    assert_eq!(client_body["output"][1]["type"], "function_call");
}
