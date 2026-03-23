use super::*;

#[tokio::test]
async fn gateway_executes_openai_chat_antigravity_gemini_cross_format_function_call_stream_via_local_stream_rewrite(
) {
    use base64::Engine as _;

    #[derive(Debug, Clone)]
    struct SeenReportStreamRequest {
        report_kind: String,
        body: String,
    }

    let seen_report = Arc::new(Mutex::new(None::<SeenReportStreamRequest>));
    let seen_report_clone = Arc::clone(&seen_report);
    let plan_hits = Arc::new(Mutex::new(0usize));
    let plan_hits_clone = Arc::clone(&plan_hits);
    let execute_hits = Arc::new(Mutex::new(0usize));
    let execute_hits_clone = Arc::clone(&execute_hits);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/resolve",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "proxy_public",
                    "route_class": "ai_public",
                    "route_family": "openai",
                    "route_kind": "chat",
                    "auth_endpoint_signature": "openai:chat",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-openai-chat-stream-antigravity-gemini-tool-123",
                        "api_key_id": "key-openai-chat-stream-antigravity-gemini-tool-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/chat/completions"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-stream",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_stream_decision",
                    "decision_kind": "openai_chat_stream",
                    "request_id": "req-openai-chat-stream-antigravity-gemini-tool-123",
                    "candidate_id": "cand-openai-chat-stream-antigravity-gemini-tool-123",
                    "provider_name": "antigravity",
                    "provider_id": "provider-openai-chat-stream-antigravity-gemini-tool-123",
                    "endpoint_id": "endpoint-openai-chat-stream-antigravity-gemini-tool-123",
                    "key_id": "key-openai-chat-stream-antigravity-gemini-tool-123",
                    "upstream_base_url": "https://generativelanguage.googleapis.com",
                    "upstream_url": "https://generativelanguage.googleapis.com/v1beta/models/claude-sonnet-4-5:streamGenerateContent?alt=sse",
                    "auth_header": "authorization",
                    "auth_value": "Bearer upstream-key",
                    "provider_api_format": "gemini:chat",
                    "client_api_format": "openai:chat",
                    "model_name": "gpt-5",
                    "mapped_model": "claude-sonnet-4-5",
                    "provider_request_headers": {
                        "content-type": "application/json",
                        "authorization": "Bearer upstream-key",
                        "accept": "text/event-stream"
                    },
                    "provider_request_body": {
                        "contents": [{"role": "user", "parts": [{"text": "weather"}]}]
                    },
                    "content_type": "application/json",
                    "report_kind": "openai_chat_stream_success",
                    "report_context": {
                        "user_id": "user-openai-chat-stream-antigravity-gemini-tool-123",
                        "api_key_id": "key-openai-chat-stream-antigravity-gemini-tool-123",
                        "request_id": "req-openai-chat-stream-antigravity-gemini-tool-123",
                        "model": "gpt-5",
                        "provider_name": "antigravity",
                        "provider_id": "provider-openai-chat-stream-antigravity-gemini-tool-123",
                        "endpoint_id": "endpoint-openai-chat-stream-antigravity-gemini-tool-123",
                        "key_id": "key-openai-chat-stream-antigravity-gemini-tool-123",
                        "provider_api_format": "gemini:chat",
                        "client_api_format": "openai:chat",
                        "mapped_model": "claude-sonnet-4-5",
                        "has_envelope": true,
                        "envelope_name": "antigravity:v1internal",
                        "needs_conversion": true
                    }
                }))
            }),
        )
        .route(
            "/api/internal/gateway/plan-stream",
            any(move |_request: Request| {
                let plan_hits_inner = Arc::clone(&plan_hits_clone);
                async move {
                    *plan_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({"action": "proxy_public"}))
                }
            }),
        )
        .route(
            "/api/internal/gateway/report-stream",
            any(move |request: Request| {
                let seen_report_inner = Arc::clone(&seen_report_clone);
                async move {
                    let (_parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("report payload should parse");
                    let encoded_body = payload
                        .get("body_base64")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default();
                    let decoded_body = base64::engine::general_purpose::STANDARD
                        .decode(encoded_body)
                        .expect("stream body should decode");
                    *seen_report_inner.lock().expect("mutex should lock") =
                        Some(SeenReportStreamRequest {
                            report_kind: payload
                                .get("report_kind")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            body: String::from_utf8(decoded_body).expect("body should be utf8"),
                        });
                    Json(json!({"ok": true}))
                }
            }),
        )
        .route(
            "/api/internal/gateway/execute-stream",
            any(move |_request: Request| {
                let execute_hits_inner = Arc::clone(&execute_hits_clone);
                async move {
                    *execute_hits_inner.lock().expect("mutex should lock") += 1;
                    let mut response = Response::builder()
                        .status(StatusCode::OK)
                        .body(Body::from("fallback-stream"))
                        .expect("response should build");
                    response.headers_mut().insert(
                        http::header::CONTENT_TYPE,
                        HeaderValue::from_static("text/plain"),
                    );
                    response.headers_mut().insert(
                        HeaderName::from_static(CONTROL_EXECUTED_HEADER),
                        HeaderValue::from_static("true"),
                    );
                    response
                }
            }),
        );

    let executor = Router::new().route(
        "/v1/execute/stream",
        any(|_request: Request| async move {
            let gemini_stream = concat!(
                "data: {\"response\":{\"responseId\":\"resp_antigravity_chat_tool_stream_123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Need a tool.\"},{\"functionCall\":{\"name\":\"get_weather\",\"args\":{\"city\":\"SF\"}}}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"modelVersion\":\"claude-sonnet-4-5\"},\"responseId\":\"resp_antigravity_chat_tool_stream_123\"}\n\n"
            );
            let frames = format!(
                concat!(
                    "{{\"type\":\"headers\",\"payload\":{{\"kind\":\"headers\",\"status_code\":200,\"headers\":{{\"content-type\":\"text/event-stream\"}}}}}}\n",
                    "{{\"type\":\"data\",\"payload\":{{\"kind\":\"data\",\"text\":{}}}}}\n",
                    "{{\"type\":\"telemetry\",\"payload\":{{\"kind\":\"telemetry\",\"telemetry\":{{\"elapsed_ms\":41,\"ttfb_ms\":12,\"upstream_bytes\":31}}}}}}\n",
                    "{{\"type\":\"eof\",\"payload\":{{\"kind\":\"eof\"}}}}\n"
                ),
                serde_json::to_string(gemini_stream).expect("stream should encode"),
            );
            let mut response = Response::builder()
                .status(StatusCode::OK)
                .body(Body::from(frames))
                .expect("response should build");
            response.headers_mut().insert(
                http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/x-ndjson"),
            );
            response
        }),
    );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let (executor_url, executor_handle) = start_server(executor).await;
    let gateway = build_router_with_endpoints(
        upstream_url.clone(),
        Some(upstream_url.clone()),
        Some(executor_url.clone()),
    )
    .expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(
            TRACE_ID_HEADER,
            "trace-openai-chat-stream-antigravity-gemini-tool-123",
        )
        .body("{\"model\":\"gpt-5\",\"messages\":[],\"stream\":true}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let response_text = response.text().await.expect("body should read");
    assert!(response_text.contains("\"object\":\"chat.completion.chunk\""));
    assert!(response_text.contains("\"tool_calls\":[{"));
    assert!(response_text.contains("\"name\":\"get_weather\""));
    assert!(response_text.contains("\"finish_reason\":\"tool_calls\""));
    assert!(response_text.contains("data: [DONE]"));

    wait_until(300, || {
        seen_report
            .lock()
            .expect("mutex should lock")
            .as_ref()
            .is_some()
    })
    .await;
    let seen_report_request = seen_report
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("report-stream should be captured");
    assert_eq!(
        seen_report_request.report_kind,
        "openai_chat_stream_success"
    );
    assert!(seen_report_request.body.contains("\"tool_calls\":[{"));
    assert_eq!(*plan_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
