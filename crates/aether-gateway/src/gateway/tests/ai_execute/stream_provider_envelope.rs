use super::*;

#[tokio::test]
async fn gateway_executes_kiro_claude_cli_stream_via_local_stream_rewrite() {
    use base64::Engine as _;

    fn crc32(data: &[u8]) -> u32 {
        let mut crc = 0xffff_ffffu32;
        for &byte in data {
            crc ^= byte as u32;
            for _ in 0..8 {
                let mask = if crc & 1 == 1 { 0xedb8_8320 } else { 0 };
                crc = (crc >> 1) ^ mask;
            }
        }
        !crc
    }

    fn encode_string_header(name: &str, value: &str) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(name.len() as u8);
        out.extend_from_slice(name.as_bytes());
        out.push(7);
        out.extend_from_slice(&(value.len() as u16).to_be_bytes());
        out.extend_from_slice(value.as_bytes());
        out
    }

    fn encode_event_frame(
        message_type: &str,
        event_type: Option<&str>,
        payload: serde_json::Value,
    ) -> Vec<u8> {
        let mut headers = encode_string_header(":message-type", message_type);
        if let Some(event_type) = event_type {
            headers.extend_from_slice(&encode_string_header(":event-type", event_type));
        }
        let payload = serde_json::to_vec(&payload).expect("payload should encode");
        let total_len = 12 + headers.len() + payload.len() + 4;
        let mut out = Vec::with_capacity(total_len);
        out.extend_from_slice(&(total_len as u32).to_be_bytes());
        out.extend_from_slice(&(headers.len() as u32).to_be_bytes());
        let prelude_crc = crc32(&out[..8]);
        out.extend_from_slice(&prelude_crc.to_be_bytes());
        out.extend_from_slice(&headers);
        out.extend_from_slice(&payload);
        let message_crc = crc32(&out);
        out.extend_from_slice(&message_crc.to_be_bytes());
        out
    }

    let decision_hits = Arc::new(Mutex::new(0usize));
    let decision_hits_clone = Arc::clone(&decision_hits);
    let plan_hits = Arc::new(Mutex::new(0usize));
    let plan_hits_clone = Arc::clone(&plan_hits);
    let report_hits = Arc::new(Mutex::new(0usize));
    let report_hits_clone = Arc::clone(&report_hits);
    let seen_report = Arc::new(Mutex::new(None::<serde_json::Value>));
    let seen_report_clone = Arc::clone(&seen_report);
    let execute_hits = Arc::new(Mutex::new(0usize));
    let execute_hits_clone = Arc::clone(&execute_hits);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/resolve",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "proxy_public",
                    "route_class": "ai_public",
                    "route_family": "claude",
                    "route_kind": "cli",
                    "auth_endpoint_signature": "claude:cli",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-kiro-cli-stream-123",
                        "api_key_id": "key-kiro-cli-stream-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/messages"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-stream",
            any(move |_request: Request| {
                let decision_hits_inner = Arc::clone(&decision_hits_clone);
                async move {
                    *decision_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({
                        "action": "executor_stream_decision",
                        "decision_kind": "claude_cli_stream",
                        "request_id": "req-kiro-cli-stream-123",
                        "candidate_id": "cand-kiro-cli-stream-123",
                        "provider_name": "kiro",
                        "provider_id": "provider-kiro-cli-stream-123",
                        "endpoint_id": "endpoint-kiro-cli-stream-123",
                        "key_id": "key-kiro-cli-stream-123",
                        "upstream_base_url": "https://kiro.example",
                        "upstream_url": "https://kiro.example/generateAssistantResponse",
                        "auth_header": "authorization",
                        "auth_value": "Bearer upstream-key",
                        "provider_api_format": "claude:cli",
                        "client_api_format": "claude:cli",
                        "model_name": "claude-sonnet-4",
                        "mapped_model": "claude-sonnet-4-upstream",
                        "provider_request_headers": {
                            "content-type": "application/json",
                            "authorization": "Bearer upstream-key",
                            "x-provider-extra": "1"
                        },
                        "provider_request_body": {
                            "model": "claude-sonnet-4-upstream",
                            "messages": [],
                            "stream": true
                        },
                        "content_type": "application/json",
                        "report_kind": "claude_cli_stream_success",
                        "report_context": {
                            "user_id": "user-kiro-cli-stream-123",
                            "api_key_id": "key-kiro-cli-stream-123",
                            "request_id": "req-kiro-cli-stream-123",
                            "provider_name": "kiro",
                            "provider_id": "provider-kiro-cli-stream-123",
                            "endpoint_id": "endpoint-kiro-cli-stream-123",
                            "key_id": "key-kiro-cli-stream-123",
                            "client_api_format": "claude:cli",
                            "provider_api_format": "claude:cli",
                            "model": "claude-sonnet-4",
                            "mapped_model": "claude-sonnet-4-upstream",
                            "has_envelope": true,
                            "envelope_name": "kiro:generateAssistantResponse",
                            "needs_conversion": false,
                            "original_request_body": {
                                "model": "claude-sonnet-4",
                                "messages": [],
                                "stream": true
                            }
                        }
                    }))
                }
            }),
        )
        .route(
            "/api/internal/gateway/plan-sync",
            any(|_request: Request| async move {
                let mut response = Response::builder()
                    .status(StatusCode::CONFLICT)
                    .body(Body::from("{\"action\":\"proxy_public\"}"))
                    .expect("response should build");
                response.headers_mut().insert(
                    HeaderName::from_static(CONTROL_ACTION_HEADER),
                    HeaderValue::from_static(CONTROL_ACTION_PROXY_PUBLIC),
                );
                response
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
                let report_hits_inner = Arc::clone(&report_hits_clone);
                let seen_report_inner = Arc::clone(&seen_report_clone);
                async move {
                    *report_hits_inner.lock().expect("mutex should lock") += 1;
                    let (_parts, body) = request.into_parts();
                    let body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&body).expect("report payload should parse");
                    *seen_report_inner.lock().expect("mutex should lock") = Some(payload);
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
        any(|request: Request| async move {
            let (_parts, body) = request.into_parts();
            let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
            let payload: serde_json::Value =
                serde_json::from_slice(&raw_body).expect("executor payload should parse");
            assert_eq!(
                payload.get("url").and_then(|value| value.as_str()),
                Some("https://kiro.example/generateAssistantResponse")
            );
            let kiro_frames = [
                encode_event_frame(
                    "event",
                    Some("assistantResponseEvent"),
                    json!({"content": "Hello from Kiro"}),
                ),
                encode_event_frame(
                    "event",
                    Some("contextUsageEvent"),
                    json!({"contextUsagePercentage": 1.0}),
                ),
            ]
            .concat();
            let frames = format!(
                concat!(
                    "{{\"type\":\"headers\",\"payload\":{{\"kind\":\"headers\",\"status_code\":200,\"headers\":{{\"content-type\":\"application/vnd.amazon.eventstream\"}}}}}}\n",
                    "{{\"type\":\"data\",\"payload\":{{\"kind\":\"data\",\"chunk_b64\":\"{}\"}}}}\n",
                    "{{\"type\":\"telemetry\",\"payload\":{{\"kind\":\"telemetry\",\"telemetry\":{{\"elapsed_ms\":27,\"upstream_bytes\":64}}}}}}\n",
                    "{{\"type\":\"eof\",\"payload\":{{\"kind\":\"eof\"}}}}\n"
                ),
                base64::engine::general_purpose::STANDARD.encode(kiro_frames)
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
        .post(format!("{gateway_url}/v1/messages"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-kiro-cli-stream-123")
        .header(http::header::AUTHORIZATION, "Bearer client-key")
        .header("anthropic-beta", "output-128k-2025-02-19")
        .body("{\"model\":\"claude-sonnet-4\",\"messages\":[],\"stream\":true}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("text/event-stream")
    );
    let body = response.text().await.expect("body should read");
    assert!(body.contains("event: message_start"));
    assert!(body.contains("\"type\":\"content_block_delta\""));
    assert!(body.contains("Hello from Kiro"));
    assert!(body.contains("\"stop_reason\":\"end_turn\""));
    wait_until(300, || *report_hits.lock().expect("mutex should lock") == 1).await;
    let report_payload = seen_report
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("report-stream should be captured");
    let report_body = base64::engine::general_purpose::STANDARD
        .decode(
            report_payload
                .get("body_base64")
                .and_then(|value| value.as_str())
                .expect("report body should exist"),
        )
        .expect("report body should decode");
    let report_text = String::from_utf8(report_body).expect("report body should be utf8");
    assert!(report_text.contains("Hello from Kiro"));
    assert!(report_text.contains("\"stop_reason\":\"end_turn\""));
    assert_eq!(*decision_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*plan_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_antigravity_gemini_cli_stream_via_executor_decision() {
    use base64::Engine as _;

    let decision_hits = Arc::new(Mutex::new(0usize));
    let decision_hits_clone = Arc::clone(&decision_hits);
    let plan_hits = Arc::new(Mutex::new(0usize));
    let plan_hits_clone = Arc::clone(&plan_hits);
    let report_hits = Arc::new(Mutex::new(0usize));
    let report_hits_clone = Arc::clone(&report_hits);
    let seen_report = Arc::new(Mutex::new(None::<serde_json::Value>));
    let seen_report_clone = Arc::clone(&seen_report);
    let execute_hits = Arc::new(Mutex::new(0usize));
    let execute_hits_clone = Arc::clone(&execute_hits);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/resolve",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "proxy_public",
                    "route_class": "ai_public",
                    "route_family": "gemini",
                    "route_kind": "cli",
                    "auth_endpoint_signature": "gemini:cli",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-antigravity-gemini-cli-stream-decision-123",
                        "api_key_id": "key-antigravity-gemini-cli-stream-decision-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1beta/models/gemini-cli:streamGenerateContent"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-stream",
            any(move |_request: Request| {
                let decision_hits_inner = Arc::clone(&decision_hits_clone);
                async move {
                    *decision_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({
                        "action": "executor_stream_decision",
                        "decision_kind": "gemini_cli_stream",
                        "request_id": "req-antigravity-gemini-cli-stream-decision-123",
                        "candidate_id": "cand-antigravity-gemini-cli-stream-decision-123",
                        "provider_name": "antigravity",
                        "provider_id": "provider-antigravity-gemini-cli-stream-decision-123",
                        "endpoint_id": "endpoint-antigravity-gemini-cli-stream-decision-123",
                        "key_id": "key-antigravity-gemini-cli-stream-decision-123",
                        "upstream_base_url": "https://generativelanguage.googleapis.com",
                        "upstream_url": "https://generativelanguage.googleapis.com/v1beta/models/claude-sonnet-4-5:streamGenerateContent",
                        "auth_header": "authorization",
                        "auth_value": "Bearer upstream-key",
                        "provider_api_format": "gemini:cli",
                        "client_api_format": "gemini:cli",
                        "model_name": "claude-sonnet-4-5",
                        "mapped_model": "claude-sonnet-4-5",
                        "provider_request_headers": {
                            "content-type": "application/json",
                            "accept": "text/event-stream",
                            "authorization": "Bearer upstream-key",
                            "x-provider-extra": "1"
                        },
                        "provider_request_body": {
                            "contents": [],
                            "generationConfig": {"temperature": 0.2}
                        },
                        "content_type": "application/json",
                        "report_kind": "gemini_cli_stream_success",
                        "report_context": {
                            "user_id": "user-antigravity-gemini-cli-stream-decision-123",
                            "api_key_id": "key-antigravity-gemini-cli-stream-decision-123",
                            "provider_name": "antigravity",
                            "provider_id": "provider-antigravity-gemini-cli-stream-decision-123",
                            "endpoint_id": "endpoint-antigravity-gemini-cli-stream-decision-123",
                            "key_id": "key-antigravity-gemini-cli-stream-decision-123",
                            "request_id": "req-antigravity-gemini-cli-stream-decision-123",
                            "client_api_format": "gemini:cli",
                            "provider_api_format": "gemini:cli",
                            "model": "claude-sonnet-4-5",
                            "mapped_model": "claude-sonnet-4-5",
                            "has_envelope": true,
                            "envelope_name": "antigravity:v1internal",
                            "needs_conversion": false
                        }
                    }))
                }
            }),
        )
        .route(
            "/api/internal/gateway/plan-sync",
            any(|_request: Request| async move {
                let mut response = Response::builder()
                    .status(StatusCode::CONFLICT)
                    .body(Body::from("{\"action\":\"proxy_public\"}"))
                    .expect("response should build");
                response.headers_mut().insert(
                    HeaderName::from_static(CONTROL_ACTION_HEADER),
                    HeaderValue::from_static(CONTROL_ACTION_PROXY_PUBLIC),
                );
                response
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
                let report_hits_inner = Arc::clone(&report_hits_clone);
                let seen_report_inner = Arc::clone(&seen_report_clone);
                async move {
                    *report_hits_inner.lock().expect("mutex should lock") += 1;
                    let (_parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("report payload should parse");
                    *seen_report_inner.lock().expect("mutex should lock") = Some(payload);
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
        any(|request: Request| async move {
            let (_parts, body) = request.into_parts();
            let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
            let payload: serde_json::Value =
                serde_json::from_slice(&raw_body).expect("executor payload should parse");
            assert_eq!(
                payload.get("url").and_then(|value| value.as_str()),
                Some(
                    "https://generativelanguage.googleapis.com/v1beta/models/claude-sonnet-4-5:streamGenerateContent"
                )
            );
            assert_eq!(
                payload
                    .get("headers")
                    .and_then(|value| value.get("authorization"))
                    .and_then(|value| value.as_str()),
                Some("Bearer upstream-key")
            );
            let frames = concat!(
                "{\"type\":\"headers\",\"payload\":{\"kind\":\"headers\",\"status_code\":200,\"headers\":{\"content-type\":\"text/event-stream\"}}}\n",
                "{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"data: {\\\"response\\\":{\\\"candidates\\\":[{\\\"content\\\":{\\\"parts\\\":[{\\\"functionCall\\\":{\\\"name\\\":\\\"get_weather\\\",\\\"args\\\":{\\\"city\\\":\\\"SF\\\"}}}],\\\"role\\\":\\\"model\\\"},\\\"index\\\":0}],\\\"modelVersion\\\":\\\"claude-sonnet-4-5\\\"},\\\"responseId\\\":\\\"resp_antigravity_stream_123\\\"}\\n\\n\"}}\n",
                "{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"data: {\\\"response\\\":{\\\"candidates\\\":[{\\\"content\\\":{\\\"parts\\\":[{\\\"text\\\":\\\"Hello Antigravity Stream\\\"}],\\\"role\\\":\\\"model\\\"},\\\"finishReason\\\":\\\"STOP\\\",\\\"index\\\":0}],\\\"modelVersion\\\":\\\"claude-sonnet-4-5\\\",\\\"usageMetadata\\\":{\\\"promptTokenCount\\\":3,\\\"candidatesTokenCount\\\":5,\\\"totalTokenCount\\\":10}},\\\"responseId\\\":\\\"resp_antigravity_stream_123\\\"}\\n\\n\"}}\n",
                "{\"type\":\"telemetry\",\"payload\":{\"kind\":\"telemetry\",\"telemetry\":{\"elapsed_ms\":34,\"upstream_bytes\":26}}}\n",
                "{\"type\":\"eof\",\"payload\":{\"kind\":\"eof\"}}\n"
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
        .post(format!(
            "{gateway_url}/v1beta/models/gemini-cli:streamGenerateContent"
        ))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header("user-agent", "GeminiCLI/1.0")
        .header(http::header::AUTHORIZATION, "Bearer client-key")
        .header(
            TRACE_ID_HEADER,
            "trace-antigravity-gemini-cli-stream-decision-123",
        )
        .body("{\"contents\":[]}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.text().await.expect("body should read"),
        concat!(
            "data: {\"_v1internal_response_id\":\"resp_antigravity_stream_123\",\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"args\":{\"city\":\"SF\"},\"id\":\"call_get_weather_0\",\"name\":\"get_weather\"}}],\"role\":\"model\"},\"index\":0}],\"modelVersion\":\"claude-sonnet-4-5\"}\n\n",
            "data: {\"_v1internal_response_id\":\"resp_antigravity_stream_123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello Antigravity Stream\"}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"modelVersion\":\"claude-sonnet-4-5\",\"usageMetadata\":{\"candidatesTokenCount\":5,\"promptTokenCount\":3,\"totalTokenCount\":10}}\n\n"
        )
    );
    wait_until(300, || *report_hits.lock().expect("mutex should lock") == 1).await;
    let report_payload = seen_report
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("report-stream should be captured");
    let report_body = base64::engine::general_purpose::STANDARD
        .decode(
            report_payload
                .get("body_base64")
                .and_then(|value| value.as_str())
                .expect("report body should exist"),
        )
        .expect("report body should decode");
    let report_text = String::from_utf8(report_body).expect("report body should be utf8");
    assert!(report_text.contains("\"_v1internal_response_id\":\"resp_antigravity_stream_123\""));
    assert!(report_text.contains("\"id\":\"call_get_weather_0\""));
    assert_eq!(*decision_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*plan_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_antigravity_gemini_chat_stream_via_executor_decision() {
    use base64::Engine as _;

    let decision_hits = Arc::new(Mutex::new(0usize));
    let decision_hits_clone = Arc::clone(&decision_hits);
    let plan_hits = Arc::new(Mutex::new(0usize));
    let plan_hits_clone = Arc::clone(&plan_hits);
    let report_hits = Arc::new(Mutex::new(0usize));
    let report_hits_clone = Arc::clone(&report_hits);
    let seen_report = Arc::new(Mutex::new(None::<serde_json::Value>));
    let seen_report_clone = Arc::clone(&seen_report);
    let execute_hits = Arc::new(Mutex::new(0usize));
    let execute_hits_clone = Arc::clone(&execute_hits);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/resolve",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "proxy_public",
                    "route_class": "ai_public",
                    "route_family": "gemini",
                    "route_kind": "chat",
                    "auth_endpoint_signature": "gemini:chat",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-antigravity-gemini-chat-stream-decision-123",
                        "api_key_id": "key-antigravity-gemini-chat-stream-decision-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1beta/models/gemini-2.5-pro:streamGenerateContent"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-stream",
            any(move |_request: Request| {
                let decision_hits_inner = Arc::clone(&decision_hits_clone);
                async move {
                    *decision_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({
                        "action": "executor_stream_decision",
                        "decision_kind": "gemini_chat_stream",
                        "request_id": "req-antigravity-gemini-chat-stream-decision-123",
                        "candidate_id": "cand-antigravity-gemini-chat-stream-decision-123",
                        "provider_name": "antigravity",
                        "provider_id": "provider-antigravity-gemini-chat-stream-decision-123",
                        "endpoint_id": "endpoint-antigravity-gemini-chat-stream-decision-123",
                        "key_id": "key-antigravity-gemini-chat-stream-decision-123",
                        "upstream_base_url": "https://generativelanguage.googleapis.com",
                        "upstream_url": "https://generativelanguage.googleapis.com/v1beta/models/claude-sonnet-4-5:streamGenerateContent",
                        "auth_header": "authorization",
                        "auth_value": "Bearer upstream-key",
                        "provider_api_format": "gemini:chat",
                        "client_api_format": "gemini:chat",
                        "model_name": "claude-sonnet-4-5",
                        "mapped_model": "claude-sonnet-4-5",
                        "provider_request_headers": {
                            "content-type": "application/json",
                            "accept": "text/event-stream",
                            "authorization": "Bearer upstream-key",
                            "x-provider-extra": "1"
                        },
                        "provider_request_body": {
                            "contents": [],
                            "generationConfig": {"temperature": 0.2}
                        },
                        "content_type": "application/json",
                        "report_kind": "gemini_chat_stream_success",
                        "report_context": {
                            "user_id": "user-antigravity-gemini-chat-stream-decision-123",
                            "api_key_id": "key-antigravity-gemini-chat-stream-decision-123",
                            "provider_name": "antigravity",
                            "provider_id": "provider-antigravity-gemini-chat-stream-decision-123",
                            "endpoint_id": "endpoint-antigravity-gemini-chat-stream-decision-123",
                            "key_id": "key-antigravity-gemini-chat-stream-decision-123",
                            "request_id": "req-antigravity-gemini-chat-stream-decision-123",
                            "client_api_format": "gemini:chat",
                            "provider_api_format": "gemini:chat",
                            "model": "claude-sonnet-4-5",
                            "mapped_model": "claude-sonnet-4-5",
                            "has_envelope": true,
                            "envelope_name": "antigravity:v1internal",
                            "needs_conversion": false
                        }
                    }))
                }
            }),
        )
        .route(
            "/api/internal/gateway/plan-sync",
            any(|_request: Request| async move {
                let mut response = Response::builder()
                    .status(StatusCode::CONFLICT)
                    .body(Body::from("{\"action\":\"proxy_public\"}"))
                    .expect("response should build");
                response.headers_mut().insert(
                    HeaderName::from_static(CONTROL_ACTION_HEADER),
                    HeaderValue::from_static(CONTROL_ACTION_PROXY_PUBLIC),
                );
                response
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
                let report_hits_inner = Arc::clone(&report_hits_clone);
                let seen_report_inner = Arc::clone(&seen_report_clone);
                async move {
                    *report_hits_inner.lock().expect("mutex should lock") += 1;
                    let (_parts, body) = request.into_parts();
                    let body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&body).expect("report payload should parse");
                    *seen_report_inner.lock().expect("mutex should lock") = Some(payload);
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
        any(|request: Request| async move {
            let (_parts, body) = request.into_parts();
            let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
            let payload: serde_json::Value =
                serde_json::from_slice(&raw_body).expect("executor payload should parse");
            assert_eq!(
                payload.get("url").and_then(|value| value.as_str()),
                Some(
                    "https://generativelanguage.googleapis.com/v1beta/models/claude-sonnet-4-5:streamGenerateContent"
                )
            );
            assert_eq!(
                payload
                    .get("headers")
                    .and_then(|value| value.get("authorization"))
                    .and_then(|value| value.as_str()),
                Some("Bearer upstream-key")
            );
            let frames = concat!(
                "{\"type\":\"headers\",\"payload\":{\"kind\":\"headers\",\"status_code\":200,\"headers\":{\"content-type\":\"text/event-stream\"}}}\n",
                "{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"data: {\\\"response\\\":{\\\"candidates\\\":[{\\\"content\\\":{\\\"parts\\\":[{\\\"functionCall\\\":{\\\"name\\\":\\\"get_weather\\\",\\\"args\\\":{\\\"city\\\":\\\"SF\\\"}}}],\\\"role\\\":\\\"model\\\"},\\\"index\\\":0}],\\\"modelVersion\\\":\\\"claude-sonnet-4-5\\\"},\\\"responseId\\\":\\\"resp_antigravity_chat_stream_123\\\"}\\n\\n\"}}\n",
                "{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"data: {\\\"response\\\":{\\\"candidates\\\":[{\\\"content\\\":{\\\"parts\\\":[{\\\"text\\\":\\\"Hello Antigravity Chat Stream\\\"}],\\\"role\\\":\\\"model\\\"},\\\"finishReason\\\":\\\"STOP\\\",\\\"index\\\":0}],\\\"modelVersion\\\":\\\"claude-sonnet-4-5\\\",\\\"usageMetadata\\\":{\\\"promptTokenCount\\\":3,\\\"candidatesTokenCount\\\":5,\\\"totalTokenCount\\\":10}},\\\"responseId\\\":\\\"resp_antigravity_chat_stream_123\\\"}\\n\\n\"}}\n",
                "{\"type\":\"telemetry\",\"payload\":{\"kind\":\"telemetry\",\"telemetry\":{\"elapsed_ms\":34,\"upstream_bytes\":26}}}\n",
                "{\"type\":\"eof\",\"payload\":{\"kind\":\"eof\"}}\n"
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
        .post(format!(
            "{gateway_url}/v1beta/models/gemini-2.5-pro:streamGenerateContent"
        ))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(http::header::AUTHORIZATION, "Bearer client-key")
        .header(
            TRACE_ID_HEADER,
            "trace-antigravity-gemini-chat-stream-decision-123",
        )
        .body("{\"contents\":[]}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.text().await.expect("body should read"),
        concat!(
            "data: {\"_v1internal_response_id\":\"resp_antigravity_chat_stream_123\",\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"args\":{\"city\":\"SF\"},\"id\":\"call_get_weather_0\",\"name\":\"get_weather\"}}],\"role\":\"model\"},\"index\":0}],\"modelVersion\":\"claude-sonnet-4-5\"}\n\n",
            "data: {\"_v1internal_response_id\":\"resp_antigravity_chat_stream_123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello Antigravity Chat Stream\"}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"modelVersion\":\"claude-sonnet-4-5\",\"usageMetadata\":{\"candidatesTokenCount\":5,\"promptTokenCount\":3,\"totalTokenCount\":10}}\n\n"
        )
    );
    wait_until(300, || *report_hits.lock().expect("mutex should lock") == 1).await;
    let report_payload = seen_report
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("report-stream should be captured");
    let report_body = base64::engine::general_purpose::STANDARD
        .decode(
            report_payload
                .get("body_base64")
                .and_then(|value| value.as_str())
                .expect("report body should exist"),
        )
        .expect("report body should decode");
    let report_text = String::from_utf8(report_body).expect("report body should be utf8");
    assert!(
        report_text.contains("\"_v1internal_response_id\":\"resp_antigravity_chat_stream_123\"")
    );
    assert!(report_text.contains("\"id\":\"call_get_weather_0\""));
    assert_eq!(*decision_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*plan_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
