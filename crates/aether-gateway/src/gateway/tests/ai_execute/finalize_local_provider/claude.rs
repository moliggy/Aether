use super::*;

#[tokio::test]
async fn gateway_executes_claude_chat_sync_upstream_stream_via_local_finalize_response() {
    use base64::Engine as _;

    #[derive(Debug, Clone)]
    struct SeenFinalizeSyncRequest;

    let seen_finalize = Arc::new(Mutex::new(None::<SeenFinalizeSyncRequest>));
    let seen_finalize_clone = Arc::clone(&seen_finalize);
    let report_hits = Arc::new(Mutex::new(0usize));
    let report_hits_clone = Arc::clone(&report_hits);
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
                    "route_kind": "chat",
                    "auth_endpoint_signature": "claude:chat",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-claude-chat-stream-sync-direct-123",
                        "api_key_id": "key-claude-chat-stream-sync-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/messages"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync_decision",
                    "decision_kind": "claude_chat_sync",
                    "request_id": "req-claude-chat-stream-sync-direct-123",
                    "candidate_id": "cand-claude-chat-stream-sync-direct-123",
                    "provider_name": "claude",
                    "provider_id": "provider-claude-chat-stream-sync-direct-123",
                    "endpoint_id": "endpoint-claude-chat-stream-sync-direct-123",
                    "key_id": "key-claude-chat-stream-sync-direct-123",
                    "upstream_base_url": "https://api.anthropic.example",
                    "upstream_url": "https://api.anthropic.example/v1/messages",
                    "auth_header": "x-api-key",
                    "auth_value": "upstream-secret",
                    "provider_api_format": "claude:chat",
                    "client_api_format": "claude:chat",
                    "model_name": "claude-sonnet-4",
                    "mapped_model": "claude-sonnet-4-upstream",
                    "provider_request_headers": {
                        "content-type": "application/json",
                        "accept": "text/event-stream",
                        "x-api-key": "upstream-secret"
                    },
                    "provider_request_body": {
                        "model": "claude-sonnet-4-upstream",
                        "messages": [],
                        "stream": true
                    },
                    "content_type": "application/json",
                    "upstream_is_stream": true,
                    "report_kind": "claude_chat_sync_finalize",
                    "report_context": {
                        "user_id": "user-claude-chat-stream-sync-direct-123",
                        "api_key_id": "key-claude-chat-stream-sync-direct-123",
                        "provider_id": "provider-claude-chat-stream-sync-direct-123",
                        "endpoint_id": "endpoint-claude-chat-stream-sync-direct-123",
                        "key_id": "key-claude-chat-stream-sync-direct-123",
                        "client_api_format": "claude:chat",
                        "provider_api_format": "claude:chat",
                        "request_id": "req-claude-chat-stream-sync-direct-123",
                        "model": "claude-sonnet-4",
                        "has_envelope": false,
                        "needs_conversion": false
                    }
                }))
            }),
        )
        .route(
            "/api/internal/gateway/finalize-sync",
            any(move |request: Request| {
                let seen_finalize_inner = Arc::clone(&seen_finalize_clone);
                async move {
                    let (parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value = serde_json::from_slice(&raw_body)
                        .expect("finalize payload should parse");
                    let _ = parts;
                    let _ = payload;
                    *seen_finalize_inner.lock().expect("mutex should lock") =
                        Some(SeenFinalizeSyncRequest);
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    let mut response = Response::builder()
                        .status(StatusCode::OK)
                        .body(Body::from(
                            "{\"id\":\"ignored-claude-finalize-response\",\"type\":\"message\",\"content\":[]}",
                        ))
                        .expect("response should build");
                    response.headers_mut().insert(
                        http::header::CONTENT_TYPE,
                        HeaderValue::from_static("application/json"),
                    );
                    response.headers_mut().insert(
                        HeaderName::from_static(CONTROL_EXECUTED_HEADER),
                        HeaderValue::from_static("true"),
                    );
                    response
                }
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(move |_request: Request| {
                let report_hits_inner = Arc::clone(&report_hits_clone);
                async move {
                    *report_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({"ok": true}))
                }
            }),
        )
        .route(
            "/api/internal/gateway/execute-sync",
            any(move |_request: Request| {
                let execute_hits_inner = Arc::clone(&execute_hits_clone);
                async move {
                    *execute_hits_inner.lock().expect("mutex should lock") += 1;
                    let mut response = Response::builder()
                        .status(StatusCode::CREATED)
                        .body(Body::from("{\"fallback\":true}"))
                        .expect("response should build");
                    response.headers_mut().insert(
                        http::header::CONTENT_TYPE,
                        HeaderValue::from_static("application/json"),
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
        "/v1/execute/sync",
        any(|_request: Request| async move {
            Json(json!({
                "request_id": "req-claude-chat-stream-sync-direct-123",
                "status_code": 200,
                "headers": {
                    "content-type": "text/event-stream"
                },
                "body": {
                    "body_bytes_b64": base64::engine::general_purpose::STANDARD.encode(
                        concat!(
                            "event: message_start\n",
                            "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_claude_sync_123\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-upstream\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null}}\n\n",
                            "event: content_block_start\n",
                            "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
                            "event: content_block_delta\n",
                            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}\n\n",
                            "event: content_block_delta\n",
                            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\" Claude\"}}\n\n",
                            "event: content_block_stop\n",
                            "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
                            "event: message_delta\n",
                            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"input_tokens\":5,\"output_tokens\":7}}\n\n",
                            "event: message_stop\n",
                            "data: {\"type\":\"message_stop\"}\n\n"
                        )
                    )
                },
                "telemetry": {
                    "elapsed_ms": 34
                }
            }))
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

    let started_at = std::time::Instant::now();
    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/messages"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-claude-chat-stream-sync-direct-123")
        .body("{\"model\":\"claude-sonnet-4\",\"messages\":[]}")
        .send()
        .await
        .expect("request should succeed");
    let elapsed = started_at.elapsed();

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "id": "msg_claude_sync_123",
            "type": "message",
            "role": "assistant",
            "model": "claude-sonnet-4-upstream",
            "content": [{
                "type": "text",
                "text": "Hello Claude"
            }],
            "stop_reason": "end_turn",
            "stop_sequence": null,
            "usage": {
                "input_tokens": 5,
                "output_tokens": 7
            }
        })
    );
    assert!(
        elapsed < std::time::Duration::from_millis(350),
        "response should not wait for finalize-sync background task"
    );

    wait_until(700, || *report_hits.lock().expect("mutex should lock") == 1).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        seen_finalize.lock().expect("mutex should lock").is_none(),
        "finalize-sync should not be called when local finalize can downgrade to success report"
    );
    assert_eq!(*report_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_claude_cli_sync_upstream_stream_via_local_finalize_response() {
    use base64::Engine as _;

    #[derive(Debug, Clone)]
    struct SeenFinalizeSyncRequest;

    let seen_finalize = Arc::new(Mutex::new(None::<SeenFinalizeSyncRequest>));
    let seen_finalize_clone = Arc::clone(&seen_finalize);
    let report_hits = Arc::new(Mutex::new(0usize));
    let report_hits_clone = Arc::clone(&report_hits);
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
                        "user_id": "user-claude-cli-stream-sync-direct-123",
                        "api_key_id": "key-claude-cli-stream-sync-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/messages"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync_decision",
                    "decision_kind": "claude_cli_sync",
                    "request_id": "req-claude-cli-stream-sync-direct-123",
                    "candidate_id": "cand-claude-cli-stream-sync-direct-123",
                    "provider_name": "claude",
                    "provider_id": "provider-claude-cli-stream-sync-direct-123",
                    "endpoint_id": "endpoint-claude-cli-stream-sync-direct-123",
                    "key_id": "key-claude-cli-stream-sync-direct-123",
                    "upstream_base_url": "https://api.anthropic.example",
                    "upstream_url": "https://api.anthropic.example/v1/messages",
                    "auth_header": "x-api-key",
                    "auth_value": "upstream-secret",
                    "provider_api_format": "claude:cli",
                    "client_api_format": "claude:cli",
                    "model_name": "claude-code",
                    "mapped_model": "claude-code-upstream",
                    "provider_request_headers": {
                        "content-type": "application/json",
                        "accept": "text/event-stream",
                        "x-api-key": "upstream-secret"
                    },
                    "provider_request_body": {
                        "model": "claude-code-upstream",
                        "messages": [],
                        "stream": true
                    },
                    "content_type": "application/json",
                    "upstream_is_stream": true,
                    "report_kind": "claude_cli_sync_finalize",
                    "report_context": {
                        "user_id": "user-claude-cli-stream-sync-direct-123",
                        "api_key_id": "key-claude-cli-stream-sync-direct-123",
                        "provider_id": "provider-claude-cli-stream-sync-direct-123",
                        "endpoint_id": "endpoint-claude-cli-stream-sync-direct-123",
                        "key_id": "key-claude-cli-stream-sync-direct-123",
                        "client_api_format": "claude:cli",
                        "provider_api_format": "claude:cli",
                        "request_id": "req-claude-cli-stream-sync-direct-123",
                        "model": "claude-code",
                        "has_envelope": true,
                        "envelope_name": "claude:cli",
                        "needs_conversion": false
                    }
                }))
            }),
        )
        .route(
            "/api/internal/gateway/finalize-sync",
            any(move |request: Request| {
                let seen_finalize_inner = Arc::clone(&seen_finalize_clone);
                async move {
                    let (parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value = serde_json::from_slice(&raw_body)
                        .expect("finalize payload should parse");
                    let _ = parts;
                    let _ = payload;
                    *seen_finalize_inner.lock().expect("mutex should lock") =
                        Some(SeenFinalizeSyncRequest);
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    let mut response = Response::builder()
                        .status(StatusCode::OK)
                        .body(Body::from(
                            "{\"id\":\"ignored-claude-cli-finalize-response\",\"type\":\"message\",\"content\":[]}",
                        ))
                        .expect("response should build");
                    response.headers_mut().insert(
                        http::header::CONTENT_TYPE,
                        HeaderValue::from_static("application/json"),
                    );
                    response.headers_mut().insert(
                        HeaderName::from_static(CONTROL_EXECUTED_HEADER),
                        HeaderValue::from_static("true"),
                    );
                    response
                }
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(move |_request: Request| {
                let report_hits_inner = Arc::clone(&report_hits_clone);
                async move {
                    *report_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({"ok": true}))
                }
            }),
        )
        .route(
            "/api/internal/gateway/execute-sync",
            any(move |_request: Request| {
                let execute_hits_inner = Arc::clone(&execute_hits_clone);
                async move {
                    *execute_hits_inner.lock().expect("mutex should lock") += 1;
                    let mut response = Response::builder()
                        .status(StatusCode::CREATED)
                        .body(Body::from("{\"fallback\":true}"))
                        .expect("response should build");
                    response.headers_mut().insert(
                        http::header::CONTENT_TYPE,
                        HeaderValue::from_static("application/json"),
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
        "/v1/execute/sync",
        any(|_request: Request| async move {
            Json(json!({
                "request_id": "req-claude-cli-stream-sync-direct-123",
                "status_code": 200,
                "headers": {
                    "content-type": "text/event-stream"
                },
                "body": {
                    "body_bytes_b64": base64::engine::general_purpose::STANDARD.encode(
                        concat!(
                            "event: message_start\n",
                            "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_claude_cli_sync_123\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-code-upstream\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null}}\n\n",
                            "event: content_block_start\n",
                            "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
                            "event: content_block_delta\n",
                            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}\n\n",
                            "event: content_block_delta\n",
                            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\" Claude CLI\"}}\n\n",
                            "event: content_block_stop\n",
                            "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
                            "event: message_delta\n",
                            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"input_tokens\":4,\"output_tokens\":6}}\n\n",
                            "event: message_stop\n",
                            "data: {\"type\":\"message_stop\"}\n\n"
                        )
                    )
                },
                "telemetry": {
                    "elapsed_ms": 32
                }
            }))
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

    let started_at = std::time::Instant::now();
    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/messages"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(http::header::AUTHORIZATION, "Bearer cli-key")
        .header(TRACE_ID_HEADER, "trace-claude-cli-stream-sync-direct-123")
        .body("{\"model\":\"claude-code\",\"messages\":[]}")
        .send()
        .await
        .expect("request should succeed");
    let elapsed = started_at.elapsed();

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "id": "msg_claude_cli_sync_123",
            "type": "message",
            "role": "assistant",
            "model": "claude-code-upstream",
            "content": [{
                "type": "text",
                "text": "Hello Claude CLI"
            }],
            "stop_reason": "end_turn",
            "stop_sequence": null,
            "usage": {
                "input_tokens": 4,
                "output_tokens": 6
            }
        })
    );
    assert!(
        elapsed < std::time::Duration::from_millis(350),
        "response should not wait for finalize-sync background task"
    );

    wait_until(700, || *report_hits.lock().expect("mutex should lock") == 1).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        seen_finalize.lock().expect("mutex should lock").is_none(),
        "finalize-sync should not be called when local finalize can downgrade to success report"
    );
    assert_eq!(*report_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
