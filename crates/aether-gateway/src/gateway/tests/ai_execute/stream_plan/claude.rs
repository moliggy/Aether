use super::*;

#[tokio::test]
async fn gateway_executes_claude_chat_stream_via_executor_plan() {
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
                        "user_id": "user-claude-stream-direct-123",
                        "api_key_id": "key-claude-stream-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/messages"
                }))
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
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_stream",
                    "plan_kind": "claude_chat_stream",
                    "plan": {
                        "request_id": "req-claude-chat-stream-direct-123",
                        "provider_name": "claude",
                        "provider_id": "provider-claude-chat-stream-direct-123",
                        "endpoint_id": "endpoint-claude-chat-stream-direct-123",
                        "key_id": "key-claude-chat-stream-direct-123",
                        "method": "POST",
                        "url": "https://api.anthropic.example/v1/messages",
                        "headers": {
                            "x-api-key": "upstream-key",
                            "content-type": "application/json",
                            "accept": "text/event-stream"
                        },
                        "body": {
                            "json_body": {
                                "model": "claude-sonnet-4",
                                "messages": [],
                                "stream": true
                            }
                        },
                        "stream": true,
                        "client_api_format": "claude:chat",
                        "provider_api_format": "claude:chat",
                        "model_name": "claude-sonnet-4"
                    },
                    "report_kind": "claude_chat_stream_success",
                    "report_context": {
                        "user_id": "user-claude-stream-direct-123",
                        "api_key_id": "key-claude-stream-direct-123",
                        "client_api_format": "claude:chat"
                    }
                }))
            }),
        )
        .route(
            "/api/internal/gateway/report-stream",
            any(move |_request: Request| {
                let report_hits_inner = Arc::clone(&report_hits_clone);
                async move {
                    *report_hits_inner.lock().expect("mutex should lock") += 1;
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
            let frames = concat!(
                "{\"type\":\"headers\",\"payload\":{\"kind\":\"headers\",\"status_code\":200,\"headers\":{\"content-type\":\"text/event-stream\"}}}\n",
                "{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"event: message_start\\ndata: {\\\"type\\\":\\\"message_start\\\"}\\n\\n\"}}\n",
                "{\"type\":\"telemetry\",\"payload\":{\"kind\":\"telemetry\",\"telemetry\":{\"elapsed_ms\":37,\"upstream_bytes\":42}}}\n",
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
        .post(format!("{gateway_url}/v1/messages"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header("x-api-key", "client-key")
        .header(TRACE_ID_HEADER, "trace-claude-chat-stream-direct-123")
        .body("{\"model\":\"claude-sonnet-4\",\"messages\":[],\"stream\":true}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.text().await.expect("body should read"),
        "event: message_start\ndata: {\"type\":\"message_start\"}\n\n"
    );
    wait_until(300, || *report_hits.lock().expect("mutex should lock") == 1).await;
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_claude_cli_stream_via_executor_plan() {
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
                        "user_id": "user-claude-cli-stream-direct-123",
                        "api_key_id": "key-claude-cli-stream-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/messages"
                }))
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
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_stream",
                    "plan_kind": "claude_cli_stream",
                    "plan": {
                        "request_id": "req-claude-cli-stream-direct-123",
                        "provider_name": "claude",
                        "provider_id": "provider-claude-cli-stream-direct-123",
                        "endpoint_id": "endpoint-claude-cli-stream-direct-123",
                        "key_id": "key-claude-cli-stream-direct-123",
                        "method": "POST",
                        "url": "https://api.anthropic.example/v1/messages",
                        "headers": {
                            "x-api-key": "upstream-key",
                            "content-type": "application/json",
                            "accept": "text/event-stream"
                        },
                        "body": {
                            "json_body": {
                                "model": "claude-sonnet-4",
                                "messages": [],
                                "stream": true
                            }
                        },
                        "stream": true,
                        "client_api_format": "claude:cli",
                        "provider_api_format": "claude:cli",
                        "model_name": "claude-sonnet-4"
                    },
                    "report_kind": "claude_cli_stream_success",
                    "report_context": {
                        "user_id": "user-claude-cli-stream-direct-123",
                        "api_key_id": "key-claude-cli-stream-direct-123",
                        "client_api_format": "claude:cli"
                    }
                }))
            }),
        )
        .route(
            "/api/internal/gateway/report-stream",
            any(move |_request: Request| {
                let report_hits_inner = Arc::clone(&report_hits_clone);
                async move {
                    *report_hits_inner.lock().expect("mutex should lock") += 1;
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
            let frames = concat!(
                "{\"type\":\"headers\",\"payload\":{\"kind\":\"headers\",\"status_code\":200,\"headers\":{\"content-type\":\"text/event-stream\"}}}\n",
                "{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"event: message_start\\ndata: {\\\"type\\\":\\\"message_start\\\"}\\n\\n\"}}\n",
                "{\"type\":\"telemetry\",\"payload\":{\"kind\":\"telemetry\",\"telemetry\":{\"elapsed_ms\":35,\"upstream_bytes\":42}}}\n",
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
        .post(format!("{gateway_url}/v1/messages"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-claude-cli-stream-direct-123")
        .header(http::header::AUTHORIZATION, "Bearer client-key")
        .header("anthropic-beta", "output-128k-2025-02-19")
        .body("{\"model\":\"claude-sonnet-4\",\"messages\":[],\"stream\":true}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.text().await.expect("body should read"),
        "event: message_start\ndata: {\"type\":\"message_start\"}\n\n"
    );
    wait_until(300, || *report_hits.lock().expect("mutex should lock") == 1).await;
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
