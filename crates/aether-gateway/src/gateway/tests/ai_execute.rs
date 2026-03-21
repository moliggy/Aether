use super::*;

#[tokio::test]
async fn gateway_executes_sync_ai_route_via_control_execute_endpoint() {
    #[derive(Debug, Clone)]
    struct SeenExecuteSyncRequest {
        trace_id: String,
        path: String,
        model: String,
        user_id: String,
    }

    let seen_execute = Arc::new(Mutex::new(None::<SeenExecuteSyncRequest>));
    let seen_execute_clone = Arc::clone(&seen_execute);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);

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
                        "user_id": "user-sync-123",
                        "api_key_id": "key-sync-123",
                        "balance_remaining": 12.5,
                        "access_allowed": true
                    },
                    "public_path": "/v1/chat/completions"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/execute-sync",
            any(move |request: Request| {
                let seen_execute_inner = Arc::clone(&seen_execute_clone);
                async move {
                    let (parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("execute payload should parse");
                    *seen_execute_inner.lock().expect("mutex should lock") =
                        Some(SeenExecuteSyncRequest {
                            trace_id: parts
                                .headers
                                .get(TRACE_ID_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            path: payload
                                .get("path")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            model: payload
                                .get("body_json")
                                .and_then(|value| value.get("model"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            user_id: payload
                                .get("auth_context")
                                .and_then(|value| value.get("user_id"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        });
                    let mut response = Response::builder()
                        .status(StatusCode::CREATED)
                        .body(Body::from("{\"ok\":true}"))
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
            "/v1/chat/completions",
            any(move |_request: Request| {
                let public_hits_inner = Arc::clone(&public_hits_clone);
                async move {
                    *public_hits_inner.lock().expect("mutex should lock") += 1;
                    (StatusCode::IM_A_TEAPOT, Body::from("public-route-hit"))
                }
            }),
        );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_control(upstream_url.clone(), Some(upstream_url))
        .expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-sync-123")
        .body("{\"model\":\"gpt-5\",\"messages\":[]}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(
        response
            .headers()
            .get(CONTROL_ROUTE_CLASS_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some("ai_public")
    );
    assert_eq!(
        response
            .headers()
            .get(GATEWAY_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some("rust-phase3b")
    );
    assert_eq!(
        response.text().await.expect("body should read"),
        "{\"ok\":true}"
    );

    let seen_execute_request = seen_execute
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("execute-sync should be captured");
    assert_eq!(seen_execute_request.trace_id, "trace-sync-123");
    assert_eq!(seen_execute_request.path, "/v1/chat/completions");
    assert_eq!(seen_execute_request.model, "gpt-5");
    assert_eq!(seen_execute_request.user_id, "user-sync-123");
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_stream_ai_route_via_control_stream_endpoint() {
    #[derive(Debug, Clone)]
    struct SeenExecuteStreamRequest {
        trace_id: String,
        path: String,
        stream: bool,
    }

    let seen_execute = Arc::new(Mutex::new(None::<SeenExecuteStreamRequest>));
    let seen_execute_clone = Arc::clone(&seen_execute);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);

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
                        "user_id": "user-stream-123",
                        "api_key_id": "key-stream-123",
                        "balance_remaining": 8.0,
                        "access_allowed": true
                    },
                    "public_path": "/v1/chat/completions"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/execute-stream",
            any(move |request: Request| {
                let seen_execute_inner = Arc::clone(&seen_execute_clone);
                async move {
                    let (parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("execute payload should parse");
                    *seen_execute_inner.lock().expect("mutex should lock") =
                        Some(SeenExecuteStreamRequest {
                            trace_id: parts
                                .headers
                                .get(TRACE_ID_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            path: payload
                                .get("path")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            stream: payload
                                .get("body_json")
                                .and_then(|value| value.get("stream"))
                                .and_then(|value| value.as_bool())
                                .unwrap_or(false),
                        });
                    let stream = futures_util::stream::iter([
                        Ok::<_, Infallible>(Bytes::from_static(b"data: one\n\n")),
                        Ok::<_, Infallible>(Bytes::from_static(b"data: [DONE]\n\n")),
                    ]);
                    let mut response = Response::builder()
                        .status(StatusCode::OK)
                        .body(Body::from_stream(stream))
                        .expect("response should build");
                    response.headers_mut().insert(
                        http::header::CONTENT_TYPE,
                        HeaderValue::from_static("text/event-stream"),
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
            "/v1/chat/completions",
            any(move |_request: Request| {
                let public_hits_inner = Arc::clone(&public_hits_clone);
                async move {
                    *public_hits_inner.lock().expect("mutex should lock") += 1;
                    (StatusCode::IM_A_TEAPOT, Body::from("public-route-hit"))
                }
            }),
        );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_control(upstream_url.clone(), Some(upstream_url))
        .expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-stream-123")
        .body("{\"model\":\"gpt-5\",\"messages\":[],\"stream\":true}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(CONTROL_ROUTE_CLASS_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some("ai_public")
    );
    assert_eq!(
        response
            .headers()
            .get(GATEWAY_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some("rust-phase3b")
    );
    assert_eq!(
        response.text().await.expect("body should read"),
        "data: one\n\ndata: [DONE]\n\n"
    );

    let seen_execute_request = seen_execute
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("execute-stream should be captured");
    assert_eq!(seen_execute_request.trace_id, "trace-stream-123");
    assert_eq!(seen_execute_request.path, "/v1/chat/completions");
    assert!(seen_execute_request.stream);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_openai_chat_stream_via_executor_plan() {
    use base64::Engine as _;

    #[derive(Debug, Clone)]
    struct SeenPlanStreamRequest {
        trace_id: String,
        path: String,
        stream: bool,
    }

    #[derive(Debug, Clone)]
    struct SeenReportStreamRequest {
        trace_id: String,
        report_kind: String,
        status_code: u64,
        body: String,
    }

    let seen_plan = Arc::new(Mutex::new(None::<SeenPlanStreamRequest>));
    let seen_plan_clone = Arc::clone(&seen_plan);
    let seen_report = Arc::new(Mutex::new(None::<SeenReportStreamRequest>));
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
                    "route_family": "openai",
                    "route_kind": "chat",
                    "auth_endpoint_signature": "openai:chat",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-openai-stream-direct-123",
                        "api_key_id": "key-openai-stream-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/chat/completions"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/plan-stream",
            any(move |request: Request| {
                let seen_plan_inner = Arc::clone(&seen_plan_clone);
                async move {
                    let (parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("plan payload should parse");
                    *seen_plan_inner.lock().expect("mutex should lock") =
                        Some(SeenPlanStreamRequest {
                            trace_id: parts
                                .headers
                                .get(TRACE_ID_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            path: payload
                                .get("path")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            stream: payload
                                .get("body_json")
                                .and_then(|value| value.get("stream"))
                                .and_then(|value| value.as_bool())
                                .unwrap_or(false),
                        });
                    Json(json!({
                        "action": "executor_stream",
                        "plan_kind": "openai_chat_stream",
                        "plan": {
                            "request_id": "req-openai-chat-stream-direct-123",
                            "provider_name": "openai",
                            "provider_id": "provider-openai-chat-stream-direct-123",
                            "endpoint_id": "endpoint-openai-chat-stream-direct-123",
                            "key_id": "key-openai-chat-stream-direct-123",
                            "method": "POST",
                            "url": "https://api.openai.example/v1/chat/completions",
                            "headers": {
                                "authorization": "Bearer upstream-key",
                                "content-type": "application/json",
                                "accept": "text/event-stream"
                            },
                            "body": {
                                "json_body": {
                                    "model": "gpt-5",
                                    "messages": [],
                                    "stream": true
                                }
                            },
                            "stream": true,
                            "client_api_format": "openai:chat",
                            "provider_api_format": "openai:chat",
                            "model_name": "gpt-5"
                        },
                        "report_kind": "openai_chat_stream_success",
                        "report_context": {
                            "user_id": "user-openai-stream-direct-123",
                            "api_key_id": "key-openai-stream-direct-123"
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
            "/api/internal/gateway/report-stream",
            any(move |request: Request| {
                let seen_report_inner = Arc::clone(&seen_report_clone);
                async move {
                    let (parts, body) = request.into_parts();
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
                            trace_id: parts
                                .headers
                                .get(TRACE_ID_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            report_kind: payload
                                .get("report_kind")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            status_code: payload
                                .get("status_code")
                                .and_then(|value| value.as_u64())
                                .unwrap_or_default(),
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
            let frames = concat!(
                "{\"type\":\"headers\",\"payload\":{\"kind\":\"headers\",\"status_code\":200,\"headers\":{\"content-type\":\"text/event-stream\"}}}\n",
                "{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"data: {\\\"id\\\":\\\"chatcmpl-123\\\"}\\n\\n\"}}\n",
                "{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"data: [DONE]\\n\\n\"}}\n",
                "{\"type\":\"telemetry\",\"payload\":{\"kind\":\"telemetry\",\"telemetry\":{\"elapsed_ms\":41,\"ttfb_ms\":12,\"upstream_bytes\":31}}}\n",
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
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-openai-chat-stream-direct-123")
        .body("{\"model\":\"gpt-5\",\"messages\":[],\"stream\":true}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.text().await.expect("body should read"),
        "data: {\"id\":\"chatcmpl-123\"}\n\ndata: [DONE]\n\n"
    );

    let seen_plan_request = seen_plan
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("plan-stream should be captured");
    assert_eq!(
        seen_plan_request.trace_id,
        "trace-openai-chat-stream-direct-123"
    );
    assert_eq!(seen_plan_request.path, "/v1/chat/completions");
    assert!(seen_plan_request.stream);

    let seen_report_request = seen_report
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("report-stream should be captured");
    assert_eq!(
        seen_report_request.trace_id,
        "trace-openai-chat-stream-direct-123"
    );
    assert_eq!(
        seen_report_request.report_kind,
        "openai_chat_stream_success"
    );
    assert_eq!(seen_report_request.status_code, 200);
    assert_eq!(
        seen_report_request.body,
        "data: {\"id\":\"chatcmpl-123\"}\n\ndata: [DONE]\n\n"
    );

    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

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
    assert_eq!(*report_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_gemini_chat_stream_via_executor_plan() {
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
                    "route_family": "gemini",
                    "route_kind": "chat",
                    "auth_endpoint_signature": "gemini:chat",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-gemini-stream-direct-123",
                        "api_key_id": "key-gemini-stream-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1beta/models/gemini-2.5-pro:streamGenerateContent"
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
                    "plan_kind": "gemini_chat_stream",
                    "plan": {
                        "request_id": "req-gemini-chat-stream-direct-123",
                        "provider_name": "gemini",
                        "provider_id": "provider-gemini-chat-stream-direct-123",
                        "endpoint_id": "endpoint-gemini-chat-stream-direct-123",
                        "key_id": "key-gemini-chat-stream-direct-123",
                        "method": "POST",
                        "url": "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro:streamGenerateContent",
                        "headers": {
                            "x-goog-api-key": "upstream-key",
                            "content-type": "application/json",
                            "accept": "text/event-stream"
                        },
                        "body": {
                            "json_body": {
                                "contents": []
                            }
                        },
                        "stream": true,
                        "client_api_format": "gemini:chat",
                        "provider_api_format": "gemini:chat",
                        "model_name": "gemini-2.5-pro"
                    },
                    "report_kind": "gemini_chat_stream_success",
                    "report_context": {
                        "user_id": "user-gemini-stream-direct-123",
                        "api_key_id": "key-gemini-stream-direct-123",
                        "client_api_format": "gemini:chat"
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
                "{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"data: {\\\"candidates\\\":[]}\\n\\n\"}}\n",
                "{\"type\":\"telemetry\",\"payload\":{\"kind\":\"telemetry\",\"telemetry\":{\"elapsed_ms\":33,\"upstream_bytes\":26}}}\n",
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
        .header("x-goog-api-key", "client-key")
        .header(TRACE_ID_HEADER, "trace-gemini-chat-stream-direct-123")
        .body("{\"contents\":[]}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.text().await.expect("body should read"),
        "data: {\"candidates\":[]}\n\n"
    );
    assert_eq!(*report_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_openai_cli_stream_via_executor_plan() {
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
                    "route_family": "openai",
                    "route_kind": "cli",
                    "auth_endpoint_signature": "openai:cli",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-openai-cli-stream-direct-123",
                        "api_key_id": "key-openai-cli-stream-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/responses"
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
                    "plan_kind": "openai_cli_stream",
                    "plan": {
                        "request_id": "req-openai-cli-stream-direct-123",
                        "provider_name": "openai",
                        "provider_id": "provider-openai-cli-stream-direct-123",
                        "endpoint_id": "endpoint-openai-cli-stream-direct-123",
                        "key_id": "key-openai-cli-stream-direct-123",
                        "method": "POST",
                        "url": "https://api.openai.example/v1/responses",
                        "headers": {
                            "authorization": "Bearer upstream-key",
                            "content-type": "application/json",
                            "accept": "text/event-stream"
                        },
                        "body": {
                            "json_body": {
                                "model": "gpt-5",
                                "input": "hello",
                                "stream": true
                            }
                        },
                        "stream": true,
                        "client_api_format": "openai:cli",
                        "provider_api_format": "openai:cli",
                        "model_name": "gpt-5"
                    },
                    "report_kind": "openai_cli_stream_success",
                    "report_context": {
                        "user_id": "user-openai-cli-stream-direct-123",
                        "api_key_id": "key-openai-cli-stream-direct-123",
                        "client_api_format": "openai:cli"
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
                "{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"event: response.completed\\ndata: {\\\"type\\\":\\\"response.completed\\\"}\\n\\n\"}}\n",
                "{\"type\":\"telemetry\",\"payload\":{\"kind\":\"telemetry\",\"telemetry\":{\"elapsed_ms\":39,\"upstream_bytes\":59}}}\n",
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
        .post(format!("{gateway_url}/v1/responses"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-openai-cli-stream-direct-123")
        .body("{\"model\":\"gpt-5\",\"input\":\"hello\",\"stream\":true}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.text().await.expect("body should read"),
        "event: response.completed\ndata: {\"type\":\"response.completed\"}\n\n"
    );
    assert_eq!(*report_hits.lock().expect("mutex should lock"), 1);
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
    assert_eq!(*report_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_gemini_cli_stream_via_executor_plan() {
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
                    "route_family": "gemini",
                    "route_kind": "cli",
                    "auth_endpoint_signature": "gemini:cli",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-gemini-cli-stream-direct-123",
                        "api_key_id": "key-gemini-cli-stream-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1beta/models/gemini-cli:streamGenerateContent"
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
                    "plan_kind": "gemini_cli_stream",
                    "plan": {
                        "request_id": "req-gemini-cli-stream-direct-123",
                        "provider_name": "gemini",
                        "provider_id": "provider-gemini-cli-stream-direct-123",
                        "endpoint_id": "endpoint-gemini-cli-stream-direct-123",
                        "key_id": "key-gemini-cli-stream-direct-123",
                        "method": "POST",
                        "url": "https://generativelanguage.googleapis.com/v1beta/models/gemini-cli:streamGenerateContent",
                        "headers": {
                            "x-goog-api-key": "upstream-key",
                            "content-type": "application/json",
                            "accept": "text/event-stream"
                        },
                        "body": {
                            "json_body": {
                                "contents": []
                            }
                        },
                        "stream": true,
                        "client_api_format": "gemini:cli",
                        "provider_api_format": "gemini:cli",
                        "model_name": "gemini-cli"
                    },
                    "report_kind": "gemini_cli_stream_success",
                    "report_context": {
                        "user_id": "user-gemini-cli-stream-direct-123",
                        "api_key_id": "key-gemini-cli-stream-direct-123",
                        "client_api_format": "gemini:cli"
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
                "{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"data: {\\\"candidates\\\":[]}\\n\\n\"}}\n",
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
        .header(TRACE_ID_HEADER, "trace-gemini-cli-stream-direct-123")
        .body("{\"contents\":[]}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.text().await.expect("body should read"),
        "data: {\"candidates\":[]}\n\n"
    );
    assert_eq!(*report_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_openai_chat_sync_via_executor_plan() {
    #[derive(Debug, Clone)]
    struct SeenPlanSyncRequest {
        trace_id: String,
        path: String,
        model: String,
    }

    #[derive(Debug, Clone)]
    struct SeenExecutorSyncRequest {
        trace_id: String,
        url: String,
    }

    #[derive(Debug, Clone)]
    struct SeenReportSyncRequest {
        trace_id: String,
        report_kind: String,
        status_code: u64,
    }

    let seen_plan = Arc::new(Mutex::new(None::<SeenPlanSyncRequest>));
    let seen_plan_clone = Arc::clone(&seen_plan);
    let seen_executor = Arc::new(Mutex::new(None::<SeenExecutorSyncRequest>));
    let seen_executor_clone = Arc::clone(&seen_executor);
    let seen_report = Arc::new(Mutex::new(None::<SeenReportSyncRequest>));
    let seen_report_clone = Arc::clone(&seen_report);
    let execute_hits = Arc::new(Mutex::new(0usize));
    let execute_hits_clone = Arc::clone(&execute_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);

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
                        "user_id": "user-chat-direct-123",
                        "api_key_id": "key-chat-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/chat/completions"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/plan-sync",
            any(move |request: Request| {
                let seen_plan_inner = Arc::clone(&seen_plan_clone);
                async move {
                    let (parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("plan payload should parse");
                    *seen_plan_inner.lock().expect("mutex should lock") =
                        Some(SeenPlanSyncRequest {
                            trace_id: parts
                                .headers
                                .get(TRACE_ID_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            path: payload
                                .get("path")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            model: payload
                                .get("body_json")
                                .and_then(|value| value.get("model"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        });
                    Json(json!({
                        "action": "executor_sync",
                        "plan_kind": "openai_chat_sync",
                        "plan": {
                            "request_id": "req-openai-chat-direct-123",
                            "provider_name": "openai",
                            "provider_id": "provider-openai-chat-direct-123",
                            "endpoint_id": "endpoint-openai-chat-direct-123",
                            "key_id": "key-openai-chat-direct-123",
                            "method": "POST",
                            "url": "https://api.openai.example/v1/chat/completions",
                            "headers": {
                                "authorization": "Bearer upstream-key",
                                "content-type": "application/json"
                            },
                            "body": {
                                "json_body": {
                                    "model": "gpt-5",
                                    "messages": []
                                }
                            },
                            "stream": false,
                            "client_api_format": "openai:chat",
                            "provider_api_format": "openai:chat",
                            "model_name": "gpt-5"
                        },
                        "report_kind": "openai_chat_sync_success",
                        "report_context": {
                            "user_id": "user-chat-direct-123",
                            "api_key_id": "key-chat-direct-123"
                        }
                    }))
                }
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(move |request: Request| {
                let seen_report_inner = Arc::clone(&seen_report_clone);
                async move {
                    let (parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("report payload should parse");
                    *seen_report_inner.lock().expect("mutex should lock") =
                        Some(SeenReportSyncRequest {
                            trace_id: parts
                                .headers
                                .get(TRACE_ID_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            report_kind: payload
                                .get("report_kind")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            status_code: payload
                                .get("status_code")
                                .and_then(|value| value.as_u64())
                                .unwrap_or_default(),
                        });
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
        )
        .route(
            "/v1/chat/completions",
            any(move |_request: Request| {
                let public_hits_inner = Arc::clone(&public_hits_clone);
                async move {
                    *public_hits_inner.lock().expect("mutex should lock") += 1;
                    (StatusCode::IM_A_TEAPOT, Body::from("public-route-hit"))
                }
            }),
        );

    let executor = Router::new().route(
        "/v1/execute/sync",
        any(move |request: Request| {
            let seen_executor_inner = Arc::clone(&seen_executor_clone);
            async move {
                let (parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value =
                    serde_json::from_slice(&raw_body).expect("executor payload should parse");
                *seen_executor_inner.lock().expect("mutex should lock") =
                    Some(SeenExecutorSyncRequest {
                        trace_id: parts
                            .headers
                            .get(TRACE_ID_HEADER)
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or_default()
                            .to_string(),
                        url: payload
                            .get("url")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                    });
                Json(json!({
                    "request_id": "req-openai-chat-direct-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "id": "chatcmpl-123",
                            "object": "chat.completion",
                            "model": "gpt-5",
                            "choices": [],
                            "usage": {
                                "prompt_tokens": 1,
                                "completion_tokens": 2,
                                "total_tokens": 3
                            }
                        }
                    },
                    "telemetry": {
                        "elapsed_ms": 41
                    }
                }))
            }
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
        .header(TRACE_ID_HEADER, "trace-openai-chat-direct-123")
        .body("{\"model\":\"gpt-5\",\"messages\":[]}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "id": "chatcmpl-123",
            "object": "chat.completion",
            "model": "gpt-5",
            "choices": [],
            "usage": {
                "prompt_tokens": 1,
                "completion_tokens": 2,
                "total_tokens": 3
            }
        })
    );

    let seen_plan_request = seen_plan
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("plan-sync should be captured");
    assert_eq!(seen_plan_request.trace_id, "trace-openai-chat-direct-123");
    assert_eq!(seen_plan_request.path, "/v1/chat/completions");
    assert_eq!(seen_plan_request.model, "gpt-5");

    let seen_executor_request = seen_executor
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("executor sync should be captured");
    assert_eq!(
        seen_executor_request.trace_id,
        "trace-openai-chat-direct-123"
    );
    assert_eq!(
        seen_executor_request.url,
        "https://api.openai.example/v1/chat/completions"
    );

    let seen_report_request = seen_report
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("report-sync should be captured");
    assert_eq!(seen_report_request.trace_id, "trace-openai-chat-direct-123");
    assert_eq!(seen_report_request.report_kind, "openai_chat_sync_success");
    assert_eq!(seen_report_request.status_code, 200);

    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_openai_cli_sync_via_executor_plan() {
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
                    "route_family": "openai",
                    "route_kind": "cli",
                    "auth_endpoint_signature": "openai:cli",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-cli-direct-123",
                        "api_key_id": "key-cli-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/responses"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/plan-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync",
                    "plan_kind": "openai_cli_sync",
                    "plan": {
                        "request_id": "req-openai-cli-direct-123",
                        "provider_name": "openai",
                        "provider_id": "provider-openai-cli-direct-123",
                        "endpoint_id": "endpoint-openai-cli-direct-123",
                        "key_id": "key-openai-cli-direct-123",
                        "method": "POST",
                        "url": "https://api.openai.example/v1/responses",
                        "headers": {
                            "authorization": "Bearer upstream-key",
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "model": "gpt-5",
                                "input": "hello"
                            }
                        },
                        "stream": false,
                        "client_api_format": "openai:cli",
                        "provider_api_format": "openai:cli",
                        "model_name": "gpt-5"
                    },
                    "report_kind": "openai_cli_sync_success",
                    "report_context": {
                        "user_id": "user-cli-direct-123",
                        "api_key_id": "key-cli-direct-123",
                        "client_api_format": "openai:cli"
                    }
                }))
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
                        .status(StatusCode::OK)
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
                "request_id": "req-openai-cli-direct-123",
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "id": "resp_cli_123",
                        "object": "response",
                        "model": "gpt-5",
                        "output": [],
                        "usage": {
                            "input_tokens": 1,
                            "output_tokens": 2,
                            "total_tokens": 3
                        }
                    }
                },
                "telemetry": {
                    "elapsed_ms": 52
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

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/responses"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-openai-cli-direct-123")
        .body("{\"model\":\"gpt-5\",\"input\":\"hello\"}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "id": "resp_cli_123",
            "object": "response",
            "model": "gpt-5",
            "output": [],
            "usage": {
                "input_tokens": 1,
                "output_tokens": 2,
                "total_tokens": 3
            }
        })
    );
    assert_eq!(*report_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_openai_compact_sync_via_executor_plan() {
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
                    "route_family": "openai",
                    "route_kind": "compact",
                    "auth_endpoint_signature": "openai:compact",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-compact-direct-123",
                        "api_key_id": "key-compact-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/responses/compact"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/plan-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync",
                    "plan_kind": "openai_compact_sync",
                    "plan": {
                        "request_id": "req-openai-compact-direct-123",
                        "provider_name": "openai",
                        "provider_id": "provider-openai-compact-direct-123",
                        "endpoint_id": "endpoint-openai-compact-direct-123",
                        "key_id": "key-openai-compact-direct-123",
                        "method": "POST",
                        "url": "https://api.openai.example/v1/responses/compact",
                        "headers": {
                            "authorization": "Bearer upstream-key",
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "model": "gpt-5",
                                "input": "hello"
                            }
                        },
                        "stream": false,
                        "client_api_format": "openai:compact",
                        "provider_api_format": "openai:compact",
                        "model_name": "gpt-5"
                    },
                    "report_kind": "openai_cli_sync_success",
                    "report_context": {
                        "user_id": "user-compact-direct-123",
                        "api_key_id": "key-compact-direct-123",
                        "client_api_format": "openai:compact"
                    }
                }))
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
                        .status(StatusCode::OK)
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
                "request_id": "req-openai-compact-direct-123",
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "id": "resp_compact_123",
                        "object": "response",
                        "model": "gpt-5",
                        "output": [],
                        "usage": {
                            "input_tokens": 4,
                            "output_tokens": 5,
                            "total_tokens": 9
                        }
                    }
                },
                "telemetry": {
                    "elapsed_ms": 49
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

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/responses/compact"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-openai-compact-direct-123")
        .body("{\"model\":\"gpt-5\",\"input\":\"hello\"}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "id": "resp_compact_123",
            "object": "response",
            "model": "gpt-5",
            "output": [],
            "usage": {
                "input_tokens": 4,
                "output_tokens": 5,
                "total_tokens": 9
            }
        })
    );
    assert_eq!(*report_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_claude_chat_sync_via_executor_plan() {
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
                        "user_id": "user-claude-direct-123",
                        "api_key_id": "key-claude-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/messages"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/plan-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync",
                    "plan_kind": "claude_chat_sync",
                    "plan": {
                        "request_id": "req-claude-direct-123",
                        "provider_name": "claude",
                        "provider_id": "provider-claude-direct-123",
                        "endpoint_id": "endpoint-claude-direct-123",
                        "key_id": "key-claude-direct-123",
                        "method": "POST",
                        "url": "https://api.anthropic.example/v1/messages",
                        "headers": {
                            "x-api-key": "upstream-key",
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "model": "claude-sonnet-4",
                                "messages": []
                            }
                        },
                        "stream": false,
                        "client_api_format": "claude:chat",
                        "provider_api_format": "claude:chat",
                        "model_name": "claude-sonnet-4"
                    },
                    "report_kind": "claude_chat_sync_success",
                    "report_context": {
                        "user_id": "user-claude-direct-123",
                        "api_key_id": "key-claude-direct-123",
                        "client_api_format": "claude:chat"
                    }
                }))
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
                        .status(StatusCode::OK)
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
                "request_id": "req-claude-direct-123",
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "id": "msg_123",
                        "type": "message",
                        "model": "claude-sonnet-4",
                        "role": "assistant",
                        "content": [],
                        "usage": {
                            "input_tokens": 3,
                            "output_tokens": 5
                        }
                    }
                },
                "telemetry": {
                    "elapsed_ms": 61
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

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/messages"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-claude-direct-123")
        .body("{\"model\":\"claude-sonnet-4\",\"messages\":[]}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "id": "msg_123",
            "type": "message",
            "model": "claude-sonnet-4",
            "role": "assistant",
            "content": [],
            "usage": {
                "input_tokens": 3,
                "output_tokens": 5
            }
        })
    );
    assert_eq!(*report_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_gemini_chat_sync_via_executor_plan() {
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
                    "route_family": "gemini",
                    "route_kind": "chat",
                    "auth_endpoint_signature": "gemini:chat",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-gemini-direct-123",
                        "api_key_id": "key-gemini-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1beta/models/gemini-2.5-pro:generateContent"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/plan-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync",
                    "plan_kind": "gemini_chat_sync",
                    "plan": {
                        "request_id": "req-gemini-direct-123",
                        "provider_name": "gemini",
                        "provider_id": "provider-gemini-direct-123",
                        "endpoint_id": "endpoint-gemini-direct-123",
                        "key_id": "key-gemini-direct-123",
                        "method": "POST",
                        "url": "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro:generateContent",
                        "headers": {
                            "x-goog-api-key": "upstream-key",
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "contents": []
                            }
                        },
                        "stream": false,
                        "client_api_format": "gemini:chat",
                        "provider_api_format": "gemini:chat",
                        "model_name": "gemini-2.5-pro"
                    },
                    "report_kind": "gemini_chat_sync_success",
                    "report_context": {
                        "user_id": "user-gemini-direct-123",
                        "api_key_id": "key-gemini-direct-123",
                        "client_api_format": "gemini:chat"
                    }
                }))
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
                        .status(StatusCode::OK)
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
                "request_id": "req-gemini-direct-123",
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "candidates": [],
                        "usageMetadata": {
                            "promptTokenCount": 7,
                            "candidatesTokenCount": 11,
                            "totalTokenCount": 18
                        }
                    }
                },
                "telemetry": {
                    "elapsed_ms": 57
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

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/v1beta/models/gemini-2.5-pro:generateContent"
        ))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-gemini-direct-123")
        .body("{\"contents\":[]}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "candidates": [],
            "usageMetadata": {
                "promptTokenCount": 7,
                "candidatesTokenCount": 11,
                "totalTokenCount": 18
            }
        })
    );
    assert_eq!(*report_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_claude_cli_sync_via_executor_plan() {
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
                        "user_id": "user-claude-cli-direct-123",
                        "api_key_id": "key-claude-cli-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/messages"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/plan-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync",
                    "plan_kind": "claude_cli_sync",
                    "plan": {
                        "request_id": "req-claude-cli-direct-123",
                        "provider_name": "claude",
                        "provider_id": "provider-claude-cli-direct-123",
                        "endpoint_id": "endpoint-claude-cli-direct-123",
                        "key_id": "key-claude-cli-direct-123",
                        "method": "POST",
                        "url": "https://api.anthropic.example/v1/messages",
                        "headers": {
                            "x-api-key": "upstream-key",
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "model": "claude-code",
                                "messages": []
                            }
                        },
                        "stream": false,
                        "client_api_format": "claude:cli",
                        "provider_api_format": "claude:cli",
                        "model_name": "claude-code"
                    },
                    "report_kind": "claude_cli_sync_success",
                    "report_context": {
                        "user_id": "user-claude-cli-direct-123",
                        "api_key_id": "key-claude-cli-direct-123",
                        "client_api_format": "claude:cli"
                    }
                }))
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
                        .status(StatusCode::OK)
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
                "request_id": "req-claude-cli-direct-123",
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "id": "msg_cli_123",
                        "type": "message",
                        "content": [],
                        "usage": {
                            "input_tokens": 2,
                            "output_tokens": 4
                        }
                    }
                },
                "telemetry": {
                    "elapsed_ms": 58
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

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/messages"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(http::header::AUTHORIZATION, "Bearer cli-key")
        .header(TRACE_ID_HEADER, "trace-claude-cli-direct-123")
        .body("{\"model\":\"claude-code\",\"messages\":[]}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(*report_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_gemini_cli_sync_via_executor_plan() {
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
                    "route_family": "gemini",
                    "route_kind": "cli",
                    "auth_endpoint_signature": "gemini:cli",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-gemini-cli-direct-123",
                        "api_key_id": "key-gemini-cli-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1beta/models/gemini-cli:generateContent"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/plan-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync",
                    "plan_kind": "gemini_cli_sync",
                    "plan": {
                        "request_id": "req-gemini-cli-direct-123",
                        "provider_name": "gemini",
                        "provider_id": "provider-gemini-cli-direct-123",
                        "endpoint_id": "endpoint-gemini-cli-direct-123",
                        "key_id": "key-gemini-cli-direct-123",
                        "method": "POST",
                        "url": "https://generativelanguage.googleapis.com/v1beta/models/gemini-cli:generateContent",
                        "headers": {
                            "x-goog-api-key": "upstream-key",
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "contents": []
                            }
                        },
                        "stream": false,
                        "client_api_format": "gemini:cli",
                        "provider_api_format": "gemini:cli",
                        "model_name": "gemini-cli"
                    },
                    "report_kind": "gemini_cli_sync_success",
                    "report_context": {
                        "user_id": "user-gemini-cli-direct-123",
                        "api_key_id": "key-gemini-cli-direct-123",
                        "client_api_format": "gemini:cli"
                    }
                }))
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
                        .status(StatusCode::OK)
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
                "request_id": "req-gemini-cli-direct-123",
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "candidates": [],
                        "usageMetadata": {
                            "promptTokenCount": 5,
                            "candidatesTokenCount": 8,
                            "totalTokenCount": 13
                        }
                    }
                },
                "telemetry": {
                    "elapsed_ms": 54
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

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/v1beta/models/gemini-cli:generateContent"
        ))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header("user-agent", "GeminiCLI/1.0")
        .header(TRACE_ID_HEADER, "trace-gemini-cli-direct-123")
        .body("{\"contents\":[]}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(*report_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_falls_back_to_control_execute_when_openai_chat_direct_sync_errors() {
    let execute_hits = Arc::new(Mutex::new(0usize));
    let execute_hits_clone = Arc::clone(&execute_hits);
    let report_hits = Arc::new(Mutex::new(0usize));
    let report_hits_clone = Arc::clone(&report_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);

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
                        "user_id": "user-chat-fallback-123",
                        "api_key_id": "key-chat-fallback-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/chat/completions"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/plan-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync",
                    "plan_kind": "openai_chat_sync",
                    "plan": {
                        "request_id": "req-openai-chat-fallback-123",
                        "provider_name": "openai",
                        "provider_id": "provider-openai-chat-fallback-123",
                        "endpoint_id": "endpoint-openai-chat-fallback-123",
                        "key_id": "key-openai-chat-fallback-123",
                        "method": "POST",
                        "url": "https://api.openai.example/v1/chat/completions",
                        "headers": {
                            "authorization": "Bearer upstream-key",
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "model": "gpt-5",
                                "messages": []
                            }
                        },
                        "stream": false,
                        "client_api_format": "openai:chat",
                        "provider_api_format": "openai:chat",
                        "model_name": "gpt-5"
                    },
                    "report_kind": "openai_chat_sync_success",
                    "report_context": {
                        "user_id": "user-chat-fallback-123",
                        "api_key_id": "key-chat-fallback-123"
                    }
                }))
            }),
        )
        .route(
            "/api/internal/gateway/execute-sync",
            any(move |_request: Request| {
                let execute_hits_inner = Arc::clone(&execute_hits_clone);
                async move {
                    *execute_hits_inner.lock().expect("mutex should lock") += 1;
                    let mut response = Response::builder()
                        .status(StatusCode::OK)
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
            "/v1/chat/completions",
            any(move |_request: Request| {
                let public_hits_inner = Arc::clone(&public_hits_clone);
                async move {
                    *public_hits_inner.lock().expect("mutex should lock") += 1;
                    (StatusCode::IM_A_TEAPOT, Body::from("public-route-hit"))
                }
            }),
        );

    let executor = Router::new().route(
        "/v1/execute/sync",
        any(|_request: Request| async move {
            Json(json!({
                "request_id": "req-openai-chat-fallback-123",
                "status_code": 429,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "error": {
                            "message": "rate limited"
                        }
                    }
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

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-openai-chat-fallback-123")
        .body("{\"model\":\"gpt-5\",\"messages\":[]}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.text().await.expect("body should read"),
        "{\"fallback\":true}"
    );
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*report_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
