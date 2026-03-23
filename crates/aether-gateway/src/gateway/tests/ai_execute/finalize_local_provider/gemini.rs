use super::*;

#[tokio::test]
async fn gateway_executes_gemini_chat_sync_upstream_stream_via_local_finalize_response() {
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
                    "route_family": "gemini",
                    "route_kind": "chat",
                    "auth_endpoint_signature": "gemini:chat",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-gemini-chat-stream-sync-direct-123",
                        "api_key_id": "key-gemini-chat-stream-sync-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1beta/models/gemini-2.5-pro:generateContent"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync_decision",
                    "decision_kind": "gemini_chat_sync",
                    "request_id": "req-gemini-chat-stream-sync-direct-123",
                    "candidate_id": "cand-gemini-chat-stream-sync-direct-123",
                    "provider_name": "gemini",
                    "provider_id": "provider-gemini-chat-stream-sync-direct-123",
                    "endpoint_id": "endpoint-gemini-chat-stream-sync-direct-123",
                    "key_id": "key-gemini-chat-stream-sync-direct-123",
                    "upstream_base_url": "https://generativelanguage.googleapis.com",
                    "upstream_url": "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro-upstream:streamGenerateContent?alt=sse",
                    "auth_header": "x-goog-api-key",
                    "auth_value": "upstream-secret",
                    "provider_api_format": "gemini:chat",
                    "client_api_format": "gemini:chat",
                    "model_name": "gemini-2.5-pro",
                    "mapped_model": "gemini-2.5-pro-upstream",
                    "provider_request_headers": {
                        "content-type": "application/json",
                        "accept": "text/event-stream",
                        "x-goog-api-key": "upstream-secret"
                    },
                    "provider_request_body": {
                        "contents": [],
                        "model": "gemini-2.5-pro-upstream"
                    },
                    "content_type": "application/json",
                    "upstream_is_stream": true,
                    "report_kind": "gemini_chat_sync_finalize",
                    "report_context": {
                        "user_id": "user-gemini-chat-stream-sync-direct-123",
                        "api_key_id": "key-gemini-chat-stream-sync-direct-123",
                        "provider_id": "provider-gemini-chat-stream-sync-direct-123",
                        "endpoint_id": "endpoint-gemini-chat-stream-sync-direct-123",
                        "key_id": "key-gemini-chat-stream-sync-direct-123",
                        "client_api_format": "gemini:chat",
                        "provider_api_format": "gemini:chat",
                        "request_id": "req-gemini-chat-stream-sync-direct-123",
                        "model": "gemini-2.5-pro",
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
                            "{\"candidates\":[],\"modelVersion\":\"ignored-gemini-finalize\"}",
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
                "request_id": "req-gemini-chat-stream-sync-direct-123",
                "status_code": 200,
                "headers": {
                    "content-type": "text/event-stream"
                },
                "body": {
                    "body_bytes_b64": base64::engine::general_purpose::STANDARD.encode(
                        concat!(
                            "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"He\"}],\"role\":\"model\"},\"index\":0}],\"modelVersion\":\"gemini-2.5-pro-upstream\"}\n\n",
                            "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello Gemini\"}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"modelVersion\":\"gemini-2.5-pro-upstream\",\"usageMetadata\":{\"promptTokenCount\":5,\"candidatesTokenCount\":7,\"totalTokenCount\":12}}\n\n"
                        )
                    )
                },
                "telemetry": {
                    "elapsed_ms": 36
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
        .post(format!(
            "{gateway_url}/v1beta/models/gemini-2.5-pro:generateContent"
        ))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-gemini-chat-stream-sync-direct-123")
        .body("{\"contents\":[]}")
        .send()
        .await
        .expect("request should succeed");
    let elapsed = started_at.elapsed();

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
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
                "promptTokenCount": 5,
                "candidatesTokenCount": 7,
                "totalTokenCount": 12
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
async fn gateway_executes_gemini_cli_sync_upstream_stream_via_local_finalize_response() {
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
                    "route_family": "gemini",
                    "route_kind": "cli",
                    "auth_endpoint_signature": "gemini:cli",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-gemini-cli-stream-sync-direct-123",
                        "api_key_id": "key-gemini-cli-stream-sync-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1beta/models/gemini-cli:generateContent"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync_decision",
                    "decision_kind": "gemini_cli_sync",
                    "request_id": "req-gemini-cli-stream-sync-direct-123",
                    "candidate_id": "cand-gemini-cli-stream-sync-direct-123",
                    "provider_name": "gemini",
                    "provider_id": "provider-gemini-cli-stream-sync-direct-123",
                    "endpoint_id": "endpoint-gemini-cli-stream-sync-direct-123",
                    "key_id": "key-gemini-cli-stream-sync-direct-123",
                    "upstream_base_url": "https://generativelanguage.googleapis.com",
                    "upstream_url": "https://generativelanguage.googleapis.com/v1beta/models/gemini-cli-upstream:streamGenerateContent?alt=sse",
                    "auth_header": "x-goog-api-key",
                    "auth_value": "upstream-secret",
                    "provider_api_format": "gemini:cli",
                    "client_api_format": "gemini:cli",
                    "model_name": "gemini-cli",
                    "mapped_model": "gemini-cli-upstream",
                    "provider_request_headers": {
                        "content-type": "application/json",
                        "accept": "text/event-stream",
                        "x-goog-api-key": "upstream-secret"
                    },
                    "provider_request_body": {
                        "contents": [],
                        "generationConfig": {
                            "temperature": 0.2
                        }
                    },
                    "content_type": "application/json",
                    "upstream_is_stream": true,
                    "report_kind": "gemini_cli_sync_finalize",
                    "report_context": {
                        "user_id": "user-gemini-cli-stream-sync-direct-123",
                        "api_key_id": "key-gemini-cli-stream-sync-direct-123",
                        "provider_id": "provider-gemini-cli-stream-sync-direct-123",
                        "endpoint_id": "endpoint-gemini-cli-stream-sync-direct-123",
                        "key_id": "key-gemini-cli-stream-sync-direct-123",
                        "client_api_format": "gemini:cli",
                        "provider_api_format": "gemini:cli",
                        "request_id": "req-gemini-cli-stream-sync-direct-123",
                        "model": "gemini-cli",
                        "has_envelope": true,
                        "envelope_name": "gemini_cli:v1internal",
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
                            "{\"candidates\":[],\"modelVersion\":\"ignored-gemini-cli-finalize\"}",
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
                "request_id": "req-gemini-cli-stream-sync-direct-123",
                "status_code": 200,
                "headers": {
                    "content-type": "text/event-stream"
                },
                "body": {
                    "body_bytes_b64": base64::engine::general_purpose::STANDARD.encode(
                        concat!(
                            "data: {\"response\":{\"responseId\":\"resp_gemini_cli_sync_123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"He\"}],\"role\":\"model\"},\"index\":0}],\"modelVersion\":\"gemini-cli-upstream\"}}\n\n",
                            "data: {\"response\":{\"responseId\":\"resp_gemini_cli_sync_123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello Gemini CLI\"}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"modelVersion\":\"gemini-cli-upstream\",\"usageMetadata\":{\"promptTokenCount\":3,\"candidatesTokenCount\":5,\"totalTokenCount\":10}}}\n\n"
                        )
                    )
                },
                "telemetry": {
                    "elapsed_ms": 33
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
        .post(format!(
            "{gateway_url}/v1beta/models/gemini-cli:generateContent"
        ))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header("user-agent", "GeminiCLI/1.0")
        .header(TRACE_ID_HEADER, "trace-gemini-cli-stream-sync-direct-123")
        .body("{\"contents\":[]}")
        .send()
        .await
        .expect("request should succeed");
    let elapsed = started_at.elapsed();

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "responseId": "resp_gemini_cli_sync_123",
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
                "totalTokenCount": 10
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
async fn gateway_executes_antigravity_gemini_cli_sync_upstream_stream_via_local_finalize_response()
{
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
                    "route_family": "gemini",
                    "route_kind": "cli",
                    "auth_endpoint_signature": "gemini:cli",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-antigravity-cli-stream-sync-direct-123",
                        "api_key_id": "key-antigravity-cli-stream-sync-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1beta/models/gemini-cli:generateContent"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync_decision",
                    "decision_kind": "gemini_cli_sync",
                    "request_id": "req-antigravity-cli-stream-sync-direct-123",
                    "candidate_id": "cand-antigravity-cli-stream-sync-direct-123",
                    "provider_name": "antigravity",
                    "provider_id": "provider-antigravity-cli-stream-sync-direct-123",
                    "endpoint_id": "endpoint-antigravity-cli-stream-sync-direct-123",
                    "key_id": "key-antigravity-cli-stream-sync-direct-123",
                    "upstream_base_url": "https://generativelanguage.googleapis.com",
                    "upstream_url": "https://generativelanguage.googleapis.com/v1beta/models/gemini-cli-upstream:streamGenerateContent?alt=sse",
                    "auth_header": "authorization",
                    "auth_value": "Bearer upstream-secret",
                    "provider_api_format": "gemini:cli",
                    "client_api_format": "gemini:cli",
                    "model_name": "claude-sonnet-4-5",
                    "mapped_model": "claude-sonnet-4-5",
                    "provider_request_headers": {
                        "content-type": "application/json",
                        "accept": "text/event-stream",
                        "authorization": "Bearer upstream-secret"
                    },
                    "provider_request_body": {
                        "contents": [],
                        "generationConfig": {
                            "temperature": 0.2
                        }
                    },
                    "content_type": "application/json",
                    "upstream_is_stream": true,
                    "report_kind": "gemini_cli_sync_finalize",
                    "report_context": {
                        "user_id": "user-antigravity-cli-stream-sync-direct-123",
                        "api_key_id": "key-antigravity-cli-stream-sync-direct-123",
                        "provider_id": "provider-antigravity-cli-stream-sync-direct-123",
                        "endpoint_id": "endpoint-antigravity-cli-stream-sync-direct-123",
                        "key_id": "key-antigravity-cli-stream-sync-direct-123",
                        "client_api_format": "gemini:cli",
                        "provider_api_format": "gemini:cli",
                        "request_id": "req-antigravity-cli-stream-sync-direct-123",
                        "model": "claude-sonnet-4-5",
                        "mapped_model": "claude-sonnet-4-5",
                        "has_envelope": true,
                        "envelope_name": "antigravity:v1internal",
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
                            "{\"candidates\":[],\"modelVersion\":\"ignored-antigravity-finalize\"}",
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
                "request_id": "req-antigravity-cli-stream-sync-direct-123",
                "status_code": 200,
                "headers": {
                    "content-type": "text/event-stream"
                },
                "body": {
                    "body_bytes_b64": base64::engine::general_purpose::STANDARD.encode(
                        concat!(
                            "data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"name\":\"get_weather\",\"args\":{\"city\":\"SF\"}}}],\"role\":\"model\"},\"index\":0}],\"modelVersion\":\"claude-sonnet-4-5\"},\"responseId\":\"resp_antigravity_cli_sync_123\"}\n\n",
                            "data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello Antigravity CLI\"},{\"functionCall\":{\"name\":\"get_weather\",\"args\":{\"city\":\"SF\"}}}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"modelVersion\":\"claude-sonnet-4-5\",\"usageMetadata\":{\"promptTokenCount\":3,\"candidatesTokenCount\":5,\"totalTokenCount\":10}},\"responseId\":\"resp_antigravity_cli_sync_123\"}\n\n"
                        )
                    )
                },
                "telemetry": {
                    "elapsed_ms": 33
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
        .post(format!(
            "{gateway_url}/v1beta/models/gemini-cli:generateContent"
        ))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header("user-agent", "GeminiCLI/1.0")
        .header(
            TRACE_ID_HEADER,
            "trace-antigravity-cli-stream-sync-direct-123",
        )
        .body("{\"contents\":[]}")
        .send()
        .await
        .expect("request should succeed");
    let elapsed = started_at.elapsed();

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "_v1internal_response_id": "resp_antigravity_cli_sync_123",
            "candidates": [{
                "content": {
                    "parts": [
                        {"text": "Hello Antigravity CLI"},
                        {
                            "functionCall": {
                                "name": "get_weather",
                                "args": {"city": "SF"},
                                "id": "call_get_weather_0"
                            }
                        }
                    ],
                    "role": "model"
                },
                "finishReason": "STOP",
                "index": 0
            }],
            "modelVersion": "claude-sonnet-4-5",
            "usageMetadata": {
                "promptTokenCount": 3,
                "candidatesTokenCount": 5,
                "totalTokenCount": 10
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
        "finalize-sync should not be called when antigravity local finalize can downgrade to success report"
    );
    assert_eq!(*report_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
