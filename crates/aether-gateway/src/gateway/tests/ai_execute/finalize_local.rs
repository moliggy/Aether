use super::*;

#[tokio::test]
async fn gateway_executes_openai_chat_sync_upstream_stream_via_local_finalize_response() {
    use base64::Engine as _;

    #[derive(Debug, Clone)]
    struct SeenFinalizeSyncRequest;

    let seen_finalize = Arc::new(Mutex::new(None::<SeenFinalizeSyncRequest>));
    let seen_finalize_clone = Arc::clone(&seen_finalize);
    let report_hits = Arc::new(Mutex::new(0usize));
    let report_hits_clone = Arc::clone(&report_hits);
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
                        "user_id": "user-chat-stream-sync-direct-123",
                        "api_key_id": "key-chat-stream-sync-direct-123",
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
                        "request_id": "req-openai-chat-stream-sync-direct-123",
                        "provider_name": "openai",
                        "provider_id": "provider-openai-chat-stream-sync-direct-123",
                        "endpoint_id": "endpoint-openai-chat-stream-sync-direct-123",
                        "key_id": "key-openai-chat-stream-sync-direct-123",
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
                                "messages": []
                            }
                        },
                        "stream": true,
                        "client_api_format": "openai:chat",
                        "provider_api_format": "openai:chat",
                        "model_name": "gpt-5"
                    },
                    "report_kind": "openai_chat_sync_finalize",
                    "report_context": {
                        "user_id": "user-chat-stream-sync-direct-123",
                        "api_key_id": "key-chat-stream-sync-direct-123",
                        "provider_id": "provider-openai-chat-stream-sync-direct-123",
                        "endpoint_id": "endpoint-openai-chat-stream-sync-direct-123",
                        "key_id": "key-openai-chat-stream-sync-direct-123",
                        "client_api_format": "openai:chat",
                        "provider_api_format": "openai:chat",
                        "request_id": "req-openai-chat-stream-sync-direct-123",
                        "model": "gpt-5",
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
                            "{\"id\":\"ignored-finalize-response\",\"object\":\"chat.completion\",\"choices\":[]}",
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
                "request_id": "req-openai-chat-stream-sync-direct-123",
                "status_code": 200,
                "headers": {
                    "content-type": "text/event-stream"
                },
                "body": {
                    "body_bytes_b64": base64::engine::general_purpose::STANDARD.encode(
                        concat!(
                            "data: {\"id\":\"chatcmpl-stream-sync-upstream-123\",\"object\":\"chat.completion.chunk\",",
                            "\"created\":1,\"model\":\"gpt-5\",\"choices\":[{\"index\":0,",
                            "\"delta\":{\"role\":\"assistant\",\"content\":\"Hello\"},\"finish_reason\":null}]}\n\n",
                            "data: {\"id\":\"chatcmpl-stream-sync-upstream-123\",\"object\":\"chat.completion.chunk\",",
                            "\"model\":\"gpt-5\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"},",
                            "\"finish_reason\":null}]}\n\n",
                            "data: {\"id\":\"chatcmpl-stream-sync-upstream-123\",\"object\":\"chat.completion.chunk\",",
                            "\"model\":\"gpt-5\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                            "data: [DONE]\n\n"
                        )
                    )
                },
                "telemetry": {
                    "elapsed_ms": 31
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
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-openai-chat-stream-sync-direct-123")
        .body("{\"model\":\"gpt-5\",\"messages\":[]}")
        .send()
        .await
        .expect("request should succeed");
    let elapsed = started_at.elapsed();

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "id": "chatcmpl-stream-sync-upstream-123",
            "object": "chat.completion",
            "created": 1,
            "model": "gpt-5",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Hello world"
                },
                "finish_reason": "stop"
            }]
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
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_openai_chat_cross_format_upstream_stream_via_local_finalize_response() {
    use base64::Engine as _;

    #[derive(Debug, Clone)]
    struct SeenReportSyncRequest {
        trace_id: String,
        report_kind: String,
        status_code: u64,
        upstream_id: String,
        client_id: String,
    }

    let seen_report = Arc::new(Mutex::new(None::<SeenReportSyncRequest>));
    let seen_report_clone = Arc::clone(&seen_report);
    let report_hits = Arc::new(Mutex::new(0usize));
    let report_hits_clone = Arc::clone(&report_hits);
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
                        "user_id": "user-openai-chat-xfmt-stream-123",
                        "api_key_id": "key-openai-chat-xfmt-stream-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/chat/completions"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync_decision",
                    "decision_kind": "openai_chat_sync",
                    "request_id": "req-openai-chat-xfmt-stream-123",
                    "candidate_id": "cand-openai-chat-xfmt-stream-123",
                    "provider_name": "gemini",
                    "provider_id": "provider-openai-chat-xfmt-stream-123",
                    "endpoint_id": "endpoint-openai-chat-xfmt-stream-123",
                    "key_id": "key-openai-chat-xfmt-stream-123",
                    "upstream_base_url": "https://api.gemini.example",
                    "upstream_url": "https://api.gemini.example/v1beta/models/gemini-2.5-pro:streamGenerateContent",
                    "auth_header": "authorization",
                    "auth_value": "Bearer upstream-key",
                    "provider_api_format": "gemini:chat",
                    "client_api_format": "openai:chat",
                    "model_name": "gpt-5",
                    "mapped_model": "gemini-2.5-pro",
                    "provider_request_headers": {
                        "authorization": "Bearer upstream-key",
                        "content-type": "application/json",
                        "accept": "text/event-stream"
                    },
                    "provider_request_body": {
                        "contents": [],
                        "stream": true
                    },
                    "content_type": "application/json",
                    "upstream_is_stream": true,
                    "report_kind": "openai_chat_sync_finalize",
                    "report_context": {
                        "user_id": "user-openai-chat-xfmt-stream-123",
                        "api_key_id": "key-openai-chat-xfmt-stream-123",
                        "provider_id": "provider-openai-chat-xfmt-stream-123",
                        "endpoint_id": "endpoint-openai-chat-xfmt-stream-123",
                        "key_id": "key-openai-chat-xfmt-stream-123",
                        "client_api_format": "openai:chat",
                        "provider_api_format": "gemini:chat",
                        "request_id": "req-openai-chat-xfmt-stream-123",
                        "model": "gpt-5",
                        "has_envelope": false,
                        "needs_conversion": true
                    }
                }))
            }),
        )
        .route(
            "/api/internal/gateway/finalize-sync",
            any(|_request: Request| async move {
                (
                    StatusCode::IM_A_TEAPOT,
                    Body::from("finalize-sync-should-not-be-hit"),
                )
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(move |request: Request| {
                let seen_report_inner = Arc::clone(&seen_report_clone);
                let report_hits_inner = Arc::clone(&report_hits_clone);
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
                            upstream_id: payload
                                .get("body_json")
                                .and_then(|value| value.get("responseId"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            client_id: payload
                                .get("client_body_json")
                                .and_then(|value| value.get("id"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        });
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
                "request_id": "req-openai-chat-xfmt-stream-123",
                "status_code": 200,
                "headers": {
                    "content-type": "text/event-stream"
                },
                "body": {
                    "body_bytes_b64": base64::engine::general_purpose::STANDARD.encode(
                        concat!(
                            "data: {\"responseId\":\"resp-gemini-chat-stream-123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello \"}],\"role\":\"model\"},\"index\":0}],\"modelVersion\":\"gemini-2.5-pro-upstream\"}\n\n",
                            "data: {\"responseId\":\"resp-gemini-chat-stream-123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Gemini Chat\"}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"modelVersion\":\"gemini-2.5-pro-upstream\",\"usageMetadata\":{\"promptTokenCount\":1,\"candidatesTokenCount\":2,\"totalTokenCount\":3}}\n\n"
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
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-openai-chat-xfmt-stream-123")
        .body("{\"model\":\"gpt-5\",\"messages\":[]}")
        .send()
        .await
        .expect("request should succeed");
    let elapsed = started_at.elapsed();

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "id": "resp-gemini-chat-stream-123",
            "object": "chat.completion",
            "model": "gemini-2.5-pro-upstream",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Gemini Chat"
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
    assert!(
        elapsed < std::time::Duration::from_millis(350),
        "response should not wait for finalize-sync background task"
    );

    wait_until(700, || {
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
        .expect("report-sync should be captured");
    assert_eq!(
        seen_report_request.trace_id,
        "trace-openai-chat-xfmt-stream-123"
    );
    assert_eq!(seen_report_request.report_kind, "openai_chat_sync_success");
    assert_eq!(seen_report_request.status_code, 200);
    assert_eq!(
        seen_report_request.upstream_id,
        "resp-gemini-chat-stream-123"
    );
    assert_eq!(seen_report_request.client_id, "resp-gemini-chat-stream-123");

    assert_eq!(*report_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_openai_chat_cross_format_tool_use_upstream_stream_via_local_finalize_response(
) {
    use base64::Engine as _;

    #[derive(Debug, Clone)]
    struct SeenReportSyncRequest {
        report_kind: String,
        client_id: String,
    }

    let seen_report = Arc::new(Mutex::new(None::<SeenReportSyncRequest>));
    let seen_report_clone = Arc::clone(&seen_report);
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
                    "route_kind": "chat",
                    "auth_endpoint_signature": "openai:chat",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-openai-chat-xfmt-tool-stream-123",
                        "api_key_id": "key-openai-chat-xfmt-tool-stream-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/chat/completions"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync_decision",
                    "decision_kind": "openai_chat_sync",
                    "request_id": "req-openai-chat-xfmt-tool-stream-123",
                    "candidate_id": "cand-openai-chat-xfmt-tool-stream-123",
                    "provider_name": "claude",
                    "provider_id": "provider-openai-chat-xfmt-tool-stream-123",
                    "endpoint_id": "endpoint-openai-chat-xfmt-tool-stream-123",
                    "key_id": "key-openai-chat-xfmt-tool-stream-123",
                    "upstream_base_url": "https://api.anthropic.example",
                    "upstream_url": "https://api.anthropic.example/v1/messages",
                    "auth_header": "x-api-key",
                    "auth_value": "upstream-secret",
                    "provider_api_format": "claude:chat",
                    "client_api_format": "openai:chat",
                    "model_name": "gpt-5",
                    "mapped_model": "claude-sonnet-4-upstream",
                    "provider_request_headers": {
                        "x-api-key": "upstream-secret",
                        "content-type": "application/json",
                        "accept": "text/event-stream"
                    },
                    "provider_request_body": {
                        "model": "claude-sonnet-4-upstream",
                        "messages": [],
                        "stream": true
                    },
                    "content_type": "application/json",
                    "upstream_is_stream": true,
                    "report_kind": "openai_chat_sync_finalize",
                    "report_context": {
                        "user_id": "user-openai-chat-xfmt-tool-stream-123",
                        "api_key_id": "key-openai-chat-xfmt-tool-stream-123",
                        "provider_id": "provider-openai-chat-xfmt-tool-stream-123",
                        "endpoint_id": "endpoint-openai-chat-xfmt-tool-stream-123",
                        "key_id": "key-openai-chat-xfmt-tool-stream-123",
                        "client_api_format": "openai:chat",
                        "provider_api_format": "claude:chat",
                        "request_id": "req-openai-chat-xfmt-tool-stream-123",
                        "model": "gpt-5",
                        "has_envelope": false,
                        "needs_conversion": true
                    }
                }))
            }),
        )
        .route(
            "/api/internal/gateway/finalize-sync",
            any(|_request: Request| async move {
                (
                    StatusCode::IM_A_TEAPOT,
                    Body::from("finalize-sync-should-not-be-hit"),
                )
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(move |request: Request| {
                let seen_report_inner = Arc::clone(&seen_report_clone);
                let report_hits_inner = Arc::clone(&report_hits_clone);
                async move {
                    let (_parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("report payload should parse");
                    *seen_report_inner.lock().expect("mutex should lock") =
                        Some(SeenReportSyncRequest {
                            report_kind: payload
                                .get("report_kind")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            client_id: payload
                                .get("client_body_json")
                                .and_then(|value| value.get("id"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        });
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
                "request_id": "req-openai-chat-xfmt-tool-stream-123",
                "status_code": 200,
                "headers": {
                    "content-type": "text/event-stream"
                },
                "body": {
                    "body_bytes_b64": base64::engine::general_purpose::STANDARD.encode(
                        concat!(
                            "event: message_start\n",
                            "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_tool_claude_123\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-upstream\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null}}\n\n",
                            "event: content_block_start\n",
                            "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"Checking.\"}}\n\n",
                            "event: content_block_stop\n",
                            "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
                            "event: content_block_start\n",
                            "data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_123\",\"name\":\"get_weather\",\"input\":{\"location\":\"Tokyo\"}}}\n\n",
                            "event: content_block_stop\n",
                            "data: {\"type\":\"content_block_stop\",\"index\":1}\n\n",
                            "event: message_delta\n",
                            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"input_tokens\":5,\"output_tokens\":7}}\n\n",
                            "event: message_stop\n",
                            "data: {\"type\":\"message_stop\"}\n\n"
                        )
                    )
                },
                "telemetry": {
                    "elapsed_ms": 31
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
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-openai-chat-xfmt-tool-stream-123")
        .body("{\"model\":\"gpt-5\",\"messages\":[{\"role\":\"user\",\"content\":\"weather\"}]}")
        .send()
        .await
        .expect("request should succeed");
    let elapsed = started_at.elapsed();

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "id": "msg_tool_claude_123",
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
    assert!(
        elapsed < std::time::Duration::from_millis(350),
        "response should not wait for finalize-sync background task"
    );

    wait_until(700, || *report_hits.lock().expect("mutex should lock") == 1).await;
    let seen_report_request = seen_report
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("report-sync should be captured");
    assert_eq!(seen_report_request.report_kind, "openai_chat_sync_success");
    assert_eq!(seen_report_request.client_id, "msg_tool_claude_123");
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
