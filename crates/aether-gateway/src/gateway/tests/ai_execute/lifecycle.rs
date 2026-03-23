use super::*;

#[tokio::test]
async fn gateway_does_not_block_sync_response_on_report_sync() {
    let report_hits = Arc::new(Mutex::new(0usize));
    let report_hits_clone = Arc::clone(&report_hits);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/auth-context",
            any(|_request: Request| async move {
                Json(json!({
                    "auth_context": {
                        "user_id": "user-chat-async-report-123",
                        "api_key_id": "key-chat-async-report-123",
                        "access_allowed": true
                    }
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
                        "request_id": "req-openai-chat-async-report-123",
                        "provider_name": "openai",
                        "provider_id": "provider-openai-chat-async-report-123",
                        "endpoint_id": "endpoint-openai-chat-async-report-123",
                        "key_id": "key-openai-chat-async-report-123",
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
                        "user_id": "user-chat-async-report-123",
                        "api_key_id": "key-chat-async-report-123"
                    }
                }))
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(move |_request: Request| {
                let report_hits_inner = Arc::clone(&report_hits_clone);
                async move {
                    tokio::time::sleep(std::time::Duration::from_millis(1_500)).await;
                    *report_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({"ok": true}))
                }
            }),
        )
        .route(
            "/v1/chat/completions",
            any(|_request: Request| async move { (StatusCode::IM_A_TEAPOT, Body::from("public")) }),
        );

    let executor = Router::new().route(
        "/v1/execute/sync",
        any(|_request: Request| async move {
            Json(json!({
                "request_id": "req-openai-chat-async-report-123",
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "id": "chatcmpl-async-report-123",
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
                    "elapsed_ms": 12
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

    let started_at = tokio::time::Instant::now();
    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(http::header::AUTHORIZATION, "Bearer sk-test")
        .header(TRACE_ID_HEADER, "trace-openai-chat-async-report-123")
        .body("{\"model\":\"gpt-5\",\"messages\":[]}")
        .send()
        .await
        .expect("request should succeed");
    let elapsed = started_at.elapsed();

    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        elapsed < std::time::Duration::from_millis(1_100),
        "sync response waited too long for report-sync: {:?}",
        elapsed
    );

    wait_until(1_800, || {
        *report_hits.lock().expect("mutex should lock") == 1
    })
    .await;

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_stops_executor_stream_when_client_disconnects() {
    let seen_report = Arc::new(Mutex::new(0usize));
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
                        "user_id": "user-openai-stream-disconnect-123",
                        "api_key_id": "key-openai-stream-disconnect-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/chat/completions"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/plan-stream",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_stream",
                    "plan_kind": "openai_chat_stream",
                    "plan": {
                        "request_id": "req-openai-chat-stream-disconnect-123",
                        "provider_name": "openai",
                        "provider_id": "provider-openai-chat-stream-disconnect-123",
                        "endpoint_id": "endpoint-openai-chat-stream-disconnect-123",
                        "key_id": "key-openai-chat-stream-disconnect-123",
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
                        "user_id": "user-openai-stream-disconnect-123",
                        "api_key_id": "key-openai-stream-disconnect-123"
                    }
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
            "/api/internal/gateway/report-stream",
            any(move |_request: Request| {
                let seen_report_inner = Arc::clone(&seen_report_clone);
                async move {
                    *seen_report_inner.lock().expect("mutex should lock") += 1;
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
        "/v1/execute/stream",
        any(|_request: Request| async move {
            let body_stream = async_stream::stream! {
                yield Ok::<Bytes, Infallible>(Bytes::from_static(
                    b"{\"type\":\"headers\",\"payload\":{\"kind\":\"headers\",\"status_code\":200,\"headers\":{\"content-type\":\"text/event-stream\"}}}\n"
                ));
                yield Ok::<Bytes, Infallible>(Bytes::from_static(
                    b"{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"data: {\\\"id\\\":\\\"chatcmpl-first\\\"}\\n\\n\"}}\n"
                ));
                tokio::time::sleep(std::time::Duration::from_millis(120)).await;
                yield Ok::<Bytes, Infallible>(Bytes::from_static(
                    b"{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"data: [DONE]\\n\\n\"}}\n"
                ));
                yield Ok::<Bytes, Infallible>(Bytes::from_static(
                    b"{\"type\":\"telemetry\",\"payload\":{\"kind\":\"telemetry\",\"telemetry\":{\"elapsed_ms\":41,\"ttfb_ms\":12,\"upstream_bytes\":31}}}\n"
                ));
                yield Ok::<Bytes, Infallible>(Bytes::from_static(
                    b"{\"type\":\"eof\",\"payload\":{\"kind\":\"eof\"}}\n"
                ));
            };
            let mut response = Response::builder()
                .status(StatusCode::OK)
                .body(Body::from_stream(body_stream))
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
        .header(TRACE_ID_HEADER, "trace-openai-chat-stream-disconnect-123")
        .body("{\"model\":\"gpt-5\",\"messages\":[],\"stream\":true}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let mut response = response;
    let first_chunk = response
        .chunk()
        .await
        .expect("first chunk should read")
        .expect("first chunk should exist");
    assert_eq!(
        first_chunk,
        Bytes::from_static(b"data: {\"id\":\"chatcmpl-first\"}\n\n")
    );
    drop(response);

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    assert_eq!(*seen_report.lock().expect("mutex should lock"), 0);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
