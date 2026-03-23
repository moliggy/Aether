use super::*;

#[tokio::test]
async fn gateway_finalizes_openai_chat_direct_sync_json_errors_without_control_execute() {
    #[derive(Debug, Clone)]
    struct SeenReportSyncRequest {
        trace_id: String,
        report_kind: String,
        status_code: u64,
        error_message: String,
    }

    let seen_report = Arc::new(Mutex::new(None::<SeenReportSyncRequest>));
    let seen_report_clone = Arc::clone(&seen_report);
    let execute_hits = Arc::new(Mutex::new(0usize));
    let execute_hits_clone = Arc::clone(&execute_hits);
    let report_hits = Arc::new(Mutex::new(0usize));
    let report_hits_clone = Arc::clone(&report_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);
    let finalize_hits = Arc::new(Mutex::new(0usize));
    let finalize_hits_clone = Arc::clone(&finalize_hits);

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
            "/api/internal/gateway/finalize-sync",
            any(move |_request: Request| {
                let finalize_hits_inner = Arc::clone(&finalize_hits_clone);
                async move {
                    *finalize_hits_inner.lock().expect("mutex should lock") += 1;
                    (StatusCode::IM_A_TEAPOT, Body::from("unexpected-finalize"))
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
                    *report_hits_inner.lock().expect("mutex should lock") += 1;
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
                            error_message: payload
                                .get("body_json")
                                .and_then(|value| value.get("error"))
                                .and_then(|value| value.get("message"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        });
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
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

    let started_at = std::time::Instant::now();
    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-openai-chat-fallback-123")
        .body("{\"model\":\"gpt-5\",\"messages\":[]}")
        .send()
        .await
        .expect("request should succeed");
    let elapsed = started_at.elapsed();

    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        response.text().await.expect("body should read"),
        "{\"error\":{\"message\":\"rate limited\"}}"
    );
    assert!(
        elapsed < std::time::Duration::from_millis(450),
        "response should not wait for report-sync background task"
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
        "trace-openai-chat-fallback-123"
    );
    assert_eq!(seen_report_request.report_kind, "openai_chat_sync_error");
    assert_eq!(seen_report_request.status_code, 429);
    assert_eq!(seen_report_request.error_message, "rate limited");

    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*report_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*finalize_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
