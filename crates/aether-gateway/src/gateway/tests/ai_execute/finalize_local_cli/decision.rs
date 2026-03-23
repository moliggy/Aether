use super::*;

#[tokio::test]
async fn gateway_executes_openai_cli_sync_via_executor_finalize_decision() {
    #[derive(Debug, Clone)]
    struct SeenReportSyncRequest {
        trace_id: String,
        report_kind: String,
        status_code: u64,
        upstream_id: String,
        client_id: String,
    }
    #[derive(Debug, Clone)]
    struct SeenExecutorSyncRequest {
        url: String,
        provider_api_format: String,
        client_api_format: String,
        body: serde_json::Value,
    }

    let seen_report = Arc::new(Mutex::new(None::<SeenReportSyncRequest>));
    let seen_report_clone = Arc::clone(&seen_report);
    let seen_executor = Arc::new(Mutex::new(None::<SeenExecutorSyncRequest>));
    let seen_executor_clone = Arc::clone(&seen_executor);
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
                        "user_id": "user-cli-finalize-123",
                        "api_key_id": "key-cli-finalize-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/responses"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync_decision",
                    "decision_kind": "openai_cli_sync",
                    "request_id": "req-openai-cli-finalize-123",
                    "candidate_id": "cand-openai-cli-finalize-123",
                    "provider_name": "gemini",
                    "provider_id": "provider-openai-cli-finalize-123",
                    "endpoint_id": "endpoint-openai-cli-finalize-123",
                    "key_id": "key-openai-cli-finalize-123",
                    "upstream_base_url": "https://api.gemini.example",
                    "upstream_url": "https://api.gemini.example/v1beta/models/gemini-2.5-pro:generateContent",
                    "auth_header": "authorization",
                    "auth_value": "Bearer upstream-key",
                    "provider_api_format": "gemini:cli",
                    "client_api_format": "openai:cli",
                    "model_name": "gpt-5",
                    "mapped_model": "gemini-2.5-pro",
                    "provider_request_headers": {
                        "authorization": "Bearer upstream-key",
                        "content-type": "application/json",
                        "x-provider-extra": "1"
                    },
                    "provider_request_body": {
                        "contents": []
                    },
                    "content_type": "application/json",
                    "report_kind": "openai_cli_sync_finalize",
                    "report_context": {
                        "user_id": "user-cli-finalize-123",
                        "api_key_id": "key-cli-finalize-123",
                        "provider_id": "provider-openai-cli-finalize-123",
                        "endpoint_id": "endpoint-openai-cli-finalize-123",
                        "key_id": "key-openai-cli-finalize-123",
                        "client_api_format": "openai:cli",
                        "provider_api_format": "gemini:cli",
                        "request_id": "req-openai-cli-finalize-123",
                        "model": "gpt-5"
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
                                .and_then(|value| {
                                    value
                                        .get("responseId")
                                        .or_else(|| value.get("id"))
                                })
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
        any(move |request: Request| {
            let seen_executor_inner = Arc::clone(&seen_executor_clone);
            async move {
                let raw_body = to_bytes(request.into_body(), usize::MAX)
                    .await
                    .expect("body should read");
                let payload: serde_json::Value =
                    serde_json::from_slice(&raw_body).expect("executor payload should parse");
                *seen_executor_inner.lock().expect("mutex should lock") =
                    Some(SeenExecutorSyncRequest {
                        url: payload
                            .get("url")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        provider_api_format: payload
                            .get("provider_api_format")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        client_api_format: payload
                            .get("client_api_format")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        body: payload
                            .get("body")
                            .and_then(|value| value.get("json_body"))
                            .cloned()
                            .unwrap_or_else(|| json!({})),
                    });
                Json(json!({
                    "request_id": "req-openai-cli-finalize-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "responseId": "upstream-cli-123",
                            "candidates": [{
                                "content": {
                                    "parts": [{"text": "Hello Gemini CLI"}],
                                    "role": "model"
                                },
                                "finishReason": "STOP",
                                "index": 0
                            }],
                            "modelVersion": "gemini-2.5-pro-upstream",
                            "usageMetadata": {
                                "promptTokenCount": 2,
                                "candidatesTokenCount": 3,
                                "totalTokenCount": 5
                            }
                        }
                    },
                    "telemetry": {
                        "elapsed_ms": 37
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

    let started_at = std::time::Instant::now();
    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/responses"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-openai-cli-finalize-123")
        .body("{\"model\":\"gpt-5\",\"input\":\"hello\"}")
        .send()
        .await
        .expect("request should succeed");
    let elapsed = started_at.elapsed();

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "id": "upstream-cli-123",
            "object": "response",
            "status": "completed",
            "model": "gemini-2.5-pro-upstream",
            "output": [{
                "type": "message",
                "id": "upstream-cli-123_msg",
                "role": "assistant",
                "status": "completed",
                "content": [{
                    "type": "output_text",
                    "text": "Hello Gemini CLI",
                    "annotations": []
                }]
            }],
            "usage": {
                "input_tokens": 2,
                "output_tokens": 3,
                "total_tokens": 5
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
        "trace-openai-cli-finalize-123"
    );
    assert_eq!(seen_report_request.report_kind, "openai_cli_sync_success");
    assert_eq!(seen_report_request.status_code, 200);
    assert_eq!(seen_report_request.upstream_id, "upstream-cli-123");
    assert_eq!(seen_report_request.client_id, "upstream-cli-123");
    let seen_executor_request = seen_executor
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("executor request should be captured");
    assert_eq!(
        seen_executor_request.url,
        "https://api.gemini.example/v1beta/models/gemini-2.5-pro:generateContent"
    );
    assert_eq!(seen_executor_request.provider_api_format, "gemini:cli");
    assert_eq!(seen_executor_request.client_api_format, "openai:cli");
    assert_eq!(seen_executor_request.body, json!({"contents": []}));

    assert_eq!(*report_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
