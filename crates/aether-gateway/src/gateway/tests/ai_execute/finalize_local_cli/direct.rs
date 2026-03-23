use super::*;

#[tokio::test]
async fn gateway_executes_openai_cli_sync_upstream_stream_via_local_finalize_response() {
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
                    "route_family": "openai",
                    "route_kind": "cli",
                    "auth_endpoint_signature": "openai:cli",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-cli-stream-sync-direct-123",
                        "api_key_id": "key-cli-stream-sync-direct-123",
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
                    "request_id": "req-openai-cli-stream-sync-direct-123",
                    "candidate_id": "cand-openai-cli-stream-sync-direct-123",
                    "provider_name": "openai",
                    "provider_id": "provider-openai-cli-stream-sync-direct-123",
                    "endpoint_id": "endpoint-openai-cli-stream-sync-direct-123",
                    "key_id": "key-openai-cli-stream-sync-direct-123",
                    "upstream_base_url": "https://api.openai.example",
                    "upstream_url": "https://api.openai.example/v1/responses",
                    "auth_header": "authorization",
                    "auth_value": "Bearer upstream-key",
                    "provider_api_format": "openai:cli",
                    "client_api_format": "openai:cli",
                    "model_name": "gpt-5",
                    "mapped_model": "gpt-5",
                    "provider_request_headers": {
                        "authorization": "Bearer upstream-key",
                        "content-type": "application/json",
                        "accept": "text/event-stream"
                    },
                    "provider_request_body": {
                        "model": "gpt-5",
                        "input": "hello",
                        "stream": true
                    },
                    "content_type": "application/json",
                    "upstream_is_stream": true,
                    "report_kind": "openai_cli_sync_finalize",
                    "report_context": {
                        "user_id": "user-cli-stream-sync-direct-123",
                        "api_key_id": "key-cli-stream-sync-direct-123",
                        "provider_id": "provider-openai-cli-stream-sync-direct-123",
                        "endpoint_id": "endpoint-openai-cli-stream-sync-direct-123",
                        "key_id": "key-openai-cli-stream-sync-direct-123",
                        "client_api_format": "openai:cli",
                        "provider_api_format": "openai:cli",
                        "request_id": "req-openai-cli-stream-sync-direct-123",
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
                            "{\"id\":\"ignored-cli-finalize-response\",\"object\":\"response\",\"output\":[]}",
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
                "request_id": "req-openai-cli-stream-sync-direct-123",
                "status_code": 200,
                "headers": {
                    "content-type": "text/event-stream"
                },
                "body": {
                    "body_bytes_b64": base64::engine::general_purpose::STANDARD.encode(
                        concat!(
                            "event: response.created\n",
                            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_stream_001\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"in_progress\",\"output\":[]}}\n\n",
                            "event: response.output_text.delta\n",
                            "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"Hello\"}\n\n",
                            "event: response.completed\n",
                            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_stream_001\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":2,\"total_tokens\":3}}}\n\n"
                        )
                    )
                },
                "telemetry": {
                    "elapsed_ms": 29
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
        .post(format!("{gateway_url}/v1/responses"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-openai-cli-stream-sync-direct-123")
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
            "id": "resp_stream_001",
            "object": "response",
            "model": "gpt-5",
            "status": "completed",
            "output": [],
            "usage": {
                "input_tokens": 1,
                "output_tokens": 2,
                "total_tokens": 3
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
