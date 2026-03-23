use super::*;

#[tokio::test]
async fn gateway_executes_openai_cli_stream_via_executor_decision() {
    let decision_hits = Arc::new(Mutex::new(0usize));
    let decision_hits_clone = Arc::clone(&decision_hits);
    let plan_hits = Arc::new(Mutex::new(0usize));
    let plan_hits_clone = Arc::clone(&plan_hits);
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
                        "user_id": "user-openai-cli-stream-decision-123",
                        "api_key_id": "key-openai-cli-stream-decision-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/responses"
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
                        "decision_kind": "openai_cli_stream",
                        "request_id": "req-openai-cli-stream-decision-123",
                        "candidate_id": "cand-openai-cli-stream-decision-123",
                        "provider_name": "openai",
                        "provider_id": "provider-openai-cli-stream-decision-123",
                        "endpoint_id": "endpoint-openai-cli-stream-decision-123",
                        "key_id": "key-openai-cli-stream-decision-123",
                        "upstream_base_url": "https://api.openai.example",
                        "auth_header": "authorization",
                        "auth_value": "Bearer upstream-key",
                        "provider_api_format": "openai:cli",
                        "client_api_format": "openai:cli",
                        "model_name": "gpt-5",
                        "mapped_model": "gpt-5-upstream",
                        "prompt_cache_key": "cache-key-123",
                        "upstream_url": "https://api.openai.example/v1/responses",
                        "provider_request_headers": {
                            "content-type": "application/json",
                            "authorization": "Bearer upstream-key",
                            "x-provider-extra": "1"
                        },
                        "provider_request_body": {
                            "model": "gpt-5-upstream",
                            "input": "hello",
                            "stream": true,
                            "metadata": {"decision": "exact"}
                        },
                        "content_type": "application/json",
                        "report_kind": "openai_cli_stream_success",
                        "report_context": {
                            "user_id": "user-openai-cli-stream-decision-123",
                            "api_key_id": "key-openai-cli-stream-decision-123",
                            "client_api_format": "openai:cli"
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
        any(|request: Request| async move {
            let (_parts, body) = request.into_parts();
            let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
            let payload: serde_json::Value =
                serde_json::from_slice(&raw_body).expect("executor payload should parse");
            assert_eq!(
                payload.get("url").and_then(|value| value.as_str()),
                Some("https://api.openai.example/v1/responses")
            );
            assert_eq!(
                payload
                    .get("headers")
                    .and_then(|value| value.get("authorization"))
                    .and_then(|value| value.as_str()),
                Some("Bearer upstream-key")
            );
            assert_eq!(
                payload
                    .get("headers")
                    .and_then(|value| value.get("accept"))
                    .and_then(|value| value.as_str()),
                Some("text/event-stream")
            );
            assert_eq!(
                payload
                    .get("headers")
                    .and_then(|value| value.get("x-provider-extra"))
                    .and_then(|value| value.as_str()),
                Some("1")
            );
            assert_eq!(
                payload
                    .get("body")
                    .and_then(|value| value.get("json_body"))
                    .and_then(|value| value.get("model"))
                    .and_then(|value| value.as_str()),
                Some("gpt-5-upstream")
            );
            assert_eq!(
                payload
                    .get("body")
                    .and_then(|value| value.get("json_body"))
                    .and_then(|value| value.get("stream"))
                    .and_then(|value| value.as_bool()),
                Some(true)
            );
            assert_eq!(
                payload
                    .get("body")
                    .and_then(|value| value.get("json_body"))
                    .and_then(|value| value.get("metadata"))
                    .and_then(|value| value.get("decision"))
                    .and_then(|value| value.as_str()),
                Some("exact")
            );
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
        .header(TRACE_ID_HEADER, "trace-openai-cli-stream-decision-123")
        .body("{\"model\":\"gpt-5\",\"input\":\"hello\",\"stream\":true}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.text().await.expect("body should read"),
        "event: response.completed\ndata: {\"type\":\"response.completed\"}\n\n"
    );
    wait_until(300, || *report_hits.lock().expect("mutex should lock") == 1).await;
    assert_eq!(*decision_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*plan_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
