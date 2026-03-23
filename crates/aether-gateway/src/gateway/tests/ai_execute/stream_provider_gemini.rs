use super::*;

#[tokio::test]
async fn gateway_executes_gemini_chat_stream_via_executor_decision() {
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
                    "route_family": "gemini",
                    "route_kind": "chat",
                    "auth_endpoint_signature": "gemini:chat",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-gemini-stream-decision-123",
                        "api_key_id": "key-gemini-stream-decision-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1beta/models/gemini-2.5-pro:streamGenerateContent"
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
                        "decision_kind": "gemini_chat_stream",
                        "request_id": "req-gemini-chat-stream-decision-123",
                        "candidate_id": "cand-gemini-chat-stream-decision-123",
                        "provider_name": "gemini",
                        "provider_id": "provider-gemini-chat-stream-decision-123",
                        "endpoint_id": "endpoint-gemini-chat-stream-decision-123",
                        "key_id": "key-gemini-chat-stream-decision-123",
                        "upstream_base_url": "https://generativelanguage.googleapis.com",
                        "upstream_url": "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro-upstream:streamGenerateContent",
                        "auth_header": "x-goog-api-key",
                        "auth_value": "upstream-key",
                        "provider_api_format": "gemini:chat",
                        "client_api_format": "gemini:chat",
                        "model_name": "gemini-2.5-pro",
                        "mapped_model": "gemini-2.5-pro-upstream",
                        "prompt_cache_key": "cache-key-123",
                        "provider_request_headers": {
                            "content-type": "application/json",
                            "x-goog-api-key": "upstream-key",
                            "x-provider-extra": "1"
                        },
                        "provider_request_body": {
                            "contents": [],
                            "generationConfig": {"temperature": 0.2},
                            "prompt_cache_key": "cache-key-123"
                        },
                        "content_type": "application/json",
                        "report_kind": "gemini_chat_stream_success",
                        "report_context": {
                            "user_id": "user-gemini-stream-decision-123",
                            "api_key_id": "key-gemini-stream-decision-123",
                            "client_api_format": "gemini:chat"
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
                Some(
                    "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro-upstream:streamGenerateContent"
                )
            );
            assert_eq!(
                payload
                    .get("headers")
                    .and_then(|value| value.get("x-goog-api-key"))
                    .and_then(|value| value.as_str()),
                Some("upstream-key")
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
                    .and_then(|value| value.get("prompt_cache_key"))
                    .and_then(|value| value.as_str()),
                Some("cache-key-123")
            );
            assert_eq!(
                payload
                    .get("body")
                    .and_then(|value| value.get("json_body"))
                    .and_then(|value| value.get("generationConfig"))
                    .and_then(|value| value.get("temperature"))
                    .and_then(|value| value.as_f64()),
                Some(0.2)
            );
            assert!(
                payload
                    .get("body")
                    .and_then(|value| value.get("json_body"))
                    .and_then(|value| value.get("stream"))
                    .is_none()
            );
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
        .header(TRACE_ID_HEADER, "trace-gemini-chat-stream-decision-123")
        .body("{\"contents\":[]}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.text().await.expect("body should read"),
        "data: {\"candidates\":[]}\n\n"
    );
    wait_until(300, || *report_hits.lock().expect("mutex should lock") == 1).await;
    assert_eq!(*decision_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*plan_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_gemini_cli_stream_via_executor_decision() {
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
                    "route_family": "gemini",
                    "route_kind": "cli",
                    "auth_endpoint_signature": "gemini:cli",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-gemini-cli-stream-decision-123",
                        "api_key_id": "key-gemini-cli-stream-decision-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1beta/models/gemini-cli:streamGenerateContent"
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
                        "decision_kind": "gemini_cli_stream",
                        "request_id": "req-gemini-cli-stream-decision-123",
                        "candidate_id": "cand-gemini-cli-stream-decision-123",
                        "provider_name": "gemini",
                        "provider_id": "provider-gemini-cli-stream-decision-123",
                        "endpoint_id": "endpoint-gemini-cli-stream-decision-123",
                        "key_id": "key-gemini-cli-stream-decision-123",
                        "upstream_base_url": "https://generativelanguage.googleapis.com",
                        "upstream_url": "https://generativelanguage.googleapis.com/v1beta/models/gemini-cli-upstream:streamGenerateContent",
                        "auth_header": "x-goog-api-key",
                        "auth_value": "upstream-key",
                        "provider_api_format": "gemini:cli",
                        "client_api_format": "gemini:cli",
                        "model_name": "gemini-cli",
                        "mapped_model": "gemini-cli-upstream",
                        "prompt_cache_key": "cache-key-123",
                        "provider_request_headers": {
                            "content-type": "application/json",
                            "x-goog-api-key": "upstream-key",
                            "x-provider-extra": "1"
                        },
                        "provider_request_body": {
                            "contents": [],
                            "generationConfig": {"temperature": 0.2},
                            "prompt_cache_key": "cache-key-123"
                        },
                        "content_type": "application/json",
                        "report_kind": "gemini_cli_stream_success",
                        "report_context": {
                            "user_id": "user-gemini-cli-stream-decision-123",
                            "api_key_id": "key-gemini-cli-stream-decision-123",
                            "client_api_format": "gemini:cli"
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
                Some(
                    "https://generativelanguage.googleapis.com/v1beta/models/gemini-cli-upstream:streamGenerateContent"
                )
            );
            assert_eq!(
                payload
                    .get("headers")
                    .and_then(|value| value.get("x-goog-api-key"))
                    .and_then(|value| value.as_str()),
                Some("upstream-key")
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
                    .and_then(|value| value.get("prompt_cache_key"))
                    .and_then(|value| value.as_str()),
                Some("cache-key-123")
            );
            assert_eq!(
                payload
                    .get("body")
                    .and_then(|value| value.get("json_body"))
                    .and_then(|value| value.get("generationConfig"))
                    .and_then(|value| value.get("temperature"))
                    .and_then(|value| value.as_f64()),
                Some(0.2)
            );
            assert!(
                payload
                    .get("body")
                    .and_then(|value| value.get("json_body"))
                    .and_then(|value| value.get("stream"))
                    .is_none()
            );
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
        .header(TRACE_ID_HEADER, "trace-gemini-cli-stream-decision-123")
        .body("{\"contents\":[]}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.text().await.expect("body should read"),
        "data: {\"candidates\":[]}\n\n"
    );
    wait_until(300, || *report_hits.lock().expect("mutex should lock") == 1).await;
    assert_eq!(*decision_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*plan_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_gemini_cli_v1internal_stream_via_executor_decision() {
    use base64::Engine as _;

    let decision_hits = Arc::new(Mutex::new(0usize));
    let decision_hits_clone = Arc::clone(&decision_hits);
    let plan_hits = Arc::new(Mutex::new(0usize));
    let plan_hits_clone = Arc::clone(&plan_hits);
    let report_hits = Arc::new(Mutex::new(0usize));
    let report_hits_clone = Arc::clone(&report_hits);
    let seen_report = Arc::new(Mutex::new(None::<serde_json::Value>));
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
                    "route_family": "gemini",
                    "route_kind": "cli",
                    "auth_endpoint_signature": "gemini:cli",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-gemini-cli-v1internal-stream-123",
                        "api_key_id": "key-gemini-cli-v1internal-stream-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1beta/models/gemini-cli:streamGenerateContent"
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
                        "decision_kind": "gemini_cli_stream",
                        "request_id": "req-gemini-cli-v1internal-stream-123",
                        "candidate_id": "cand-gemini-cli-v1internal-stream-123",
                        "provider_name": "gemini_cli",
                        "provider_id": "provider-gemini-cli-v1internal-stream-123",
                        "endpoint_id": "endpoint-gemini-cli-v1internal-stream-123",
                        "key_id": "key-gemini-cli-v1internal-stream-123",
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
                            "generationConfig": {"temperature": 0.2}
                        },
                        "content_type": "application/json",
                        "report_kind": "gemini_cli_stream_success",
                        "report_context": {
                            "user_id": "user-gemini-cli-v1internal-stream-123",
                            "api_key_id": "key-gemini-cli-v1internal-stream-123",
                            "provider_name": "gemini_cli",
                            "provider_id": "provider-gemini-cli-v1internal-stream-123",
                            "endpoint_id": "endpoint-gemini-cli-v1internal-stream-123",
                            "key_id": "key-gemini-cli-v1internal-stream-123",
                            "request_id": "req-gemini-cli-v1internal-stream-123",
                            "client_api_format": "gemini:cli",
                            "provider_api_format": "gemini:cli",
                            "model": "gemini-cli",
                            "mapped_model": "gemini-cli-upstream",
                            "has_envelope": true,
                            "envelope_name": "gemini_cli:v1internal",
                            "needs_conversion": false
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
            any(move |request: Request| {
                let report_hits_inner = Arc::clone(&report_hits_clone);
                let seen_report_inner = Arc::clone(&seen_report_clone);
                async move {
                    *report_hits_inner.lock().expect("mutex should lock") += 1;
                    let (_parts, body) = request.into_parts();
                    let body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&body).expect("report payload should parse");
                    *seen_report_inner.lock().expect("mutex should lock") = Some(payload);
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
                "{\"type\":\"data\",\"payload\":{\"kind\":\"data\",\"text\":\"data: {\\\"response\\\":{\\\"candidates\\\":[{\\\"content\\\":{\\\"parts\\\":[{\\\"text\\\":\\\"Hello Gemini CLI v1internal\\\"}],\\\"role\\\":\\\"model\\\"},\\\"index\\\":0}],\\\"modelVersion\\\":\\\"gemini-cli-upstream\\\"}}\\n\\n\"}}\n",
                "{\"type\":\"telemetry\",\"payload\":{\"kind\":\"telemetry\",\"telemetry\":{\"elapsed_ms\":28,\"upstream_bytes\":24}}}\n",
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
        .header("x-goog-api-key", "client-key")
        .header(TRACE_ID_HEADER, "trace-gemini-cli-v1internal-stream-123")
        .body("{\"contents\":[]}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.text().await.expect("body should read"),
        "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello Gemini CLI v1internal\"}],\"role\":\"model\"},\"index\":0}],\"modelVersion\":\"gemini-cli-upstream\"}\n\n"
    );
    wait_until(300, || *report_hits.lock().expect("mutex should lock") == 1).await;
    let report_payload = seen_report
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("report-stream should be captured");
    let report_body = base64::engine::general_purpose::STANDARD
        .decode(
            report_payload
                .get("body_base64")
                .and_then(|value| value.as_str())
                .expect("report body should exist"),
        )
        .expect("report body should decode");
    let report_text = String::from_utf8(report_body).expect("report body should be utf8");
    assert!(report_text.contains("\"Hello Gemini CLI v1internal\""));
    assert!(!report_text.contains("\"response\":{"));
    assert_eq!(*decision_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*plan_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
