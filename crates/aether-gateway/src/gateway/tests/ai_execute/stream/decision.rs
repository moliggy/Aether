use super::*;

#[tokio::test]
async fn gateway_executes_openai_chat_stream_via_executor_decision() {
    use base64::Engine as _;

    #[derive(Debug, Clone)]
    struct SeenDecisionStreamRequest {
        trace_id: String,
        path: String,
        stream: bool,
        auth_context_present: bool,
    }

    #[derive(Debug, Clone)]
    struct SeenExecutorStreamRequest {
        trace_id: String,
        url: String,
        model: String,
        stream: bool,
        prompt_cache_key: String,
        accept: String,
        authorization: String,
        provider_extra: String,
        decision_marker: String,
    }

    #[derive(Debug, Clone)]
    struct SeenReportStreamRequest {
        trace_id: String,
        report_kind: String,
        status_code: u64,
        body: String,
    }

    let seen_decision = Arc::new(Mutex::new(None::<SeenDecisionStreamRequest>));
    let seen_decision_clone = Arc::clone(&seen_decision);
    let seen_executor = Arc::new(Mutex::new(None::<SeenExecutorStreamRequest>));
    let seen_executor_clone = Arc::clone(&seen_executor);
    let seen_report = Arc::new(Mutex::new(None::<SeenReportStreamRequest>));
    let seen_report_clone = Arc::clone(&seen_report);
    let plan_hits = Arc::new(Mutex::new(0usize));
    let plan_hits_clone = Arc::clone(&plan_hits);
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
                        "user_id": "user-openai-stream-decision-123",
                        "api_key_id": "key-openai-stream-decision-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/chat/completions"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-stream",
            any(move |request: Request| {
                let seen_decision_inner = Arc::clone(&seen_decision_clone);
                async move {
                    let (parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("decision payload should parse");
                    *seen_decision_inner.lock().expect("mutex should lock") =
                        Some(SeenDecisionStreamRequest {
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
                            auth_context_present: payload
                                .get("auth_context")
                                .is_some_and(|value| !value.is_null()),
                        });
                    Json(json!({
                        "action": "executor_stream_decision",
                        "decision_kind": "openai_chat_stream",
                        "request_id": "req-openai-chat-stream-decision-123",
                        "candidate_id": "cand-openai-chat-stream-decision-123",
                        "provider_name": "openai",
                        "provider_id": "provider-openai-chat-stream-decision-123",
                        "endpoint_id": "endpoint-openai-chat-stream-decision-123",
                        "key_id": "key-openai-chat-stream-decision-123",
                        "upstream_base_url": "https://api.openai.example",
                        "auth_header": "authorization",
                        "auth_value": "Bearer upstream-key",
                        "provider_api_format": "openai:chat",
                        "client_api_format": "openai:chat",
                        "model_name": "gpt-5",
                        "mapped_model": "gpt-5-upstream",
                        "prompt_cache_key": "cache-key-123",
                        "upstream_url": "https://api.openai.example/v1/chat/completions",
                        "provider_request_headers": {
                            "content-type": "application/json",
                            "authorization": "Bearer upstream-key",
                            "x-provider-extra": "1"
                        },
                        "provider_request_body": {
                            "model": "gpt-5-upstream",
                            "messages": [],
                            "stream": true,
                            "prompt_cache_key": "cache-key-123",
                            "metadata": {"decision": "exact"}
                        },
                        "content_type": "application/json",
                        "report_kind": "openai_chat_stream_success",
                        "report_context": {
                            "user_id": "user-openai-stream-decision-123",
                            "api_key_id": "key-openai-stream-decision-123",
                            "request_id": "req-openai-chat-stream-decision-123",
                            "model": "gpt-5",
                            "provider_name": "openai",
                            "provider_id": "provider-openai-chat-stream-decision-123",
                            "endpoint_id": "endpoint-openai-chat-stream-decision-123",
                            "key_id": "key-openai-chat-stream-decision-123",
                            "provider_api_format": "openai:chat",
                            "client_api_format": "openai:chat",
                            "mapped_model": "gpt-5-upstream",
                            "original_headers": {
                                "content-type": "application/json"
                            },
                            "original_request_body": {
                                "model": "gpt-5",
                                "messages": [],
                                "stream": true
                            }
                        }
                    }))
                }
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
        any(move |request: Request| {
            let seen_executor_inner = Arc::clone(&seen_executor_clone);
            async move {
                let (parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value =
                    serde_json::from_slice(&raw_body).expect("executor payload should parse");
                *seen_executor_inner.lock().expect("mutex should lock") =
                    Some(SeenExecutorStreamRequest {
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
                        model: payload
                            .get("body")
                            .and_then(|value| value.get("json_body"))
                            .and_then(|value| value.get("model"))
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        stream: payload
                            .get("body")
                            .and_then(|value| value.get("json_body"))
                            .and_then(|value| value.get("stream"))
                            .and_then(|value| value.as_bool())
                            .unwrap_or(false),
                        prompt_cache_key: payload
                            .get("body")
                            .and_then(|value| value.get("json_body"))
                            .and_then(|value| value.get("prompt_cache_key"))
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        accept: payload
                            .get("headers")
                            .and_then(|value| value.get("accept"))
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        authorization: payload
                            .get("headers")
                            .and_then(|value| value.get("authorization"))
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        provider_extra: payload
                            .get("headers")
                            .and_then(|value| value.get("x-provider-extra"))
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        decision_marker: payload
                            .get("body")
                            .and_then(|value| value.get("json_body"))
                            .and_then(|value| value.get("metadata"))
                            .and_then(|value| value.get("decision"))
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                    });
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
        .header(TRACE_ID_HEADER, "trace-openai-chat-stream-decision-123")
        .body("{\"model\":\"gpt-5\",\"messages\":[],\"stream\":true}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.text().await.expect("body should read"),
        "data: {\"id\":\"chatcmpl-123\"}\n\ndata: [DONE]\n\n"
    );

    let seen_decision_request = seen_decision
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("decision-stream should be captured");
    assert_eq!(
        seen_decision_request.trace_id,
        "trace-openai-chat-stream-decision-123"
    );
    assert_eq!(seen_decision_request.path, "/v1/chat/completions");
    assert!(seen_decision_request.stream);
    assert!(!seen_decision_request.auth_context_present);

    let seen_executor_request = seen_executor
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("executor stream should be captured");
    assert_eq!(
        seen_executor_request.trace_id,
        "trace-openai-chat-stream-decision-123"
    );
    assert_eq!(
        seen_executor_request.url,
        "https://api.openai.example/v1/chat/completions"
    );
    assert_eq!(seen_executor_request.model, "gpt-5-upstream");
    assert!(seen_executor_request.stream);
    assert_eq!(seen_executor_request.prompt_cache_key, "cache-key-123");
    assert_eq!(seen_executor_request.accept, "text/event-stream");
    assert_eq!(seen_executor_request.authorization, "Bearer upstream-key");
    assert_eq!(seen_executor_request.provider_extra, "1");
    assert_eq!(seen_executor_request.decision_marker, "exact");

    let seen_report_request = seen_report
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("report-stream should be captured");
    assert_eq!(
        seen_report_request.trace_id,
        "trace-openai-chat-stream-decision-123"
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

    assert_eq!(*plan_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
