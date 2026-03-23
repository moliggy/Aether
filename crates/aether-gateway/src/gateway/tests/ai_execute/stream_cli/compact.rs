use super::*;

#[tokio::test]
async fn gateway_executes_openai_compact_gemini_cross_format_stream_via_local_stream_rewrite() {
    use base64::Engine as _;

    #[derive(Debug, Clone)]
    struct SeenReportStreamRequest {
        trace_id: String,
        report_kind: String,
        status_code: u64,
        body: String,
    }

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
                    "route_kind": "compact",
                    "auth_endpoint_signature": "openai:compact",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-openai-compact-stream-gemini-xfmt-direct-123",
                        "api_key_id": "key-openai-compact-stream-gemini-xfmt-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/responses/compact"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-stream",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_stream_decision",
                    "decision_kind": "openai_compact_stream",
                    "request_id": "req-openai-compact-stream-gemini-xfmt-direct-123",
                    "candidate_id": "cand-openai-compact-stream-gemini-xfmt-direct-123",
                    "provider_name": "gemini",
                    "provider_id": "provider-openai-compact-stream-gemini-xfmt-direct-123",
                    "endpoint_id": "endpoint-openai-compact-stream-gemini-xfmt-direct-123",
                    "key_id": "key-openai-compact-stream-gemini-xfmt-direct-123",
                    "upstream_base_url": "https://generativelanguage.googleapis.com",
                    "upstream_url": "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro-upstream:streamGenerateContent?alt=sse",
                    "auth_header": "x-goog-api-key",
                    "auth_value": "upstream-key",
                    "provider_api_format": "gemini:cli",
                    "client_api_format": "openai:compact",
                    "model_name": "gpt-5",
                    "mapped_model": "gemini-2.5-pro-upstream",
                    "provider_request_headers": {
                        "content-type": "application/json",
                        "x-goog-api-key": "upstream-key",
                        "accept": "text/event-stream"
                    },
                    "provider_request_body": {
                        "contents": [{"role": "user", "parts": [{"text": "hello"}]}]
                    },
                    "content_type": "application/json",
                    "report_kind": "openai_cli_stream_success",
                    "report_context": {
                        "user_id": "user-openai-compact-stream-gemini-xfmt-direct-123",
                        "api_key_id": "key-openai-compact-stream-gemini-xfmt-direct-123",
                        "request_id": "req-openai-compact-stream-gemini-xfmt-direct-123",
                        "model": "gpt-5",
                        "provider_name": "gemini",
                        "provider_id": "provider-openai-compact-stream-gemini-xfmt-direct-123",
                        "endpoint_id": "endpoint-openai-compact-stream-gemini-xfmt-direct-123",
                        "key_id": "key-openai-compact-stream-gemini-xfmt-direct-123",
                        "provider_api_format": "gemini:cli",
                        "client_api_format": "openai:compact",
                        "mapped_model": "gemini-2.5-pro-upstream",
                        "original_headers": {
                            "content-type": "application/json"
                        },
                        "original_request_body": {
                            "model": "gpt-5",
                            "input": "hello",
                            "stream": true
                        },
                        "has_envelope": false,
                        "needs_conversion": true
                    }
                }))
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
        any(|_request: Request| async move {
            let gemini_stream = concat!(
                "data: {\"responseId\":\"resp_openai_compact_gemini_stream_123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello Gemini Compact\"}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"modelVersion\":\"gemini-2.5-pro-upstream\",\"usageMetadata\":{\"promptTokenCount\":2,\"candidatesTokenCount\":3,\"totalTokenCount\":5}}\n\n"
            );
            let frames = format!(
                concat!(
                    "{{\"type\":\"headers\",\"payload\":{{\"kind\":\"headers\",\"status_code\":200,\"headers\":{{\"content-type\":\"text/event-stream\"}}}}}}\n",
                    "{{\"type\":\"data\",\"payload\":{{\"kind\":\"data\",\"text\":{}}}}}\n",
                    "{{\"type\":\"telemetry\",\"payload\":{{\"kind\":\"telemetry\",\"telemetry\":{{\"elapsed_ms\":41,\"ttfb_ms\":12,\"upstream_bytes\":31}}}}}}\n",
                    "{{\"type\":\"eof\",\"payload\":{{\"kind\":\"eof\"}}}}\n"
                ),
                serde_json::to_string(gemini_stream).expect("stream should encode"),
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
        .post(format!("{gateway_url}/v1/responses/compact"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(
            TRACE_ID_HEADER,
            "trace-openai-compact-stream-gemini-xfmt-direct-123",
        )
        .body("{\"model\":\"gpt-5\",\"input\":\"hello\",\"stream\":true}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let response_text = response.text().await.expect("body should read");
    assert!(response_text.contains("event: response.completed"));
    assert!(response_text.contains("\"type\":\"response.completed\""));
    assert!(response_text.contains("\"object\":\"response\""));
    assert!(response_text.contains("\"text\":\"Hello Gemini Compact\""));
    assert!(response_text.contains("\"total_tokens\":5"));

    wait_until(300, || {
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
        .expect("report-stream should be captured");
    assert_eq!(
        seen_report_request.trace_id,
        "trace-openai-compact-stream-gemini-xfmt-direct-123"
    );
    assert_eq!(seen_report_request.report_kind, "openai_cli_stream_success");
    assert_eq!(seen_report_request.status_code, 200);
    assert!(seen_report_request
        .body
        .contains("event: response.completed"));
    assert!(seen_report_request
        .body
        .contains("\"text\":\"Hello Gemini Compact\""));

    assert_eq!(*plan_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
