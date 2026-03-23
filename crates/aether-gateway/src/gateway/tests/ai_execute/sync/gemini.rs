use super::*;

#[tokio::test]
async fn gateway_executes_gemini_chat_sync_via_executor_decision() {
    #[derive(Debug, Clone)]
    struct SeenDecisionSyncRequest {
        trace_id: String,
        path: String,
        auth_context_present: bool,
    }

    #[derive(Debug, Clone)]
    struct SeenExecutorSyncRequest {
        trace_id: String,
        url: String,
        has_model_field: bool,
        prompt_cache_key: String,
        auth_header_value: String,
        exact_temperature: f64,
    }

    #[derive(Debug, Clone)]
    struct SeenReportSyncRequest {
        trace_id: String,
        report_kind: String,
        has_model_field: bool,
        provider_auth: String,
    }

    let seen_decision = Arc::new(Mutex::new(None::<SeenDecisionSyncRequest>));
    let seen_decision_clone = Arc::clone(&seen_decision);
    let seen_executor = Arc::new(Mutex::new(None::<SeenExecutorSyncRequest>));
    let seen_executor_clone = Arc::clone(&seen_executor);
    let seen_report = Arc::new(Mutex::new(None::<SeenReportSyncRequest>));
    let seen_report_clone = Arc::clone(&seen_report);
    let plan_hits = Arc::new(Mutex::new(0usize));
    let plan_hits_clone = Arc::clone(&plan_hits);
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
                    "route_family": "gemini",
                    "route_kind": "chat",
                    "auth_endpoint_signature": "gemini:chat",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-gemini-chat-decision-123",
                        "api_key_id": "key-gemini-chat-decision-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1beta/models/gemini-2.5-pro:generateContent"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-sync",
            any(move |request: Request| {
                let seen_decision_inner = Arc::clone(&seen_decision_clone);
                async move {
                    let (parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("decision payload should parse");
                    *seen_decision_inner.lock().expect("mutex should lock") =
                        Some(SeenDecisionSyncRequest {
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
                            auth_context_present: payload
                                .get("auth_context")
                                .is_some_and(|value| !value.is_null()),
                        });
                    let provider_request_headers = json!({
                        "content-type": "application/json",
                        "x-goog-api-key": "upstream-key",
                        "x-provider-extra": "1"
                    });
                    let provider_request_body = json!({
                        "contents": [{
                            "role": "user",
                            "parts": [{"text": "hello"}]
                        }],
                        "prompt_cache_key": "cache-key-123",
                        "generationConfig": {
                            "temperature": 0.2
                        }
                    });
                    let report_context = json!({
                        "user_id": "user-gemini-chat-decision-123",
                        "api_key_id": "key-gemini-chat-decision-123",
                        "request_id": "req-gemini-chat-decision-123",
                        "model": "gemini-2.5-pro",
                        "provider_name": "gemini",
                        "provider_id": "provider-gemini-chat-decision-123",
                        "endpoint_id": "endpoint-gemini-chat-decision-123",
                        "key_id": "key-gemini-chat-decision-123",
                        "provider_api_format": "gemini:chat",
                        "client_api_format": "gemini:chat",
                        "mapped_model": "gemini-2.5-pro-upstream",
                        "original_headers": {
                            "content-type": "application/json"
                        },
                        "original_request_body": {
                            "contents": [{
                                "role": "user",
                                "parts": [{"text": "hello"}]
                            }]
                        }
                    });
                    Json(json!({
                        "action": "executor_sync_decision",
                        "decision_kind": "gemini_chat_sync",
                        "request_id": "req-gemini-chat-decision-123",
                        "candidate_id": "cand-gemini-chat-decision-123",
                        "provider_name": "gemini",
                        "provider_id": "provider-gemini-chat-decision-123",
                        "endpoint_id": "endpoint-gemini-chat-decision-123",
                        "key_id": "key-gemini-chat-decision-123",
                        "upstream_base_url": "https://generativelanguage.googleapis.com",
                        "upstream_url": "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro-upstream:generateContent",
                        "auth_header": "x-goog-api-key",
                        "auth_value": "upstream-key",
                        "provider_api_format": "gemini:chat",
                        "client_api_format": "gemini:chat",
                        "model_name": "gemini-2.5-pro",
                        "mapped_model": "gemini-2.5-pro-upstream",
                        "prompt_cache_key": "cache-key-123",
                        "provider_request_headers": provider_request_headers,
                        "provider_request_body": provider_request_body,
                        "content_type": "application/json",
                        "report_kind": "gemini_chat_sync_success",
                        "report_context": report_context
                    }))
                }
            }),
        )
        .route(
            "/api/internal/gateway/plan-sync",
            any(move |_request: Request| {
                let plan_hits_inner = Arc::clone(&plan_hits_clone);
                async move {
                    *plan_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({"action": "proxy_public"}))
                }
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(move |request: Request| {
                let seen_report_inner = Arc::clone(&seen_report_clone);
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
                            has_model_field: payload
                                .get("report_context")
                                .and_then(|value| value.get("provider_request_body"))
                                .and_then(|value| value.get("model"))
                                .is_some(),
                            provider_auth: payload
                                .get("report_context")
                                .and_then(|value| value.get("provider_request_headers"))
                                .and_then(|value| value.get("x-goog-api-key"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        });
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
            "/v1beta/models/gemini-2.5-pro:generateContent",
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
        any(move |request: Request| {
            let seen_executor_inner = Arc::clone(&seen_executor_clone);
            async move {
                let (parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value =
                    serde_json::from_slice(&raw_body).expect("executor payload should parse");
                *seen_executor_inner.lock().expect("mutex should lock") =
                    Some(SeenExecutorSyncRequest {
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
                        has_model_field: payload
                            .get("body")
                            .and_then(|value| value.get("json_body"))
                            .and_then(|value| value.get("model"))
                            .is_some(),
                        prompt_cache_key: payload
                            .get("body")
                            .and_then(|value| value.get("json_body"))
                            .and_then(|value| value.get("prompt_cache_key"))
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        auth_header_value: payload
                            .get("headers")
                            .and_then(|value| value.get("x-goog-api-key"))
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        exact_temperature: payload
                            .get("body")
                            .and_then(|value| value.get("json_body"))
                            .and_then(|value| value.get("generationConfig"))
                            .and_then(|value| value.get("temperature"))
                            .and_then(|value| value.as_f64())
                            .unwrap_or_default(),
                    });
                Json(json!({
                    "request_id": "req-gemini-chat-decision-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "candidates": [],
                            "usageMetadata": {
                                "promptTokenCount": 1,
                                "candidatesTokenCount": 2,
                                "totalTokenCount": 3
                            }
                        }
                    },
                    "telemetry": {
                        "elapsed_ms": 41
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

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/v1beta/models/gemini-2.5-pro:generateContent"
        ))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header("x-goog-api-key", "client-key")
        .header(TRACE_ID_HEADER, "trace-gemini-chat-decision-123")
        .body("{\"contents\":[{\"role\":\"user\",\"parts\":[{\"text\":\"hello\"}]}]}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);

    let seen_decision_request = seen_decision
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("decision-sync should be captured");
    assert_eq!(
        seen_decision_request.trace_id,
        "trace-gemini-chat-decision-123"
    );
    assert_eq!(
        seen_decision_request.path,
        "/v1beta/models/gemini-2.5-pro:generateContent"
    );
    assert!(!seen_decision_request.auth_context_present);

    let seen_executor_request = seen_executor
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("executor sync should be captured");
    assert_eq!(
        seen_executor_request.trace_id,
        "trace-gemini-chat-decision-123"
    );
    assert_eq!(
        seen_executor_request.url,
        "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro-upstream:generateContent"
    );
    assert!(!seen_executor_request.has_model_field);
    assert_eq!(seen_executor_request.prompt_cache_key, "cache-key-123");
    assert_eq!(seen_executor_request.auth_header_value, "upstream-key");
    assert!((seen_executor_request.exact_temperature - 0.2).abs() < f64::EPSILON);

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
        .expect("report-sync should be captured");
    assert_eq!(
        seen_report_request.trace_id,
        "trace-gemini-chat-decision-123"
    );
    assert_eq!(seen_report_request.report_kind, "gemini_chat_sync_success");
    assert!(!seen_report_request.has_model_field);
    assert_eq!(seen_report_request.provider_auth, "upstream-key");

    assert_eq!(*plan_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
