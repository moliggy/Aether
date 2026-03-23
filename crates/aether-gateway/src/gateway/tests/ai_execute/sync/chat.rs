use super::*;

#[tokio::test]
async fn gateway_executes_openai_chat_sync_via_executor_plan() {
    #[derive(Debug, Clone)]
    struct SeenPlanSyncRequest {
        trace_id: String,
        path: String,
        model: String,
        auth_context_present: bool,
    }

    #[derive(Debug, Clone)]
    struct SeenExecutorSyncRequest {
        trace_id: String,
        url: String,
    }

    #[derive(Debug, Clone)]
    struct SeenReportSyncRequest {
        trace_id: String,
        report_kind: String,
        status_code: u64,
    }

    let seen_plan = Arc::new(Mutex::new(None::<SeenPlanSyncRequest>));
    let seen_plan_clone = Arc::clone(&seen_plan);
    let seen_executor = Arc::new(Mutex::new(None::<SeenExecutorSyncRequest>));
    let seen_executor_clone = Arc::clone(&seen_executor);
    let seen_report = Arc::new(Mutex::new(None::<SeenReportSyncRequest>));
    let seen_report_clone = Arc::clone(&seen_report);
    let execute_hits = Arc::new(Mutex::new(0usize));
    let execute_hits_clone = Arc::clone(&execute_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/plan-sync",
            any(move |request: Request| {
                let seen_plan_inner = Arc::clone(&seen_plan_clone);
                async move {
                    let (parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("plan payload should parse");
                    *seen_plan_inner.lock().expect("mutex should lock") =
                        Some(SeenPlanSyncRequest {
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
                            model: payload
                                .get("body_json")
                                .and_then(|value| value.get("model"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            auth_context_present: payload
                                .get("auth_context")
                                .is_some_and(|value| !value.is_null()),
                        });
                    Json(json!({
                        "action": "executor_sync",
                        "plan_kind": "openai_chat_sync",
                        "plan": {
                            "request_id": "req-openai-chat-direct-123",
                            "provider_name": "openai",
                            "provider_id": "provider-openai-chat-direct-123",
                            "endpoint_id": "endpoint-openai-chat-direct-123",
                            "key_id": "key-openai-chat-direct-123",
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
                            "user_id": "user-chat-direct-123",
                            "api_key_id": "key-chat-direct-123"
                        }
                    }))
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
                            status_code: payload
                                .get("status_code")
                                .and_then(|value| value.as_u64())
                                .unwrap_or_default(),
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
                    });
                Json(json!({
                    "request_id": "req-openai-chat-direct-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "id": "chatcmpl-123",
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
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-openai-chat-direct-123")
        .body("{\"model\":\"gpt-5\",\"messages\":[]}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "id": "chatcmpl-123",
            "object": "chat.completion",
            "model": "gpt-5",
            "choices": [],
            "usage": {
                "prompt_tokens": 1,
                "completion_tokens": 2,
                "total_tokens": 3
            }
        })
    );

    let seen_plan_request = seen_plan
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("plan-sync should be captured");
    assert_eq!(seen_plan_request.trace_id, "trace-openai-chat-direct-123");
    assert_eq!(seen_plan_request.path, "/v1/chat/completions");
    assert_eq!(seen_plan_request.model, "gpt-5");
    assert!(!seen_plan_request.auth_context_present);

    let seen_executor_request = seen_executor
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("executor sync should be captured");
    assert_eq!(
        seen_executor_request.trace_id,
        "trace-openai-chat-direct-123"
    );
    assert_eq!(
        seen_executor_request.url,
        "https://api.openai.example/v1/chat/completions"
    );

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
    assert_eq!(seen_report_request.trace_id, "trace-openai-chat-direct-123");
    assert_eq!(seen_report_request.report_kind, "openai_chat_sync_success");
    assert_eq!(seen_report_request.status_code, 200);

    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_openai_chat_sync_via_executor_decision() {
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
        model: String,
        prompt_cache_key: String,
        authorization: String,
    }

    #[derive(Debug, Clone)]
    struct SeenReportSyncRequest {
        trace_id: String,
        report_kind: String,
        provider_model: String,
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
                    Json(json!({
                        "action": "executor_sync_decision",
                        "decision_kind": "openai_chat_sync",
                        "request_id": "req-openai-chat-decision-123",
                        "candidate_id": "cand-openai-chat-decision-123",
                        "provider_name": "openai",
                        "provider_id": "provider-openai-chat-decision-123",
                        "endpoint_id": "endpoint-openai-chat-decision-123",
                        "key_id": "key-openai-chat-decision-123",
                        "upstream_base_url": "https://api.openai.example",
                        "auth_header": "authorization",
                        "auth_value": "Bearer upstream-key",
                        "provider_api_format": "openai:chat",
                        "client_api_format": "openai:chat",
                        "model_name": "gpt-5",
                        "mapped_model": "gpt-5-upstream",
                        "prompt_cache_key": "cache-key-123",
                        "content_type": "application/json",
                        "report_kind": "openai_chat_sync_success",
                        "report_context": {
                            "user_id": "user-chat-decision-123",
                            "api_key_id": "key-chat-decision-123",
                            "request_id": "req-openai-chat-decision-123",
                            "model": "gpt-5",
                            "provider_name": "openai",
                            "provider_id": "provider-openai-chat-decision-123",
                            "endpoint_id": "endpoint-openai-chat-decision-123",
                            "key_id": "key-openai-chat-decision-123",
                            "provider_api_format": "openai:chat",
                            "client_api_format": "openai:chat",
                            "mapped_model": "gpt-5-upstream",
                            "original_headers": {
                                "content-type": "application/json"
                            },
                            "original_request_body": {
                                "model": "gpt-5",
                                "messages": []
                            }
                        }
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
                            provider_model: payload
                                .get("report_context")
                                .and_then(|value| value.get("provider_request_body"))
                                .and_then(|value| value.get("model"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            provider_auth: payload
                                .get("report_context")
                                .and_then(|value| value.get("provider_request_headers"))
                                .and_then(|value| value.get("authorization"))
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
                        model: payload
                            .get("body")
                            .and_then(|value| value.get("json_body"))
                            .and_then(|value| value.get("model"))
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        prompt_cache_key: payload
                            .get("body")
                            .and_then(|value| value.get("json_body"))
                            .and_then(|value| value.get("prompt_cache_key"))
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        authorization: payload
                            .get("headers")
                            .and_then(|value| value.get("authorization"))
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                    });
                Json(json!({
                    "request_id": "req-openai-chat-decision-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "id": "chatcmpl-123",
                            "object": "chat.completion",
                            "model": "gpt-5-upstream",
                            "choices": [],
                            "usage": {
                                "prompt_tokens": 1,
                                "completion_tokens": 2,
                                "total_tokens": 3
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
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-openai-chat-decision-123")
        .body("{\"model\":\"gpt-5\",\"messages\":[]}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(EXECUTION_PATH_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some(EXECUTION_PATH_EXECUTOR_SYNC)
    );
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(response_json["model"], "gpt-5-upstream");

    let seen_decision_request = seen_decision
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("decision-sync should be captured");
    assert_eq!(
        seen_decision_request.trace_id,
        "trace-openai-chat-decision-123"
    );
    assert_eq!(seen_decision_request.path, "/v1/chat/completions");
    assert!(!seen_decision_request.auth_context_present);

    let seen_executor_request = seen_executor
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("executor sync should be captured");
    assert_eq!(
        seen_executor_request.trace_id,
        "trace-openai-chat-decision-123"
    );
    assert_eq!(
        seen_executor_request.url,
        "https://api.openai.example/v1/chat/completions"
    );
    assert_eq!(seen_executor_request.model, "gpt-5-upstream");
    assert_eq!(seen_executor_request.prompt_cache_key, "cache-key-123");
    assert_eq!(seen_executor_request.authorization, "Bearer upstream-key");

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
        "trace-openai-chat-decision-123"
    );
    assert_eq!(seen_report_request.report_kind, "openai_chat_sync_success");
    assert_eq!(seen_report_request.provider_model, "gpt-5-upstream");
    assert_eq!(seen_report_request.provider_auth, "Bearer upstream-key");

    assert_eq!(*plan_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
