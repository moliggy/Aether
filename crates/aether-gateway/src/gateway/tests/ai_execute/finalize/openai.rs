use super::*;

#[tokio::test]
async fn gateway_executes_openai_chat_sync_via_executor_finalize_plan() {
    #[derive(Debug, Clone)]
    struct SeenReportSyncRequest {
        trace_id: String,
        report_kind: String,
        status_code: u64,
        upstream_id: String,
        client_id: String,
    }

    let seen_report = Arc::new(Mutex::new(None::<SeenReportSyncRequest>));
    let seen_report_clone = Arc::clone(&seen_report);
    let report_hits = Arc::new(Mutex::new(0usize));
    let report_hits_clone = Arc::clone(&report_hits);
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
                        "user_id": "user-chat-finalize-123",
                        "api_key_id": "key-chat-finalize-123",
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
                        "request_id": "req-openai-chat-finalize-123",
                        "provider_name": "gemini",
                        "provider_id": "provider-openai-chat-finalize-123",
                        "endpoint_id": "endpoint-openai-chat-finalize-123",
                        "key_id": "key-openai-chat-finalize-123",
                        "method": "POST",
                        "url": "https://api.gemini.example/v1beta/models/gemini-2.5-pro:generateContent",
                        "headers": {
                            "authorization": "Bearer upstream-key",
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "contents": []
                            }
                        },
                        "stream": false,
                        "client_api_format": "openai:chat",
                        "provider_api_format": "gemini:chat",
                        "model_name": "gpt-5"
                    },
                    "report_kind": "openai_chat_sync_finalize",
                    "report_context": {
                        "user_id": "user-chat-finalize-123",
                        "api_key_id": "key-chat-finalize-123",
                        "provider_id": "provider-openai-chat-finalize-123",
                        "endpoint_id": "endpoint-openai-chat-finalize-123",
                        "key_id": "key-openai-chat-finalize-123",
                        "client_api_format": "openai:chat",
                        "provider_api_format": "gemini:chat",
                        "request_id": "req-openai-chat-finalize-123",
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
                                .and_then(|value| value.get("responseId"))
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
                "request_id": "req-openai-chat-finalize-123",
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "responseId": "resp-gemini-chat-123",
                        "candidates": [{
                            "content": {
                                "parts": [{"text": "done"}],
                                "role": "model"
                            },
                            "finishReason": "STOP",
                            "index": 0
                        }],
                        "modelVersion": "gemini-2.5-pro-upstream",
                        "usageMetadata": {
                            "promptTokenCount": 1,
                            "candidatesTokenCount": 2,
                            "totalTokenCount": 3
                        }
                    }
                },
                "telemetry": {
                    "elapsed_ms": 48
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
        .header(TRACE_ID_HEADER, "trace-openai-chat-finalize-123")
        .body("{\"model\":\"gpt-5\",\"messages\":[]}")
        .send()
        .await
        .expect("request should succeed");
    let elapsed = started_at.elapsed();

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "id": "resp-gemini-chat-123",
            "object": "chat.completion",
            "model": "gemini-2.5-pro-upstream",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "done"
                },
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 1,
                "completion_tokens": 2,
                "total_tokens": 3
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
        "trace-openai-chat-finalize-123"
    );
    assert_eq!(seen_report_request.report_kind, "openai_chat_sync_success");
    assert_eq!(seen_report_request.status_code, 200);
    assert_eq!(seen_report_request.upstream_id, "resp-gemini-chat-123");
    assert_eq!(seen_report_request.client_id, "resp-gemini-chat-123");

    assert_eq!(*report_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_openai_cli_sync_via_executor_plan() {
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
                        "user_id": "user-cli-direct-123",
                        "api_key_id": "key-cli-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/responses"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/plan-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync",
                    "plan_kind": "openai_cli_sync",
                    "plan": {
                        "request_id": "req-openai-cli-direct-123",
                        "provider_name": "openai",
                        "provider_id": "provider-openai-cli-direct-123",
                        "endpoint_id": "endpoint-openai-cli-direct-123",
                        "key_id": "key-openai-cli-direct-123",
                        "method": "POST",
                        "url": "https://api.openai.example/v1/responses",
                        "headers": {
                            "authorization": "Bearer upstream-key",
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "model": "gpt-5",
                                "input": "hello"
                            }
                        },
                        "stream": false,
                        "client_api_format": "openai:cli",
                        "provider_api_format": "openai:cli",
                        "model_name": "gpt-5"
                    },
                    "report_kind": "openai_cli_sync_success",
                    "report_context": {
                        "user_id": "user-cli-direct-123",
                        "api_key_id": "key-cli-direct-123",
                        "client_api_format": "openai:cli"
                    }
                }))
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
        );

    let executor = Router::new().route(
        "/v1/execute/sync",
        any(|_request: Request| async move {
            Json(json!({
                "request_id": "req-openai-cli-direct-123",
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "id": "resp_cli_123",
                        "object": "response",
                        "model": "gpt-5",
                        "output": [],
                        "usage": {
                            "input_tokens": 1,
                            "output_tokens": 2,
                            "total_tokens": 3
                        }
                    }
                },
                "telemetry": {
                    "elapsed_ms": 52
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

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/responses"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-openai-cli-direct-123")
        .body("{\"model\":\"gpt-5\",\"input\":\"hello\"}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "id": "resp_cli_123",
            "object": "response",
            "model": "gpt-5",
            "output": [],
            "usage": {
                "input_tokens": 1,
                "output_tokens": 2,
                "total_tokens": 3
            }
        })
    );
    wait_until(300, || *report_hits.lock().expect("mutex should lock") == 1).await;
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_openai_compact_sync_via_executor_plan() {
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
                    "route_kind": "compact",
                    "auth_endpoint_signature": "openai:compact",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-compact-direct-123",
                        "api_key_id": "key-compact-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/responses/compact"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/plan-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync",
                    "plan_kind": "openai_compact_sync",
                    "plan": {
                        "request_id": "req-openai-compact-direct-123",
                        "provider_name": "openai",
                        "provider_id": "provider-openai-compact-direct-123",
                        "endpoint_id": "endpoint-openai-compact-direct-123",
                        "key_id": "key-openai-compact-direct-123",
                        "method": "POST",
                        "url": "https://api.openai.example/v1/responses/compact",
                        "headers": {
                            "authorization": "Bearer upstream-key",
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "model": "gpt-5",
                                "input": "hello"
                            }
                        },
                        "stream": false,
                        "client_api_format": "openai:compact",
                        "provider_api_format": "openai:compact",
                        "model_name": "gpt-5"
                    },
                    "report_kind": "openai_cli_sync_success",
                    "report_context": {
                        "user_id": "user-compact-direct-123",
                        "api_key_id": "key-compact-direct-123",
                        "client_api_format": "openai:compact"
                    }
                }))
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
        );

    let executor = Router::new().route(
        "/v1/execute/sync",
        any(|_request: Request| async move {
            Json(json!({
                "request_id": "req-openai-compact-direct-123",
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "id": "resp_compact_123",
                        "object": "response",
                        "model": "gpt-5",
                        "output": [],
                        "usage": {
                            "input_tokens": 4,
                            "output_tokens": 5,
                            "total_tokens": 9
                        }
                    }
                },
                "telemetry": {
                    "elapsed_ms": 49
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

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/responses/compact"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-openai-compact-direct-123")
        .body("{\"model\":\"gpt-5\",\"input\":\"hello\"}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "id": "resp_compact_123",
            "object": "response",
            "model": "gpt-5",
            "output": [],
            "usage": {
                "input_tokens": 4,
                "output_tokens": 5,
                "total_tokens": 9
            }
        })
    );
    wait_until(300, || *report_hits.lock().expect("mutex should lock") == 1).await;
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
