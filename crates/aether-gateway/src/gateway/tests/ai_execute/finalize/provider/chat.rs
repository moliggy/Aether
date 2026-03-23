use super::*;

#[tokio::test]
async fn gateway_executes_claude_chat_sync_via_executor_plan() {
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
                    "route_family": "claude",
                    "route_kind": "chat",
                    "auth_endpoint_signature": "claude:chat",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-claude-direct-123",
                        "api_key_id": "key-claude-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/messages"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/plan-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync",
                    "plan_kind": "claude_chat_sync",
                    "plan": {
                        "request_id": "req-claude-direct-123",
                        "provider_name": "claude",
                        "provider_id": "provider-claude-direct-123",
                        "endpoint_id": "endpoint-claude-direct-123",
                        "key_id": "key-claude-direct-123",
                        "method": "POST",
                        "url": "https://api.anthropic.example/v1/messages",
                        "headers": {
                            "x-api-key": "upstream-key",
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "model": "claude-sonnet-4",
                                "messages": []
                            }
                        },
                        "stream": false,
                        "client_api_format": "claude:chat",
                        "provider_api_format": "claude:chat",
                        "model_name": "claude-sonnet-4"
                    },
                    "report_kind": "claude_chat_sync_success",
                    "report_context": {
                        "user_id": "user-claude-direct-123",
                        "api_key_id": "key-claude-direct-123",
                        "client_api_format": "claude:chat"
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
                "request_id": "req-claude-direct-123",
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "id": "msg_123",
                        "type": "message",
                        "model": "claude-sonnet-4",
                        "role": "assistant",
                        "content": [],
                        "usage": {
                            "input_tokens": 3,
                            "output_tokens": 5
                        }
                    }
                },
                "telemetry": {
                    "elapsed_ms": 61
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
        .post(format!("{gateway_url}/v1/messages"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-claude-direct-123")
        .body("{\"model\":\"claude-sonnet-4\",\"messages\":[]}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "id": "msg_123",
            "type": "message",
            "model": "claude-sonnet-4",
            "role": "assistant",
            "content": [],
            "usage": {
                "input_tokens": 3,
                "output_tokens": 5
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
async fn gateway_executes_gemini_chat_sync_via_executor_plan() {
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
                        "user_id": "user-gemini-direct-123",
                        "api_key_id": "key-gemini-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1beta/models/gemini-2.5-pro:generateContent"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/plan-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync",
                    "plan_kind": "gemini_chat_sync",
                    "plan": {
                        "request_id": "req-gemini-direct-123",
                        "provider_name": "gemini",
                        "provider_id": "provider-gemini-direct-123",
                        "endpoint_id": "endpoint-gemini-direct-123",
                        "key_id": "key-gemini-direct-123",
                        "method": "POST",
                        "url": "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro:generateContent",
                        "headers": {
                            "x-goog-api-key": "upstream-key",
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "contents": []
                            }
                        },
                        "stream": false,
                        "client_api_format": "gemini:chat",
                        "provider_api_format": "gemini:chat",
                        "model_name": "gemini-2.5-pro"
                    },
                    "report_kind": "gemini_chat_sync_success",
                    "report_context": {
                        "user_id": "user-gemini-direct-123",
                        "api_key_id": "key-gemini-direct-123",
                        "client_api_format": "gemini:chat"
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
                "request_id": "req-gemini-direct-123",
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "candidates": [],
                        "usageMetadata": {
                            "promptTokenCount": 7,
                            "candidatesTokenCount": 11,
                            "totalTokenCount": 18
                        }
                    }
                },
                "telemetry": {
                    "elapsed_ms": 57
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
        .post(format!(
            "{gateway_url}/v1beta/models/gemini-2.5-pro:generateContent"
        ))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-gemini-direct-123")
        .body("{\"contents\":[]}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "candidates": [],
            "usageMetadata": {
                "promptTokenCount": 7,
                "candidatesTokenCount": 11,
                "totalTokenCount": 18
            }
        })
    );
    wait_until(300, || *report_hits.lock().expect("mutex should lock") == 1).await;
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
