use super::*;

#[tokio::test]
async fn gateway_reuses_cached_auth_context_for_direct_executor_plans() {
    #[derive(Debug, Clone)]
    struct SeenPlanSyncRequest {
        trace_id: String,
        auth_context_present: bool,
        auth_context_user_id: String,
    }

    let seen_plan = Arc::new(Mutex::new(Vec::<SeenPlanSyncRequest>::new()));
    let seen_plan_clone = Arc::clone(&seen_plan);
    let seen_report = Arc::new(Mutex::new(0usize));
    let seen_report_clone = Arc::clone(&seen_report);
    let auth_context_hits = Arc::new(Mutex::new(0usize));
    let auth_context_hits_clone = Arc::clone(&auth_context_hits);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/auth-context",
            any(move |_request: Request| {
                let auth_context_hits_inner = Arc::clone(&auth_context_hits_clone);
                async move {
                    *auth_context_hits_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({
                        "auth_context": {
                            "user_id": "user-chat-cache-123",
                            "api_key_id": "key-chat-cache-123",
                            "access_allowed": true
                        }
                    }))
                }
            }),
        )
        .route(
            "/api/internal/gateway/plan-sync",
            any(move |request: Request| {
                let seen_plan_inner = Arc::clone(&seen_plan_clone);
                async move {
                    let (parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("plan payload should parse");
                    seen_plan_inner
                        .lock()
                        .expect("mutex should lock")
                        .push(SeenPlanSyncRequest {
                            trace_id: parts
                                .headers
                                .get(TRACE_ID_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            auth_context_present: payload
                                .get("auth_context")
                                .is_some_and(|value| !value.is_null()),
                            auth_context_user_id: payload
                                .get("auth_context")
                                .and_then(|value| value.get("user_id"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        });
                    Json(json!({
                        "action": "executor_sync",
                        "plan_kind": "openai_chat_sync",
                        "plan": {
                            "request_id": "req-openai-chat-cache-123",
                            "provider_name": "openai",
                            "provider_id": "provider-openai-chat-cache-123",
                            "endpoint_id": "endpoint-openai-chat-cache-123",
                            "key_id": "key-openai-chat-cache-123",
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
                            "user_id": "user-chat-cache-123",
                            "api_key_id": "key-chat-cache-123"
                        },
                        "auth_context": {
                            "user_id": "user-chat-cache-123",
                            "api_key_id": "key-chat-cache-123",
                            "access_allowed": true
                        }
                    }))
                }
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(move |_request: Request| {
                let seen_report_inner = Arc::clone(&seen_report_clone);
                async move {
                    *seen_report_inner.lock().expect("mutex should lock") += 1;
                    Json(json!({"ok": true}))
                }
            }),
        );

    let executor = Router::new().route(
        "/v1/execute/sync",
        any(|_request: Request| async move {
            Json(json!({
                "request_id": "req-openai-chat-cache-123",
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "id": "chatcmpl-cache-123",
                        "object": "chat.completion",
                        "model": "gpt-5",
                        "choices": [],
                        "usage": {
                            "prompt_tokens": 1,
                            "completion_tokens": 2,
                            "total_tokens": 3
                        }
                    }
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

    let client = reqwest::Client::new();
    for trace_id in ["trace-openai-chat-cache-1", "trace-openai-chat-cache-2"] {
        let response = client
            .post(format!("{gateway_url}/v1/chat/completions"))
            .header(http::header::CONTENT_TYPE, "application/json")
            .header(http::header::AUTHORIZATION, "Bearer sk-cache")
            .header(TRACE_ID_HEADER, trace_id)
            .body("{\"model\":\"gpt-5\",\"messages\":[]}")
            .send()
            .await
            .expect("request should succeed");
        assert_eq!(response.status(), StatusCode::OK);
    }

    wait_until(300, || *seen_report.lock().expect("mutex should lock") == 2).await;

    let seen_plan_requests = seen_plan.lock().expect("mutex should lock").clone();
    assert_eq!(seen_plan_requests.len(), 2);
    assert_eq!(seen_plan_requests[0].trace_id, "trace-openai-chat-cache-1");
    assert!(!seen_plan_requests[0].auth_context_present);
    assert_eq!(seen_plan_requests[0].auth_context_user_id, "");
    assert_eq!(seen_plan_requests[1].trace_id, "trace-openai-chat-cache-2");
    assert!(seen_plan_requests[1].auth_context_present);
    assert_eq!(
        seen_plan_requests[1].auth_context_user_id,
        "user-chat-cache-123"
    );
    assert_eq!(*auth_context_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_reuses_cached_auth_context_when_falling_back_to_control_execute() {
    #[derive(Debug, Clone)]
    struct SeenExecuteSyncRequest {
        trace_id: String,
        user_id: String,
    }

    let plan_hits = Arc::new(Mutex::new(0usize));
    let plan_hits_clone = Arc::clone(&plan_hits);
    let seen_execute = Arc::new(Mutex::new(None::<SeenExecuteSyncRequest>));
    let seen_execute_clone = Arc::clone(&seen_execute);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/plan-sync",
            any(move |_request: Request| {
                let plan_hits_inner = Arc::clone(&plan_hits_clone);
                async move {
                    let mut plan_hits_guard = plan_hits_inner.lock().expect("mutex should lock");
                    *plan_hits_guard += 1;
                    if *plan_hits_guard == 1 {
                        let mut response = Response::builder()
                            .status(StatusCode::OK)
                            .body(Body::from(
                                json!({
                                    "action": "executor_sync",
                                    "plan_kind": "openai_chat_sync",
                                    "plan": {
                                        "request_id": "req-openai-chat-cache-exec-123",
                                        "provider_name": "openai",
                                        "provider_id": "provider-openai-chat-cache-exec-123",
                                        "endpoint_id": "endpoint-openai-chat-cache-exec-123",
                                        "key_id": "key-openai-chat-cache-exec-123",
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
                                        "user_id": "user-chat-cache-exec-123",
                                        "api_key_id": "key-chat-cache-exec-123"
                                    },
                                    "auth_context": {
                                        "user_id": "user-chat-cache-exec-123",
                                        "api_key_id": "key-chat-cache-exec-123",
                                        "access_allowed": true
                                    }
                                })
                                .to_string(),
                            ))
                            .expect("response should build");
                        response.headers_mut().insert(
                            http::header::CONTENT_TYPE,
                            HeaderValue::from_static("application/json"),
                        );
                        return response;
                    }

                    let mut response = Response::builder()
                        .status(StatusCode::CONFLICT)
                        .body(Body::from("{\"action\":\"proxy_public\"}"))
                        .expect("response should build");
                    response.headers_mut().insert(
                        HeaderName::from_static(CONTROL_ACTION_HEADER),
                        HeaderValue::from_static(CONTROL_ACTION_PROXY_PUBLIC),
                    );
                    response.headers_mut().insert(
                        http::header::CONTENT_TYPE,
                        HeaderValue::from_static("application/json"),
                    );
                    response
                }
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(|_request: Request| async move { Json(json!({"ok": true})) }),
        )
        .route(
            "/api/internal/gateway/execute-sync",
            any(move |request: Request| {
                let seen_execute_inner = Arc::clone(&seen_execute_clone);
                async move {
                    let raw_body = to_bytes(request.into_body(), usize::MAX)
                        .await
                        .expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("execute payload should parse");
                    *seen_execute_inner.lock().expect("mutex should lock") =
                        Some(SeenExecuteSyncRequest {
                            trace_id: payload
                                .get("trace_id")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            user_id: payload
                                .get("auth_context")
                                .and_then(|value| value.get("user_id"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        });
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
                "request_id": "req-openai-chat-cache-exec-123",
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "id": "chatcmpl-cache-exec-123",
                        "object": "chat.completion",
                        "model": "gpt-5",
                        "choices": [],
                        "usage": {
                            "prompt_tokens": 1,
                            "completion_tokens": 2,
                            "total_tokens": 3
                        }
                    }
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

    let client = reqwest::Client::new();
    let first_response = client
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(http::header::AUTHORIZATION, "Bearer sk-cache-fallback")
        .header(TRACE_ID_HEADER, "trace-openai-chat-cache-exec-1")
        .body("{\"model\":\"gpt-5\",\"messages\":[]}")
        .send()
        .await
        .expect("first request should succeed");
    assert_eq!(first_response.status(), StatusCode::OK);

    let second_response = client
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(http::header::AUTHORIZATION, "Bearer sk-cache-fallback")
        .header(CONTROL_EXECUTE_FALLBACK_HEADER, "true")
        .header(TRACE_ID_HEADER, "trace-openai-chat-cache-exec-2")
        .body("{\"model\":\"gpt-5\",\"messages\":[]}")
        .send()
        .await
        .expect("second request should succeed");
    assert_eq!(second_response.status(), StatusCode::OK);
    assert_eq!(
        second_response.text().await.expect("body should read"),
        "{\"fallback\":true}"
    );

    let seen_execute_request = seen_execute
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("execute-sync should be captured");
    assert_eq!(
        seen_execute_request.trace_id,
        "trace-openai-chat-cache-exec-2"
    );
    assert_eq!(seen_execute_request.user_id, "user-chat-cache-exec-123");

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
