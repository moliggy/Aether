use super::*;

#[tokio::test]
async fn gateway_executes_openai_video_create_via_executor_sync_finalize() {
    #[derive(Debug, Clone)]
    struct SeenPlanSyncRequest {
        trace_id: String,
        path: String,
        model: String,
    }

    #[derive(Debug, Clone)]
    struct SeenExecutorSyncRequest {
        method: String,
        url: String,
        provider_api_format: String,
    }

    #[derive(Debug, Clone)]
    struct SeenReportSyncRequest {
        trace_id: String,
        report_kind: String,
        status_code: u64,
        upstream_id: String,
        local_task_id: String,
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
            "/api/internal/gateway/resolve",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "proxy_public",
                    "route_class": "ai_public",
                    "route_family": "openai",
                    "route_kind": "video",
                    "auth_endpoint_signature": "openai:video",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-video-create-direct-123",
                        "api_key_id": "key-video-create-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/videos"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-sync",
            any(move |request: Request| {
                let seen_plan_inner = Arc::clone(&seen_plan_clone);
                async move {
                    let (parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).unwrap_or_else(|_| json!({}));
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
                        });
                    Json(json!({
                        "action": "executor_sync_decision",
                        "decision_kind": "openai_video_create_sync",
                        "request_id": "req-openai-video-create-direct-123",
                        "provider_name": "openai",
                        "provider_id": "provider-openai-video-create-direct-123",
                        "endpoint_id": "endpoint-openai-video-create-direct-123",
                        "key_id": "key-openai-video-create-direct-123",
                        "upstream_base_url": "https://api.openai.example",
                        "upstream_url": "https://api.openai.example/v1/videos",
                        "provider_request_method": "POST",
                        "auth_header": "",
                        "auth_value": "",
                        "provider_request_headers": {
                            "authorization": "Bearer upstream-key",
                            "content-type": "application/json"
                        },
                        "provider_request_body": {
                            "model": "sora-2",
                            "prompt": "hello"
                        },
                        "content_type": "application/json",
                        "client_api_format": "openai:video",
                        "provider_api_format": "openai:video",
                        "model_name": "sora-2",
                        "report_kind": "openai_video_create_sync_finalize",
                        "report_context": {
                            "user_id": "user-video-create-direct-123",
                            "api_key_id": "key-video-create-direct-123",
                            "model": "sora-2",
                            "original_request_body": {
                                "model": "sora-2",
                                "prompt": "hello"
                            }
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
                            upstream_id: payload
                                .get("body_json")
                                .and_then(|value| value.get("id"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            local_task_id: payload
                                .get("report_context")
                                .and_then(|value| value.get("local_task_id"))
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
            "/v1/videos",
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
                let (_parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value =
                    serde_json::from_slice(&raw_body).expect("executor payload should parse");
                *seen_executor_inner.lock().expect("mutex should lock") =
                    Some(SeenExecutorSyncRequest {
                        method: payload
                            .get("method")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        url: payload
                            .get("url")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        provider_api_format: payload
                            .get("provider_api_format")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                    });
                Json(json!({
                    "request_id": "req-openai-video-create-direct-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "id": "ext-video-task-123",
                            "status": "submitted"
                        }
                    },
                    "telemetry": {
                        "elapsed_ms": 57
                    }
                }))
            }
        }),
    );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let (executor_url, executor_handle) = start_server(executor).await;
    let gateway =
        build_router_with_endpoints(upstream_url.clone(), Some(upstream_url), Some(executor_url))
            .expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/videos"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-openai-video-create-direct-123")
        .body("{\"model\":\"sora-2\",\"prompt\":\"hello\"}")
        .send()
        .await
        .expect("request should succeed");

    let status = response.status();
    let response_body = response.text().await.expect("body should read");
    assert_eq!(status, StatusCode::OK);
    let response_json: serde_json::Value =
        serde_json::from_str(&response_body).expect("body should parse");
    let local_task_id = response_json
        .get("id")
        .and_then(|value| value.as_str())
        .expect("response id should exist");
    assert!(!local_task_id.is_empty());
    assert_eq!(response_json.get("object"), Some(&json!("video")));
    assert_eq!(response_json.get("status"), Some(&json!("queued")));
    assert_eq!(response_json.get("progress"), Some(&json!(0)));
    assert_eq!(response_json.get("model"), Some(&json!("sora-2")));
    assert_eq!(response_json.get("prompt"), Some(&json!("hello")));
    assert!(response_json
        .get("created_at")
        .and_then(|value| value.as_u64())
        .is_some());

    let seen_plan_request = seen_plan
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("decision-sync should be captured");
    assert_eq!(
        seen_plan_request.trace_id,
        "trace-openai-video-create-direct-123"
    );
    assert_eq!(seen_plan_request.path, "/v1/videos");
    assert_eq!(seen_plan_request.model, "sora-2");

    let seen_executor_request = seen_executor
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("executor sync should be captured");
    assert_eq!(seen_executor_request.method, "POST");
    assert_eq!(
        seen_executor_request.url,
        "https://api.openai.example/v1/videos"
    );
    assert_eq!(seen_executor_request.provider_api_format, "openai:video");

    let seen_report_request = seen_report
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("report-sync should be captured");
    assert_eq!(
        seen_report_request.trace_id,
        "trace-openai-video-create-direct-123"
    );
    assert_eq!(
        seen_report_request.report_kind,
        "openai_video_create_sync_success"
    );
    assert_eq!(seen_report_request.status_code, 200);
    assert_eq!(seen_report_request.upstream_id, "ext-video-task-123");
    assert_eq!(seen_report_request.local_task_id, local_task_id);

    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_openai_video_create_with_background_report_when_rust_owns_video_tasks() {
    #[derive(Debug, Clone)]
    struct SeenReportSyncRequest {
        report_kind: String,
        local_task_id: String,
    }

    let seen_report = Arc::new(Mutex::new(None::<SeenReportSyncRequest>));
    let seen_report_clone = Arc::clone(&seen_report);
    let report_hits = Arc::new(Mutex::new(0usize));
    let report_hits_clone = Arc::clone(&report_hits);
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
                    "route_kind": "video",
                    "auth_endpoint_signature": "openai:video",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-video-create-rust-owned-123",
                        "api_key_id": "key-video-create-rust-owned-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/videos"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync_decision",
                    "decision_kind": "openai_video_create_sync",
                    "request_id": "req-openai-video-create-rust-owned-123",
                    "provider_name": "openai",
                    "provider_id": "provider-openai-video-create-rust-owned-123",
                    "endpoint_id": "endpoint-openai-video-create-rust-owned-123",
                    "key_id": "key-openai-video-create-rust-owned-123",
                    "upstream_base_url": "https://api.openai.example",
                    "upstream_url": "https://api.openai.example/v1/videos",
                    "provider_request_method": "POST",
                    "auth_header": "",
                    "auth_value": "",
                    "provider_request_headers": {
                        "authorization": "Bearer upstream-key",
                        "content-type": "application/json"
                    },
                    "provider_request_body": {
                        "model": "sora-2",
                        "prompt": "hello"
                    },
                    "content_type": "application/json",
                    "client_api_format": "openai:video",
                    "provider_api_format": "openai:video",
                    "model_name": "sora-2",
                    "report_kind": "openai_video_create_sync_finalize",
                    "report_context": {
                        "user_id": "user-video-create-rust-owned-123",
                        "api_key_id": "key-video-create-rust-owned-123",
                        "model": "sora-2",
                        "original_request_body": {
                            "model": "sora-2",
                            "prompt": "hello"
                        }
                    }
                }))
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(move |request: Request| {
                let seen_report_inner = Arc::clone(&seen_report_clone);
                let report_hits_inner = Arc::clone(&report_hits_clone);
                async move {
                    tokio::time::sleep(std::time::Duration::from_millis(1_500)).await;
                    let (_parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("report payload should parse");
                    *seen_report_inner.lock().expect("mutex should lock") =
                        Some(SeenReportSyncRequest {
                            report_kind: payload
                                .get("report_kind")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            local_task_id: payload
                                .get("report_context")
                                .and_then(|value| value.get("local_task_id"))
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
            "/v1/videos",
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
                "request_id": "req-openai-video-create-rust-owned-123",
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "id": "ext-video-task-123",
                        "status": "submitted"
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
    let gateway_state = AppState::new_with_executor(
        upstream_url.clone(),
        Some(upstream_url.clone()),
        Some(executor_url),
    )
    .expect("gateway state should build")
    .with_video_task_truth_source_mode(VideoTaskTruthSourceMode::RustAuthoritative);
    let gateway = build_router_with_state(gateway_state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let started_at = tokio::time::Instant::now();
    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/videos"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-openai-video-create-rust-owned-123")
        .body("{\"model\":\"sora-2\",\"prompt\":\"hello\"}")
        .send()
        .await
        .expect("request should succeed");
    let elapsed = started_at.elapsed();

    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        elapsed < std::time::Duration::from_millis(1_100),
        "sync response waited too long for report-sync: {:?}",
        elapsed
    );

    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    let local_task_id = response_json
        .get("id")
        .and_then(|value| value.as_str())
        .expect("response id should exist")
        .to_string();
    assert!(!local_task_id.is_empty());

    wait_until(1_800, || {
        *report_hits.lock().expect("mutex should lock") == 1
    })
    .await;

    let seen_report_request = seen_report
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("report-sync should be captured");
    assert_eq!(
        seen_report_request.report_kind,
        "openai_video_create_sync_success"
    );
    assert_eq!(seen_report_request.local_task_id, local_task_id);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_openai_video_remix_via_executor_sync_finalize() {
    #[derive(Debug, Clone)]
    struct SeenPlanSyncRequest {
        trace_id: String,
        path: String,
        prompt: String,
    }

    #[derive(Debug, Clone)]
    struct SeenExecutorSyncRequest {
        method: String,
        url: String,
        provider_api_format: String,
    }

    #[derive(Debug, Clone)]
    struct SeenReportSyncRequest {
        trace_id: String,
        report_kind: String,
        status_code: u64,
        upstream_id: String,
        local_task_id: String,
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
            "/api/internal/gateway/resolve",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "proxy_public",
                    "route_class": "ai_public",
                    "route_family": "openai",
                    "route_kind": "video",
                    "auth_endpoint_signature": "openai:video",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-video-remix-direct-123",
                        "api_key_id": "key-video-remix-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/videos/task-123/remix"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-sync",
            any(move |request: Request| {
                let seen_plan_inner = Arc::clone(&seen_plan_clone);
                async move {
                    let (parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).unwrap_or_else(|_| json!({}));
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
                            prompt: payload
                                .get("body_json")
                                .and_then(|value| value.get("prompt"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        });
                    Json(json!({
                        "action": "executor_sync_decision",
                        "decision_kind": "openai_video_remix_sync",
                        "request_id": "req-openai-video-remix-direct-123",
                        "provider_name": "openai",
                        "provider_id": "provider-openai-video-remix-direct-123",
                        "endpoint_id": "endpoint-openai-video-remix-direct-123",
                        "key_id": "key-openai-video-remix-direct-123",
                        "upstream_base_url": "https://api.openai.example",
                        "upstream_url": "https://api.openai.example/v1/videos/ext-123/remix",
                        "provider_request_method": "POST",
                        "auth_header": "",
                        "auth_value": "",
                        "provider_request_headers": {
                            "authorization": "Bearer upstream-key",
                            "content-type": "application/json"
                        },
                        "provider_request_body": {
                            "prompt": "remix this"
                        },
                        "content_type": "application/json",
                        "client_api_format": "openai:video",
                        "provider_api_format": "openai:video",
                        "model_name": "sora-2",
                        "report_kind": "openai_video_remix_sync_finalize",
                        "report_context": {
                            "user_id": "user-video-remix-direct-123",
                            "api_key_id": "key-video-remix-direct-123",
                            "task_id": "task-123",
                            "model": "sora-2",
                            "original_request_body": {
                                "prompt": "remix this"
                            }
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
                            upstream_id: payload
                                .get("body_json")
                                .and_then(|value| value.get("id"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            local_task_id: payload
                                .get("report_context")
                                .and_then(|value| value.get("local_task_id"))
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
            "/v1/videos/task-123/remix",
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
                let (_parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value =
                    serde_json::from_slice(&raw_body).expect("executor payload should parse");
                *seen_executor_inner.lock().expect("mutex should lock") =
                    Some(SeenExecutorSyncRequest {
                        method: payload
                            .get("method")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        url: payload
                            .get("url")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        provider_api_format: payload
                            .get("provider_api_format")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                    });
                Json(json!({
                    "request_id": "req-openai-video-remix-direct-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "id": "ext-remix-task-123",
                            "status": "submitted"
                        }
                    },
                    "telemetry": {
                        "elapsed_ms": 61
                    }
                }))
            }
        }),
    );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let (executor_url, executor_handle) = start_server(executor).await;
    let gateway =
        build_router_with_endpoints(upstream_url.clone(), Some(upstream_url), Some(executor_url))
            .expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/videos/task-123/remix"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-openai-video-remix-direct-123")
        .body("{\"prompt\":\"remix this\"}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    let local_task_id = response_json
        .get("id")
        .and_then(|value| value.as_str())
        .expect("response id should exist");
    assert!(!local_task_id.is_empty());
    assert_eq!(response_json.get("object"), Some(&json!("video")));
    assert_eq!(response_json.get("status"), Some(&json!("queued")));
    assert_eq!(response_json.get("progress"), Some(&json!(0)));
    assert_eq!(response_json.get("prompt"), Some(&json!("remix this")));
    assert_eq!(
        response_json.get("remixed_from_video_id"),
        Some(&json!("task-123"))
    );

    let seen_plan_request = seen_plan
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("decision-sync should be captured");
    assert_eq!(
        seen_plan_request.trace_id,
        "trace-openai-video-remix-direct-123"
    );
    assert_eq!(seen_plan_request.path, "/v1/videos/task-123/remix");
    assert_eq!(seen_plan_request.prompt, "remix this");

    let seen_executor_request = seen_executor
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("executor sync should be captured");
    assert_eq!(seen_executor_request.method, "POST");
    assert_eq!(
        seen_executor_request.url,
        "https://api.openai.example/v1/videos/ext-123/remix"
    );
    assert_eq!(seen_executor_request.provider_api_format, "openai:video");

    let seen_report_request = seen_report
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("report-sync should be captured");
    assert_eq!(
        seen_report_request.trace_id,
        "trace-openai-video-remix-direct-123"
    );
    assert_eq!(
        seen_report_request.report_kind,
        "openai_video_remix_sync_success"
    );
    assert_eq!(seen_report_request.status_code, 200);
    assert_eq!(seen_report_request.upstream_id, "ext-remix-task-123");
    assert_eq!(seen_report_request.local_task_id, local_task_id);

    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
