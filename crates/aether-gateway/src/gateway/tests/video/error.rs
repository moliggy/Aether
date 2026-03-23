use super::*;

#[tokio::test]
async fn gateway_finalizes_openai_video_create_sync_http_errors_without_control_execute() {
    #[derive(Debug, Clone)]
    struct SeenReportSyncRequest {
        trace_id: String,
        report_kind: String,
        status_code: u64,
        error_message: String,
    }

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
                        "user_id": "user-openai-video-create-error-123",
                        "api_key_id": "key-openai-video-create-error-123",
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
                    "request_id": "req-openai-video-create-error-123",
                    "provider_name": "openai",
                    "provider_id": "provider-openai-video-create-error-123",
                    "endpoint_id": "endpoint-openai-video-create-error-123",
                    "key_id": "key-openai-video-create-error-123",
                    "upstream_base_url": "https://api.openai.example",
                    "upstream_url": "https://api.openai.example/v1/videos",
                    "provider_request_method": "POST",
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
                        "user_id": "user-openai-video-create-error-123",
                        "api_key_id": "key-openai-video-create-error-123"
                    }
                }))
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
                            error_message: payload
                                .get("body_json")
                                .and_then(|value| value.get("error"))
                                .and_then(|value| value.get("message"))
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
        any(|_request: Request| async move {
            Json(json!({
                "request_id": "req-openai-video-create-error-123",
                "status_code": 429,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "error": {
                            "message": "rate limited"
                        }
                    }
                }
            }))
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
        .header(TRACE_ID_HEADER, "trace-openai-video-create-error-123")
        .body("{\"model\":\"sora-2\",\"prompt\":\"hello\"}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(response_json, json!({"error": {"message": "rate limited"}}));

    wait_until(300, || {
        seen_report.lock().expect("mutex should lock").is_some()
    })
    .await;
    let seen_report_request = seen_report
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("report-sync should be captured");
    assert_eq!(
        seen_report_request.trace_id,
        "trace-openai-video-create-error-123"
    );
    assert_eq!(
        seen_report_request.report_kind,
        "openai_video_create_sync_error"
    );
    assert_eq!(seen_report_request.status_code, 429);
    assert_eq!(seen_report_request.error_message, "rate limited");
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_finalizes_openai_video_delete_sync_http_errors_without_control_execute() {
    #[derive(Debug, Clone)]
    struct SeenReportSyncRequest {
        report_kind: String,
        status_code: u64,
    }

    let seen_report = Arc::new(Mutex::new(None::<SeenReportSyncRequest>));
    let seen_report_clone = Arc::clone(&seen_report);

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
                        "user_id": "user-openai-video-delete-error-123",
                        "api_key_id": "key-openai-video-delete-error-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/videos/task-123"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync_decision",
                    "decision_kind": "openai_video_delete_sync",
                    "request_id": "req-openai-video-delete-error-123",
                    "provider_name": "openai",
                    "provider_id": "provider-openai-video-delete-error-123",
                    "endpoint_id": "endpoint-openai-video-delete-error-123",
                    "key_id": "key-openai-video-delete-error-123",
                    "upstream_base_url": "https://api.openai.example",
                    "upstream_url": "https://api.openai.example/v1/videos/ext-123",
                    "provider_request_method": "DELETE",
                    "provider_request_headers": {
                        "authorization": "Bearer upstream-key"
                    },
                    "client_api_format": "openai:video",
                    "provider_api_format": "openai:video",
                    "model_name": "sora-2",
                    "report_kind": "openai_video_delete_sync_finalize",
                    "report_context": {
                        "user_id": "user-openai-video-delete-error-123",
                        "api_key_id": "key-openai-video-delete-error-123",
                        "task_id": "task-123"
                    }
                }))
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(move |request: Request| {
                let seen_report_inner = Arc::clone(&seen_report_clone);
                async move {
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
            "/v1/videos/task-123",
            any(|_request: Request| async move {
                (StatusCode::IM_A_TEAPOT, Body::from("public-route-hit"))
            }),
        );

    let executor = Router::new().route(
        "/v1/execute/sync",
        any(|_request: Request| async move {
            Json(json!({
                "request_id": "req-openai-video-delete-error-123",
                "status_code": 409,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "error": {
                            "message": "cannot delete active task"
                        }
                    }
                }
            }))
        }),
    );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let (executor_url, executor_handle) = start_server(executor).await;
    let gateway =
        build_router_with_endpoints(upstream_url.clone(), Some(upstream_url), Some(executor_url))
            .expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .delete(format!("{gateway_url}/v1/videos/task-123"))
        .header(TRACE_ID_HEADER, "trace-openai-video-delete-error-123")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({"error": {"message": "cannot delete active task"}})
    );

    wait_until(300, || {
        seen_report.lock().expect("mutex should lock").is_some()
    })
    .await;
    let seen_report_request = seen_report
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("report-sync should be captured");
    assert_eq!(
        seen_report_request.report_kind,
        "openai_video_delete_sync_error"
    );
    assert_eq!(seen_report_request.status_code, 409);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_finalizes_gemini_video_cancel_sync_http_errors_without_control_execute() {
    #[derive(Debug, Clone)]
    struct SeenReportSyncRequest {
        report_kind: String,
        status_code: u64,
    }

    let seen_report = Arc::new(Mutex::new(None::<SeenReportSyncRequest>));
    let seen_report_clone = Arc::clone(&seen_report);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/resolve",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "proxy_public",
                    "route_class": "ai_public",
                    "route_family": "gemini",
                    "route_kind": "video",
                    "auth_endpoint_signature": "gemini:video",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-gemini-video-cancel-error-123",
                        "api_key_id": "key-gemini-video-cancel-error-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1beta/models/veo-3/operations/task-123:cancel"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync_decision",
                    "decision_kind": "gemini_video_cancel_sync",
                    "request_id": "req-gemini-video-cancel-error-123",
                    "provider_name": "gemini",
                    "provider_id": "provider-gemini-video-cancel-error-123",
                    "endpoint_id": "endpoint-gemini-video-cancel-error-123",
                    "key_id": "key-gemini-video-cancel-error-123",
                    "upstream_base_url": "https://generativelanguage.googleapis.com",
                    "upstream_url": "https://generativelanguage.googleapis.com/v1beta/models/veo-3/operations/ext-123:cancel",
                    "provider_request_method": "POST",
                    "provider_request_headers": {
                        "x-goog-api-key": "upstream-key"
                    },
                    "provider_request_body": {},
                    "client_api_format": "gemini:video",
                    "provider_api_format": "gemini:video",
                    "model_name": "veo-3",
                    "report_kind": "gemini_video_cancel_sync_finalize",
                    "report_context": {
                        "user_id": "user-gemini-video-cancel-error-123",
                        "api_key_id": "key-gemini-video-cancel-error-123",
                        "task_id": "task-123"
                    }
                }))
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(move |request: Request| {
                let seen_report_inner = Arc::clone(&seen_report_clone);
                async move {
                    let (_parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX)
                        .await
                        .expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("report payload should parse");
                    *seen_report_inner.lock().expect("mutex should lock") =
                        Some(SeenReportSyncRequest {
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
            "/v1beta/models/veo-3/operations/task-123:cancel",
            any(|_request: Request| async move {
                (StatusCode::IM_A_TEAPOT, Body::from("public-route-hit"))
            }),
        );

    let executor = Router::new().route(
        "/v1/execute/sync",
        any(|_request: Request| async move {
            Json(json!({
                "request_id": "req-gemini-video-cancel-error-123",
                "status_code": 409,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "error": {
                            "message": "cannot cancel completed operation"
                        }
                    }
                }
            }))
        }),
    );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let (executor_url, executor_handle) = start_server(executor).await;
    let gateway =
        build_router_with_endpoints(upstream_url.clone(), Some(upstream_url), Some(executor_url))
            .expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/v1beta/models/veo-3/operations/task-123:cancel"
        ))
        .header(TRACE_ID_HEADER, "trace-gemini-video-cancel-error-123")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({"error": {"message": "cannot cancel completed operation"}})
    );

    wait_until(300, || {
        seen_report.lock().expect("mutex should lock").is_some()
    })
    .await;
    let seen_report_request = seen_report
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("report-sync should be captured");
    assert_eq!(
        seen_report_request.report_kind,
        "gemini_video_cancel_sync_error"
    );
    assert_eq!(seen_report_request.status_code, 409);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
