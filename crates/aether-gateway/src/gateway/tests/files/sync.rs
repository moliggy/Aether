use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};

use super::*;

#[tokio::test]
async fn gateway_executes_gemini_files_get_via_executor_sync_plan() {
    #[derive(Debug, Clone)]
    struct SeenPlanSyncRequest {
        trace_id: String,
        method: String,
        path: String,
        query_string: String,
        auth_context_present: bool,
    }

    #[derive(Debug, Clone)]
    struct SeenExecutorSyncRequest {
        trace_id: String,
        method: String,
        url: String,
        stream: bool,
        client_api_format: String,
    }

    #[derive(Debug, Clone)]
    struct SeenReportSyncRequest {
        trace_id: String,
        report_kind: String,
        status_code: u64,
        file_key_id: String,
        user_id: String,
        file_name: String,
    }

    let seen_plan = Arc::new(Mutex::new(None::<SeenPlanSyncRequest>));
    let seen_plan_clone = Arc::clone(&seen_plan);
    let seen_executor = Arc::new(Mutex::new(None::<SeenExecutorSyncRequest>));
    let seen_executor_clone = Arc::clone(&seen_executor);
    let seen_report = Arc::new(Mutex::new(None::<SeenReportSyncRequest>));
    let seen_report_clone = Arc::clone(&seen_report);
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
                    "route_kind": "files",
                    "auth_endpoint_signature": "gemini:chat",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-files-sync-123",
                        "api_key_id": "key-files-sync-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1beta/files/files/abc-123"
                }))
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
                    *seen_plan_inner.lock().expect("mutex should lock") =
                        Some(SeenPlanSyncRequest {
                            trace_id: parts
                                .headers
                                .get(TRACE_ID_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            method: payload
                                .get("method")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            path: payload
                                .get("path")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            query_string: payload
                                .get("query_string")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            auth_context_present: payload
                                .get("auth_context")
                                .is_some_and(|value| !value.is_null()),
                        });
                    Json(json!({
                        "action": "executor_sync",
                        "plan_kind": "gemini_files_get",
                        "plan": {
                            "request_id": "req-files-sync-123",
                            "provider_id": "provider-files-sync-123",
                            "endpoint_id": "endpoint-files-sync-123",
                            "key_id": "file-key-sync-123",
                            "provider_name": "gemini",
                            "method": "GET",
                            "url": "https://files.example/v1beta/files/files/abc-123?view=FULL",
                            "headers": {
                                "authorization": "Bearer upstream-key"
                            },
                            "body": {},
                            "stream": false,
                            "client_api_format": "gemini:files",
                            "provider_api_format": "gemini:files",
                            "model_name": "gemini-files"
                        },
                        "report_kind": "gemini_files_store_mapping",
                        "report_context": {
                            "file_key_id": "file-key-sync-123",
                            "user_id": "user-files-sync-123"
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
                            file_key_id: payload
                                .get("report_context")
                                .and_then(|value| value.get("file_key_id"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            user_id: payload
                                .get("report_context")
                                .and_then(|value| value.get("user_id"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            file_name: payload
                                .get("body_json")
                                .and_then(|value| value.get("name"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        });
                    Json(json!({"ok": true}))
                }
            }),
        )
        .route(
            "/v1beta/files/files/abc-123",
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
                        stream: payload
                            .get("stream")
                            .and_then(|value| value.as_bool())
                            .unwrap_or(true),
                        client_api_format: payload
                            .get("client_api_format")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                    });
                Json(json!({
                    "request_id": "req-files-sync-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "name": "files/abc-123",
                            "displayName": "ABC"
                        }
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
        .get(format!(
            "{gateway_url}/v1beta/files/files/abc-123?view=FULL"
        ))
        .header(TRACE_ID_HEADER, "trace-files-sync-123")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/json")
    );
    assert_eq!(
        response
            .json::<serde_json::Value>()
            .await
            .expect("json should parse"),
        json!({
            "name": "files/abc-123",
            "displayName": "ABC"
        })
    );

    let seen_plan_request = seen_plan
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("plan-sync should be captured");
    assert_eq!(seen_plan_request.trace_id, "trace-files-sync-123");
    assert_eq!(seen_plan_request.method, "GET");
    assert_eq!(seen_plan_request.path, "/v1beta/files/files/abc-123");
    assert_eq!(seen_plan_request.query_string, "view=FULL");
    assert!(!seen_plan_request.auth_context_present);

    let seen_executor_request = seen_executor
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("executor sync should be captured");
    assert_eq!(seen_executor_request.trace_id, "trace-files-sync-123");
    assert_eq!(seen_executor_request.method, "GET");
    assert_eq!(
        seen_executor_request.url,
        "https://files.example/v1beta/files/files/abc-123?view=FULL"
    );
    assert!(!seen_executor_request.stream);
    assert_eq!(seen_executor_request.client_api_format, "gemini:files");

    wait_until(300, || {
        seen_report.lock().expect("mutex should lock").is_some()
    })
    .await;

    let seen_report_request = seen_report
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("report-sync should be captured");
    assert_eq!(seen_report_request.trace_id, "trace-files-sync-123");
    assert_eq!(
        seen_report_request.report_kind,
        "gemini_files_store_mapping"
    );
    assert_eq!(seen_report_request.status_code, 200);
    assert_eq!(seen_report_request.file_key_id, "file-key-sync-123");
    assert_eq!(seen_report_request.user_id, "user-files-sync-123");
    assert_eq!(seen_report_request.file_name, "files/abc-123");
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_gemini_files_upload_via_executor_sync_plan() {
    #[derive(Debug, Clone)]
    struct SeenPlanSyncRequest {
        method: String,
        path: String,
        body_base64: String,
    }

    #[derive(Debug, Clone)]
    struct SeenExecutorSyncRequest {
        method: String,
        url: String,
        body_bytes_b64: String,
    }

    #[derive(Debug, Clone)]
    struct SeenReportSyncRequest {
        report_kind: String,
        file_key_id: String,
        user_id: String,
        file_name: String,
    }

    let seen_plan = Arc::new(Mutex::new(None::<SeenPlanSyncRequest>));
    let seen_plan_clone = Arc::clone(&seen_plan);
    let seen_executor = Arc::new(Mutex::new(None::<SeenExecutorSyncRequest>));
    let seen_executor_clone = Arc::clone(&seen_executor);
    let seen_report = Arc::new(Mutex::new(None::<SeenReportSyncRequest>));
    let seen_report_clone = Arc::clone(&seen_report);
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
                    "route_kind": "files",
                    "auth_endpoint_signature": "gemini:chat",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-files-upload-123",
                        "api_key_id": "key-files-upload-123",
                        "access_allowed": true
                    },
                    "public_path": "/upload/v1beta/files"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/plan-sync",
            any(move |request: Request| {
                let seen_plan_inner = Arc::clone(&seen_plan_clone);
                async move {
                    let (_parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("plan payload should parse");
                    *seen_plan_inner.lock().expect("mutex should lock") =
                        Some(SeenPlanSyncRequest {
                            method: payload
                                .get("method")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            path: payload
                                .get("path")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            body_base64: payload
                                .get("body_base64")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        });
                    Json(json!({
                        "action": "executor_sync",
                        "plan_kind": "gemini_files_upload",
                        "plan": {
                            "request_id": "req-files-upload-123",
                            "provider_id": "provider-files-upload-123",
                            "endpoint_id": "endpoint-files-upload-123",
                            "key_id": "file-key-upload-123",
                            "provider_name": "gemini",
                            "method": "POST",
                            "url": "https://files.example/upload/v1beta/files?uploadType=resumable",
                            "headers": {
                                "content-type": "application/octet-stream",
                                "x-goog-api-key": "upstream-key"
                            },
                            "body": {
                                "body_bytes_b64": "dXBsb2FkLWJ5dGVz"
                            },
                            "stream": false,
                            "client_api_format": "gemini:files",
                            "provider_api_format": "gemini:files",
                            "model_name": "gemini-files"
                        },
                        "report_kind": "gemini_files_store_mapping",
                        "report_context": {
                            "file_key_id": "file-key-upload-123",
                            "user_id": "user-files-upload-123"
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
                            file_key_id: payload
                                .get("report_context")
                                .and_then(|value| value.get("file_key_id"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            user_id: payload
                                .get("report_context")
                                .and_then(|value| value.get("user_id"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            file_name: payload
                                .get("body_json")
                                .and_then(|value| value.get("file"))
                                .and_then(|value| value.get("name"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        });
                    Json(json!({"ok": true}))
                }
            }),
        )
        .route(
            "/upload/v1beta/files",
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
                        body_bytes_b64: payload
                            .get("body")
                            .and_then(|value| value.get("body_bytes_b64"))
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                    });
                Json(json!({
                    "request_id": "req-files-upload-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "file": {
                                "name": "files/uploaded-123"
                            }
                        }
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
        .post(format!(
            "{gateway_url}/upload/v1beta/files?uploadType=resumable"
        ))
        .header(http::header::CONTENT_TYPE, "application/octet-stream")
        .header(TRACE_ID_HEADER, "trace-files-upload-123")
        .body("upload-bytes")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .json::<serde_json::Value>()
            .await
            .expect("json should parse"),
        json!({"file": {"name": "files/uploaded-123"}})
    );

    let seen_plan_request = seen_plan
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("plan-sync should be captured");
    assert_eq!(seen_plan_request.method, "POST");
    assert_eq!(seen_plan_request.path, "/upload/v1beta/files");
    assert_eq!(
        BASE64_STANDARD
            .decode(seen_plan_request.body_base64)
            .expect("body should decode"),
        b"upload-bytes"
    );

    let seen_executor_request = seen_executor
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("executor sync should be captured");
    assert_eq!(seen_executor_request.method, "POST");
    assert_eq!(
        seen_executor_request.url,
        "https://files.example/upload/v1beta/files?uploadType=resumable"
    );
    assert_eq!(
        BASE64_STANDARD
            .decode(seen_executor_request.body_bytes_b64)
            .expect("executor body should decode"),
        b"upload-bytes"
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
        "gemini_files_store_mapping"
    );
    assert_eq!(seen_report_request.file_key_id, "file-key-upload-123");
    assert_eq!(seen_report_request.user_id, "user-files-upload-123");
    assert_eq!(seen_report_request.file_name, "files/uploaded-123");
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_gemini_files_delete_via_executor_sync_plan() {
    #[derive(Debug, Clone)]
    struct SeenExecutorSyncRequest {
        method: String,
        url: String,
    }

    #[derive(Debug, Clone)]
    struct SeenReportSyncRequest {
        report_kind: String,
        file_name: String,
    }

    let seen_executor = Arc::new(Mutex::new(None::<SeenExecutorSyncRequest>));
    let seen_executor_clone = Arc::clone(&seen_executor);
    let seen_report = Arc::new(Mutex::new(None::<SeenReportSyncRequest>));
    let seen_report_clone = Arc::clone(&seen_report);
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
                    "route_kind": "files",
                    "auth_endpoint_signature": "gemini:chat",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-files-delete-123",
                        "api_key_id": "key-files-delete-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1beta/files/files/abc-123"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/plan-sync",
            any(|_request: Request| async move {
                Json(json!({
                    "action": "executor_sync",
                    "plan_kind": "gemini_files_delete",
                    "plan": {
                        "request_id": "req-files-delete-123",
                        "provider_id": "provider-files-delete-123",
                        "endpoint_id": "endpoint-files-delete-123",
                        "key_id": "file-key-delete-123",
                        "provider_name": "gemini",
                        "method": "DELETE",
                        "url": "https://files.example/v1beta/files/files/abc-123",
                        "headers": {
                            "x-goog-api-key": "upstream-key"
                        },
                        "body": {},
                        "stream": false,
                        "client_api_format": "gemini:files",
                        "provider_api_format": "gemini:files",
                        "model_name": "gemini-files"
                    },
                    "report_kind": "gemini_files_delete_mapping",
                    "report_context": {
                        "file_name": "files/abc-123"
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
                            file_name: payload
                                .get("report_context")
                                .and_then(|value| value.get("file_name"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        });
                    Json(json!({"ok": true}))
                }
            }),
        )
        .route(
            "/v1beta/files/files/abc-123",
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
                    });
                Json(json!({
                    "request_id": "req-files-delete-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {}
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
        .delete(format!("{gateway_url}/v1beta/files/files/abc-123"))
        .header(TRACE_ID_HEADER, "trace-files-delete-123")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .json::<serde_json::Value>()
            .await
            .expect("json should parse"),
        json!({})
    );

    let seen_executor_request = seen_executor
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("executor sync should be captured");
    assert_eq!(seen_executor_request.method, "DELETE");
    assert_eq!(
        seen_executor_request.url,
        "https://files.example/v1beta/files/files/abc-123"
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
        "gemini_files_delete_mapping"
    );
    assert_eq!(seen_report_request.file_name, "files/abc-123");
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
