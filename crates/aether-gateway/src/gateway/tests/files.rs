use aether_contracts::{StreamFrame, StreamFramePayload, StreamFrameType};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};

use super::*;

#[tokio::test]
async fn gateway_executes_gemini_files_download_via_control_sync_endpoint() {
    #[derive(Debug, Clone)]
    struct SeenExecuteFilesRequest {
        trace_id: String,
        method: String,
        path: String,
        body_base64: Option<String>,
    }

    let seen_execute = Arc::new(Mutex::new(None::<SeenExecuteFilesRequest>));
    let seen_execute_clone = Arc::clone(&seen_execute);
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
                        "user_id": "user-files-123",
                        "api_key_id": "key-files-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1beta/files/file-123:download"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/execute-sync",
            any(move |request: Request| {
                let seen_execute_inner = Arc::clone(&seen_execute_clone);
                async move {
                    let (parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("execute payload should parse");
                    *seen_execute_inner.lock().expect("mutex should lock") =
                        Some(SeenExecuteFilesRequest {
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
                            body_base64: payload
                                .get("body_base64")
                                .and_then(|value| value.as_str())
                                .map(ToOwned::to_owned),
                        });
                    let mut response = Response::builder()
                        .status(StatusCode::OK)
                        .body(Body::from("file-bytes"))
                        .expect("response should build");
                    response.headers_mut().insert(
                        http::header::CONTENT_TYPE,
                        HeaderValue::from_static("application/octet-stream"),
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
            "/v1beta/files/file-123:download",
            any(move |_request: Request| {
                let public_hits_inner = Arc::clone(&public_hits_clone);
                async move {
                    *public_hits_inner.lock().expect("mutex should lock") += 1;
                    (StatusCode::IM_A_TEAPOT, Body::from("public-route-hit"))
                }
            }),
        );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_control(upstream_url.clone(), Some(upstream_url))
        .expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/v1beta/files/file-123:download?alt=media"
        ))
        .header(TRACE_ID_HEADER, "trace-files-download-123")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.text().await.expect("body should read"),
        "file-bytes"
    );

    let seen_execute_request = seen_execute
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("execute-sync should be captured");
    assert_eq!(seen_execute_request.trace_id, "trace-files-download-123");
    assert_eq!(seen_execute_request.method, "GET");
    assert_eq!(seen_execute_request.path, "/v1beta/files/file-123:download");
    assert!(seen_execute_request.body_base64.is_none());
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_gemini_files_upload_via_control_sync_endpoint() {
    #[derive(Debug, Clone)]
    struct SeenExecuteFilesUploadRequest {
        path: String,
        body_base64: String,
        content_type: String,
    }

    let seen_execute = Arc::new(Mutex::new(None::<SeenExecuteFilesUploadRequest>));
    let seen_execute_clone = Arc::clone(&seen_execute);
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
                        "user_id": "user-files-456",
                        "api_key_id": "key-files-456",
                        "access_allowed": true
                    },
                    "public_path": "/upload/v1beta/files"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/execute-sync",
            any(move |request: Request| {
                let seen_execute_inner = Arc::clone(&seen_execute_clone);
                async move {
                    let (_parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("execute payload should parse");
                    *seen_execute_inner.lock().expect("mutex should lock") =
                        Some(SeenExecuteFilesUploadRequest {
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
                            content_type: payload
                                .get("headers")
                                .and_then(|value| value.get("content-type"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        });
                    let mut response = Response::builder()
                        .status(StatusCode::CREATED)
                        .body(Body::from("{\"uploaded\":true}"))
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
            "/upload/v1beta/files",
            any(move |_request: Request| {
                let public_hits_inner = Arc::clone(&public_hits_clone);
                async move {
                    *public_hits_inner.lock().expect("mutex should lock") += 1;
                    (StatusCode::IM_A_TEAPOT, Body::from("public-route-hit"))
                }
            }),
        );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_control(upstream_url.clone(), Some(upstream_url))
        .expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/upload/v1beta/files?uploadType=resumable"
        ))
        .header(http::header::CONTENT_TYPE, "application/octet-stream")
        .body("upload-body-bytes")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(
        response.text().await.expect("body should read"),
        "{\"uploaded\":true}"
    );

    let seen_execute_request = seen_execute
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("execute-sync should be captured");
    assert_eq!(seen_execute_request.path, "/upload/v1beta/files");
    assert_eq!(
        BASE64_STANDARD
            .decode(seen_execute_request.body_base64)
            .expect("body should decode"),
        b"upload-body-bytes"
    );
    assert_eq!(
        seen_execute_request.content_type,
        "application/octet-stream"
    );
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_gemini_files_download_via_executor_stream_plan() {
    #[derive(Debug, Clone)]
    struct SeenPlanStreamRequest {
        trace_id: String,
        method: String,
        path: String,
        query_string: String,
        user_id: String,
    }

    #[derive(Debug, Clone)]
    struct SeenExecutorStreamRequest {
        trace_id: String,
        method: String,
        url: String,
        stream: bool,
        client_api_format: String,
    }

    let seen_plan = Arc::new(Mutex::new(None::<SeenPlanStreamRequest>));
    let seen_plan_clone = Arc::clone(&seen_plan);
    let seen_executor = Arc::new(Mutex::new(None::<SeenExecutorStreamRequest>));
    let seen_executor_clone = Arc::clone(&seen_executor);
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
                        "user_id": "user-files-direct-123",
                        "api_key_id": "key-files-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1beta/files/file-123:download"
                }))
            }),
        )
        .route(
            "/api/internal/gateway/plan-stream",
            any(move |request: Request| {
                let seen_plan_inner = Arc::clone(&seen_plan_clone);
                async move {
                    let (parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("plan payload should parse");
                    *seen_plan_inner.lock().expect("mutex should lock") =
                        Some(SeenPlanStreamRequest {
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
                            user_id: payload
                                .get("auth_context")
                                .and_then(|value| value.get("user_id"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        });
                    Json(json!({
                        "action": "executor_stream",
                        "plan_kind": "gemini_files_download",
                        "plan": {
                            "request_id": "req-files-direct-123",
                            "provider_id": "provider-files-direct-123",
                            "endpoint_id": "endpoint-files-direct-123",
                            "key_id": "key-files-direct-123",
                            "provider_name": "gemini",
                            "method": "GET",
                            "url": "https://files.example/v1beta/files/file-123:download?alt=media",
                            "headers": {
                                "authorization": "Bearer upstream-key"
                            },
                            "body": {},
                            "stream": true,
                            "client_api_format": "gemini:files",
                            "provider_api_format": "gemini:files",
                            "model_name": "gemini-files"
                        }
                    }))
                }
            }),
        )
        .route(
            "/v1beta/files/file-123:download",
            any(move |_request: Request| {
                let public_hits_inner = Arc::clone(&public_hits_clone);
                async move {
                    *public_hits_inner.lock().expect("mutex should lock") += 1;
                    (StatusCode::IM_A_TEAPOT, Body::from("public-route-hit"))
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
                            .unwrap_or(false),
                        client_api_format: payload
                            .get("client_api_format")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                    });

                let frames = [
                    StreamFrame {
                        frame_type: StreamFrameType::Headers,
                        payload: StreamFramePayload::Headers {
                            status_code: 200,
                            headers: std::collections::BTreeMap::from([(
                                "content-type".to_string(),
                                "application/octet-stream".to_string(),
                            )]),
                        },
                    },
                    StreamFrame {
                        frame_type: StreamFrameType::Data,
                        payload: StreamFramePayload::Data {
                            chunk_b64: Some(BASE64_STANDARD.encode(b"file-direct-")),
                            text: None,
                        },
                    },
                    StreamFrame {
                        frame_type: StreamFrameType::Data,
                        payload: StreamFramePayload::Data {
                            chunk_b64: Some(BASE64_STANDARD.encode(b"bytes")),
                            text: None,
                        },
                    },
                    StreamFrame::eof(),
                ];
                let body = frames
                    .into_iter()
                    .map(|frame| serde_json::to_string(&frame).expect("frame should serialize"))
                    .collect::<Vec<_>>()
                    .join("\n")
                    + "\n";
                let mut response = Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from(body))
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
    let gateway =
        build_router_with_endpoints(upstream_url.clone(), Some(upstream_url), Some(executor_url))
            .expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/v1beta/files/file-123:download?alt=media"
        ))
        .header(TRACE_ID_HEADER, "trace-files-direct-123")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/octet-stream")
    );
    assert_eq!(
        response
            .headers()
            .get(CONTROL_ROUTE_CLASS_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some("ai_public")
    );
    assert_eq!(
        response.bytes().await.expect("body should read"),
        Bytes::from_static(b"file-direct-bytes")
    );

    let seen_plan_request = seen_plan
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("plan-stream should be captured");
    assert_eq!(seen_plan_request.trace_id, "trace-files-direct-123");
    assert_eq!(seen_plan_request.method, "GET");
    assert_eq!(seen_plan_request.path, "/v1beta/files/file-123:download");
    assert_eq!(seen_plan_request.query_string, "alt=media");
    assert_eq!(seen_plan_request.user_id, "user-files-direct-123");

    let seen_executor_request = seen_executor
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("executor stream should be captured");
    assert_eq!(seen_executor_request.trace_id, "trace-files-direct-123");
    assert_eq!(seen_executor_request.method, "GET");
    assert_eq!(
        seen_executor_request.url,
        "https://files.example/v1beta/files/file-123:download?alt=media"
    );
    assert!(seen_executor_request.stream);
    assert_eq!(seen_executor_request.client_api_format, "gemini:files");
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_gemini_files_get_via_executor_sync_plan() {
    #[derive(Debug, Clone)]
    struct SeenPlanSyncRequest {
        trace_id: String,
        method: String,
        path: String,
        query_string: String,
        user_id: String,
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
                            user_id: payload
                                .get("auth_context")
                                .and_then(|value| value.get("user_id"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
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
    assert_eq!(seen_plan_request.user_id, "user-files-sync-123");

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
