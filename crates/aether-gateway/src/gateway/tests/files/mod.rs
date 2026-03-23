use super::*;

mod stream;
mod sync;

#[tokio::test]
async fn gateway_executes_gemini_files_get_via_executor_sync_decision() {
    #[derive(Debug, Clone)]
    struct SeenDecisionSyncRequest {
        trace_id: String,
        path: String,
    }

    #[derive(Debug, Clone)]
    struct SeenExecutorSyncRequest {
        method: String,
        url: String,
        provider_api_format: String,
    }

    let seen_decision = Arc::new(Mutex::new(None::<SeenDecisionSyncRequest>));
    let seen_decision_clone = Arc::clone(&seen_decision);
    let seen_executor = Arc::new(Mutex::new(None::<SeenExecutorSyncRequest>));
    let seen_executor_clone = Arc::clone(&seen_executor);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/auth-context",
            any(|_request: Request| async move {
                Json(json!({
                    "auth_context": {
                        "user_id": "user-files-get-decision-123",
                        "api_key_id": "key-files-get-decision-123",
                        "access_allowed": true
                    }
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
                        });
                    Json(json!({
                        "action": "executor_sync_decision",
                        "decision_kind": "gemini_files_get",
                        "request_id": "req-files-get-decision-123",
                        "provider_name": "gemini",
                        "provider_id": "provider-files-get-decision-123",
                        "endpoint_id": "endpoint-files-get-decision-123",
                        "key_id": "key-files-get-decision-123",
                        "upstream_url": "https://generativelanguage.googleapis.com/v1beta/files/files/abc-123",
                        "provider_api_format": "gemini:files",
                        "client_api_format": "gemini:files",
                        "model_name": "gemini-files",
                        "provider_request_headers": {
                            "x-goog-api-key": "provider-key"
                        },
                        "report_kind": "gemini_files_store_mapping",
                        "report_context": {
                            "file_key_id": "key-files-get-decision-123",
                            "user_id": "user-files-get-decision-123"
                        }
                    }))
                }
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(|_request: Request| async move { StatusCode::NO_CONTENT }),
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
                        provider_api_format: payload
                            .get("provider_api_format")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                    });
                Json(json!({
                    "request_id": "req-files-get-decision-123",
                    "status_code": 200,
                    "headers": {"content-type": "application/json"},
                    "body": {"json_body": {"name": "files/abc-123"}}
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
        .get(format!("{gateway_url}/v1beta/files/files/abc-123"))
        .header(TRACE_ID_HEADER, "trace-files-get-decision-123")
        .header("x-goog-api-key", "client-key")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.text().await.expect("body should read"),
        "{\"name\":\"files/abc-123\"}"
    );

    let seen_decision_request = seen_decision
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("decision-sync should be captured");
    assert_eq!(
        seen_decision_request.trace_id,
        "trace-files-get-decision-123"
    );
    assert_eq!(seen_decision_request.path, "/v1beta/files/files/abc-123");

    let seen_executor_request = seen_executor
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("executor request should be captured");
    assert_eq!(seen_executor_request.method, "GET");
    assert_eq!(
        seen_executor_request.url,
        "https://generativelanguage.googleapis.com/v1beta/files/files/abc-123"
    );
    assert_eq!(seen_executor_request.provider_api_format, "gemini:files");
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

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
