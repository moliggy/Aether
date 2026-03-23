use super::*;

mod error;
mod gemini_sync_create;
mod gemini_sync_task;
mod openai_sync_create;
mod openai_sync_task;
mod stream;

#[tokio::test]
async fn gateway_executes_video_get_route_via_control_sync_endpoint() {
    #[derive(Debug, Clone)]
    struct SeenExecuteVideoRequest {
        method: String,
        path: String,
        body_json: serde_json::Value,
    }

    let seen_execute = Arc::new(Mutex::new(None::<SeenExecuteVideoRequest>));
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
                    "route_family": "openai",
                    "route_kind": "video",
                    "auth_endpoint_signature": "openai:video",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-video-123",
                        "api_key_id": "key-video-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/videos/task-123"
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
                        Some(SeenExecuteVideoRequest {
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
                            body_json: payload
                                .get("body_json")
                                .cloned()
                                .unwrap_or_else(|| json!({})),
                        });
                    let mut response = Response::builder()
                        .status(StatusCode::OK)
                        .body(Body::from("{\"status\":\"queued\"}"))
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
            "/v1/videos/task-123",
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
        .get(format!("{gateway_url}/v1/videos/task-123"))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.text().await.expect("body should read"),
        "{\"status\":\"queued\"}"
    );

    let seen_execute_request = seen_execute
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("execute-sync should be captured");
    assert_eq!(seen_execute_request.method, "GET");
    assert_eq!(seen_execute_request.path, "/v1/videos/task-123");
    assert_eq!(seen_execute_request.body_json, json!({}));
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_refreshes_openai_video_task_from_local_registry_when_rust_owns_truth_source() {
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct SeenExecutorRequest {
        method: String,
        url: String,
    }

    let seen_decision_paths = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen_decision_paths_clone = Arc::clone(&seen_decision_paths);
    let seen_executor_requests = Arc::new(Mutex::new(Vec::<SeenExecutorRequest>::new()));
    let seen_executor_requests_clone = Arc::clone(&seen_executor_requests);
    let fallback_execute_hits = Arc::new(Mutex::new(0usize));
    let fallback_execute_hits_clone = Arc::clone(&fallback_execute_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_videos_clone = Arc::clone(&public_hits);
    let public_hits_task_clone = Arc::clone(&public_hits);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/resolve",
            any(|request: Request| async move {
                let path = request.uri().path().to_string();
                let public_path = if path == "/v1/videos" {
                    "/v1/videos".to_string()
                } else {
                    path
                };
                Json(json!({
                    "action": "proxy_public",
                    "route_class": "ai_public",
                    "route_family": "openai",
                    "route_kind": "video",
                    "auth_endpoint_signature": "openai:video",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-video-registry-123",
                        "api_key_id": "key-video-registry-123",
                        "access_allowed": true
                    },
                    "public_path": public_path
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-sync",
            any(move |request: Request| {
                let seen_decision_paths_inner = Arc::clone(&seen_decision_paths_clone);
                async move {
                    let (_parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("decision payload should parse");
                    seen_decision_paths_inner
                        .lock()
                        .expect("mutex should lock")
                        .push(
                            payload
                                .get("path")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        );
                    Json(json!({
                        "action": "executor_sync_decision",
                        "decision_kind": "openai_video_create_sync",
                        "request_id": "req-openai-video-registry-123",
                        "provider_name": "openai",
                        "provider_id": "provider-openai-video-registry-123",
                        "endpoint_id": "endpoint-openai-video-registry-123",
                        "key_id": "key-openai-video-registry-123",
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
                            "user_id": "user-video-registry-123",
                            "api_key_id": "key-video-registry-123",
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
            any(|_request: Request| async move { Json(json!({"ok": true})) }),
        )
        .route(
            "/api/internal/gateway/execute-sync",
            any(move |_request: Request| {
                let fallback_execute_hits_inner = Arc::clone(&fallback_execute_hits_clone);
                async move {
                    *fallback_execute_hits_inner
                        .lock()
                        .expect("mutex should lock") += 1;
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
        )
        .route(
            "/v1/videos",
            any(move |_request: Request| {
                let public_hits_inner = Arc::clone(&public_hits_videos_clone);
                async move {
                    *public_hits_inner.lock().expect("mutex should lock") += 1;
                    (StatusCode::IM_A_TEAPOT, Body::from("public-route-hit"))
                }
            }),
        )
        .route(
            "/v1/videos/{task_id}",
            any(move |_request: Request| {
                let public_hits_inner = Arc::clone(&public_hits_task_clone);
                async move {
                    *public_hits_inner.lock().expect("mutex should lock") += 1;
                    (StatusCode::IM_A_TEAPOT, Body::from("public-route-hit"))
                }
            }),
        );

    let executor = Router::new().route(
        "/v1/execute/sync",
        any(move |request: Request| {
            let seen_executor_requests_inner = Arc::clone(&seen_executor_requests_clone);
            async move {
                let (_parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value =
                    serde_json::from_slice(&raw_body).expect("executor payload should parse");
                let method = payload
                    .get("method")
                    .and_then(|value| value.as_str())
                    .unwrap_or_default()
                    .to_string();
                let url = payload
                    .get("url")
                    .and_then(|value| value.as_str())
                    .unwrap_or_default()
                    .to_string();
                seen_executor_requests_inner
                    .lock()
                    .expect("mutex should lock")
                    .push(SeenExecutorRequest {
                        method: method.clone(),
                        url: url.clone(),
                    });
                if method == "POST" && url == "https://api.openai.example/v1/videos" {
                    return Json(json!({
                        "request_id": "req-openai-video-registry-123",
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
                    }));
                }

                Json(json!({
                    "request_id": "req-openai-video-registry-get-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "id": "ext-video-task-123",
                            "status": "processing",
                            "progress": 37
                        }
                    },
                    "telemetry": {
                        "elapsed_ms": 31
                    }
                }))
            }
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

    let create_response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/videos"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-openai-video-registry-create-123")
        .body("{\"model\":\"sora-2\",\"prompt\":\"hello\"}")
        .send()
        .await
        .expect("create request should succeed");
    assert_eq!(create_response.status(), StatusCode::OK);
    let create_json: serde_json::Value = create_response.json().await.expect("body should parse");
    let local_task_id = create_json
        .get("id")
        .and_then(|value| value.as_str())
        .expect("response id should exist")
        .to_string();

    let get_response = reqwest::Client::new()
        .get(format!("{gateway_url}/v1/videos/{local_task_id}"))
        .header(TRACE_ID_HEADER, "trace-openai-video-registry-get-123")
        .send()
        .await
        .expect("get request should succeed");

    assert_eq!(get_response.status(), StatusCode::OK);
    let get_json: serde_json::Value = get_response.json().await.expect("body should parse");
    assert_eq!(
        get_json.get("id").and_then(|value| value.as_str()),
        Some(local_task_id.as_str())
    );
    assert_eq!(get_json.get("status"), Some(&json!("processing")));
    assert_eq!(get_json.get("progress"), Some(&json!(37)));
    assert_eq!(
        seen_decision_paths
            .lock()
            .expect("mutex should lock")
            .as_slice(),
        ["/v1/videos"]
    );
    assert_eq!(
        seen_executor_requests
            .lock()
            .expect("mutex should lock")
            .as_slice(),
        [
            SeenExecutorRequest {
                method: "POST".to_string(),
                url: "https://api.openai.example/v1/videos".to_string(),
            },
            SeenExecutorRequest {
                method: "GET".to_string(),
                url: "https://api.openai.example/v1/videos/ext-video-task-123".to_string(),
            }
        ]
    );
    assert_eq!(*fallback_execute_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_refreshes_gemini_video_task_from_local_registry_when_rust_owns_truth_source() {
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct SeenExecutorRequest {
        method: String,
        url: String,
    }

    let seen_decision_paths = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen_decision_paths_clone = Arc::clone(&seen_decision_paths);
    let seen_executor_requests = Arc::new(Mutex::new(Vec::<SeenExecutorRequest>::new()));
    let seen_executor_requests_clone = Arc::clone(&seen_executor_requests);
    let fallback_execute_hits = Arc::new(Mutex::new(0usize));
    let fallback_execute_hits_clone = Arc::clone(&fallback_execute_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_create_clone = Arc::clone(&public_hits);
    let public_hits_get_clone = Arc::clone(&public_hits);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/resolve",
            any(|request: Request| async move {
                Json(json!({
                    "action": "proxy_public",
                    "route_class": "ai_public",
                    "route_family": "gemini",
                    "route_kind": "video",
                    "auth_endpoint_signature": "gemini:video",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-gemini-video-registry-123",
                        "api_key_id": "key-gemini-video-registry-123",
                        "access_allowed": true
                    },
                    "public_path": request.uri().path().to_string()
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-sync",
            any(move |request: Request| {
                let seen_decision_paths_inner = Arc::clone(&seen_decision_paths_clone);
                async move {
                    let (_parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("decision payload should parse");
                    seen_decision_paths_inner
                        .lock()
                        .expect("mutex should lock")
                        .push(
                            payload
                                .get("path")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        );
                    Json(json!({
                        "action": "executor_sync_decision",
                        "decision_kind": "gemini_video_create_sync",
                        "request_id": "req-gemini-video-registry-123",
                        "provider_name": "gemini",
                        "provider_id": "provider-gemini-video-registry-123",
                        "endpoint_id": "endpoint-gemini-video-registry-123",
                        "key_id": "key-gemini-video-registry-123",
                        "upstream_base_url": "https://generativelanguage.googleapis.com",
                        "upstream_url": "https://generativelanguage.googleapis.com/v1beta/models/veo-3:predictLongRunning",
                        "provider_request_method": "POST",
                        "auth_header": "",
                        "auth_value": "",
                        "provider_request_headers": {
                            "x-goog-api-key": "upstream-key",
                            "content-type": "application/json"
                        },
                        "provider_request_body": {
                            "prompt": "make a video"
                        },
                        "content_type": "application/json",
                        "client_api_format": "gemini:video",
                        "provider_api_format": "gemini:video",
                        "model_name": "veo-3",
                        "report_kind": "gemini_video_create_sync_finalize",
                        "report_context": {
                            "user_id": "user-gemini-video-registry-123",
                            "api_key_id": "key-gemini-video-registry-123",
                            "model": "veo-3"
                        }
                    }))
                }
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(|_request: Request| async move { Json(json!({"ok": true})) }),
        )
        .route(
            "/api/internal/gateway/execute-sync",
            any(move |_request: Request| {
                let fallback_execute_hits_inner = Arc::clone(&fallback_execute_hits_clone);
                async move {
                    *fallback_execute_hits_inner.lock().expect("mutex should lock") += 1;
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
        )
        .route(
            "/v1beta/models/veo-3:predictLongRunning",
            any(move |_request: Request| {
                let public_hits_inner = Arc::clone(&public_hits_create_clone);
                async move {
                    *public_hits_inner.lock().expect("mutex should lock") += 1;
                    (StatusCode::IM_A_TEAPOT, Body::from("public-route-hit"))
                }
            }),
        )
        .route(
            "/v1beta/models/veo-3/operations/{task_id}",
            any(move |_request: Request| {
                let public_hits_inner = Arc::clone(&public_hits_get_clone);
                async move {
                    *public_hits_inner.lock().expect("mutex should lock") += 1;
                    (StatusCode::IM_A_TEAPOT, Body::from("public-route-hit"))
                }
            }),
        );

    let executor = Router::new().route(
        "/v1/execute/sync",
        any(move |request: Request| {
            let seen_executor_requests_inner = Arc::clone(&seen_executor_requests_clone);
            async move {
                let (_parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value =
                    serde_json::from_slice(&raw_body).expect("executor payload should parse");
                let method = payload
                    .get("method")
                    .and_then(|value| value.as_str())
                    .unwrap_or_default()
                    .to_string();
                let url = payload
                    .get("url")
                    .and_then(|value| value.as_str())
                    .unwrap_or_default()
                    .to_string();
                seen_executor_requests_inner
                    .lock()
                    .expect("mutex should lock")
                    .push(SeenExecutorRequest {
                        method: method.clone(),
                        url: url.clone(),
                    });
                if method == "POST"
                    && url
                        == "https://generativelanguage.googleapis.com/v1beta/models/veo-3:predictLongRunning"
                {
                    return Json(json!({
                        "request_id": "req-gemini-video-registry-123",
                        "status_code": 200,
                        "headers": {
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "name": "operations/ext-video-123"
                            }
                        },
                        "telemetry": {
                            "elapsed_ms": 55
                        }
                    }));
                }

                Json(json!({
                    "request_id": "req-gemini-video-registry-get-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "name": "operations/ext-video-123",
                            "done": false,
                            "metadata": {
                                "state": "PROCESSING"
                            }
                        }
                    },
                    "telemetry": {
                        "elapsed_ms": 29
                    }
                }))
            }
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

    let create_response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/v1beta/models/veo-3:predictLongRunning"
        ))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-gemini-video-registry-create-123")
        .body("{\"prompt\":\"make a video\"}")
        .send()
        .await
        .expect("create request should succeed");
    assert_eq!(create_response.status(), StatusCode::OK);
    let create_json: serde_json::Value = create_response.json().await.expect("body should parse");
    let local_short_id = create_json
        .get("name")
        .and_then(|value| value.as_str())
        .and_then(|value| value.rsplit('/').next())
        .expect("local operation short id should exist")
        .to_string();

    let get_response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/v1beta/models/veo-3/operations/{local_short_id}"
        ))
        .header(TRACE_ID_HEADER, "trace-gemini-video-registry-get-123")
        .send()
        .await
        .expect("get request should succeed");

    assert_eq!(get_response.status(), StatusCode::OK);
    let get_json: serde_json::Value = get_response.json().await.expect("body should parse");
    assert_eq!(
        get_json.get("name").and_then(|value| value.as_str()),
        Some(format!("models/veo-3/operations/{local_short_id}").as_str())
    );
    assert_eq!(get_json.get("done"), Some(&json!(false)));
    assert_eq!(
        get_json.get("metadata"),
        Some(&json!({"state": "PROCESSING"}))
    );
    assert_eq!(
        seen_decision_paths
            .lock()
            .expect("mutex should lock")
            .as_slice(),
        ["/v1beta/models/veo-3:predictLongRunning"]
    );
    assert_eq!(
        seen_executor_requests
            .lock()
            .expect("mutex should lock")
            .as_slice(),
        [
            SeenExecutorRequest {
                method: "POST".to_string(),
                url: "https://generativelanguage.googleapis.com/v1beta/models/veo-3:predictLongRunning"
                    .to_string(),
            },
            SeenExecutorRequest {
                method: "GET".to_string(),
                url: "https://generativelanguage.googleapis.com/v1beta/models/veo-3/operations/ext-video-123"
                    .to_string(),
            }
        ]
    );
    assert_eq!(*fallback_execute_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_reads_cancelled_openai_video_task_from_local_registry_when_rust_owns_truth_source()
{
    let seen_decision_paths = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen_decision_paths_clone = Arc::clone(&seen_decision_paths);
    let seen_executor_requests = Arc::new(Mutex::new(Vec::<(String, String)>::new()));
    let seen_executor_requests_clone = Arc::clone(&seen_executor_requests);
    let execute_hits = Arc::new(Mutex::new(0usize));
    let execute_hits_clone = Arc::clone(&execute_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/resolve",
            any(|request: Request| async move {
                Json(json!({
                    "action": "proxy_public",
                    "route_class": "ai_public",
                    "route_family": "openai",
                    "route_kind": "video",
                    "auth_endpoint_signature": "openai:video",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-video-cancel-registry-123",
                        "api_key_id": "key-video-cancel-registry-123",
                        "access_allowed": true
                    },
                    "public_path": request.uri().path().to_string()
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-sync",
            any(move |request: Request| {
                let seen_decision_paths_inner = Arc::clone(&seen_decision_paths_clone);
                async move {
                    let (_parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("decision payload should parse");
                    let path = payload
                        .get("path")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string();
                    seen_decision_paths_inner
                        .lock()
                        .expect("mutex should lock")
                        .push(path.clone());
                    if path == "/v1/videos" {
                        return Json(json!({
                            "action": "executor_sync_decision",
                            "decision_kind": "openai_video_create_sync",
                            "request_id": "req-openai-video-cancel-create-123",
                            "provider_name": "openai",
                            "provider_id": "provider-openai-video-cancel-create-123",
                            "endpoint_id": "endpoint-openai-video-cancel-create-123",
                            "key_id": "key-openai-video-cancel-create-123",
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
                                "user_id": "user-video-cancel-registry-123",
                                "api_key_id": "key-video-cancel-registry-123",
                                "model": "sora-2",
                                "original_request_body": {
                                    "model": "sora-2",
                                    "prompt": "hello"
                                }
                            }
                        }));
                    }

                    let task_id = path
                        .strip_prefix("/v1/videos/")
                        .and_then(|value| value.strip_suffix("/cancel"))
                        .expect("cancel path should include local task id");
                    Json(json!({
                        "action": "executor_sync_decision",
                        "decision_kind": "openai_video_cancel_sync",
                        "request_id": "req-openai-video-cancel-cancel-123",
                        "provider_name": "openai",
                        "provider_id": "provider-openai-video-cancel-cancel-123",
                        "endpoint_id": "endpoint-openai-video-cancel-cancel-123",
                        "key_id": "key-openai-video-cancel-cancel-123",
                        "upstream_base_url": "https://api.openai.example",
                        "upstream_url": "https://api.openai.example/v1/videos/ext-video-task-123",
                        "provider_request_method": "DELETE",
                        "auth_header": "",
                        "auth_value": "",
                        "provider_request_headers": {
                            "authorization": "Bearer upstream-key"
                        },
                        "provider_request_body": null,
                        "client_api_format": "openai:video",
                        "provider_api_format": "openai:video",
                        "model_name": "sora-2",
                        "report_kind": "openai_video_cancel_sync_finalize",
                        "report_context": {
                            "user_id": "user-video-cancel-registry-123",
                            "api_key_id": "key-video-cancel-registry-123",
                            "task_id": task_id
                        }
                    }))
                }
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(|_request: Request| async move { Json(json!({"ok": true})) }),
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
        )
        .route(
            "/{*path}",
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
            let seen_executor_requests_inner = Arc::clone(&seen_executor_requests_clone);
            async move {
                let (_parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value =
                    serde_json::from_slice(&raw_body).expect("executor payload should parse");
                seen_executor_requests_inner
                    .lock()
                    .expect("mutex should lock")
                    .push((
                        payload
                            .get("method")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        payload
                            .get("url")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                    ));
                let url = payload
                    .get("url")
                    .and_then(|value| value.as_str())
                    .unwrap_or_default();
                if url.ends_with("/v1/videos") {
                    return Json(json!({
                        "request_id": "req-openai-video-cancel-create-123",
                        "status_code": 200,
                        "headers": {
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "id": "ext-video-task-123",
                                "status": "submitted"
                            }
                        }
                    }));
                }

                Json(json!({
                    "request_id": "req-openai-video-cancel-cancel-123",
                    "status_code": 204,
                    "headers": {}
                }))
            }
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

    let create_response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/videos"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(
            TRACE_ID_HEADER,
            "trace-openai-video-cancel-registry-create-123",
        )
        .body("{\"model\":\"sora-2\",\"prompt\":\"hello\"}")
        .send()
        .await
        .expect("create request should succeed");
    assert_eq!(create_response.status(), StatusCode::OK);
    let create_json: serde_json::Value = create_response.json().await.expect("body should parse");
    let local_task_id = create_json
        .get("id")
        .and_then(|value| value.as_str())
        .expect("response id should exist")
        .to_string();

    let cancel_response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/videos/{local_task_id}/cancel"))
        .header(
            TRACE_ID_HEADER,
            "trace-openai-video-cancel-registry-cancel-123",
        )
        .send()
        .await
        .expect("cancel request should succeed");
    assert_eq!(cancel_response.status(), StatusCode::OK);
    assert_eq!(
        cancel_response
            .json::<serde_json::Value>()
            .await
            .expect("body should parse"),
        json!({})
    );

    let get_response = reqwest::Client::new()
        .get(format!("{gateway_url}/v1/videos/{local_task_id}"))
        .header(
            TRACE_ID_HEADER,
            "trace-openai-video-cancel-registry-get-123",
        )
        .send()
        .await
        .expect("get request should succeed");
    assert_eq!(get_response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        get_response
            .json::<serde_json::Value>()
            .await
            .expect("body should parse"),
        json!({"detail": "Video task was cancelled"})
    );
    assert_eq!(
        *seen_decision_paths.lock().expect("mutex should lock"),
        vec!["/v1/videos".to_string()]
    );
    assert_eq!(
        *seen_executor_requests.lock().expect("mutex should lock"),
        vec![
            (
                "POST".to_string(),
                "https://api.openai.example/v1/videos".to_string(),
            ),
            (
                "DELETE".to_string(),
                "https://api.openai.example/v1/videos/ext-video-task-123".to_string(),
            ),
        ]
    );
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_reads_deleted_openai_video_task_from_local_registry_when_rust_owns_truth_source() {
    let seen_decision_paths = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen_decision_paths_clone = Arc::clone(&seen_decision_paths);
    let seen_executor_requests = Arc::new(Mutex::new(Vec::<(String, String)>::new()));
    let seen_executor_requests_clone = Arc::clone(&seen_executor_requests);
    let execute_hits = Arc::new(Mutex::new(0usize));
    let execute_hits_clone = Arc::clone(&execute_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/resolve",
            any(|request: Request| async move {
                Json(json!({
                    "action": "proxy_public",
                    "route_class": "ai_public",
                    "route_family": "openai",
                    "route_kind": "video",
                    "auth_endpoint_signature": "openai:video",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-video-delete-registry-123",
                        "api_key_id": "key-video-delete-registry-123",
                        "access_allowed": true
                    },
                    "public_path": request.uri().path().to_string()
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-sync",
            any(move |request: Request| {
                let seen_decision_paths_inner = Arc::clone(&seen_decision_paths_clone);
                async move {
                    let (_parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("decision payload should parse");
                    let path = payload
                        .get("path")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string();
                    seen_decision_paths_inner
                        .lock()
                        .expect("mutex should lock")
                        .push(path.clone());
                    Json(json!({
                        "action": "executor_sync_decision",
                        "decision_kind": "openai_video_create_sync",
                        "request_id": "req-openai-video-delete-create-123",
                        "provider_name": "openai",
                        "provider_id": "provider-openai-video-delete-create-123",
                        "endpoint_id": "endpoint-openai-video-delete-create-123",
                        "key_id": "key-openai-video-delete-create-123",
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
                            "user_id": "user-video-delete-registry-123",
                            "api_key_id": "key-video-delete-registry-123",
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
            any(|_request: Request| async move { Json(json!({"ok": true})) }),
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
        )
        .route(
            "/{*path}",
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
            let seen_executor_requests_inner = Arc::clone(&seen_executor_requests_clone);
            async move {
                let (_parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value =
                    serde_json::from_slice(&raw_body).expect("executor payload should parse");
                let method = payload
                    .get("method")
                    .and_then(|value| value.as_str())
                    .unwrap_or_default()
                    .to_string();
                let url = payload
                    .get("url")
                    .and_then(|value| value.as_str())
                    .unwrap_or_default()
                    .to_string();
                seen_executor_requests_inner
                    .lock()
                    .expect("mutex should lock")
                    .push((method.clone(), url.clone()));
                if method == "POST" && url.ends_with("/v1/videos") {
                    return Json(json!({
                        "request_id": "req-openai-video-delete-create-123",
                        "status_code": 200,
                        "headers": {
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "id": "ext-video-task-123",
                                "status": "submitted"
                            }
                        }
                    }));
                }
                if method == "GET" && url.ends_with("/v1/videos/ext-video-task-123") {
                    return Json(json!({
                        "request_id": "req-openai-video-delete-refresh-123",
                        "status_code": 200,
                        "headers": {
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "id": "ext-video-task-123",
                                "status": "completed",
                                "progress": 100,
                                "completed_at": 1712345688u64
                            }
                        }
                    }));
                }

                Json(json!({
                    "request_id": "req-openai-video-delete-delete-123",
                    "status_code": 404,
                    "headers": {
                        "content-type": "application/json"
                    }
                }))
            }
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

    let create_response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/videos"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(
            TRACE_ID_HEADER,
            "trace-openai-video-delete-registry-create-123",
        )
        .body("{\"model\":\"sora-2\",\"prompt\":\"hello\"}")
        .send()
        .await
        .expect("create request should succeed");
    assert_eq!(create_response.status(), StatusCode::OK);
    let create_json: serde_json::Value = create_response.json().await.expect("body should parse");
    let local_task_id = create_json
        .get("id")
        .and_then(|value| value.as_str())
        .expect("response id should exist")
        .to_string();

    let get_before_delete_response = reqwest::Client::new()
        .get(format!("{gateway_url}/v1/videos/{local_task_id}"))
        .header(
            TRACE_ID_HEADER,
            "trace-openai-video-delete-registry-refresh-123",
        )
        .send()
        .await
        .expect("refresh get request should succeed");
    assert_eq!(get_before_delete_response.status(), StatusCode::OK);
    assert_eq!(
        get_before_delete_response
            .json::<serde_json::Value>()
            .await
            .expect("body should parse")
            .get("status"),
        Some(&json!("completed"))
    );

    let delete_response = reqwest::Client::new()
        .delete(format!("{gateway_url}/v1/videos/{local_task_id}"))
        .header(
            TRACE_ID_HEADER,
            "trace-openai-video-delete-registry-delete-123",
        )
        .send()
        .await
        .expect("delete request should succeed");
    assert_eq!(delete_response.status(), StatusCode::OK);
    assert_eq!(
        delete_response
            .json::<serde_json::Value>()
            .await
            .expect("body should parse"),
        json!({
            "id": local_task_id,
            "object": "video",
            "deleted": true
        })
    );

    let get_response = reqwest::Client::new()
        .get(format!("{gateway_url}/v1/videos/{local_task_id}"))
        .header(
            TRACE_ID_HEADER,
            "trace-openai-video-delete-registry-get-123",
        )
        .send()
        .await
        .expect("get request should succeed");
    assert_eq!(get_response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        get_response
            .json::<serde_json::Value>()
            .await
            .expect("body should parse"),
        json!({"detail": "Video task not found"})
    );
    assert_eq!(
        *seen_decision_paths.lock().expect("mutex should lock"),
        vec!["/v1/videos".to_string()]
    );
    assert_eq!(
        *seen_executor_requests.lock().expect("mutex should lock"),
        vec![
            (
                "POST".to_string(),
                "https://api.openai.example/v1/videos".to_string(),
            ),
            (
                "GET".to_string(),
                "https://api.openai.example/v1/videos/ext-video-task-123".to_string(),
            ),
            (
                "DELETE".to_string(),
                "https://api.openai.example/v1/videos/ext-video-task-123".to_string(),
            ),
        ]
    );
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_background_video_task_poller_refreshes_openai_task_before_local_delete() {
    let seen_decision_paths = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen_decision_paths_clone = Arc::clone(&seen_decision_paths);
    let seen_executor_requests = Arc::new(Mutex::new(Vec::<(String, String)>::new()));
    let seen_executor_requests_clone = Arc::clone(&seen_executor_requests);
    let execute_hits = Arc::new(Mutex::new(0usize));
    let execute_hits_clone = Arc::clone(&execute_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/resolve",
            any(|request: Request| async move {
                Json(json!({
                    "action": "proxy_public",
                    "route_class": "ai_public",
                    "route_family": "openai",
                    "route_kind": "video",
                    "auth_endpoint_signature": "openai:video",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-video-poller-123",
                        "api_key_id": "key-video-poller-123",
                        "access_allowed": true
                    },
                    "public_path": request.uri().path().to_string()
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-sync",
            any(move |request: Request| {
                let seen_decision_paths_inner = Arc::clone(&seen_decision_paths_clone);
                async move {
                    let (_parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("decision payload should parse");
                    seen_decision_paths_inner
                        .lock()
                        .expect("mutex should lock")
                        .push(
                            payload
                                .get("path")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        );
                    Json(json!({
                        "action": "executor_sync_decision",
                        "decision_kind": "openai_video_create_sync",
                        "request_id": "req-openai-video-poller-create-123",
                        "provider_name": "openai",
                        "provider_id": "provider-openai-video-poller-123",
                        "endpoint_id": "endpoint-openai-video-poller-123",
                        "key_id": "key-openai-video-poller-123",
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
                            "user_id": "user-video-poller-123",
                            "api_key_id": "key-video-poller-123",
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
            any(|_request: Request| async move { Json(json!({"ok": true})) }),
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
        )
        .route(
            "/{*path}",
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
            let seen_executor_requests_inner = Arc::clone(&seen_executor_requests_clone);
            async move {
                let (_parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value =
                    serde_json::from_slice(&raw_body).expect("executor payload should parse");
                let method = payload
                    .get("method")
                    .and_then(|value| value.as_str())
                    .unwrap_or_default()
                    .to_string();
                let url = payload
                    .get("url")
                    .and_then(|value| value.as_str())
                    .unwrap_or_default()
                    .to_string();
                seen_executor_requests_inner
                    .lock()
                    .expect("mutex should lock")
                    .push((method.clone(), url.clone()));
                if method == "POST" && url.ends_with("/v1/videos") {
                    return Json(json!({
                        "request_id": "req-openai-video-poller-create-123",
                        "status_code": 200,
                        "headers": {
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "id": "ext-video-task-123",
                                "status": "submitted"
                            }
                        }
                    }));
                }
                if method == "GET" && url.ends_with("/v1/videos/ext-video-task-123") {
                    return Json(json!({
                        "request_id": "req-openai-video-poller-refresh-123",
                        "status_code": 200,
                        "headers": {
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "id": "ext-video-task-123",
                                "status": "completed",
                                "progress": 100,
                                "completed_at": 1712345688u64
                            }
                        }
                    }));
                }

                Json(json!({
                    "request_id": "req-openai-video-poller-delete-123",
                    "status_code": 404,
                    "headers": {
                        "content-type": "application/json"
                    }
                }))
            }
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
    .with_video_task_truth_source_mode(VideoTaskTruthSourceMode::RustAuthoritative)
    .with_video_task_poller_config(std::time::Duration::from_millis(25), 8);
    let background_tasks = gateway_state.spawn_background_tasks();
    let gateway = build_router_with_state(gateway_state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let create_response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/videos"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-openai-video-poller-create-123")
        .body("{\"model\":\"sora-2\",\"prompt\":\"hello\"}")
        .send()
        .await
        .expect("create request should succeed");
    assert_eq!(create_response.status(), StatusCode::OK);
    let create_json: serde_json::Value = create_response.json().await.expect("body should parse");
    let local_task_id = create_json
        .get("id")
        .and_then(|value| value.as_str())
        .expect("response id should exist")
        .to_string();

    wait_until(500, || {
        seen_executor_requests
            .lock()
            .expect("mutex should lock")
            .iter()
            .any(|(method, url)| {
                method == "GET" && url == "https://api.openai.example/v1/videos/ext-video-task-123"
            })
    })
    .await;

    let delete_response = reqwest::Client::new()
        .delete(format!("{gateway_url}/v1/videos/{local_task_id}"))
        .header(TRACE_ID_HEADER, "trace-openai-video-poller-delete-123")
        .send()
        .await
        .expect("delete request should succeed");
    assert_eq!(delete_response.status(), StatusCode::OK);
    assert_eq!(
        delete_response
            .json::<serde_json::Value>()
            .await
            .expect("body should parse"),
        json!({
            "id": local_task_id,
            "object": "video",
            "deleted": true
        })
    );

    assert_eq!(
        *seen_decision_paths.lock().expect("mutex should lock"),
        vec!["/v1/videos".to_string()]
    );
    let executor_requests = seen_executor_requests
        .lock()
        .expect("mutex should lock")
        .clone();
    assert!(executor_requests.contains(&(
        "POST".to_string(),
        "https://api.openai.example/v1/videos".to_string()
    )));
    assert!(executor_requests.contains(&(
        "GET".to_string(),
        "https://api.openai.example/v1/videos/ext-video-task-123".to_string()
    )));
    assert!(executor_requests.contains(&(
        "DELETE".to_string(),
        "https://api.openai.example/v1/videos/ext-video-task-123".to_string()
    )));
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    for handle in background_tasks {
        handle.abort();
    }
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_reads_cancelled_gemini_video_task_from_local_registry_when_rust_owns_truth_source()
{
    let seen_decision_paths = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen_decision_paths_clone = Arc::clone(&seen_decision_paths);
    let seen_executor_requests = Arc::new(Mutex::new(Vec::<(String, String)>::new()));
    let seen_executor_requests_clone = Arc::clone(&seen_executor_requests);
    let execute_hits = Arc::new(Mutex::new(0usize));
    let execute_hits_clone = Arc::clone(&execute_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/resolve",
            any(|request: Request| async move {
                Json(json!({
                    "action": "proxy_public",
                    "route_class": "ai_public",
                    "route_family": "gemini",
                    "route_kind": "video",
                    "auth_endpoint_signature": "gemini:video",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-gemini-video-cancel-registry-123",
                        "api_key_id": "key-gemini-video-cancel-registry-123",
                        "access_allowed": true
                    },
                    "public_path": request.uri().path().to_string()
                }))
            }),
        )
        .route(
            "/api/internal/gateway/decision-sync",
            any(move |request: Request| {
                let seen_decision_paths_inner = Arc::clone(&seen_decision_paths_clone);
                async move {
                let (_parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value =
                    serde_json::from_slice(&raw_body).expect("decision payload should parse");
                let path = payload
                    .get("path")
                    .and_then(|value| value.as_str())
                    .unwrap_or_default()
                    .to_string();
                seen_decision_paths_inner
                    .lock()
                    .expect("mutex should lock")
                    .push(path.clone());
                if path == "/v1beta/models/veo-3:predictLongRunning" {
                    return Json(json!({
                        "action": "executor_sync_decision",
                        "decision_kind": "gemini_video_create_sync",
                        "request_id": "req-gemini-video-cancel-create-123",
                        "provider_name": "gemini",
                        "provider_id": "provider-gemini-video-cancel-create-123",
                        "endpoint_id": "endpoint-gemini-video-cancel-create-123",
                        "key_id": "key-gemini-video-cancel-create-123",
                        "upstream_base_url": "https://generativelanguage.googleapis.com",
                        "upstream_url": "https://generativelanguage.googleapis.com/v1beta/models/veo-3:predictLongRunning",
                        "provider_request_method": "POST",
                        "auth_header": "",
                        "auth_value": "",
                        "provider_request_headers": {
                            "x-goog-api-key": "upstream-key",
                            "content-type": "application/json"
                        },
                        "provider_request_body": {
                            "prompt": "make a video"
                        },
                        "content_type": "application/json",
                        "client_api_format": "gemini:video",
                        "provider_api_format": "gemini:video",
                        "model_name": "veo-3",
                        "report_kind": "gemini_video_create_sync_finalize",
                        "report_context": {
                            "user_id": "user-gemini-video-cancel-registry-123",
                            "api_key_id": "key-gemini-video-cancel-registry-123",
                            "model": "veo-3"
                        }
                    }));
                }

                let operation_name = path.trim_start_matches('/');
                Json(json!({
                    "action": "executor_sync_decision",
                    "decision_kind": "gemini_video_cancel_sync",
                    "request_id": "req-gemini-video-cancel-cancel-123",
                    "provider_name": "gemini",
                    "provider_id": "provider-gemini-video-cancel-cancel-123",
                    "endpoint_id": "endpoint-gemini-video-cancel-cancel-123",
                    "key_id": "key-gemini-video-cancel-cancel-123",
                    "upstream_base_url": "https://generativelanguage.googleapis.com",
                    "upstream_url": "https://generativelanguage.googleapis.com/v1beta/models/veo-3/operations/ext-video-123:cancel",
                    "provider_request_method": "POST",
                    "auth_header": "",
                    "auth_value": "",
                    "provider_request_headers": {
                        "x-goog-api-key": "upstream-key",
                        "content-type": "application/json"
                    },
                    "provider_request_body": {},
                    "content_type": "application/json",
                    "client_api_format": "gemini:video",
                    "provider_api_format": "gemini:video",
                    "model_name": "veo-3",
                    "report_kind": "gemini_video_cancel_sync_finalize",
                    "report_context": {
                        "user_id": "user-gemini-video-cancel-registry-123",
                        "api_key_id": "key-gemini-video-cancel-registry-123",
                        "task_id": operation_name
                    }
                }))
                }
            }),
        )
        .route(
            "/api/internal/gateway/report-sync",
            any(|_request: Request| async move { Json(json!({"ok": true})) }),
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
        )
        .route(
            "/{*path}",
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
            let seen_executor_requests_inner = Arc::clone(&seen_executor_requests_clone);
            async move {
                let (_parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value =
                    serde_json::from_slice(&raw_body).expect("executor payload should parse");
                seen_executor_requests_inner
                    .lock()
                    .expect("mutex should lock")
                    .push((
                        payload
                            .get("method")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        payload
                            .get("url")
                            .and_then(|value| value.as_str())
                            .unwrap_or_default()
                            .to_string(),
                    ));
                let url = payload
                    .get("url")
                    .and_then(|value| value.as_str())
                    .unwrap_or_default();
                if url.ends_with(":predictLongRunning") {
                    return Json(json!({
                        "request_id": "req-gemini-video-cancel-create-123",
                        "status_code": 200,
                        "headers": {
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "name": "operations/ext-video-123"
                            }
                        }
                    }));
                }

                Json(json!({
                    "request_id": "req-gemini-video-cancel-cancel-123",
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
    let gateway_state = AppState::new_with_executor(
        upstream_url.clone(),
        Some(upstream_url.clone()),
        Some(executor_url),
    )
    .expect("gateway state should build")
    .with_video_task_truth_source_mode(VideoTaskTruthSourceMode::RustAuthoritative);
    let gateway = build_router_with_state(gateway_state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let create_response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/v1beta/models/veo-3:predictLongRunning"
        ))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header("x-goog-api-key", "client-key")
        .header(
            TRACE_ID_HEADER,
            "trace-gemini-video-cancel-registry-create-123",
        )
        .body("{\"prompt\":\"make a video\"}")
        .send()
        .await
        .expect("create request should succeed");
    assert_eq!(create_response.status(), StatusCode::OK);
    let create_json: serde_json::Value = create_response.json().await.expect("body should parse");
    let operation_name = create_json
        .get("name")
        .and_then(|value| value.as_str())
        .expect("operation name should exist")
        .to_string();

    let cancel_response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1beta/{operation_name}:cancel"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header("x-goog-api-key", "client-key")
        .header(
            TRACE_ID_HEADER,
            "trace-gemini-video-cancel-registry-cancel-123",
        )
        .body("{}")
        .send()
        .await
        .expect("cancel request should succeed");
    assert_eq!(cancel_response.status(), StatusCode::OK);
    assert_eq!(
        cancel_response
            .json::<serde_json::Value>()
            .await
            .expect("body should parse"),
        json!({})
    );

    let get_response = reqwest::Client::new()
        .get(format!("{gateway_url}/v1beta/{operation_name}"))
        .header("x-goog-api-key", "client-key")
        .header(
            TRACE_ID_HEADER,
            "trace-gemini-video-cancel-registry-get-123",
        )
        .send()
        .await
        .expect("get request should succeed");
    assert_eq!(get_response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        get_response
            .json::<serde_json::Value>()
            .await
            .expect("body should parse"),
        json!({"detail": "Video task was cancelled"})
    );
    assert_eq!(
        *seen_decision_paths.lock().expect("mutex should lock"),
        vec!["/v1beta/models/veo-3:predictLongRunning".to_string()]
    );
    assert_eq!(
        *seen_executor_requests.lock().expect("mutex should lock"),
        vec![
            (
                "POST".to_string(),
                "https://generativelanguage.googleapis.com/v1beta/models/veo-3:predictLongRunning"
                    .to_string(),
            ),
            (
                "POST".to_string(),
                "https://generativelanguage.googleapis.com/v1beta/models/veo-3/operations/ext-video-123:cancel"
                    .to_string(),
            ),
        ]
    );
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
