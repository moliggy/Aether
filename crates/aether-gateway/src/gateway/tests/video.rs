use aether_contracts::{StreamFrame, StreamFramePayload, StreamFrameType};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};

use super::*;

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
async fn gateway_executes_openai_video_content_via_executor_stream_plan() {
    #[derive(Debug, Clone)]
    struct SeenPlanStreamRequest {
        trace_id: String,
        path: String,
        user_id: String,
    }

    #[derive(Debug, Clone)]
    struct SeenExecutorStreamRequest {
        method: String,
        url: String,
        provider_api_format: String,
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
                    "route_family": "openai",
                    "route_kind": "video",
                    "auth_endpoint_signature": "openai:video",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-video-direct-123",
                        "api_key_id": "key-video-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/videos/task-123/content"
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
                            path: payload
                                .get("path")
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
                        "plan_kind": "openai_video_content",
                        "plan": {
                            "request_id": "req-video-direct-123",
                            "provider_id": "provider-video-direct-123",
                            "endpoint_id": "endpoint-video-direct-123",
                            "key_id": "key-video-direct-123",
                            "provider_name": "openai",
                            "method": "GET",
                            "url": "https://cdn.example.com/video.mp4",
                            "headers": {},
                            "body": {},
                            "stream": true,
                            "client_api_format": "openai:video",
                            "provider_api_format": "openai:video",
                            "model_name": "sora-2"
                        }
                    }))
                }
            }),
        )
        .route(
            "/v1/videos/task-123/content",
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
                let (_parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value =
                    serde_json::from_slice(&raw_body).expect("executor payload should parse");
                *seen_executor_inner.lock().expect("mutex should lock") =
                    Some(SeenExecutorStreamRequest {
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

                let frames = [
                    StreamFrame {
                        frame_type: StreamFrameType::Headers,
                        payload: StreamFramePayload::Headers {
                            status_code: 200,
                            headers: std::collections::BTreeMap::from([(
                                "content-type".to_string(),
                                "video/mp4".to_string(),
                            )]),
                        },
                    },
                    StreamFrame {
                        frame_type: StreamFrameType::Data,
                        payload: StreamFramePayload::Data {
                            chunk_b64: Some(BASE64_STANDARD.encode(b"openai-")),
                            text: None,
                        },
                    },
                    StreamFrame {
                        frame_type: StreamFrameType::Data,
                        payload: StreamFramePayload::Data {
                            chunk_b64: Some(BASE64_STANDARD.encode(b"video")),
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
            "{gateway_url}/v1/videos/task-123/content?variant=video"
        ))
        .header(TRACE_ID_HEADER, "trace-video-direct-123")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("video/mp4")
    );
    assert_eq!(
        response.bytes().await.expect("body should read"),
        Bytes::from_static(b"openai-video")
    );

    let seen_plan_request = seen_plan
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("plan-stream should be captured");
    assert_eq!(seen_plan_request.trace_id, "trace-video-direct-123");
    assert_eq!(seen_plan_request.path, "/v1/videos/task-123/content");
    assert_eq!(seen_plan_request.user_id, "user-video-direct-123");

    let seen_executor_request = seen_executor
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("executor stream should be captured");
    assert_eq!(seen_executor_request.method, "GET");
    assert_eq!(
        seen_executor_request.url,
        "https://cdn.example.com/video.mp4"
    );
    assert_eq!(seen_executor_request.provider_api_format, "openai:video");
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

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
    struct SeenFinalizeSyncRequest {
        trace_id: String,
        report_kind: String,
        status_code: u64,
        upstream_id: String,
    }

    let seen_plan = Arc::new(Mutex::new(None::<SeenPlanSyncRequest>));
    let seen_plan_clone = Arc::clone(&seen_plan);
    let seen_executor = Arc::new(Mutex::new(None::<SeenExecutorSyncRequest>));
    let seen_executor_clone = Arc::clone(&seen_executor);
    let seen_finalize = Arc::new(Mutex::new(None::<SeenFinalizeSyncRequest>));
    let seen_finalize_clone = Arc::clone(&seen_finalize);
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
                        });
                    Json(json!({
                        "action": "executor_sync",
                        "plan_kind": "openai_video_create_sync",
                        "plan": {
                            "request_id": "req-openai-video-create-direct-123",
                            "provider_name": "openai",
                            "provider_id": "provider-openai-video-create-direct-123",
                            "endpoint_id": "endpoint-openai-video-create-direct-123",
                            "key_id": "key-openai-video-create-direct-123",
                            "method": "POST",
                            "url": "https://api.openai.example/v1/videos",
                            "headers": {
                                "authorization": "Bearer upstream-key",
                                "content-type": "application/json"
                            },
                            "body": {
                                "json_body": {
                                    "model": "sora-2",
                                    "prompt": "hello"
                                }
                            },
                            "stream": false,
                            "client_api_format": "openai:video",
                            "provider_api_format": "openai:video",
                            "model_name": "sora-2"
                        },
                        "report_kind": "openai_video_create_sync_finalize",
                        "report_context": {
                            "user_id": "user-video-create-direct-123",
                            "api_key_id": "key-video-create-direct-123"
                        }
                    }))
                }
            }),
        )
        .route(
            "/api/internal/gateway/finalize-sync",
            any(move |request: Request| {
                let seen_finalize_inner = Arc::clone(&seen_finalize_clone);
                async move {
                    let (parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value = serde_json::from_slice(&raw_body)
                        .expect("finalize payload should parse");
                    *seen_finalize_inner.lock().expect("mutex should lock") =
                        Some(SeenFinalizeSyncRequest {
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
                        });
                    let mut response = Response::builder()
                        .status(StatusCode::OK)
                        .body(Body::from(
                            "{\"id\":\"vid_local_123\",\"object\":\"video\",\"status\":\"submitted\"}",
                        ))
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

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "id": "vid_local_123",
            "object": "video",
            "status": "submitted"
        })
    );

    let seen_plan_request = seen_plan
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("plan-sync should be captured");
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

    let seen_finalize_request = seen_finalize
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("finalize-sync should be captured");
    assert_eq!(
        seen_finalize_request.trace_id,
        "trace-openai-video-create-direct-123"
    );
    assert_eq!(
        seen_finalize_request.report_kind,
        "openai_video_create_sync_finalize"
    );
    assert_eq!(seen_finalize_request.status_code, 200);
    assert_eq!(seen_finalize_request.upstream_id, "ext-video-task-123");

    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);
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
    struct SeenFinalizeSyncRequest {
        trace_id: String,
        report_kind: String,
        status_code: u64,
        upstream_id: String,
    }

    let seen_plan = Arc::new(Mutex::new(None::<SeenPlanSyncRequest>));
    let seen_plan_clone = Arc::clone(&seen_plan);
    let seen_executor = Arc::new(Mutex::new(None::<SeenExecutorSyncRequest>));
    let seen_executor_clone = Arc::clone(&seen_executor);
    let seen_finalize = Arc::new(Mutex::new(None::<SeenFinalizeSyncRequest>));
    let seen_finalize_clone = Arc::clone(&seen_finalize);
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
                            prompt: payload
                                .get("body_json")
                                .and_then(|value| value.get("prompt"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        });
                    Json(json!({
                        "action": "executor_sync",
                        "plan_kind": "openai_video_remix_sync",
                        "plan": {
                            "request_id": "req-openai-video-remix-direct-123",
                            "provider_name": "openai",
                            "provider_id": "provider-openai-video-remix-direct-123",
                            "endpoint_id": "endpoint-openai-video-remix-direct-123",
                            "key_id": "key-openai-video-remix-direct-123",
                            "method": "POST",
                            "url": "https://api.openai.example/v1/videos/ext-123/remix",
                            "headers": {
                                "authorization": "Bearer upstream-key",
                                "content-type": "application/json"
                            },
                            "body": {
                                "json_body": {
                                    "prompt": "remix this"
                                }
                            },
                            "stream": false,
                            "client_api_format": "openai:video",
                            "provider_api_format": "openai:video",
                            "model_name": "sora-2"
                        },
                        "report_kind": "openai_video_remix_sync_finalize",
                        "report_context": {
                            "user_id": "user-video-remix-direct-123",
                            "api_key_id": "key-video-remix-direct-123",
                            "task_id": "task-123"
                        }
                    }))
                }
            }),
        )
        .route(
            "/api/internal/gateway/finalize-sync",
            any(move |request: Request| {
                let seen_finalize_inner = Arc::clone(&seen_finalize_clone);
                async move {
                    let (parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value = serde_json::from_slice(&raw_body)
                        .expect("finalize payload should parse");
                    *seen_finalize_inner.lock().expect("mutex should lock") =
                        Some(SeenFinalizeSyncRequest {
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
                        });
                    let mut response = Response::builder()
                        .status(StatusCode::OK)
                        .body(Body::from(
                            "{\"id\":\"vid_local_remix_123\",\"object\":\"video\",\"status\":\"submitted\"}",
                        ))
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
    assert_eq!(
        response_json,
        json!({
            "id": "vid_local_remix_123",
            "object": "video",
            "status": "submitted"
        })
    );

    let seen_plan_request = seen_plan
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("plan-sync should be captured");
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

    let seen_finalize_request = seen_finalize
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("finalize-sync should be captured");
    assert_eq!(
        seen_finalize_request.trace_id,
        "trace-openai-video-remix-direct-123"
    );
    assert_eq!(
        seen_finalize_request.report_kind,
        "openai_video_remix_sync_finalize"
    );
    assert_eq!(seen_finalize_request.status_code, 200);
    assert_eq!(seen_finalize_request.upstream_id, "ext-remix-task-123");

    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_gemini_video_create_via_executor_sync_finalize() {
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
    struct SeenFinalizeSyncRequest {
        trace_id: String,
        report_kind: String,
        status_code: u64,
        upstream_name: String,
    }

    let seen_plan = Arc::new(Mutex::new(None::<SeenPlanSyncRequest>));
    let seen_plan_clone = Arc::clone(&seen_plan);
    let seen_executor = Arc::new(Mutex::new(None::<SeenExecutorSyncRequest>));
    let seen_executor_clone = Arc::clone(&seen_executor);
    let seen_finalize = Arc::new(Mutex::new(None::<SeenFinalizeSyncRequest>));
    let seen_finalize_clone = Arc::clone(&seen_finalize);
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
                    "route_family": "gemini",
                    "route_kind": "video",
                    "auth_endpoint_signature": "gemini:video",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-gemini-video-create-direct-123",
                        "api_key_id": "key-gemini-video-create-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1beta/models/veo-3:predictLongRunning"
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
                        "action": "executor_sync",
                        "plan_kind": "gemini_video_create_sync",
                        "plan": {
                            "request_id": "req-gemini-video-create-direct-123",
                            "provider_name": "gemini",
                            "provider_id": "provider-gemini-video-create-direct-123",
                            "endpoint_id": "endpoint-gemini-video-create-direct-123",
                            "key_id": "key-gemini-video-create-direct-123",
                            "method": "POST",
                            "url": "https://generativelanguage.googleapis.com/v1beta/models/veo-3:predictLongRunning",
                            "headers": {
                                "x-goog-api-key": "upstream-key",
                                "content-type": "application/json"
                            },
                            "body": {
                                "json_body": {
                                    "prompt": "make a video"
                                }
                            },
                            "stream": false,
                            "client_api_format": "gemini:video",
                            "provider_api_format": "gemini:video",
                            "model_name": "veo-3"
                        },
                        "report_kind": "gemini_video_create_sync_finalize",
                        "report_context": {
                            "user_id": "user-gemini-video-create-direct-123",
                            "api_key_id": "key-gemini-video-create-direct-123",
                            "model": "veo-3"
                        }
                    }))
                }
            }),
        )
        .route(
            "/api/internal/gateway/finalize-sync",
            any(move |request: Request| {
                let seen_finalize_inner = Arc::clone(&seen_finalize_clone);
                async move {
                    let (parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value = serde_json::from_slice(&raw_body)
                        .expect("finalize payload should parse");
                    *seen_finalize_inner.lock().expect("mutex should lock") =
                        Some(SeenFinalizeSyncRequest {
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
                            upstream_name: payload
                                .get("body_json")
                                .and_then(|value| value.get("name"))
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        });
                    let mut response = Response::builder()
                        .status(StatusCode::OK)
                        .body(Body::from(
                            "{\"name\":\"models/veo-3/operations/aev_123\",\"done\":false,\"metadata\":{}}",
                        ))
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
            "/v1beta/models/veo-3:predictLongRunning",
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
                    "request_id": "req-gemini-video-create-direct-123",
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
                        "elapsed_ms": 58
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
            "{gateway_url}/v1beta/models/veo-3:predictLongRunning"
        ))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header("x-goog-api-key", "client-key")
        .header(TRACE_ID_HEADER, "trace-gemini-video-create-direct-123")
        .body("{\"prompt\":\"make a video\"}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "name": "models/veo-3/operations/aev_123",
            "done": false,
            "metadata": {}
        })
    );

    let seen_plan_request = seen_plan
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("plan-sync should be captured");
    assert_eq!(
        seen_plan_request.trace_id,
        "trace-gemini-video-create-direct-123"
    );
    assert_eq!(
        seen_plan_request.path,
        "/v1beta/models/veo-3:predictLongRunning"
    );
    assert_eq!(seen_plan_request.prompt, "make a video");

    let seen_executor_request = seen_executor
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("executor sync should be captured");
    assert_eq!(seen_executor_request.method, "POST");
    assert_eq!(
        seen_executor_request.url,
        "https://generativelanguage.googleapis.com/v1beta/models/veo-3:predictLongRunning"
    );
    assert_eq!(seen_executor_request.provider_api_format, "gemini:video");

    let seen_finalize_request = seen_finalize
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("finalize-sync should be captured");
    assert_eq!(
        seen_finalize_request.trace_id,
        "trace-gemini-video-create-direct-123"
    );
    assert_eq!(
        seen_finalize_request.report_kind,
        "gemini_video_create_sync_finalize"
    );
    assert_eq!(seen_finalize_request.status_code, 200);
    assert_eq!(
        seen_finalize_request.upstream_name,
        "operations/ext-video-123"
    );

    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_openai_video_delete_via_executor_sync_finalize() {
    #[derive(Debug, Clone)]
    struct SeenPlanSyncRequest {
        trace_id: String,
        path: String,
    }

    #[derive(Debug, Clone)]
    struct SeenExecutorSyncRequest {
        method: String,
        url: String,
        provider_api_format: String,
    }

    #[derive(Debug, Clone)]
    struct SeenFinalizeSyncRequest {
        trace_id: String,
        report_kind: String,
        status_code: u64,
    }

    let seen_plan = Arc::new(Mutex::new(None::<SeenPlanSyncRequest>));
    let seen_plan_clone = Arc::clone(&seen_plan);
    let seen_executor = Arc::new(Mutex::new(None::<SeenExecutorSyncRequest>));
    let seen_executor_clone = Arc::clone(&seen_executor);
    let seen_finalize = Arc::new(Mutex::new(None::<SeenFinalizeSyncRequest>));
    let seen_finalize_clone = Arc::clone(&seen_finalize);
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
                        "user_id": "user-openai-video-delete-direct-123",
                        "api_key_id": "key-openai-video-delete-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/videos/task-123"
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
                            path: payload
                                .get("path")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        });
                    Json(json!({
                        "action": "executor_sync",
                        "plan_kind": "openai_video_delete_sync",
                        "plan": {
                            "request_id": "req-openai-video-delete-direct-123",
                            "provider_name": "openai",
                            "provider_id": "provider-openai-video-delete-direct-123",
                            "endpoint_id": "endpoint-openai-video-delete-direct-123",
                            "key_id": "key-openai-video-delete-direct-123",
                            "method": "DELETE",
                            "url": "https://api.openai.example/v1/videos/ext-123",
                            "headers": {
                                "authorization": "Bearer upstream-key"
                            },
                            "body": {},
                            "stream": false,
                            "client_api_format": "openai:video",
                            "provider_api_format": "openai:video",
                            "model_name": "sora-2"
                        },
                        "report_kind": "openai_video_delete_sync_finalize",
                        "report_context": {
                            "user_id": "user-openai-video-delete-direct-123",
                            "api_key_id": "key-openai-video-delete-direct-123",
                            "task_id": "task-123"
                        }
                    }))
                }
            }),
        )
        .route(
            "/api/internal/gateway/finalize-sync",
            any(move |request: Request| {
                let seen_finalize_inner = Arc::clone(&seen_finalize_clone);
                async move {
                    let (parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("finalize payload should parse");
                    *seen_finalize_inner.lock().expect("mutex should lock") =
                        Some(SeenFinalizeSyncRequest {
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
                    let mut response = Response::builder()
                        .status(StatusCode::OK)
                        .body(Body::from(
                            "{\"id\":\"task-123\",\"object\":\"video\",\"deleted\":true}",
                        ))
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
            "/v1/videos/task-123",
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
                    "request_id": "req-openai-video-delete-direct-123",
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
    let gateway =
        build_router_with_endpoints(upstream_url.clone(), Some(upstream_url), Some(executor_url))
            .expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .delete(format!("{gateway_url}/v1/videos/task-123"))
        .header(TRACE_ID_HEADER, "trace-openai-video-delete-direct-123")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(
        response_json,
        json!({
            "id": "task-123",
            "object": "video",
            "deleted": true
        })
    );

    let seen_plan_request = seen_plan
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("plan-sync should be captured");
    assert_eq!(
        seen_plan_request.trace_id,
        "trace-openai-video-delete-direct-123"
    );
    assert_eq!(seen_plan_request.path, "/v1/videos/task-123");

    let seen_executor_request = seen_executor
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("executor sync should be captured");
    assert_eq!(seen_executor_request.method, "DELETE");
    assert_eq!(
        seen_executor_request.url,
        "https://api.openai.example/v1/videos/ext-123"
    );
    assert_eq!(seen_executor_request.provider_api_format, "openai:video");

    let seen_finalize_request = seen_finalize
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("finalize-sync should be captured");
    assert_eq!(
        seen_finalize_request.trace_id,
        "trace-openai-video-delete-direct-123"
    );
    assert_eq!(
        seen_finalize_request.report_kind,
        "openai_video_delete_sync_finalize"
    );
    assert_eq!(seen_finalize_request.status_code, 404);

    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_openai_video_cancel_via_executor_sync_finalize() {
    #[derive(Debug, Clone)]
    struct SeenPlanSyncRequest {
        trace_id: String,
        path: String,
    }

    #[derive(Debug, Clone)]
    struct SeenExecutorSyncRequest {
        method: String,
        url: String,
        provider_api_format: String,
    }

    #[derive(Debug, Clone)]
    struct SeenFinalizeSyncRequest {
        trace_id: String,
        report_kind: String,
        status_code: u64,
    }

    let seen_plan = Arc::new(Mutex::new(None::<SeenPlanSyncRequest>));
    let seen_plan_clone = Arc::clone(&seen_plan);
    let seen_executor = Arc::new(Mutex::new(None::<SeenExecutorSyncRequest>));
    let seen_executor_clone = Arc::clone(&seen_executor);
    let seen_finalize = Arc::new(Mutex::new(None::<SeenFinalizeSyncRequest>));
    let seen_finalize_clone = Arc::clone(&seen_finalize);
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
                        "user_id": "user-openai-video-cancel-direct-123",
                        "api_key_id": "key-openai-video-cancel-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1/videos/task-123/cancel"
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
                            path: payload
                                .get("path")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        });
                    Json(json!({
                        "action": "executor_sync",
                        "plan_kind": "openai_video_cancel_sync",
                        "plan": {
                            "request_id": "req-openai-video-cancel-direct-123",
                            "provider_name": "openai",
                            "provider_id": "provider-openai-video-cancel-direct-123",
                            "endpoint_id": "endpoint-openai-video-cancel-direct-123",
                            "key_id": "key-openai-video-cancel-direct-123",
                            "method": "DELETE",
                            "url": "https://api.openai.example/v1/videos/ext-123",
                            "headers": {
                                "authorization": "Bearer upstream-key"
                            },
                            "body": {},
                            "stream": false,
                            "client_api_format": "openai:video",
                            "provider_api_format": "openai:video",
                            "model_name": "sora-2"
                        },
                        "report_kind": "openai_video_cancel_sync_finalize",
                        "report_context": {
                            "user_id": "user-openai-video-cancel-direct-123",
                            "api_key_id": "key-openai-video-cancel-direct-123",
                            "task_id": "task-123"
                        }
                    }))
                }
            }),
        )
        .route(
            "/api/internal/gateway/finalize-sync",
            any(move |request: Request| {
                let seen_finalize_inner = Arc::clone(&seen_finalize_clone);
                async move {
                    let (parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("finalize payload should parse");
                    *seen_finalize_inner.lock().expect("mutex should lock") =
                        Some(SeenFinalizeSyncRequest {
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
                    let mut response = Response::builder()
                        .status(StatusCode::OK)
                        .body(Body::from("{}"))
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
            "/v1/videos/task-123/cancel",
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
                    "request_id": "req-openai-video-cancel-direct-123",
                    "status_code": 204,
                    "headers": {},
                    "telemetry": {
                        "elapsed_ms": 33
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
        .post(format!("{gateway_url}/v1/videos/task-123/cancel"))
        .header("authorization", "Bearer client-key")
        .header(TRACE_ID_HEADER, "trace-openai-video-cancel-direct-123")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(response_json, json!({}));

    let seen_plan_request = seen_plan
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("plan-sync should be captured");
    assert_eq!(
        seen_plan_request.trace_id,
        "trace-openai-video-cancel-direct-123"
    );
    assert_eq!(seen_plan_request.path, "/v1/videos/task-123/cancel");

    let seen_executor_request = seen_executor
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("executor sync should be captured");
    assert_eq!(seen_executor_request.method, "DELETE");
    assert_eq!(
        seen_executor_request.url,
        "https://api.openai.example/v1/videos/ext-123"
    );
    assert_eq!(seen_executor_request.provider_api_format, "openai:video");

    let seen_finalize_request = seen_finalize
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("finalize-sync should be captured");
    assert_eq!(
        seen_finalize_request.trace_id,
        "trace-openai-video-cancel-direct-123"
    );
    assert_eq!(
        seen_finalize_request.report_kind,
        "openai_video_cancel_sync_finalize"
    );
    assert_eq!(seen_finalize_request.status_code, 204);

    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_executes_gemini_video_cancel_via_executor_sync_finalize() {
    #[derive(Debug, Clone)]
    struct SeenPlanSyncRequest {
        trace_id: String,
        path: String,
    }

    #[derive(Debug, Clone)]
    struct SeenExecutorSyncRequest {
        method: String,
        url: String,
        provider_api_format: String,
    }

    #[derive(Debug, Clone)]
    struct SeenFinalizeSyncRequest {
        trace_id: String,
        report_kind: String,
        status_code: u64,
    }

    let seen_plan = Arc::new(Mutex::new(None::<SeenPlanSyncRequest>));
    let seen_plan_clone = Arc::clone(&seen_plan);
    let seen_executor = Arc::new(Mutex::new(None::<SeenExecutorSyncRequest>));
    let seen_executor_clone = Arc::clone(&seen_executor);
    let seen_finalize = Arc::new(Mutex::new(None::<SeenFinalizeSyncRequest>));
    let seen_finalize_clone = Arc::clone(&seen_finalize);
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
                    "route_family": "gemini",
                    "route_kind": "video",
                    "auth_endpoint_signature": "gemini:video",
                    "executor_candidate": true,
                    "auth_context": {
                        "user_id": "user-gemini-video-cancel-direct-123",
                        "api_key_id": "key-gemini-video-cancel-direct-123",
                        "access_allowed": true
                    },
                    "public_path": "/v1beta/models/veo-3/operations/ext-123:cancel"
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
                            path: payload
                                .get("path")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        });
                    Json(json!({
                        "action": "executor_sync",
                        "plan_kind": "gemini_video_cancel_sync",
                        "plan": {
                            "request_id": "req-gemini-video-cancel-direct-123",
                            "provider_name": "gemini",
                            "provider_id": "provider-gemini-video-cancel-direct-123",
                            "endpoint_id": "endpoint-gemini-video-cancel-direct-123",
                            "key_id": "key-gemini-video-cancel-direct-123",
                            "method": "POST",
                            "url": "https://generativelanguage.googleapis.com/v1beta/models/veo-3/operations/ext-123:cancel",
                            "headers": {
                                "authorization": "Bearer upstream-key",
                                "content-type": "application/json"
                            },
                            "body": {
                                "json_body": {}
                            },
                            "stream": false,
                            "client_api_format": "gemini:video",
                            "provider_api_format": "gemini:video",
                            "model_name": "veo-3"
                        },
                        "report_kind": "gemini_video_cancel_sync_finalize",
                        "report_context": {
                            "user_id": "user-gemini-video-cancel-direct-123",
                            "api_key_id": "key-gemini-video-cancel-direct-123",
                            "task_id": "models/veo-3/operations/ext-123"
                        }
                    }))
                }
            }),
        )
        .route(
            "/api/internal/gateway/finalize-sync",
            any(move |request: Request| {
                let seen_finalize_inner = Arc::clone(&seen_finalize_clone);
                async move {
                    let (parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("finalize payload should parse");
                    *seen_finalize_inner.lock().expect("mutex should lock") =
                        Some(SeenFinalizeSyncRequest {
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
                    let mut response = Response::builder()
                        .status(StatusCode::OK)
                        .body(Body::from("{}"))
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
            "/v1beta/models/veo-3/operations/ext-123:cancel",
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
                    "request_id": "req-gemini-video-cancel-direct-123",
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {}
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
    let gateway =
        build_router_with_endpoints(upstream_url.clone(), Some(upstream_url), Some(executor_url))
            .expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/v1beta/models/veo-3/operations/ext-123:cancel"
        ))
        .header("x-goog-api-key", "client-key")
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(TRACE_ID_HEADER, "trace-gemini-video-cancel-direct-123")
        .body("{}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let response_json: serde_json::Value = response.json().await.expect("body should parse");
    assert_eq!(response_json, json!({}));

    let seen_plan_request = seen_plan
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("plan-sync should be captured");
    assert_eq!(
        seen_plan_request.trace_id,
        "trace-gemini-video-cancel-direct-123"
    );
    assert_eq!(
        seen_plan_request.path,
        "/v1beta/models/veo-3/operations/ext-123:cancel"
    );

    let seen_executor_request = seen_executor
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("executor sync should be captured");
    assert_eq!(seen_executor_request.method, "POST");
    assert_eq!(
        seen_executor_request.url,
        "https://generativelanguage.googleapis.com/v1beta/models/veo-3/operations/ext-123:cancel"
    );
    assert_eq!(seen_executor_request.provider_api_format, "gemini:video");

    let seen_finalize_request = seen_finalize
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("finalize-sync should be captured");
    assert_eq!(
        seen_finalize_request.trace_id,
        "trace-gemini-video-cancel-direct-123"
    );
    assert_eq!(
        seen_finalize_request.report_kind,
        "gemini_video_cancel_sync_finalize"
    );
    assert_eq!(seen_finalize_request.status_code, 200);

    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    executor_handle.abort();
    upstream_handle.abort();
}
