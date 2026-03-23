use super::*;

#[tokio::test]
async fn gateway_skips_repeated_direct_plan_attempts_after_proxy_public_fallback() {
    let plan_hits = Arc::new(Mutex::new(0usize));
    let plan_hits_clone = Arc::clone(&plan_hits);
    let execute_hits = Arc::new(Mutex::new(0usize));
    let execute_hits_clone = Arc::clone(&execute_hits);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/plan-sync",
            any(move |_request: Request| {
                let plan_hits_inner = Arc::clone(&plan_hits_clone);
                async move {
                    *plan_hits_inner.lock().expect("mutex should lock") += 1;
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

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_endpoints(
        "http://127.0.0.1:9",
        Some(upstream_url),
        Some("http://127.0.0.1:9".to_string()),
    )
    .expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let client = reqwest::Client::new();
    for trace_id in ["trace-openai-chat-bypass-1", "trace-openai-chat-bypass-2"] {
        let response = client
            .post(format!("{gateway_url}/v1/chat/completions"))
            .header(http::header::CONTENT_TYPE, "application/json")
            .header(CONTROL_EXECUTE_FALLBACK_HEADER, "true")
            .header(TRACE_ID_HEADER, trace_id)
            .body("{\"model\":\"gpt-5\",\"messages\":[]}")
            .send()
            .await
            .expect("request should succeed");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.text().await.expect("body should read"),
            "{\"fallback\":true}"
        );
    }

    assert_eq!(*plan_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 2);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_defaults_to_public_proxy_when_control_execute_fallback_is_not_enabled() {
    let execute_hits = Arc::new(Mutex::new(0usize));
    let execute_hits_clone = Arc::clone(&execute_hits);
    let public_hits = Arc::new(Mutex::new(0usize));
    let public_hits_clone = Arc::clone(&public_hits);
    let public_execution_path = Arc::new(Mutex::new(None::<String>));
    let public_execution_path_clone = Arc::clone(&public_execution_path);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/plan-sync",
            any(|_request: Request| async move {
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
        )
        .route(
            "/v1/chat/completions",
            any(move |request: Request| {
                let public_hits_inner = Arc::clone(&public_hits_clone);
                let public_execution_path_inner = Arc::clone(&public_execution_path_clone);
                async move {
                    *public_hits_inner.lock().expect("mutex should lock") += 1;
                    *public_execution_path_inner
                        .lock()
                        .expect("mutex should lock") = Some(
                        request
                            .headers()
                            .get(EXECUTION_PATH_HEADER)
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or_default()
                            .to_string(),
                    );
                    let mut response = Response::builder()
                        .status(StatusCode::ACCEPTED)
                        .body(Body::from("{\"proxied\":true}"))
                        .expect("response should build");
                    response.headers_mut().insert(
                        http::header::CONTENT_TYPE,
                        HeaderValue::from_static("application/json"),
                    );
                    response
                }
            }),
        );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_endpoints(
        upstream_url.clone(),
        Some(upstream_url.clone()),
        Some("http://127.0.0.1:9".to_string()),
    )
    .expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .body("{\"model\":\"gpt-5\",\"messages\":[]}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(
        response
            .headers()
            .get(EXECUTION_PATH_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some(EXECUTION_PATH_PUBLIC_PROXY_AFTER_EXECUTOR_MISS)
    );
    assert_eq!(
        response.text().await.expect("body should read"),
        "{\"proxied\":true}"
    );
    assert_eq!(*execute_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*public_hits.lock().expect("mutex should lock"), 1);
    assert_eq!(
        public_execution_path
            .lock()
            .expect("mutex should lock")
            .clone()
            .as_deref(),
        Some(EXECUTION_PATH_PUBLIC_PROXY_AFTER_EXECUTOR_MISS)
    );

    gateway_handle.abort();
    upstream_handle.abort();
}
